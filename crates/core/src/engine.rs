//! Engine lifecycle — Phase 1A. Boots a multi-threaded Tokio runtime on its
//! OWN threads (the Node event loop must never block), owns the listener,
//! and shuts down without blocking the caller.
//!
//! Build spec: docs/ENGINEERING.md §5.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

use crate::broadcast::{broadcast, FanoutReport, FanoutTarget};
use crate::config::{Config, ConfigError};
use crate::connection::backpressure::{Mailbox, OutboundFrame, PushOutcome};
use crate::connection::registry::Registry;
use crate::connection::{
    run_connection, CloseSignal, ConnCtx, ConnHandle, CLOSE_BACKPRESSURE, CLOSE_GOING_AWAY,
    CONTROL_QUEUE_CAPACITY,
};
use crate::events::{EngineEvent, EventSender};
use crate::ids::{ConnectionId, RoomId};
use crate::metrics::Metrics;
use crate::rooms::{MembershipChange, RoomRegistry};
use crate::transport::{Transport, WebSocketTransport};

#[derive(Debug)]
pub enum EngineError {
    Config(ConfigError),
    Io(String),
    AlreadyListening,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Config(e) => write!(f, "{e}"),
            EngineError::Io(e) => write!(f, "io error: {e}"),
            EngineError::AlreadyListening => write!(f, "already listening"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<ConfigError> for EngineError {
    fn from(e: ConfigError) -> Self {
        EngineError::Config(e)
    }
}

/// Result of `Engine::send`, surfaced to the SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendStatus {
    /// Accepted into the connection's send queue — Phase 1's full delivery
    /// promise (ARCHITECTURE.md §4 "delivery semantics").
    Queued,
    /// Overflow policy consumed or displaced frames (already counted).
    Backpressure,
    /// Unknown/stale connection ID.
    NotFound,
}

pub struct Engine {
    runtime: Option<tokio::runtime::Runtime>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    metrics: Arc<Metrics>,
    events: EventSender,
    config: Arc<Config>,
    shutdown_tx: watch::Sender<bool>,
    listening: Mutex<bool>,
}

impl Engine {
    /// Validate config and boot the runtime. Returns the engine plus the
    /// receiving end of the BOUNDED engine→bridge event channel; the caller
    /// (crates/node) supplies the capacity so the RFC-cited constant lives
    /// next to its citation in bridge.rs.
    pub fn start(
        config: Config,
        event_queue_capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<EngineEvent>), EngineError> {
        config.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("beamsocket-engine")
            .enable_all()
            .build()
            .map_err(|e| EngineError::Io(e.to_string()))?;
        let metrics = Arc::new(Metrics::default());
        let (events, rx) = EventSender::bounded(event_queue_capacity, metrics.clone());
        let (shutdown_tx, _) = watch::channel(false);
        Ok((
            Self {
                runtime: Some(runtime),
                registry: Arc::new(Registry::new()),
                rooms: Arc::new(RoomRegistry::new()),
                metrics,
                events,
                config: Arc::new(config),
                shutdown_tx,
                listening: Mutex::new(false),
            },
            rx,
        ))
    }

    /// A handle for driving async work from foreign threads (the bridge's
    /// drain thread does NOT use this — it owns a current-thread runtime so
    /// engine shutdown can never strand it; see crates/node/src/bridge.rs).
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime
            .as_ref()
            .expect("runtime present until shutdown")
            .handle()
            .clone()
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn connection_count(&self) -> u64 {
        Metrics::get(&self.metrics.connections)
    }

    /// Diagnostic/sweep view of live connection IDs (shutdown, tests). Not a
    /// hot path — walks every shard.
    pub fn live_connection_ids(&self) -> Vec<ConnectionId> {
        self.registry
            .handles()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    /// Bind and start accepting. Blocks the caller only for the bind itself
    /// (milliseconds); the accept loop runs on the engine runtime. Returns
    /// the actually-bound port (`port = 0` → ephemeral, used by tests).
    pub fn listen(&self, port: u16) -> Result<u16, EngineError> {
        let mut listening = self.listening.lock().unwrap();
        if *listening {
            return Err(EngineError::AlreadyListening); // one listener in Phase 1A
        }
        let handle = self.handle();
        let listener = handle
            .block_on(TcpListener::bind(("0.0.0.0", port)))
            .map_err(|e| EngineError::Io(e.to_string()))?;
        let actual = listener
            .local_addr()
            .map_err(|e| EngineError::Io(e.to_string()))?
            .port();
        let ctx = Arc::new(ConnCtx {
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            events: self.events.clone(),
        });
        let registry = self.registry.clone();
        let rooms = self.rooms.clone();
        let shutdown_rx = self.shutdown_tx.subscribe();
        handle.spawn(accept_loop::<WebSocketTransport>(
            listener,
            ctx,
            registry,
            rooms,
            shutdown_rx,
        ));
        *listening = true;
        Ok(actual)
    }

    // ── Phase 1B: rooms + broadcast (fan-out entirely in Rust, Rule 1) ──

    /// Join a room (auto-created on first join). Sync JS→Rust call.
    pub fn join(&self, id: ConnectionId, room: &str) -> MembershipChange {
        self.rooms.join(&self.registry, id, RoomId(room.to_owned()))
    }

    /// Leave a room (auto-destroyed on last leave). Sync JS→Rust call.
    pub fn leave(&self, id: ConnectionId, room: &str) -> MembershipChange {
        self.rooms
            .leave(&self.registry, id, &RoomId(room.to_owned()))
    }

    /// One FFI call per broadcast; the payload is ONE allocation, cloned by
    /// refcount into each member's bounded mailbox (ENGINEERING.md §6).
    pub fn broadcast_room(
        &self,
        room: &str,
        data: bytes::Bytes,
        is_binary: bool,
        except: &[ConnectionId],
    ) -> FanoutReport {
        broadcast(
            &self.registry,
            &self.rooms,
            FanoutTarget::Room(&RoomId(room.to_owned())),
            data,
            is_binary,
            except,
        )
    }

    /// Broadcast to every live connection.
    pub fn broadcast_all(
        &self,
        data: bytes::Bytes,
        is_binary: bool,
        except: &[ConnectionId],
    ) -> FanoutReport {
        broadcast(
            &self.registry,
            &self.rooms,
            FanoutTarget::All,
            data,
            is_binary,
            except,
        )
    }

    pub fn room_count(&self) -> usize {
        self.rooms.room_count()
    }

    /// Membership size of one room (diagnostics/tests).
    pub fn room_member_count(&self, room: &str) -> usize {
        self.rooms.member_count(&RoomId(room.to_owned()))
    }

    /// JS→Rust hot path: synchronous, lock is per-shard + per-connection
    /// (Rule 2), never blocks on IO.
    pub fn send(&self, id: ConnectionId, data: bytes::Bytes, is_binary: bool) -> SendStatus {
        let Some(handle) = self.registry.get(id) else {
            return SendStatus::NotFound;
        };
        match handle.mailbox.push(OutboundFrame { data, is_binary }) {
            PushOutcome::Queued => SendStatus::Queued,
            PushOutcome::DroppedNewest | PushOutcome::DroppedOldest => SendStatus::Backpressure,
            PushOutcome::Disconnect => {
                handle.initiate_close(CLOSE_BACKPRESSURE, "backpressure", true);
                SendStatus::Backpressure
            }
            PushOutcome::Closed => SendStatus::NotFound,
        }
    }

    /// Server-initiated close (socket.close(code, reason) in JS).
    pub fn close_connection(&self, id: ConnectionId, code: u16, reason: &str) -> bool {
        match self.registry.get(id) {
            Some(handle) => {
                handle.initiate_close(code, reason, true);
                true
            }
            None => false,
        }
    }

    /// Stop accepting, sweep-close every connection (1001 going away), then
    /// tear the runtime down in the background. NEVER blocks the caller
    /// (the Node event loop) — full drain semantics are Phase 1D.
    pub fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true); // accept loop exits
        for (_, handle) in self.registry.handles() {
            handle.initiate_close(CLOSE_GOING_AWAY, "server shutting down", true);
        }
        if let Some(runtime) = self.runtime.take() {
            let registry = self.registry.clone();
            std::thread::spawn(move || {
                // Grace window for close handshakes, then hard stop.
                let deadline = std::time::Instant::now() + Duration::from_secs(3);
                while !registry.is_empty() && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(25));
                }
                runtime.shutdown_timeout(Duration::from_secs(1));
            });
        }
    }
}

async fn accept_loop<T: Transport>(
    listener: TcpListener,
    ctx: Arc<ConnCtx>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((tcp, _peer)) => {
                    let _ = tcp.set_nodelay(true);
                    // One isolated task per connection (tokio contains its
                    // panics; run_connection contains them WITH cleanup).
                    tokio::spawn(setup_connection::<T>(
                        tcp,
                        ctx.clone(),
                        registry.clone(),
                        rooms.clone(),
                    ));
                }
                Err(_) => {
                    // Transient accept errors (e.g. EMFILE): don't spin.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        }
    }
}

async fn setup_connection<T: Transport>(
    tcp: TcpStream,
    ctx: Arc<ConnCtx>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
) {
    // Handshake failures never reach JS — the socket just goes away.
    let Ok((sink, source)) = T::accept(tcp, &ctx.config).await else {
        return;
    };
    let mailbox = Mailbox::new(
        ctx.config.backpressure.high_water_mark,
        ctx.config.backpressure.policy,
        ctx.metrics.clone(),
    );
    let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
    let (close, close_rx) = CloseSignal::new();
    let handle = ConnHandle {
        mailbox,
        control: control_tx,
        close,
    };
    let id = registry.insert(handle.clone());
    Metrics::add(&ctx.metrics.connections, 1);

    run_connection(id, source, sink, handle, control_rx, close_rx, ctx.clone()).await;

    // Bidirectional membership cleanup: the entry (and its room set) comes
    // out first, so no join can race the sweep — O(rooms) per §6.
    if let Some((_, joined)) = registry.remove_full(id) {
        rooms.disconnect_cleanup(id, joined);
    }
    Metrics::sub(&ctx.metrics.connections, 1);
}
