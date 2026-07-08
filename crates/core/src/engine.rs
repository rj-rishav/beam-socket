//! Engine lifecycle — Phase 1A. Boots a multi-threaded Tokio runtime on its
//! OWN threads (the Node event loop must never block), owns the listener,
//! and shuts down without blocking the caller.
//!
//! Build spec: docs/ENGINEERING.md §5.

use std::net::IpAddr;
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
use crate::identity::{AuthorizeOutcome, AuthorizeResolution, Authorizer, IdentityRegistry};
use crate::ids::{ConnectionId, RoomId, UserId};
use crate::limits::{
    header_value, AdmittedUpgrade, ClientIpResolver, Gate, IpLimiter, HTTP_SERVICE_UNAVAILABLE,
    HTTP_TOO_MANY_REQUESTS,
};
use crate::metrics::Metrics;
use crate::presence::{LocalPresence, PresenceEntry, PresenceStore};
use crate::rooms::{MembershipChange, RoomRegistry};
use crate::transport::{Accepted, FrameSink, FrameSource, OutFrame, Transport, WebSocketTransport};

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

/// A Node-parsed HTTP upgrade request handed to `engine.attach` (Phase 1.1,
/// RFC 0002 §5). The handshake was already read by Node, so the engine receives
/// the parsed pieces rather than reading them off the wire.
#[derive(Debug, Clone)]
pub struct ParsedUpgrade {
    pub method: String,
    /// Request target (path+query) — used for `path` routing + `AuthorizeRequest.url`.
    pub url: String,
    /// Request headers, names lowercased (the SDK lowercases before crossing).
    pub headers: Vec<(String, String)>,
}

/// Result of `engine.attach` (RFC 0002 §4). On `Rejected`, the engine has
/// already written the HTTP error status to the dup'd fd and will close it; the
/// SDK just detaches the Node socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachOutcome {
    /// Admitted — the handoff is proceeding on the engine runtime (authorize,
    /// then the normal 1A–1D lifecycle).
    Accepted,
    /// Rejected before the 101 with this HTTP status (429 per-IP, 503 draining,
    /// 500 adoption failure). The engine owns writing it + closing.
    Rejected(u16),
}

pub struct Engine {
    runtime: Option<tokio::runtime::Runtime>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    identity: Arc<IdentityRegistry>,
    metrics: Arc<Metrics>,
    events: EventSender,
    config: Arc<Config>,
    /// Client-IP resolution + per-IP admission, run inside the handshake.
    gate: Arc<Gate>,
    /// Present only when the SDK registered an `authorize` hook. `None` = accept
    /// all, no round-trip to JS, userId unbound (1A/1B behavior).
    authorizer: Option<Arc<Authorizer>>,
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
        // Phase 1C: whether the SDK registered an `authorize` hook. When false,
        // no authorizer is built and connections are accepted without a JS
        // round-trip (userId unbound) — the 1A/1B behavior.
        has_authorize: bool,
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

        // Build the admission gate (config validated above, so the resolver
        // parses cleanly) and, if an authorize hook exists, the authorizer.
        let resolver = ClientIpResolver::from_trust_proxy(&config.trust_proxy)?;
        let limiter = Arc::new(IpLimiter::new(config.limits.max_connections_per_ip));
        let gate = Arc::new(Gate::new(resolver, limiter, metrics.clone()));
        let authorizer = has_authorize.then(|| {
            Arc::new(Authorizer::new(
                events.clone(),
                config.authorize.max_pending,
                config.authorize.timeout,
                metrics.clone(),
            ))
        });

        Ok((
            Self {
                runtime: Some(runtime),
                registry: Arc::new(Registry::new()),
                rooms: Arc::new(RoomRegistry::new()),
                identity: Arc::new(IdentityRegistry::new()),
                metrics,
                events,
                config: Arc::new(config),
                gate,
                authorizer,
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
        let shutdown_rx = self.shutdown_tx.subscribe();
        handle.spawn(accept_loop::<WebSocketTransport>(
            listener,
            self.conn_ctx(),
            self.registry.clone(),
            self.rooms.clone(),
            self.identity.clone(),
            self.gate.clone(),
            self.authorizer.clone(),
            shutdown_rx,
        ));
        *listening = true;
        Ok(actual)
    }

    /// The per-connection shared context (config, metrics, events). Shared by
    /// the own-port accept loop and the Phase 1.1 attach path.
    fn conn_ctx(&self) -> Arc<ConnCtx> {
        Arc::new(ConnCtx {
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            events: self.events.clone(),
        })
    }

    /// **Attach path (Phase 1.1, RFC 0002 §4/§5).** Adopt a Node-owned socket
    /// (already dup'd into `std_stream` by the binding) whose HTTP upgrade Node
    /// already parsed. Runs the SAME admission `Gate` as own-port SYNCHRONOUSLY
    /// (so the caller learns Accepted/Rejected(status) immediately), then, on
    /// admit, spawns the 101 + head replay + authorize + normal lifecycle on the
    /// engine runtime. Called from the Node thread; enters the runtime context
    /// to register the socket with the reactor.
    pub fn attach(
        &self,
        std_stream: std::net::TcpStream,
        peer: IpAddr,
        parsed: ParsedUpgrade,
        head: bytes::Bytes,
    ) -> AttachOutcome {
        let Some(runtime) = self.runtime.as_ref() else {
            return AttachOutcome::Rejected(HTTP_SERVICE_UNAVAILABLE); // engine down
        };
        let handle = runtime.handle().clone();
        let _enter = handle.enter(); // TcpStream::from_std needs the reactor in TLS
        let stream = match TcpStream::from_std(std_stream) {
            Ok(s) => s,
            Err(_) => return AttachOutcome::Rejected(500),
        };

        // Same admission gate as own-port, but run here (not in a handshake
        // callback) so the synchronous return carries the reject status.
        match self.gate.admit(peer, parsed.headers, parsed.url) {
            Err(status) => {
                handle.spawn(write_http_error_and_close(stream, status));
                AttachOutcome::Rejected(status)
            }
            Ok(admitted) => {
                let ws_key =
                    header_value(&admitted.headers, "sec-websocket-key").unwrap_or_default();
                handle.spawn(setup_attached(
                    stream,
                    ws_key,
                    head,
                    admitted,
                    self.conn_ctx(),
                    self.registry.clone(),
                    self.rooms.clone(),
                    self.identity.clone(),
                    self.authorizer.clone(),
                ));
                AttachOutcome::Accepted
            }
        }
    }

    // ── Phase 1B: rooms + broadcast (fan-out entirely in Rust, Rule 1) ──

    /// Join a room (auto-created on first join). Sync JS→Rust call.
    /// `maxRoomsPerConnection` is enforced in `rooms.rs` (Phase 1C).
    pub fn join(&self, id: ConnectionId, room: &str) -> MembershipChange {
        self.rooms.join(
            &self.registry,
            id,
            RoomId(room.to_owned()),
            self.config.limits.max_rooms_per_connection,
        )
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
            &self.identity,
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
            &self.identity,
            FanoutTarget::All,
            data,
            is_binary,
            except,
        )
    }

    // ── Phase 1C: identity ──

    /// Fan a payload out to every device of one user (`io.toUser().send()`),
    /// entirely in Rust over the sharded identity index — one FFI call, one
    /// allocation regardless of device count (reuses the 1B broadcast path).
    pub fn broadcast_user(
        &self,
        user_id: &str,
        data: bytes::Bytes,
        is_binary: bool,
        except: &[ConnectionId],
    ) -> FanoutReport {
        broadcast(
            &self.registry,
            &self.rooms,
            &self.identity,
            FanoutTarget::User(&UserId(user_id.to_owned())),
            data,
            is_binary,
            except,
        )
    }

    /// JS replied to an `authorize` request (the `resolveAuthorize` command).
    /// No-op if there is no authorizer or the id is unknown/stale.
    pub fn resolve_authorize(&self, request_id: u64, outcome: AuthorizeOutcome) {
        if let Some(auth) = &self.authorizer {
            auth.resolve(request_id, outcome);
        }
    }

    /// Distinct users with ≥1 live connection.
    pub fn user_count(&self) -> u64 {
        self.identity.user_count() as u64
    }

    /// Live device count for a user (diagnostics/tests).
    pub fn user_device_count(&self, user_id: &str) -> usize {
        self.identity.device_count(&UserId(user_id.to_owned()))
    }

    /// Distinct client IPs currently tracked by `maxConnectionsPerIp`
    /// (leak-test diagnostic; 0 when the limit is unlimited).
    pub fn tracked_ips(&self) -> usize {
        self.gate.tracked_ips()
    }

    // ── Phase 1D: presence ──

    /// The `(connectionId, userId)` pairs of a room's live members (Phase 1D).
    /// Metadata is joined SDK-side (it lives in JS). One call; the SDK makes one
    /// FFI hop for the whole room.
    pub fn presence_list(&self, room: &str) -> Vec<PresenceEntry> {
        LocalPresence.room_presence(&self.rooms, &self.registry, &RoomId(room.to_owned()))
    }

    pub fn room_count(&self) -> usize {
        self.rooms.room_count()
    }

    /// Bridge back-pressure gauge (`metrics().bridgePressure`), 0.0..=1.0.
    pub fn bridge_pressure(&self) -> f64 {
        self.events.pressure()
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

    /// Graceful close (Phase 1D). BLOCKS the calling thread until drained or the
    /// timeout — the napi binding runs this on the libuv threadpool, never the
    /// Node loop. Sequence: stop admitting (new upgrades → 503) → sweep-close
    /// every live connection (1001) → wait up to `timeout` for in-flight writes
    /// to flush and sockets to close → force-close stragglers (1001) → stop the
    /// accept loop and the runtime. Consuming `self` drops the engine's
    /// `EventSender`s, which closes the engine→bridge channel so the bridge drain
    /// thread exits and releases the `ThreadsafeFunction` — the precondition for
    /// the Node process to exit on its own.
    pub fn close(mut self, timeout: Duration) {
        use std::time::Instant;
        // 1. New upgrades → 503; the accept loop stays up to answer them.
        self.gate.set_draining(true);
        // 2. Ask every live connection to close gracefully (1001 going away):
        //    the write loop flushes its mailbox, sends Close, and awaits the
        //    peer's ack (bounded by CLOSE_GRACE).
        for (_, handle) in self.registry.handles() {
            handle.initiate_close(CLOSE_GOING_AWAY, "server draining", true);
        }
        // 3. Wait for the sockets to drain, up to the caller's timeout.
        let deadline = Instant::now() + timeout;
        while !self.registry.is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        // 4. Force-close stragglers still open at the timeout (non-graceful,
        //    still reported 1001 going away).
        for (_, handle) in self.registry.handles() {
            handle.initiate_close(CLOSE_GOING_AWAY, "drain timeout", false);
        }
        // 5. Stop accepting entirely and tear the runtime down. A short grace
        //    lets stragglers' Closed events flush to the bridge first.
        let _ = self.shutdown_tx.send(true);
        if let Some(runtime) = self.runtime.take() {
            let hard = Instant::now() + Duration::from_millis(200);
            while !self.registry.is_empty() && Instant::now() < hard {
                std::thread::sleep(Duration::from_millis(10));
            }
            runtime.shutdown_timeout(Duration::from_secs(1));
        }
        // self dropped here → EventSenders gone → bridge channel closes.
    }

    /// Stop accepting, sweep-close every connection (1001 going away), then
    /// tear the runtime down in the background. NEVER blocks the caller
    /// (the Node event loop). Abrupt counterpart to `close` — no drain wait.
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

#[allow(clippy::too_many_arguments)]
async fn accept_loop<T: Transport>(
    listener: TcpListener,
    ctx: Arc<ConnCtx>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    identity: Arc<IdentityRegistry>,
    gate: Arc<Gate>,
    authorizer: Option<Arc<Authorizer>>,
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
                Ok((tcp, peer)) => {
                    let _ = tcp.set_nodelay(true);
                    // One isolated task per connection (tokio contains its
                    // panics; run_connection contains them WITH cleanup).
                    tokio::spawn(setup_connection::<T>(
                        tcp,
                        peer.ip(),
                        ctx.clone(),
                        registry.clone(),
                        rooms.clone(),
                        identity.clone(),
                        gate.clone(),
                        authorizer.clone(),
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

#[allow(clippy::too_many_arguments)]
async fn setup_connection<T: Transport>(
    tcp: TcpStream,
    peer: IpAddr,
    ctx: Arc<ConnCtx>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    identity: Arc<IdentityRegistry>,
    gate: Arc<Gate>,
    authorizer: Option<Arc<Authorizer>>,
) {
    // The gate runs INSIDE the handshake: maxConnectionsPerIp is a plain HTTP
    // 429 before any WebSocket exists. A rejected/failed handshake never
    // reaches JS — the socket just goes away (the per-IP slot, if reserved, is
    // released inside `accept`).
    if let Ok(accepted) = T::accept(tcp, peer, &ctx.config, &gate).await {
        run_admitted(accepted, ctx, registry, rooms, identity, authorizer).await;
    }
}

/// The shared post-admission tail — authorize, insert, bind identity, run the
/// connection, clean up. Both producers converge here: `accept` (own-port) and
/// `adopt` (attach, RFC 0002 §5). Generic over the frame halves; both paths hand
/// in `WsSink`/`WsSource`.
async fn run_admitted<Snk, Src>(
    accepted: Accepted<Snk, Src>,
    ctx: Arc<ConnCtx>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    identity: Arc<IdentityRegistry>,
    authorizer: Option<Arc<Authorizer>>,
) where
    Snk: FrameSink,
    Src: FrameSource,
{
    let Accepted {
        sink,
        source,
        upgrade,
    } = accepted;
    // Hold the per-IP admission slot for the whole connection lifetime: dropping
    // this guard (on ANY return below) releases it, so a churn of
    // connect/disconnect leaves the per-IP table empty (leak test).
    let _ip_guard = upgrade.guard;

    // Authorize — the one connection-time JS round-trip (Rule 1: once per
    // connection, never per message). No hook registered → accept all, userId
    // unbound (1A/1B behavior), no round-trip.
    let (user_id, auth_request) = match &authorizer {
        Some(auth) => match auth
            .authorize(upgrade.client_ip, upgrade.url, upgrade.headers)
            .await
        {
            AuthorizeResolution::Accept {
                user_id,
                request_id,
            } => (user_id, Some(request_id)),
            AuthorizeResolution::Reject { code, reason } => {
                // Already upgraded; close the socket with the app's (or the
                // engine's transient) code. No registry insert, no Opened event.
                reject_after_upgrade(sink, code, reason).await;
                return; // _ip_guard drops → per-IP slot released
            }
        },
        None => (None, None),
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
    // Store the userId in the registry entry (conn→user, for presence) as well
    // as the sharded user index (user→conns, for toUser).
    let id = registry.insert(handle.clone(), user_id.clone());
    Metrics::add(&ctx.metrics.connections, 1);
    // Bind identity BEFORE Opened fires so `toUser` can reach a brand-new
    // device immediately. Unbound below on disconnect.
    if let Some(uid) = &user_id {
        identity.bind(uid.clone(), id);
    }

    run_connection(
        id,
        source,
        sink,
        handle,
        control_rx,
        close_rx,
        ctx.clone(),
        auth_request,
    )
    .await;

    // Bidirectional membership cleanup: the entry (and its room set) comes
    // out first, so no join can race the sweep — O(rooms) per §6.
    if let Some((_, joined)) = registry.remove_full(id) {
        rooms.disconnect_cleanup(id, joined);
    }
    // Unbind identity (auto-destroys the user entry on its last device).
    if let Some(uid) = &user_id {
        identity.unbind(uid, id);
    }
    Metrics::sub(&ctx.metrics.connections, 1);
}

/// Send a close frame with the authorize-reject code to an already-upgraded
/// socket, then tear it down. Best-effort — the client is going away regardless.
async fn reject_after_upgrade<Snk: FrameSink>(mut sink: Snk, code: u16, reason: &str) {
    let _ = sink
        .send_frame(OutFrame::Close {
            code,
            reason: reason.to_owned(),
        })
        .await;
    sink.shutdown().await;
}

/// Attach tail (RFC 0002 §5): complete the 101 + head replay via `adopt`, then
/// converge on the shared `run_admitted`. Runs on the engine runtime.
#[allow(clippy::too_many_arguments)]
async fn setup_attached(
    stream: TcpStream,
    ws_key: String,
    head: bytes::Bytes,
    admitted: AdmittedUpgrade,
    ctx: Arc<ConnCtx>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    identity: Arc<IdentityRegistry>,
    authorizer: Option<Arc<Authorizer>>,
) {
    // On adopt failure (rare handshake-completion error), `admitted` drops at
    // the end of this fn → its per-IP guard releases the slot; nothing else to
    // clean up. On success we converge on the shared own-port tail.
    if let Ok((sink, source)) = WebSocketTransport::adopt(stream, &ws_key, head, &ctx.config).await
    {
        run_admitted(
            Accepted {
                sink,
                source,
                upgrade: admitted,
            },
            ctx,
            registry,
            rooms,
            identity,
            authorizer,
        )
        .await;
    }
}

/// Write an HTTP error status (pre-101 attach rejection) to the dup'd socket and
/// close it. The client sees e.g. `429`/`503`; the SDK detaches the Node side.
async fn write_http_error_and_close(mut stream: TcpStream, status: u16) {
    use tokio::io::AsyncWriteExt;
    let reason = match status {
        HTTP_TOO_MANY_REQUESTS => "Too Many Requests",
        HTTP_SERVICE_UNAVAILABLE => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let resp =
        format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}
