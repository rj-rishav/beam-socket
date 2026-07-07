//! The #[napi] surface — TRANSLATION ONLY (logic lives in beamsocket-core).
//! Compiled only with `--features napi` so `cargo test --workspace` never
//! links Node symbols (see Cargo.toml).
//!
//! Command surface is flat functions/methods taking primitive IDs
//! (ARCHITECTURE.md §2.2): connection IDs cross as two u32 halves (hi, lo)
//! because a u64 exceeds f64's 2^53 integer precision and BigInt marshaling
//! costs more than two doubles.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use beamsocket_core::broadcast::FanoutReport;
use beamsocket_core::config::{BackpressurePolicy, Config, TrustProxy};
use beamsocket_core::engine::{Engine, SendStatus};
use beamsocket_core::identity::AuthorizeOutcome;
use beamsocket_core::ids::ConnectionId;
use beamsocket_core::metrics::Metrics;
use beamsocket_core::rooms::MembershipChange;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::JsFunction;
use napi_derive::napi;

use crate::bridge::{drain_loop, ENGINE_BRIDGE_QUEUE_CAPACITY};
use crate::buffers::should_externalize;

/// Bound on the TSFN delivery queue (Rule 5). Small on purpose: with Blocking
/// call mode a slow JS consumer back-pressures the drain thread, which stops
/// draining the bounded engine→bridge queue, which then sheds with a counter —
/// the RFC 0001 survival-gate behavior. A large TSFN queue would be an
/// uncounted buffer that hides true pressure (spike lib.rs, same rationale).
const TSFN_QUEUE_BOUND: usize = 4;

#[napi(object)]
pub struct JsConfig {
    pub max_payload_bytes: Option<f64>,
    pub high_water_mark: Option<f64>,
    /// "disconnect" | "drop-newest" | "drop-oldest"
    pub backpressure_policy: Option<String>,
    pub ping_interval_ms: Option<f64>,
    pub pong_timeout_ms: Option<f64>,
    // ── Phase 1C ──
    /// 0 = unlimited. Enforced at the HTTP upgrade (429) before a WS exists.
    pub max_connections_per_ip: Option<f64>,
    /// 0 = unlimited. Enforced in `join`, in Rust, before any per-message work.
    pub max_rooms_per_connection: Option<f64>,
    /// trustProxy mode: "never" (default) | "always" | "cidrs" (the SDK maps
    /// `false | true | string[]` onto this).
    pub trust_proxy_mode: Option<String>,
    /// CIDR allowlist when `trust_proxy_mode == "cidrs"`.
    pub trust_proxy_cidrs: Option<Vec<String>>,
    /// authorize round-trip timeout (ms).
    pub authorize_timeout_ms: Option<f64>,
    /// Bounded pending-upgrade table size (Rule 5).
    pub max_pending_authorizations: Option<f64>,
}

fn to_config(js: JsConfig) -> Result<Config> {
    let mut c = Config::default();
    if let Some(v) = js.max_payload_bytes {
        c.limits.max_payload_bytes = v as usize;
    }
    if let Some(v) = js.high_water_mark {
        c.backpressure.high_water_mark = v as usize;
    }
    if let Some(p) = js.backpressure_policy.as_deref() {
        c.backpressure.policy = match p {
            "disconnect" => BackpressurePolicy::Disconnect,
            "drop-newest" => BackpressurePolicy::DropNewest,
            "drop-oldest" => BackpressurePolicy::DropOldest,
            other => {
                return Err(Error::from_reason(format!(
                    "unknown backpressure policy {other:?}"
                )))
            }
        };
    }
    if let Some(v) = js.ping_interval_ms {
        c.keepalive.ping_interval = std::time::Duration::from_millis(v as u64);
    }
    if let Some(v) = js.pong_timeout_ms {
        c.keepalive.pong_timeout = std::time::Duration::from_millis(v as u64);
    }
    if let Some(v) = js.max_connections_per_ip {
        c.limits.max_connections_per_ip = v as u32;
    }
    if let Some(v) = js.max_rooms_per_connection {
        c.limits.max_rooms_per_connection = v as u32;
    }
    c.trust_proxy = match js.trust_proxy_mode.as_deref() {
        None | Some("never") => TrustProxy::Never,
        Some("always") => TrustProxy::Always,
        Some("cidrs") => TrustProxy::Cidrs(js.trust_proxy_cidrs.unwrap_or_default()),
        Some(other) => {
            return Err(Error::from_reason(format!(
                "unknown trustProxy mode {other:?}"
            )))
        }
    };
    if let Some(v) = js.authorize_timeout_ms {
        c.authorize.timeout = std::time::Duration::from_millis(v as u64);
    }
    if let Some(v) = js.max_pending_authorizations {
        c.authorize.max_pending = v as usize;
    }
    // Surface config errors (e.g. a malformed trustProxy CIDR) at construction.
    c.validate()
        .map_err(|e| Error::from_reason(e.to_string()))?;
    Ok(c)
}

/// Counter snapshot (full metrics surface is Phase 1D; these are the Rule 5
/// counters plus what the Phase 1A gates need).
#[napi(object)]
pub struct JsStats {
    pub connections: f64,
    pub users: f64,
    pub rooms: f64,
    pub messages_in: f64,
    pub messages_out: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub backpressure_drops: f64,
    /// Rust→JS bridge saturation gauge (in-flight ÷ capacity), 0.0..=1.0.
    pub bridge_pressure: f64,
    pub bridge_dropped: f64,
    // Phase 1C admission-control rejections (every reject is counted).
    pub admission_rejected_ip: f64,
    pub authorize_rejected: f64,
    pub authorize_timed_out: f64,
    pub pending_overflow: f64,
}

#[inline]
fn halves_to_u64(hi: u32, lo: u32) -> u64 {
    ((hi as u64) << 32) | lo as u64
}

#[inline]
fn conn_id(hi: u32, lo: u32) -> ConnectionId {
    ConnectionId(halves_to_u64(hi, lo))
}

#[napi]
pub struct BeamEngine {
    engine: Option<Engine>,
    metrics: Arc<Metrics>,
    drain: Option<std::thread::JoinHandle<()>>,
}

#[napi]
impl BeamEngine {
    /// Boot the engine (its own multi-threaded Tokio runtime; the Node loop
    /// is never blocked) and wire the graduated design-C bridge: bounded
    /// engine→bridge queue (ENGINE_BRIDGE_QUEUE_CAPACITY), drain thread
    /// batching at BRIDGE_BATCH/BRIDGE_FLUSH_INTERVAL, one Blocking TSFN call
    /// per flush carrying one flat-encoded buffer.
    #[napi(factory)]
    pub fn start(cfg: JsConfig, has_authorize: bool, on_flush: JsFunction) -> Result<BeamEngine> {
        let config = to_config(cfg)?;
        // Authorize requests ride the SAME batched design-C bridge (a new kind
        // byte, no second channel); JS replies out-of-band via `resolveAuthorize`
        // (a flat command like send/join). `has_authorize` tells the engine
        // whether to run the round-trip at all — no hook means accept-all with
        // no JS involvement (Rule 1).
        let (engine, rx) = Engine::start(config, ENGINE_BRIDGE_QUEUE_CAPACITY, has_authorize)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let metrics = engine.metrics();

        let tsfn: ThreadsafeFunction<Vec<u8>, ErrorStrategy::Fatal> = on_flush
            .create_threadsafe_function(
                TSFN_QUEUE_BOUND,
                |ctx: ThreadSafeCallContext<Vec<u8>>| {
                    // One Buffer per flush: external (zero-copy, one finalizer
                    // amortized over the whole batch) at/above the measured
                    // 16 KB crossover, copied into V8 below it — RFC 0001
                    // §"Copy vs external buffers", via buffers.rs.
                    let buf = if should_externalize(ctx.value.len()) {
                        ctx.env.create_buffer_with_data(ctx.value)?.into_raw()
                    } else {
                        ctx.env.create_buffer_copy(ctx.value)?.into_raw()
                    };
                    Ok(vec![buf])
                },
            )?;

        // Dedicated drain thread with its OWN current-thread runtime (for the
        // flush timer): engine shutdown can never strand it — the engine
        // side's senders dropping closes the channel, the drain flushes what
        // it holds, exits, and releases the TSFN so Node can exit.
        let drain = std::thread::Builder::new()
            .name("beamsocket-bridge".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .expect("bridge drain runtime");
                rt.block_on(drain_loop(rx, |flush_buf| {
                    // Blocking: back-pressure into the bounded engine→bridge
                    // queue instead of growing the TSFN queue without bound.
                    tsfn.call(flush_buf, ThreadsafeFunctionCallMode::Blocking);
                }));
                // tsfn dropped here → released → Node's loop can exit.
            })
            .map_err(|e| Error::from_reason(format!("spawn drain thread: {e}")))?;

        Ok(BeamEngine {
            engine: Some(engine),
            metrics,
            drain: Some(drain),
        })
    }

    /// Bind + start accepting. Returns the actually-bound port (0 = ephemeral).
    #[napi]
    pub fn listen(&self, port: u32) -> Result<u32> {
        let engine = self.engine()?;
        engine
            .listen(port as u16)
            .map(|p| p as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JS→Rust hot path. Sync napi call: registry lookup + bounded-mailbox
    /// push, sub-µs (RFC 0001 §"JS→Rust direction"). The Buffer is copied
    /// once into Rust-owned memory — the single unavoidable outbound copy
    /// (ARCHITECTURE.md §2.2). Returns 0 Queued / 1 Backpressure / 2 NotFound.
    #[napi]
    pub fn send(&self, id_hi: u32, id_lo: u32, data: Buffer, is_binary: bool) -> Result<u32> {
        let engine = self.engine()?;
        Ok(status_code(engine.send(
            conn_id(id_hi, id_lo),
            bytes::Bytes::from(data.to_vec()),
            is_binary,
        )))
    }

    /// Text fast path: avoids constructing a Buffer on the JS side.
    #[napi]
    pub fn send_text(&self, id_hi: u32, id_lo: u32, data: String) -> Result<u32> {
        let engine = self.engine()?;
        Ok(status_code(engine.send(
            conn_id(id_hi, id_lo),
            bytes::Bytes::from(data.into_bytes()),
            false,
        )))
    }

    /// Server-initiated close handshake. Close bookkeeping runs in Rust
    /// (Rule 1); JS only learns the outcome from the Closed event.
    #[napi]
    pub fn close_connection(
        &self,
        id_hi: u32,
        id_lo: u32,
        code: u32,
        reason: String,
    ) -> Result<bool> {
        let engine = self.engine()?;
        Ok(engine.close_connection(conn_id(id_hi, id_lo), code as u16, &reason))
    }

    #[napi]
    pub fn connection_count(&self) -> Result<f64> {
        Ok(self.engine()?.connection_count() as f64)
    }

    // ── Phase 1B: rooms + broadcast. Fan-out happens in Rust — each of
    // these is ONE FFI call regardless of recipient count (Rule 1). ──

    /// Join a room. Returns 0 Changed / 1 NoOp (already joined) / 2 NotFound.
    #[napi]
    pub fn join(&self, id_hi: u32, id_lo: u32, room: String) -> Result<u32> {
        Ok(membership_code(
            self.engine()?.join(conn_id(id_hi, id_lo), &room),
        ))
    }

    /// Leave a room. Same result codes as `join`.
    #[napi]
    pub fn leave(&self, id_hi: u32, id_lo: u32, room: String) -> Result<u32> {
        Ok(membership_code(
            self.engine()?.leave(conn_id(id_hi, id_lo), &room),
        ))
    }

    /// Room broadcast. `except`: flat [hi0, lo0, hi1, lo1, …] id pairs.
    /// One payload copy at this boundary (Buffer→Bytes, the single
    /// unavoidable outbound copy), then refcount clones per recipient.
    #[napi]
    pub fn broadcast_room(
        &self,
        room: String,
        data: Buffer,
        is_binary: bool,
        except: Uint32Array,
    ) -> Result<JsFanout> {
        let engine = self.engine()?;
        let report = engine.broadcast_room(
            &room,
            bytes::Bytes::from(data.to_vec()),
            is_binary,
            &except_ids(&except),
        );
        Ok(report.into())
    }

    /// Text fast path for room broadcast.
    #[napi]
    pub fn broadcast_text_room(
        &self,
        room: String,
        data: String,
        except: Uint32Array,
    ) -> Result<JsFanout> {
        let engine = self.engine()?;
        let report = engine.broadcast_room(
            &room,
            bytes::Bytes::from(data.into_bytes()),
            false,
            &except_ids(&except),
        );
        Ok(report.into())
    }

    /// Broadcast to every live connection.
    #[napi]
    pub fn broadcast_all(
        &self,
        data: Buffer,
        is_binary: bool,
        except: Uint32Array,
    ) -> Result<JsFanout> {
        let engine = self.engine()?;
        let report = engine.broadcast_all(
            bytes::Bytes::from(data.to_vec()),
            is_binary,
            &except_ids(&except),
        );
        Ok(report.into())
    }

    /// Text fast path for global broadcast.
    #[napi]
    pub fn broadcast_text_all(&self, data: String, except: Uint32Array) -> Result<JsFanout> {
        let engine = self.engine()?;
        let report = engine.broadcast_all(
            bytes::Bytes::from(data.into_bytes()),
            false,
            &except_ids(&except),
        );
        Ok(report.into())
    }

    #[napi]
    pub fn room_count(&self) -> Result<f64> {
        Ok(self.engine()?.room_count() as f64)
    }

    // ── Phase 1C: identity + authorize ──

    /// JS's reply to an `authorize` request (delivered via the batched bridge).
    /// `accept` + optional `userId` bind the connection; otherwise `code` is the
    /// close code. One flat command, like send/join — the round-trip is the
    /// engine's, this is just the return leg. `req_hi`/`req_lo` are the
    /// request_id halves (u64 across the FFI as two u32s, same as conn ids).
    #[napi]
    pub fn resolve_authorize(
        &self,
        req_hi: u32,
        req_lo: u32,
        accept: bool,
        user_id: String,
        has_user_id: bool,
        code: u32,
    ) -> Result<()> {
        let outcome = if accept {
            AuthorizeOutcome::Accept {
                user_id: has_user_id.then_some(user_id),
            }
        } else {
            AuthorizeOutcome::Reject { code: code as u16 }
        };
        self.engine()?
            .resolve_authorize(halves_to_u64(req_hi, req_lo), outcome);
        Ok(())
    }

    /// Fan out to every device of a user (`io.toUser(id).send()`). One FFI call;
    /// fan-out runs entirely in Rust over the sharded identity index.
    #[napi]
    pub fn broadcast_user(
        &self,
        user_id: String,
        data: Buffer,
        is_binary: bool,
        except: Uint32Array,
    ) -> Result<JsFanout> {
        let engine = self.engine()?;
        Ok(engine
            .broadcast_user(
                &user_id,
                bytes::Bytes::from(data.to_vec()),
                is_binary,
                &except_ids(&except),
            )
            .into())
    }

    /// Text fast path for user fan-out.
    #[napi]
    pub fn broadcast_text_user(
        &self,
        user_id: String,
        data: String,
        except: Uint32Array,
    ) -> Result<JsFanout> {
        let engine = self.engine()?;
        Ok(engine
            .broadcast_user(
                &user_id,
                bytes::Bytes::from(data.into_bytes()),
                false,
                &except_ids(&except),
            )
            .into())
    }

    // ── Phase 1D: presence ──

    /// The (connectionId halves, userId) pairs of a room's live members — ONE
    /// FFI call. The SDK joins metadata (which lives in JS) per entry.
    #[napi]
    pub fn presence_list(&self, room: String) -> Result<Vec<JsPresenceEntry>> {
        let engine = self.engine()?;
        Ok(engine
            .presence_list(&room)
            .into_iter()
            .map(|e| {
                let has_user_id = e.user.is_some();
                JsPresenceEntry {
                    id_hi: (e.id.0 >> 32) as u32,
                    id_lo: e.id.0 as u32,
                    user_id: e.user.map(|u| u.0).unwrap_or_default(),
                    has_user_id,
                }
            })
            .collect())
    }

    #[napi]
    pub fn stats(&self) -> Result<JsStats> {
        let engine = self.engine()?;
        let m = &self.metrics;
        Ok(JsStats {
            connections: m.connections.load(Ordering::Relaxed) as f64,
            users: engine.user_count() as f64,
            rooms: engine.room_count() as f64,
            bridge_pressure: engine.bridge_pressure(),
            messages_in: m.messages_in.load(Ordering::Relaxed) as f64,
            messages_out: m.messages_out.load(Ordering::Relaxed) as f64,
            bytes_in: m.bytes_in.load(Ordering::Relaxed) as f64,
            bytes_out: m.bytes_out.load(Ordering::Relaxed) as f64,
            backpressure_drops: m.backpressure_drops.load(Ordering::Relaxed) as f64,
            bridge_dropped: m.bridge_dropped.load(Ordering::Relaxed) as f64,
            admission_rejected_ip: m.admission_rejected_ip.load(Ordering::Relaxed) as f64,
            authorize_rejected: m.authorize_rejected.load(Ordering::Relaxed) as f64,
            authorize_timed_out: m.authorize_timed_out.load(Ordering::Relaxed) as f64,
            pending_overflow: m.pending_overflow.load(Ordering::Relaxed) as f64,
        })
    }

    /// Graceful close (Phase 1D): stop admitting (new upgrades → 503), drain
    /// existing sockets, force-close stragglers at `timeoutMs`, stop the
    /// runtime, and RELEASE the ThreadsafeFunction so the Node process exits on
    /// its own. Returns a Promise — the drain runs on the libuv threadpool, so
    /// the Node loop is never blocked for the timeout window (and stays free to
    /// service the bridge's final TSFN callbacks, which is what lets the join
    /// below complete). Idempotent: a second call resolves immediately.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn close(&mut self, timeout_ms: f64) -> AsyncTask<CloseTask> {
        AsyncTask::new(CloseTask {
            engine: self.engine.take(),
            drain: self.drain.take(),
            timeout: std::time::Duration::from_millis(timeout_ms.max(0.0) as u64),
        })
    }

    /// Abrupt teardown: stop accepting, sweep-close (1001), background runtime
    /// stop. Never blocks the Node loop and does NOT wait for a drain — prefer
    /// `close` for graceful shutdown. Idempotent.
    #[napi]
    pub fn shutdown(&mut self) {
        if let Some(engine) = self.engine.take() {
            engine.shutdown();
        }
        // Deliberately NOT joining the drain thread here: it may be parked in
        // a Blocking TSFN call waiting for THIS event loop to make room —
        // joining would deadlock (spike lib.rs, same rationale). It exits on
        // its own once the event channel closes, releasing the TSFN.
        self.drain.take();
    }

    fn engine(&self) -> Result<&Engine> {
        self.engine
            .as_ref()
            .ok_or_else(|| Error::from_reason("engine already shut down"))
    }
}

fn status_code(s: SendStatus) -> u32 {
    match s {
        SendStatus::Queued => 0,
        SendStatus::Backpressure => 1,
        SendStatus::NotFound => 2,
    }
}

fn membership_code(c: MembershipChange) -> u32 {
    match c {
        MembershipChange::Changed => 0,
        MembershipChange::NoOp => 1,
        MembershipChange::NotFound => 2,
        // Phase 1C: join refused by maxRoomsPerConnection.
        MembershipChange::LimitExceeded => 3,
    }
}

/// Decode the flat [hi, lo, hi, lo, …] except list (a trailing odd half is a
/// caller bug and is ignored).
fn except_ids(pairs: &Uint32Array) -> Vec<ConnectionId> {
    pairs.chunks_exact(2).map(|p| conn_id(p[0], p[1])).collect()
}

/// Fan-out accounting (mirrors beamsocket_core::broadcast::FanoutReport).
/// Informational — Phase 1 delivery semantics are frame delivery.
#[napi(object)]
pub struct JsFanout {
    pub attempted: f64,
    pub queued: f64,
    pub backpressured: f64,
    pub missing: f64,
}

impl From<FanoutReport> for JsFanout {
    fn from(r: FanoutReport) -> Self {
        JsFanout {
            attempted: r.attempted as f64,
            queued: r.queued as f64,
            backpressured: r.backpressured as f64,
            missing: r.missing as f64,
        }
    }
}

/// One presence entry (Phase 1D). connectionId crosses as two u32 halves; the
/// SDK joins `metadata` (which lives in JS) by connection id.
#[napi(object)]
pub struct JsPresenceEntry {
    pub id_hi: u32,
    pub id_lo: u32,
    pub user_id: String,
    pub has_user_id: bool,
}

/// Backs `BeamEngine::close` (Phase 1D). Runs the graceful drain on the libuv
/// threadpool (`compute`), off the Node loop, then joins the bridge drain thread
/// so the ThreadsafeFunction is fully released before the Promise resolves —
/// this is the "process exits by itself" guarantee.
pub struct CloseTask {
    engine: Option<Engine>,
    drain: Option<std::thread::JoinHandle<()>>,
    timeout: std::time::Duration,
}

impl Task for CloseTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        // Off the Node loop. Consuming the engine drains sockets and closes the
        // engine→bridge channel.
        if let Some(engine) = self.engine.take() {
            engine.close(self.timeout);
        }
        // Channel closed → the bridge drain loop returns and drops the TSFN.
        // Join it so no dangling TSFN keeps the event loop referenced. Safe to
        // join here (we are on the threadpool, not the Node thread, which stays
        // free to service the bridge's final callbacks).
        if let Some(drain) = self.drain.take() {
            let _ = drain.join();
        }
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<()> {
        Ok(())
    }
}
