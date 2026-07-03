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

use beamsocket_core::config::{BackpressurePolicy, Config};
use beamsocket_core::engine::{Engine, SendStatus};
use beamsocket_core::ids::ConnectionId;
use beamsocket_core::metrics::Metrics;

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
    Ok(c)
}

/// Counter snapshot (full metrics surface is Phase 1D; these are the Rule 5
/// counters plus what the Phase 1A gates need).
#[napi(object)]
pub struct JsStats {
    pub connections: f64,
    pub messages_in: f64,
    pub messages_out: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub backpressure_drops: f64,
    pub bridge_dropped: f64,
}

#[inline]
fn conn_id(hi: u32, lo: u32) -> ConnectionId {
    ConnectionId(((hi as u64) << 32) | lo as u64)
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
    pub fn start(cfg: JsConfig, on_flush: JsFunction) -> Result<BeamEngine> {
        let config = to_config(cfg)?;
        let (engine, rx) = Engine::start(config, ENGINE_BRIDGE_QUEUE_CAPACITY)
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

    #[napi]
    pub fn stats(&self) -> JsStats {
        let m = &self.metrics;
        JsStats {
            connections: m.connections.load(Ordering::Relaxed) as f64,
            messages_in: m.messages_in.load(Ordering::Relaxed) as f64,
            messages_out: m.messages_out.load(Ordering::Relaxed) as f64,
            bytes_in: m.bytes_in.load(Ordering::Relaxed) as f64,
            bytes_out: m.bytes_out.load(Ordering::Relaxed) as f64,
            backpressure_drops: m.backpressure_drops.load(Ordering::Relaxed) as f64,
            bridge_dropped: m.bridge_dropped.load(Ordering::Relaxed) as f64,
        }
    }

    /// Phase 1A teardown: stop accepting, sweep-close (1001), background
    /// runtime stop. Never blocks the Node loop; full drain semantics are
    /// Phase 1D. Idempotent.
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
