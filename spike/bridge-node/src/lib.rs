//! The bridge under test. Designs A/B/C selectable at start.
//! Spec: RFC 0001 §3. Build order: B first (predicted winner), then A, then C.
//!
//! THROWAWAY-GRADE (RFC 0001 §8). Only the winning design's logic + constants
//! graduate into `crates/node/src/bridge.rs`.
//!
//! Latency correlation: the generator stamps each event with a CLOCK_MONOTONIC
//! timestamp, expressed as `rel_ns = enqueue - epoch` (f64, small enough to be
//! exact for a 10-minute run). The harness reads `rel_now_ns()` once at start
//! to align Node's `hrtime` clock to the same frame, then every subsequent
//! handler-entry time is computed in JS with no per-event FFI.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use bridge_core::{flat, spawn_generator, Event, Generator, GeneratorConfig};
use bytes::Bytes;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, JsBuffer, JsFunction};
use napi_derive::napi;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Design {
    /// One TSFN call per event. Baseline — expected to lose.
    NaiveTsfn,
    /// TSFN per flush; events as a JS array of objects. Flush at N or timer.
    BatchedObjects,
    /// TSFN per flush; events encoded into one contiguous Buffer,
    /// decoded by a JS cursor reader.
    BatchedFlat,
}

impl Design {
    fn parse(s: &str) -> Result<Design> {
        match s {
            "A" => Ok(Design::NaiveTsfn),
            "B" => Ok(Design::BatchedObjects),
            "C" => Ok(Design::BatchedFlat),
            other => Err(Error::from_reason(format!("unknown design {other:?}"))),
        }
    }
}

/// Config object passed from JS. All numbers arrive as f64; cast as needed.
#[napi(object)]
pub struct JsConfig {
    pub design: String,        // "A" | "B" | "C"
    pub events_per_sec: f64,   // 0 = as-fast-as-possible (ceiling probe)
    pub payload_bytes: f64,
    pub duration_secs: f64,
    pub queue_capacity: f64,
    pub batch: f64,            // N (designs B/C)
    pub timer_ms: f64,         // flush timer (designs B/C)
    /// Inbound payload buffers: external/zero-copy (true) vs copied into V8
    /// (false). The copy-vs-external crossover measurement (RFC 0001 §2 Q3).
    pub external: bool,
}

/// Snapshot returned by `pressure()`. Everything the primary gate needs to see
/// while saturated: pressure rises, drops are counted, nothing is silent.
#[napi(object)]
pub struct Pressure {
    pub produced: f64,
    pub dropped: f64,
    pub delivered: f64,
    pub flushes: f64,
    /// In-flight events not yet handled by JS: (produced - dropped) - delivered.
    /// Bounded by queue_capacity + TSFN queue bound; rises under saturation.
    pub queue_depth: f64,
    /// Normalized bridge pressure, queue_depth / queue_capacity (0..~1+).
    pub bridge_pressure: f64,
}

/// Lock-free consumer-side counters, shared with the drain thread.
#[derive(Default)]
struct Counters {
    delivered: AtomicU64,
    flushes: AtomicU64,
}

/// One event, bridge-side (rel_ns already computed).
struct EvLite {
    conn_id: u32,
    rel_ns: f64,
    payload: Bytes,
}

impl EvLite {
    #[inline]
    fn from(ev: Event, epoch_ns: u64) -> Self {
        EvLite {
            conn_id: ev.conn_id as u32,
            rel_ns: (ev.enqueued_at_ns.saturating_sub(epoch_ns)) as f64,
            payload: ev.payload,
        }
    }
}

// TSFN payload types (one per call). ErrorStrategy::Fatal => `call` takes the
// value directly.
struct MsgA {
    conn_id: u32,
    rel_ns: f64,
    payload: Bytes,
    external: bool,
}
struct MsgB {
    events: Vec<EvLite>,
    external: bool,
}
struct MsgC {
    buf: Vec<u8>,
    external: bool,
}

/// Build a Node Buffer for a payload: external/zero-copy (borrow the `Bytes`,
/// finalizer drops it) or copied into V8-managed memory.
#[inline]
fn payload_buffer(env: &Env, bytes: Bytes, external: bool) -> Result<JsBuffer> {
    if external && !bytes.is_empty() {
        let ptr = bytes.as_ptr() as *mut u8;
        let len = bytes.len();
        // SAFETY: `bytes` is moved into the finalizer hint, keeping the backing
        // allocation alive until V8 finalizes the buffer; `ptr`/`len` describe
        // exactly that allocation. We treat it read-only (the consumer never
        // mutates), and the refcounted `Bytes` means the last finalizer frees.
        // Zero-copy, one finalizer per buffer — the cost RFC 0001 §2 Q3 weighs.
        let b = unsafe {
            env.create_buffer_with_borrowed_data(ptr, len, bytes, |_hint: Bytes, _env: Env| {})
        }?;
        Ok(b.into_raw())
    } else {
        Ok(env.create_buffer_copy(bytes)?.into_raw())
    }
}

#[napi]
pub struct Bridge {
    // Runtime kept alive for the run; dropped => generator + drain torn down.
    rt: Option<tokio::runtime::Runtime>,
    generator: Generator,
    counters: Arc<Counters>,
    drain: Option<JoinHandle<()>>,
    // Set on stop/drop: the drain loop breaks immediately, discarding any
    // still-buffered events instead of processing the whole backlog through a
    // (possibly slow) handler at teardown.
    abort: Arc<AtomicBool>,
    queue_capacity: f64,
}

#[napi]
impl Bridge {
    /// Start a run: spawn the generator + drain the bounded queue into JS via
    /// the selected design. Returns immediately; the run proceeds on its own
    /// threads (the Node event loop is never blocked).
    #[napi(factory)]
    pub fn start(cfg: JsConfig, callback: JsFunction) -> Result<Bridge> {
        let design = Design::parse(&cfg.design)?;
        let gcfg = GeneratorConfig {
            events_per_sec: cfg.events_per_sec as u64,
            payload_bytes: cfg.payload_bytes as usize,
            duration_secs: cfg.duration_secs as u64,
            queue_capacity: (cfg.queue_capacity as usize).max(1),
        };
        let batch = (cfg.batch as usize).max(1);
        let timer = Duration::from_secs_f64((cfg.timer_ms / 1000.0).max(0.0));
        let external = cfg.external;

        // Bound the TSFN delivery queue too (Rule 5): with Blocking call mode,
        // a slow JS consumer back-pressures the drain thread, which stops
        // draining the bounded generator queue, which then overflows and counts
        // drops. Keep this bound SMALL so the *bounded generator queue* is the
        // real buffer and the visible pressure point — a large TSFN queue is an
        // uncounted buffer that inflates latency and hides true pressure. A few
        // slots is enough to keep the JS thread pipelined.
        let tsfn_bound: usize = match design {
            Design::NaiveTsfn => 512, // per-event calls
            _ => 4,                   // per-flush calls
        };

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| Error::from_reason(format!("runtime build: {e}")))?;

        let (rx, generator) = {
            let _guard = rt.enter();
            spawn_generator(gcfg)
        };
        let epoch_ns = generator.epoch_ns;
        let counters = Arc::new(Counters::default());
        let abort = Arc::new(AtomicBool::new(false));
        let handle = rt.handle().clone();
        let counters_drain = counters.clone();
        let abort_drain = abort.clone();

        // Build the design-appropriate TSFN and drain loop on a dedicated
        // thread that drives the runtime handle (so the blocking TSFN calls
        // never starve the generator's tokio workers).
        let drain = match design {
            Design::NaiveTsfn => {
                let tsfn: ThreadsafeFunction<MsgA, ErrorStrategy::Fatal> = callback
                    .create_threadsafe_function(tsfn_bound, |ctx: ThreadSafeCallContext<MsgA>| {
                        let mut o = ctx.env.create_object()?;
                        o.set_named_property(
                            "connId",
                            ctx.env.create_double(ctx.value.conn_id as f64)?,
                        )?;
                        let p = payload_buffer(&ctx.env, ctx.value.payload, ctx.value.external)?;
                        o.set_named_property("payload", p)?;
                        o.set_named_property("t", ctx.env.create_double(ctx.value.rel_ns)?)?;
                        Ok(vec![o])
                    })?;
                std::thread::spawn(move || {
                    handle.block_on(drain_a(rx, tsfn, counters_drain, abort_drain, epoch_ns, external));
                })
            }
            Design::BatchedObjects => {
                let tsfn: ThreadsafeFunction<MsgB, ErrorStrategy::Fatal> = callback
                    .create_threadsafe_function(tsfn_bound, |ctx: ThreadSafeCallContext<MsgB>| {
                        let ext = ctx.value.external;
                        let evs = ctx.value.events;
                        let mut arr = ctx.env.create_array_with_length(evs.len())?;
                        for (i, ev) in evs.into_iter().enumerate() {
                            let mut o = ctx.env.create_object()?;
                            o.set_named_property(
                                "connId",
                                ctx.env.create_double(ev.conn_id as f64)?,
                            )?;
                            let p = payload_buffer(&ctx.env, ev.payload, ext)?;
                            o.set_named_property("payload", p)?;
                            o.set_named_property("t", ctx.env.create_double(ev.rel_ns)?)?;
                            arr.set_element(i as u32, o)?;
                        }
                        Ok(vec![arr])
                    })?;
                std::thread::spawn(move || {
                    handle.block_on(drain_batched::<MsgB>(
                        rx, tsfn, counters_drain, abort_drain, epoch_ns, batch, timer, external,
                    ));
                })
            }
            Design::BatchedFlat => {
                let tsfn: ThreadsafeFunction<MsgC, ErrorStrategy::Fatal> = callback
                    .create_threadsafe_function(tsfn_bound, |ctx: ThreadSafeCallContext<MsgC>| {
                        // One buffer per flush: external (own the Vec, one
                        // finalizer) or copied into V8.
                        let buf = if ctx.value.external {
                            ctx.env.create_buffer_with_data(ctx.value.buf)?.into_raw()
                        } else {
                            ctx.env.create_buffer_copy(ctx.value.buf)?.into_raw()
                        };
                        Ok(vec![buf])
                    })?;
                std::thread::spawn(move || {
                    handle.block_on(drain_batched::<MsgC>(
                        rx, tsfn, counters_drain, abort_drain, epoch_ns, batch, timer, external,
                    ));
                })
            }
        };

        Ok(Bridge {
            rt: Some(rt),
            generator,
            counters,
            drain: Some(drain),
            abort,
            queue_capacity: cfg.queue_capacity.max(1.0),
        })
    }

    /// Rust CLOCK_MONOTONIC now, relative to the run epoch (f64 ns). The harness
    /// calls this once at start to align Node's hrtime clock to this frame.
    #[napi]
    pub fn rel_now_ns(&self) -> f64 {
        (bridge_core::now_ns().saturating_sub(self.generator.epoch_ns)) as f64
    }

    /// Live pressure snapshot (queryable while saturated — RFC 0001 §5).
    #[napi]
    pub fn pressure(&self) -> Pressure {
        let produced = self.generator.stats.produced();
        let dropped = self.generator.stats.dropped();
        let delivered = self.counters.delivered.load(Ordering::Relaxed);
        let flushes = self.counters.flushes.load(Ordering::Relaxed);
        let enqueued = produced.saturating_sub(dropped);
        let depth = enqueued.saturating_sub(delivered);
        Pressure {
            produced: produced as f64,
            dropped: dropped as f64,
            delivered: delivered as f64,
            flushes: flushes as f64,
            queue_depth: depth as f64,
            bridge_pressure: depth as f64 / self.queue_capacity,
        }
    }

    /// Signal the generator to stop. NON-blocking: we must not join the drain
    /// thread from the JS thread. Under backpressure the drain thread may be
    /// parked inside a `Blocking` TSFN call waiting for the JS event loop to
    /// make room; joining here would block the very loop it's waiting on
    /// (deadlock). Instead we just stop production — the generator's `tx` then
    /// drops, `rx` closes, the drain loop finishes on its own as the event loop
    /// keeps draining the TSFN, and the TSFN handle is released so Node can
    /// exit. Callers wanting a settled `pressure()` snapshot sleep briefly.
    #[napi]
    pub fn stop(&mut self) {
        self.abort.store(true, Ordering::Relaxed);
        self.generator.stop();
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.abort.store(true, Ordering::Relaxed);
        self.generator.stop();
        // Tear down OFF the JS thread: joining the drain thread (which may be
        // parked in a Blocking TSFN call) from here would deadlock the loop.
        let drain = self.drain.take();
        let rt = self.rt.take();
        std::thread::spawn(move || {
            if let Some(h) = drain {
                let _ = h.join();
            }
            if let Some(rt) = rt {
                rt.shutdown_timeout(Duration::from_millis(500));
            }
        });
    }
}

// ---- Drain loops ---------------------------------------------------------

async fn drain_a(
    mut rx: tokio::sync::mpsc::Receiver<Event>,
    tsfn: ThreadsafeFunction<MsgA, ErrorStrategy::Fatal>,
    counters: Arc<Counters>,
    abort: Arc<AtomicBool>,
    epoch_ns: u64,
    external: bool,
) {
    while let Some(ev) = rx.recv().await {
        if abort.load(Ordering::Relaxed) {
            break; // discard backlog on teardown
        }
        let lite = EvLite::from(ev, epoch_ns);
        counters.delivered.fetch_add(1, Ordering::Relaxed);
        counters.flushes.fetch_add(1, Ordering::Relaxed);
        // Blocking: back-pressure to the bounded queue instead of growing the
        // TSFN queue without bound.
        tsfn.call(
            MsgA {
                conn_id: lite.conn_id,
                rel_ns: lite.rel_ns,
                payload: lite.payload,
                external,
            },
            ThreadsafeFunctionCallMode::Blocking,
        );
    }
}

/// Shared batching loop for designs B and C. `T::build` decides whether the
/// batch ships as an array of objects (B) or one contiguous buffer (C).
#[allow(clippy::too_many_arguments)] // throwaway spike; params are all distinct
async fn drain_batched<T>(
    mut rx: tokio::sync::mpsc::Receiver<Event>,
    tsfn: ThreadsafeFunction<T, ErrorStrategy::Fatal>,
    counters: Arc<Counters>,
    abort: Arc<AtomicBool>,
    epoch_ns: u64,
    batch: usize,
    timer: Duration,
    external: bool,
) where
    T: FlushMsg + 'static,
{
    let mut buf: Vec<EvLite> = Vec::with_capacity(batch);
    loop {
        if abort.load(Ordering::Relaxed) {
            break; // discard backlog on teardown
        }
        // Wait for the first event of a batch (no timer running yet).
        match rx.recv().await {
            Some(ev) => buf.push(EvLite::from(ev, epoch_ns)),
            None => break,
        }
        // Fill until batch size or the flush timer fires (measured from the
        // first event — the oldest event's wait bounds the added latency).
        let deadline = tokio::time::sleep(timer);
        tokio::pin!(deadline);
        let mut closed = false;
        while buf.len() < batch {
            tokio::select! {
                r = rx.recv() => match r {
                    Some(ev) => buf.push(EvLite::from(ev, epoch_ns)),
                    None => { closed = true; break; }
                },
                _ = &mut deadline => break,
            }
        }
        flush(&tsfn, &counters, &mut buf, external);
        if closed {
            break;
        }
    }
    // Flush anything left after the channel closed.
    flush(&tsfn, &counters, &mut buf, external);
}

fn flush<T: FlushMsg>(
    tsfn: &ThreadsafeFunction<T, ErrorStrategy::Fatal>,
    counters: &Counters,
    buf: &mut Vec<EvLite>,
    external: bool,
) {
    if buf.is_empty() {
        return;
    }
    let events = std::mem::take(buf);
    let n = events.len() as u64;
    counters.delivered.fetch_add(n, Ordering::Relaxed);
    counters.flushes.fetch_add(1, Ordering::Relaxed);
    let msg = T::build(events, external);
    tsfn.call(msg, ThreadsafeFunctionCallMode::Blocking);
}

/// Lets the batching loop build either a `MsgB` (objects) or `MsgC` (flat).
trait FlushMsg: Sized {
    fn build(events: Vec<EvLite>, external: bool) -> Self;
}

impl FlushMsg for MsgB {
    fn build(events: Vec<EvLite>, external: bool) -> Self {
        MsgB { events, external }
    }
}

impl FlushMsg for MsgC {
    fn build(events: Vec<EvLite>, external: bool) -> Self {
        let cap = flat::encoded_len(events.iter().map(|e| e.payload.len()));
        let mut out = Vec::with_capacity(cap);
        flat::push_header(&mut out, events.len() as u32);
        for e in &events {
            flat::push_event(&mut out, e.conn_id, e.rel_ns, &e.payload);
        }
        MsgC { buf: out, external }
    }
}
