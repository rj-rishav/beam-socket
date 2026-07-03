//! Synthetic event generator for the RFC 0001 spike. NO real I/O in here.
//!
//! A Tokio task produces `Event { conn_id, enqueued_at_ns, payload }` at a
//! controlled rate into a **bounded** mpsc channel. Overflow is counted, never
//! silent (Rule 5 / RFC 0001 §3). The generator is the only thing feeding the
//! bridge, so the number the harness measures is the bridge's number — nothing
//! else contaminates it (RFC 0001 §9).

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Event {
    pub conn_id: u64,
    /// CLOCK_MONOTONIC nanoseconds at enqueue — latency measurement's start.
    /// Same clock domain as Node's `process.hrtime.bigint()` on Linux, so the
    /// two are directly correlatable (see `now_ns`).
    pub enqueued_at_ns: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy)]
pub struct GeneratorConfig {
    /// Offered load. `0` means "as fast as possible" (ramp-to-failure / ceiling
    /// probe): the generator offers unbounded load and the delivered rate the
    /// harness observes is the consumer ceiling.
    pub events_per_sec: u64,
    pub payload_bytes: usize,
    pub duration_secs: u64,
    /// Bounded queue capacity (Rule 5). Overflow is counted, never silent.
    pub queue_capacity: usize,
}

/// Lock-free counters. Read from the JS thread via the bridge's `pressure()`
/// getter while the generator runs, so no `Mutex` on the hot path.
#[derive(Debug, Default)]
pub struct GeneratorStats {
    pub produced: AtomicU64,
    pub dropped: AtomicU64,
}

impl GeneratorStats {
    pub fn produced(&self) -> u64 {
        self.produced.load(Ordering::Relaxed)
    }
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Handle returned by [`spawn_generator`]. Owns the shared counters, the run
/// epoch (for relative-ns latency math), and a stop flag.
pub struct Generator {
    pub stats: Arc<GeneratorStats>,
    /// CLOCK_MONOTONIC ns captured when the generator was spawned. All
    /// latency math is expressed relative to this to keep values inside f64's
    /// exact-integer range for a 10-minute run.
    pub epoch_ns: u64,
    stop: Arc<AtomicBool>,
}

impl Generator {
    /// Signal the generator to stop; idempotent.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// CLOCK_MONOTONIC in nanoseconds. On Linux this is the exact clock libuv uses
/// for `process.hrtime.bigint()`, so a timestamp taken here and one taken in JS
/// share an origin and can be subtracted directly.
#[inline]
pub fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, owned timespec; CLOCK_MONOTONIC always exists.
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
}

/// Build a deterministic, **valid-JSON** payload of approximately `n` bytes.
/// Valid JSON so the informational `json` consumer profile (`JSON.parse` +
/// `JSON.stringify`) — the one that headlines the results doc — actually
/// exercises real parse/serialize work. Content is irrelevant to the
/// copy-vs-external buffer measurement; only the size matters there.
pub fn make_payload(n: usize) -> Bytes {
    // `{"d":"<pad>"}` — overhead is 8 bytes of framing.
    const FRAME: usize = 8;
    let pad = n.saturating_sub(FRAME).max(1);
    let mut v = Vec::with_capacity(pad + FRAME);
    v.extend_from_slice(b"{\"d\":\"");
    v.resize(pad + 6, b'a');
    v.extend_from_slice(b"\"}");
    Bytes::from(v)
}

/// Spawn the generator on the ambient Tokio runtime. Returns the receiving end
/// of the bounded queue plus a [`Generator`] handle.
///
/// Overflow policy: **drop-newest**. When the bounded channel is full,
/// `try_send` fails and the event is discarded and counted in `dropped`. We
/// measure the overflow; we do not prevent it (RFC 0001 §3, spike README).
pub fn spawn_generator(config: GeneratorConfig) -> (mpsc::Receiver<Event>, Generator) {
    let (tx, rx) = mpsc::channel::<Event>(config.queue_capacity.max(1));
    let stats = Arc::new(GeneratorStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let epoch_ns = now_ns();

    let template = make_payload(config.payload_bytes);

    let stats_task = stats.clone();
    let stop_task = stop.clone();

    tokio::spawn(async move {
        let deadline_ns = if config.duration_secs == 0 {
            u64::MAX
        } else {
            epoch_ns + config.duration_secs * 1_000_000_000
        };
        let mut conn: u64 = 0;

        if config.events_per_sec == 0 {
            // Unbounded offer (ceiling probe): emit as fast as the consumer
            // drains, yielding periodically so the runtime stays responsive.
            let mut since_yield: u32 = 0;
            loop {
                if stop_task.load(Ordering::Relaxed) || now_ns() >= deadline_ns {
                    break;
                }
                emit(&tx, &stats_task, &template, &mut conn);
                since_yield += 1;
                if since_yield >= 1024 {
                    since_yield = 0;
                    tokio::task::yield_now().await;
                }
            }
            return;
        }

        // Paced emission. Wake on a fixed 1 ms tick and emit the per-tick
        // quota, using absolute deadlines so we don't accumulate drift. 1 ms is
        // the reliable Linux timer granularity; per-event enqueue timestamps
        // are taken at true send time, so intra-tick bursting does not distort
        // per-event latency.
        const TICK_NS: u64 = 1_000_000;
        let per_tick = config.events_per_sec.div_ceil(1000).max(1);
        let mut next = epoch_ns + TICK_NS;

        loop {
            if stop_task.load(Ordering::Relaxed) || now_ns() >= deadline_ns {
                break;
            }
            for _ in 0..per_tick {
                emit(&tx, &stats_task, &template, &mut conn);
            }
            // Sleep until the next tick boundary.
            let now = now_ns();
            if next > now {
                let dur = std::time::Duration::from_nanos(next - now);
                tokio::time::sleep(dur).await;
            } else {
                // Behind schedule; give the runtime a chance without sleeping.
                tokio::task::yield_now().await;
            }
            next += TICK_NS;
        }
    });

    (
        rx,
        Generator {
            stats,
            epoch_ns,
            stop,
        },
    )
}

#[inline]
fn emit(
    tx: &mpsc::Sender<Event>,
    stats: &GeneratorStats,
    template: &Bytes,
    conn: &mut u64,
) {
    let ev = Event {
        conn_id: *conn,
        enqueued_at_ns: now_ns(),
        payload: template.clone(), // Bytes clone = refcount bump, no copy
    };
    *conn = conn.wrapping_add(1);
    stats.produced.fetch_add(1, Ordering::Relaxed);
    // Drop-newest on overflow; counted, never silent.
    if tx.try_send(ev).is_err() {
        stats.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Design C flat wire format. One contiguous buffer per flush, decoded by a JS
/// cursor reader. Little-endian throughout.
///
/// ```text
/// [u32 count]
/// repeated count times:
///   [u32 conn_id][f64 rel_ns][u32 payload_len][payload bytes]
/// ```
pub mod flat {
    /// One event's decodable fields (payload borrowed).
    pub struct FlatEvent<'a> {
        pub conn_id: u32,
        pub rel_ns: f64,
        pub payload: &'a [u8],
    }

    /// Exact encoded size for `events`, so the flush buffer allocates once.
    pub fn encoded_len(payload_lens: impl Iterator<Item = usize>) -> usize {
        4 + payload_lens.map(|l| 4 + 8 + 4 + l).sum::<usize>()
    }

    /// Append one event to `out`.
    #[inline]
    pub fn push_event(out: &mut Vec<u8>, conn_id: u32, rel_ns: f64, payload: &[u8]) {
        out.extend_from_slice(&conn_id.to_le_bytes());
        out.extend_from_slice(&rel_ns.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
    }

    /// Write the count header. Call once, before any `push_event`.
    #[inline]
    pub fn push_header(out: &mut Vec<u8>, count: u32) {
        out.extend_from_slice(&count.to_le_bytes());
    }

    /// Decode a whole flush buffer. Mirrors the JS cursor reader exactly so the
    /// round-trip is testable in `cargo test`.
    pub fn decode(buf: &[u8]) -> Vec<FlatEvent<'_>> {
        let mut out = Vec::new();
        if buf.len() < 4 {
            return out;
        }
        let count = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut off = 4usize;
        for _ in 0..count {
            let conn_id = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
            off += 4;
            let rel_ns = f64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
            off += 8;
            let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let payload = &buf[off..off + len];
            off += len;
            out.push(FlatEvent {
                conn_id,
                rel_ns,
                payload,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// RFC 0001 §3 / ENGINEERING.md §4 step 1: fill the bounded queue and
    /// assert the overflow policy fires and drops are *counted*.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_queue_counts_drops() {
        // Tiny queue, high offered load, and we never drain -> overflow.
        let cfg = GeneratorConfig {
            events_per_sec: 0, // as fast as possible
            payload_bytes: 64,
            duration_secs: 1,
            queue_capacity: 8,
        };
        let (_rx, gen) = spawn_generator(cfg);
        // Deliberately do NOT read from _rx: the queue fills and stays full.
        tokio::time::sleep(Duration::from_millis(150)).await;
        gen.stop();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let produced = gen.stats.produced();
        let dropped = gen.stats.dropped();
        assert!(produced > 0, "generator should have produced events");
        assert!(
            dropped > 0,
            "un-drained bounded queue must count drops (produced={produced}, dropped={dropped})"
        );
        // Everything above capacity that we didn't drain must have been dropped.
        assert!(
            dropped >= produced - cfg.queue_capacity as u64,
            "drops undercount: produced={produced} dropped={dropped} cap={}",
            cfg.queue_capacity
        );
    }

    /// The paced generator should track its offered rate to a reasonable
    /// tolerance when the consumer keeps up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn paced_rate_is_approximately_honored() {
        let cfg = GeneratorConfig {
            events_per_sec: 100_000,
            payload_bytes: 64,
            duration_secs: 0,
            queue_capacity: 100_000,
        };
        let (mut rx, gen) = spawn_generator(cfg);
        let received = Arc::new(AtomicU64::new(0));
        let r2 = received.clone();
        let drain = tokio::spawn(async move {
            while let Some(_ev) = rx.recv().await {
                r2.fetch_add(1, Ordering::Relaxed);
            }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        gen.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), drain).await;

        let got = received.load(Ordering::Relaxed);
        // 100k/s for ~0.5s ≈ 50k. Allow a wide band for CI jitter.
        assert!(
            (20_000..90_000).contains(&got),
            "expected ~50k events in 0.5s, got {got}"
        );
    }

    /// RFC 0001 / README: "encoder round-trip". Encode a batch with the design
    /// C codec, decode it, assert every field survives.
    #[test]
    fn flat_encoder_round_trips() {
        let batch: Vec<(u32, f64, Vec<u8>)> = vec![
            (0, 12.5, b"{}".to_vec()),
            (7, 1_234_567.875, vec![0u8; 4096]),
            (u32::MAX, 0.0, b"a".to_vec()),
            (42, -3.5, Vec::new()),
        ];
        let cap = flat::encoded_len(batch.iter().map(|(_, _, p)| p.len()));
        let mut buf = Vec::with_capacity(cap);
        flat::push_header(&mut buf, batch.len() as u32);
        for (c, t, p) in &batch {
            flat::push_event(&mut buf, *c, *t, p);
        }
        assert_eq!(buf.len(), cap, "encoded_len must be exact (one allocation)");

        let decoded = flat::decode(&buf);
        assert_eq!(decoded.len(), batch.len());
        for (d, (c, t, p)) in decoded.iter().zip(batch.iter()) {
            assert_eq!(d.conn_id, *c);
            assert_eq!(d.rel_ns, *t);
            assert_eq!(d.payload, p.as_slice());
        }
    }

    #[test]
    fn payload_is_valid_json_and_sized() {
        for n in [16usize, 64, 512, 4096] {
            let p = make_payload(n);
            let s = std::str::from_utf8(&p).unwrap();
            // Structural check (no serde_json dependency in this throwaway crate).
            assert!(s.starts_with("{\"d\":\""), "payload not JSON-shaped: {s:.16}");
            assert!(s.ends_with("\"}"));
            assert!(p.len() >= n.min(9), "payload too small for n={n}: {}", p.len());
        }
    }
}
