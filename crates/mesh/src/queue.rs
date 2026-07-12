//! Per-peer outbound **data** queue (RFC 0004 §4.6) — the star of this PR's
//! Rule 5 audit: **byte-bounded, drop-newest-and-count, per-peer pressure
//! gauge, and the enqueuer never blocks.**
//!
//! Boundedness is by **bytes, not frames** (§4.6): a 64 KiB frame must not
//! count the same as a 64 B one, so a frame-count cap would lie. The queue
//! holds encoded frame buffers and tracks their total size; over the high-water
//! mark, the *incoming* (newest) frame is dropped and counted — the local
//! fan-out that produced it has already delivered locally; a slow peer only
//! costs that peer its own relayed copy, never head-of-line blocking for
//! everyone else.
//!
//! The writer drains this queue **coalesced** — [`PeerQueue::drain_coalesced`]
//! packs everything queued (up to a cap) into one buffer so the link does one
//! `write` per wakeup, not one per frame. That is a hard requirement, not an
//! optimization: the spike measured per-frame writes at 3.8 ms p99 and
//! coalesced writes at 680 µs — a 5.5× cliff (`0004-results.md`, "What the
//! spike changed" #1). The coalesce cap constant lives with the writer
//! ([`crate::link::COALESCE_CAP_BYTES`]).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use tokio::sync::Notify;

/// The result of an enqueue. `Dropped` means the queue was at its byte cap and
/// the newest frame was shed (and counted) — never an error, never a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Enqueued,
    Dropped,
}

/// A byte-bounded, non-blocking, drop-newest-and-count outbound queue for one
/// peer link.
pub struct PeerQueue {
    inner: Mutex<VecDeque<Vec<u8>>>,
    /// Total bytes currently queued. Written under `inner`'s lock; read
    /// lock-free for the pressure gauge.
    queued_bytes: AtomicUsize,
    hwm_bytes: usize,
    /// `relayDrops` for this peer (§4.6). Every shed frame is counted — an
    /// invisible drop is a Rule 5 violation.
    drops: AtomicU64,
    /// Wakes the writer when a frame is enqueued or the queue is closed.
    notify: Notify,
    closed: AtomicBool,
}

impl PeerQueue {
    pub fn new(hwm_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            queued_bytes: AtomicUsize::new(0),
            hwm_bytes,
            drops: AtomicU64::new(0),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Enqueue one encoded frame. **Never blocks.** If the frame would push the
    /// queued total over the high-water mark, it is dropped-newest and counted,
    /// and the writer is not woken (nothing new to send).
    ///
    /// A single frame larger than the whole HWM is dropped too (it can never
    /// fit); that is a misconfiguration — `max_frame` should be ≤ HWM — but the
    /// queue's job is to bound memory, so it sheds and counts rather than ever
    /// exceeding the cap.
    pub fn push(&self, frame: Vec<u8>) -> PushOutcome {
        let mut q = self.inner.lock().unwrap();
        let queued = self.queued_bytes.load(Ordering::Relaxed);
        if queued + frame.len() > self.hwm_bytes {
            self.drops.fetch_add(1, Ordering::Relaxed);
            return PushOutcome::Dropped;
        }
        self.queued_bytes.fetch_add(frame.len(), Ordering::Relaxed);
        q.push_back(frame);
        drop(q);
        self.notify.notify_one();
        PushOutcome::Enqueued
    }

    /// Drain queued frames into `out` (appending), FIFO, until either the queue
    /// empties or adding the next frame would exceed `cap` bytes — **but always
    /// at least one frame** if any is present, so a frame larger than `cap`
    /// still makes progress. Returns the number of frames drained.
    ///
    /// This is the coalescing step: the writer calls it once per wakeup and
    /// issues a single `write` for the whole `out` buffer.
    pub fn drain_coalesced(&self, out: &mut Vec<u8>, cap: usize) -> usize {
        let mut q = self.inner.lock().unwrap();
        let mut drained = 0usize;
        // `front`'s borrow ends at `front.len()` (NLL), before the pop below —
        // peek-then-pop is fine here because we never use the reference after
        // mutating.
        while let Some(front) = q.front() {
            if !out.is_empty() && out.len() + front.len() > cap {
                break;
            }
            let frame = q.pop_front().unwrap();
            self.queued_bytes.fetch_sub(frame.len(), Ordering::Relaxed);
            out.extend_from_slice(&frame);
            drained += 1;
        }
        drained
    }

    /// Bytes currently queued (lock-free).
    #[inline]
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    /// The high-water mark this queue was built with.
    #[inline]
    pub fn hwm_bytes(&self) -> usize {
        self.hwm_bytes
    }

    /// Per-peer pressure gauge in `0.0..=1.0` (§4.6). `queued / HWM`, clamped —
    /// the number `stats()` exposes so an operator sees a peer filling up before
    /// it starts shedding.
    pub fn pressure(&self) -> f64 {
        if self.hwm_bytes == 0 {
            return 0.0;
        }
        (self.queued_bytes() as f64 / self.hwm_bytes as f64).min(1.0)
    }

    /// Total frames shed by overflow (`relayDrops`).
    #[inline]
    pub fn drops(&self) -> u64 {
        self.drops.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queued_bytes() == 0
    }

    /// Mark the queue closed and wake the writer so it can drain-and-exit.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Await the next enqueue (or close). The writer loop's park point.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(n: usize, fill: u8) -> Vec<u8> {
        vec![fill; n]
    }

    #[test]
    fn push_under_cap_accumulates_and_raises_pressure() {
        let q = PeerQueue::new(1000);
        assert_eq!(q.pressure(), 0.0);
        assert_eq!(q.push(frame(400, 1)), PushOutcome::Enqueued);
        assert_eq!(q.queued_bytes(), 400);
        assert!((q.pressure() - 0.4).abs() < 1e-9);
        assert_eq!(q.push(frame(400, 2)), PushOutcome::Enqueued);
        assert!((q.pressure() - 0.8).abs() < 1e-9);
    }

    #[test]
    fn over_cap_drops_newest_and_counts_never_blocks() {
        let q = PeerQueue::new(1000);
        assert_eq!(q.push(frame(800, 1)), PushOutcome::Enqueued);
        // 800 + 300 > 1000 → the NEWEST is dropped; the queued 800 is untouched.
        assert_eq!(q.push(frame(300, 2)), PushOutcome::Dropped);
        assert_eq!(q.queued_bytes(), 800, "old frames survive; newest shed");
        assert_eq!(q.drops(), 1);
        // Pressure never exceeds 1.0 even as drops mount.
        for _ in 0..100 {
            let _ = q.push(frame(300, 3));
        }
        assert_eq!(q.drops(), 101);
        assert!(q.pressure() <= 1.0);
    }

    #[test]
    fn drain_coalesces_fifo_into_one_buffer() {
        let q = PeerQueue::new(10_000);
        q.push(frame(3, 0xA));
        q.push(frame(3, 0xB));
        q.push(frame(3, 0xC));
        let mut out = Vec::new();
        let n = q.drain_coalesced(&mut out, 10_000);
        assert_eq!(n, 3, "all three coalesced in one drain");
        assert_eq!(out, vec![0xA, 0xA, 0xA, 0xB, 0xB, 0xB, 0xC, 0xC, 0xC]);
        assert_eq!(q.queued_bytes(), 0);
    }

    #[test]
    fn drain_respects_cap_but_always_makes_progress() {
        let q = PeerQueue::new(10_000);
        q.push(frame(100, 1));
        q.push(frame(100, 2));
        // cap smaller than two frames → first drain takes one, second takes one.
        let mut out = Vec::new();
        assert_eq!(q.drain_coalesced(&mut out, 150), 1);
        assert_eq!(out.len(), 100);
        out.clear();
        assert_eq!(q.drain_coalesced(&mut out, 150), 1);
        assert_eq!(q.queued_bytes(), 0);

        // A single frame larger than cap still drains (progress guarantee).
        q.push(frame(500, 9));
        out.clear();
        assert_eq!(q.drain_coalesced(&mut out, 100), 1);
        assert_eq!(out.len(), 500);
    }

    #[test]
    fn oversize_single_frame_is_dropped_not_stored() {
        let q = PeerQueue::new(100);
        assert_eq!(q.push(frame(200, 1)), PushOutcome::Dropped);
        assert_eq!(q.queued_bytes(), 0);
        assert_eq!(q.drops(), 1);
    }
}
