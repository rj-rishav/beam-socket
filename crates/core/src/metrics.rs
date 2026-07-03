//! Lock-free atomic counters. The full snapshot API (`io.metrics()`) is
//! Phase 1D; the counters that Rule 5 requires ("every queue bounded, with an
//! overflow policy AND a metric") exist from Phase 1A, because an invisible
//! drop is an automatic PR rejection.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    /// Currently open connections (gauge).
    pub connections: AtomicU64,
    pub messages_in: AtomicU64,
    pub messages_out: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    /// Frames dropped / connections cut by a send-queue overflow policy.
    pub backpressure_drops: AtomicU64,
    /// `Message` events dropped at the bounded engine→bridge queue
    /// (drop-newest, RFC 0001 graduation). Open/close events are never
    /// dropped — see events.rs.
    pub bridge_dropped: AtomicU64,
}

impl Metrics {
    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn sub(counter: &AtomicU64, n: u64) {
        counter.fetch_sub(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
