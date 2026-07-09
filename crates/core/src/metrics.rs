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

    // ── Phase 1C admission control (every rejection is counted — an invisible
    // reject is as much a Rule 5 violation as an invisible drop). ──
    /// Handshakes rejected at the HTTP upgrade by `maxConnectionsPerIp` (429),
    /// before a WebSocket ever exists (limits.rs).
    pub admission_rejected_ip: AtomicU64,
    /// Connections closed because `authorize` returned `{ accept: false }`.
    pub authorize_rejected: AtomicU64,
    /// Connections closed because the `authorize` promise never settled within
    /// `authorize.timeout` — rejected-and-cleaned, never leaked (identity.rs).
    pub authorize_timed_out: AtomicU64,
    /// Handshakes rejected because the bounded pending-upgrade table was full
    /// (Rule 5 overflow policy for the authorize round-trip).
    pub pending_overflow: AtomicU64,
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

/// Derived EWMA rates (Phase 2A), written ONLY by the 1 Hz sampler task and read
/// lock-free by `stats()`. Each rate is an f64 stored as its bit pattern in an
/// `AtomicU64`, over a ~1 s and a ~10 s window. The sampler reads the counters
/// above; it never touches the message path (§12 rule 1 — zero hot-path cost).
#[derive(Debug, Default)]
pub struct Rates {
    pub messages_in_1s: AtomicU64,
    pub messages_in_10s: AtomicU64,
    pub messages_out_1s: AtomicU64,
    pub messages_out_10s: AtomicU64,
    pub bytes_in_1s: AtomicU64,
    pub bytes_in_10s: AtomicU64,
    pub bytes_out_1s: AtomicU64,
    pub bytes_out_10s: AtomicU64,
}

impl Rates {
    #[inline]
    pub fn store(slot: &AtomicU64, value: f64) {
        slot.store(value.to_bits(), Ordering::Relaxed);
    }

    #[inline]
    pub fn load(slot: &AtomicU64) -> f64 {
        f64::from_bits(slot.load(Ordering::Relaxed))
    }
}
