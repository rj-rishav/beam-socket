//! The 1 Hz rate sampler — Phase 2A (ENGINEERING.md §12.1).
//!
//! The ONLY new runtime task the observability surface adds. It reads the
//! EXISTING lock-free counters (`metrics.rs`) once per interval and derives EWMA
//! rates (msgs/s, bytes/s, in/out) over a ~1 s and a ~10 s window, writing them
//! into `Rates` (also lock-free). It never touches the message path and never
//! takes a lock — the whole point of §12 rule 1: **diagnostics are free when
//! unused**. When the sampler is disabled (`observability.sampler_ms == 0`) this
//! task is never spawned and `stats().rates` is absent.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::{Metrics, Rates};

/// A read of the four rate-bearing counters at one instant.
#[derive(Clone, Copy)]
pub struct Counts {
    pub messages_in: u64,
    pub messages_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl Counts {
    pub fn read(m: &Metrics) -> Self {
        Self {
            messages_in: Metrics::get(&m.messages_in),
            messages_out: Metrics::get(&m.messages_out),
            bytes_in: Metrics::get(&m.bytes_in),
            bytes_out: Metrics::get(&m.bytes_out),
        }
    }
}

/// EWMA smoothing: `prev + alpha·(sample − prev)`.
#[inline]
fn ewma(prev: f64, sample: f64, alpha: f64) -> f64 {
    prev + alpha * (sample - prev)
}

/// Per-window smoothing factors for a sampling interval of `dt` seconds and time
/// constants of 1 s and 10 s: `alpha = 1 − e^(−dt/tau)`.
pub fn alphas(dt: f64) -> (f64, f64) {
    (1.0 - (-dt / 1.0).exp(), 1.0 - (-dt / 10.0).exp())
}

/// Fold one counter delta (over `dt` seconds) into its (1 s, 10 s) rate pair.
pub fn ewma_step(
    rate_1s: &AtomicU64,
    rate_10s: &AtomicU64,
    delta: u64,
    dt: f64,
    alpha_1s: f64,
    alpha_10s: f64,
) {
    let inst = if dt > 0.0 { delta as f64 / dt } else { 0.0 };
    Rates::store(rate_1s, ewma(Rates::load(rate_1s), inst, alpha_1s));
    Rates::store(rate_10s, ewma(Rates::load(rate_10s), inst, alpha_10s));
}

/// The sampler loop. Runs on the engine runtime; aborted when the runtime is
/// torn down (`close`/`shutdown`) — it owns nothing that outlives the engine.
pub async fn run(metrics: Arc<Metrics>, rates: Arc<Rates>, interval: Duration) {
    let dt = interval.as_secs_f64().max(1e-3);
    let (a1, a10) = alphas(dt);
    let mut last = Counts::read(&metrics);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // consume the immediate first tick so deltas span `interval`

    loop {
        ticker.tick().await;
        let now = Counts::read(&metrics);
        ewma_step(
            &rates.messages_in_1s,
            &rates.messages_in_10s,
            now.messages_in.wrapping_sub(last.messages_in),
            dt,
            a1,
            a10,
        );
        ewma_step(
            &rates.messages_out_1s,
            &rates.messages_out_10s,
            now.messages_out.wrapping_sub(last.messages_out),
            dt,
            a1,
            a10,
        );
        ewma_step(
            &rates.bytes_in_1s,
            &rates.bytes_in_10s,
            now.bytes_in.wrapping_sub(last.bytes_in),
            dt,
            a1,
            a10,
        );
        ewma_step(
            &rates.bytes_out_1s,
            &rates.bytes_out_10s,
            now.bytes_out.wrapping_sub(last.bytes_out),
            dt,
            a1,
            a10,
        );
        last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_rises_under_load_then_decays_to_zero() {
        let dt = 1.0;
        let (a1, a10) = alphas(dt);
        let r1 = AtomicU64::new(0);
        let r10 = AtomicU64::new(0);

        // 100 msgs/s offered for 20 ticks → the 1s rate converges near 100.
        for _ in 0..20 {
            ewma_step(&r1, &r10, 100, dt, a1, a10);
        }
        let loaded_1s = Rates::load(&r1);
        assert!(
            loaded_1s > 90.0,
            "1s rate should approach 100, got {loaded_1s}"
        );

        // Load stops (delta 0). Within 3 one-second windows the 1s rate is ~0.
        for _ in 0..3 {
            ewma_step(&r1, &r10, 0, dt, a1, a10);
        }
        let decayed = Rates::load(&r1);
        assert!(
            decayed < 10.0,
            "1s rate should decay toward 0 in 3 windows, got {decayed}"
        );

        // The 10s window is slower: still elevated, still finite (smoothing works).
        let slow = Rates::load(&r10);
        assert!(slow > decayed && slow.is_finite());
    }

    #[test]
    fn zero_dt_is_safe() {
        let r1 = AtomicU64::new(0);
        let r10 = AtomicU64::new(0);
        ewma_step(&r1, &r10, 5, 0.0, 0.5, 0.1); // no panic / no NaN
        assert_eq!(Rates::load(&r1), 0.0);
    }
}
