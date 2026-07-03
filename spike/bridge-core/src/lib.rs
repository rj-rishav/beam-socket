//! Synthetic event generator for the RFC 0001 spike. NO real I/O in here.

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Event {
    pub conn_id: u64,
    /// Nanosecond timestamp at enqueue — the latency measurement's start.
    pub enqueued_at_ns: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy)]
pub struct GeneratorConfig {
    pub events_per_sec: u64,
    pub payload_bytes: usize,
    pub duration_secs: u64,
    /// Bounded queue capacity (Rule 5). Overflow is counted, never silent.
    pub queue_capacity: usize,
}

#[derive(Debug, Default)]
pub struct GeneratorStats {
    pub produced: u64,
    pub dropped: u64,
}

/// TODO(Phase 0, step 1 — ENGINEERING.md §4): produce events at the
/// configured rate into a bounded tokio mpsc; count drops on overflow.
/// Required unit test: fill the queue, assert drops are counted.
pub fn spawn_generator(
    _config: GeneratorConfig,
) -> (tokio::sync::mpsc::Receiver<Event>, std::sync::Arc<std::sync::Mutex<GeneratorStats>>) {
    todo!("Phase 0, step 1 — see docs/ENGINEERING.md §4")
}
