//! Metrics — Phase 1D (bridge_pressure counter lands in Phase 0 graduation).
//!
//! Lock-free atomic counters: connections, users, messages in/out, bytes
//! in/out, backpressure drops, bridge_pressure, room count. Snapshot API for
//! io.metrics(); optional Prometheus text exposition later.
