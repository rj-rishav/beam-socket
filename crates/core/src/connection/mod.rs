//! The per-connection task — Phase 1A. This is the BEAM-inspired part:
//! each connection is an isolated Tokio task with its own bounded mailbox.
//!
//! Responsibilities: read loop, write loop, bounded send queue, ping/pong
//! keepalive, close handshake. A panic here must tear down ONE connection,
//! never the runtime — wrap the task, test the containment (ENGINEERING.md §5).
//!
//! Rule 1 reminder: ping/pong and close bookkeeping NEVER call into JS.

pub mod backpressure;
pub mod registry;
