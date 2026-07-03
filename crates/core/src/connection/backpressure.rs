//! Bounded send queue + high/low water marks — Phase 1A.
//!
//! Overflow behavior comes from config::BackpressurePolicy. Every overflow
//! increments a metric (never silent — RFC 0001 primary-gate philosophy).
//!
//! Required test: fill the queue, assert the policy fires and the drop
//! counter moves.
