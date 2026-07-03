//! Fan-out engine — Phase 1B.
//!
//! Serialize the payload ONCE into Bytes; clone the refcounted handle into
//! each recipient's send queue. One allocation regardless of recipient count.
//! Fan-out never enters JS (Rule 1).
//!
//! Required test: broadcast with one saturated member — that member hits its
//! backpressure policy, everyone else is unaffected.
