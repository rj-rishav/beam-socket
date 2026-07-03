//! beamsocket-core — the Rust data plane.
//!
//! Ground rules (docs/ENGINEERING.md §1) enforced in review:
//! 1. No per-message JS unless the app subscribed.
//! 2. No global locks on hot paths — registries are sharded.
//! 3. Safety features must work behind load balancers.
//! 4. Per-connection state documents its memory cost.
//! 5. Every queue is bounded.
//!
//! This crate must NEVER depend on napi. If you need JS, you are in the
//! wrong crate — see crates/node.

pub mod broadcast;
pub mod config;
pub mod connection;
pub mod engine;
pub mod events;
pub mod identity;
pub mod ids;
pub mod limits;
pub mod metrics;
pub mod presence;
pub mod rooms;
pub mod transport;
