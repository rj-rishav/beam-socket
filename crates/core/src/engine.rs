//! Engine lifecycle. Phase 1A: boots a multi-threaded Tokio runtime on its
//! OWN threads (the Node event loop must never block), owns the listener,
//! and shuts down cleanly.
//!
//! Build spec: docs/ENGINEERING.md §5.

use crate::config::Config;

pub struct Engine {
    #[allow(dead_code)]
    config: Config,
    // Phase 1A: tokio::runtime::Runtime, listener handle, shutdown signal.
}

impl Engine {
    /// Phase 1A: validate config, start Tokio runtime, bind listener.
    pub fn start(config: Config) -> Self {
        Self { config }
    }

    /// Phase 1D: stop accepting → drain → flush → close (ENGINEERING.md §8).
    pub fn shutdown(self) {}
}
