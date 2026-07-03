//! Runtime configuration. Mirrors the TS `BeamSocketConfig` in
//! packages/beamsocket/src/types.ts — keep the two in sync.

/// Admission limits, enforced in Rust before any JS runs. (Phase 1C)
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_payload_bytes: usize,
    pub max_connections_per_ip: u32,
    pub max_rooms_per_connection: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1 << 20,
            max_connections_per_ip: 0, // 0 = unlimited
            max_rooms_per_connection: 100,
        }
    }
}

/// What to do when a connection's bounded send queue overflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePolicy {
    Disconnect,
    DropNewest,
    DropOldest,
}

#[derive(Debug, Clone)]
pub struct Backpressure {
    pub high_water_mark: usize,
    pub policy: BackpressurePolicy,
}

impl Default for Backpressure {
    fn default() -> Self {
        Self {
            high_water_mark: 64 * 1024,
            policy: BackpressurePolicy::Disconnect,
        }
    }
}

/// `false` | `true` | CIDR allowlist. Security boundary — see ARCHITECTURE.md §4.
#[derive(Debug, Clone, Default)]
pub enum TrustProxy {
    #[default]
    Never,
    Always,
    Cidrs(Vec<String>), // parsed + validated in limits.rs (Phase 1C)
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub limits: Limits,
    pub backpressure: Backpressure,
    pub trust_proxy: TrustProxy,
}
