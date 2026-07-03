//! Runtime configuration. Mirrors the TS `BeamSocketConfig` in
//! packages/beamsocket/src/types.ts — keep the two in sync.

use std::time::Duration;

/// Admission limits, enforced in Rust before any JS runs. (Phase 1C; Phase 1A
/// already enforces `max_payload_bytes` via the WebSocket codec's message cap.)
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

/// Server-side keepalive (Rule 1: runs entirely in Rust, never JS).
///
/// The engine pings a connection idle for `ping_interval`; if no pong (nor any
/// other frame) arrives within `pong_timeout` of the ping, the connection is
/// presumed dead and torn down, reported with close code 1006 (abnormal —
/// never sent on the wire).
#[derive(Debug, Clone)]
pub struct Keepalive {
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
}

impl Default for Keepalive {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(10),
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
    pub keepalive: Keepalive,
    pub trust_proxy: TrustProxy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid config: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Validate on construction (ENGINEERING.md §5): fail loudly at startup
    /// rather than misbehave at runtime.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.limits.max_payload_bytes == 0 {
            return Err(ConfigError("limits.maxPayloadBytes must be > 0".into()));
        }
        if self.backpressure.high_water_mark == 0 {
            return Err(ConfigError("backpressure.highWaterMark must be > 0".into()));
        }
        if self.keepalive.ping_interval < Duration::from_millis(1) {
            return Err(ConfigError("keepalive.pingIntervalMs must be >= 1".into()));
        }
        if self.keepalive.pong_timeout < Duration::from_millis(1) {
            return Err(ConfigError("keepalive.pongTimeoutMs must be >= 1".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn invalid_fields_rejected() {
        let mut c = Config::default();
        c.limits.max_payload_bytes = 0;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.backpressure.high_water_mark = 0;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.keepalive.ping_interval = Duration::ZERO;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.keepalive.pong_timeout = Duration::ZERO;
        assert!(c.validate().is_err());
    }
}
