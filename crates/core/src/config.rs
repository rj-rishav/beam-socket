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

/// The `authorize` hook (Phase 1C): a JS round-trip run once per connection at
/// upgrade time. These knobs are Rust-side safety rails, NOT the hook itself —
/// the hook lives in JS. Both are Rule 5 concerns: the pending-upgrade table is
/// a bounded queue, and an authorize promise that never settles must not leak.
#[derive(Debug, Clone)]
pub struct Authorize {
    /// How long to wait for the JS `authorize` promise to settle before the
    /// connection is rejected-and-cleaned (never left hanging). Default ~10 s.
    pub timeout: Duration,
    /// Upper bound on concurrently-pending authorizations (Rule 5). Overflow →
    /// reject at the door; unauthenticated handshakes are a DoS surface, so the
    /// pending table cannot grow without bound. 0 is rejected by `validate`.
    pub max_pending: usize,
}

impl Default for Authorize {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            max_pending: 8192,
        }
    }
}

/// Observability (Phase 2A). The only new runtime element is a 1 Hz sampler
/// task that derives rates from the EXISTING counters — never per-message work
/// (§12 rule 1: diagnostics are free when unused).
#[derive(Debug, Clone)]
pub struct Observability {
    /// Sampler tick interval, ms. Default 1000 (1 Hz). **0 disables the sampler
    /// entirely** — no task, and `stats().rates` reports absent.
    pub sampler_ms: u64,
}

impl Default for Observability {
    fn default() -> Self {
        Self { sampler_ms: 1000 }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub limits: Limits,
    pub backpressure: Backpressure,
    pub keepalive: Keepalive,
    pub trust_proxy: TrustProxy,
    pub authorize: Authorize,
    pub observability: Observability,
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
        if self.authorize.timeout < Duration::from_millis(1) {
            return Err(ConfigError("authorize.timeoutMs must be >= 1".into()));
        }
        if self.authorize.max_pending == 0 {
            return Err(ConfigError(
                "authorize.maxPending must be > 0 (the pending-upgrade table is bounded)".into(),
            ));
        }
        // Fail loudly at startup on a malformed trustProxy CIDR rather than
        // silently treat every peer as untrusted at runtime (a security
        // boundary must not degrade quietly — ARCHITECTURE §4).
        crate::limits::ClientIpResolver::from_trust_proxy(&self.trust_proxy)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // These tests deliberately build a default config and mutate ONE field to
    // prove `validate()` rejects it — struct-update syntax would obscure that.
    #![allow(clippy::field_reassign_with_default)]

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

        let mut c = Config::default();
        c.authorize.timeout = Duration::ZERO;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.authorize.max_pending = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn valid_trust_proxy_cidrs_accepted() {
        let mut c = Config::default();
        c.trust_proxy = TrustProxy::Cidrs(vec![
            "10.0.0.0/8".into(),
            "172.16.0.0/12".into(),
            "::1/128".into(),
            "fd00::/8".into(),
        ]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn malformed_trust_proxy_cidr_rejected() {
        let mut c = Config::default();
        c.trust_proxy = TrustProxy::Cidrs(vec!["not-a-cidr".into()]);
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.trust_proxy = TrustProxy::Cidrs(vec!["10.0.0.0/33".into()]); // impossible prefix
        assert!(c.validate().is_err());
    }
}
