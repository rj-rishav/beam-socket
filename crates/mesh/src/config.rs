//! Per-link configuration and its defaults (RFC 0004 §4.4/§4.6/§4.7, §5).
//!
//! The SDK-facing config (`cluster: { listen, seeds, secret, nodeId, … }`) is a
//! 3D concern; this is the *link layer's* view of it — the subset a single
//! peer link needs to negotiate, authenticate, and bound itself. Defaults come
//! from the RFC and the spike (`0004-results.md`); every numeric default cites
//! where it comes from.

use std::time::Duration;

use crate::frame::{MAX_FRAME_CEILING, MIN_FRAME_FLOOR};
use crate::hello::MAX_CLUSTER_NAME_LEN;

/// Everything a link needs about *this* node and how it may talk to a peer.
///
/// Rule 4 (per-peer memory cost): the only unbounded-by-config field is the
/// data queue, capped by `queue_hwm_bytes`. Total per-peer worst case is
/// `queue_hwm_bytes` + a fixed handshake/lifecycle overhead; times N ≤ 50 peers
/// is the mesh's memory envelope (stated in the PR notes and §8 of the RFC).
///
/// `Debug` is hand-written to **redact the secret** — a cluster secret in a log
/// line is exactly the kind of leak §4.7 is trying to avoid.
#[derive(Clone)]
pub struct LinkConfig {
    /// This node's wire protocol version. Real deployments pin
    /// [`crate::PROTOCOL_VERSION`]; tests vary it to drive the interop matrix.
    pub protocol_version: u16,
    /// Operator-assigned node id, unique in the mesh (§4.5). A peer presenting
    /// *our* id is a config collision and is refused at HELLO.
    pub node_id: u16,
    /// This node's incarnation (SWIM currency, 3B). Carried in HELLO and the
    /// transcript now; interpreted later.
    pub incarnation: u64,
    /// The cluster label. A peer with a different name is refused at HELLO,
    /// before auth — the accidental staging↔prod barrier (§4.4).
    pub cluster_name: String,
    /// The shared cluster secret (§4.7). Never crosses the wire; only its HMAC
    /// over the challenge+transcript does. Empty is a config error.
    pub secret: Vec<u8>,
    /// The largest frame this node will accept. Default 16 MiB (§4.4, "must
    /// exceed maxPayloadBytes + envelope"); the link speaks `min(local, peer)`.
    pub max_frame: u32,
    /// Feature bits this node supports (see [`crate::handshake`] for the
    /// catalog). The link uses the **intersection** with the peer.
    pub features: u32,
    /// The handshake must reach AUTH within this, or the link is closed and
    /// counted (`authTimeouts`). Guards a peer that connects and then stalls.
    pub auth_timeout: Duration,
    /// On an otherwise-idle link, send a PING this often (§4.4 liveness,
    /// distinct from SWIM UDP probes).
    pub idle_ping_interval: Duration,
    /// If no frame arrives for this long, the link is dead — closed and counted.
    /// Must exceed `idle_ping_interval` so a healthy peer's PONG resets it.
    pub idle_dead_after: Duration,
    /// Per-peer **data** queue high-water mark, in **bytes** (§4.6: byte-capped,
    /// not frame-capped — a 64 KiB frame must not count as one 64 B frame).
    /// Default 1 MiB — the spike's `LINK_HWM_BYTES` (`0004-results.md`).
    pub queue_hwm_bytes: usize,
    /// Reconnect backoff (used by 3B's reconnect loop; the link layer only
    /// exposes the schedule — [`Backoff`]).
    pub backoff: Backoff,
}

impl std::fmt::Debug for LinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkConfig")
            .field("protocol_version", &self.protocol_version)
            .field("node_id", &self.node_id)
            .field("incarnation", &self.incarnation)
            .field("cluster_name", &self.cluster_name)
            .field(
                "secret",
                &format_args!("<{} bytes redacted>", self.secret.len()),
            )
            .field("max_frame", &self.max_frame)
            .field("features", &format_args!("{:#010x}", self.features))
            .field("auth_timeout", &self.auth_timeout)
            .field("idle_ping_interval", &self.idle_ping_interval)
            .field("idle_dead_after", &self.idle_dead_after)
            .field("queue_hwm_bytes", &self.queue_hwm_bytes)
            .field("backoff", &self.backoff)
            .finish()
    }
}

impl LinkConfig {
    /// The default 16 MiB max frame (§4.4).
    pub const DEFAULT_MAX_FRAME: u32 = 16 << 20;
    /// The spike's 1 MiB per-link data HWM (`0004-results.md`, §4.6).
    pub const DEFAULT_QUEUE_HWM_BYTES: usize = 1 << 20;

    /// A config with RFC/spike defaults for everything but identity + secret.
    pub fn new(node_id: u16, cluster_name: impl Into<String>, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            protocol_version: crate::PROTOCOL_VERSION,
            node_id,
            incarnation: 0,
            cluster_name: cluster_name.into(),
            secret: secret.into(),
            max_frame: Self::DEFAULT_MAX_FRAME,
            features: 0,
            auth_timeout: Duration::from_secs(5),
            idle_ping_interval: Duration::from_secs(15),
            idle_dead_after: Duration::from_secs(45),
            queue_hwm_bytes: Self::DEFAULT_QUEUE_HWM_BYTES,
            backoff: Backoff::default(),
        }
    }

    /// Validate the static shape of the config at startup, so a misconfigured
    /// mesh fails loudly before a link is ever attempted (the 1C `Config`
    /// precedent — parse errors surface at construction, not at runtime).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.secret.is_empty() {
            return Err(ConfigError(
                "cluster secret must not be empty (§4.7)".into(),
            ));
        }
        if self.cluster_name.len() > MAX_CLUSTER_NAME_LEN {
            return Err(ConfigError(format!(
                "cluster name too long: {} > {MAX_CLUSTER_NAME_LEN}",
                self.cluster_name.len()
            )));
        }
        if self.max_frame < MIN_FRAME_FLOOR || self.max_frame > MAX_FRAME_CEILING {
            return Err(ConfigError(format!(
                "max_frame {} out of range [{MIN_FRAME_FLOOR}, {MAX_FRAME_CEILING}]",
                self.max_frame
            )));
        }
        if self.idle_dead_after <= self.idle_ping_interval {
            return Err(ConfigError(
                "idle_dead_after must exceed idle_ping_interval so a PONG can reset it".into(),
            ));
        }
        Ok(())
    }
}

/// Reconnect-with-backoff schedule (§4.7: "repeated failures get backoff, not
/// retry storms"). The link layer computes the schedule; the *reconnect loop*
/// that consumes it is 3B — [`Backoff::next_delay`] is the seam.
#[derive(Debug, Clone)]
pub struct Backoff {
    pub base: Duration,
    pub max: Duration,
    /// Multiplier per attempt (exponential). 2.0 doubles each time.
    pub factor: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(500),
            max: Duration::from_secs(30),
            factor: 2.0,
        }
    }
}

impl Backoff {
    /// Delay before attempt `n` (0-based): `min(max, base * factor^n)`. Pure and
    /// deterministic — jitter is the caller's to add (3B). Kept here so the
    /// schedule is unit-testable now and the reconnect loop is a thin consumer.
    pub fn next_delay(&self, attempt: u32) -> Duration {
        let scaled = self.base.as_secs_f64() * self.factor.powi(attempt as i32);
        let capped = scaled.min(self.max.as_secs_f64());
        Duration::from_secs_f64(capped)
    }
}

/// A static configuration error, surfaced at startup (never mid-link).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mesh config error: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        LinkConfig::new(1, "prod", b"s3cret".to_vec())
            .validate()
            .unwrap();
    }

    #[test]
    fn empty_secret_is_rejected() {
        let c = LinkConfig::new(1, "prod", Vec::new());
        assert!(c.validate().is_err());
    }

    #[test]
    fn dead_after_must_exceed_ping() {
        let mut c = LinkConfig::new(1, "prod", b"x".to_vec());
        c.idle_dead_after = c.idle_ping_interval;
        assert!(c.validate().is_err());
    }

    #[test]
    fn tiny_or_huge_max_frame_rejected() {
        let mut c = LinkConfig::new(1, "prod", b"x".to_vec());
        c.max_frame = 8; // below the floor
        assert!(c.validate().is_err());
        c.max_frame = MAX_FRAME_CEILING + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn backoff_grows_then_caps() {
        let b = Backoff {
            base: Duration::from_millis(500),
            max: Duration::from_secs(4),
            factor: 2.0,
        };
        assert_eq!(b.next_delay(0), Duration::from_millis(500));
        assert_eq!(b.next_delay(1), Duration::from_secs(1));
        assert_eq!(b.next_delay(2), Duration::from_secs(2));
        assert_eq!(b.next_delay(3), Duration::from_secs(4));
        assert_eq!(b.next_delay(10), Duration::from_secs(4), "capped at max");
    }
}
