//! Admission control — Phase 1C. Enforced BEFORE any JS runs.
//!
//! - `max_connections_per_ip`: enforced at the HTTP upgrade with a 429, before
//!   a WebSocket exists — cheaper than closing after the handshake, and the
//!   right layer to shed an unauthenticated flood.
//! - `max_payload_bytes`: wired to the codec's message/frame cap in
//!   transport/websocket.rs (close 1009). Documented here, enforced there.
//! - `max_rooms_per_connection`: enforced in `rooms::join` (rooms.rs) — this
//!   replaces the Phase 1B "exists but unenforced" state.
//! - `trust_proxy`: `Never | Always | Cidrs` — the client-IP resolver. With
//!   `Cidrs`, X-Forwarded-For is honored ONLY when the socket peer is inside
//!   the list, and parsed RIGHT-TO-LEFT (leftmost-first is spoofable). This is
//!   a security boundary; spoofed XFF is a tested case (ENGINEERING.md §7).
//!
//! Rule 3: every limit here is tested in direct AND simulated-proxy topologies.

use std::net::IpAddr;
use std::sync::Arc;

use dashmap::DashMap;
use ipnet::IpNet;

use crate::config::{ConfigError, TrustProxy};
use crate::metrics::Metrics;

/// HTTP status returned to a rejected upgrade when `maxConnectionsPerIp` is hit.
/// The reject happens during the handshake, before the WebSocket is
/// established — so it is an HTTP status, not a WebSocket close code.
pub const HTTP_TOO_MANY_REQUESTS: u16 = 429;

/// Resolves the client IP from the socket peer address and the (untrusted-by-
/// default) `X-Forwarded-For` header, per the `trustProxy` policy.
///
/// This is the single choke point for "who is this client, for the purposes of
/// per-IP limits and `AuthorizeRequest.ip`?" — get it wrong and every per-IP
/// safety feature either misfires behind a load balancer or is trivially
/// spoofable (Rule 3 + ARCHITECTURE §4).
#[derive(Debug, Clone)]
pub enum ClientIpResolver {
    /// `trustProxy: false` — the socket peer address, always. XFF ignored.
    PeerOnly,
    /// `trustProxy: true` — trust ANY peer's XFF; take the rightmost hop (the
    /// address the immediate upstream saw). Only safe when the runtime is
    /// unreachable except through the proxy (docs warn loudly — ARCHITECTURE §4).
    RightmostForwarded,
    /// `trustProxy: [CIDR…]` — honor XFF only when the peer is a trusted proxy,
    /// walking right-to-left and skipping trusted hops.
    TrustedProxies(Vec<IpNet>),
}

impl ClientIpResolver {
    /// Build + validate the resolver from config. Parsing failures are surfaced
    /// as `ConfigError` so a malformed CIDR fails at startup, not silently at
    /// runtime (called from `Config::validate`).
    pub fn from_trust_proxy(tp: &TrustProxy) -> Result<Self, ConfigError> {
        match tp {
            TrustProxy::Never => Ok(Self::PeerOnly),
            TrustProxy::Always => Ok(Self::RightmostForwarded),
            TrustProxy::Cidrs(cidrs) => {
                let mut nets = Vec::with_capacity(cidrs.len());
                for c in cidrs {
                    let net: IpNet = c
                        .trim()
                        .parse()
                        .map_err(|e| ConfigError(format!("invalid trustProxy CIDR {c:?}: {e}")))?;
                    nets.push(net);
                }
                Ok(Self::TrustedProxies(nets))
            }
        }
    }

    /// Resolve the client IP. `peer` is the socket's actual peer address;
    /// `forwarded_for` is the raw `X-Forwarded-For` header value if present.
    pub fn resolve(&self, peer: IpAddr, forwarded_for: Option<&str>) -> IpAddr {
        let peer = canonical(peer);
        match self {
            Self::PeerOnly => peer,
            Self::RightmostForwarded => forwarded_for.and_then(rightmost_hop).unwrap_or(peer),
            Self::TrustedProxies(nets) => {
                // Only a trusted peer may speak for a client via XFF; otherwise
                // the peer IS the client and its XFF is ignored (anti-spoof).
                if !trusted(nets, &peer) {
                    return peer;
                }
                match forwarded_for {
                    Some(xff) => resolve_via_trusted_chain(nets, peer, xff),
                    None => peer,
                }
            }
        }
    }
}

/// Normalize IPv4-mapped IPv6 (`::ffff:a.b.c.d`) down to plain IPv4 so a
/// dual-stack listener and a CIDR list agree on identity. No-op for others.
#[inline]
fn canonical(ip: IpAddr) -> IpAddr {
    ip.to_canonical()
}

fn trusted(nets: &[IpNet], ip: &IpAddr) -> bool {
    nets.iter().any(|n| n.contains(ip))
}

/// Parse one XFF hop as a bare IP. XFF entries are bare addresses; anything
/// else (a port, garbage) fails to parse and is treated as a chain break.
fn parse_hop(s: &str) -> Option<IpAddr> {
    s.trim().parse::<IpAddr>().ok().map(canonical)
}

/// `trustProxy: true` — the rightmost parseable hop.
fn rightmost_hop(xff: &str) -> Option<IpAddr> {
    xff.rsplit(',').find_map(parse_hop)
}

/// `trustProxy: [CIDR…]` with a trusted peer. Walk the header right-to-left:
/// the rightmost entry was appended by the hop that connected to us (trusted,
/// since the peer is trusted), so skip trusted hops; the FIRST untrusted
/// address is the real client. Leftmost-first would let the client spoof its
/// own address — we never do that.
fn resolve_via_trusted_chain(nets: &[IpNet], peer: IpAddr, xff: &str) -> IpAddr {
    let hops: Vec<&str> = xff.split(',').collect();
    let mut leftmost_trusted: Option<IpAddr> = None;
    for (i, part) in hops.iter().enumerate().rev() {
        match parse_hop(part) {
            Some(ip) => {
                if !trusted(nets, &ip) {
                    return ip; // first untrusted from the right = client
                }
                if i == 0 {
                    leftmost_trusted = Some(ip);
                }
            }
            // A malformed hop breaks the chain of trust: we can't attribute
            // anything to its left. Fall back to the peer — an unspoofable
            // value — rather than trust a forged/garbled entry.
            None => return peer,
        }
    }
    // Every hop was a trusted proxy (or the header was empty): the leftmost
    // recorded address is the earliest known origin. Fall back to peer if the
    // header yielded nothing.
    leftmost_trusted.unwrap_or(peer)
}

/// Per-IP connection admission (`maxConnectionsPerIp`). Sharded via `DashMap`
/// (Rule 2 — no global lock; admission runs on every accept). A live count is
/// held per IP only while ≥1 connection from it exists, so idle IPs cost
/// nothing.
pub struct IpLimiter {
    max_per_ip: u32, // 0 = unlimited
    counts: DashMap<IpAddr, u32>,
}

impl IpLimiter {
    pub fn new(max_per_ip: u32) -> Self {
        Self {
            max_per_ip,
            counts: DashMap::new(),
        }
    }

    #[inline]
    pub fn unlimited(&self) -> bool {
        self.max_per_ip == 0
    }

    /// Reserve one slot for `ip`. `Some(guard)` on success — the guard releases
    /// the slot on drop, so every admitted connection is released on ANY
    /// teardown path (handshake failure, authorize reject, normal disconnect)
    /// without a bespoke cleanup call. `None` = `ip` is at its cap.
    pub fn try_admit(self: &Arc<Self>, ip: IpAddr) -> Option<IpAdmitGuard> {
        if self.max_per_ip == 0 {
            return Some(IpAdmitGuard { limiter: None, ip });
        }
        let mut entry = self.counts.entry(ip).or_insert(0);
        if *entry >= self.max_per_ip {
            return None;
        }
        *entry += 1;
        drop(entry);
        Some(IpAdmitGuard {
            limiter: Some(self.clone()),
            ip,
        })
    }

    fn release(&self, ip: IpAddr) {
        if let Some(mut entry) = self.counts.get_mut(&ip) {
            *entry -= 1;
            if *entry == 0 {
                drop(entry);
                // Re-check emptiness under the map lock so a concurrent admit
                // between the drop and here is not lost (same idiom as rooms.rs).
                self.counts.remove_if(&ip, |_, v| *v == 0);
            }
        }
    }

    /// Current live connection count for `ip` (diagnostics/tests).
    pub fn current(&self, ip: &IpAddr) -> u32 {
        self.counts.get(ip).map_or(0, |e| *e)
    }

    /// Number of distinct IPs currently holding ≥1 connection (tests).
    pub fn tracked_ips(&self) -> usize {
        self.counts.len()
    }
}

/// RAII release of one per-IP admission slot. Held for a connection's whole
/// lifetime; dropping it (disconnect, reject, panic) frees the slot.
pub struct IpAdmitGuard {
    limiter: Option<Arc<IpLimiter>>, // None = unlimited → no-op guard
    ip: IpAddr,
}

impl IpAdmitGuard {
    pub fn client_ip(&self) -> IpAddr {
        self.ip
    }
}

impl Drop for IpAdmitGuard {
    fn drop(&mut self) {
        if let Some(limiter) = &self.limiter {
            limiter.release(self.ip);
        }
    }
}

/// The admission gate, run INSIDE the WebSocket handshake callback (sync). It
/// resolves the client IP and enforces `maxConnectionsPerIp` before the upgrade
/// completes. Everything it needs is cheap and lock-local, so it is safe to run
/// on the handshake's critical path.
pub struct Gate {
    resolver: ClientIpResolver,
    limiter: Arc<IpLimiter>,
    metrics: Arc<Metrics>,
}

/// A successful admission: the resolved client IP, the captured request
/// metadata for `authorize`, and the RAII per-IP slot guard.
pub struct AdmittedUpgrade {
    pub client_ip: IpAddr,
    pub headers: Vec<(String, String)>,
    pub url: String,
    pub guard: IpAdmitGuard,
}

impl Gate {
    pub fn new(resolver: ClientIpResolver, limiter: Arc<IpLimiter>, metrics: Arc<Metrics>) -> Self {
        Self {
            resolver,
            limiter,
            metrics,
        }
    }

    /// Resolve the client IP for `peer` given the request headers (looks up
    /// `X-Forwarded-For`). Pure — no side effects; used by `admit` and tests.
    pub fn resolve_client_ip(&self, peer: IpAddr, headers: &[(String, String)]) -> IpAddr {
        let xff = header_value(headers, "x-forwarded-for");
        self.resolver.resolve(peer, xff.as_deref())
    }

    /// Distinct IPs currently holding ≥1 connection (leak-test diagnostic).
    pub fn tracked_ips(&self) -> usize {
        self.limiter.tracked_ips()
    }

    /// Enforce `maxConnectionsPerIp` at the upgrade. `Ok` → admitted (the guard
    /// holds the per-IP slot); `Err(status)` → reject the upgrade with this HTTP
    /// status, before any WebSocket exists.
    pub fn admit(
        &self,
        peer: IpAddr,
        headers: Vec<(String, String)>,
        url: String,
    ) -> Result<AdmittedUpgrade, u16> {
        let client_ip = self.resolve_client_ip(peer, &headers);
        match self.limiter.try_admit(client_ip) {
            Some(guard) => Ok(AdmittedUpgrade {
                client_ip,
                headers,
                url,
                guard,
            }),
            None => {
                Metrics::add(&self.metrics.admission_rejected_ip, 1);
                Err(HTTP_TOO_MANY_REQUESTS)
            }
        }
    }
}

/// Case-insensitive header lookup. Header names are captured lowercased at the
/// handshake (see transport/websocket.rs), so this is a plain match; kept
/// case-insensitive anyway so tests and direct callers need not pre-normalize.
pub fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn cidr_resolver(cidrs: &[&str]) -> ClientIpResolver {
        ClientIpResolver::from_trust_proxy(&TrustProxy::Cidrs(
            cidrs.iter().map(|s| s.to_string()).collect(),
        ))
        .unwrap()
    }

    // ---------- trustProxy: false (peer only) ----------

    #[test]
    fn peer_only_ignores_xff() {
        let r = ClientIpResolver::PeerOnly;
        assert_eq!(
            r.resolve(ip("203.0.113.9"), Some("1.2.3.4")),
            ip("203.0.113.9"),
            "an untrusted peer's XFF must never be honored"
        );
    }

    // ---------- trustProxy: CIDR list — the spoof-resistance core ----------

    #[test]
    fn untrusted_peer_xff_ignored_peer_used() {
        // Peer is NOT in the trusted list → its XFF is spoof and must be dropped.
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r.resolve(ip("203.0.113.9"), Some("1.2.3.4, 5.6.7.8")),
            ip("203.0.113.9")
        );
    }

    #[test]
    fn trusted_peer_single_proxy_returns_client() {
        // Peer is the trusted proxy; XFF holds one entry = the real client.
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r.resolve(ip("10.0.0.7"), Some("203.0.113.9")),
            ip("203.0.113.9")
        );
    }

    #[test]
    fn trusted_peer_mixed_chain_walks_right_to_left() {
        // client, then two trusted proxies. Peer is the last proxy. Walking
        // right-to-left skips the two trusted hops; the first untrusted is the
        // client. A leftmost-first reader would return the same here — so add
        // the adversarial case below.
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r.resolve(ip("10.0.0.1"), Some("203.0.113.9, 10.0.0.9, 10.0.0.5")),
            ip("203.0.113.9")
        );
    }

    #[test]
    fn spoofed_leftmost_is_not_trusted() {
        // Adversary sets XFF's LEFTMOST entry to a forged IP, then the real
        // client address is what the first trusted proxy appended. Right-to-left
        // stops at the first untrusted hop = the REAL client (198.51.100.5),
        // never the forged leftmost (1.1.1.1). This is the whole point.
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r.resolve(ip("10.0.0.1"), Some("1.1.1.1, 198.51.100.5, 10.0.0.9")),
            ip("198.51.100.5"),
            "must not trust the client-controlled leftmost value"
        );
    }

    #[test]
    fn all_hops_trusted_falls_back_to_leftmost() {
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r.resolve(ip("10.0.0.1"), Some("10.0.0.2, 10.0.0.3, 10.0.0.4")),
            ip("10.0.0.2")
        );
    }

    #[test]
    fn malformed_hop_falls_back_to_peer() {
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r.resolve(ip("10.0.0.1"), Some("garbage, 10.0.0.9")),
            ip("10.0.0.1"),
            "a malformed hop breaks the chain — use the unspoofable peer"
        );
    }

    #[test]
    fn trusted_peer_no_xff_uses_peer() {
        let r = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(r.resolve(ip("10.0.0.7"), None), ip("10.0.0.7"));
    }

    #[test]
    fn ipv6_cidrs_and_mapped_addresses() {
        let r = cidr_resolver(&["fd00::/8"]);
        // Trusted IPv6 proxy forwards an IPv6 client.
        assert_eq!(
            r.resolve(ip("fd00::1"), Some("2001:db8::42")),
            ip("2001:db8::42")
        );
        // IPv4-mapped peer is canonicalized before the CIDR check.
        let r4 = cidr_resolver(&["10.0.0.0/8"]);
        assert_eq!(
            r4.resolve(ip("::ffff:10.0.0.7"), Some("203.0.113.9")),
            ip("203.0.113.9")
        );
    }

    // ---------- trustProxy: true (rightmost) ----------

    #[test]
    fn always_takes_rightmost_hop() {
        let r = ClientIpResolver::RightmostForwarded;
        assert_eq!(
            r.resolve(ip("10.0.0.1"), Some("1.1.1.1, 2.2.2.2, 3.3.3.3")),
            ip("3.3.3.3")
        );
        assert_eq!(r.resolve(ip("10.0.0.1"), None), ip("10.0.0.1"));
    }

    // ---------- per-IP limiter (direct topology; proxy is exercised via the
    // resolver above + the engine test in tests/phase1c.rs, Rule 3) ----------

    #[test]
    fn limiter_admits_up_to_cap_then_rejects() {
        let l = Arc::new(IpLimiter::new(2));
        let a = ip("203.0.113.1");
        let g1 = l.try_admit(a).expect("1st admitted");
        let _g2 = l.try_admit(a).expect("2nd admitted");
        assert!(l.try_admit(a).is_none(), "3rd over cap must be rejected");
        assert_eq!(l.current(&a), 2);
        // A different IP has its own budget.
        assert!(l.try_admit(ip("203.0.113.2")).is_some());
        // Releasing one slot lets the next in.
        drop(g1);
        assert_eq!(l.current(&a), 1);
        assert!(l.try_admit(a).is_some());
    }

    #[test]
    fn limiter_zero_means_unlimited_and_tracks_nothing() {
        let l = Arc::new(IpLimiter::new(0));
        let a = ip("203.0.113.1");
        let _g: Vec<_> = (0..1000).map(|_| l.try_admit(a).unwrap()).collect();
        assert_eq!(l.tracked_ips(), 0, "unlimited limiter holds no state");
    }

    #[test]
    fn guard_release_frees_ip_entry() {
        let l = Arc::new(IpLimiter::new(4));
        let a = ip("203.0.113.1");
        {
            let _g = l.try_admit(a).unwrap();
            assert_eq!(l.tracked_ips(), 1);
        }
        assert_eq!(l.tracked_ips(), 0, "last release must drop the IP entry");
    }
}
