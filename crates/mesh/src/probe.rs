//! The UDP **probe plane** (RFC 0004 §4.2 detection + §4.4 freeze).
//!
//! This plane does one thing: decide who is un-ack'd. It carries PING / ACK /
//! PING-REQ **only** — **no member state** ever rides UDP (§4.4 review hit 3:
//! UDP packets carry no negotiated context, so membership *dissemination* is
//! TCP-only). The probe cycle here feeds Suspect/Dead decisions into the shared
//! [`Membership`] table; [`crate::membership_sync`] spreads those decisions over
//! TCP.
//!
//! **The packet format is frozen** (§4.4): version-stamped, append-only, and
//! byte-for-byte stable — the `golden_*` tests fail if any byte moves. Every
//! packet is authenticated by an HMAC over its body (a forged packet is a
//! remote kick vector, §4.7), and a per-sender `(incarnation, seq)` high-water
//! mark drops replays.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::UdpSocket;

use crate::crypto::{constant_time_eq, hmac_sha256};
use crate::swim::{Membership, SwimParams};

/// Magic opening every probe packet — distinct from the TCP link's `BSMH` so a
/// stray packet on the wrong socket is rejected immediately.
pub const PROBE_MAGIC: [u8; 4] = *b"BSMP";
/// The frozen probe-format version. Bumped only if the format must change
/// incompatibly — and §4.4 says such a change moves probing onto TCP instead,
/// so in practice this never bumps.
pub const PROBE_VERSION: u8 = 1;

const KIND_PING: u8 = 1;
const KIND_ACK: u8 = 2;
const KIND_PING_REQ: u8 = 3;

/// HMAC tag length (SHA-256).
const TAG_LEN: usize = 32;
/// magic(4) + version(1) + kind(1) + from(2) + seq(8) + inc(4).
const HEADER_LEN: usize = 20;

/// A probe packet. Note what is **absent**: there is no member list, no
/// `Update`, no state payload — the type itself is the §4.4 guarantee that
/// membership never rides UDP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbePacket {
    /// Liveness probe. `reply_to = Some` means this is an indirect probe relayed
    /// on behalf of that origin — the ACK goes straight back there.
    Ping {
        from: u16,
        seq: u64,
        inc: u32,
        reply_to: Option<SocketAddr>,
    },
    /// Liveness reply, echoing the prober's `seq`.
    Ack { from: u16, seq: u64, inc: u32 },
    /// "Please probe `target` for me and have it ACK me directly."
    PingReq {
        from: u16,
        seq: u64,
        inc: u32,
        from_addr: SocketAddr,
        target: u16,
        target_addr: SocketAddr,
    },
}

impl ProbePacket {
    fn kind(&self) -> u8 {
        match self {
            ProbePacket::Ping { .. } => KIND_PING,
            ProbePacket::Ack { .. } => KIND_ACK,
            ProbePacket::PingReq { .. } => KIND_PING_REQ,
        }
    }

    fn header(&self) -> (u16, u64, u32) {
        match *self {
            ProbePacket::Ping { from, seq, inc, .. }
            | ProbePacket::Ack { from, seq, inc }
            | ProbePacket::PingReq { from, seq, inc, .. } => (from, seq, inc),
        }
    }

    /// The sender's node id (`from`) — the replay guard's key.
    pub fn from(&self) -> u16 {
        self.header().0
    }

    /// `(inc, seq)` — the replay high-water key.
    pub fn stamp(&self) -> (u32, u64) {
        let (_, seq, inc) = self.header();
        (inc, seq)
    }

    /// Encode to the frozen wire form with a trailing HMAC over the body.
    pub fn encode(&self, secret: &[u8]) -> Vec<u8> {
        let (from, seq, inc) = self.header();
        let mut buf = Vec::with_capacity(HEADER_LEN + TAG_LEN + 40);
        buf.extend_from_slice(&PROBE_MAGIC);
        buf.push(PROBE_VERSION);
        buf.push(self.kind());
        buf.extend_from_slice(&from.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&inc.to_le_bytes());
        match self {
            ProbePacket::Ping { reply_to, .. } => {
                buf.push(reply_to.is_some() as u8);
                if let Some(a) = reply_to {
                    put_addr(&mut buf, *a);
                }
            }
            ProbePacket::Ack { .. } => {}
            ProbePacket::PingReq {
                from_addr,
                target,
                target_addr,
                ..
            } => {
                put_addr(&mut buf, *from_addr);
                buf.extend_from_slice(&target.to_le_bytes());
                put_addr(&mut buf, *target_addr);
            }
        }
        let mac = hmac_sha256(secret, &buf);
        buf.extend_from_slice(&mac);
        buf
    }

    /// Decode + authenticate. `Forged` on a bad MAC; `BadMagic`/`BadVersion` on a
    /// stray or future-incompatible packet; `Malformed` on a short/garbled body.
    /// Trailing bytes beyond the known fields are ignored (append-only).
    pub fn decode(buf: &[u8], secret: &[u8]) -> Result<ProbePacket, ProbeError> {
        if buf.len() < HEADER_LEN + TAG_LEN {
            return Err(ProbeError::Malformed);
        }
        if buf[0..4] != PROBE_MAGIC {
            return Err(ProbeError::BadMagic);
        }
        if buf[4] != PROBE_VERSION {
            return Err(ProbeError::BadVersion(buf[4]));
        }
        let (body, tag) = buf.split_at(buf.len() - TAG_LEN);
        let expect = hmac_sha256(secret, body);
        if !constant_time_eq(tag, &expect) {
            return Err(ProbeError::Forged);
        }

        let kind = body[5];
        let from = u16::from_le_bytes([body[6], body[7]]);
        let seq = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let inc = u32::from_le_bytes(body[16..20].try_into().unwrap());
        let mut off = HEADER_LEN;

        match kind {
            KIND_PING => {
                let has_reply = *body.get(off).ok_or(ProbeError::Malformed)?;
                off += 1;
                let reply_to = if has_reply != 0 {
                    Some(get_addr(body, &mut off).ok_or(ProbeError::Malformed)?)
                } else {
                    None
                };
                Ok(ProbePacket::Ping {
                    from,
                    seq,
                    inc,
                    reply_to,
                })
            }
            KIND_ACK => Ok(ProbePacket::Ack { from, seq, inc }),
            KIND_PING_REQ => {
                let from_addr = get_addr(body, &mut off).ok_or(ProbeError::Malformed)?;
                let target = u16::from_le_bytes(
                    body.get(off..off + 2)
                        .ok_or(ProbeError::Malformed)?
                        .try_into()
                        .unwrap(),
                );
                off += 2;
                let target_addr = get_addr(body, &mut off).ok_or(ProbeError::Malformed)?;
                Ok(ProbePacket::PingReq {
                    from,
                    seq,
                    inc,
                    from_addr,
                    target,
                    target_addr,
                })
            }
            other => Err(ProbeError::UnknownKind(other)),
        }
    }
}

/// Encode a `SocketAddr` as `[family u8][ip bytes][port u16 LE]` — the shared
/// address wire form used by both planes (probe reply-to/targets, and TCP
/// membership updates).
pub(crate) fn put_addr(buf: &mut Vec<u8>, a: SocketAddr) {
    match a.ip() {
        IpAddr::V4(ip) => {
            buf.push(4);
            buf.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            buf.push(6);
            buf.extend_from_slice(&ip.octets());
        }
    }
    buf.extend_from_slice(&a.port().to_le_bytes());
}

pub(crate) fn get_addr(buf: &[u8], off: &mut usize) -> Option<SocketAddr> {
    let fam = *buf.get(*off)?;
    *off += 1;
    let ip: IpAddr = match fam {
        4 => {
            let o: [u8; 4] = buf.get(*off..*off + 4)?.try_into().ok()?;
            *off += 4;
            IpAddr::from(o)
        }
        6 => {
            let o: [u8; 16] = buf.get(*off..*off + 16)?.try_into().ok()?;
            *off += 16;
            IpAddr::from(o)
        }
        _ => return None,
    };
    let port = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?);
    *off += 2;
    Some(SocketAddr::new(ip, port))
}

/// Probe-plane decode outcomes. Every non-`Ok` variant is dropped and counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeError {
    BadMagic,
    BadVersion(u8),
    /// HMAC did not verify — a forged (or corrupted) packet.
    Forged,
    Malformed,
    UnknownKind(u8),
}

/// Probe-plane counters (defense visibility, §4.7 + Rule 5).
#[derive(Debug, Default)]
pub struct ProbeCounters {
    pub pings_sent: AtomicU64,
    pub acks_sent: AtomicU64,
    pub indirect_sent: AtomicU64,
    /// Packets with a bad HMAC — forged or corrupted (§4.7).
    pub forged: AtomicU64,
    /// Packets dropped by the `(inc, seq)` replay high-water mark.
    pub replayed: AtomicU64,
    /// Packets that failed to decode (bad magic/version/body).
    pub malformed: AtomicU64,
}

impl ProbeCounters {
    fn inc(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }
}

/// The async UDP probe plane. Shares the [`Membership`] table with the TCP
/// dissemination plane; both lock it briefly.
pub struct ProbePlane {
    socket: Arc<UdpSocket>,
    membership: Arc<Mutex<Membership>>,
    params: SwimParams,
    secret: Vec<u8>,
    /// Socket-level partition injection (Rule: fault injection at the mesh
    /// layer, not iptables, so the heal gate runs in CI). Packets from a denied
    /// id are dropped as if lost.
    deny: Arc<Mutex<HashSet<u16>>>,
    seq: AtomicU64,
    /// seq → probed id; the recv loop clears it on ACK.
    pending: Mutex<HashMap<u64, u16>>,
    /// from → highest `(inc, seq)` accepted; drops replays.
    replay: Mutex<HashMap<u16, (u32, u64)>>,
    counters: Arc<ProbeCounters>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl ProbePlane {
    /// Start on a **pre-bound** socket. The node binds UDP first (so it can bind
    /// TCP to the same port for peers to reach both planes at one address), then
    /// hands the socket here.
    pub fn start(
        socket: Arc<UdpSocket>,
        membership: Arc<Mutex<Membership>>,
        params: SwimParams,
        secret: Vec<u8>,
        deny: Arc<Mutex<HashSet<u16>>>,
    ) -> Arc<Self> {
        let plane = Arc::new(Self {
            socket,
            membership,
            params,
            secret,
            deny,
            seq: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            replay: Mutex::new(HashMap::new()),
            counters: Arc::new(ProbeCounters::default()),
            tasks: Mutex::new(Vec::new()),
        });
        let h1 = tokio::spawn(plane.clone().recv_loop());
        let h2 = tokio::spawn(plane.clone().probe_loop());
        *plane.tasks.lock().unwrap() = vec![h1, h2];
        plane
    }

    pub fn counters(&self) -> Arc<ProbeCounters> {
        self.counters.clone()
    }

    /// Abort the probe tasks — the node's "kill -9" (stop acking, stop probing).
    pub fn shutdown(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }

    fn self_view(&self) -> (u16, u32) {
        let m = self.membership.lock().unwrap();
        (m.self_id, m.self_incarnation())
    }

    async fn send(&self, to: SocketAddr, pkt: &ProbePacket) {
        let buf = pkt.encode(&self.secret);
        let _ = self.socket.send_to(&buf, to).await;
    }

    async fn recv_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 2048];
        loop {
            let Ok((n, src)) = self.socket.recv_from(&mut buf).await else {
                continue;
            };
            let pkt = match ProbePacket::decode(&buf[..n], &self.secret) {
                Ok(p) => p,
                Err(ProbeError::Forged) => {
                    ProbeCounters::inc(&self.counters.forged);
                    continue;
                }
                Err(_) => {
                    ProbeCounters::inc(&self.counters.malformed);
                    continue;
                }
            };

            // Partition injection: drop as if lost.
            if self.deny.lock().unwrap().contains(&pkt.from()) {
                continue;
            }

            // Replay guard on originated probes (PING / PING-REQ). ACKs are
            // matched by the pending map, so they need no seq guard.
            if matches!(pkt, ProbePacket::Ping { .. } | ProbePacket::PingReq { .. })
                && !self.accept_stamp(pkt.from(), pkt.stamp())
            {
                ProbeCounters::inc(&self.counters.replayed);
                continue;
            }

            self.handle(pkt, src).await;
        }
    }

    /// Advance the per-sender replay high-water. Returns false (drop) if the
    /// stamp is not strictly newer than the last accepted one.
    fn accept_stamp(&self, from: u16, stamp: (u32, u64)) -> bool {
        let mut r = self.replay.lock().unwrap();
        match r.get(&from) {
            Some(&last) if stamp <= last => false,
            _ => {
                r.insert(from, stamp);
                true
            }
        }
    }

    async fn handle(self: &Arc<Self>, pkt: ProbePacket, src: SocketAddr) {
        let rt = self.params.retransmit;
        match pkt {
            ProbePacket::Ping {
                from,
                seq,
                reply_to,
                ..
            } => {
                // Evidence of life: clear a local suspicion of the sender.
                {
                    let mut m = self.membership.lock().unwrap();
                    m.note_direct_ack(from, rt);
                }
                let (self_id, self_inc) = self.self_view();
                let ack = ProbePacket::Ack {
                    from: self_id,
                    seq,
                    inc: self_inc,
                };
                let dest = reply_to.unwrap_or(src);
                self.send(dest, &ack).await;
                ProbeCounters::inc(&self.counters.acks_sent);
            }
            ProbePacket::Ack { from, seq, .. } => {
                let matched = self.pending.lock().unwrap().remove(&seq) == Some(from);
                if matched {
                    let mut m = self.membership.lock().unwrap();
                    m.note_direct_ack(from, rt);
                }
            }
            ProbePacket::PingReq {
                seq,
                from_addr,
                target_addr,
                ..
            } => {
                // Relay a probe to the target; its ACK goes straight to the
                // origin (classic SWIM indirect shortcut).
                let (self_id, self_inc) = self.self_view();
                let ping = ProbePacket::Ping {
                    from: self_id,
                    seq,
                    inc: self_inc,
                    reply_to: Some(from_addr),
                };
                self.send(target_addr, &ping).await;
                let _ = target_addr;
            }
        }
    }

    /// The SWIM probe cycle: suspicion GC, one direct probe, escalate to
    /// indirect, then Suspect. Runs until the plane is dropped.
    async fn probe_loop(self: Arc<Self>) {
        let p = self.params;
        loop {
            tokio::time::sleep(p.period).await;

            // Suspicion timeout → Dead (eviction), queued for TCP dissemination.
            {
                let mut m = self.membership.lock().unwrap();
                m.expire_suspects(p.suspicion_timeout, p.retransmit);
            }

            let Some((target, taddr)) = ({
                let mut m = self.membership.lock().unwrap();
                m.next_probe_target()
            }) else {
                continue;
            };

            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            self.pending.lock().unwrap().insert(seq, target);
            let (self_id, self_inc) = self.self_view();
            let ping = ProbePacket::Ping {
                from: self_id,
                seq,
                inc: self_inc,
                reply_to: None,
            };
            self.send(taddr, &ping).await;
            ProbeCounters::inc(&self.counters.pings_sent);
            tokio::time::sleep(p.probe_timeout).await;

            if !self.pending.lock().unwrap().contains_key(&seq) {
                continue; // acked
            }

            // Indirect probes via k random Alive helpers.
            let helpers = {
                let mut m = self.membership.lock().unwrap();
                m.indirect_helpers(target, p.indirect_k)
            };
            let (self_id, self_inc, self_addr) = {
                let m = self.membership.lock().unwrap();
                (m.self_id, m.self_incarnation(), m.self_addr)
            };
            for (_, haddr) in helpers {
                let req = ProbePacket::PingReq {
                    from: self_id,
                    seq,
                    inc: self_inc,
                    from_addr: self_addr,
                    target,
                    target_addr: taddr,
                };
                self.send(haddr, &req).await;
                ProbeCounters::inc(&self.counters.indirect_sent);
            }
            tokio::time::sleep(p.probe_timeout).await;

            if self.pending.lock().unwrap().remove(&seq).is_some() {
                // No direct or indirect ACK → Suspect (queued for dissemination).
                let mut m = self.membership.lock().unwrap();
                m.suspect(target, p.retransmit);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    const SECRET: &[u8] = b"cluster-secret";

    #[test]
    fn round_trip_all_kinds() {
        let pkts = [
            ProbePacket::Ping {
                from: 3,
                seq: 42,
                inc: 7,
                reply_to: None,
            },
            ProbePacket::Ping {
                from: 3,
                seq: 42,
                inc: 7,
                reply_to: Some(a(9001)),
            },
            ProbePacket::Ack {
                from: 5,
                seq: 42,
                inc: 9,
            },
            ProbePacket::PingReq {
                from: 1,
                seq: 99,
                inc: 2,
                from_addr: a(7001),
                target: 4,
                target_addr: a(7004),
            },
        ];
        for p in pkts {
            let bytes = p.encode(SECRET);
            assert_eq!(ProbePacket::decode(&bytes, SECRET).unwrap(), p);
        }
    }

    #[test]
    fn golden_ping_bytes_are_frozen() {
        // A canonical no-reply PING. The structural bytes are hardcoded; the
        // last 32 bytes must equal the HMAC over the body. If any offset moves,
        // this fails (§4.4 frozen format).
        let p = ProbePacket::Ping {
            from: 0x0102,
            seq: 0x0807_0605_0403_0201,
            inc: 0x0C0B_0A09,
            reply_to: None,
        };
        let bytes = p.encode(SECRET);
        let body: &[u8] = &[
            b'B', b'S', b'M', b'P', // magic
            0x01, // version
            0x01, // kind = PING
            0x02, 0x01, // from = 0x0102 LE
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // seq LE
            0x09, 0x0A, 0x0B, 0x0C, // inc LE
            0x00, // has_reply = 0
        ];
        assert_eq!(&bytes[..body.len()], body, "probe header/body layout moved");
        assert_eq!(
            bytes.len(),
            body.len() + TAG_LEN,
            "unexpected trailing bytes"
        );
        assert_eq!(
            &bytes[body.len()..],
            &crate::crypto::hmac_sha256(SECRET, body),
            "trailing tag must be HMAC over the exact body"
        );
    }

    #[test]
    fn forged_hmac_is_rejected() {
        let p = ProbePacket::Ack {
            from: 5,
            seq: 1,
            inc: 1,
        };
        let mut bytes = p.encode(SECRET);
        *bytes.last_mut().unwrap() ^= 0xFF; // corrupt the tag
        assert_eq!(ProbePacket::decode(&bytes, SECRET), Err(ProbeError::Forged));
        // Wrong secret is also a forgery from our side.
        let good = p.encode(SECRET);
        assert_eq!(
            ProbePacket::decode(&good, b"other-secret"),
            Err(ProbeError::Forged)
        );
    }

    #[test]
    fn tampered_body_breaks_the_mac() {
        let p = ProbePacket::Ping {
            from: 1,
            seq: 1,
            inc: 1,
            reply_to: None,
        };
        let mut bytes = p.encode(SECRET);
        bytes[6] ^= 0x01; // flip a bit in `from`
        assert_eq!(ProbePacket::decode(&bytes, SECRET), Err(ProbeError::Forged));
    }

    #[test]
    fn probe_packet_size_is_independent_of_membership() {
        // §4.4 assertion: a probe carries NO member state, so its size is fixed
        // regardless of how many members the sender knows. A no-reply PING is
        // always header(20) + has_reply flag(1) + tag(32) = 53 bytes.
        let p = ProbePacket::Ping {
            from: 1,
            seq: 1,
            inc: 1,
            reply_to: None,
        };
        assert_eq!(p.encode(SECRET).len(), HEADER_LEN + 1 + TAG_LEN);
    }

    #[test]
    fn bad_magic_and_version_rejected() {
        let p = ProbePacket::Ack {
            from: 1,
            seq: 1,
            inc: 1,
        };
        let mut bytes = p.encode(SECRET);
        let saved = bytes.clone();
        bytes[0] = b'X';
        assert_eq!(
            ProbePacket::decode(&bytes, SECRET),
            Err(ProbeError::BadMagic)
        );
        let mut bytes = saved;
        bytes[4] = 2;
        // Version is checked before the MAC (a future-version packet is refused
        // outright, not silently mis-parsed).
        assert_eq!(
            ProbePacket::decode(&bytes, SECRET),
            Err(ProbeError::BadVersion(2))
        );
    }
}
