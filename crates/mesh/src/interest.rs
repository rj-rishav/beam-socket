//! Interest-advertised routing (RFC 0004 §4.3 + §4.4 INTEREST/INTEREST_DIGEST).
//!
//! Each node advertises which **rooms** and **users** it currently hosts (≥1
//! local member / device). Every node holds an interest table — `origin →
//! {rooms, users}` — and a targeted relay goes **only** to the peers hosting
//! that target. That is the whole efficiency win: a `toRoom(r)` that no remote
//! node cares about relays to no one (the spike measured ~22× fewer inter-node
//! bytes than flood, `0004-results.md`).
//!
//! Three disciplines make this safe (§4.3):
//! - **Edge-triggered:** only the 0→1 and 1→0 transitions of "do I host r?"
//!   publish an update — join #2..#n of the same room are invisible to the
//!   mesh, so interest chatter is independent of per-room membership churn.
//! - **Per-origin seq:** every edge carries the origin's monotonic seq; a
//!   receiver applies only strictly-newer seqs and drops stale/reordered/dup
//!   ones (counted). The edge stream is therefore *lossy by design*.
//! - **Anti-entropy digest:** a periodic hash of each origin's sorted set,
//!   compared lazily; a mismatch triggers a full snapshot resync. This is the
//!   self-healing net under the lossy edge stream (the [`crate::membership_sync`]
//!   digest pattern, reused).
//!
//! This module is **pure** (no IO, no sockets), so routing correctness is
//! model-checkable against a flood reference under proptest. The node
//! ([`crate::node`]) wires it to the links; 3D's engine consumes
//! [`InterestState::interested_peers`] to decide where a relay goes — **3C
//! decides where, 3D sends.**

use std::collections::{BTreeSet, HashMap, HashSet};

/// A cluster node id (the SWIM/link id).
pub type NodeId = u16;

/// A relay target: a room or a user. The routing API answers "which remote
/// peers host this?".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    Room(String),
    User(String),
}

/// Routing mode (§4.3 / §13.3). `Interest` is the default; `Flood` is the
/// operational fallback lever — relay to **all** live peers, ignore the interest
/// table. Flood exists for the case where interest state is ever suspected wrong
/// in production: correctness over efficiency, on demand. It is **never** the
/// default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Routing {
    #[default]
    Interest,
    Flood,
}

/// One origin's advertised interest — the rooms and users it hosts, at a given
/// seq. `BTreeSet` so the digest hash is deterministic across nodes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct OriginInterest {
    seq: u64,
    rooms: BTreeSet<String>,
    users: BTreeSet<String>,
}

impl OriginInterest {
    fn hosts(&self, target: &Target) -> bool {
        match target {
            Target::Room(r) => self.rooms.contains(r),
            Target::User(u) => self.users.contains(u),
        }
    }

    /// A content hash of the SET (not the seq): two nodes that applied the same
    /// edges get the same hash; one that dropped an edge does not — that is how
    /// the digest detects a lost edge even at equal seq.
    fn hash(&self) -> u64 {
        let mut buf = Vec::new();
        for r in &self.rooms {
            buf.push(1u8);
            buf.extend_from_slice(&(r.len() as u16).to_le_bytes());
            buf.extend_from_slice(r.as_bytes());
        }
        for u in &self.users {
            buf.push(2u8);
            buf.extend_from_slice(&(u.len() as u16).to_le_bytes());
            buf.extend_from_slice(u.as_bytes());
        }
        fnv1a64(&buf)
    }
}

/// Deterministic (non-randomized) hash — `std`'s `DefaultHasher` is seeded per
/// process, so it cannot be used for a value two nodes must agree on.
fn fnv1a64(data: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// An edge-triggered interest change (0→1 add, 1→0 remove) from an origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterestEdge {
    pub origin: NodeId,
    pub seq: u64,
    pub add: bool,
    pub target: Target,
}

/// A full snapshot of an origin's interest — the anti-entropy repair payload and
/// the link-up full exchange (same shape as the membership push-pull).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterestSnapshot {
    pub origin: NodeId,
    pub seq: u64,
    pub rooms: Vec<String>,
    pub users: Vec<String>,
}

/// One digest entry: an origin, the seq we last applied for it, and the content
/// hash of its set.
pub type DigestEntry = (NodeId, u64, u64);

/// Interest counters — folded into the node's stats surface for 3D (§13.3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InterestCounters {
    /// Interest updates (edges + snapshots) accepted from peers.
    pub interest_in: u64,
    /// Edges we generated locally (0→1 / 1→0 transitions).
    pub interest_out: u64,
    /// Updates dropped by the seq discipline (stale / duplicate / reordered).
    pub seq_drops: u64,
    /// Snapshot resyncs the digest applied (a repaired divergence).
    pub digest_repairs: u64,
}

/// The interest table + routing logic for one node. Pure; the node owns it
/// behind a lock and moves the encoded frames.
pub struct InterestState {
    self_id: NodeId,
    local: OriginInterest,
    remote: HashMap<NodeId, OriginInterest>,
    routing: Routing,
    counters: InterestCounters,
}

impl InterestState {
    pub fn new(self_id: NodeId, routing: Routing) -> Self {
        Self {
            self_id,
            local: OriginInterest::default(),
            remote: HashMap::new(),
            routing,
            counters: InterestCounters::default(),
        }
    }

    pub fn counters(&self) -> InterestCounters {
        self.counters
    }

    pub fn set_routing(&mut self, routing: Routing) {
        self.routing = routing;
    }

    /// Total interest entries held (rooms + users across all known origins) —
    /// the Rule 4 size gauge.
    pub fn table_size(&self) -> usize {
        self.local.rooms.len()
            + self.local.users.len()
            + self
                .remote
                .values()
                .map(|o| o.rooms.len() + o.users.len())
                .sum::<usize>()
    }

    // ── local interest (the seam 3D's engine drives; a test double in 3C) ──

    /// Note that this node now hosts (0→1) or no longer hosts (1→0) `target`.
    /// Returns an edge to disseminate **only** on a real transition — join
    /// #2..#n (already hosting) returns `None` (edge-triggered, §4.3).
    pub fn local_set(&mut self, target: Target, hosting: bool) -> Option<InterestEdge> {
        let changed = match &target {
            Target::Room(r) => {
                if hosting {
                    self.local.rooms.insert(r.clone())
                } else {
                    self.local.rooms.remove(r)
                }
            }
            Target::User(u) => {
                if hosting {
                    self.local.users.insert(u.clone())
                } else {
                    self.local.users.remove(u)
                }
            }
        };
        if !changed {
            return None;
        }
        self.local.seq += 1;
        self.counters.interest_out += 1;
        Some(InterestEdge {
            origin: self.self_id,
            seq: self.local.seq,
            add: hosting,
            target,
        })
    }

    // ── applying peer updates ──

    /// Apply an inbound edge. Drops (and counts) anything not strictly newer
    /// than the origin's last applied seq — stale, duplicate, or reordered. A
    /// true gap (seq jumps ahead) is applied and advances the seq; the digest
    /// repairs whatever the gap skipped.
    pub fn apply_edge(&mut self, edge: &InterestEdge) -> bool {
        if edge.origin == self.self_id {
            return false; // our own echo; we are authoritative locally
        }
        let o = self.remote.entry(edge.origin).or_default();
        if edge.seq <= o.seq {
            self.counters.seq_drops += 1;
            return false;
        }
        o.seq = edge.seq;
        let set = match &edge.target {
            Target::Room(_) => &mut o.rooms,
            Target::User(_) => &mut o.users,
        };
        let key = match &edge.target {
            Target::Room(r) => r.clone(),
            Target::User(u) => u.clone(),
        };
        if edge.add {
            set.insert(key);
        } else {
            set.remove(&key);
        }
        self.counters.interest_in += 1;
        true
    }

    /// Apply a full snapshot (anti-entropy repair / link-up exchange). Accepts
    /// when the snapshot's seq is ≥ ours for that origin (the origin re-sending
    /// its own set at equal seq is the divergence repair). Snapshots about
    /// ourselves are ignored — we are authoritative for our own interest.
    pub fn apply_snapshot(&mut self, snap: &InterestSnapshot) -> bool {
        if snap.origin == self.self_id {
            return false;
        }
        let cur = self.remote.get(&snap.origin);
        let accept = cur.map(|o| snap.seq >= o.seq).unwrap_or(true);
        if !accept {
            return false;
        }
        let replacement = OriginInterest {
            seq: snap.seq,
            rooms: snap.rooms.iter().cloned().collect(),
            users: snap.users.iter().cloned().collect(),
        };
        let changed = cur != Some(&replacement);
        self.remote.insert(snap.origin, replacement);
        if changed {
            self.counters.digest_repairs += 1;
        }
        self.counters.interest_in += 1;
        changed
    }

    /// Sweep an evicted peer's interest (the 3B lesson applied to interest
    /// state: a Dead node's interest never lingers as a stuck entry, §13.3).
    pub fn sweep_origin(&mut self, origin: NodeId) {
        self.remote.remove(&origin);
    }

    /// The remote origin ids we currently hold interest for (the node reconciles
    /// these against the SWIM alive set to sweep evicted peers).
    pub fn known_origins(&self) -> Vec<NodeId> {
        self.remote.keys().copied().collect()
    }

    /// A snapshot of our own interest — the link-up full-exchange payload.
    pub fn local_snapshot(&self) -> InterestSnapshot {
        self.snapshot_of_local()
    }

    // ── anti-entropy digest ──

    /// The digest of everything we know: `(origin, seq, set-hash)` for ourselves
    /// and every remote origin.
    pub fn build_digest(&self) -> Vec<DigestEntry> {
        let mut out = vec![(self.self_id, self.local.seq, self.local.hash())];
        for (id, o) in &self.remote {
            out.push((*id, o.seq, o.hash()));
        }
        out
    }

    /// Compare a peer's digest to ours and produce the snapshots to send back:
    /// for our own origin, any difference (we are authoritative — repair it);
    /// for a remote origin, only when we are strictly ahead (forward newer
    /// info). This is what makes a dropped edge self-heal within one cycle.
    pub fn respond_to_digest(&self, peer: &[DigestEntry]) -> Vec<InterestSnapshot> {
        let peer_map: HashMap<NodeId, (u64, u64)> =
            peer.iter().map(|(o, s, h)| (*o, (*s, *h))).collect();
        let mut out = Vec::new();

        // Our own origin.
        let mine = (self.local.seq, self.local.hash());
        let send_self = match peer_map.get(&self.self_id) {
            None => {
                !self.local.rooms.is_empty() || !self.local.users.is_empty() || self.local.seq > 0
            }
            Some(p) => *p != mine,
        };
        if send_self {
            out.push(self.snapshot_of_local());
        }

        // Remote origins: forward when strictly ahead.
        for (id, o) in &self.remote {
            let ahead = match peer_map.get(id) {
                None => o.seq > 0,
                Some((pseq, _)) => o.seq > *pseq,
            };
            if ahead {
                out.push(snapshot_of(*id, o));
            }
        }
        out
    }

    fn snapshot_of_local(&self) -> InterestSnapshot {
        InterestSnapshot {
            origin: self.self_id,
            seq: self.local.seq,
            rooms: self.local.rooms.iter().cloned().collect(),
            users: self.local.users.iter().cloned().collect(),
        }
    }

    // ── the routing decision (the seam 3D consumes) ──

    /// The remote peers to relay `target` to. `alive` is the set of currently
    /// reachable peers (the caller passes the SWIM alive set) — a partitioned or
    /// evicted peer is never a relay target (§13.3: no relay to unreachable). In
    /// `Flood` mode every alive peer is returned, interest ignored (the lever).
    pub fn interested_peers(&self, target: &Target, alive: &HashSet<NodeId>) -> Vec<NodeId> {
        match self.routing {
            Routing::Flood => {
                let mut v: Vec<NodeId> = alive
                    .iter()
                    .copied()
                    .filter(|id| *id != self.self_id)
                    .collect();
                v.sort_unstable();
                v
            }
            Routing::Interest => {
                let mut v: Vec<NodeId> = self
                    .remote
                    .iter()
                    .filter(|(id, o)| alive.contains(id) && o.hosts(target))
                    .map(|(id, _)| *id)
                    .collect();
                v.sort_unstable();
                v
            }
        }
    }
}

fn snapshot_of(origin: NodeId, o: &OriginInterest) -> InterestSnapshot {
    InterestSnapshot {
        origin,
        seq: o.seq,
        rooms: o.rooms.iter().cloned().collect(),
        users: o.users.iter().cloned().collect(),
    }
}

// ── wire codec ──
//
// INTEREST (frame kind 0x05) carries an edge or a snapshot; INTEREST_DIGEST
// (0x06) carries the digest. Both ride the 3A negotiated links, feature-gated by
// `features::INTEREST_ROUTING`, and egress through the 3A `PeerQueue` — no new
// queue type (Rule 5, the membership_sync precedent).

const U_EDGE: u8 = 1;
const U_SNAPSHOT: u8 = 2;
const T_ROOM: u8 = 1;
const T_USER: u8 = 2;

/// An INTEREST-frame payload: an edge or a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterestUpdate {
    Edge(InterestEdge),
    Snapshot(InterestSnapshot),
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn get_str(buf: &[u8], off: &mut usize) -> Option<String> {
    let len = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let bytes = buf.get(*off..*off + len)?;
    *off += len;
    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
}

fn put_target(buf: &mut Vec<u8>, t: &Target) {
    match t {
        Target::Room(r) => {
            buf.push(T_ROOM);
            put_str(buf, r);
        }
        Target::User(u) => {
            buf.push(T_USER);
            put_str(buf, u);
        }
    }
}

fn get_target(buf: &[u8], off: &mut usize) -> Option<Target> {
    let kind = *buf.get(*off)?;
    *off += 1;
    let s = get_str(buf, off)?;
    match kind {
        T_ROOM => Some(Target::Room(s)),
        T_USER => Some(Target::User(s)),
        _ => None,
    }
}

fn put_strs(buf: &mut Vec<u8>, items: &[String]) {
    buf.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for s in items {
        put_str(buf, s);
    }
}

fn get_strs(buf: &[u8], off: &mut usize) -> Option<Vec<String>> {
    let n = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        v.push(get_str(buf, off)?);
    }
    Some(v)
}

impl InterestUpdate {
    /// Encode the body of an INTEREST (0x05) frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            InterestUpdate::Edge(e) => {
                buf.push(U_EDGE);
                buf.extend_from_slice(&e.origin.to_le_bytes());
                buf.extend_from_slice(&e.seq.to_le_bytes());
                buf.push(e.add as u8);
                put_target(&mut buf, &e.target);
            }
            InterestUpdate::Snapshot(s) => {
                buf.push(U_SNAPSHOT);
                buf.extend_from_slice(&s.origin.to_le_bytes());
                buf.extend_from_slice(&s.seq.to_le_bytes());
                put_strs(&mut buf, &s.rooms);
                put_strs(&mut buf, &s.users);
            }
        }
        buf
    }

    /// Decode an INTEREST (0x05) frame body.
    pub fn decode(body: &[u8]) -> Option<InterestUpdate> {
        let sub = *body.first()?;
        let mut off = 1;
        let origin = u16::from_le_bytes(body.get(off..off + 2)?.try_into().ok()?);
        off += 2;
        let seq = u64::from_le_bytes(body.get(off..off + 8)?.try_into().ok()?);
        off += 8;
        match sub {
            U_EDGE => {
                let add = *body.get(off)? != 0;
                off += 1;
                let target = get_target(body, &mut off)?;
                Some(InterestUpdate::Edge(InterestEdge {
                    origin,
                    seq,
                    add,
                    target,
                }))
            }
            U_SNAPSHOT => {
                let rooms = get_strs(body, &mut off)?;
                let users = get_strs(body, &mut off)?;
                Some(InterestUpdate::Snapshot(InterestSnapshot {
                    origin,
                    seq,
                    rooms,
                    users,
                }))
            }
            _ => None,
        }
    }
}

/// Encode an INTEREST_DIGEST (0x06) frame body.
pub fn encode_digest(entries: &[DigestEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for (origin, seq, hash) in entries {
        buf.extend_from_slice(&origin.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&hash.to_le_bytes());
    }
    buf
}

/// Decode an INTEREST_DIGEST (0x06) frame body.
pub fn decode_digest(body: &[u8]) -> Option<Vec<DigestEntry>> {
    let mut off = 0;
    let n = u16::from_le_bytes(body.get(off..off + 2)?.try_into().ok()?) as usize;
    off += 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let origin = u16::from_le_bytes(body.get(off..off + 2)?.try_into().ok()?);
        off += 2;
        let seq = u64::from_le_bytes(body.get(off..off + 8)?.try_into().ok()?);
        off += 8;
        let hash = u64::from_le_bytes(body.get(off..off + 8)?.try_into().ok()?);
        off += 8;
        out.push((origin, seq, hash));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room(s: &str) -> Target {
        Target::Room(s.to_string())
    }
    fn alive(ids: &[NodeId]) -> HashSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn edge_triggered_only_transitions_publish() {
        let mut s = InterestState::new(1, Routing::Interest);
        assert!(s.local_set(room("a"), true).is_some(), "0->1 publishes");
        assert!(
            s.local_set(room("a"), true).is_none(),
            "join #2 is invisible"
        );
        assert!(s.local_set(room("a"), false).is_some(), "1->0 publishes");
        assert!(s.local_set(room("a"), false).is_none(), "already gone");
        assert_eq!(s.counters().interest_out, 2);
    }

    #[test]
    fn apply_edge_and_route() {
        let mut s = InterestState::new(1, Routing::Interest);
        s.apply_edge(&InterestEdge {
            origin: 2,
            seq: 1,
            add: true,
            target: room("x"),
        });
        assert_eq!(s.interested_peers(&room("x"), &alive(&[2, 3])), vec![2]);
        assert!(s.interested_peers(&room("y"), &alive(&[2, 3])).is_empty());
        // an unreachable host is not a relay target
        assert!(s.interested_peers(&room("x"), &alive(&[3])).is_empty());
    }

    #[test]
    fn seq_discipline_drops_stale_reordered_dup() {
        let mut s = InterestState::new(1, Routing::Interest);
        assert!(s.apply_edge(&InterestEdge {
            origin: 2,
            seq: 5,
            add: true,
            target: room("x")
        }));
        // stale
        assert!(!s.apply_edge(&InterestEdge {
            origin: 2,
            seq: 4,
            add: true,
            target: room("y")
        }));
        // duplicate
        assert!(!s.apply_edge(&InterestEdge {
            origin: 2,
            seq: 5,
            add: true,
            target: room("z")
        }));
        assert_eq!(s.counters().seq_drops, 2);
        // y and z were dropped — only x is known
        assert_eq!(s.interested_peers(&room("x"), &alive(&[2])), vec![2]);
        assert!(s.interested_peers(&room("y"), &alive(&[2])).is_empty());
    }

    #[test]
    fn digest_repairs_a_dropped_edge() {
        // A hosts x,y; B missed the `y` edge (seq gap). B's digest to A reveals
        // B is behind on A; A's response snapshot repairs B in one cycle.
        let mut a = InterestState::new(1, Routing::Interest);
        a.local_set(room("x"), true);
        a.local_set(room("y"), true);

        let mut b = InterestState::new(2, Routing::Interest);
        // B receives only the first edge (x@seq1); the y@seq2 edge is "dropped".
        b.apply_edge(&InterestEdge {
            origin: 1,
            seq: 1,
            add: true,
            target: room("x"),
        });
        assert!(
            b.interested_peers(&room("y"), &alive(&[1])).is_empty(),
            "B is missing y"
        );

        // Anti-entropy: A responds to B's digest.
        let repairs = a.respond_to_digest(&b.build_digest());
        let mut repaired = false;
        for snap in &repairs {
            if b.apply_snapshot(snap) {
                repaired = true;
            }
        }
        assert!(repaired, "digest must produce a repair");
        assert_eq!(
            b.interested_peers(&room("y"), &alive(&[1])),
            vec![1],
            "B repaired within one cycle"
        );
        assert!(b.counters().digest_repairs >= 1);
    }

    #[test]
    fn flood_lever_returns_all_peers_ignoring_interest() {
        let mut s = InterestState::new(1, Routing::Flood);
        // no interest known at all
        assert_eq!(
            s.interested_peers(&room("anything"), &alive(&[2, 3, 4])),
            vec![2, 3, 4]
        );
        // switching back to interest respects the (empty) table
        s.set_routing(Routing::Interest);
        assert!(s
            .interested_peers(&room("anything"), &alive(&[2, 3, 4]))
            .is_empty());
    }

    #[test]
    fn wire_codec_round_trips() {
        let updates = [
            InterestUpdate::Edge(InterestEdge {
                origin: 3,
                seq: 42,
                add: true,
                target: Target::Room("room-7".into()),
            }),
            InterestUpdate::Edge(InterestEdge {
                origin: 3,
                seq: 43,
                add: false,
                target: Target::User("u-9".into()),
            }),
            InterestUpdate::Snapshot(InterestSnapshot {
                origin: 5,
                seq: 100,
                rooms: vec!["a".into(), "b".into()],
                users: vec!["x".into()],
            }),
        ];
        for u in updates {
            assert_eq!(InterestUpdate::decode(&u.encode()), Some(u));
        }
        let digest: Vec<DigestEntry> = vec![(1, 5, 0xDEAD), (2, 9, 0xBEEF)];
        assert_eq!(decode_digest(&encode_digest(&digest)), Some(digest));
    }

    #[test]
    fn evicted_peer_interest_is_swept() {
        let mut s = InterestState::new(1, Routing::Interest);
        s.apply_edge(&InterestEdge {
            origin: 2,
            seq: 1,
            add: true,
            target: room("x"),
        });
        s.sweep_origin(2);
        assert!(
            s.interested_peers(&room("x"), &alive(&[2])).is_empty(),
            "no stuck interest for an evicted peer"
        );
    }
}
