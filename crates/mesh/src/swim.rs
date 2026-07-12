//! SWIM membership — the failure-detector state machine (RFC 0004 §4.2),
//! graduated from `spike/mesh/src/swim.rs`.
//!
//! This module is **transport-agnostic**: it is the member table, the SWIM
//! precedence/merge rules, incarnation refutation, the bounded gossip list, and
//! the probe-target scheduler. It does no IO. Two planes drive it:
//! - the **UDP probe plane** ([`crate::probe`]) — failure *detection* only;
//!   PING/ACK/PING-REQ carry **no member state** (§4.4 freeze: UDP is
//!   probe-only), they just decide who is un-ack'd;
//! - the **TCP dissemination plane** ([`crate::membership_sync`]) — spreads the
//!   join push-pull, gossip, and anti-entropy digest over negotiated links.
//!
//! What graduated unchanged from the spike: precedence (higher incarnation
//! wins; equal incarnation Dead > Suspect > Alive), refute-on-self-suspicion,
//! and push-pull's load-bearing property (the contacted node must *see* the
//! joiner's claim about it to refute). What changed: JSON-over-UDP became a
//! frozen binary probe format with no member state, and dissemination moved to
//! TCP.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// SWIM tuning (§4.2 table). **The shipped default is [`SwimParams::tuned`]**,
/// on measurement: the literature row missed the 5 s kill-detection gate at
/// 8.9 s, the tuned row made it at 4.8 s with zero false positives under
/// oversubscription (`0004-results.md`, gate scoreboard + P1). The literature
/// row stays selectable for jittery networks; it is not the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwimParams {
    /// Protocol period `T`: one probe per period.
    pub period: Duration,
    /// No direct ACK within this → escalate to indirect probes.
    pub probe_timeout: Duration,
    /// Number of random relays asked for an indirect probe.
    pub indirect_k: usize,
    /// No refutation within this after Suspect → Dead (eviction).
    pub suspicion_timeout: Duration,
    /// Max membership updates piggybacked per TCP dissemination frame.
    pub gossip_max: usize,
    /// Retransmissions per accepted update (`λ·log₂(N+1)`, N ≤ 50 → 8).
    pub retransmit: u32,
}

impl SwimParams {
    /// The **shipped default** (§4.2 tuned row, `0004-results.md`): the row that
    /// passed the kill-detection gate. Do not retune these without a new
    /// results entry (the RFC 0001 rule).
    pub fn tuned() -> Self {
        Self {
            period: Duration::from_millis(500),
            probe_timeout: Duration::from_millis(250),
            indirect_k: 3,
            // 2·T·log(N) ≈ 2.5 s @ N=5.
            suspicion_timeout: Duration::from_millis(2500),
            gossip_max: 8,
            retransmit: 8,
        }
    }

    /// The memberlist-ish literature row — selectable for jittery networks, but
    /// **not** the default: it failed the detection gate at 8.9 s
    /// (`0004-results.md`). Kept so a deployment can widen the timers without a
    /// recompile; a node that ships this instead of [`tuned`] has opted out of
    /// the measured default deliberately.
    pub fn literature() -> Self {
        Self {
            period: Duration::from_millis(1000),
            probe_timeout: Duration::from_millis(500),
            indirect_k: 3,
            // 4·T·log(N) ≈ 5 s @ N=5.
            suspicion_timeout: Duration::from_millis(5000),
            gossip_max: 8,
            retransmit: 8,
        }
    }
}

impl Default for SwimParams {
    fn default() -> Self {
        Self::tuned()
    }
}

/// A member's liveness state. Precedence at equal incarnation runs
/// `Dead`, then `Suspect`, then `Alive` — a claim of death only loses to a
/// *newer* incarnation (the refutation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MState {
    Alive,
    Suspect,
    Dead,
}

fn precedence(s: MState) -> u8 {
    match s {
        MState::Alive => 0,
        MState::Suspect => 1,
        MState::Dead => 2,
    }
}

/// One disseminated membership fact. This is the payload of the **TCP**
/// dissemination plane; it never rides UDP (§4.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub id: u16,
    pub addr: SocketAddr,
    pub state: MState,
    pub inc: u32,
}

/// A live member entry. `since` is a monotonic instant used only for the
/// suspicion timer (never disseminated).
#[derive(Debug, Clone)]
pub struct Member {
    pub addr: SocketAddr,
    pub state: MState,
    pub inc: u32,
    pub since: Instant,
}

/// Membership counters (§13.2: joined/suspected/dead/refuted surface for 3D's
/// `stats()`). Plain `u64` behind the membership lock — control-plane rate, no
/// hot path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MembershipCounters {
    /// First time a peer became Alive in our table.
    pub joined: u64,
    /// Transitions into Suspect.
    pub suspected: u64,
    /// Transitions into Dead (evictions).
    pub dead: u64,
    /// Times *we* refuted a suspicion/death of ourselves (bumped incarnation).
    pub refuted: u64,
    /// Times a peer we had marked Suspect/Dead came back Alive.
    pub revived: u64,
}

/// A point-in-time member entry for the table API (3D reads this into `stats()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberInfo {
    pub id: u16,
    pub addr: SocketAddr,
    pub state: MState,
    pub inc: u32,
}

/// The bound on the in-memory gossip retransmit list. This is **not** a new
/// wire queue (Rule 5 — the wire egress is the 3A [`crate::queue::PeerQueue`]);
/// it is the SWIM dissemination buffer, one live entry per member id, so its
/// natural size is O(N) — the cap is a belt-and-suspenders ceiling.
const GOSSIP_CAP: usize = 256;

/// The membership table and SWIM logic for one node. Held behind an
/// `Arc<Mutex<..>>`; both planes lock it briefly.
pub struct Membership {
    pub self_id: u16,
    pub self_addr: SocketAddr,
    self_inc: u32,
    members: HashMap<u16, Member>,
    /// (update, remaining retransmissions) — the piggyback list drained onto
    /// TCP dissemination frames. Bounded (`GOSSIP_CAP`), one entry per id.
    gossip: VecDeque<(Update, u32)>,
    counters: MembershipCounters,
    probe_order: Vec<u16>,
    probe_idx: usize,
    rng: Rng,
}

impl Membership {
    pub fn new(self_id: u16, self_addr: SocketAddr) -> Self {
        Self {
            self_id,
            self_addr,
            self_inc: 1,
            members: HashMap::new(),
            gossip: VecDeque::new(),
            counters: MembershipCounters::default(),
            probe_order: Vec::new(),
            probe_idx: 0,
            rng: Rng::seeded(),
        }
    }

    pub fn self_incarnation(&self) -> u32 {
        self.self_inc
    }

    pub fn counters(&self) -> MembershipCounters {
        self.counters
    }

    /// The current member table (excludes self), sorted by id for stable output.
    pub fn table(&self) -> Vec<MemberInfo> {
        let mut v: Vec<MemberInfo> = self
            .members
            .iter()
            .map(|(id, m)| MemberInfo {
                id: *id,
                addr: m.addr,
                state: m.state,
                inc: m.inc,
            })
            .collect();
        v.sort_by_key(|m| m.id);
        v
    }

    /// Count of members we currently consider Alive (excludes self).
    pub fn alive_count(&self) -> usize {
        self.members
            .values()
            .filter(|m| m.state == MState::Alive)
            .count()
    }

    /// Does the table hold any non-Alive entry? (The heal gate asserts this is
    /// false after convergence — "zero stuck entries".)
    pub fn has_non_alive(&self) -> bool {
        self.members.values().any(|m| m.state != MState::Alive)
    }

    fn self_update(&self) -> Update {
        Update {
            id: self.self_id,
            addr: self.self_addr,
            state: MState::Alive,
            inc: self.self_inc,
        }
    }

    /// Full state incl. self — the push half of push-pull, and the digest-repair
    /// payload.
    pub fn full_state(&self) -> Vec<Update> {
        let mut v: Vec<Update> = self
            .members
            .iter()
            .map(|(id, m)| Update {
                id: *id,
                addr: m.addr,
                state: m.state,
                inc: m.inc,
            })
            .collect();
        v.push(self.self_update());
        v
    }

    fn enqueue(&mut self, u: Update, retransmit: u32) {
        // One live entry per id: the newest fact supersedes older gossip.
        self.gossip.retain(|(g, _)| g.id != u.id);
        self.gossip.push_back((u, retransmit));
        while self.gossip.len() > GOSSIP_CAP {
            self.gossip.pop_front();
        }
    }

    /// Drain up to `max` updates for a dissemination frame, decrementing each
    /// entry's retransmit budget. Always includes a fresh self-alive at the
    /// front — cheap, and it is what revives a peer that had marked us dead
    /// (incarnation revival on contact, §4.8).
    pub fn take_piggyback(&mut self, max: usize) -> Vec<Update> {
        let mut out = vec![self.self_update()];
        let mut still = VecDeque::new();
        while let Some((u, n)) = self.gossip.pop_front() {
            if out.len() < max {
                out.push(u.clone());
                if n > 1 {
                    still.push_back((u, n - 1));
                }
            } else {
                still.push_back((u, n));
            }
        }
        self.gossip = still;
        out
    }

    /// Merge one disseminated fact under SWIM precedence. Returns true if local
    /// state changed (the caller re-gossips on true — epidemic spread).
    ///
    /// The `self` branch is the anti-false-positive valve **and** the push-pull
    /// heal: hearing any non-Alive claim about ourselves at our incarnation or
    /// higher triggers a refutation with a bumped incarnation.
    pub fn merge(&mut self, u: &Update, retransmit: u32) -> bool {
        if u.id == self.self_id {
            if u.state != MState::Alive && u.inc >= self.self_inc {
                self.self_inc = u.inc + 1;
                self.counters.refuted += 1;
                let refute = self.self_update();
                self.enqueue(refute, retransmit);
                return true;
            }
            return false;
        }

        let accept = match self.members.get(&u.id) {
            None => true,
            Some(m) => {
                u.inc > m.inc || (u.inc == m.inc && precedence(u.state) > precedence(m.state))
            }
        };
        if !accept {
            return false;
        }

        let prev = self.members.get(&u.id).map(|m| m.state);
        self.members.insert(
            u.id,
            Member {
                addr: u.addr,
                state: u.state,
                inc: u.inc,
                since: Instant::now(),
            },
        );
        self.count_transition(prev, u.state);
        self.enqueue(u.clone(), retransmit);
        true
    }

    fn count_transition(&mut self, prev: Option<MState>, next: MState) {
        match (prev, next) {
            (None, MState::Alive) => self.counters.joined += 1,
            (None, MState::Suspect) => self.counters.suspected += 1,
            (None, MState::Dead) => self.counters.dead += 1,
            (Some(p), n) if p != n => match n {
                MState::Alive => self.counters.revived += 1,
                MState::Suspect => self.counters.suspected += 1,
                MState::Dead => self.counters.dead += 1,
            },
            _ => {}
        }
    }

    /// A direct ACK is proof of life: clear a same-incarnation suspicion (which
    /// gossip precedence would otherwise not undo). Returns true if it revived.
    pub fn note_direct_ack(&mut self, from: u16, retransmit: u32) -> bool {
        let revive = matches!(self.members.get(&from), Some(m) if m.state == MState::Suspect);
        if !revive {
            return false;
        }
        let (addr, inc) = {
            let m = &self.members[&from];
            (m.addr, m.inc)
        };
        self.members.insert(
            from,
            Member {
                addr,
                state: MState::Alive,
                inc,
                since: Instant::now(),
            },
        );
        self.counters.revived += 1;
        self.enqueue(
            Update {
                id: from,
                addr,
                state: MState::Alive,
                inc,
            },
            retransmit,
        );
        true
    }

    /// Mark `target` Suspect if it is currently Alive (no ack came back). Returns
    /// true if it transitioned.
    pub fn suspect(&mut self, target: u16, retransmit: u32) -> bool {
        let Some(m) = self.members.get(&target) else {
            return false;
        };
        if m.state != MState::Alive {
            return false;
        }
        let sus = Update {
            id: target,
            addr: m.addr,
            state: MState::Suspect,
            inc: m.inc,
        };
        self.merge(&sus, retransmit)
    }

    /// Promote every Suspect past its suspicion timeout to Dead (eviction),
    /// disseminated. Returns the ids newly evicted.
    pub fn expire_suspects(&mut self, timeout: Duration, retransmit: u32) -> Vec<u16> {
        let now = Instant::now();
        let expired: Vec<(u16, SocketAddr, u32)> = self
            .members
            .iter()
            .filter(|(_, m)| m.state == MState::Suspect && now.duration_since(m.since) > timeout)
            .map(|(id, m)| (*id, m.addr, m.inc))
            .collect();
        let mut dead = Vec::new();
        for (id, addr, inc) in expired {
            let up = Update {
                id,
                addr,
                state: MState::Dead,
                inc,
            };
            if self.merge(&up, retransmit) {
                dead.push(id);
            }
        }
        dead
    }

    /// The next probe target: round-robin over a periodically-reshuffled list of
    /// non-Dead members (the SWIM probe order). `None` if we know no one.
    pub fn next_probe_target(&mut self) -> Option<(u16, SocketAddr)> {
        let live: Vec<u16> = self
            .members
            .iter()
            .filter(|(_, m)| m.state != MState::Dead)
            .map(|(id, _)| *id)
            .collect();
        if live.is_empty() {
            return None;
        }
        self.probe_order.retain(|id| live.contains(id));
        if self.probe_idx >= self.probe_order.len() {
            self.probe_order = live;
            self.rng.shuffle(&mut self.probe_order);
            self.probe_idx = 0;
        }
        let id = self.probe_order[self.probe_idx];
        self.probe_idx += 1;
        self.members.get(&id).map(|m| (id, m.addr))
    }

    /// `k` random Alive members other than `exclude` — the indirect-probe relays.
    pub fn indirect_helpers(&mut self, exclude: u16, k: usize) -> Vec<(u16, SocketAddr)> {
        let mut hs: Vec<(u16, SocketAddr)> = self
            .members
            .iter()
            .filter(|(id, m)| **id != exclude && m.state == MState::Alive)
            .map(|(id, m)| (*id, m.addr))
            .collect();
        self.rng.shuffle(&mut hs);
        hs.truncate(k);
        hs
    }

    /// The addr for a member id, if known (probe plane resolves targets).
    pub fn addr_of(&self, id: u16) -> Option<SocketAddr> {
        self.members.get(&id).map(|m| m.addr)
    }
}

/// A tiny xorshift64* PRNG — enough to randomize probe order without pulling in
/// the `rand` crate (the crate stays std + tokio only, the 3A choice). Seeded
/// once from the OS CSPRNG.
struct Rng(u64);

impl Rng {
    fn seeded() -> Self {
        let n = crate::crypto::random_nonce();
        let seed = u64::from_le_bytes(n[0..8].try_into().unwrap()) | 1;
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn shuffle<T>(&mut self, v: &mut [T]) {
        // Fisher-Yates.
        let n = v.len();
        for i in (1..n).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            v.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn alive(id: u16, inc: u32) -> Update {
        Update {
            id,
            addr: addr(9000 + id),
            state: MState::Alive,
            inc,
        }
    }

    #[test]
    fn tuned_is_the_default_and_matches_the_cited_row() {
        // cite-or-fail: the shipped default must be the measured tuned row.
        assert_eq!(SwimParams::default(), SwimParams::tuned());
        let t = SwimParams::tuned();
        assert_eq!(t.period, Duration::from_millis(500));
        assert_eq!(t.probe_timeout, Duration::from_millis(250));
        assert_eq!(t.suspicion_timeout, Duration::from_millis(2500));
        assert_eq!(t.indirect_k, 3);
        // and it is NOT the literature row that failed the gate.
        assert_ne!(SwimParams::default(), SwimParams::literature());
    }

    #[test]
    fn precedence_higher_incarnation_wins() {
        let mut m = Membership::new(1, addr(1));
        assert!(m.merge(&alive(2, 5), 3));
        // lower incarnation is ignored
        assert!(!m.merge(&alive(2, 4), 3));
        // higher incarnation accepted
        assert!(m.merge(&alive(2, 6), 3));
        assert_eq!(m.table()[0].inc, 6);
    }

    #[test]
    fn equal_incarnation_dead_beats_alive() {
        let mut m = Membership::new(1, addr(1));
        m.merge(&alive(2, 5), 3);
        let dead = Update {
            id: 2,
            addr: addr(9002),
            state: MState::Dead,
            inc: 5,
        };
        assert!(m.merge(&dead, 3));
        // equal-incarnation Alive must NOT revive a Dead — only a newer inc does
        assert!(!m.merge(&alive(2, 5), 3));
        assert!(m.merge(&alive(2, 6), 3));
    }

    #[test]
    fn refutes_suspicion_of_self_with_bumped_incarnation() {
        // THE push-pull heal at the table level (spike failure #2): a node that
        // hears "you are dead @ my incarnation" refutes by bumping past it.
        let mut m = Membership::new(2, addr(2));
        let start_inc = m.self_incarnation();
        let dead_self = Update {
            id: 2,
            addr: addr(2),
            state: MState::Dead,
            inc: start_inc,
        };
        assert!(m.merge(&dead_self, 3), "must refute");
        assert!(m.self_incarnation() > start_inc, "incarnation bumped");
        assert_eq!(m.counters().refuted, 1);
        // The refutation is queued for dissemination as a fresh self-alive.
        let pb = m.take_piggyback(8);
        assert!(pb
            .iter()
            .any(|u| u.id == 2 && u.state == MState::Alive && u.inc > start_inc));
    }

    #[test]
    fn direct_ack_revives_a_suspect() {
        let mut m = Membership::new(1, addr(1));
        m.merge(&alive(2, 5), 3);
        m.suspect(2, 3);
        assert_eq!(m.table()[0].state, MState::Suspect);
        assert!(m.note_direct_ack(2, 3));
        assert_eq!(m.table()[0].state, MState::Alive);
        assert_eq!(m.counters().revived, 1);
    }

    #[test]
    fn piggyback_always_carries_self_alive() {
        let m = &mut Membership::new(7, addr(7));
        let pb = m.take_piggyback(8);
        assert_eq!(pb[0].id, 7);
        assert_eq!(pb[0].state, MState::Alive);
    }

    #[test]
    fn counters_track_transitions() {
        let mut m = Membership::new(1, addr(1));
        m.merge(&alive(2, 1), 3); // joined
        m.suspect(2, 3); // suspected
        let dead = m.expire_suspects(Duration::from_millis(0), 3); // dead
        assert_eq!(dead, vec![2]);
        let c = m.counters();
        assert_eq!((c.joined, c.suspected, c.dead), (1, 1, 1));
    }
}
