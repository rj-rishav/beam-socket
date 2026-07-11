//! SWIM-style membership (RFC 0004 §4.2): probe/ack over UDP, indirect
//! probes, suspicion before eviction, incarnation refutation, piggybacked
//! dissemination, join push-pull, periodic re-seed (the heal path).
//!
//! JSON over UDP: throwaway-grade on purpose — membership packets are
//! low-rate control plane; the spike's latency-sensitive path (relay) uses
//! hand-rolled binary instead.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

use crate::{epoch_ms, SwimParams};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MState {
    Alive,
    Suspect,
    Dead,
}

/// One disseminated membership fact. Precedence (SWIM): higher incarnation
/// wins; equal incarnation: Dead > Suspect > Alive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub id: u16,
    pub addr: String,
    pub state: MState,
    pub inc: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Packet {
    /// `reply_to` set = this is an indirect probe on behalf of (id, addr):
    /// ack goes straight back to the origin.
    Ping {
        from: u16,
        seq: u64,
        reply_to: Option<(u16, String)>,
        updates: Vec<Update>,
    },
    Ack {
        from: u16,
        seq: u64,
        updates: Vec<Update>,
    },
    PingReq {
        from: u16,
        from_addr: String,
        seq: u64,
        target: u16,
        target_addr: String,
        updates: Vec<Update>,
    },
    /// Join / re-seed contact: full-state PUSH-pull. The push half is what
    /// makes partition heal work: the contacted node must see the joiner's
    /// "you are dead" claim about IT to trigger its own refutation —
    /// otherwise equal-incarnation Dead outranks Alive forever (stuck
    /// entries, found by the partition scenario's first run).
    Join {
        from: u16,
        addr: String,
        inc: u32,
        state: Vec<Update>,
    },
    JoinReply {
        from: u16,
        members: Vec<Update>,
    },
}

#[derive(Debug, Clone)]
pub struct Member {
    pub addr: SocketAddr,
    pub state: MState,
    pub inc: u32,
    pub since_ms: u64,
}

/// A membership state transition, timestamped for the coordinator (epoch ms
/// so kill times correlate across processes).
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub t_ms: u64,
    pub peer: u16,
    pub state: String,
}

pub struct Membership {
    pub self_id: u16,
    pub self_addr: String,
    pub self_inc: u32,
    pub members: HashMap<u16, Member>,
    /// (update, remaining retransmissions) — piggyback queue.
    gossip: VecDeque<(Update, u32)>,
    pub events: Vec<Event>,
    pub refutations: u64,
    probe_order: Vec<u16>,
    probe_idx: usize,
}

impl Membership {
    fn new(self_id: u16, self_addr: String) -> Self {
        Self {
            self_id,
            self_addr,
            self_inc: 1,
            members: HashMap::new(),
            gossip: VecDeque::new(),
            events: Vec::new(),
            refutations: 0,
            probe_order: Vec::new(),
            probe_idx: 0,
        }
    }

    fn enqueue(&mut self, u: Update, retransmit: u32) {
        // One live queue entry per (id): the newest fact supersedes.
        self.gossip.retain(|(g, _)| g.id != u.id);
        self.gossip.push_back((u, retransmit));
        if self.gossip.len() > 64 {
            self.gossip.pop_front(); // bounded (Rule 5, even in a spike)
        }
    }

    fn take_piggyback(&mut self, max: usize) -> Vec<Update> {
        let mut out = Vec::new();
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

    fn self_update(&self) -> Update {
        Update {
            id: self.self_id,
            addr: self.self_addr.clone(),
            state: MState::Alive,
            inc: self.self_inc,
        }
    }

    fn full_state(&self) -> Vec<Update> {
        let mut v: Vec<Update> = self
            .members
            .iter()
            .map(|(id, m)| Update {
                id: *id,
                addr: m.addr.to_string(),
                state: m.state,
                inc: m.inc,
            })
            .collect();
        v.push(self.self_update());
        v
    }

    /// Merge one disseminated fact (SWIM precedence). Returns true if it
    /// changed local state (then it re-gossips — epidemic dissemination).
    fn merge(&mut self, u: &Update, retransmit: u32) -> bool {
        if u.id == self.self_id {
            // Someone claims I'm suspect/dead → refute with a bumped
            // incarnation (the anti-false-positive valve, §4.2).
            if u.state != MState::Alive && u.inc >= self.self_inc {
                self.self_inc = u.inc + 1;
                self.refutations += 1;
                let refute = self.self_update();
                self.enqueue(refute, retransmit);
                return true;
            }
            return false;
        }
        let Ok(addr) = u.addr.parse::<SocketAddr>() else {
            return false;
        };
        let now = epoch_ms();
        let accept = match self.members.get(&u.id) {
            None => true,
            Some(m) => {
                u.inc > m.inc || (u.inc == m.inc && precedence(u.state) > precedence(m.state))
            }
        };
        if accept {
            let changed_state = self
                .members
                .get(&u.id)
                .map(|m| m.state != u.state)
                .unwrap_or(true);
            self.members.insert(
                u.id,
                Member {
                    addr,
                    state: u.state,
                    inc: u.inc,
                    since_ms: now,
                },
            );
            if changed_state {
                self.events.push(Event {
                    t_ms: now,
                    peer: u.id,
                    state: format!("{:?}", u.state).to_lowercase(),
                });
            }
            self.enqueue(u.clone(), retransmit);
        }
        accept
    }

    /// Round-robin over a shuffled member list (the SWIM probe order).
    fn next_probe_target(&mut self) -> Option<(u16, SocketAddr)> {
        use rand::seq::SliceRandom;
        let live: HashSet<u16> = self
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
            self.probe_order = live.iter().copied().collect();
            self.probe_order.shuffle(&mut rand::thread_rng());
            self.probe_idx = 0;
        }
        let id = self.probe_order[self.probe_idx];
        self.probe_idx += 1;
        self.members.get(&id).map(|m| (id, m.addr))
    }
}

fn precedence(s: MState) -> u8 {
    match s {
        MState::Alive => 0,
        MState::Suspect => 1,
        MState::Dead => 2,
    }
}

pub struct Swim {
    pub params: SwimParams,
    pub membership: Arc<Mutex<Membership>>,
    /// Node ids whose traffic we drop — socket-level partition injection.
    pub deny: Arc<Mutex<HashSet<u16>>>,
    socket: Arc<UdpSocket>,
    seeds: Vec<SocketAddr>,
    seq: AtomicU64,
    /// seq → probed id; the receive loop removes on ack.
    pending: Mutex<HashMap<u64, u16>>,
}

impl Swim {
    pub async fn start(
        self_id: u16,
        bind: SocketAddr,
        seeds: Vec<SocketAddr>,
        params: SwimParams,
        deny: Arc<Mutex<HashSet<u16>>>,
    ) -> Arc<Self> {
        let socket = Arc::new(UdpSocket::bind(bind).await.expect("swim bind"));
        let membership = Arc::new(Mutex::new(Membership::new(self_id, bind.to_string())));
        let swim = Arc::new(Self {
            params,
            membership,
            deny,
            socket,
            seeds,
            seq: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        });
        tokio::spawn(swim.clone().recv_loop());
        tokio::spawn(swim.clone().probe_loop());
        tokio::spawn(swim.clone().reseed_loop());
        swim
    }

    fn piggyback(&self) -> Vec<Update> {
        let mut m = self.membership.lock().unwrap();
        let mut ups = m.take_piggyback(self.params.gossip_max);
        // Always carry a fresh self-alive: cheap, and it is what heals
        // dead-marked entries on contact (incarnation revival, §4.8).
        ups.push(m.self_update());
        ups
    }

    async fn send(&self, to: SocketAddr, p: &Packet) {
        let buf = serde_json::to_vec(p).unwrap();
        let _ = self.socket.send_to(&buf, to).await;
    }

    async fn recv_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let Ok((n, src)) = self.socket.recv_from(&mut buf).await else {
                continue;
            };
            let Ok(pkt) = serde_json::from_slice::<Packet>(&buf[..n]) else {
                continue;
            };
            let from = match &pkt {
                Packet::Ping { from, .. }
                | Packet::Ack { from, .. }
                | Packet::PingReq { from, .. }
                | Packet::Join { from, .. }
                | Packet::JoinReply { from, .. } => *from,
            };
            // Socket-level partition injection: pretend the packet was lost.
            if self.deny.lock().unwrap().contains(&from) {
                continue;
            }
            self.handle(pkt, src).await;
        }
    }

    async fn handle(self: &Arc<Self>, pkt: Packet, src: SocketAddr) {
        let rt = self.params.retransmit;
        match pkt {
            Packet::Ping {
                from: _,
                seq,
                reply_to,
                updates,
            } => {
                {
                    let mut m = self.membership.lock().unwrap();
                    for u in &updates {
                        m.merge(u, rt);
                    }
                }
                let ups = self.piggyback();
                let ack = Packet::Ack {
                    from: {
                        let m = self.membership.lock().unwrap();
                        m.self_id
                    },
                    seq,
                    updates: ups,
                };
                let dest = match &reply_to {
                    Some((_, addr)) => addr.parse().unwrap_or(src),
                    None => src,
                };
                self.send(dest, &ack).await;
            }
            Packet::Ack { from, seq, updates } => {
                let direct = { self.pending.lock().unwrap().remove(&seq) == Some(from) };
                let mut m = self.membership.lock().unwrap();
                for u in &updates {
                    m.merge(u, rt);
                }
                if direct {
                    // Direct evidence of life: clear a local suspicion at the
                    // same incarnation. (Same-incarnation Alive normally
                    // loses to Suspect; a direct ack is the exception.)
                    let revived = match m.members.get(&from) {
                        Some(mem) if mem.state == MState::Suspect => Some((mem.addr, mem.inc)),
                        _ => None,
                    };
                    if let Some((addr, inc)) = revived {
                        let now = epoch_ms();
                        m.members.insert(
                            from,
                            Member {
                                addr,
                                state: MState::Alive,
                                inc,
                                since_ms: now,
                            },
                        );
                        m.events.push(Event {
                            t_ms: now,
                            peer: from,
                            state: "alive".into(),
                        });
                        m.enqueue(
                            Update {
                                id: from,
                                addr: addr.to_string(),
                                state: MState::Alive,
                                inc,
                            },
                            rt,
                        );
                    }
                }
            }
            Packet::PingReq {
                from,
                from_addr,
                seq,
                target,
                target_addr,
                updates,
            } => {
                {
                    let mut m = self.membership.lock().unwrap();
                    for u in &updates {
                        m.merge(u, rt);
                    }
                }
                // Probe the target on the origin's behalf; the ack goes
                // straight back to the origin (classic SWIM shortcut).
                if let Ok(taddr) = target_addr.parse::<SocketAddr>() {
                    let _ = target;
                    let ups = self.piggyback();
                    let self_id = { self.membership.lock().unwrap().self_id };
                    let ping = Packet::Ping {
                        from: self_id,
                        seq,
                        reply_to: Some((from, from_addr)),
                        updates: ups,
                    };
                    self.send(taddr, &ping).await;
                }
            }
            Packet::Join {
                from,
                addr,
                inc,
                state,
            } => {
                let reply = {
                    let mut m = self.membership.lock().unwrap();
                    // Push half first (may trigger our own refutation), so
                    // the pull half below already carries the bumped self.
                    for u in &state {
                        m.merge(u, rt);
                    }
                    m.merge(
                        &Update {
                            id: from,
                            addr,
                            state: MState::Alive,
                            inc,
                        },
                        rt,
                    );
                    Packet::JoinReply {
                        from: m.self_id,
                        members: m.full_state(),
                    }
                };
                self.send(src, &reply).await;
            }
            Packet::JoinReply { members, .. } => {
                let mut m = self.membership.lock().unwrap();
                for u in &members {
                    m.merge(u, rt);
                }
            }
        }
    }

    /// The SWIM probe cycle: one target per period; direct probe → indirect
    /// probes → suspect. Suspicion GC runs at each tick.
    async fn probe_loop(self: Arc<Self>) {
        let p = self.params;
        loop {
            tokio::time::sleep(Duration::from_millis(p.period_ms)).await;

            // Suspicion timeout → dead (eviction), disseminated.
            {
                let mut m = self.membership.lock().unwrap();
                let now = epoch_ms();
                let expired: Vec<(u16, SocketAddr, u32)> = m
                    .members
                    .iter()
                    .filter(|(_, mem)| {
                        mem.state == MState::Suspect
                            && now.saturating_sub(mem.since_ms) > p.suspicion_ms
                    })
                    .map(|(id, mem)| (*id, mem.addr, mem.inc))
                    .collect();
                for (id, addr, inc) in expired {
                    let dead = Update {
                        id,
                        addr: addr.to_string(),
                        state: MState::Dead,
                        inc,
                    };
                    m.merge(&dead, p.retransmit);
                }
            }

            let Some((target, taddr)) = ({
                let mut m = self.membership.lock().unwrap();
                m.next_probe_target()
            }) else {
                continue;
            };

            // Direct probe.
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            self.pending.lock().unwrap().insert(seq, target);
            let self_id = { self.membership.lock().unwrap().self_id };
            let ping = Packet::Ping {
                from: self_id,
                seq,
                reply_to: None,
                updates: self.piggyback(),
            };
            self.send(taddr, &ping).await;
            tokio::time::sleep(Duration::from_millis(p.probe_timeout_ms)).await;

            if self.pending.lock().unwrap().contains_key(&seq) {
                // Indirect probes via k random helpers.
                let helpers: Vec<SocketAddr> = {
                    use rand::seq::SliceRandom;
                    let m = self.membership.lock().unwrap();
                    let mut hs: Vec<SocketAddr> = m
                        .members
                        .iter()
                        .filter(|(id, mem)| **id != target && mem.state == MState::Alive)
                        .map(|(_, mem)| mem.addr)
                        .collect();
                    hs.shuffle(&mut rand::thread_rng());
                    hs.truncate(p.indirect_k);
                    hs
                };
                let (self_id, self_addr) = {
                    let m = self.membership.lock().unwrap();
                    (m.self_id, m.self_addr.clone())
                };
                for h in helpers {
                    let req = Packet::PingReq {
                        from: self_id,
                        from_addr: self_addr.clone(),
                        seq,
                        target,
                        target_addr: taddr.to_string(),
                        updates: self.piggyback(),
                    };
                    self.send(h, &req).await;
                }
                tokio::time::sleep(Duration::from_millis(p.probe_timeout_ms)).await;

                if self.pending.lock().unwrap().remove(&seq).is_some() {
                    // No direct or indirect ack → suspect, disseminated.
                    let mut m = self.membership.lock().unwrap();
                    if let Some(mem) = m.members.get(&target) {
                        if mem.state == MState::Alive {
                            let sus = Update {
                                id: target,
                                addr: mem.addr.to_string(),
                                state: MState::Suspect,
                                inc: mem.inc,
                            };
                            m.merge(&sus, p.retransmit);
                        }
                    }
                }
            }
        }
    }

    /// Low-rate re-contact of seeds (join push-pull). This is BOTH the cold
    /// -start join and the partition-heal path (§4.8): it runs regardless of
    /// what state the seed is marked, so two islands that each evicted the
    /// other re-merge as soon as packets flow again.
    async fn reseed_loop(self: Arc<Self>) {
        loop {
            let (self_id, addr, inc, state) = {
                let m = self.membership.lock().unwrap();
                (m.self_id, m.self_addr.clone(), m.self_inc, m.full_state())
            };
            // ThreadRng is !Send — pick the seed before any await point.
            let seed = if self.seeds.is_empty() {
                None
            } else {
                let i = rand::random::<usize>() % self.seeds.len();
                Some(self.seeds[i])
            };
            if let Some(seed) = seed {
                let join = Packet::Join {
                    from: self_id,
                    addr,
                    inc,
                    state,
                };
                self.send(seed, &join).await;
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    }
}
