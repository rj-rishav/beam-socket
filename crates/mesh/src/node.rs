//! The mesh **node** — the runnable assembly of the two membership planes
//! (RFC 0004 §4.2, ENGINEERING §13.2).
//!
//! One node binds a UDP socket (the [`crate::probe`] failure detector) and a TCP
//! listener (the [`crate::membership_sync`] dissemination plane, over 3A links)
//! on the **same address**. It manages one link per peer (the 3A "one link per
//! pair" rule, higher id dials lower, plus a seed dial for bootstrap/heal),
//! routes inbound MEMBERSHIP frames to the table, spreads changes by gossip +
//! anti-entropy digest, and feeds link death into suspicion.
//!
//! This is the crate-level membership subsystem; the engine/SDK wiring that
//! surfaces it into `stats()` is 3D. The crate still has no reverse
//! dependencies.
//!
//! **Fault injection is at this layer, not iptables** (§13.2): [`MeshNode::
//! set_partition`] denies a set of peers — dropping their UDP and severing their
//! TCP links — so the partition-heal gate runs in CI.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

use crate::config::{Backoff, LinkConfig};
use crate::frame::{Frame, FrameKind};
use crate::link::{CloseHandler, InboundHandler, Link, LinkHandle, LinkHooks};
use crate::membership_sync::{self, MembershipMsg};
use crate::probe::{ProbeCounters, ProbePlane};
use crate::swim::{MState, MemberInfo, Membership, MembershipCounters, SwimParams};

/// How often the node dials missing peers, spreads gossip, and (a tenth as
/// often) runs anti-entropy. Fast enough for the <2 s convergence gate; slow
/// enough to be control-plane cheap for N ≤ 50.
const DIAL_INTERVAL: Duration = Duration::from_millis(300);
const GOSSIP_INTERVAL: Duration = Duration::from_millis(300);
const DIGEST_INTERVAL: Duration = Duration::from_millis(1000);

/// Config for one mesh node (the link-layer view of §5's `cluster` block).
#[derive(Debug, Clone)]
pub struct MeshConfig {
    pub node_id: u16,
    pub listen: SocketAddr,
    pub seeds: Vec<SocketAddr>,
    pub secret: Vec<u8>,
    pub cluster_name: String,
    pub params: SwimParams,
}

impl MeshConfig {
    pub fn new(node_id: u16, listen: SocketAddr, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            node_id,
            listen,
            seeds: Vec::new(),
            secret: secret.into(),
            cluster_name: "default".to_string(),
            params: SwimParams::tuned(),
        }
    }
}

struct Inner {
    self_id: u16,
    addr: SocketAddr,
    cluster_name: String,
    secret: Vec<u8>,
    params: SwimParams,
    seeds: Vec<SocketAddr>,
    membership: Arc<Mutex<Membership>>,
    links: Mutex<HashMap<u16, LinkHandle>>,
    deny: Arc<Mutex<HashSet<u16>>>,
    dial_backoff: Mutex<HashMap<SocketAddr, (u32, Instant)>>,
    backoff: Backoff,
    shutdown: AtomicBool,
}

impl Inner {
    fn link_cfg(&self) -> LinkConfig {
        LinkConfig::new(self.self_id, self.cluster_name.clone(), self.secret.clone())
    }

    fn is_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Per-link callbacks: route inbound MEMBERSHIP frames, and feed link death
    /// into suspicion.
    fn hooks(self: &Arc<Self>) -> LinkHooks {
        let a = self.clone();
        let inbound: InboundHandler = Arc::new(move |peer, frame| a.on_inbound(peer, frame));
        let b = self.clone();
        let on_close: CloseHandler = Arc::new(move |peer| b.on_link_closed(peer));
        LinkHooks {
            inbound: Some(inbound),
            on_close: Some(on_close),
        }
    }

    fn on_inbound(&self, peer: u16, frame: &Frame) {
        if frame.kind != FrameKind::Membership {
            return;
        }
        if self.deny.lock().unwrap().contains(&peer) {
            return;
        }
        let Some(msg) = MembershipMsg::decode(&frame.body) else {
            return;
        };
        let resp = {
            let mut m = self.membership.lock().unwrap();
            membership_sync::apply(msg, &mut m, self.params.retransmit)
        };
        if let Some(resp) = resp {
            self.send_to(peer, &resp);
        }
    }

    fn on_link_closed(&self, peer: u16) {
        self.links.lock().unwrap().remove(&peer);
        if self.is_down() {
            return;
        }
        // A dead TCP link is evidence the peer may be gone → suspicion. If it is
        // actually alive, a probe ACK or a fresh sync revives it.
        let mut m = self.membership.lock().unwrap();
        m.suspect(peer, self.params.retransmit);
    }

    fn send_to(&self, peer: u16, msg: &MembershipMsg) {
        let handle = self.links.lock().unwrap().get(&peer).cloned();
        if let Some(h) = handle {
            let _ = h.try_send(Frame::new(FrameKind::Membership, msg.encode()));
        }
    }

    /// Register a freshly-established link (dial or accept), enforcing one link
    /// per pair, then kick off the push-pull join over it.
    fn register(self: &Arc<Self>, handle: LinkHandle) {
        let peer = handle.peer_node_id();
        if self.is_down() || self.deny.lock().unwrap().contains(&peer) {
            handle.close();
            return;
        }
        {
            let mut links = self.links.lock().unwrap();
            if links.contains_key(&peer) {
                handle.close(); // a racing dial/accept lost; keep the existing
                return;
            }
            links.insert(peer, handle);
        }
        // Push-pull join: push our full state (so the peer sees any stale claim
        // about itself and refutes) and request theirs back.
        let sync = {
            let m = self.membership.lock().unwrap();
            MembershipMsg::Sync {
                reply: true,
                updates: m.full_state(),
            }
        };
        self.send_to(peer, &sync);
    }

    /// Addresses we should have a link to but don't: seeds (bootstrap/heal) and
    /// members with a lower id (the dial rule), minus denied peers.
    fn dial_targets(&self) -> Vec<SocketAddr> {
        let linked_ids: HashSet<u16> = self.links.lock().unwrap().keys().copied().collect();
        let denied = self.deny.lock().unwrap().clone();
        let table = self.membership.lock().unwrap().table();

        // Listen addrs of peers we already have a link to (dedup seeds by addr).
        let linked_addrs: HashSet<SocketAddr> = table
            .iter()
            .filter(|mi| linked_ids.contains(&mi.id))
            .map(|mi| mi.addr)
            .collect();
        // Listen addrs of denied peers (don't dial across a partition).
        let denied_addrs: HashSet<SocketAddr> = table
            .iter()
            .filter(|mi| denied.contains(&mi.id))
            .map(|mi| mi.addr)
            .collect();

        let mut out: Vec<SocketAddr> = Vec::new();
        for s in &self.seeds {
            if *s != self.addr && !linked_addrs.contains(s) && !denied_addrs.contains(s) {
                out.push(*s);
            }
        }
        for mi in &table {
            if mi.id < self.self_id
                && mi.state != MState::Dead
                && !linked_ids.contains(&mi.id)
                && !denied.contains(&mi.id)
                && !out.contains(&mi.addr)
            {
                out.push(mi.addr);
            }
        }
        out
    }

    fn dial_ready(&self, addr: SocketAddr) -> bool {
        match self.dial_backoff.lock().unwrap().get(&addr) {
            Some((_, next)) => Instant::now() >= *next,
            None => true,
        }
    }
    fn dial_ok(&self, addr: SocketAddr) {
        self.dial_backoff.lock().unwrap().remove(&addr);
    }
    fn dial_fail(&self, addr: SocketAddr) {
        let mut bo = self.dial_backoff.lock().unwrap();
        let e = bo.entry(addr).or_insert((0, Instant::now()));
        e.0 += 1;
        e.1 = Instant::now() + self.backoff.next_delay(e.0);
    }

    fn all_links(&self) -> Vec<LinkHandle> {
        self.links.lock().unwrap().values().cloned().collect()
    }
}

/// A running mesh node. Dropping it does **not** stop it (the loops hold their
/// own `Arc`s); call [`MeshNode::shutdown`] for that — which is also how a test
/// simulates a `kill -9`.
pub struct MeshNode {
    inner: Arc<Inner>,
    probe: Arc<ProbePlane>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl MeshNode {
    pub async fn start(config: MeshConfig) -> std::io::Result<Arc<MeshNode>> {
        // Bind UDP first so we can bind TCP to the same (possibly :0-resolved)
        // port — peers reach both planes at one address.
        let udp = Arc::new(UdpSocket::bind(config.listen).await?);
        let addr = udp.local_addr()?;
        let listener = TcpListener::bind(addr).await?;

        let membership = Arc::new(Mutex::new(Membership::new(config.node_id, addr)));
        let deny = Arc::new(Mutex::new(HashSet::new()));
        let probe = ProbePlane::start(
            udp,
            membership.clone(),
            config.params,
            config.secret.clone(),
            deny.clone(),
        );

        let inner = Arc::new(Inner {
            self_id: config.node_id,
            addr,
            cluster_name: config.cluster_name,
            secret: config.secret,
            params: config.params,
            seeds: config.seeds,
            membership,
            links: Mutex::new(HashMap::new()),
            deny,
            dial_backoff: Mutex::new(HashMap::new()),
            backoff: Backoff::default(),
            shutdown: AtomicBool::new(false),
        });

        let tasks = vec![
            tokio::spawn(accept_loop(inner.clone(), listener)),
            tokio::spawn(dial_loop(inner.clone())),
            tokio::spawn(gossip_loop(inner.clone())),
            tokio::spawn(digest_loop(inner.clone())),
        ];

        Ok(Arc::new(MeshNode {
            inner,
            probe,
            tasks: Mutex::new(tasks),
        }))
    }

    pub fn addr(&self) -> SocketAddr {
        self.inner.addr
    }
    pub fn self_id(&self) -> u16 {
        self.inner.self_id
    }
    pub fn member_table(&self) -> Vec<MemberInfo> {
        self.inner.membership.lock().unwrap().table()
    }
    pub fn membership_counters(&self) -> MembershipCounters {
        self.inner.membership.lock().unwrap().counters()
    }
    pub fn probe_counters(&self) -> Arc<ProbeCounters> {
        self.probe.counters()
    }
    pub fn self_incarnation(&self) -> u32 {
        self.inner.membership.lock().unwrap().self_incarnation()
    }
    /// Members we currently consider Alive (excludes self).
    pub fn alive_count(&self) -> usize {
        self.inner.membership.lock().unwrap().alive_count()
    }
    /// True if the table holds any Suspect/Dead entry (the heal gate asserts
    /// this is false — "zero stuck entries").
    pub fn has_non_alive(&self) -> bool {
        self.inner.membership.lock().unwrap().has_non_alive()
    }

    /// Inject a partition: deny `peers` — drop their UDP, sever their TCP links,
    /// and stop dialing them. Symmetric across the split when both sides call it.
    pub fn set_partition(&self, peers: Vec<u16>) {
        let set: HashSet<u16> = peers.into_iter().collect();
        let to_close: Vec<LinkHandle> = {
            let links = self.inner.links.lock().unwrap();
            links
                .iter()
                .filter(|(id, _)| set.contains(id))
                .map(|(_, h)| h.clone())
                .collect()
        };
        for h in to_close {
            h.close();
        }
        *self.inner.deny.lock().unwrap() = set;
    }

    /// Clear the partition (the heal path, §4.8): UDP flows again and the dial
    /// loop re-contacts seeds/peers, whose push-pull sync triggers refutation.
    pub fn heal(&self) {
        self.inner.deny.lock().unwrap().clear();
        self.inner.dial_backoff.lock().unwrap().clear();
    }

    /// Stop the node abruptly (the test `kill -9`): abort every task, stop
    /// acking, and drop the links.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.probe.shutdown();
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        let links = self.inner.links.lock().unwrap();
        for h in links.values() {
            h.close();
        }
    }
}

async fn accept_loop(inner: Arc<Inner>, listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            if inner.is_down() {
                return;
            }
            continue;
        };
        let inner2 = inner.clone();
        tokio::spawn(async move {
            let hooks = inner2.hooks();
            if let Ok(h) = Link::accept_with(stream, inner2.link_cfg(), hooks).await {
                inner2.register(h);
            }
        });
    }
}

async fn dial_loop(inner: Arc<Inner>) {
    loop {
        tokio::time::sleep(DIAL_INTERVAL).await;
        if inner.is_down() {
            return;
        }
        for addr in inner.dial_targets() {
            if !inner.dial_ready(addr) {
                continue;
            }
            let inner2 = inner.clone();
            tokio::spawn(async move {
                let hooks = inner2.hooks();
                match Link::connect_with(addr, inner2.link_cfg(), hooks).await {
                    Ok(h) => {
                        inner2.dial_ok(addr);
                        inner2.register(h);
                    }
                    Err(_) => inner2.dial_fail(addr),
                }
            });
        }
    }
}

async fn gossip_loop(inner: Arc<Inner>) {
    loop {
        tokio::time::sleep(GOSSIP_INTERVAL).await;
        if inner.is_down() {
            return;
        }
        let ups = {
            let mut m = inner.membership.lock().unwrap();
            m.take_piggyback(inner.params.gossip_max)
        };
        let frame = Frame::new(FrameKind::Membership, MembershipMsg::Gossip(ups).encode());
        for h in inner.all_links() {
            let _ = h.try_send(frame.clone());
        }
    }
}

async fn digest_loop(inner: Arc<Inner>) {
    loop {
        tokio::time::sleep(DIGEST_INTERVAL).await;
        if inner.is_down() {
            return;
        }
        let msg = {
            let m = inner.membership.lock().unwrap();
            membership_sync::build_digest(&m)
        };
        let frame = Frame::new(FrameKind::Membership, msg.encode());
        for h in inner.all_links() {
            let _ = h.try_send(frame.clone());
        }
    }
}
