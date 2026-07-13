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

use bytes::Bytes;
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

use crate::config::{Backoff, LinkConfig};
use crate::frame::{Flags, Frame, FrameKind};
use crate::handshake::features;
use crate::interest::{self, InterestCounters, InterestState, InterestUpdate, Routing, Target};
use crate::link::{CloseHandler, InboundHandler, Link, LinkHandle, LinkHooks};
use crate::membership_sync::{self, MembershipMsg};
use crate::probe::{ProbeCounters, ProbePlane};
use crate::queue::PushOutcome;
use crate::swim::{MState, MemberInfo, Membership, MembershipCounters, SwimParams};

/// How often the node dials missing peers, spreads gossip, and (a tenth as
/// often) runs anti-entropy. Fast enough for the <2 s convergence gate; slow
/// enough to be control-plane cheap for N ≤ 50.
const DIAL_INTERVAL: Duration = Duration::from_millis(300);
const GOSSIP_INTERVAL: Duration = Duration::from_millis(300);
const DIGEST_INTERVAL: Duration = Duration::from_millis(1000);

/// Which targeting verb a relayed frame carries (§4.3). The mesh moves the
/// bytes; **core owns the body format and the local fan-out** — the mesh never
/// interprets a relay payload (it carries frames, not state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayKind {
    Room,
    User,
    All,
    Socket,
}

impl RelayKind {
    fn frame_kind(self) -> FrameKind {
        match self {
            RelayKind::Room => FrameKind::RelayRoom,
            RelayKind::User => FrameKind::RelayUser,
            RelayKind::All => FrameKind::RelayAll,
            RelayKind::Socket => FrameKind::RelaySocket,
        }
    }
    fn from_frame_kind(k: FrameKind) -> Option<RelayKind> {
        Some(match k {
            FrameKind::RelayRoom => RelayKind::Room,
            FrameKind::RelayUser => RelayKind::User,
            FrameKind::RelayAll => RelayKind::All,
            FrameKind::RelaySocket => RelayKind::Socket,
            _ => return None,
        })
    }
}

/// Called when a RELAY_* frame arrives, with `(kind, is_binary, body)`. Core's
/// [`crate::node`] consumer decodes the body and fans out to **local** recipients
/// only — a received relay is **never re-forwarded** (loop prevention, §4.3).
/// Runs on the link reader task; keep it cheap.
pub type RelayHandler = Arc<dyn Fn(RelayKind, bool, Vec<u8>) + Send + Sync>;

/// The result of relaying one frame to a set of peers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RelaySendReport {
    /// Peers the frame was enqueued to.
    pub sent: u64,
    /// Peers whose bounded queue was full (drop-and-count, §4.6).
    pub dropped: u64,
    /// Peers whose link had not negotiated the RELAY feature (suppressed).
    pub suppressed: u64,
    /// Interested peers with **no live link** — unreachable (a partition in
    /// flight). Dropped-and-counted, no queue-and-forward (1C currency, §13.4).
    pub no_link: u64,
}

/// Config for one mesh node (the link-layer view of §5's `cluster` block).
#[derive(Debug, Clone)]
pub struct MeshConfig {
    pub node_id: u16,
    pub listen: SocketAddr,
    pub seeds: Vec<SocketAddr>,
    pub secret: Vec<u8>,
    pub cluster_name: String,
    pub params: SwimParams,
    /// Routing mode (§4.3). Defaults to `Interest`; `Flood` is the operational
    /// fallback lever, never the default.
    pub routing: Routing,
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
            routing: Routing::Interest,
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
    interest: Arc<Mutex<InterestState>>,
    links: Mutex<HashMap<u16, LinkHandle>>,
    deny: Arc<Mutex<HashSet<u16>>>,
    dial_backoff: Mutex<HashMap<SocketAddr, (u32, Instant)>>,
    backoff: Backoff,
    shutdown: AtomicBool,
    /// Set by core (3D) to receive RELAY_* frames for local fan-out. `None` for
    /// a pure-membership node (3B/3C tests).
    relay_handler: Option<RelayHandler>,
}

impl Inner {
    fn link_cfg(&self) -> LinkConfig {
        let mut c = LinkConfig::new(self.self_id, self.cluster_name.clone(), self.secret.clone());
        // Advertise interest routing + relay so INTEREST/INTEREST_DIGEST and
        // RELAY_* frames pass the 3A feature-intersection + sender-suppression
        // checks on the link.
        c.features = features::INTEREST_ROUTING | features::RELAY;
        c
    }

    fn alive_peer_ids(&self) -> HashSet<u16> {
        self.membership
            .lock()
            .unwrap()
            .table()
            .into_iter()
            .filter(|mi| mi.state == MState::Alive)
            .map(|mi| mi.id)
            .collect()
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
        if self.deny.lock().unwrap().contains(&peer) {
            return;
        }
        match frame.kind {
            FrameKind::Membership => {
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
            FrameKind::Interest => {
                let Some(update) = InterestUpdate::decode(&frame.body) else {
                    return;
                };
                let mut i = self.interest.lock().unwrap();
                match update {
                    InterestUpdate::Edge(e) => {
                        i.apply_edge(&e);
                    }
                    InterestUpdate::Snapshot(s) => {
                        i.apply_snapshot(&s);
                    }
                }
            }
            FrameKind::InterestDigest => {
                let Some(digest) = interest::decode_digest(&frame.body) else {
                    return;
                };
                let snaps = self.interest.lock().unwrap().respond_to_digest(&digest);
                for s in snaps {
                    self.send_interest(peer, &InterestUpdate::Snapshot(s));
                }
            }
            FrameKind::RelayRoom
            | FrameKind::RelayUser
            | FrameKind::RelayAll
            | FrameKind::RelaySocket => {
                // Hand to core for LOCAL fan-out. The mesh never re-forwards a
                // received relay (loop prevention, §4.3) — there is no send here.
                if let (Some(h), Some(kind)) =
                    (&self.relay_handler, RelayKind::from_frame_kind(frame.kind))
                {
                    h(kind, frame.flags.has(Flags::BINARY), frame.body.clone());
                }
            }
            _ => {}
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

    fn send_interest(&self, peer: u16, update: &InterestUpdate) {
        let handle = self.links.lock().unwrap().get(&peer).cloned();
        if let Some(h) = handle {
            let _ = h.try_send(Frame::new(FrameKind::Interest, update.encode()));
        }
    }

    /// Broadcast an already-encoded frame to every current link.
    fn broadcast_frame(&self, frame: Frame) {
        for h in self.all_links() {
            let _ = h.try_send(frame.clone());
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

        // Full interest exchange on link-up (§4.3, same shape as the membership
        // push-pull): send our local interest snapshot so the new peer learns
        // what we host immediately; the digest converges the rest.
        let snap = self.interest.lock().unwrap().local_snapshot();
        self.send_interest(peer, &InterestUpdate::Snapshot(snap));
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
    /// Start a node with no relay handler (a pure membership/interest node — the
    /// 3B/3C tests). 3D uses [`MeshNode::start_with_relay`].
    pub async fn start(config: MeshConfig) -> std::io::Result<Arc<MeshNode>> {
        Self::start_with_relay(config, None).await
    }

    /// Start a node, registering `relay_handler` to receive RELAY_* frames for
    /// local fan-out (3D). The mesh carries the frames; core owns the payload.
    pub async fn start_with_relay(
        config: MeshConfig,
        relay_handler: Option<RelayHandler>,
    ) -> std::io::Result<Arc<MeshNode>> {
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

        let interest = Arc::new(Mutex::new(InterestState::new(
            config.node_id,
            config.routing,
        )));

        let inner = Arc::new(Inner {
            self_id: config.node_id,
            addr,
            cluster_name: config.cluster_name,
            secret: config.secret,
            params: config.params,
            seeds: config.seeds,
            membership,
            interest,
            links: Mutex::new(HashMap::new()),
            deny,
            dial_backoff: Mutex::new(HashMap::new()),
            backoff: Backoff::default(),
            shutdown: AtomicBool::new(false),
            relay_handler,
        });

        let tasks = vec![
            tokio::spawn(accept_loop(inner.clone(), listener)),
            tokio::spawn(dial_loop(inner.clone())),
            tokio::spawn(gossip_loop(inner.clone())),
            tokio::spawn(digest_loop(inner.clone())),
            tokio::spawn(interest_loop(inner.clone())),
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

    // ── interest routing (3C) ──

    /// Note a local hosting transition (0→1 `hosting=true`, 1→0 `false`) for a
    /// room/user. **This is the seam 3D's engine drives** from the local
    /// room/identity registries; in 3C a test double calls it. On a real
    /// transition the edge is disseminated to every peer (edge-triggered, §4.3).
    pub fn set_local_interest(&self, target: Target, hosting: bool) {
        let edge = self
            .inner
            .interest
            .lock()
            .unwrap()
            .local_set(target, hosting);
        if let Some(edge) = edge {
            let frame = Frame::new(FrameKind::Interest, InterestUpdate::Edge(edge).encode());
            self.inner.broadcast_frame(frame);
        }
    }

    /// **The routing seam 3D consumes:** the remote peers to relay `target` to.
    /// Empty = no relay (nobody remote hosts it). Unreachable peers are excluded;
    /// in `Flood` mode every alive peer is returned.
    pub fn interested_peers(&self, target: &Target) -> Vec<u16> {
        let alive = self.inner.alive_peer_ids();
        self.inner
            .interest
            .lock()
            .unwrap()
            .interested_peers(target, &alive)
    }

    pub fn interest_counters(&self) -> InterestCounters {
        self.inner.interest.lock().unwrap().counters()
    }

    /// Flip the routing lever at runtime (interest ⇄ flood).
    pub fn set_routing(&self, routing: Routing) {
        self.inner.interest.lock().unwrap().set_routing(routing);
    }

    // ── relay send (3D) ──

    /// Build a RELAY frame **once** (`[len][kind][flags][metadata][payload]`) and
    /// refcount-clone it to each of `peers` (§4.6 serialize-once across the hop):
    /// the **payload is copied exactly once** into the frame `Bytes`, then only
    /// its refcount is bumped per peer. `metadata` is the small per-verb prefix
    /// (room/user name, except list) core assembled; `payload` is the app
    /// message the SDK serialized once. A peer with no link, a full queue, or no
    /// RELAY feature is tallied, never fatal.
    pub fn relay(
        &self,
        peers: &[u16],
        kind: RelayKind,
        is_binary: bool,
        metadata: &[u8],
        payload: &[u8],
    ) -> RelaySendReport {
        let frame_kind = kind.frame_kind();
        let flags = if is_binary { Flags::BINARY } else { 0 };
        let body_len = metadata.len() + payload.len();
        let mut buf = Vec::with_capacity(6 + body_len);
        let len = (2 + body_len) as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.push(frame_kind.as_u8());
        buf.push(flags);
        buf.extend_from_slice(metadata);
        buf.extend_from_slice(payload);
        let frame = Bytes::from(buf);

        // Snapshot each peer's handle (or None if unreachable), drop the links
        // lock, then enqueue (never hold two locks across the pushes).
        let handles: Vec<Option<LinkHandle>> = {
            let links = self.inner.links.lock().unwrap();
            peers.iter().map(|p| links.get(p).cloned()).collect()
        };
        let mut report = RelaySendReport::default();
        for h in handles {
            match h {
                None => report.no_link += 1,
                Some(h) => match h.try_send_encoded(frame_kind, frame.clone()) {
                    Ok(PushOutcome::Enqueued) => report.sent += 1,
                    Ok(PushOutcome::Dropped) => report.dropped += 1,
                    Err(_) => report.suppressed += 1,
                },
            }
        }
        report
    }

    /// Current peer count (Alive links) — a `stats().cluster` field.
    pub fn peer_count(&self) -> usize {
        self.inner.links.lock().unwrap().len()
    }

    /// `(nodeId, pressure)` for each live link — the §4.6 per-peer gauge for
    /// `stats().cluster.peers[]`.
    pub fn peer_pressures(&self) -> Vec<(u16, f64)> {
        let mut v: Vec<(u16, f64)> = self
            .inner
            .links
            .lock()
            .unwrap()
            .iter()
            .map(|(id, h)| (*id, h.pressure()))
            .collect();
        v.sort_by_key(|(id, _)| *id);
        v
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

/// Interest anti-entropy: sweep evicted peers' interest (no stuck entries, the
/// 3B lesson) and send the interest digest so any dropped edge self-heals.
async fn interest_loop(inner: Arc<Inner>) {
    loop {
        tokio::time::sleep(DIGEST_INTERVAL).await;
        if inner.is_down() {
            return;
        }
        // Sweep interest for any origin not currently Alive in membership.
        let alive = inner.alive_peer_ids();
        {
            let mut i = inner.interest.lock().unwrap();
            for id in i.known_origins() {
                if !alive.contains(&id) {
                    i.sweep_origin(id);
                }
            }
        }
        let digest = inner.interest.lock().unwrap().build_digest();
        let frame = Frame::new(FrameKind::InterestDigest, interest::encode_digest(&digest));
        for h in inner.all_links() {
            let _ = h.try_send(frame.clone());
        }
    }
}
