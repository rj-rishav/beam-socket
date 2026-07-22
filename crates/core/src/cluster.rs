//! Cluster integration (Phase 3D, RFC 0004 §4.3/§4.5/§4.6) — the seam where
//! `crates/mesh` attaches to the Engine.
//!
//! The Engine holds an `Option<Arc<Cluster>>`. **`None` is single-node**: no
//! mesh, no relay, no cost — the whole of this module is dead weight the linker
//! keeps but the runtime never touches (§12 rule 1 applied to the mesh).
//!
//! When present, a targeting verb does its **unchanged local fan-out** (1B/1C)
//! and then, if any remote node hosts the target, relays the payload once to
//! those nodes via a RELAY_* frame. The receive side fans out to LOCAL
//! recipients only — a received relay is **never re-forwarded** (loop
//! prevention: the origin already sent to every interested node). The app
//! payload is serialized once (at the FFI boundary) and shared by refcount:
//! local recipients clone the `Bytes`, and the relay copies it once into a
//! single frame `Bytes` that every peer link refcount-clones (§4.6).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use beamsocket_mesh::{MeshConfig, MeshNode, RelayHandler, RelayKind, SwimParams, Target};

use crate::broadcast::{broadcast, FanoutTarget};
use crate::config::ClusterConfig;
use crate::connection::backpressure::OutboundFrame;
use crate::connection::registry::Registry;
use crate::identity::IdentityRegistry;
use crate::ids::{ConnectionId, RoomId, UserId};
use crate::metrics::Metrics;
use crate::rooms::RoomRegistry;

/// A node-scoped connection id (§4.5): `(node, local)`. The public `socket.id`
/// string encodes this; here it is the wire form for `toSocket` targets and for
/// cross-node `except` filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeConnId {
    pub node: u16,
    pub local: ConnectionId,
}

/// Relay counters — surfaced in `stats().cluster` (Phase 2A discipline).
#[derive(Debug, Default)]
pub struct RelayCounters {
    /// Relay frames enqueued to peers (summed across peers).
    pub relay_out: AtomicU64,
    /// Relay frames received and fanned out locally.
    pub relay_in: AtomicU64,
    /// Relay frames shed by a peer's full outbound queue (§4.6).
    pub relay_drops: AtomicU64,
    /// Received relays that were re-forwarded — **must stay 0** (loop
    /// prevention). A nonzero value is a bug, asserted in the gate test.
    pub re_relays: AtomicU64,
}

impl RelayCounters {
    fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }
}

/// The cluster attached to an Engine. Owns the mesh node and the local
/// registries the relay receive-side fans out into.
pub struct Cluster {
    node: Arc<MeshNode>,
    self_node_id: u16,
    relay: Arc<RelayCounters>,
}

impl Cluster {
    /// Boot the mesh and register the relay receive handler. Async — the Engine
    /// calls it inside its runtime.
    pub async fn start(
        cfg: &ClusterConfig,
        registry: Arc<Registry>,
        rooms: Arc<RoomRegistry>,
        identity: Arc<IdentityRegistry>,
        metrics: Arc<Metrics>,
    ) -> std::io::Result<Arc<Cluster>> {
        let self_node_id = cfg.node_id;
        let relay = Arc::new(RelayCounters::default());

        // The receive handler: decode the relay body and fan out to LOCAL
        // recipients only. Captures the registries by refcount; never relays.
        let reg = registry.clone();
        let rms = rooms.clone();
        let idn = identity.clone();
        let met = metrics.clone();
        let rc = relay.clone();
        let handler: RelayHandler = Arc::new(move |kind, is_binary, body| {
            deliver_local(
                &reg,
                &rms,
                &idn,
                &met,
                &rc,
                self_node_id,
                kind,
                is_binary,
                &body,
            );
        });

        let mut mesh_cfg = MeshConfig::new(cfg.node_id, cfg.listen, cfg.secret.clone());
        mesh_cfg.seeds = cfg.seeds.clone();
        mesh_cfg.cluster_name = cfg.cluster_name.clone();
        mesh_cfg.params = SwimParams::tuned();

        let node = MeshNode::start_with_relay(mesh_cfg, Some(handler)).await?;
        Ok(Arc::new(Cluster {
            node,
            self_node_id,
            relay,
        }))
    }

    pub fn node_id(&self) -> u16 {
        self.self_node_id
    }
    pub fn mesh(&self) -> &Arc<MeshNode> {
        &self.node
    }
    pub fn relay_counters(&self) -> &Arc<RelayCounters> {
        &self.relay
    }
    /// Remote peers hosting `room` (the routing decision for `toRoom`).
    pub fn interested_room_peers(&self, room: &str) -> Vec<u16> {
        self.node.interested_peers(&Target::Room(room.to_string()))
    }
    /// Remote peers hosting `user` (the routing decision for `toUser`).
    pub fn interested_user_peers(&self, user: &str) -> Vec<u16> {
        self.node.interested_peers(&Target::User(user.to_string()))
    }

    // ── interest (edge-triggered; the Engine calls these on real transitions) ──

    /// The local host set now includes (or excludes) `room`.
    pub fn set_room_interest(&self, room: &str, hosting: bool) {
        self.node
            .set_local_interest(Target::Room(room.to_string()), hosting);
    }
    /// The local host set now includes (or excludes) `user`.
    pub fn set_user_interest(&self, user: &str, hosting: bool) {
        self.node
            .set_local_interest(Target::User(user.to_string()), hosting);
    }

    // ── relay send (called AFTER local fan-out) ──

    pub fn relay_room(&self, room: &str, payload: &Bytes, is_binary: bool, except: &[NodeConnId]) {
        let peers = self.node.interested_peers(&Target::Room(room.to_string()));
        if peers.is_empty() {
            return;
        }
        let mut meta = Vec::new();
        put_str(&mut meta, room);
        put_excepts(&mut meta, except);
        self.do_relay(&peers, RelayKind::Room, is_binary, &meta, payload);
    }

    pub fn relay_user(&self, user: &str, payload: &Bytes, is_binary: bool, except: &[NodeConnId]) {
        let peers = self.node.interested_peers(&Target::User(user.to_string()));
        if peers.is_empty() {
            return;
        }
        let mut meta = Vec::new();
        put_str(&mut meta, user);
        put_excepts(&mut meta, except);
        self.do_relay(&peers, RelayKind::User, is_binary, &meta, payload);
    }

    /// Broadcast: relay to **all** live peers (interest is definitionally
    /// everyone).
    pub fn relay_all(&self, payload: &Bytes, is_binary: bool, except: &[NodeConnId]) {
        let peers: Vec<u16> = self
            .node
            .peer_pressures()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        if peers.is_empty() {
            return;
        }
        let mut meta = Vec::new();
        put_excepts(&mut meta, except);
        self.do_relay(&peers, RelayKind::All, is_binary, &meta, payload);
    }

    /// `toSocket` to a remote id: route to the owning node only (§4.5). A local
    /// target (node == self) is handled by the Engine's existing `send` and does
    /// not reach here.
    pub fn relay_socket(&self, target: NodeConnId, payload: &Bytes, is_binary: bool) {
        if target.node == self.self_node_id {
            return;
        }
        let meta = target.local.0.to_le_bytes();
        self.do_relay(&[target.node], RelayKind::Socket, is_binary, &meta, payload);
    }

    fn do_relay(
        &self,
        peers: &[u16],
        kind: RelayKind,
        is_binary: bool,
        meta: &[u8],
        payload: &Bytes,
    ) {
        let report = self.node.relay(peers, kind, is_binary, meta, payload);
        RelayCounters::add(&self.relay.relay_out, report.sent);
        // A full peer queue OR an unreachable interested peer is a drop — 1C
        // currency, no queue-and-forward (§13.4).
        RelayCounters::add(&self.relay.relay_drops, report.dropped + report.no_link);
    }
}

/// Fan a received relay out to LOCAL recipients only — never re-forwarded.
#[allow(clippy::too_many_arguments)]
fn deliver_local(
    registry: &Arc<Registry>,
    rooms: &Arc<RoomRegistry>,
    identity: &Arc<IdentityRegistry>,
    metrics: &Arc<Metrics>,
    relay: &Arc<RelayCounters>,
    self_node: u16,
    kind: RelayKind,
    is_binary: bool,
    body: &[u8],
) {
    RelayCounters::add(&relay.relay_in, 1);
    let _ = metrics; // reserved for a per-relay metric; counters above suffice
    match kind {
        RelayKind::Room => {
            let mut off = 0;
            let Some(room) = get_str(body, &mut off) else {
                return;
            };
            let Some(excepts) = get_excepts(body, &mut off) else {
                return;
            };
            let payload = Bytes::copy_from_slice(&body[off..]);
            let local_except = excepts_for(self_node, &excepts);
            broadcast(
                registry,
                rooms,
                identity,
                FanoutTarget::Room(&RoomId(room)),
                payload,
                is_binary,
                &local_except,
            );
        }
        RelayKind::User => {
            let mut off = 0;
            let Some(user) = get_str(body, &mut off) else {
                return;
            };
            let Some(excepts) = get_excepts(body, &mut off) else {
                return;
            };
            let payload = Bytes::copy_from_slice(&body[off..]);
            let local_except = excepts_for(self_node, &excepts);
            broadcast(
                registry,
                rooms,
                identity,
                FanoutTarget::User(&UserId(user)),
                payload,
                is_binary,
                &local_except,
            );
        }
        RelayKind::All => {
            let mut off = 0;
            let Some(excepts) = get_excepts(body, &mut off) else {
                return;
            };
            let payload = Bytes::copy_from_slice(&body[off..]);
            let local_except = excepts_for(self_node, &excepts);
            broadcast(
                registry,
                rooms,
                identity,
                FanoutTarget::All,
                payload,
                is_binary,
                &local_except,
            );
        }
        RelayKind::Socket => {
            if body.len() < 8 {
                return;
            }
            let local = u64::from_le_bytes(body[0..8].try_into().unwrap());
            let payload = Bytes::copy_from_slice(&body[8..]);
            if let Some(handle) = registry.get(ConnectionId(local)) {
                handle.mailbox.push(OutboundFrame {
                    data: payload,
                    is_binary,
                });
            }
        }
    }
}

// ── wire helpers (core owns the relay body format) ──

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

fn put_excepts(buf: &mut Vec<u8>, excepts: &[NodeConnId]) {
    buf.extend_from_slice(&(excepts.len() as u16).to_le_bytes());
    for e in excepts {
        buf.extend_from_slice(&e.node.to_le_bytes());
        buf.extend_from_slice(&e.local.0.to_le_bytes());
    }
}

fn get_excepts(buf: &[u8], off: &mut usize) -> Option<Vec<NodeConnId>> {
    let n = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let node = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?);
        *off += 2;
        let local = u64::from_le_bytes(buf.get(*off..*off + 8)?.try_into().ok()?);
        *off += 8;
        v.push(NodeConnId {
            node,
            local: ConnectionId(local),
        });
    }
    Some(v)
}

/// The excepts that apply to *this* node — a remote node ignores excepts naming
/// other nodes' connections.
fn excepts_for(self_node: u16, excepts: &[NodeConnId]) -> Vec<ConnectionId> {
    excepts
        .iter()
        .filter(|e| e.node == self_node)
        .map(|e| e.local)
        .collect()
}
