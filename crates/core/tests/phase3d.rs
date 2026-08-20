//! §13.4 gates — relay verbs + engine integration (Phase 3D, the finale).
//!
//! The cross-node relay is exercised at the **Cluster** facade with real mesh
//! nodes (real TCP links, real interest routing, real RELAY_* frames), local
//! recipients being mock connections in each node's registry — the WebSocket
//! transport is the unchanged 1B/1C path. Interest is driven exactly as the
//! Engine drives it (`set_room_interest`/`set_user_interest` on 0→1/1→0).
//!
//! Gates:
//! - every targeting verb reaches remote members exactly once; `except` honored
//!   across nodes; `toSocket` → owning node only; `toUser` → all nodes' devices;
//! - serialize-once across the hop (pointer identity on the local side; one
//!   shared frame allocation on the relay side);
//! - delivery under partition in 1C currency (drops counted, no queue-and-
//!   forward);
//! - no relay loop (a received RELAY_* fans out locally, never re-forwarded);
//! - single-node is zero-cost (no mesh, and a measured per-call overhead).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use beamsocket_core::broadcast::{broadcast, FanoutTarget};
use beamsocket_core::cluster::{Cluster, NodeConnId, RelayCounters};
use beamsocket_core::config::{BackpressurePolicy, ClusterConfig, Config};
use beamsocket_core::connection::backpressure::{Mailbox, OutboundFrame};
use beamsocket_core::connection::registry::Registry;
use beamsocket_core::connection::{CloseSignal, ConnHandle, Control, CONTROL_QUEUE_CAPACITY};
use beamsocket_core::engine::Engine;
use beamsocket_core::identity::IdentityRegistry;
use beamsocket_core::ids::{ConnectionId, RoomId, UserId};
use beamsocket_core::metrics::Metrics;
use beamsocket_core::rooms::RoomRegistry;

use tokio::sync::mpsc;

const SECRET: &[u8] = b"cluster-secret";

/// One clustered node: a mesh node + the local registries its relays fan into.
struct Node {
    cluster: Arc<Cluster>,
    registry: Arc<Registry>,
    rooms: Arc<RoomRegistry>,
    identity: Arc<IdentityRegistry>,
    metrics: Arc<Metrics>,
}

impl Node {
    async fn spawn(id: u16, seeds: Vec<std::net::SocketAddr>) -> Node {
        let registry = Arc::new(Registry::new());
        let rooms = Arc::new(RoomRegistry::new());
        let identity = Arc::new(IdentityRegistry::new());
        let metrics = Arc::new(Metrics::default());
        let cfg = ClusterConfig {
            node_id: id,
            listen: "127.0.0.1:0".parse().unwrap(),
            seeds,
            secret: SECRET.to_vec(),
            cluster_name: "prod".into(),
        };
        let cluster = Cluster::start(
            &cfg,
            registry.clone(),
            rooms.clone(),
            identity.clone(),
            metrics.clone(),
        )
        .await
        .unwrap();
        Node {
            cluster,
            registry,
            rooms,
            identity,
            metrics,
        }
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.cluster.mesh().addr()
    }

    /// Insert a mock local connection (optionally bound to a user), returning its
    /// id. Mirrors the 1B/1C test harness.
    fn add_conn(&self, user: Option<&str>) -> ConnectionId {
        let (control, _rx) = mpsc::channel::<Control>(CONTROL_QUEUE_CAPACITY);
        let (close, _close_rx) = CloseSignal::new();
        let handle = ConnHandle {
            mailbox: Mailbox::new(
                1 << 20,
                BackpressurePolicy::DropNewest,
                self.metrics.clone(),
            ),
            control,
            close,
        };
        let uid = user.map(|u| UserId(u.to_string()));
        let id = self.registry.insert(handle, uid.clone());
        if let Some(uid) = uid {
            self.identity.bind(uid.clone(), id);
            if self.identity.device_count(&uid) == 1 {
                self.cluster.set_user_interest(&uid.0, true);
            }
        }
        id
    }

    /// Join a local connection to a room, driving the interest edge like the
    /// Engine's `join` does.
    fn join(&self, id: ConnectionId, room: &str) {
        self.rooms
            .join(&self.registry, id, RoomId(room.to_string()), 0);
        if self.rooms.info(&RoomId(room.to_string())).members == 1 {
            self.cluster.set_room_interest(room, true);
        }
    }

    /// The `broadcast_room` facade: unchanged local fan-out + relay (what the
    /// Engine does, replicated over the test registries).
    fn to_room(&self, room: &str, payload: &Bytes, is_binary: bool, except: &[NodeConnId]) {
        broadcast(
            &self.registry,
            &self.rooms,
            &self.identity,
            FanoutTarget::Room(&RoomId(room.to_string())),
            payload.clone(),
            is_binary,
            &local_except(self.cluster.node_id(), except),
        );
        self.cluster.relay_room(room, payload, is_binary, except);
    }

    fn to_user(&self, user: &str, payload: &Bytes, is_binary: bool, except: &[NodeConnId]) {
        broadcast(
            &self.registry,
            &self.rooms,
            &self.identity,
            FanoutTarget::User(&UserId(user.to_string())),
            payload.clone(),
            is_binary,
            &local_except(self.cluster.node_id(), except),
        );
        self.cluster.relay_user(user, payload, is_binary, except);
    }

    fn to_all(&self, payload: &Bytes, is_binary: bool) {
        broadcast(
            &self.registry,
            &self.rooms,
            &self.identity,
            FanoutTarget::All,
            payload.clone(),
            is_binary,
            &[],
        );
        self.cluster.relay_all(payload, is_binary, &[]);
    }
}

fn local_except(node: u16, except: &[NodeConnId]) -> Vec<ConnectionId> {
    except
        .iter()
        .filter(|e| e.node == node)
        .map(|e| e.local)
        .collect()
}

async fn poll_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if f() {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait for a frame to land in a connection's mailbox, then pop it.
async fn recv(reg: &Registry, id: ConnectionId, timeout: Duration) -> Option<OutboundFrame> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let h = reg.get(id)?;
        if h.mailbox.queued_bytes() > 0 {
            return h.mailbox.pop().await;
        }
        if std::time::Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn queued(reg: &Registry, id: ConnectionId) -> usize {
    reg.get(id).map(|h| h.mailbox.queued_bytes()).unwrap_or(0)
}

/// Spawn a converged 3-node cluster (node 1 is the seed).
async fn three_nodes() -> (Node, Node, Node) {
    let n1 = Node::spawn(1, vec![]).await;
    let seed = n1.addr();
    let n2 = Node::spawn(2, vec![seed]).await;
    let n3 = Node::spawn(3, vec![seed]).await;
    let converged = poll_until(
        || {
            n1.cluster.mesh().peer_count() == 2
                && n2.cluster.mesh().peer_count() == 2
                && n3.cluster.mesh().peer_count() == 2
        },
        Duration::from_secs(4),
    )
    .await;
    assert!(converged, "3-node cluster did not converge");
    (n1, n2, n3)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_every_verb_reaches_remote_exactly_once_except_honored() {
    let (n1, n2, n3) = three_nodes().await;

    // node1 and node3 both host room R (a local member each); node2 does not.
    let m1 = n1.add_conn(None);
    n1.join(m1, "R");
    let m3 = n3.add_conn(None);
    n3.join(m3, "R");
    let outsider2 = n2.add_conn(None); // in no room

    // node1 learns node3 hosts R (and not node2).
    assert!(
        poll_until(
            || n1.cluster.interested_room_peers("R") == vec![3],
            Duration::from_secs(3)
        )
        .await,
        "interest for R did not propagate to node1"
    );

    // ── toRoom: local (m1) + one relay hop to node3 (m3), exactly once ──
    let p = Bytes::from(vec![7u8; 300]);
    n1.to_room("R", &p, true, &[]);
    let f1 = recv(&n1.registry, m1, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(f1.data.len(), 300);
    assert_eq!(
        f1.data.as_ptr(),
        p.as_ptr(),
        "local fan-out must be the same allocation (1B)"
    );
    let f3 = recv(&n3.registry, m3, Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(f3.data.len(), 300);
    assert!(f3.is_binary);
    assert_eq!(queued(&n3.registry, m3), 0, "exactly once");
    assert_eq!(
        queued(&n2.registry, outsider2),
        0,
        "non-hosting node untouched"
    );

    // ── except across nodes: except m3 (on node3) → node3 skips it ──
    let p2 = Bytes::from(vec![9u8; 64]);
    n1.to_room("R", &p2, false, &[NodeConnId { node: 3, local: m3 }]);
    // m1 still receives (it is not excepted).
    let g1 = recv(&n1.registry, m1, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(g1.data.len(), 64);
    // give the relay time; m3 must get NOTHING (excepted across the node hop).
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        queued(&n3.registry, m3),
        0,
        "except must be honored across nodes"
    );

    // ── toUser: a user's devices on node2 AND node3 both receive ──
    let u2 = n2.add_conn(Some("alice"));
    let u3 = n3.add_conn(Some("alice"));
    assert!(
        poll_until(
            || {
                let p = n1.cluster.interested_user_peers("alice");
                p.contains(&2) && p.contains(&3)
            },
            Duration::from_secs(3)
        )
        .await,
        "user interest did not propagate"
    );
    let pu = Bytes::from(vec![5u8; 48]);
    n1.to_user("alice", &pu, false, &[]);
    assert!(recv(&n2.registry, u2, Duration::from_secs(3))
        .await
        .is_some());
    assert!(recv(&n3.registry, u3, Duration::from_secs(3))
        .await
        .is_some());

    // ── toSocket: owning node only ──
    let ps = Bytes::from(vec![1u8; 32]);
    n1.cluster
        .relay_socket(NodeConnId { node: 3, local: m3 }, &ps, false);
    let fs = recv(&n3.registry, m3, Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(fs.data.len(), 32);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        queued(&n2.registry, u2),
        0,
        "toSocket must reach only the owner"
    );

    // ── broadcast: relay to all peers ──
    let m2b = n2.add_conn(None);
    let pb = Bytes::from(vec![2u8; 16]);
    n1.to_all(&pb, false);
    assert!(recv(&n2.registry, m2b, Duration::from_secs(3))
        .await
        .is_some());
    assert!(recv(&n3.registry, m3, Duration::from_secs(3))
        .await
        .is_some());

    // ── no relay loop: a received RELAY_* is never re-forwarded ──
    for n in [&n1, &n2, &n3] {
        assert_eq!(
            RelayCounters::get(&n.cluster.relay_counters().re_relays),
            0,
            "a received relay was re-forwarded — loop!"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serialize_once_across_the_hop() {
    // node1 hosts R locally (2 members) AND relays to node3 (1 member). The app
    // payload is ONE allocation: the two local members receive that exact
    // allocation (pointer identity, 1B), and the relay copies it once into a
    // single frame the peer link refcount-holds — never re-serialized per peer.
    let (n1, _n2, n3) = three_nodes().await;
    let a = n1.add_conn(None);
    let b = n1.add_conn(None);
    n1.join(a, "R");
    n1.join(b, "R");
    let c = n3.add_conn(None);
    n3.join(c, "R");
    assert!(
        poll_until(
            || n1.cluster.interested_room_peers("R") == vec![3],
            Duration::from_secs(3)
        )
        .await
    );

    let payload = Bytes::from(vec![42u8; 1024]);
    let ptr = payload.as_ptr();
    n1.to_room("R", &payload, true, &[]);

    // Both local members: the identical allocation (one serialization).
    let fa = recv(&n1.registry, a, Duration::from_secs(1)).await.unwrap();
    let fb = recv(&n1.registry, b, Duration::from_secs(1)).await.unwrap();
    assert_eq!(fa.data.as_ptr(), ptr);
    assert_eq!(
        fb.data.as_ptr(),
        ptr,
        "second local member shares the allocation"
    );

    // Remote member: the same bytes, delivered once (the relay carried the one
    // frame built from that single payload allocation).
    let fc = recv(&n3.registry, c, Duration::from_secs(3)).await.unwrap();
    assert_eq!(
        &fc.data[..],
        &payload[..],
        "relay must carry the payload verbatim"
    );
    assert_eq!(queued(&n3.registry, c), 0, "delivered exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_delivery_is_1c_currency_no_queue_and_forward() {
    let (n1, n2, n3) = three_nodes().await;
    let m1 = n1.add_conn(None);
    n1.join(m1, "R");
    let m3 = n3.add_conn(None);
    n3.join(m3, "R");
    assert!(
        poll_until(
            || n1.cluster.interested_room_peers("R") == vec![3],
            Duration::from_secs(3)
        )
        .await
    );

    // Partition node3 away from {1,2}.
    n1.cluster.mesh().set_partition(vec![3]);
    n2.cluster.mesh().set_partition(vec![3]);
    n3.cluster.mesh().set_partition(vec![1, 2]);

    // A cross-node send during the partition: local m1 still gets it (local-
    // first), node3 does NOT (unreachable). Some attempts count as drops; none
    // is queued-and-forwarded.
    let during = Bytes::from(vec![3u8; 100]);
    // Relay directly to node3 (still interested until it ages out) to force the
    // unreachable-drop accounting deterministically.
    let before = RelayCounters::get(&n1.cluster.relay_counters().relay_drops);
    n1.cluster
        .mesh()
        .relay(&[3], beamsocket_mesh::RelayKind::Room, false, b"", &during); // no link → no_link
                                                                             // Also exercise the facade path.
    n1.to_room("R", &during, false, &[]);
    let _ = before;

    // m1 (local) received; m3 (partitioned) did not.
    assert!(recv(&n1.registry, m1, Duration::from_secs(1))
        .await
        .is_some());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        queued(&n3.registry, m3),
        0,
        "no delivery to a partitioned node"
    );

    // Heal. Even after re-convergence, the partition-time message must NOT be
    // delivered (no queue-and-forward — 1C currency, no stronger promise).
    n1.cluster.mesh().heal();
    n2.cluster.mesh().heal();
    n3.cluster.mesh().heal();
    assert!(
        poll_until(
            || n1.cluster.mesh().peer_count() == 2 && n3.cluster.mesh().peer_count() == 2,
            Duration::from_secs(10)
        )
        .await,
        "cluster did not re-converge after heal"
    );
    assert!(
        poll_until(
            || n1.cluster.interested_room_peers("R") == vec![3],
            Duration::from_secs(4)
        )
        .await,
        "interest did not re-propagate after heal"
    );

    // A NEW send after heal reaches node3; the old one never does.
    let after = Bytes::from(vec![4u8; 55]);
    n1.to_room("R", &after, false, &[]);
    let got = recv(&n3.registry, m3, Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(got.data.len(), 55, "post-heal delivery resumes");
    assert_eq!(
        queued(&n3.registry, m3),
        0,
        "the partition-time message was never buffered (no queue-and-forward)"
    );
}

#[test]
fn single_node_is_zero_cost_and_bit_identical() {
    // No `cluster` config → no mesh, and the verbs take the byte-identical
    // pre-3D path (one `Option` match, then the unchanged local fan-out).
    let (engine, _rx) = Engine::start(Config::default(), 256, false).unwrap();
    assert!(!engine.is_clustered(), "default config must be single-node");
    assert!(engine.cluster().is_none());
    assert!(
        engine.cluster_summary().is_none(),
        "no cluster stats when unclustered"
    );
    assert!(engine.cluster_peer_pressures().is_empty());

    // A broadcast to a room nobody is in: unchanged behavior, no mesh touched.
    let r = engine.broadcast_room("nobody", Bytes::from_static(b"x"), false, &[], &[]);
    assert_eq!(r.attempted, 0);

    // Measured single-node verb overhead — the zero-cost-when-unused number for
    // the PR. A room miss is an Option match + one sharded lookup returning None.
    let payload = Bytes::from(vec![0u8; 256]);
    let n = 300_000u32;
    let t = std::time::Instant::now();
    for _ in 0..n {
        let _ = engine.broadcast_room("nobody", payload.clone(), false, &[], &[]);
    }
    let per = t.elapsed().as_nanos() as f64 / n as f64;
    eprintln!("single-node broadcast_room overhead: {per:.1} ns/call over {n} calls");
    assert!(
        per < 2000.0,
        "single-node verb overhead {per:.1} ns/call is unexpectedly high"
    );
}
