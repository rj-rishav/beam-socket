//! §13.3 interest-routing gates.
//!
//! - **Routing correctness vs a flood reference model, under proptest churn**
//!   (add/remove/partition/heal with dropped edges): after convergence,
//!   `interested_peers(R)` on every node must equal the set of nodes that host
//!   R in the flood model — interest must **never under-deliver** relative to
//!   flood (a miss = a silently dropped cross-node message in 3D).
//! - **Byte-reduction cell** re-measured (50 rooms/node, 10% cross-node).
//! - **Digest repairs** a dropped edge (exercised heavily by the proptest, which
//!   drops edges and relies on the digest to converge).
//! - **Flood lever** returns all peers; **partition** ages interest out.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beamsocket_mesh::interest::{InterestState, Routing, Target};
use beamsocket_mesh::{MeshConfig, MeshNode, SwimParams};

use proptest::prelude::*;

// ─────────────────────────── the flood reference model ───────────────────────

/// A synchronous cluster of interest tables with a lossy wire, used to check
/// routing against ground truth. Rooms only (users are symmetric).
struct Sim {
    n: usize,
    nrooms: usize,
    states: Vec<InterestState>,
    /// Ground truth: `hosting[node][room]`.
    hosting: Vec<Vec<bool>>,
    /// Partition groups: reachable iff same group.
    group: Vec<usize>,
}

impl Sim {
    fn new(n: usize, nrooms: usize) -> Self {
        Self {
            n,
            nrooms,
            states: (0..n)
                .map(|i| InterestState::new(i as u16, Routing::Interest))
                .collect(),
            hosting: vec![vec![false; nrooms]; n],
            group: vec![0; n],
        }
    }

    fn room(r: usize) -> Target {
        Target::Room(format!("r{r}"))
    }

    /// A hosting transition on a node; the edge is delivered to reachable peers
    /// except those whose bit is set in `drop_mask` (simulated loss — the digest
    /// must repair those).
    fn set(&mut self, node: usize, room: usize, add: bool, drop_mask: u16) {
        self.hosting[node][room] = add;
        let edge = self.states[node].local_set(Self::room(room), add);
        if let Some(edge) = edge {
            for b in 0..self.n {
                if b != node && self.group[b] == self.group[node] && (drop_mask >> b) & 1 == 0 {
                    self.states[b].apply_edge(&edge);
                }
            }
        }
    }

    fn partition(&mut self, mask: u16) {
        for i in 0..self.n {
            self.group[i] = ((mask >> i) & 1) as usize;
        }
    }

    fn heal(&mut self) {
        for g in &mut self.group {
            *g = 0;
        }
    }

    /// Run anti-entropy digests across all reachable pairs until convergence.
    fn settle(&mut self) {
        for _ in 0..(self.n * 4 + 4) {
            for a in 0..self.n {
                for b in 0..self.n {
                    if a != b && self.group[a] == self.group[b] {
                        let digest = self.states[a].build_digest();
                        let snaps = self.states[b].respond_to_digest(&digest);
                        for s in snaps {
                            self.states[a].apply_snapshot(&s);
                        }
                    }
                }
            }
        }
    }

    /// After a full heal + settle, every node's interest routing must equal the
    /// flood ground truth — never miss a host.
    fn assert_matches_flood(&self) {
        for i in 0..self.n {
            let alive: HashSet<u16> = (0..self.n as u16).filter(|j| *j != i as u16).collect();
            for r in 0..self.nrooms {
                let got = self.states[i].interested_peers(&Self::room(r), &alive);
                let expected: Vec<u16> = (0..self.n)
                    .filter(|j| *j != i && self.hosting[*j][r])
                    .map(|j| j as u16)
                    .collect();
                assert_eq!(
                    got, expected,
                    "node {i} room r{r}: interest {got:?} != flood {expected:?}"
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Set {
        node: usize,
        room: usize,
        add: bool,
        drop_mask: u16,
    },
    Partition(u16),
    Heal,
}

fn op_strategy(n: usize, nrooms: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0..n, 0..nrooms, any::<bool>(), any::<u16>())
            .prop_map(|(node, room, add, drop_mask)| Op::Set { node, room, add, drop_mask }),
        2 => any::<u16>().prop_map(Op::Partition),
        2 => Just(Op::Heal),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    /// THE gate: interest routing == flood model after churn + edge loss.
    #[test]
    fn interested_peers_never_under_delivers_vs_flood(
        ops in prop::collection::vec(op_strategy(4, 6), 0..50)
    ) {
        let mut sim = Sim::new(4, 6);
        for op in ops {
            match op {
                Op::Set { node, room, add, drop_mask } => sim.set(node, room, add, drop_mask),
                Op::Partition(m) => sim.partition(m),
                Op::Heal => sim.heal(),
            }
        }
        sim.heal();
        sim.settle();
        sim.assert_matches_flood();
    }
}

// ─────────────────────────── byte-reduction cell ─────────────────────────────

#[test]
fn byte_reduction_vs_flood_cell() {
    // The spike's cell (0004-results.md): 50 rooms/node × 5 nodes, 10% cross-node
    // (every 10th of a node's rooms is also hosted on the next node), 20 msgs ×
    // 512 B per room. Measure inter-node bytes: interest vs flood.
    const N: usize = 5;
    const ROOMS_PER_NODE: usize = 50;
    const MSGS: usize = 20;
    const PAYLOAD: usize = 512;

    let mut states: Vec<InterestState> = (0..N)
        .map(|i| InterestState::new(i as u16, Routing::Interest))
        .collect();

    // Advertise: node i hosts its 50 rooms; every 10th is ALSO on the next node.
    let mut edges = Vec::new();
    let mut owned: Vec<Vec<String>> = vec![Vec::new(); N];
    for i in 0..N {
        for k in 0..ROOMS_PER_NODE {
            let room = format!("n{i}-r{k}");
            owned[i].push(room.clone());
            if let Some(e) = states[i].local_set(Target::Room(room.clone()), true) {
                edges.push(e);
            }
            if k % 10 == 0 {
                let nxt = (i + 1) % N;
                if let Some(e) = states[nxt].local_set(Target::Room(room.clone()), true) {
                    edges.push(e);
                }
            }
        }
    }
    for e in &edges {
        for (j, s) in states.iter_mut().enumerate() {
            if j as u16 != e.origin {
                s.apply_edge(e);
            }
        }
    }

    // Each room's owner originates MSGS messages. Interest relays only to hosts;
    // flood relays to all N-1 peers.
    let mut interest_bytes = 0usize;
    let mut flood_bytes = 0usize;
    for i in 0..N {
        let alive: HashSet<u16> = (0..N as u16).filter(|x| *x != i as u16).collect();
        for room in &owned[i] {
            let target = Target::Room(room.clone());
            let hosts = states[i].interested_peers(&target, &alive);
            interest_bytes += hosts.len() * MSGS * PAYLOAD;
            flood_bytes += (N - 1) * MSGS * PAYLOAD;
        }
    }

    let ratio = flood_bytes as f64 / interest_bytes.max(1) as f64;
    eprintln!(
        "byte-reduction cell: interest {interest_bytes} B vs flood {flood_bytes} B → {ratio:.1}× \
         (spike baseline 22×; absolute is directional in-sandbox, pinned box confirms)"
    );
    // The P3 claim is >5×; the direction (tens of ×) reproduces the spike.
    assert!(
        ratio > 5.0,
        "interest must beat flood by >5× (P3): got {ratio:.1}×"
    );
    // Sanity: local-only rooms relay to nobody; only cross-node rooms cost bytes.
    let cross_rooms = N * (ROOMS_PER_NODE / 10);
    assert_eq!(interest_bytes, cross_rooms * MSGS * PAYLOAD);
}

// ─────────────────────────── node-level integration ──────────────────────────

const SECRET: &[u8] = b"cluster-secret";

fn cfg(id: u16, seeds: Vec<SocketAddr>) -> MeshConfig {
    let mut c = MeshConfig::new(id, "127.0.0.1:0".parse().unwrap(), SECRET.to_vec());
    c.seeds = seeds;
    c.cluster_name = "prod".into();
    c.params = SwimParams::tuned();
    c
}

async fn spawn_mesh(n: u16) -> Vec<Arc<MeshNode>> {
    let seed = MeshNode::start(cfg(1, vec![])).await.unwrap();
    let seed_addr = seed.addr();
    let mut nodes = vec![seed];
    for id in 2..=n {
        nodes.push(MeshNode::start(cfg(id, vec![seed_addr])).await.unwrap());
    }
    nodes
}

async fn poll_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = Instant::now();
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

fn converged(nodes: &[Arc<MeshNode>]) -> bool {
    let n = nodes.len();
    nodes.iter().all(|nd| nd.alive_count() == n - 1)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interest_propagates_and_routes_over_a_real_mesh() {
    let nodes = spawn_mesh(3).await;
    assert!(poll_until(|| converged(&nodes), Duration::from_secs(3)).await);

    // Node 3 starts hosting room R.
    nodes[2].set_local_interest(Target::Room("R".into()), true);

    // Node 1 learns to route R to node 3 (and nothing to an unrelated room).
    let learned = poll_until(
        || nodes[0].interested_peers(&Target::Room("R".into())) == vec![3],
        Duration::from_secs(3),
    )
    .await;
    assert!(learned, "node 1 did not learn node 3 hosts R");
    assert!(nodes[0]
        .interested_peers(&Target::Room("other".into()))
        .is_empty());

    // 1→0: node 3 stops hosting R → node 1 stops routing to it.
    nodes[2].set_local_interest(Target::Room("R".into()), false);
    let unlearned = poll_until(
        || {
            nodes[0]
                .interested_peers(&Target::Room("R".into()))
                .is_empty()
        },
        Duration::from_secs(3),
    )
    .await;
    assert!(unlearned, "node 1 kept stale interest after a 1→0 edge");

    for n in &nodes {
        n.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flood_lever_relays_to_all_peers() {
    let nodes = spawn_mesh(3).await;
    assert!(poll_until(|| converged(&nodes), Duration::from_secs(3)).await);

    // In interest mode, an unadvertised room routes nowhere.
    assert!(nodes[0]
        .interested_peers(&Target::Room("z".into()))
        .is_empty());

    // Flip the lever: flood ignores the table and returns all live peers.
    nodes[0].set_routing(Routing::Flood);
    let flooded = poll_until(
        || nodes[0].interested_peers(&Target::Room("z".into())) == vec![2, 3],
        Duration::from_secs(3),
    )
    .await;
    assert!(flooded, "flood mode must relay to all live peers");

    for n in &nodes {
        n.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_ages_out_interest_no_relay_to_unreachable() {
    let nodes = spawn_mesh(3).await;
    assert!(poll_until(|| converged(&nodes), Duration::from_secs(3)).await);

    nodes[2].set_local_interest(Target::Room("R".into()), true);
    assert!(
        poll_until(
            || nodes[0].interested_peers(&Target::Room("R".into())) == vec![3],
            Duration::from_secs(3)
        )
        .await
    );

    // Partition node 3 away from {1,2}. Once node 1 evicts it, its interest must
    // no longer be a relay target (no relay to unreachable; no stuck entry).
    nodes[0].set_partition(vec![3]);
    nodes[1].set_partition(vec![3]);
    nodes[2].set_partition(vec![1, 2]);

    let aged_out = poll_until(
        || {
            nodes[0]
                .interested_peers(&Target::Room("R".into()))
                .is_empty()
        },
        Duration::from_secs(6),
    )
    .await;
    assert!(aged_out, "interest for an unreachable peer must age out");

    for n in &nodes {
        n.shutdown();
    }
}
