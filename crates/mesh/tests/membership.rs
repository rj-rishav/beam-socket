//! §13.2 membership gates — real multi-node meshes on loopback (UDP probe plane
//! + TCP dissemination), fault-injected at the mesh layer so they run in CI.
//!
//! - cold-start convergence < 2 s (5 nodes)
//! - `kill -9` detection < 5 s at the tuned row
//! - partition → island → heal with **zero stuck entries** (THE spike
//!   regression: equal-incarnation Dead-vs-Alive resolved by push-pull)
//! - forged / replayed probes ignored and counted
//! - a soak chunk with zero false positives (the full 30-min loaded run stays on
//!   the pinned-box list)

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beamsocket_mesh::probe::{ProbeCounters, ProbePacket};
use beamsocket_mesh::swim::MState;
use beamsocket_mesh::{MeshConfig, MeshNode, SwimParams};

use tokio::net::UdpSocket;

const SECRET: &[u8] = b"cluster-secret";

fn cfg(id: u16, seeds: Vec<SocketAddr>) -> MeshConfig {
    let mut c = MeshConfig::new(id, "127.0.0.1:0".parse().unwrap(), SECRET.to_vec());
    c.seeds = seeds;
    c.cluster_name = "prod".into();
    c.params = SwimParams::tuned();
    c
}

/// Spawn an `n`-node mesh: node 1 is the seed, 2..=n bootstrap from it.
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

/// Every node sees the other `n-1` as Alive and holds no stuck (non-Alive) entry.
fn all_converged(nodes: &[Arc<MeshNode>]) -> bool {
    let n = nodes.len();
    nodes
        .iter()
        .all(|nd| nd.alive_count() == n - 1 && !nd.has_non_alive())
}

fn shutdown_all(nodes: &[Arc<MeshNode>]) {
    for n in nodes {
        n.shutdown();
    }
}

fn sees_dead(nd: &MeshNode, victim: u16) -> bool {
    nd.member_table()
        .iter()
        .any(|mi| mi.id == victim && mi.state == MState::Dead)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_start_convergence_under_2s() {
    let nodes = spawn_mesh(5).await;
    let start = Instant::now();
    let ok = poll_until(|| all_converged(&nodes), Duration::from_secs(2)).await;
    let elapsed = start.elapsed();
    assert!(
        ok,
        "5-node mesh did not converge within 2 s (took ≥ {elapsed:?}); node1 sees {} alive",
        nodes[0].alive_count()
    );
    shutdown_all(&nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_detection_under_5s_tuned() {
    let nodes = spawn_mesh(5).await;
    assert!(
        poll_until(|| all_converged(&nodes), Duration::from_secs(3)).await,
        "precondition: mesh must converge first"
    );

    let victim = nodes[4].self_id();
    nodes[4].shutdown(); // kill -9 semantics: stop acking, drop links

    let survivors: Vec<Arc<MeshNode>> = nodes[..4].to_vec();
    let start = Instant::now();
    let detected = poll_until(
        || survivors.iter().all(|nd| sees_dead(nd, victim)),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        detected,
        "kill of node {victim} not detected by every survivor within 5 s (took ≥ {:?})",
        start.elapsed()
    );
    shutdown_all(&survivors);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_islands_then_heal_zero_stuck() {
    // THE regression (spike failure #2): a partition leaves each island with the
    // other marked Dead at some incarnation; a pull-only join would leave those
    // entries permanently stuck behind equal-incarnation Dead-beats-Alive. The
    // push half of push-pull makes each side hear "you are dead" and refute.
    let nodes = spawn_mesh(5).await;
    assert!(
        poll_until(|| all_converged(&nodes), Duration::from_secs(3)).await,
        "precondition: converge first"
    );

    // Split ids {1,2} | {3,4,5}. nodes[0..2] have ids 1,2; nodes[2..5] have 3,4,5.
    for nd in &nodes[0..2] {
        nd.set_partition(vec![3, 4, 5]);
    }
    for nd in &nodes[2..5] {
        nd.set_partition(vec![1, 2]);
    }

    // Two stable islands: island A (ids 1,2) sees just 1 other Alive and marks
    // 3,4,5 non-Alive; island B (ids 3,4,5) sees 2 others Alive.
    let islands = poll_until(
        || {
            nodes[0].alive_count() == 1
                && nodes[1].alive_count() == 1
                && nodes[2].alive_count() == 2
                && nodes[3].alive_count() == 2
                && nodes[4].alive_count() == 2
        },
        Duration::from_secs(7),
    )
    .await;
    assert!(islands, "partition did not settle into two islands");
    assert!(
        nodes[0].has_non_alive(),
        "island A should hold the evicted island B as non-Alive"
    );

    // Heal: clear the deny sets everywhere.
    for nd in &nodes {
        nd.heal();
    }

    let healed = poll_until(|| all_converged(&nodes), Duration::from_secs(15)).await;
    assert!(
        healed,
        "heal left stuck entries — node1 alive={} non_alive={}, node3 alive={} non_alive={}",
        nodes[0].alive_count(),
        nodes[0].has_non_alive(),
        nodes[2].alive_count(),
        nodes[2].has_non_alive(),
    );
    // Zero stuck, stated explicitly: no node holds any non-Alive entry.
    for nd in &nodes {
        assert!(
            !nd.has_non_alive(),
            "a node still holds a stuck entry post-heal"
        );
    }
    shutdown_all(&nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forged_and_replayed_probes_ignored_and_counted() {
    // A single node with an open probe socket; inject raw UDP.
    let node = MeshNode::start(cfg(1, vec![])).await.unwrap();
    let target = node.addr();
    let attacker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let counters = node.probe_counters();

    // Forged: a PING sealed with the WRONG secret → bad HMAC.
    let forged = ProbePacket::Ping {
        from: 99,
        seq: 1,
        inc: 1,
        reply_to: None,
    }
    .encode(b"wrong-secret");
    attacker.send_to(&forged, target).await.unwrap();
    assert!(
        poll_until(
            || ProbeCounters::get(&counters.forged) >= 1,
            Duration::from_secs(2)
        )
        .await,
        "forged probe must be counted"
    );

    // Replay: a validly-sealed PING sent twice — the second is a stale-stamp
    // replay and is dropped-and-counted.
    let valid = ProbePacket::Ping {
        from: 98,
        seq: 7,
        inc: 1,
        reply_to: None,
    }
    .encode(SECRET);
    attacker.send_to(&valid, target).await.unwrap();
    attacker.send_to(&valid, target).await.unwrap();
    assert!(
        poll_until(
            || ProbeCounters::get(&counters.replayed) >= 1,
            Duration::from_secs(2)
        )
        .await,
        "replayed probe must be counted"
    );
    node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_chunk_no_false_positives() {
    // The env-permitted chunk (the full loaded 30-min stays on the pinned box).
    // Under a converged, idle mesh there must be zero false positives: no node
    // ever evicts or even suspects a live peer.
    let nodes = spawn_mesh(4).await;
    assert!(
        poll_until(|| all_converged(&nodes), Duration::from_secs(3)).await,
        "precondition: converge first"
    );

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        assert!(
            all_converged(&nodes),
            "false positive: a live peer was suspected/evicted during the soak"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for nd in &nodes {
        let c = nd.membership_counters();
        assert_eq!(c.dead, 0, "zero false-positive evictions");
        assert_eq!(c.suspected, 0, "zero spurious suspicions");
        // Table size stable — no phantom-member leak (the RSS-flat proxy).
        assert_eq!(nd.member_table().len(), nodes.len() - 1);
    }
    shutdown_all(&nodes);
}
