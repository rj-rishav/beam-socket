//! Scenario driver (RFC 0004 §7): spawns node processes on loopback, drives
//! them over the admin socket, measures, writes JSON to spike/mesh/results/.
//!
//! Scenarios: converge | kill | soak | relay | slowpeer | routing | partition

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use mesh_spike::{admin_port, epoch_ms, swim_port};

const BASE: u16 = 47000;

struct Cluster {
    children: Vec<(u16, Child)>,
    base: u16,
}

impl Cluster {
    fn spawn(n: u16, params: &str, stagger_ms: u64) -> Self {
        let node_bin = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join("node");
        let seeds = format!("127.0.0.1:{}", swim_port(BASE, 0));
        let mut children = Vec::new();
        for id in 0..n {
            let child = Command::new(&node_bin)
                .args([
                    "--id",
                    &id.to_string(),
                    "--base-port",
                    &BASE.to_string(),
                    "--seeds",
                    &seeds,
                    "--params",
                    params,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn node");
            children.push((id, child));
            std::thread::sleep(Duration::from_millis(stagger_ms));
        }
        Cluster {
            children,
            base: BASE,
        }
    }

    fn kill(&mut self, id: u16) -> u64 {
        let t = epoch_ms();
        if let Some((_, child)) = self.children.iter_mut().find(|(i, _)| *i == id) {
            let _ = child.kill(); // SIGKILL — the kill -9 of the work order
            let _ = child.wait();
        }
        t
    }

    fn ids(&self) -> Vec<u16> {
        self.children.iter().map(|(i, _)| *i).collect()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, child) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Let TIME_WAIT/port reuse settle between scenarios.
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// One admin request → one JSON response (fresh connection per call:
/// throwaway-simple, and it doubles as a node-responsiveness probe).
fn admin(base: u16, id: u16, req: Value) -> Option<Value> {
    let addr = format!("127.0.0.1:{}", admin_port(base, id));
    let mut stream = TcpStream::connect(&addr).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let mut line = req.to_string();
    line.push('\n');
    stream.write_all(line.as_bytes()).ok()?;
    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).ok()?;
    serde_json::from_str(&resp).ok()
}

fn admin_retry(base: u16, id: u16, req: Value, tries: u32) -> Value {
    for _ in 0..tries {
        if let Some(v) = admin(base, id, req.clone()) {
            return v;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("node {id} admin unreachable");
}

/// Poll until every live node sees every OTHER live node as alive.
fn wait_converged(c: &Cluster, live: &[u16], timeout: Duration) -> Option<u64> {
    let t0 = Instant::now();
    loop {
        let mut all = true;
        for &id in live {
            let Some(v) = admin(c.base, id, json!({"cmd": "members"})) else {
                all = false;
                break;
            };
            let members = v["members"].as_object().cloned().unwrap_or_default();
            for &other in live {
                if other == id {
                    continue;
                }
                if members.get(&other.to_string()).and_then(|s| s.as_str()) != Some("alive") {
                    all = false;
                    break;
                }
            }
            if !all {
                break;
            }
        }
        if all {
            return Some(t0.elapsed().as_millis() as u64);
        }
        if t0.elapsed() > timeout {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn write_result(name: &str, v: &Value) {
    std::fs::create_dir_all("results").ok();
    let path = format!("results/{name}-{}.json", epoch_ms());
    std::fs::write(&path, serde_json::to_string_pretty(v).unwrap()).unwrap();
    println!("{}", serde_json::to_string_pretty(v).unwrap());
    println!("→ {path}");
}

fn main() {
    let mut scenario = String::from("converge");
    let mut nodes: u16 = 5;
    let mut params = String::from("tuned");
    let mut seconds: u64 = 30;
    let mut rate: u64 = 100_000;
    let mut payload: u64 = 64;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--scenario" => scenario = args.next().unwrap(),
            "--nodes" => nodes = args.next().unwrap().parse().unwrap(),
            "--params" => params = args.next().unwrap(),
            "--seconds" => seconds = args.next().unwrap().parse().unwrap(),
            "--rate" => rate = args.next().unwrap().parse().unwrap(),
            "--payload" => payload = args.next().unwrap().parse().unwrap(),
            _ => {}
        }
    }

    match scenario.as_str() {
        "converge" => converge(nodes, &params),
        "kill" => kill(nodes, &params),
        "soak" => soak(nodes, &params, seconds),
        "relay" => relay(&params, rate, payload, seconds),
        "slowpeer" => slowpeer(&params, rate, payload),
        "routing" => routing(nodes, &params),
        "partition" => partition(nodes, &params),
        other => eprintln!("unknown scenario {other}"),
    }
}

/// Gate: 5-node cold start converges < 2 s (from FIRST spawn, staggered).
fn converge(n: u16, params: &str) {
    let c = Cluster::spawn(n, params, 100);
    let live = c.ids();
    let ms = wait_converged(&c, &live, Duration::from_secs(10));
    // Elapsed includes the (n-1)·100 ms spawn stagger — reported as-is
    // (cold start means cold start).
    write_result(
        "converge",
        &json!({
            "scenario": "converge", "nodes": n, "params": params,
            "converged_ms_from_first_spawn": ms,
            "pass_lt_2000": ms.map(|m| m < 2000).unwrap_or(false),
        }),
    );
}

/// Gate: kill -9 → every survivor marks the victim dead < 5 s.
fn kill(n: u16, params: &str) {
    let mut c = Cluster::spawn(n, params, 100);
    let live = c.ids();
    wait_converged(&c, &live, Duration::from_secs(10)).expect("initial convergence");
    std::thread::sleep(Duration::from_secs(2)); // steady state

    let victim = n - 1;
    let t_kill = c.kill(victim);
    let survivors: Vec<u16> = live.into_iter().filter(|&i| i != victim).collect();

    // Wait until every survivor has evicted the victim (cap 30 s).
    let t0 = Instant::now();
    loop {
        let done = survivors.iter().all(|&id| {
            admin(c.base, id, json!({"cmd": "members"}))
                .map(|v| v["members"][victim.to_string()].as_str() == Some("dead"))
                .unwrap_or(false)
        });
        if done || t0.elapsed() > Duration::from_secs(30) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Precise per-survivor timings from their event logs.
    let mut detections = HashMap::new();
    for &id in &survivors {
        let ev = admin_retry(c.base, id, json!({"cmd": "events"}), 3);
        let mut suspect_ms = None;
        let mut dead_ms = None;
        for e in ev["events"].as_array().cloned().unwrap_or_default() {
            if e["peer"].as_u64() == Some(victim as u64) && e["t_ms"].as_u64() >= Some(t_kill) {
                match e["state"].as_str() {
                    Some("suspect") if suspect_ms.is_none() => {
                        suspect_ms = e["t_ms"].as_u64().map(|t| t - t_kill)
                    }
                    Some("dead") if dead_ms.is_none() => {
                        dead_ms = e["t_ms"].as_u64().map(|t| t - t_kill)
                    }
                    _ => {}
                }
            }
        }
        detections.insert(
            id.to_string(),
            json!({"suspect_ms": suspect_ms, "dead_ms": dead_ms}),
        );
    }
    let max_dead = detections
        .values()
        .filter_map(|d| d["dead_ms"].as_u64())
        .max();
    write_result(
        "kill",
        &json!({
            "scenario": "kill", "nodes": n, "params": params, "victim": victim,
            "per_survivor": detections,
            "max_detection_ms": max_dead,
            "pass_lt_5000": max_dead.map(|m| m < 5000).unwrap_or(false),
        }),
    );
}

/// P1 soak: CPU load on every node + relay traffic; count false-positive
/// evictions (any dead event — nobody is killed) and refutations.
fn soak(n: u16, params: &str, seconds: u64) {
    let c = Cluster::spawn(n, params, 100);
    let live = c.ids();
    wait_converged(&c, &live, Duration::from_secs(10)).expect("initial convergence");
    for &id in &live {
        admin_retry(c.base, id, json!({"cmd": "load", "on": true}), 3);
    }
    // Cross relay traffic: each node benches its neighbor for the whole soak.
    for &id in &live {
        let peer = (id + 1) % n;
        admin_retry(
            c.base,
            id,
            json!({"cmd": "bench_start", "peer": peer, "rate": 20000, "payload": 64, "seconds": seconds}),
            3,
        );
    }
    let t_start = epoch_ms();
    std::thread::sleep(Duration::from_secs(seconds));

    let mut false_positives = 0u64;
    let mut suspicions = 0u64;
    let mut refutations = 0u64;
    for &id in &live {
        let ev = admin_retry(c.base, id, json!({"cmd": "events"}), 3);
        for e in ev["events"].as_array().cloned().unwrap_or_default() {
            if e["t_ms"].as_u64() < Some(t_start) {
                continue;
            }
            match e["state"].as_str() {
                Some("dead") => false_positives += 1, // nobody was killed
                Some("suspect") => suspicions += 1,
                _ => {}
            }
        }
        let m = admin_retry(c.base, id, json!({"cmd": "members"}), 3);
        refutations += m["refutations"].as_u64().unwrap_or(0);
    }
    write_result(
        "soak",
        &json!({
            "scenario": "soak", "nodes": n, "params": params, "seconds": seconds,
            "false_positive_evictions": false_positives,
            "suspicion_events": suspicions,
            "refutations": refutations,
            "pass_zero_fp": false_positives == 0,
        }),
    );
}

/// P2 relay cell: one hop A→B, paced, one-way hop latency (shared
/// CLOCK_MONOTONIC) + rtt + drops.
fn relay(params: &str, rate: u64, payload: u64, seconds: u64) {
    let c = Cluster::spawn(2, params, 50);
    wait_converged(&c, &c.ids(), Duration::from_secs(10)).expect("convergence");
    // Wait for the TCP link (node 1 dials node 0).
    let t0 = Instant::now();
    while admin(c.base, 1, json!({"cmd": "peers"}))
        .map(|v| v["peers"].as_array().map(|a| a.is_empty()).unwrap_or(true))
        .unwrap_or(true)
    {
        assert!(t0.elapsed() < Duration::from_secs(10), "link never came up");
        std::thread::sleep(Duration::from_millis(50));
    }
    admin_retry(
        c.base,
        1,
        json!({"cmd": "bench_start", "peer": 0, "rate": rate, "payload": payload, "seconds": seconds}),
        3,
    );
    let result = loop {
        std::thread::sleep(Duration::from_millis(250));
        let r = admin_retry(c.base, 1, json!({"cmd": "bench_result"}), 3);
        if r["done"].as_bool() == Some(true) {
            break r;
        }
    };
    let hop_p99_us = result["hop_p99_us"].as_u64().unwrap_or(u64::MAX);
    write_result(
        "relay",
        &json!({
            "scenario": "relay", "params": params, "rate": rate, "payload": payload,
            "seconds": seconds, "result": result,
            "pass_hop_p99_lt_1ms": hop_p99_us < 1000,
        }),
    );
}

/// §4.6 containment: a slowed peer's link fills and drops (counted); the
/// sender stays responsive and a healthy link stays fast.
fn slowpeer(params: &str, rate: u64, payload: u64) {
    let c = Cluster::spawn(3, params, 50);
    wait_converged(&c, &c.ids(), Duration::from_secs(10)).expect("convergence");
    let t0 = Instant::now();
    while admin(c.base, 2, json!({"cmd": "peers"}))
        .map(|v| v["peers"].as_array().map(|a| a.len() < 2).unwrap_or(true))
        .unwrap_or(true)
    {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "links never came up"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Baseline: 2 → 1 (healthy).
    admin_retry(
        c.base,
        2,
        json!({"cmd": "bench_start", "peer": 1, "rate": rate, "payload": payload, "seconds": 5}),
        3,
    );
    let healthy = wait_bench(c.base, 2);

    // Slow node 0's inbound processing, then 2 → 0.
    admin_retry(c.base, 0, json!({"cmd": "slow", "ms": 5}), 3);
    admin_retry(
        c.base,
        2,
        json!({"cmd": "bench_start", "peer": 0, "rate": rate, "payload": payload, "seconds": 5}),
        3,
    );
    // While saturating the slow link, the sender must stay responsive.
    let mut admin_probe_max_us = 0u64;
    for _ in 0..10 {
        let t = Instant::now();
        admin_retry(c.base, 2, json!({"cmd": "ping"}), 3);
        admin_probe_max_us = admin_probe_max_us.max(t.elapsed().as_micros() as u64);
        std::thread::sleep(Duration::from_millis(300));
    }
    let slow = wait_bench(c.base, 2);
    let drops = slow["dropped"].as_u64().unwrap_or(0);

    // Healthy link again, after the abuse: still fast.
    admin_retry(
        c.base,
        2,
        json!({"cmd": "bench_start", "peer": 1, "rate": rate, "payload": payload, "seconds": 5}),
        3,
    );
    let healthy_after = wait_bench(c.base, 2);

    write_result(
        "slowpeer",
        &json!({
            "scenario": "slowpeer", "params": params, "rate": rate, "payload": payload,
            "healthy_before": healthy, "slow_link": slow, "healthy_after": healthy_after,
            "sender_admin_probe_max_us_during_saturation": admin_probe_max_us,
            "pass_drops_counted_never_blocked": drops > 0,
        }),
    );
}

fn wait_bench(base: u16, id: u16) -> Value {
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let r = admin_retry(base, id, json!({"cmd": "bench_result"}), 3);
        if r["done"].as_bool() == Some(true) {
            return r;
        }
    }
}

/// P3: interest-routed vs flooded publish — inter-node bytes ratio.
fn routing(n: u16, params: &str) {
    let c = Cluster::spawn(n, params, 100);
    let live = c.ids();
    wait_converged(&c, &live, Duration::from_secs(10)).expect("convergence");
    // Full mesh links up.
    let t0 = Instant::now();
    loop {
        let full = live.iter().all(|&id| {
            admin(c.base, id, json!({"cmd": "peers"}))
                .map(|v| {
                    v["peers"]
                        .as_array()
                        .map(|a| a.len() as u16 == n - 1)
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        });
        if full {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(15),
            "mesh never completed"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    for &id in &live {
        admin_retry(
            c.base,
            id,
            json!({"cmd": "advertise", "nodes": n, "rooms_per_node": 50}),
            3,
        );
    }
    std::thread::sleep(Duration::from_millis(500)); // interest frames land

    let sum_bytes = |c: &Cluster| -> u64 {
        c.ids()
            .iter()
            .map(|&id| {
                admin_retry(c.base, id, json!({"cmd": "counters"}), 3)["bytes_out"]
                    .as_u64()
                    .unwrap_or(0)
            })
            .sum()
    };

    let before_interest = sum_bytes(&c);
    for &id in &live {
        admin_retry(
            c.base,
            id,
            json!({"cmd": "publish", "nodes": n, "rooms_per_node": 50, "msgs": 20, "payload": 512, "flood": false}),
            3,
        );
    }
    std::thread::sleep(Duration::from_millis(500));
    let after_interest = sum_bytes(&c);

    let before_flood = after_interest;
    for &id in &live {
        admin_retry(
            c.base,
            id,
            json!({"cmd": "publish", "nodes": n, "rooms_per_node": 50, "msgs": 20, "payload": 512, "flood": true}),
            3,
        );
    }
    std::thread::sleep(Duration::from_millis(500));
    let after_flood = sum_bytes(&c);

    let interest_bytes = after_interest - before_interest;
    let flood_bytes = after_flood - before_flood;
    let ratio = flood_bytes as f64 / interest_bytes.max(1) as f64;
    write_result(
        "routing",
        &json!({
            "scenario": "routing", "nodes": n, "rooms_per_node": 50,
            "cross_node_membership": "10% (r % 10 == 0 also hosted on next node)",
            "msgs_per_room": 20, "payload": 512,
            "interest_bytes": interest_bytes, "flood_bytes": flood_bytes,
            "flood_over_interest": ratio,
            "pass_gt_5x": ratio > 5.0,
        }),
    );
}

/// Partition → two islands → heal → zero stuck entries.
fn partition(n: u16, params: &str) {
    let c = Cluster::spawn(n, params, 100);
    let live = c.ids();
    wait_converged(&c, &live, Duration::from_secs(10)).expect("initial convergence");

    let island_a: Vec<u16> = live.iter().copied().filter(|&i| i < 2).collect();
    let island_b: Vec<u16> = live.iter().copied().filter(|&i| i >= 2).collect();
    let t_split = epoch_ms();
    for &id in &island_a {
        admin_retry(c.base, id, json!({"cmd": "deny", "ids": island_b}), 3);
    }
    for &id in &island_b {
        admin_retry(c.base, id, json!({"cmd": "deny", "ids": island_a}), 3);
    }

    // Each side must evict the other (islands stabilize).
    let t0 = Instant::now();
    loop {
        let a_done = island_a.iter().all(|&id| {
            let v = admin_retry(c.base, id, json!({"cmd": "members"}), 3);
            island_b
                .iter()
                .all(|o| v["members"][o.to_string()].as_str() == Some("dead"))
        });
        let b_done = island_b.iter().all(|&id| {
            let v = admin_retry(c.base, id, json!({"cmd": "members"}), 3);
            island_a
                .iter()
                .all(|o| v["members"][o.to_string()].as_str() == Some("dead"))
        });
        if a_done && b_done {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "islands never stabilized"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let islands_ms = epoch_ms() - t_split;
    std::thread::sleep(Duration::from_secs(2)); // operate as islands

    // Heal.
    let t_heal = epoch_ms();
    for &id in &live {
        admin_retry(c.base, id, json!({"cmd": "deny", "ids": []}), 3);
    }
    let healed = wait_converged(&c, &live, Duration::from_secs(30));

    // Zero stuck entries: every node sees every other alive, none suspect/dead.
    let mut stuck = 0u64;
    let mut refutations = 0u64;
    for &id in &live {
        let v = admin_retry(c.base, id, json!({"cmd": "members"}), 3);
        for (_, state) in v["members"].as_object().cloned().unwrap_or_default() {
            if state.as_str() != Some("alive") {
                stuck += 1;
            }
        }
        refutations += v["refutations"].as_u64().unwrap_or(0);
    }
    write_result(
        "partition",
        &json!({
            "scenario": "partition", "nodes": n, "params": params,
            "islands": [island_a, island_b],
            "islands_stabilized_ms": islands_ms,
            "heal_ms_from_deny_clear": healed.map(|_| epoch_ms() - t_heal),
            "stuck_entries_after_heal": stuck,
            "refutations": refutations,
            "pass_zero_stuck": stuck == 0 && healed.is_some(),
        }),
    );
}
