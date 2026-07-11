//! One mesh node process. The coordinator drives it over a line-JSON admin
//! socket: query membership/counters, inject partitions (deny sets), start
//! echo benches, toggle CPU load, run the routing publish script.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use mesh_spike::relay::Relay;
use mesh_spike::swim::{MState, Swim};
use mesh_spike::{admin_port, relay_port, swim_port, SwimParams};

struct NodeCtx {
    swim: Arc<Swim>,
    relay: Arc<Relay>,
    deny: Arc<Mutex<HashSet<u16>>>,
    load_on: Arc<AtomicBool>,
    bench_running: Arc<AtomicBool>,
    bench_sent: Arc<AtomicU64>,
    bench_dropped: Arc<AtomicU64>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut id: u16 = 0;
    let mut base: u16 = 47000;
    let mut seeds: Vec<SocketAddr> = Vec::new();
    let mut params = "tuned".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--id" => id = args.next().unwrap().parse().unwrap(),
            "--base-port" => base = args.next().unwrap().parse().unwrap(),
            "--seeds" => {
                seeds = args
                    .next()
                    .unwrap()
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse().unwrap())
                    .collect()
            }
            "--params" => params = args.next().unwrap(),
            _ => {}
        }
    }

    let deny = Arc::new(Mutex::new(HashSet::new()));
    let swim_bind: SocketAddr = format!("127.0.0.1:{}", swim_port(base, id))
        .parse()
        .unwrap();
    let relay_bind: SocketAddr = format!("127.0.0.1:{}", relay_port(base, id))
        .parse()
        .unwrap();
    let swim = Swim::start(
        id,
        swim_bind,
        seeds,
        SwimParams::by_name(&params),
        deny.clone(),
    )
    .await;
    let relay = Relay::start(id, relay_bind, swim.clone(), deny.clone()).await;

    let ctx = Arc::new(NodeCtx {
        swim,
        relay,
        deny,
        load_on: Arc::new(AtomicBool::new(false)),
        bench_running: Arc::new(AtomicBool::new(false)),
        bench_sent: Arc::new(AtomicU64::new(0)),
        bench_dropped: Arc::new(AtomicU64::new(0)),
    });

    let admin: SocketAddr = format!("127.0.0.1:{}", admin_port(base, id))
        .parse()
        .unwrap();
    let listener = TcpListener::bind(admin).await.expect("admin bind");
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let (rd, mut wr) = stream.into_split();
            let mut lines = BufReader::new(rd).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let req: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let resp = handle(&ctx, &req);
                let mut out = resp.to_string();
                out.push('\n');
                if wr.write_all(out.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}

fn handle(ctx: &Arc<NodeCtx>, req: &Value) -> Value {
    match req["cmd"].as_str().unwrap_or("") {
        // Membership view: { id: state } + events + refutations.
        "members" => {
            let m = ctx.swim.membership.lock().unwrap();
            let view: serde_json::Map<String, Value> = m
                .members
                .iter()
                .map(|(id, mem)| {
                    (
                        id.to_string(),
                        json!(format!("{:?}", mem.state).to_lowercase()),
                    )
                })
                .collect();
            json!({
                "self": m.self_id,
                "inc": m.self_inc,
                "members": view,
                "refutations": m.refutations,
            })
        }
        "events" => {
            let m = ctx.swim.membership.lock().unwrap();
            json!({ "events": m.events })
        }
        // Socket-level partition injection: drop swim packets from these ids,
        // sever + refuse relay links to them. Empty list = heal.
        "deny" => {
            let ids: HashSet<u16> = req["ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_u64().map(|x| x as u16))
                        .collect()
                })
                .unwrap_or_default();
            *ctx.deny.lock().unwrap() = ids;
            json!({ "ok": true })
        }
        // CPU load: two busy-spin threads competing with the runtime (P1).
        "load" => {
            let on = req["on"].as_bool().unwrap_or(false);
            let was = ctx.load_on.swap(on, Ordering::SeqCst);
            if on && !was {
                for _ in 0..2 {
                    let flag = ctx.load_on.clone();
                    std::thread::spawn(move || {
                        let mut x: u64 = 0x9e3779b97f4a7c15;
                        while flag.load(Ordering::Relaxed) {
                            // Pointless-but-unoptimizable work.
                            x = x
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407);
                            std::hint::black_box(x);
                        }
                    });
                }
            }
            json!({ "ok": true })
        }
        "slow" => {
            ctx.relay
                .slow_ms
                .store(req["ms"].as_u64().unwrap_or(0), Ordering::Relaxed);
            json!({ "ok": true })
        }
        "peers" => json!({ "peers": ctx.relay.peers_up() }),
        // Paced echo bench toward one peer (the P2 relay cell).
        "bench_start" => {
            let peer = req["peer"].as_u64().unwrap_or(0) as u16;
            let rate = req["rate"].as_u64().unwrap_or(100_000);
            let payload = req["payload"].as_u64().unwrap_or(64) as usize;
            let seconds = req["seconds"].as_u64().unwrap_or(10);
            if ctx.bench_running.swap(true, Ordering::SeqCst) {
                return json!({ "ok": false, "err": "bench already running" });
            }
            {
                let mut b = ctx.relay.bench.lock().unwrap();
                *b = mesh_spike::relay::BenchState {
                    hop: mesh_spike::Reservoir::new(100_000),
                    rtt: mesh_spike::Reservoir::new(100_000),
                    acked: 0,
                    sent: 0,
                    done: false,
                };
            }
            ctx.bench_sent.store(0, Ordering::SeqCst);
            ctx.bench_dropped.store(0, Ordering::SeqCst);
            let ctx2 = ctx.clone();
            tokio::spawn(async move {
                run_bench(ctx2, peer, rate, payload, seconds).await;
            });
            json!({ "ok": true })
        }
        "bench_result" => {
            let running = ctx.bench_running.load(Ordering::SeqCst);
            let mut b = ctx.relay.bench.lock().unwrap();
            json!({
                "done": !running,
                "sent": ctx.bench_sent.load(Ordering::SeqCst),
                "dropped": ctx.bench_dropped.load(Ordering::SeqCst),
                "acked": b.acked,
                "hop_p50_us": b.hop.percentile(0.50) / 1_000,
                "hop_p99_us": b.hop.percentile(0.99) / 1_000,
                "rtt_p50_us": b.rtt.percentile(0.50) / 1_000,
                "rtt_p99_us": b.rtt.percentile(0.99) / 1_000,
            })
        }
        // Advertise interest for deterministically hosted rooms (P3 cell):
        // node i hosts rooms [i·per_node, (i+1)·per_node); every room with
        // index % 10 == 0 is ALSO hosted by node (i+1) % n — the "10%
        // cross-node membership" of the prediction.
        "advertise" => {
            let n = req["nodes"].as_u64().unwrap_or(5) as u16;
            let per_node = req["rooms_per_node"].as_u64().unwrap_or(50) as u32;
            let mine = hosted_rooms(ctx.relay.self_id, n, per_node);
            let mut body = Vec::with_capacity(4 + mine.len() * 4);
            body.extend_from_slice(&(mine.len() as u32).to_le_bytes());
            for r in &mine {
                body.extend_from_slice(&r.to_le_bytes());
            }
            let frame = Relay::frame(5, &body);
            for p in ctx.relay.peers_up() {
                ctx.relay.push(p, frame.clone());
            }
            json!({ "ok": true, "hosted": mine.len() })
        }
        // Publish `msgs` messages to each hosted room, interest-routed or
        // flooded. bytes_out counters (before/after, read by the
        // coordinator) are the P3 measurement.
        "publish" => {
            let n = req["nodes"].as_u64().unwrap_or(5) as u16;
            let per_node = req["rooms_per_node"].as_u64().unwrap_or(50) as u32;
            let msgs = req["msgs"].as_u64().unwrap_or(20);
            let payload = req["payload"].as_u64().unwrap_or(512) as usize;
            let flood = req["flood"].as_bool().unwrap_or(false);
            let peers = ctx.relay.peers_up();
            let mut sent = 0u64;
            for room in hosted_rooms(ctx.relay.self_id, n, per_node) {
                let targets: Vec<u16> = if flood {
                    peers.clone()
                } else {
                    ctx.relay
                        .interest
                        .lock()
                        .unwrap()
                        .get(&room)
                        .map(|s| s.iter().copied().collect())
                        .unwrap_or_default()
                };
                for _ in 0..msgs {
                    let frame = Relay::room_frame(room, payload);
                    for t in &targets {
                        if ctx.relay.push(*t, frame.clone()) {
                            sent += 1;
                        }
                    }
                }
            }
            json!({ "ok": true, "frames_sent": sent })
        }
        "counters" => {
            let c = &ctx.relay.counters;
            json!({
                "bytes_out": c.bytes_out.load(Ordering::Relaxed),
                "bytes_in": c.bytes_in.load(Ordering::Relaxed),
                "frames_out": c.frames_out.load(Ordering::Relaxed),
                "frames_in": c.frames_in.load(Ordering::Relaxed),
                "drops": c.drops.load(Ordering::Relaxed),
            })
        }
        "alive_count" => {
            let m = ctx.swim.membership.lock().unwrap();
            let alive = m
                .members
                .values()
                .filter(|mem| mem.state == MState::Alive)
                .count();
            json!({ "alive": alive })
        }
        "ping" => json!({ "ok": true }),
        other => json!({ "ok": false, "err": format!("unknown cmd {other:?}") }),
    }
}

fn hosted_rooms(id: u16, n: u16, per_node: u32) -> Vec<u32> {
    let mut rooms: Vec<u32> = (id as u32 * per_node..(id as u32 + 1) * per_node).collect();
    // 10% cross-node membership: the previous node's r%10==0 rooms are also
    // hosted here.
    let prev = ((id + n - 1) % n) as u32;
    rooms.extend((prev * per_node..(prev + 1) * per_node).filter(|r| r % 10 == 0));
    rooms
}

/// Paced sender: `rate` ECHO_REQ/s toward `peer` for `seconds`, 1 ms ticks.
/// Overflow at the bounded link queue drops-and-counts (never blocks) —
/// exactly the §4.6 policy under test.
async fn run_bench(ctx: Arc<NodeCtx>, peer: u16, rate: u64, payload: usize, seconds: u64) {
    // 250 µs pacing: at 100k/s a 1 ms tick sends 100-frame bursts whose tail
    // waits behind the burst — that queueing is pacing artifact, not hop cost.
    let per_tick = (rate / 4000).max(1);
    let ticks = seconds * 4000;
    let mut seq = 0u64;
    let mut interval = tokio::time::interval(Duration::from_micros(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    for _ in 0..ticks {
        interval.tick().await;
        for _ in 0..per_tick {
            seq += 1;
            let frame = Relay::echo_req(seq, payload);
            if ctx.relay.push(peer, frame) {
                ctx.bench_sent.fetch_add(1, Ordering::Relaxed);
            } else {
                ctx.bench_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    // Grace for in-flight acks, then mark done.
    tokio::time::sleep(Duration::from_millis(500)).await;
    ctx.relay.bench.lock().unwrap().done = true;
    ctx.bench_running.store(false, Ordering::SeqCst);
}
