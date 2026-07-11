# RFC 0004 Mesh Spike

Throwaway-grade (the RFC 0001 rule): only conclusions and constants graduate.
**No integration with the real engine, SDK, or bridge** — pure mesh dynamics.

- Spec: [../../docs/rfcs/0004-cluster-mesh.md](../../docs/rfcs/0004-cluster-mesh.md) §7
- Results go in: `../../docs/rfcs/0004-results.md` (raw JSON in `results/`)

## Run

```bash
cargo build --release
cd spike/mesh   # coordinator writes results/ relative to cwd
target=../.. # or wherever CARGO_TARGET_DIR points
$target/release/coordinator --scenario converge  --nodes 5
$target/release/coordinator --scenario kill      --params literature
$target/release/coordinator --scenario kill      --params tuned
$target/release/coordinator --scenario soak      --seconds 1800   # full RFC soak (sandbox: run in chunks)
$target/release/coordinator --scenario relay     --rate 100000 --payload 64 --seconds 10
$target/release/coordinator --scenario slowpeer  --rate 100000 --payload 512
$target/release/coordinator --scenario routing
$target/release/coordinator --scenario partition
```

## What it is

- `src/swim.rs` — SWIM-style membership over UDP (probe/ack, indirect probes,
  suspicion → eviction, incarnation refutation, piggybacked gossip, join
  push-pull, periodic re-seed = the heal path). JSON packets: low-rate control
  plane, throwaway-simple on purpose.
- `src/relay.rs` — TCP peer links, hand-rolled length-prefixed binary frames,
  per-peer **byte-bounded** outbound queue with drop-and-count (§4.6 under
  test). One-way hop latency uses `CLOCK_MONOTONIC`, comparable across
  processes on one box.
- `src/bin/node.rs` — one mesh node process + line-JSON admin socket (deny
  sets = socket-level partition injection; CPU-load threads; echo bench;
  interest advertise/publish).
- `src/bin/coordinator.rs` — spawns 2–5 node processes, drives scenarios,
  writes `results/*.json`.

## Reminders

- Fault injection is socket-level (deny sets), not iptables/netns — the
  sandbox has no net privileges. A denied peer's UDP is dropped at receive
  and its TCP links are severed/refused: a blackhole, which is the partition
  shape SWIM cares about.
- Latency = sender `clock_gettime(MONOTONIC)` at enqueue → receiver stamp at
  frame dispatch. Same clock domain, same box; no NTP skew in the number.
- The queues are bounded (byte HWM). Measure the overflow, don't prevent it.
