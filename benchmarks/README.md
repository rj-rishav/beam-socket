# Benchmarks

Comparisons vs `ws`, Socket.IO, uWebSockets.js. Built in Phase 1B.

## Honesty rules (non-negotiable)

1. uWebSockets.js is always included.
2. Losses are published as prominently as wins.
3. Never benchmark a competitor's misconfiguration.
4. Every result names the reference box below.

## Reference box

**Status: NOT a pinned box.** All numbers below come from an ephemeral,
shared sandbox with background CPU contention (~2× run variance observed in
Phase 0 on the same class of box). They are directional; the publishable run
happens on the pinned box (see "Pinned-box steps" below).

| Field | Value |
|---|---|
| CPU / cores | 11th Gen Intel Core i5-1135G7 @ 2.40 GHz (4 cores / 8 threads), shared |
| RAM | 19 GiB |
| Kernel | Ubuntu 24.04 / 6.17 |
| Node | v20.19.5 (official build — the distro Node 18 has a nonstandard ABI 109 that uWS ships no binary for) |
| Rust | 1.96.1, `--release` |

## Suites

- **Fan-out** (`fanout.mjs`): N clients subscribed to one room/topic; one
  512 B binary broadcast per round; the number reported is wall time from the
  broadcast call until **every client has received the frame**
  (client-observed completion — the honest end-to-end number, not
  server-send-loop time). 5 rounds; best + median reported.
- **Density** (same run): server RSS delta per idle connection at N.

Configurations (rule 3 — everyone at their best): `ws` canonical
`wss.clients` send loop; uWS native pub/sub `app.publish`; Socket.IO
websocket-transport-only, compression off, room emit; BeamSocket
`io.toRoom().send()` (one FFI call, fan-out in Rust). Compression off
everywhere. Client side: 4–6 worker processes using the `ws` client
(`socket.io-client` for Socket.IO).

## Results — 2026-07-04, shared sandbox (provisional)

512 B binary broadcast to all members; best / median of 5 rounds.

| Server | 5k members | 10k | 25k | idle RSS/conn @25k |
|---|---|---|---|---|
| **beamsocket** | 46 / 53 ms | 81 / 92 ms | **140 / 163 ms** | 11.3 KB |
| ws | **40 / 49 ms** | 87 / 88 ms | 217 / 246 ms | **4.4 KB** |
| uWebSockets.js | 40 / 49 ms | **76 / 84 ms** | 268 / 285 ms | **0.84 KB** |
| Socket.IO | 148 / 159 ms | 249 / 277 ms | — (client harness capped at 10k on this box) | 14.7 KB @10k |

Raw per-run JSON: `results/fanout.jsonl`.

### Read the losses first (rule 2)

- **Density: BeamSocket loses to both `ws` (~2.5×) and uWS (~13×) here.**
  uWS's C++ per-socket footprint (<1 KB) is in a different class. Our
  11–12 KB/conn is inside the <20 KB Phase 1 target but is nowhere near uWS.
  (Caveat: BeamSocket's number includes a JS `Socket` proxy per connection
  because the bench subscribes to `connection`; `ws`/uWS numbers include
  their own per-socket JS objects too.)
- **Small rooms (≤10k): fan-out is a three-way tie** between beamsocket, ws,
  and uWS within run noise. The Rust fan-out buys nothing when the send loop
  isn't the bottleneck.

### The win, with its caveat

At 25k members BeamSocket's broadcast completes ~1.5–1.9× sooner than ws/uWS
*as observed by clients on this box*. Caveat that matters: at 25k the
**client workers themselves are CPU-saturated** (4–6 Node processes sharing
8 threads with the server), so part of every number is client receive
capacity, and single-listen-socket accept throughput shapes the connect
phase, not steady state. Treat the ordering, not the magnitudes, as the
signal until the pinned-box run isolates client and server hardware.

### Pinned-box steps (before any number is published)

1. Re-run the full matrix on the pinned reference box with clients on a
   separate machine (or cores pinned apart from the server).
2. Add the **100k-member room** — the ARCHITECTURE §5 gate (<150 ms) — which
   does not fit honestly on this sandbox.
3. Socket.IO at 25k+.
4. Echo-latency suite (p99 at 10k conns, <5 ms target) — not yet written.

## Memory budget per idle connection (Phase 1D — 1B review debt)

Decomposition of the measured **~11.6 KB/conn** idle baseline (Node 20, 10k
idle, no app `message` subscription). "Measured" rows are from a run; "struct."
rows are computed from the types (with the allocator's real footprint folded in
where a measured row covers them). *Recoverable* = we could shrink it without a
protocol change; *structural* = inherent to holding a live socket here.

| Component | ~bytes/conn | Source | Structural vs recoverable |
|---|---|---|---|
| Kernel socket buffers (TCP rmem/wmem, low-watermark) | ~4–5 KB | struct. (OS) | **Structural** to a TCP socket. Recoverable only via OS tuning (`net.ipv4.tcp_rmem/wmem`), not by us; the dominant single line. |
| Codec read buffer (tungstenite `READ_BUFFER_SIZE`) | 4096 | struct. (constant) | **Recoverable.** Already cut 128 KB→4 KB in 1B; could go lazier (allocate on first frame) or smaller-initial-grow-on-demand. A concrete 1.x lever. |
| Engine bookkeeping: registry entry + `ConnHandle` (mailbox `Arc`, control `mpsc(4)`, close `watch`) | ~2.5 KB | **measured** (`engine_bookkeeping_memory_cost`, minus identity) | **Partly recoverable.** Dominated by the two always-allocated per-connection channels (control + close); a lazier "allocate the control path on first server-initiated action" design could reclaim most of it. |
| Tokio per-connection task (future state, scheduler slot) | ~0.3–0.6 KB | struct. | **Structural** to the isolated-task model (the BEAM-style crash boundary). Task stacks are lazy; only the future's captured state counts. |
| Identity index entry (authenticated only) | **~20 B** amortized (multi-device) · ~278 B worst case (all singletons) | **measured** (1C `identity_memory_cost_measurement`) | **Structural**, and tiny; absent entirely for anonymous connections. |
| JS `Socket` proxy (only if the app subscribes to `connection`) | ~0.2–0.5 KB | struct. | **Recoverable** — already created only on demand; a fully proxy-free path is possible for pure-fan-out apps that never touch a socket object. |

Reading it: the two biggest levers we own are the **codec read buffer** (4 KB,
recoverable) and the **per-connection control/close channels** (~part of the
2.5 KB, recoverable via lazy allocation). Kernel buffers dominate and are the
OS's, not ours — which is why uWebSockets.js (C++, <1 KB/conn) stays ~13× denser
until those levers are pulled. The `identity`/presence additions cost ~0 here
(~20 B amortized; presence adds no per-connection state — it reads the entry
that already exists). Numbers reproduce via `--ignored` measurement tests in
`crates/core/tests/phase1d.rs` and the density benchmark below.

## Running

```bash
# addon + dist first:
npm run build:native -w beamsocket && npm run build -w beamsocket
node benchmarks/fanout.mjs --server beamsocket --members 10000
node benchmarks/fanout.mjs --server ws --members 10000
node benchmarks/fanout.mjs --server uws --members 10000
node benchmarks/fanout.mjs --server socketio --members 10000
# also: idle-rss.mjs (Phase 1A density gate), send-microbench.mjs (JS→Rust cost)
```
