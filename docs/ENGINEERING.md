# BeamSocket Engineering Guide

**Audience:** every engineer on the project, including your first week. If anything here is unclear, that is a bug in this document — open an issue.

**Read in this order:**
1. `docs/ARCHITECTURE.md` — *why* the system is shaped this way
2. `docs/rfcs/0001-event-bridge.md` — the gate everything waits on
3. This document — *what to build, in what order, and how to know you're done*

---

## 0. The mental model (one paragraph)

BeamSocket is a Rust engine that owns all sockets, plus a thin JavaScript SDK that owns all business logic. Rust does every piece of per-message heavy lifting (routing, broadcast fan-out, keepalive, backpressure). JavaScript runs only when the application subscribed to an event. The two sides talk over a **bridge**: JS→Rust through cheap native function calls, Rust→JS through *batched* callbacks (a `ThreadsafeFunction`, or "TSFN" — napi-rs's way of safely calling into V8 from a non-JS thread).

## 1. Ground rules — never break these

Every PR is reviewed against these five rules. They come from ARCHITECTURE.md §1 and they are not negotiable.

1. **Per-message work never crosses the Rust↔JS boundary unless the app subscribed to that event.** Broadcast fan-out, room routing, heartbeats, cleanup: Rust only.
2. **No core primitive may rely on a single global lock on a hot path.** Connection, user, room, and presence registries are all sharded.
3. **Every production safety feature must work behind a load balancer**, not just direct-connect. If your feature uses a client IP, ask: "what does this do behind an ALB?"
4. **Every feature that adds per-connection state documents its memory cost** — a number, in the PR description.
5. **Every queue is bounded.** An unbounded channel anywhere in the runtime is an automatic PR rejection.

## 2. Repo map

| Path | What it is | Language |
|---|---|---|
| `crates/core/` | The engine (data plane). Never imports NAPI. Unit-testable with plain `cargo test` | Rust |
| `crates/node/` | NAPI-RS binding. Translation only — if you're writing logic here, it belongs in `core` | Rust |
| `packages/beamsocket/` | The npm package users install (SDK, control plane) | TypeScript |
| `spike/` | RFC 0001 bridge spike. Throwaway-grade code; the *winner* graduates into `crates/node` | Rust + JS |
| `benchmarks/` | Honest comparisons vs `ws`, Socket.IO, uWebSockets.js | JS |
| `examples/` | Runnable demo apps | TS |
| `docs/` | Architecture, RFCs, this guide | — |

## 3. Roadmap at a glance

| Phase | Deliverable | Exit gate | Status |
|---|---|---|---|
| **0** | Event bridge spike | `docs/rfcs/0001-results.md` written; one design passes RFC gates | ✅ merged |
| **1A** | Echo server, end-to-end | Autobahn passes; JS client echoes through the real bridge | ✅ merged |
| **1B** | Rooms + broadcast | Fan-out benchmark vs ws/uWS published | ✅ merged (benchmark provisional → pinned box) |
| **1C** | Identity + admission limits | Multi-device `toUser` works; spoofed XFF test passes | ✅ merged |
| **1D** | Presence, metrics, graceful close | 10-min soak clean → publish `0.1.0-alpha` | ✅ merged (publish rides the release blockers) |
| **1.1** | Attach to existing HTTP server | RFC 0002 written first, then Express/Fastify example | ✅ merged (macOS row CI-gated) |
| **2A** | Observability read surface | §12 gates: stats/topRooms/backpressureReport + zero-hot-path-cost proof | ✅ merged (perf guard ON≈OFF) |
| **2B** | Admin actions | §12 gates: disconnect verbs + identity/room cleanup proofs | ✅ merged (zero new teardown held) |
| **3A** | Mesh link layer (wire, handshake, coalesced writer) | §13 gates: interop matrix + attack tests | ✅ merged (126 tests; vendored crypto → audited before release) |
| **3B** | SWIM membership (graduated from spike, tuned row) | §13 gates: convergence/kill/heal + stuck-entry regression | after 3A |
| **3C** | Interest routing (+ flood fallback lever) | §13 gates: correctness vs flood model, byte-reduction cell | after 3B |
| **3D** | Relay verbs + engine integration | §13 gates: cross-node targeting E2E, 1C semantics under partition | after 3C |

Phases are strictly sequential. **Do not start a phase while the previous phase's exit gate is open.**

**Release blockers** (external hardware/credentials; tracked in README): pinned-box constant re-confirmation, pinned-box benchmarks, 10-minute soak, npm publish + install matrix, darwin CI attach run. **Parked backlog** (after Phase 2): RFC 0003 engine-side TLS; Windows fd-handoff spike.

---

## 4. Phase 0 — The Bridge Spike (current phase)

**Goal:** answer the project's only open question — *can the Rust↔JS boundary sustain the architecture?* Full spec: RFC 0001. This section is the practical how-to.

**Why first:** the bridge is the one component we cannot benchmark in pure Rust, and its overload behavior defines what users observe. Everything else waits.

### What you build (three pieces, in `spike/`)

1. **`spike/bridge-core`** (Rust): a synthetic event generator. No sockets, no WebSocket code — a Tokio task that produces `Event { conn_id, payload }` at a controlled rate into a **bounded** queue. Payload sizes and rates come from CLI/env config.
2. **`spike/bridge-node`** (Rust, NAPI): the bridge itself, with designs A, B, C selectable by flag:
   - **A — naive:** one TSFN call per event (baseline; expected to lose)
   - **B — batched objects:** collect events, flush as a JS array via one TSFN call when batch hits N or a timer fires
   - **C — batched flat:** encode events into one contiguous `Buffer` per flush; JS decodes with a cursor reader
3. **`spike/harness`** (JS): connects the consumer profiles, drives load, records stats, prints a results table.

### Step-by-step

1. Build the generator with a `run(rate, payload_size, duration)` API. Unit-test that the queue is bounded (fill it, assert overflow policy fires — count drops).
2. Implement design B first (it's the predicted winner), then A (it's trivial), then C.
3. Build the harness with the four consumer profiles from RFC 0001 §4: no-op, 10 µs busy loop, JSON handler (`JSON.parse` + `JSON.stringify`), pathological (5 ms stall every 100 ms).
4. Measure latency by stamping `hrtime`-correlated timestamps at Rust enqueue and JS handler entry.
5. Run the full matrix (RFC 0001 §4). One command per cell; harness writes JSON results.
6. Run the **primary gate**: pathological consumer at 2× measured ceiling, 10 minutes. Watch RSS and the pressure counter.
7. Write `docs/rfcs/0001-results.md` — confront every pre-registered prediction in RFC §7.

### How to test your own work

```bash
cd spike
cargo test                     # generator bounds, encoder round-trip
node harness/index.mjs --design B --rate 100000 --payload 512 --profile noop
node harness/index.mjs --matrix           # full run, writes results/*.json
```

### Definition of done

- [ ] All three designs run the full matrix without crashing
- [ ] Latency numbers are enqueue→handler, not handler→handler
- [ ] Primary gate (saturation survival) executed and recorded for each design
- [ ] `0001-results.md` written; every prediction confirmed or refuted with numbers
- [ ] Winning design + its constants identified for graduation into `crates/node`

### Do NOT

- Add WebSocket framing, TLS, rooms, or real sockets to the spike — they contaminate the measurement
- Polish the spike code — it is throwaway-grade by design
- Start Phase 1A before the results doc exists

---

## 5. Phase 1A — Echo Server

**Goal:** one real WebSocket client connects, sends a message, gets it echoed back — through the *graduated* bridge. This proves the whole stack: TS SDK → NAPI → engine → Tokio → socket and back.

### What you build

**In `crates/core`** (uncomment the dependencies in its `Cargo.toml` — they're staged for this phase):

| File | What goes in it |
|---|---|
| `config.rs` | Already stubbed. Wire real defaults; validate on construction |
| `engine.rs` | `Engine::start(config)` boots a multi-threaded Tokio runtime on its own threads; `Engine::shutdown()` stops it. The Node event loop must never block |
| `transport/websocket.rs` | Accept loop + handshake via `tokio-tungstenite`, behind the `Transport` trait in `transport/mod.rs` |
| `connection/mod.rs` | The per-connection task: read loop, write loop, bounded send queue (the connection's "mailbox"), ping/pong keepalive, close handshake. Wrap the task so a panic tears down *one* connection, never the runtime |
| `connection/registry.rs` | Sharded slab: `ConnectionId` = shard index + slab key. O(1) lookup, IDs recycled |
| `events.rs` | Already stubbed: `EngineEvent` variants flow to the bridge |

**In `crates/node`:** graduate the Phase 0 winner into `bridge.rs` with its measured constants (cite the benchmark in a comment). `buffers.rs` gets the copy-vs-external-buffer threshold from the spike, also cited.

**In `packages/beamsocket`:** make these real (they currently throw): `new BeamSocket(config)`, `io.listen(port)`, `io.on('connection')`, `socket.on('message'|'close')`, `socket.send()`, `socket.id`.

### How to test

```bash
cargo test -p beamsocket-core        # unit: registry recycling, queue bounds, config validation
npm run build -w beamsocket
node packages/beamsocket/__tests__/echo.integration.mjs   # real client ↔ echo
docker run ... crossbario/autobahn-testsuite              # protocol correctness (CI job)
```

Required tests:
- **Unit (Rust):** registry insert/remove/recycle under concurrency; send-queue overflow triggers the configured policy; a deliberately panicking connection task doesn't kill the engine.
- **Integration (JS):** connect with the `ws` client package, echo text and binary, clean close both directions.
- **Protocol:** Autobahn test suite green (excluding compression cases — permessage-deflate is off in Phase 1).
- **Informational:** RSS with 10k idle connections, recorded in the PR (target context: <20 KB/conn).
- **Informational:** JS→Rust call microbench (`send`/`join` napi call cost) — the cheap follow-up accepted in `0001-results.md` §"JS→Rust direction".

Housekeeping allowed at phase start (pre-approved, separate commit): fix the two `cargo fmt` findings in the scaffold stubs; flip `crates/node` to `cdylib` + enable the napi deps (documented first step).

### Definition of done

- [ ] Echo works from a stock `ws` client, text + binary
- [ ] Autobahn green in CI
- [ ] Connection-task panic is contained (test proves it)
- [ ] No unbounded queue anywhere (`grep` for `unbounded` is part of review)
- [ ] 10k-idle-connection RSS number recorded

### Do NOT

- Implement rooms, users, or presence "while you're in there"
- Call JS for ping/pong or close bookkeeping — that's per-message work, Rule 1

---

## 6. Phase 1B — Rooms & Broadcast

**Goal:** `io.toSocket()`, `io.toRoom().except()`, `io.broadcast()` — with fan-out fully in Rust — and the first honest public benchmark.

### What you build

- `rooms.rs`: sharded registry (`DashMap<RoomId, RoomShard>`), **bidirectional** membership (room→conns and conn→rooms) so disconnect cleanup is O(rooms of that connection). Rooms auto-create on first join, auto-destroy on last leave.
- `broadcast.rs`: serialize the payload **once** into `Bytes`, clone the refcounted handle into each recipient's send queue. One allocation regardless of recipient count.
- SDK: `socket.join/leave`, `io.toSocket/toRoom/broadcast`, `.except()`.
- `benchmarks/`: density + fan-out suites vs `ws`, Socket.IO, uWebSockets.js. Pin the reference box in `benchmarks/README.md`. Publish losses as prominently as wins.

### Required tests

- Property test: after any join/leave/disconnect sequence, room→conn and conn→room views agree, and no empty room survives.
- Fan-out correctness: every member receives exactly once; `except` honored; non-members receive nothing.
- Broadcast to a room with a saturated (slow) member: slow member hits its backpressure policy; everyone else is unaffected.
- Benchmark: 100k-member fan-out; compare against the <150 ms target (ARCHITECTURE §5).

### Definition of done

- [ ] All targeting verbs work end-to-end; fan-out never enters JS
- [ ] Property + correctness tests green
- [ ] Benchmark published with uWS included, reference box pinned

---

## 7. Phase 1C — Identity & Admission Limits

**Goal:** `User` becomes real (Rule: it's a first-class primitive), and the runtime protects itself.

### What you build

- SDK `io.authorize(fn)`: runs in JS **once per connection** at upgrade time. Returning `{ accept: true, userId }` binds the connection.
- `identity.rs`: sharded `DashMap<UserId, HashSet<ConnectionId>>`. Bind at accept, unbind at disconnect. Backs `io.toUser()` (all devices of a user).
- `limits.rs`: enforced **in Rust, before any JS runs** — `maxConnectionsPerIp`, `maxPayloadBytes`, `maxRoomsPerConnection`.
- `trustProxy`: `false | true | CIDR[]`. With a CIDR list, honor `X-Forwarded-For` only when the socket peer is inside the list. This is a security boundary — see ARCHITECTURE §4.

### Required tests

- Multi-device: one user, three connections; `toUser` reaches all three; one disconnects, `toUser` reaches two.
- Leak test: churn 10k connect/disconnect cycles; assert the identity index is empty and RSS is flat.
- Spoof test: untrusted peer sends `X-Forwarded-For` → header **ignored**, peer IP used. Trusted peer (in CIDR) → header honored.
- Per-IP limit: connection N+1 from one IP rejected with the documented close code; works in both direct and simulated-proxy topologies (Rule 3).

### Definition of done

- [ ] `toUser` fan-out entirely in Rust; identity cost measured and stated (~24–40 B/conn target)
- [ ] All four test groups green
- [ ] Rejection close codes documented in the SDK types

---

## 8. Phase 1D — Presence, Metrics, Graceful Close → `0.1.0-alpha`

### What you build

- `presence.rs`: per-connection metadata + `io.presence(room).list()` returning `{ id, userId, metadata }`.
- `metrics.rs`: lock-free atomic counters — connections, users, messagesIn/Out, bytesIn/Out, backpressure drops, `bridgePressure` (from Phase 0), room count. `io.metrics()` snapshot.
- `io.close({ timeoutMs })`: stop accepting → drain sockets → flush pending writes → close. Force-close stragglers at the timeout.
- Prebuilds: napi-rs GitHub Actions matrix for the top 6 targets (linux-gnu/musl × x64/arm64, darwin-arm64, win-x64).

### Required tests

- Presence agrees with room membership after churn (property test shared with 1B).
- `close()` drains: in-flight writes complete; new connections rejected during drain; process exits cleanly.
- **Soak:** 10 minutes at 80% ceiling — flat RSS, no GC growth, no counter drift.
- **Constant re-confirmation (release blocker):** re-run the RFC 0001 harness on the pinned reference box with the full 10-minute gate (`--gate-seconds 600`) — Phase 0 numbers came from a shared sandbox with ~2× run variance. Confirm the ≤2 ms p99 gate clears outright and lock `BRIDGE_BATCH`, `BRIDGE_FLUSH_INTERVAL`, and `EXTERNAL_BUFFER_THRESHOLD` (per `0001-results.md` §"Follow-ups").
- Install test: `npm install` of the packed tarball on each prebuild platform runs the echo example.

### Definition of done

- [ ] `0.1.0-alpha` published with prebuilds
- [ ] Soak clean; metrics documented in the README
- [ ] Every close code and every metric named in TypeScript types

---

## 9. Phase 1.1 — Attach to an Existing HTTP Server

**Goal:** `new BeamSocket({ server: httpServer })` so Express/Fastify users get one port, one TLS setup, one load balancer.

**This phase starts with RFC 0002, not code.** Handing a socket owned by Node's HTTP server to the Rust engine is genuinely tricky (fd handoff vs. stream proxying — different tradeoffs per platform). Write the RFC, get it reviewed, then implement. Exit gate: a runnable `examples/express-attach` app.

---

## 10. Testing strategy (cross-phase)

| Layer | Tool | When it runs |
|---|---|---|
| Rust unit + property | `cargo test` (+ `proptest` where noted) | every PR |
| Lint | `cargo fmt --check`, `cargo clippy -- -D warnings`, `tsc --noEmit` | every PR |
| JS integration | `node --test` against a live runtime | every PR |
| Protocol | Autobahn suite (Docker) | every PR from 1A |
| Benchmarks | `benchmarks/` suites | on demand + before each release |
| Soak | 10-min saturation runs | before each release |

**Benchmark honesty rules:** always include uWebSockets.js; publish the reference box spec; publish losses; never compare against a competitor's misconfiguration.

## 11. Working agreements (PR checklist)

- [ ] Which phase is this PR part of? (One phase per PR)
- [ ] Rule 1 audit: does any new code run JS per message without a subscription?
- [ ] New per-connection state → memory cost stated in the description (Rule 4)
- [ ] New queue/channel → bounded, with an overflow policy and a metric (Rule 5)
- [ ] Uses client IP or headers → tested behind a simulated proxy (Rule 3)
- [ ] Tests listed in this doc for the phase are green

---

## 12. Phase 2 — Runtime Maturity (observability + admin)

*(Appended after §10–§11 so merged PR references to those sections stay
stable. Phase 2 splits into 2A and 2B, one PR each.)*

**Goal:** answer "what is my runtime doing right now?" without an APM vendor,
and give operators the levers the runtime already implies. Vision reference:
`io.stats()`, `io.topRooms()`, `io.connectionCount()`, `io.memoryUsage()`,
`io.backpressureReport()`.

### The Phase 2 rule: diagnostics must be free when unused and bounded when used

1. **Zero hot-path cost.** No new per-message work, ever (Rule 1 applies to
   metrics too). Rates are derived by a 1 Hz sampler task in Rust reading the
   *existing* counters — never by instrumenting the message path.
2. **Bounded output.** Every query returns top-N with a hard cap (default 10,
   max 100). Nothing ever serializes 500k connections across the bridge. A
   diagnostic that can dump the world is an outage waiting for a keystroke.
3. **Copy-out discipline.** Registry/room iteration follows the 1B pattern —
   per-shard copy-out, merge, release; never hold a shard lock while touching
   another shard or the bridge.

### 12.1 Phase 2A — Observability read surface

| API | Source | Cost note (Rule 4) |
|---|---|---|
| `io.stats()` | uptime + existing counters + 1 Hz EWMA rates (msgs/s, bytes/s in/out) | sampler: one task, ~0 B/conn |
| `io.topRooms(n)` | per-shard partial top-N merge by member count + msgs/s | +8 B/room (one message counter/room) — stated |
| `io.connectionCount()` | registry len | free |
| `io.memoryUsage()` | structural model (1D memory table) × live counts + measured mailbox bytes-in-flight | estimates labeled as estimates |
| `io.backpressureReport({topN})` | worst mailboxes: depth, HWM %, drops, socketId, userId | reads existing per-conn gauges, no new state |
| `io.user(id).connections()` | identity index (the 1C promise) | free |
| `io.room(id).info()` | member count, msg counter | free |
| `io.metricsText()` | Prometheus text exposition of `metrics()` (ARCHITECTURE §2.1 promise) | free |

**Required tests:** rates move under load and decay to zero after; `topRooms`
agrees with a reference model under proptest churn; `backpressureReport`
surfaces a deliberately-slowed consumer as the top offender; every query
enforces its cap (ask for 1e9, get 100); `user().connections()` matches the
multi-device tests; **perf regression guard:** echo throughput/p99 on the 1D
baseline unchanged within noise with the sampler running (the zero-hot-path
proof).

**DoD:** every 2A field named in `types.ts`; sampler interval configurable and
documented; PR notes in the 1.1 format.

### 12.2 Phase 2B — Admin actions

| API | Semantics |
|---|---|
| `io.disconnectSocket(id, code?)` | close one connection (default 1000), full 1C/1D cleanup path |
| `io.disconnectUser(userId, code?)` | close every device; identity entry gone after (the 1C promise) |
| `io.closeRoom(room, code?)` | disconnect-free: `leave` all members, destroy the room |

All three are Rust-side sweeps (one FFI call), reusing the disconnect/cleanup
paths the phases already proved; **no new teardown logic** — if an admin verb
needs new cleanup code, that's a smell that the existing path leaks.

**Required tests:** disconnectUser drops all devices and empties the identity
entry; closeRoom leaves connections alive but the room gone (bidirectional
views agree — extend the 1B proptest); admin verbs during `close()` drain are
safe no-ops; each verb's close code lands on the client.

**DoD:** verbs in types.ts with their codes; Rule 4: zero new per-conn state;
PR notes honest.

### Explicitly NOT Phase 2
Clustering (Phase 3), distributed presence (Phase 4), new transports
(Phase 5), engine TLS (RFC 0003), Windows attach — parked, by name.

---

## 13. Phase 3 — Cluster Mesh (implementing frozen RFC 0004)

**The RFC is the spec** — this section only defines the sub-phase split,
the per-phase gates, and what graduates from `spike/mesh/`. All §1 and §12
rules apply across nodes; RFC 0004's freeze is conditional on the
real-hardware 30-minute soak, so **no release claims cluster support until
that soak is green** (it shares the pinned-box trip with the alpha blockers).

New crate: `crates/mesh` — pure Rust, no NAPI (same rationale as core);
core depends on it behind the Engine facade, exactly where the RFC attaches.

### 13.1 Phase 3A — Mesh link layer
Wire framing + HELLO/CHALLENGE/AUTH handshake (§4.4/§4.7 as hardened at
freeze), sender-suppression + feature-bit intersection, coalesced link
writer (the spike-forced requirement — per-frame writes failed the gate),
per-peer byte-bounded drop-and-count queues + per-peer pressure gauges.
**Gates:** N/N−1 interop matrix test; **downgrade-tamper test** and
**reflection test** (named at freeze); cross-cluster-name refusal;
link-saturation drop-and-count; unknownFrames==0 under mixed-feature load;
relay-cell microbench reproduces the spike's <1 ms p99 with the coalesced
writer.

### 13.2 Phase 3B — SWIM membership
Graduates the spike's SWIM with the TUNED parameter row (cited to
`0004-results.md` — the literature row failed the detection gate),
**push-pull join** (the stuck-entry fix), UDP probe-only frozen format with
per-packet HMAC, membership events into `stats()`/`metricsText()`.
**Gates:** cold-start convergence < 2 s (5 nodes); kill detection < 5 s;
partition → island → heal with zero stuck entries (**the spike's negative
result becomes a permanent regression test**); frozen-format golden-bytes
test (probe packets byte-identical across builds).

### 13.3 Phase 3C — Interest routing
Edge-triggered interest add/remove + per-origin seq + anti-entropy digest;
interest map; `cluster.routing: 'interest' | 'flood'` (flood is the
operational fallback lever, documented, never default).
**Gates:** routing correctness vs a flood reference model under proptest
churn (join/leave/partition); the byte-reduction cell re-measured (spike
baseline: 22×); digest repairs a deliberately corrupted interest map.

### 13.4 Phase 3D — Relay verbs + engine integration
Node-prefixed ConnectionIds (`ids.ts` codec change — the §4.5 payoff, zero
SDK API break), `toRoom`/`toUser`/`toSocket`/`broadcast` cross-node at the
Engine facade, serialize-once preserved across the hop (pointer-identity
test, 1B precedent, now spanning the relay), cluster fields in
`stats()`/`metricsText()`, `cluster: { listen, seeds, secret }` config.
**Gates:** 3-node E2E — every targeting verb reaches remote members exactly
once, `except` honored across nodes; delivery semantics under partition
stated-and-tested in 1C currency (drops counted, no stronger promise);
single-node mode with no `cluster` config is bit-identical in behavior and
perf (the zero-cost-when-unused proof, §12 rule 1 applied to the mesh);
existing 112-test suite green untouched.

### Explicitly NOT Phase 3
Distributed presence/identity state (Phase 4 — the mesh carries frames, not
state), mTLS (RFC 0003 seam), node autodiscovery beyond seeds, N > 50
topologies, any delivery guarantee stronger than single-node 1C semantics.
