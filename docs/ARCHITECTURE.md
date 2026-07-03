# BeamSocket — Phase 1 Architecture Proposal

**Status:** Draft for review
**Phase:** 1 — Core Runtime
**Author:** Principal Architecture
**Date:** 2026-07-03

---

## 1. Architectural Overview

BeamSocket is a WebSocket runtime where the **data plane lives entirely in Rust** and JavaScript acts as the **control plane**. This single decision drives everything else in this document.

```
┌─────────────────────────────────────────────┐
│  Node.js Application (business logic)       │
├─────────────────────────────────────────────┤
│  beamsocket (TypeScript SDK)                │  ← ergonomic API, types, validation
├─────────────────────────────────────────────┤
│  @beamsocket/native (NAPI-RS binding)       │  ← event batching, buffer handoff
├─────────────────────────────────────────────┤
│  beamsocket-core (Rust engine)              │  ← connections, rooms, broadcast,
│  Tokio multi-threaded runtime               │    presence, backpressure, metrics
└─────────────────────────────────────────────┘
```

### The one rule that matters

**Per-message work never crosses the FFI boundary unless the application asked for it.** Broadcasts, room fan-out, ping/pong keepalive, backpressure enforcement, and connection cleanup all execute in Rust with zero JS involvement. JavaScript is only invoked for events the application subscribed to (`connection`, `message`, `close`, custom hooks).

This is why BeamSocket can beat `ws`/Socket.IO on density: in a typical broadcast to 50k room members, `ws` does 50k JS-land sends through the event loop; BeamSocket does one FFI call and the fan-out happens across Tokio worker threads.

### Core runtime primitives

```
Connection — a live socket, owned by an isolated Rust task
User       — identity binding one or more Connections (multi-device)
Room       — named group of Connections
Message    — a frame routed between the above
```

**User is first-class, not an app-land convention.** `authorize` returns a `userId`; the engine binds the connection into a sharded `UserId → {ConnectionId}` index (unbound on disconnect); `io.toUser()` fans out to every device of that user entirely in Rust. These four primitives are the foundation for direct messaging, notifications, multi-device users, presence, and cross-node user targeting in later phases.

### Companion rules

Two rules sit alongside the fundamental rule, and carry the same weight in review:

1. **No core primitive may rely on a single global lock on a hot path.** Applies to the connection registry, user registry, room registry, and presence registry — all sharded (`DashMap` or custom sharding if benchmarks justify it). The reason is not today's `toUser().send()`; it's tomorrow's `disconnectUser()`, per-user presence, and per-user broadcast all contending on the same index.
2. **Every production safety feature must be evaluated in both direct-connect and load-balanced deployments.** Rate limiting, connection limits, IP tracking, origin checks, TLS assumptions, presence attribution — real traffic arrives as `Client → CDN → LB → Ingress → BeamSocket`, and features that assume `Client → Server` fail precisely in the deployments that matter (see `trustProxy`, §4).

### Threading model

- The Rust engine owns a **multi-threaded Tokio runtime** on its own thread pool, started lazily on `listen()`. The Node event loop is never blocked.
- **JS → Rust** calls (`send`, `join`, `broadcast`) are cheap synchronous NAPI calls that enqueue commands or mutate sharded state directly. No async bridging on the hot path.
- **Rust → JS** events flow through a single `ThreadsafeFunction` with **batched delivery**: the bridge accumulates events and flushes them as one array per flush cycle, amortizing the ~µs-scale TSFN overhead across many events.

### BEAM inspiration (applied, not cargo-culted)

Each connection is an isolated lightweight task (Tokio task ≈ BEAM process): own mailbox (bounded send queue), own lifecycle, crash isolation (a panicking connection task is caught and torn down without affecting others). Rooms are named process groups. This maps cleanly onto future distribution because the abstractions are already message-passing-shaped.

**Honesty caveat — positioning is "inspired by BEAM's architectural principles, not BEAM's fault-tolerance guarantees."** A connection-task panic is contained; Rust UB, a segfault, or OOM kills the engine *and* the Node process. BEAM survives all of those. We never claim otherwise in docs or marketing.

---

## 2. Component Breakdown

### 2.1 `beamsocket-core` (pure Rust, no NAPI dependency)

| Component | Responsibility |
|---|---|
| `engine` | Top-level lifecycle: config, runtime startup/shutdown, listener management |
| `transport` | `Transport` trait + WebSocket implementation (accept, handshake, framing). Codec abstracted behind an internal trait so tokio-tungstenite can be swapped for fastwebsockets after benchmarking |
| `connection` | Per-connection task: read loop, write loop, bounded send queue, high/low water marks, keepalive, close handshake. Registry keyed by slab index for O(1) lookup |
| `rooms` | Sharded room registry (`DashMap<RoomId, RoomShard>`). Rooms are created on first join, destroyed on last leave. Membership is bidirectional (room→conns, conn→rooms) for O(rooms) cleanup on disconnect |
| `identity` | Sharded user index (`DashMap<UserId, HashSet<ConnectionId>>`). Bound at authorize, unbound on disconnect. Backs `toUser` fan-out and future `io.user(id).connections()`. Sharded for the same reason rooms are: `toUser` is a hot path and gets no global lock |
| `limits` | Connection admission control: max connections per IP (with `trustProxy` / X-Forwarded-For support), max payload size, max rooms per connection. Enforced in Rust before any JS runs |
| `broadcast` | Fan-out engine. Serializes payload once into `Bytes`, then clones the refcounted handle into each recipient's send queue — one payload allocation regardless of recipient count |
| `presence` | Per-connection metadata + per-room presence views. Local `PresenceStore` trait implementation; the trait is the future seam for distributed presence |
| `metrics` | Lock-free atomic counters (connections, messages in/out, bytes, backpressure drops, room count). Snapshot API + optional Prometheus text exposition |
| `events` | Internal event bus: typed `EngineEvent` enum emitted toward the binding layer through an MPSC channel |

**Why core has no NAPI dependency:** it's unit-testable with plain `cargo test`, benchmarkable with criterion, and reusable for future non-Node bindings or a standalone `beamsocketd`.

### 2.2 `beamsocket-node` (NAPI-RS binding crate)

Thin by design — translation only, no logic:

- **Event bridge:** drains the core's event channel, encodes events into **one contiguous flush buffer** (RFC 0001 winning design C — "batched flat encoding"), delivers it via one `ThreadsafeFunction` call per flush; JS decodes with a cursor reader into zero-copy subarray views (zero per-message allocation). Flush triggers: batch of 256 or 1 ms timer, whichever first — both validated by the RFC 0001 sweep. The originally proposed array-of-objects design was **refuted** by measurement (capped ~250 k events/s vs C's 1.35 M+; see `docs/rfcs/0001-results.md`).
- **Buffer handoff:** external (zero-copy) buffers at/above the measured **16 KB threshold**, copy into V8 below it (GC-finalizer cost dominates small externals — RFC 0001 crossover data). Design C's per-flush buffers exceed the threshold in practice, so flushes are external and per-message data reaches handlers as subarray views. Outbound JS buffers are copied once into `Bytes` at the boundary — the single unavoidable copy, since Rust can't safely hold GC-managed memory across await points.
- **Command surface:** flat `#[napi]` functions taking primitive IDs, not object graphs. Keeps FFI marshaling trivial.

### 2.3 `beamsocket` (npm package, TypeScript SDK)

- `BeamSocketServer` — server lifecycle, listen/close, global broadcast, middleware.
- `Socket` — lightweight JS proxy around a connection ID. No per-socket native handle; methods delegate to flat native calls with the ID. Keeps JS heap cost per connection to one small object (only if the app holds a reference).
- `RoomHandle` — fluent targeting (`to(room).except(id).send(...)`).
- `Presence` — typed metadata get/set and room presence listing.
- Event demultiplexer — receives native event batches, dispatches to per-socket and server-level `EventEmitter`-compatible listeners.
- Native loader — resolves the correct prebuilt binary per platform (napi-rs standard `npm/` optional-dependency layout).

---

## 3. Folder Structure

Monorepo, Cargo workspace + npm workspaces:

```
beamsocket/
├── Cargo.toml                      # [workspace] crates/*
├── package.json                    # npm workspaces: packages/*
├── crates/
│   ├── core/                       # beamsocket-core
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── engine.rs
│   │   │   ├── config.rs
│   │   │   ├── transport/
│   │   │   │   ├── mod.rs          # Transport trait
│   │   │   │   └── websocket.rs
│   │   │   ├── connection/
│   │   │   │   ├── mod.rs          # task, read/write loops
│   │   │   │   ├── registry.rs     # slab registry
│   │   │   │   └── backpressure.rs
│   │   │   ├── rooms.rs
│   │   │   ├── identity.rs         # user → connections index
│   │   │   ├── limits.rs           # admission control
│   │   │   ├── broadcast.rs
│   │   │   ├── presence.rs
│   │   │   ├── metrics.rs
│   │   │   └── events.rs
│   │   └── benches/                # criterion micro-benchmarks
│   └── node/                       # beamsocket-node (NAPI-RS)
│       └── src/
│           ├── lib.rs              # #[napi] surface
│           ├── bridge.rs           # TSFN event batching
│           └── buffers.rs
├── packages/
│   └── beamsocket/                 # published npm package
│       ├── src/
│       │   ├── index.ts
│       │   ├── server.ts
│       │   ├── socket.ts
│       │   ├── rooms.ts
│       │   ├── presence.ts
│       │   ├── events.ts           # batch demux
│       │   ├── native.ts           # binding loader
│       │   └── types.ts
│       ├── npm/                    # per-platform prebuild packages
│       └── __tests__/
├── benchmarks/                     # vs ws, socket.io, uWebSockets.js
├── examples/
│   ├── chat/
│   └── presence-dashboard/
├── docs/
└── .github/workflows/              # CI: cargo test, autobahn, prebuilds
```

---

## 4. API Design

Familiar to anyone who has used `ws` or Socket.IO; zero Rust visible.

```ts
import { BeamSocket } from 'beamsocket';

const io = new BeamSocket({
  limits: {
    maxPayloadBytes: 1 << 20,
    maxConnectionsPerIp: 100,        // requires trustProxy behind an LB, else it misfires
    maxRoomsPerConnection: 100,
  },
  // false (default) | true | CIDR allowlist. Governs whether X-Forwarded-For
  // is the source of truth for client IP. Prefer the CIDR form in production.
  trustProxy: ['10.0.0.0/8', '172.16.0.0/12'],
  backpressure: { highWaterMark: 64 * 1024, policy: 'disconnect' },
});

// Auth hook — JS, but only once per connection (upgrade time).
// Returning userId binds the connection to a first-class User.
io.authorize(async (req) => {
  if (!allowedOrigin(req.headers.origin)) return { accept: false, code: 4403 };
  const user = await verifyToken(req.headers.authorization);
  return user
    ? { accept: true, userId: user.id, metadata: { plan: user.plan } }
    : { accept: false, code: 4401 };
});

io.on('connection', (socket) => {
  socket.join('lobby');
  socket.on('message', (data, isBinary) => socket.send(data));   // echo
  socket.on('close', (code, reason) => { /* cleanup */ });
});

// Targeting — explicit verbs, no socket/user/room namespace ambiguity.
// Every one of these is a single FFI call; fan-out happens in Rust.
io.toSocket(socketId).send(payload);
io.toUser(userId).send(payload);                    // all of the user's devices
io.toRoom('lobby').except(socketId).send(payload);
io.broadcast(payload);                              // every connection

// Presence & metrics
const members = await io.presence('lobby').list();  // [{ id, userId, metadata }]
const m = io.metrics();  // { connections, users, messagesIn, messagesOut, bytesIn, ... }

await io.listen(8080);

// Graceful shutdown: stop accepting → drain sockets → flush pending writes → close
await io.close({ timeoutMs: 30_000 });
```

Design notes:

- **`socket.id` is an opaque string**, internally a u64 slab key today. Opaque now means it can become a cluster-wide 128-bit ID in Phase 2 without breaking anyone.
- **Identity semantics:** one user, N connections (multi-device). Binding happens only via `authorize`'s returned `userId`; unbinding is automatic on disconnect. `io.user(id).connections()` and `io.disconnectUser(id)` land with the Phase 2 admin surface.
- **Delivery semantics — explicit non-goals.** Phase 1 provides *frame delivery* to a live socket, not *message delivery*: no acknowledgements, no retries, no persistence, no cross-node ordering guarantees. `send()` resolving means "accepted into the send queue," nothing more. Stated in the docs in exactly these terms so nobody assumes queue semantics.
- **`trustProxy` is a security boundary, not a convenience flag.** With `false`, the socket peer address is the client IP. With a CIDR allowlist (recommended), X-Forwarded-For is honored only when the peer is in the list. Bare `true` trusts any peer and is only safe when the runtime is unreachable except through the proxy — the docs say so loudly, because spoofed XFF otherwise bypasses every per-IP limit.
- **HTTP integration:** Phase 1 owns its port (`listen(8080)`). Phase 1.1 adds `new BeamSocket({ server: httpServer })` to attach to an existing Express/Fastify upgrade path — one deployment, one TLS setup, one load balancer. This is an adoption requirement, not a nice-to-have.
- **`authorize` replaces middleware chains** for Phase 1. Connection-time JS is fine (rare event); per-message middleware is deliberately excluded — it would drag the hot path back into JS. Revisit only with a Rust-side filter design.
- **Events use Node's `EventEmitter` contract** so existing mental models and typed-emitter patterns apply.
- **No implicit message protocol.** Phase 1 ships raw text/binary frames. A structured event protocol (Socket.IO-style named events, ack/reply) is a later layer *on top*, opt-in, so the core stays protocol-neutral for future MQTT/TCP transports.

---

## 5. Performance Considerations

**Targets (to be validated by `benchmarks/`, not claims):**

| Metric | Target |
|---|---|
| Concurrent idle connections per process | 500k+ (vs ~50–80k practical for `ws`) |
| Idle memory per connection | < 20 KB including kernel buffers (tune read buffers to 4 KB initial, grow on demand) |
| Broadcast to 100k room members | < 150 ms, zero JS event-loop stall |
| p99 echo latency at 10k conns | < 5 ms |
| Multi-core scaling | Near-linear fan-out throughput across cores (Rust plane) |
| Identity overhead | ~24–40 bytes per connection for the user index — published, not hidden |

**How the design gets there:**

- **Payload sharing:** broadcasts allocate once (`Bytes`), refcount everywhere. Fan-out cost is queue-push per recipient, not serialize-per-recipient.
- **FFI batching:** TSFN calls are the expensive unit, not events. Batching turns 10k events/sec into ~1k flushes/sec worst case.
- **Zero-copy inbound:** external buffers hand Rust memory to JS without copying. One copy outbound (unavoidable, see §2.2).
- **Sharded state:** `DashMap` for rooms avoids a global lock; the connection registry is a sharded slab; connection hot state lives inside its own task (no sharing at all).
- **Slab registry:** connection lookup is an array index within a shard, IDs are recycled, no hashing on the hot path.
- **Bounded everything:** send queues, event channel, and TSFN queue are all bounded with explicit overflow policy. Unbounded queues are how runtimes die at scale.
- **GC discipline:** the SDK allocates one small `Socket` proxy per connection *only if* the app touches it; event batch arrays are reused where NAPI allows.

**Known cost accepted:** every app-bound message pays one FFI hop and one Buffer allocation. Apps that subscribe to `message` on 500k sockets are fundamentally JS-bound; the docs must be honest that BeamSocket's wins concentrate in fan-out-heavy and connection-heavy workloads.

**The JS ceiling, stated plainly (this sentence ships in the public docs):** *BeamSocket optimizes connection management, routing, broadcasting, and networking workloads. Applications that execute substantial JavaScript per message remain limited by Node.js execution characteristics.* "Fully utilize all CPU cores" is true of the Rust plane only — application callbacks run on one event loop. Now measured, not just asserted: with a realistic `JSON.parse`+`stringify` handler, all bridge designs converge at **~75–100 k events/s per subscribed stream** — the handler, not the bridge, is the wall (RFC 0001 results, "JSON handler" section). The bridge's 5–7× advantage accrues to light handlers and to everything that never enters JS at all.

---

## 6. Scaling Considerations (designing the seams now)

Phase 1 is single-process, but three seams are built in from day one:

1. **Trait-shaped registries.** `RoomRegistry`, `PresenceStore`, and `Broadcaster` are traits with local implementations. Phase 2 clustering swaps in distributed implementations (gossip or control-plane backed) without touching connection code.
2. **Opaque, prefix-able IDs.** Connection and room IDs are opaque strings in the public API; the internal encoding can gain a node-ID prefix for cluster-wide routing with zero API change.
3. **Message-passing internals.** Components communicate via typed events and channels, not shared method calls. A channel crossing a process boundary later is an implementation detail.

Also deliberate: `SO_REUSEPORT` multi-listener support lands in the config surface now (single listener default), so multi-acceptor scaling on one box needs no redesign.

**Explicitly out of scope for Phase 1:** cross-node pub/sub, distributed presence CRDTs, sticky-session strategies, horizontal autoscaling. Designing these before a benchmarked single-node core is premature complexity.

---

## 7. Risks and Tradeoffs

| Risk | Severity | Mitigation |
|---|---|---|
| **uWebSockets.js already exists** and is fast | High (positioning) | Differentiate on DX + roadmap: TypeScript-first API, rooms/presence built-in, BEAM-style clustering path. uWS is a C++ socket library; BeamSocket is a runtime. Benchmarks must include uWS honestly |
| TSFN queue saturation under event storms | High (correctness) | Bounded queue + drop/coalesce policy + `backpressure` metric so saturation is observable, never silent |
| Chatty-FFI regression as API grows | Medium | Rule enforced in review: no per-message JS unless subscribed. Benchmark suite in CI catches regressions |
| Prebuild matrix burden (glibc/musl/arm64/win) | Medium | napi-rs standard tooling + GitHub Actions matrix; ship top 6 targets first, `napi build` fallback documented |
| permessage-deflate memory blowup (~300 KB/conn zlib contexts) | Medium | Off by default; when enabled, sliding-window limits + docs. Density claims stated without compression |
| WebSocket protocol correctness | Medium | Autobahn test suite in CI from the first release. Non-negotiable |
| Cross-language debugging burden on contributors | Medium | Strict layering (logic in core, translation in binding), tracing spans through the bridge, `RUST_LOG` passthrough |
| Codec choice regret (tungstenite vs fastwebsockets) | Low | Internal codec trait; swap is contained to `transport/websocket.rs` |

**Tradeoffs accepted:** one copy on outbound sends; no per-message middleware; raw frames instead of a message protocol in v1; Rust contribution bar is higher than a pure-JS library (acceptable — the moat *is* the Rust core).

---

## 8. Future Extensibility

- **Transports (TCP, MQTT, SSE, QUIC/HTTP-3):** everything above `transport/` operates on frames and connection IDs, not sockets. New transports implement the `Transport` trait; rooms/presence/broadcast work unchanged. QUIC arrives via `quinn` on the same Tokio runtime. **UDP is deferred to a separate RFC** — it's connectionless, which breaks the Connection primitive; it needs a session abstraction designed on its own merits, not bolted on.
- **Structured event protocol:** named events, acks, and schemas ship as an opt-in layer in the SDK (and later a Rust-side codec for hot-path filtering), keeping the core neutral.
- **Clustering (Phase 2+):** node discovery + control plane behind the registry traits (§6); room membership becomes eventually-consistent across nodes; broadcast becomes local-fanout + inter-node relay.
- **Pub/sub:** the broadcast engine generalizes to topic-based pub/sub — rooms are already topics with membership semantics.
- **Standalone daemon:** because core is NAPI-free, `beamsocketd` (config-file-driven, non-Node clients) is a packaging exercise, not a rewrite.

---

## 9. Recommended Next Steps

1. **Event bridge spike first** (RFC 0001) — the bridge is the biggest technical unknown and its saturation policy defines API semantics. No rooms/presence/targeting code until a bridge design passes the RFC's gates.
2. Scaffold the monorepo per §3 with CI (cargo test, tsc, Autobahn container); graduate the winning bridge design into `crates/node`.
3. Implement `transport` + `connection` + registry — echo server milestone, end-to-end through the validated bridge.
4. Rooms + broadcast, then the first density/fan-out benchmark vs `ws` and uWS.
5. Identity index + `toSocket`/`toUser`/`toRoom` targeting; admission limits with `trustProxy`.
6. Presence, metrics, backpressure policies, graceful `close()`; publish `0.1.0-alpha` prebuilds.
7. Phase 1.1: `{ server: httpServer }` attach for Express/Fastify integration.
