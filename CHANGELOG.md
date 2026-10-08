# Changelog

All notable changes to the `beamsocket` npm package. This project follows
[Semantic Versioning](https://semver.org/); pre-1.0 alphas may still move APIs.

## 0.2.1 — 2026-10-08 (hardening: memory, backpressure, honest benchmarks)

### Fixed
- **Cluster mesh binds TCP before UDP on an ephemeral port**, with bounded
  retries. Binding UDP on `:0` and then TCP on the same port could fail with
  `AddrInUse` on a busy host (many TIME_WAIT sockets).
- **Relayed `toSocket` now honours the `Disconnect` backpressure policy.** A
  cross-node `socket.send` that overflowed a `Disconnect`-policy mailbox
  closed the queue without signalling a close, leaving a half-dead
  connection that later reported 1006 instead of 1013. All push sites now
  share one helper (`ConnHandle::push`).
- **Empty frames are bounded by the send budget.** Zero-length frames cost 0
  budget bytes, so a slow reader could queue an unbounded number of them.
  Each frame is now charged at least 1 byte; non-empty frames are unaffected.
- **Benchmark harness: 16 KiB throughput was invalid.** The BeamSocket bench
  server used the default 64 KiB `Disconnect` budget (ws/uws buffer without
  limit), so every pipelined 16 KiB echo client was closed with 1013 within
  about a second, and `driver.mjs` counted lost replies (one `once` listener per
  request). Echoes are now correlated FIFO and the bench server matches the
  other servers' buffering. All earlier `throughput16k` numbers are void.

### Performance
- **Adaptive exclusion index for fan-out.** `.except()` lists of 32+ entries on
  targets of 64+ recipients use a temporary sorted index: 2.07–7.09× faster
  core fan-out for 256+ exclusions, neutral for small lists.
  Report: `docs/reports/adaptive-fanout-exclusions.md`.
- **Codec read buffer 4 KiB → 1 KiB: ~23% less idle memory per connection**
  (10k-connection harness, 10 alternating A/B rounds), ~2% median 16 KiB echo
  throughput cost. Report: `docs/reports/0.2.1-hardening.md`.

### Changed
- **Fixed-resource validation pipeline.** `scripts/docker-fixed-test.sh` runs
  the full test matrix and a previous-vs-current end-to-end A/B in Alpine
  containers pinned to 2 CPUs / 2 GiB (also a CI workflow). Under it, this
  release vs 0.2.0: −27% idle memory per connection (8/8 rounds), echo
  throughput unchanged, 176/176 Rust and 51/51 JS tests.
  See `docs/reports/containerized-validation.md`.
- **Test reliability:** the mesh saturation test no longer assumes kernel
  socket buffers are smaller than 4 MB, and a core teardown test no longer
  retains the engine.
- **Benchmark harness: 16 KiB large-frame throughput gate.**
  `benchmarks/driver.mjs`'s `measureThroughput` now takes a `payload`
  argument; the driver runs it at the existing 64 B baseline and again at
  16 KiB (`result.throughput16k`), a permanent regression check for any
  change to the codec's read-buffer sizing.

### Investigated, not shipped
- **Codec read-buffer shrink (0.3.0 Task 3a)** — *superseded: shipped above
  after re-measurement; the rejection below rested on the invalid 16 KiB
  benchmark.* Tried cutting
  `READ_BUFFER_SIZE` (tungstenite's codec read chunk,
  `crates/core/src/transport/websocket.rs`) from 4 KiB to 1 KiB for a
  further density win. Real ~27% per-connection memory reduction, but
  20-46% echo throughput regression on 16 KiB payloads (more, smaller
  `read_from` calls per large frame) — breaches the performance plan's
  explicit large-frame gate. Reverted; `READ_BUFFER_SIZE` is unchanged at
  `4 * 1024`. Full measurement and reasoning:
  `docs/reports/0.3.0-task3a-readbuffer.md`.

## 0.2.0 — 2026-08-20 (clustering reaches JavaScript)

Cluster mesh (RFC 0004, Phase 3) is now reachable from plain JS config. Core
already carried the mesh since Phase 3D; this release is the addon + SDK
wiring that makes it usable without touching Rust.

### Added
- **`new BeamSocket({ cluster: {...} })`** — `nodeId`, `listen`, `seeds`,
  `secret`, `clusterName`. Absent `cluster` is single-node: no mesh, no cost
  (unchanged from every prior release). A present `cluster` with a missing/
  empty `secret`, an out-of-range `nodeId`, or an empty `listen` throws at
  construction, before any FFI call.
- **Cross-node fan-out** — `toRoom`/`toUser`/`broadcast` relay to every node
  that hosts the target, exactly once per member, in addition to their
  existing local fan-out. `toSocket(id)` routes to the owning node when `id`
  names a socket on another cluster member.
- **`socket.id` node prefix** — three-segment (`node-hi-lo`) when clustered;
  unchanged two-segment form in single-node mode (byte-identical to every
  prior release).
- **`io.stats().cluster`** — `nodeId`, `peers`, `relayIn`, `relayOut`,
  `relayDrops`, `peerPressures`. `undefined` when single-node.
- **`examples/cluster`** — three processes on loopback, seeded into one
  cluster, with a `ws` client per node demonstrating cross-node chat.

### Performance
- **Adaptive bridge flush** — the Rust→JS event bridge no longer waits out
  its 1 ms batch timer when there's nothing to batch with: after the first
  event of a batch it greedily drains whatever is already queued, and if the
  queue is dry with a small batch, flushes immediately. Measured same-box
  (3 runs each side): echo p50 2.19 ms → 1.03 ms (−53%), echo throughput
  +89%, 1k-member fan-out −35%. High-load batching behavior is unchanged —
  the RFC 0001 constants (`BRIDGE_BATCH`, `BRIDGE_FLUSH_INTERVAL`, queue
  capacities) are untouched, and the fast path never fires once a burst
  exceeds 16 events. Full report: `docs/reports/0.3.0-task1-flush.md`.

### Fixed
- **Cluster mode silently dropped every client-sent message.** With
  `cluster` configured, `socket.id` is node-prefixed (three-segment), but
  the server's internal connection lookup on the bridge's message/close/
  presence paths used a second, independent two-segment key builder — the
  keys never matched, so `socket.on('message', …)` never fired and close
  cleanup never ran, under cluster config only. Single-node was unaffected
  (the two encodings coincide there), which is why the automated suite
  missed it: every cluster test drove server-initiated fan-out, never a
  client message. Found running `examples/cluster` by hand; fixed by
  deriving the lookup key from the same `encodeSocketId` that builds
  `socket.id`, with a regression test.
- **`npm test` on Node ≥ 24.** The test script passed a bare directory to
  `node --test`, which newer Node no longer auto-discovers; now an explicit
  glob.
- **`except()` honored across nodes.** Found while wiring the addon: the
  Phase 3D `Engine` facade stamped every excepted connection with the
  *sending* node's id, and a receiving peer only kept except entries tagged
  with *its own* node id — so an except naming a remote socket could never
  match and was silently dropped. The existing 3D gate test didn't catch
  this because it drove the mesh/relay layer directly rather than through
  `Engine`. Fixed by carrying a genuinely node-tagged except list end to end;
  the existing local-only except array is untouched (same wire shape, same
  cost) so single-node behavior does not change.

### Changed
- Vendored HMAC-SHA256/SHA-256 in `crates/mesh` replaced with the audited
  `hmac`/`sha2` crates (the swap promised in the Phase 3A PR notes). Same
  FIPS 180-4 / RFC 4231 known-answer vectors regression-test the new impl;
  constant-time verification unchanged.

### Release status
- ✅ Full required test matrix green (3-node JS-driven formation, every
  targeting verb cross-node, wrong-secret refusal, `kill -9` survival,
  single-node zero-cost re-proof, clean exit with mesh running) — run on
  real hardware with the rebuilt addon, 50/50 JS tests passing.
- ✅ `fmt`, `clippy --all-targets` (×3: workspace, mesh, node with
  `--features napi`), `cargo test --workspace`, `tsc`, `npm test`.
- ✅ `examples/cluster` 3-node walkthrough run by hand (found and fixed the
  cluster-mode message-drop bug above in the process).
- ✅ Published 2026-08-20 to npm under the `alpha` dist-tag; tarball
  `beamsocket-0.2.0.tgz`, integrity verified. `package-lock.json` is
  pinned to the published platform packages.
- Still `alpha`, not `latest`: the pinned-box benchmark gates and RFC 0004's
  30-minute mesh soak remain open (real-hardware work, tracked in
  `docs/plans/0.3.0-performance.md` Task 4) — same honesty bar 0.1.0-alpha.0
  shipped under.

## 0.1.0-alpha.0 — unreleased (Phase 1D)

First tagged alpha. Single-process; the whole per-message data plane runs in
Rust, off the Node event loop.

### Added
- **Presence** — `io.presence(room).list()` → `[{ id, userId, metadata }]`.
  Rust returns the room's `(id, userId)` pairs in one FFI call; the SDK joins
  `metadata` (which lives in JS). Members whose metadata was evicted join as `{}`.
- **Metrics** — `io.metrics()`, a one-FFI-call snapshot of lock-free counters:
  `connections`, `users`, `rooms`, `messagesIn/Out`, `bytesIn/Out`,
  `backpressureDrops`, `bridgePressure`, `bridgeDropped`, `admissionRejectedIp`,
  `authorizeRejected`, `authorizeTimedOut`, `pendingOverflow`,
  `authMetadataEvicted`. Every field is documented; there are no hidden counters.
- **Graceful close** — `io.close({ timeoutMs })`: stop accepting (new upgrades
  get HTTP 503), drain in-flight sockets, force-close stragglers at the timeout
  with 1001, then release the runtime — the Node process exits on its own.
- **Prebuild workflow** — napi-rs GitHub Actions matrix for the top 6 targets
  (linux gnu/musl × x64/arm64, darwin-arm64, win-x64) + the `optionalDependencies`
  layout and a platform-package resolver in the loader. (Publish is release-time.)
- Memory-budget breakdown table (per idle connection) in `benchmarks/README.md`.

### Changed
- `metrics()` and `presence()` now require a running server (they throw before
  `listen()`), consistent with the other targeting verbs.

### Earlier phases (pre-alpha, summarized)
- **1C** — identity (`authorize` → `toUser`), `trustProxy`, `maxConnectionsPerIp`,
  `maxRoomsPerConnection`, `maxPayloadBytes`; rejection codes in `RejectCode`.
- **1B** — rooms + broadcast (`toSocket`/`toRoom().except()`/`broadcast`), fan-out
  entirely in Rust; first honest benchmark vs ws / Socket.IO / uWebSockets.js.
- **1A** — echo server end-to-end through the graduated RFC 0001 bridge.

### Release blockers before `0.1.0` (see the Phase 1D PR notes)
- Pinned-box confirmation of the RFC 0001 constants (full 10-minute gate).
- Pinned-box benchmark suite (100k fan-out < 150 ms, Socket.IO ≥ 25k, echo p99).
- Full 10-minute soak at 80% ceiling.
- Actual npm publish + per-platform install test.
