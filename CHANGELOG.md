# Changelog

All notable changes to the `beamsocket` npm package. This project follows
[Semantic Versioning](https://semver.org/); pre-1.0 alphas may still move APIs.

## 0.2.0 — unreleased (clustering reaches JavaScript)

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

### Release status (2026-08-20)
- ✅ Full required test matrix green (3-node JS-driven formation, every
  targeting verb cross-node, wrong-secret refusal, `kill -9` survival,
  single-node zero-cost re-proof, clean exit with mesh running) — run on
  real hardware with the rebuilt addon, 50/50 JS tests passing.
- ✅ `fmt`, `clippy --all-targets` (×3: workspace, mesh, node with
  `--features napi`), `cargo test --workspace`, `tsc`, `npm test`.
- ✅ `examples/cluster` 3-node walkthrough run by hand (found and fixed the
  cluster-mode message-drop bug above in the process).
- ⏳ Remaining: version tag + npm publish under the `alpha` dist-tag (see
  `PUBLISH.md`), then regenerate `package-lock.json` against the published
  platform packages and run the install-and-echo smoke test.
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
