# Changelog

All notable changes to the `beamsocket` npm package. This project follows
[Semantic Versioning](https://semver.org/); pre-1.0 alphas may still move APIs.

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
