# PR: Phase 1D — Presence, Metrics, Graceful Close → 0.1.0-alpha

**Phase:** 1D only (ENGINEERING.md §8) — the alpha phase. Branch:
`phase-1d-runtime`, stacked on `phase-1c-identity`. No clustering, no HTTP
attach (1.1), no new transports.

Presence, a real `metrics()` surface, graceful `close()`, the per-connection
memory breakdown (1B review debt), the prebuild workflow, and alpha prep —
`0.1.0-alpha.0` (not published).

## Preconditions (landed first, as required)

- **Hygiene 1** (`c50d25d`): CI clippy jobs gained `--all-targets` (test/bench
  targets are now linted); fixed the pre-existing `phase1b.rs` warnings
  (`SinkExt`, `id3`, `dead_id`) and allowed `field_reassign_with_default`
  narrowly in the config-mutation tests.
- **Hygiene 2** (`20122ca`): 1C PR notes flip the Rule 4 headline to worst-case-
  first (~278 B/conn single-device → ~20.5 B amortized). The correlation-map doc
  states the decided eviction policy (FIFO evict-oldest costs at most a
  connection's metadata, never its identity); added the eviction counter.

## What's in it

- **`presence.rs` + `io.presence(room).list()`** → `[{ id, userId, metadata }]`.
  The Phase 1C consequence made explicit: **Rust owns `id` + `userId`; metadata
  lives in JS.** One FFI call returns the room's `(connectionId, userId)` pairs
  (conn→userId now stored in the registry `Entry`); the SDK joins `metadata`
  from the live `Socket` objects. Members whose metadata was evicted — or, in
  Phase 4, live on another node — join as `{}` (documented). `PresenceStore` is
  trait-shaped for the distributed swap (ARCHITECTURE §2.1). Async API.
- **`metrics()`** — one FFI call, a flat snapshot of lock-free atomics:
  `connections, users, rooms, messagesIn/Out, bytesIn/Out, backpressureDrops,
  bridgePressure, bridgeDropped, admissionRejectedIp, authorizeRejected,
  authorizeTimedOut, pendingOverflow, authMetadataEvicted`. `bridgePressure` is a
  new live gauge (in-flight ÷ capacity of the engine→bridge queue — the RFC 0001
  saturation signal made queryable). Every field is named in `types.ts::Metrics`;
  no undocumented counters.
- **`io.close({ timeoutMs })`** — stop admitting (new upgrades → **HTTP 503**,
  added to `RejectCode`) → sweep-close every socket (1001) → wait up to
  `timeoutMs` for in-flight writes to flush and sockets to close → force-close
  stragglers (1001) → stop the runtime. **The napi TSFN trap is handled:** close
  runs the drain on the libuv threadpool (`AsyncTask`, off the Node loop), then
  **joins the bridge drain thread so the `ThreadsafeFunction` is dropped** —
  after which the Node process exits on its own. The clean-exit test is a child
  process that never calls `process.exit()`.
- **Memory-budget breakdown** (`benchmarks/README.md`): the ~11.6 KB/conn idle
  baseline decomposed — kernel socket buffers (~4–5 KB), codec read buffer
  (4 KB), engine bookkeeping (**~2.5 KB measured**), Tokio task, identity (~20 B),
  JS proxy — each marked structural vs recoverable, measured where feasible.
- **Prebuild workflow** (`.github/workflows/prebuild.yml` + `npm/` layout): a
  napi-rs matrix for the 6 targets (linux gnu/musl × x64/arm64, darwin-arm64,
  win-x64), `optionalDependencies`, per-platform package stubs, a loader that
  resolves the platform package, and stage/collect scripts. YAML-validated;
  **publish is release-time, not in this PR.**
- **Alpha prep**: `0.1.0-alpha.0`, README quickstart (~30 lines: authorize +
  rooms + toUser + presence + metrics + close), `CHANGELOG.md`. **Not published.**

## Exit gates (§8 / DoD)

| Gate | Status |
|---|---|
| presence/metrics/close end-to-end; every metric & close code in `types.ts` | ✅ Rust presence proptest + JS `presence.list()`, `metrics()`, `close()`; `Metrics`/`RejectCode` fully named |
| Presence ⇄ room membership agreement after churn | ✅ `tests/phase1d.rs::presence_agrees_with_membership_and_identity` (256 proptest cases, shared Op strategy with 1B) |
| Memory table published, structural/recoverable split | ✅ `benchmarks/README.md`; anchored on measured engine-bookkeeping (~2.8 KB distinct-user) + identity (~20 B) |
| Clean-exit test green (the TSFN release proof) | ✅ `runtime.integration.test.mjs` spawns a child that opens, echoes, `close()`s, and exits `0` with no `process.exit()` |
| close(): in-flight completes, 503 during drain, stragglers 1001 | ✅ JS drain-probe test (503) + graceful sweep (1001); existing echo/rooms/identity suites still exit cleanly under the new close |
| Metrics move under the right workload | ✅ connections/users/rooms/messages/bytes move on echo+join+authorize; `admissionRejectedIp` on a per-IP reject; all fields present+numeric (backpressureDrops movement covered by the `backpressure.rs` unit tests) |
| Eviction: over-cap evicts oldest, counter moves, new conn still gets metadata | ✅ `correlation.test.mjs` (`BoundedMetaMap`, small cap) |
| Soak at ~80% ceiling, flat RSS | ⚠️ ran the env-permitted window: **12 s, 2.4 M conns churned, RSS Δ +2.6 MB (~1 B/cycle — flat), zero residual**. Full 10-min soak is a pinned-box blocker (below) |
| fmt, clippy `--all-targets -D warnings` (+napi), `cargo test --workspace`, tsc, npm test | ✅ all green (57 Rust tests, 14 JS tests) |

## Rules audit (1C-PR style)

- **Rule 1 — no per-message JS.** Presence, metrics, and close are control-plane
  calls (once per query / once per shutdown), never per message. Presence fan-out
  (the `(id, userId)` collection) runs in Rust; the metadata join is a JS-heap
  loop over an already-delivered array, not an FFI-per-member. The bridge stream
  is unchanged.
- **Rule 2 — no global lock on a hot path.** Presence reads the sharded room +
  connection registries (member list copied out, then per-shard `user_of`);
  metrics are lock-free atomics + registry `len()`s. No new shared lock.
- **Rule 3 — works behind a load balancer.** close()'s 503 is emitted at the
  same handshake gate that resolves `trustProxy`; nothing here newly assumes
  direct-connect.
- **Rule 4 — per-connection cost stated.** **Metrics: ~0 B/conn** — process-
  global atomics, no per-connection state (stated in `types.ts`). **Presence:
  ~0 B/conn of its own** — it reads the conn→userId that now lives in the
  registry `Entry`; that field is the only add, one `Option<UserId>` per
  connection (a null pointer for anonymous, a shared string for authenticated —
  the userId the identity index already holds). Full idle breakdown (~11.6
  KB/conn) is the new `benchmarks/README.md` table.
- **Rule 5 — every queue bounded.** No new queues. `bridgePressure` now makes the
  existing bounded engine→bridge queue's depth queryable; the JS correlation map
  is bounded (`BoundedMetaMap`, evictions counted). `grep -r unbounded`: zero.
- **Bridge constants unchanged.** `BRIDGE_BATCH=256`, `BRIDGE_FLUSH_INTERVAL=1
  ms`, `ENGINE_BRIDGE_QUEUE_CAPACITY=8192`, `EXTERNAL_BUFFER_THRESHOLD=16 KB`.
  The 1B lock-order invariant is untouched (presence follows the same
  copy-out-then-read discipline as fan-out).

## Close codes / HTTP statuses added

| Situation | Code | Layer |
|---|---|---|
| New upgrade during a graceful `close()` drain | **HTTP 503** | HTTP upgrade (no WebSocket) |
| Graceful close of an existing / straggler socket | 1001 (going away) | WebSocket close |

(1D adds no new *authorize* codes; 1C's `RejectCode` set is unchanged apart from
`SERVICE_UNAVAILABLE`.)

## Release blockers (named; not executable in this sandbox)

- Pinned-box constant re-confirmation (`0001-results.md` "Follow-ups",
  `--gate-seconds 600`).
- Pinned-box benchmark suite: the 100k-member fan-out gate (<150 ms,
  ARCHITECTURE §5), Socket.IO ≥25k, echo p99 <5 ms.
- Full **10-minute** soak at 80% ceiling (this PR ran the env-permitted 12 s).
- **Actual npm publish** + per-platform install test (the prebuild workflow is
  written and YAML-validated; publish needs a tag + `NPM_TOKEN`).

## Deviations / follow-ups (honest)

- **Soak duration** is the sandbox-permitted 12 s, not 10 min — same constraint
  and precedent as Phase 0's 36 s foreground gate. Numbers are directional.
- **RSS-flatness is recorded, not asserted** (allocator page-granularity);
  the logical invariants (registry drains to 0, rooms to 0) ARE asserted.
- **`backpressureDrops` movement** is asserted at the Rust mailbox level
  (`backpressure.rs`), not re-triggered in the JS metrics test (forcing a slow
  consumer over localhost is timing-fragile); every field is still checked
  present+numeric in `metrics()`.
- **Engine bookkeeping ~2.5 KB/conn** is higher than the raw struct sizes because
  the per-connection control + close channels are always allocated — flagged in
  the memory table as the recoverable lever (lazy control-path allocation) for a
  1.x density pass, alongside the 4 KB codec read buffer.
- Prebuild `.node` binaries are git-ignored (`*.node`); only the `npm/` package
  stubs + scripts + workflow are committed.

## PR checklist (§11)

- [x] One phase per PR — Phase 1D only (no clustering / HTTP attach)
- [x] Rule 1 audit above — presence/metrics/close are control-plane
- [x] Rule 4: metrics ~0 B/conn, presence ~0 B/conn of its own; idle breakdown published
- [x] Rule 5: no new queues; correlation map bounded + counted; `grep unbounded` zero
- [x] Rule 3: close 503 rides the same trustProxy-aware gate
- [x] §8 tests green: `cargo fmt --check`, `clippy --all-targets -D warnings`
      (+ `--features napi`), `cargo test --workspace` (57 tests), `tsc --noEmit`,
      `npm test` (14: api-surface + correlation + echo + rooms + identity + runtime)
