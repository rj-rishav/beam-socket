# PR: Phase 2A — Observability Read Surface

**Phase:** 2A only (ENGINEERING.md §12). Branch: `phase-2a-observability`, from
`master`. A **read-only** diagnostics surface — eight queries — plus a 1 Hz rate
sampler. No admin verbs (that's 2B), no clustering hooks, no per-connection rate
tracking. The governing constraint is **zero hot-path cost**: nothing here adds
per-message work.

## What's in it

Eight queries, each **one FFI call**, each shape mirrored in `types.ts`:

- **`io.stats()`** — every `Metrics` field + `uptimeMs` + `rates` (the sampler
  EWMAs; `null` when the sampler is off).
- **`io.connectionCount()`** — live gauge.
- **`io.topRooms(n = 10)`** — top rooms by members, then messages, then name;
  bounded (cap 100).
- **`io.room(id).info()`** — `{ room, members, messages, exists }`.
- **`io.user(id).connections()`** — the user's live device socket ids (the 1C
  identity promise).
- **`io.memoryUsage()`** — structural model × live counts + measured mailbox
  bytes-in-flight, labeled `estimated: true`.
- **`io.backpressureReport({ topN = 10 })`** — the `topN` deepest send queues
  (`socketId`, `userId`, `depthBytes`, `hwmBytes`, `hwmPercent`) + a global
  `totalDrops`; bounded (cap 100).
- **`io.metricsText()`** — Prometheus text exposition, `beamsocket_` prefix,
  `# HELP`/`# TYPE`, rate windows via `{window="1s"|"10s"}`.

Mechanism:

- **1 Hz sampler (`crates/core/src/sampler.rs`).** The only new task. It reads
  the EXISTING counters once per `observability.samplerMs` (default 1000, `0`
  disables — no task, `rates` absent) and writes EWMAs (τ = 1 s / 10 s) into a
  single process-global `Rates` (8 `AtomicU64`, f64-as-bits). Spawned in
  `engine.rs` start() only when `sampler_ms > 0` (`engine.rs:226`). Never locks,
  never touches the message path.
- **Per-room message counter (Rule 4, +8 B/room).** `RoomEntry.messages:
  AtomicU64` (`rooms.rs:43`). Incremented inside the existing broadcast fan-out
  where the room is **already resolved** — `record_and_members` **replaces** the
  old `rooms.members()` lookup (`broadcast.rs:67`): same map hit, one extra
  atomic add, no new lookup, and the echo path never touches it.
- **Bounded top-N.** `topRooms` / `backpressureReport` each build a transient
  min-heap of capacity `top_n + 1` (`rooms.rs:154`, `engine.rs:524`), copy-out
  per entry under its own shard lock, merge outside all locks. Cap clamped to 100
  by `clamp_top_n` (`binding.rs:749`); `0`/negative → error.
- **Memory model.** `estimatedHeapBytes = conns·6600 + rooms·160 + users·40`
  (`engine.rs:137`, constants from the 1D memory table) + summed live
  `mailbox.queued_bytes()`.

## Exit gates (DoD §12.1)

| Gate | Status |
|---|---|
| All 8 APIs end-to-end, every shape in `types.ts` | ✅ `observability.integration.test.mjs` exercises each through the SDK |
| Rates rise under load, decay to ~0 within 3 windows | ✅ `stats(): rates rise … decay` (drives echo ~1.2 s, then asserts 1s-rate < 30% of peak after 3.2 s) |
| `topRooms` vs reference model under churn | ✅ `phase2a.rs::top_rooms_matches_reference` (proptest, 256 cases, join/leave/disconnect/broadcast vs a HashMap reference) |
| `backpressureReport` surfaces a slowed consumer as top offender, with `userId` | ✅ paused client + ~12 MB push → top mailbox is that socket, `userId="laggard"`, `depthBytes>0` |
| Cap enforcement (1e9 → ≤100; 0/neg → error) | ✅ `topRooms cap` + `topRooms(0)`/`(-5)`/`backpressureReport({topN:0})` throw |
| `user().connections()` matches 1C multi-device | ✅ two `alice` devices listed, drops to one on close |
| `metricsText()` parses | ✅ strict per-line regex: every sample has a preceding `# TYPE`, numeric values, `beamsocket_*` names + `{window="1s"}` |
| Sampler off (`samplerMs:0`) → `rates` null, no crash | ✅ `sampler off` test + `phase2a.rs::sampler_on_off_controls_rates_presence` |
| **PERF REGRESSION GUARD (zero-hot-path proof)** | ✅ **see numbers below** |
| fmt, clippy `--all-targets -D warnings` (+`--features napi`), `cargo test --workspace`, `tsc --noEmit`, `npm test` | ✅ 51 Rust tests, 35 JS tests, all green |

### Perf regression guard — the zero-hot-path proof

Echo round-trip, sampler **ON** (`samplerMs:1000`) vs **OFF** (`samplerMs:0`),
same process, back-to-back (`PERF GUARD` test):

```
throughput  off = 381 msg/s   on = 386 msg/s
p99         off = 2.995 ms     on = 2.971 ms
```

ON and OFF are statistically indistinguishable (ON marginally *faster* — within
noise). This is the proof that the sampler and the room counter add no hot-path
cost: the sampler runs off-thread over existing atomics, and the counter rides an
already-resolved lookup on the room-broadcast path (never the echo path). Numbers
are sequential-RTT-derived (latency-bound), so the absolute figure is a
comparison baseline, not a peak-throughput claim; the ON≈OFF equality is the
gate.

## Rules audit (§12 carries PR-rejection weight)

- **Rule 1 — zero hot-path cost.** No new per-message JS: the eight queries are
  pull-based, called on demand. No new per-message Rust instrumentation: the only
  write-path change is the room counter, and it **rides the existing fan-out**
  (`record_and_members` replaced `members()` at `broadcast.rs:67` — same map
  access, one atomic add) and only on room broadcasts; echo/direct sends never
  touch it. The sampler reads counters off-thread once per second. Proven by the
  perf guard above.
- **Rule 4 — per-connection cost.** Rooms grow by **+8 B each**
  (`RoomEntry.messages: AtomicU64`); an idle/auto-destroyed room costs nothing.
  Per-**connection** sampler cost is **~0 B**: rates are ONE process-global
  `Rates` (8 atomics total), not per-connection state.
- **Rule 5 — every queue bounded.** No new queues. The top-N merges are transient
  bounded `BinaryHeap`s (capacity `top_n + 1`), dropped at end of call; `top_n`
  is clamped to 100. `grep -rn unbounded`: zero.
- **Bridge constants untouched.** `BRIDGE_BATCH`/`FLUSH_INTERVAL`/queue capacity/
  `EXTERNAL_BUFFER_THRESHOLD` unchanged; the 1B per-shard copy-out lock invariant
  is preserved by every new query (copy-out under a shard lock, merge/format
  outside all locks, never two shard locks at once, never a lock across the
  bridge).

## Deviations / follow-ups (honest)

- **`backpressureReport` per-connection drops → global `totalDrops`.** The
  mailbox has no per-connection drop counter and §12.1 forbids adding new state,
  so per-mailbox rows report depth/HWM only and drop *attribution* is the global
  `totalDrops` at the envelope. Per-connection drop counters are a 2B candidate.
- **`user().connections()` is not top-N.** It returns *all* of one user's live
  devices. That set is naturally bounded (devices per user, not a scan of all
  connections), so it needs no cap; the top-N discipline applies to the
  cross-population scans (`topRooms`, `backpressureReport`).
- **Memory figure is a model, not a measurement.** `estimatedHeapBytes` is
  structural constants (from the 1D memory table) × live counts; only
  `mailboxBytesInFlight` is measured. Hence `estimated: true` always.
- **Perf guard is an in-run ON-vs-OFF comparison** (sequential RTT), not a
  re-run of the 1D standalone throughput bench — the same-process A/B removes
  sandbox variance and is the cleanest apples-to-apples zero-hot-path proof.
- **napi digit-boundary field names.** napi renders `messages_in_1s` as
  `messagesIn1S` (capital `S` after the digit); the SDK maps these to the public
  `{ perSec1s, perSec10s }` shape. Noted so the native/SDK boundary stays in sync.

## PR checklist (§12)

- [x] One phase per PR — Phase 2A read surface only (no admin verbs, no
      clustering, no per-connection rate tracking)
- [x] Rule 1 — no per-message JS and no per-message Rust instrumentation; counter
      rides the existing fan-out; perf guard proves it
- [x] Rule 4 — +8 B/room stated; sampler ~0 B/conn
- [x] Rule 5 — no new queues; top-N merges transient/bounded; `grep unbounded` zero
- [x] Bounded output — every scan query clamps to 100 (default 10); 0/neg errors
- [x] Copy-out discipline — per-shard copy-out, merge outside locks (1B pattern)
- [x] Gates green: `cargo fmt --check`, `clippy --all-targets -D warnings`
      (+ `--features napi`), `cargo test --workspace` (51), `tsc --noEmit`,
      `npm test` (35: + observability.integration)
