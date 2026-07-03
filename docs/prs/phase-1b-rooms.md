# PR: Phase 1B — Rooms & Broadcast

**Phase:** 1B only (ENGINEERING.md §6). No identity, limits, or presence.
Branch: `phase-1b-rooms`, stacked on `phase-1a-echo` (1A's Autobahn gate
closes on its first CI run; nothing here touches protocol framing).

`io.toSocket()`, `io.toRoom().except()`, `io.broadcast()` — fan-out fully in
Rust — plus the first honest public benchmark vs ws / Socket.IO / uWS.

## What's in it

- **Codec bump (prerequisite):** tokio-tungstenite 0.24 → 0.29. Payloads are
  refcounted `Bytes` end-to-end (codec read buffer → EngineEvent → mailbox →
  codec write), which is what makes "one allocation regardless of recipient
  count" true at the socket, not just in the queue. Contained to
  `transport/` + payload types, per the ARCHITECTURE §7 codec-swap seam.
  Codec read buffer set to 4 KB initial (ARCHITECTURE §5); all 1A tests green
  on the new codec.
- **`rooms.rs`:** sharded `DashMap<RoomId, HashSet<ConnectionId>>` (Rule 2),
  bidirectional membership — conn→rooms lives in the connection registry
  entry, so disconnect cleanup is O(rooms of that connection). Auto-create on
  first join, auto-destroy on last leave. **Lock-order invariant documented
  in the module:** membership mutations run under the conn shard lock and
  touch the room map from there (conn-shard → room-map, never reversed);
  fan-out copies member lists out and releases the room guard before any
  conn lock. The conn shard lock serializes a connection's membership, which
  closes the join-vs-disconnect ghost-membership race (`remove_full` takes
  the room set out atomically).
- **`broadcast.rs`:** payload copied ONCE at the FFI boundary, then a
  refcount clone per recipient's bounded mailbox; `except` filtered during
  fan-out; slow members hit their own overflow policy (non-blocking pushes).
  `FanoutReport { attempted, queued, backpressured, missing }` surfaced.
- **napi/SDK:** flat `join`/`leave`/`broadcastRoom`/`broadcastAll` (+ text
  fast paths; `except` crosses as flat u32 id-half pairs). `Target` is real
  for `toSocket`/`toRoom`/`broadcast` with `.except()`; `socket.join/leave`.
  Every targeting verb is ONE FFI call regardless of recipient count.
- **Benchmarks:** `benchmarks/fanout.mjs` (+ per-framework servers, client
  workers) measuring client-observed fan-out completion and idle RSS;
  results + method + caveats in `benchmarks/README.md`.

## Exit gates (§6)

| Gate | Status |
|---|---|
| All targeting verbs end-to-end; fan-out never enters JS | ✅ rooms integration test (JS) + engine test (Rust); one native call per verb |
| Property test: views agree, no empty room survives | ✅ `tests/phase1b.rs`, 256 proptest cases over join/leave/disconnect sequences + final sweep asserts `room_count == 0` |
| Fan-out: exactly once, `except` honored, non-members nothing | ✅ including a **pointer-identity assertion** that every recipient's frame shares the broadcast's allocation (refcount, not copy) |
| Saturated member isolated | ✅ slow member sheds/disconnects alone; healthy members receive; drops counted |
| Benchmark published, uWS included, box pinned | ⚠️ published **as provisional** — shared sandbox, not a pinned box (spec'd in benchmarks/README.md with the caveats); 100k-member <150 ms gate explicitly deferred to the pinned box, where clients can be isolated from the server |

## Benchmark summary (provisional, shared sandbox, Node 20)

512 B broadcast, client-observed completion, best/median of 5: at ≤10k
members beamsocket ≈ ws ≈ uWS (client-bound tie); at 25k beamsocket 140/163
ms vs ws 217/246 and uWS 268/285 — ordering is the signal, magnitudes are
polluted by client-worker CPU saturation. **Losses published:** idle RSS/conn
beamsocket 11.3 KB vs ws 4.4 KB and uWS **0.84 KB** — uWS is ~13× denser
than us today. Socket.IO: 148–277 ms and 14.7 KB/conn at its scales.

## Rule 4 — memory cost

Per membership: **~80 B measured** (10k conns × 1 room: 11.67 KB/conn vs
11.59 KB/conn without rooms; matches the ~100–150 B structural estimate —
ConnectionId in the room set + RoomId clone in the conn entry + HashSet
overhead). Per room: one DashMap entry + set overhead, ~150 B + name. Idle
per-connection baseline on Node 20: 11.6 KB/conn at 10k.

## Rule 5 — queues

No new queues. Fan-out reuses the per-connection bounded mailbox (HWM bytes +
policy + `backpressure_drops`); `FanoutReport.backpressured` makes shedding
visible per broadcast. `grep -r unbounded`: zero matches (crates, SDK, tests,
benches).

## Rule 1 audit

Join/leave/broadcast are synchronous JS→Rust commands; fan-out loops,
membership bookkeeping, and disconnect cleanup never call JS. The bridge
event stream is unchanged from 1A.

## Deviations / follow-ups (not blockers)

- `maxRoomsPerConnection` exists in config but is **not enforced** — that's
  `limits.rs`, Phase 1C by the roadmap.
- Benchmarks ran on official Node v20.19.5: the sandbox's distro Node 18
  carries a nonstandard ABI (109) for which uWS ships no binary. One
  consistent runtime for all frameworks.
- Pinned-box list: 100k-member gate, Socket.IO ≥25k, echo-latency suite
  (rides the existing 1D re-confirmation blocker).
- The npm devDependency set now includes socket.io / socket.io-client / uWS
  (bench-only).

## PR checklist (§11)

- [x] One phase per PR — Phase 1B only
- [x] Rule 1 audit above
- [x] Rule 4: membership cost measured (~80 B), stated above
- [x] Rule 5: no new queues; report + counters cover fan-out shedding
- [ ] Rule 3 — n/a, nothing reads client IPs
- [x] §6 tests green: `cargo fmt --check`, `clippy --workspace -D warnings`
      (+ `--features napi`), `cargo test --workspace` (6 suites), `tsc
      --noEmit`, `npm test` (api-surface + echo + rooms integration)
