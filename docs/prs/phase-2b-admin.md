# PR: Phase 2B — Admin Actions

**Phase:** 2B only (ENGINEERING.md §12.2). Branch: `phase-2b-admin`, from
`master` (after the 2A merge). Three operator verbs — `disconnectSocket`,
`disconnectUser`, `closeRoom` — each a Rust-side sweep behind **one FFI call**.
No config-mutation APIs, no clustering hooks, no per-connection drop/rate
tracking (still parked). The governing constraint is the §12.2 smell rule:
**zero new teardown logic** — every verb only *calls* paths earlier phases
proved.

## What's in it

| Verb | Returns | Mechanism |
|---|---|---|
| `io.disconnectSocket(id, code?)` | `{ closed: 0\|1 }` | registry lookup → the EXISTING `initiate_close` path (same as `socket.close()`), full 1C/1D cleanup in the connection task's tail |
| `io.disconnectUser(userId, code?)` | `{ closed: n }` | identity-index **copy-out** (1B discipline — the user shard guard is released before any close), then per-device `initiate_close`; the identity entry auto-destroys on the last device's unbind (the 1C invariant, now reachable via admin) |
| `io.closeRoom(room, code?)` | `{ removed: n }` | `rooms::close_room` = `members()` copy-out + the EXISTING `leave` per member; the last leave auto-destroys the room. Disconnect-free: connections stay alive |

- **Codes** (named in `types.ts` as `AdminCloseCode`): default `1000`; the
  application range `4000–4999` is allowed. Anything else throws a
  `RangeError` in the SDK **before any FFI call** — the remaining registered
  codes (1001–1015) belong to the engine/RFC 6455 and would lie to the client.
- **Metrics:** `adminDisconnects` (one per connection actually closed — a
  3-device `disconnectUser` counts 3), `adminRoomCloses` (one per call that
  found the room). Both in `stats()`/`metrics()` and `metricsText()`
  (`beamsocket_admin_disconnects_total`, `beamsocket_admin_room_closes_total`).
- **Drain semantics (§12.2 required):** a verb during/after `close()` is a
  **safe no-op reporting 0** (binding guards the taken engine; the SDK guards
  the dropped reference) — an operator script racing shutdown must not crash.
  Before the server ever started, verbs throw `/listen()/` like every other
  method: that's a typo, not a drain.
- Nonexistent/stale id, user, or room → count `0`, never an error, metrics
  untouched.

## Zero new teardown logic (the §12.2 smell rule) — stated explicitly

**The verbs only *call* existing cleanup; no teardown code was added.**
Diff-review trail:

- `disconnectSocket`/`disconnectUser` end at `ConnHandle::initiate_close` —
  the same latch `socket.close()`, backpressure-disconnect, and the shutdown
  sweep already use. Registry removal, room membership sweep
  (`disconnect_cleanup`), identity unbind (auto-destroy on last device), and
  the connections gauge all unwind in `run_admitted`'s tail — **untouched**.
- `closeRoom` is `rooms.members()` (existing copy-out) + `rooms.leave()`
  (existing path, auto-destroy on last leave) in a loop
  (`rooms.rs::close_room`). `remove_member`/auto-destroy — **untouched**.
- The only additions in core: the two sweep loops, two metric adds, two
  counters, and two Prometheus lines. Grep the diff for
  `remove|unbind|destroy|clean`: every hit is a comment or a *call*.

## Exit gates (DoD §12.2)

| Gate | Status |
|---|---|
| Three verbs end-to-end; codes + result shapes in `types.ts` | ✅ `AdminCloseCode`, `AdminDisconnectResult`, `AdminCloseRoomResult`; exercised through the SDK in `admin.integration.test.mjs` |
| disconnectUser: 3 devices → all closed with the code, identity entry GONE, toUser reaches 0 | ✅ Rust (`phase2b.rs`, code 4005 read from real ws clients, `broadcast_user.attempted == 0`) + JS (`user().connections()` → `[]`, `stats().users == 0`) |
| closeRoom: members alive, room gone, views agree | ✅ Rust + JS (members still receive a broadcast; `room().info().exists == false`; second room untouched); **1B proptest extended** with a `CloseRoom` op (`phase1b.rs`) — views agree, no empty room survives, a swept room is gone |
| Verbs during `close()` drain: safe no-ops | ✅ JS: counts 0, no throw, during AND after the drain |
| Verb on nonexistent id/user/room → 0, no error | ✅ Rust + JS (incl. foreign/unparseable socket ids) |
| Close code lands on the client | ✅ tungstenite clients (Rust) and `ws` clients (JS) assert 1000 (default), 4001, 4005, 4999 |
| Churn: 1k `disconnectSocket` sweeps → registries empty, RSS flat | ✅ `phase2b.rs`: connections 0, users 0, rooms 0, per-IP table 0; **RSS delta 52 KB over 1000 sweeps** (allocator noise; a leaked conn entry would be ~6.6 MB) |
| Rule 1: verbs are control-plane, no new per-message anything | ✅ sweeps run only when called; no message-path diffs (see rules audit) |
| fmt, clippy `--all-targets -D warnings` ×2 (default + `--features napi`), `cargo test --workspace`, `tsc --noEmit`, `npm test` | ✅ 71 Rust tests passed (3 pre-existing ignored soak gates), 41 JS tests |

## Rules audit (§12 carries PR-rejection weight)

- **Rule 1 — zero hot-path cost.** No new per-message work anywhere: the two
  counters are bumped once per admin *call* (control plane), never on the
  message path. `broadcast.rs`, `connection/`, sampler, and the bridge
  constants (`BRIDGE_BATCH`/`FLUSH_INTERVAL`/queue capacity) are untouched —
  the diff has zero lines in them.
- **Rule 4 — zero new per-connection state.** Nothing was added to
  `ConnHandle`, registry entries, room entries, or identity entries. The two
  new counters are process-global `AtomicU64`s (16 B total).
- **Rule 5 — every queue bounded.** No new queues. Each sweep's copy-out is a
  transient `Vec` sized by the target (one user's devices / one room's
  members), dropped at end of call.
- **Copy-out discipline (1B).** `disconnectUser` copies the device set out of
  the identity shard and releases the guard before initiating any close;
  `closeRoom` copies members out under the room guard and releases it before
  touching any conn shard (then `leave` re-takes conn-shard → room-map in the
  proven lock order). No lock is ever held across another shard or the bridge.

## Deviations / notes (honest)

- **`closeRoom`'s `code` parameter has no wire effect.** §12.2 defines
  `closeRoom` as disconnect-free (no close frame is ever sent), but the work
  order's signature is `closeRoom(room, code?)`. The SDK accepts and
  *validates* it (symmetry with the other verbs; reserved for a future
  "notify members on close") and the docs say so plainly. The Rust surface
  doesn't take it at all.
- **Sweep-vs-race semantics.** A member who disconnects between `closeRoom`'s
  copy-out and its leave is a benign no-op (not counted in `removed`); a join
  that races the sweep may re-create/keep the room with the new member. The
  room is closed *as of the snapshot* — all a sweep can promise without a
  global lock (which Rule 2 forbids). Same for `disconnectUser` vs a
  connecting device. Documented on `rooms::close_room`.
- **`disconnectUser` returns devices closed, not "user existed".**
  `{ closed: 0 }` means "no live devices", whether the user never existed or
  just went offline — the identity index can't tell the difference (an empty
  user entry never survives, by 1C design).
- **`adminDisconnects` counts connections, not calls.** A 3-device
  `disconnectUser` adds 3. Chosen so the counter reconciles against
  `connections`/close events; stated in `types.ts`.
- **Churn is 1k sweeps** (the work order's number; the 1C churn remains the
  10k reference). RSS recorded-not-asserted, same discipline as 1C.
- **Drain no-op vs pre-listen throw.** §12.2 requires drain-time no-ops; a
  server that never started still fails loudly (`/listen()/`) like every
  other verb — silent 0s there would hide an operator typo. The JS-side
  distinction keys off `#closing`.

## PR checklist (§12)

- [x] One phase per PR — 2B verbs only; no config mutation, no clustering
      hooks, no per-conn drop/rate tracking; bridge constants, sampler, and
      the 2A query surface untouched
- [x] Zero new teardown logic — verbs only call `initiate_close` /
      `members()+leave` (stated explicitly above, diff-reviewable)
- [x] Rule 1 / Rule 4 / Rule 5 — control-plane only; 16 B of new global
      counters; no new queues
- [x] Bounded/copy-out discipline preserved (1B pattern in both sweeps)
- [x] Gates green: `cargo fmt --check`, `clippy --all-targets -D warnings`
      (default + `--features napi`), `cargo test --workspace` (71 passed,
      incl. 5 new in `phase2b.rs` + the extended 1B proptest), `tsc --noEmit`,
      `npm test` (41: + admin.integration, + extended api-surface)
