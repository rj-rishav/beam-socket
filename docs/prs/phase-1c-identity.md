# PR: Phase 1C — Identity & Admission Limits

**Phase:** 1C only (ENGINEERING.md §7). No presence, no `metrics()` surface, no
graceful `close()` (those are 1D). Branch: `phase-1c-identity`, stacked on
`phase-1b-rooms`.

Makes `User` a real first-class primitive (`authorize` → `toUser`), and makes
the runtime protect itself: per-IP admission, payload cap, per-connection room
cap, and `trustProxy` client-IP resolution — all enforced in Rust before any
JS runs.

## What's in it

- **`io.authorize(fn)` — the first request/response round-trip across the
  bridge.** 1A/1B events are fire-and-forget; authorize is not. The engine emits
  an `Authorize` event to JS through the SAME batched design-C bridge (a new
  `KIND_AUTHORIZE` byte — no second channel, bridge constants untouched); JS
  replies out-of-band with the flat `resolveAuthorize` command (like
  `send`/`join`). Design points:
  - **Bounded pending-upgrade table (Rule 5):** `authorize.maxPending` (default
    8192) caps concurrent pending authorizations; overflow → reject at the door
    with 1013 (`metrics.pending_overflow`). An unauthenticated handshake flood
    is a DoS surface, so this table cannot grow without bound.
  - **Timeout (`authorize.timeoutMs`, default 10 s):** a promise that never
    settles is rejected-and-cleaned (1013, `metrics.authorize_timed_out`) — the
    pending entry and its slot are always freed, never leaked.
  - **No hook registered → accept all**, no round-trip to JS, `userId` unbound
    (the 1A/1B behavior). `has_authorize` is passed to the engine so the
    round-trip only runs when an app hook exists.
- **`identity.rs`:** sharded `DashMap<UserId, HashSet<ConnectionId>>` (Rule 2).
  Bound at authorize-accept (before `Opened` fires, so a brand-new device is
  immediately reachable), unbound on disconnect; the user entry auto-destroys on
  its last device (no empty user survives — the rooms discipline).
  `io.toUser(id).send()/.except()` fans out **entirely in Rust**, reusing the 1B
  serialize-once broadcast path + `FanoutReport` — one FFI call, one allocation
  regardless of device count.
- **`limits.rs`:** enforced in Rust before any JS runs:
  - `maxConnectionsPerIp` — rejected at the **HTTP upgrade with a 429**, inside
    the handshake callback, before a WebSocket exists (cheaper than
    close-after-handshake; the right layer to shed a flood). RAII
    `IpAdmitGuard` releases the slot on *every* teardown path.
  - `maxPayloadBytes` — the codec's message/frame cap (close 1009); wired in 1A,
    documented here.
  - `maxRoomsPerConnection` — **now really enforced** in `join`, under the
    connection's shard lock (atomic check-and-insert), replacing the 1B
    "exists but unenforced" state. Returns a new `LimitExceeded` result.
- **`trustProxy: false | true | CIDR[]`** client-IP resolution (`ClientIpResolver`):
  - `false`: socket peer address, always; XFF ignored.
  - `CIDR[]`: honor XFF only when the peer is a trusted proxy, parsed
    **right-to-left**, skipping trusted hops — the first untrusted address is the
    client. Leftmost-first is spoofable; we never use it. A malformed hop breaks
    the chain and falls back to the unspoofable peer. IPv4 + IPv6, with
    IPv4-mapped canonicalization.
  - `true`: rightmost XFF hop; the type docs warn it trusts any peer.
  - The resolved IP feeds BOTH `maxConnectionsPerIp` and `AuthorizeRequest.ip`.
- **SDK:** `authorize()`, `toUser()`, `socket.userId`, `socket.metadata` are
  real. `metadata` stays in JS (Rust never serializes an arbitrary JS object);
  it is correlated to its socket by the authorize `request_id`, which
  `ConnectionOpened` now carries. Every rejection close code / HTTP status is
  named in `types.ts` (`RejectCode`).

## Exit gates (§7 / DoD)

| Gate | Status |
|---|---|
| `toUser` fan-out entirely in Rust; identity cost measured & published | ✅ `FanoutTarget::User` reuses the 1B path; **~278 B/conn** worst case (all single-device users) → **~20.5 B/conn** amortized (multi-device) measured — see Rule 4 |
| Multi-device: 1 user, 3 conns → `toUser` reaches 3; one leaves → 2; last leaves → entry gone | ✅ `tests/phase1c.rs::multi_device_to_user_reaches_every_device` + JS integration |
| Leak: 10k connect/disconnect churn → index empty, RSS flat | ✅ `churn_leaves_identity_and_ip_tables_empty_rss_flat`: `user_count==0`, `tracked_ips==0`, RSS delta +228 KB / 10k cycles (≈23 B/cycle, allocator noise) |
| Spoof: untrusted XFF ignored; trusted honored right-to-left; mixed hops | ✅ 9 resolver unit tests in `limits.rs` + proven end-to-end via the proxy per-IP test |
| Per-IP limit: N+1th rejected with documented status, direct AND proxy (Rule 3) | ✅ `per_ip_limit_direct_topology` + `per_ip_limit_proxy_topology` + JS integration (429) |
| authorize timeout fires; pending cap overflows safely; exception → reject not hang | ✅ `authorize_timeout_closes_and_cleans`, `pending_upgrade_table_overflow_rejects_safely`, JS "throwing handler rejects (1011)" |
| Every close code / HTTP rejection documented in TS types | ✅ `RejectCode` in `types.ts` (429, 1008, 1011, 1013; 1006/1009 cross-referenced) |
| fmt, clippy -D warnings (+napi), `cargo test --workspace`, tsc --noEmit, npm test | ✅ all green (55 Rust tests, 8 JS tests) |

## Rules audit (1B-PR style)

- **Rule 1 — no per-message JS.** `authorize` runs **once per connection** at
  upgrade time, never per message; it is the one sanctioned connection-time JS
  hook (ARCHITECTURE §4). `toUser` fan-out, identity bind/unbind, IP resolution,
  and all admission checks are Rust-only. The per-message bridge stream is
  unchanged apart from the rare `Authorize` control event folded into the same
  batch.
- **Rule 2 — no global lock on a hot path.** Identity is a sharded `DashMap`;
  the per-IP limiter is a sharded `DashMap`; the authorize pending table is a
  `DashMap` (though once-per-connection, not a hot path).
- **Rule 3 — works behind a load balancer.** `trustProxy` + `X-Forwarded-For`
  resolution is the whole point; the per-IP limit is tested in BOTH direct and
  simulated-proxy topologies, in Rust and JS.
- **Rule 4 — memory cost stated (worst case first).** Identity index worst
  case — every connection a distinct *single-device* user — is **~278
  B/connection**: it is dominated by the per-*user* cost (a `DashMap` entry + a
  fresh `HashSet` allocation + the userId string), which every connection pays
  in full when no user is shared. That per-user cost **amortizes toward
  ~20.5 B/connection** as soon as a user has more than one device — the
  multi-device hot path, where the marginal cost is just a `ConnectionId` in the
  user's already-allocated shared `HashSet` plus load-factor slack
  (`identity_memory_cost_measurement`, 500k devices, both cases). The honest
  headline is therefore *~278 B for the pathological all-singletons workload,
  ~20 B amortized for real multi-device users.* Per-IP limiter: one `DashMap`
  entry per *active* IP only (idle IPs cost nothing).
- **Rule 5 — every queue bounded.** The pending-upgrade table is bounded
  (`authorize.maxPending`, overflow counted in `metrics.pending_overflow`); the
  JS-side authorize correlation map is bounded (`PENDING_AUTH_CAP`). Every
  rejection has a counter (`admission_rejected_ip`, `authorize_rejected`,
  `authorize_timed_out`, `pending_overflow`). `grep -r unbounded` across
  crates + SDK: **zero matches**.
- **Bridge constants unchanged.** `BRIDGE_BATCH=256`, `BRIDGE_FLUSH_INTERVAL=1
  ms`, `ENGINE_BRIDGE_QUEUE_CAPACITY=8192`, `EXTERNAL_BUFFER_THRESHOLD=16 KB` —
  all untouched. The 1B lock-order invariant (conn-shard → room-map) is
  unchanged; identity fan-out follows the same copy-out-then-push discipline.

## Close codes / HTTP statuses (named in `types.ts::RejectCode`)

| Situation | Code | Layer |
|---|---|---|
| `maxConnectionsPerIp` hit | **HTTP 429** | HTTP upgrade (no WebSocket) |
| `authorize` `{accept:false}`, no code | **1008** | WebSocket close (default) |
| `authorize` `{accept:false, code}` | **that code** (e.g. 4401/4403) | WebSocket close |
| `authorize` timeout / pending overflow | **1013** | WebSocket close (transient) |
| `authorize` handler threw | **1011** | WebSocket close |
| payload over `maxPayloadBytes` | 1009 | codec (1A) |

## Deviations / follow-ups (not blockers)

- **`metadata` lives in JS, correlated by `request_id`.** Rust binds only the
  `userId` (for `toUser`); `socket.metadata` is attached SDK-side when `Opened`
  arrives carrying the originating `request_id`. The correlation map is bounded
  and consumed on `Opened`; the only residual is the pathological
  accepted-but-never-opened case, capped by `PENDING_AUTH_CAP`.
- **RSS-flatness is recorded, not asserted** (allocator page-granularity
  variance); the logical leak invariants — `user_count==0`, `tracked_ips==0`
  after 10k cycles — ARE asserted.
- **Pending-box constant re-confirmation** still rides the 1D blocker
  (`0001-results.md` "Follow-ups"); nothing in 1C touches the bridge constants.
- Pre-existing test-only warnings in `tests/phase1b.rs` (`SinkExt`, `id3`,
  `dead_id`'s `ids`) predate 1C and are not linted by the CI `clippy --workspace`
  gate (it does not build test targets); left untouched to respect phase scope.

## PR checklist (§11)

- [x] One phase per PR — Phase 1C only (no presence/metrics/close)
- [x] Rule 1 audit above — authorize is once-per-connection
- [x] Rule 4: identity cost measured (~20.5 B/conn) and stated
- [x] Rule 5: pending table + JS correlation map bounded; every reject counted; `grep unbounded` zero
- [x] Rule 3: per-IP limit tested direct AND simulated-proxy
- [x] §7 tests green: `cargo fmt --check`, `clippy --workspace -D warnings`
      (+ `--features napi`), `cargo test --workspace` (55 tests), `tsc --noEmit`,
      `npm test` (api-surface + echo + rooms + identity integration)
