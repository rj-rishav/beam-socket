# PR: Phase 1.1 — HTTP Server Attach (RFC 0002 implementation)

**Phase:** 1.1 only (ENGINEERING.md §9). Branch: `phase-1.1-attach`, stacked on
`rfc-0002-http-attach`. Implements the FROZEN RFC 0002 — `new BeamSocket({ server
})` so Express/Fastify users get one port, one TLS setup, one load balancer.

## Precondition (landed first)

- **RFC 0002 → FROZEN** (`b21683b`), with the two review riders folded in: §8.1
  step 1a (drain stranded pre-pause bytes into `head`), §14 macOS row (CI-proven
  before any release doc claims macOS), and the verbatim Windows throw (§7).

## What's in it (RFC section refs — no re-design)

- **Core — the `adopt` seam (§5).** A second producer into the connection
  lifecycle converging on the SAME `run_admitted` tail as own-port (authorize →
  registry insert → identity bind → `run_connection` → cleanup, byte-for-byte):
  - `Transport::adopt` (§8.3) completes the 101 (`Sec-WebSocket-Accept` from the
    Node-parsed key) and starts framing over a **`PrefixedStream`** — head-then-
    wire reads, pass-through writes — so a first frame coalesced with the upgrade
    is replayed, not lost. Own-port `accept` now runs through the SAME
    `PrefixedStream` with an EMPTY head, unifying both paths on one stream type.
  - `engine.attach(std_stream, peer, ParsedUpgrade, head)` runs the admission
    `Gate` SYNCHRONOUSLY (trustProxy IP + `maxConnectionsPerIp` + the 1D
    `draining`/503 check) and returns `Accepted` / `Rejected(status)`; on admit
    it spawns the 101 + replay + authorize + lifecycle on the engine runtime.
- **Node — `FdHandoffGuard` + napi `attach` (§9, Unix only).** `attach(fd,
  remoteAddr, method, url, headersFlat, head)` `dup()`s the fd under an RAII
  guard (armed at dup, disarmed at the std-`TcpStream` ownership transfer),
  adopts it, and returns `{ accepted, status }`. Disjoint Rust/Node fd ownership
  → no leak, no double close. `#[cfg(unix)]`; absent on Windows (SDK throws
  first).
- **SDK — `{ server, path }` (§10).** Registers the `'upgrade'` handler; claims
  only matching-path upgrades and **defers** non-matches (coexistence); **sole-
  handler mode** (no `path`) 400s malformed upgrades. On a claim: pause, **drain
  stranded pre-pause bytes** (Rider 1), hand the fd to Rust, detach the Node
  socket. `listen()` and `{ server }` are mutually exclusive; `https.Server` and
  Windows throw the **verbatim** §6/§7 messages. `io.close()` attached-mode 503s
  a racing upgrade and drains as 1D. `examples/express-attach` with the SIGTERM
  recipe.

## Exit gates (§9 + RFC §14 "Ships")

| Gate | Status |
|---|---|
| `examples/express-attach` runs: Express routes + BeamSocket at /ws, one port | ✅ smoke-run: `GET /health` 200, WS echo at `/ws`, SIGTERM → clean exit 0 |
| Coalesced first-frame replayed; empty-head; stranded-drain | ✅ `phase1_1.rs::attach_replays_coalesced_first_frame` / `attach_empty_head_normal_first_frame` (Rust, deterministic) + JS `attach_replays_coalesced_first_frame` / `attach_drains_stranded_prepause_bytes` |
| fd hygiene: no leak / no double close across churn | ✅ JS `attach_fd_hygiene_no_leak_no_double_close` (accept + per-IP-reject churn, fd count flat) |
| Coexistence + sole-handler 400 | ✅ JS coexistence (2nd listener still gets `/other`) + sole-handler 400 |
| Lifecycle: 503 during drain; `httpServer.close()` leaves WS alive; clean exit through attach | ✅ 3 JS lifecycle tests incl. a child process that exits 0 with no `process.exit()` |
| `listen()`/`{server}` mutual exclusion + verbatim throws | ✅ JS asserts the exact §6/§10.1 messages |
| All throws match RFC wording; support matrix true in README | ✅ verbatim constants; README matrix added |
| darwin CI green on the coalesced test (Rider 2) | ⚠️ **CI-only** — the sandbox is Linux; macOS uses the identical POSIX path (spike + Rust + JS green on Linux). Gated in CI before any macOS release claim |
| fmt, clippy --all-targets (+napi), cargo test, tsc, npm test | ✅ 60 Rust tests, 26 JS tests, all green |

## Rules audit (1D-PR style)

- **Rule 1 — no per-message JS.** Post-handoff an attached connection is
  indistinguishable from an own-port one — the data plane is 100% Rust
  (`run_admitted` is shared). `authorize` still crosses ONCE per connection, from
  the Node-parsed request. `adopt`/`PrefixedStream` touch bytes only during the
  one-time handshake.
- **Rule 2 — no global lock on a hot path.** No new shared state; attach reuses
  the sharded registries + the `Gate`.
- **Rule 3 — works behind a load balancer.** `trustProxy`/`authorize` run through
  attach on the Node-parsed request; tested behind a simulated proxy (JS `Rule 3`
  test: loopback-as-trusted-proxy, per-IP limit keys on `X-Forwarded-For`) and at
  the core level (`attach_gate_reject_returns_http_status`).
- **Rule 4 — per-connection cost.** **Post-handoff, Node holds ZERO
  per-connection state** — the Node socket is `destroy()`ed; the connection is
  the 1D Rust footprint (~11.6 KB/conn table applies). Asserted: `socket.destroyed
  === true` after attach, plus an attached-mode idle RSS spot-check.
- **Rule 5 — every queue bounded.** No new queues. `grep -r unbounded`: zero.
- **Bridge constants unchanged.** `BRIDGE_BATCH`/`FLUSH_INTERVAL`/queue capacity/
  `EXTERNAL_BUFFER_THRESHOLD` untouched. The 1B lock invariant is untouched. The
  1D close sequence is unchanged except the specified attached-mode listener
  handling (see deviations).

## Deviations / follow-ups (honest)

- **Gate location (§5 sketch → impl).** RFC §5 sketched `Transport::adopt` taking
  `peer`/`ParsedUpgrade`/`gate` and running the gate. The impl runs the gate in
  `engine.attach` (synchronously) so the napi return carries the reject status
  per §4's `Accepted/Rejected(status)` contract; `adopt` is the 101 + replay
  helper. Consistent with §4 **and** §5's convergence intent, factored for the
  sync return.
- **`close()` keeps the upgrade handler (vs §10.4 "remove listener").** Removing
  the listener during the race routes a racing upgrade to the app's request
  handler (or hangs a bare server) — never a 503. So `close()` sets `#closing`
  (the handler answers 503) and **keeps the handler registered**; the server is
  shutting down, so a lingering 503-only listener is harmless. This honors the
  RFC intent ("racing upgrade answered 503") more reliably than the literal
  wording; flagged for the reviewer.
- **Stranded-bytes JS test is best-effort on timing.** Real pre-pause stranded
  bytes can't be forced deterministically from userland; the test exercises the
  `socket.read()` drain path and asserts no-loss. Head replay itself is proven
  deterministically by the coalesced tests (Rust + JS).
- **macOS is CI-gated (Rider 2), not run here** (Linux sandbox). Windows and
  `https.Server` throw — no handoff, no TLS (RFC 0003).
- The `examples/express-attach` uses `express` (its own `package.json`); not a
  dependency of the `beamsocket` package.

## PR checklist (§11)

- [x] One phase per PR — Phase 1.1 only (no TLS/Windows/design-B)
- [x] Rule 1 audit — attach is a one-time handoff; data plane stays Rust
- [x] Rule 4 — Node holds zero per-connection state post-handoff (asserted)
- [x] Rule 5 — no new queues; `grep unbounded` zero
- [x] Rule 3 — trustProxy/authorize through attach tested behind a simulated proxy
- [x] §9 tests green: `cargo fmt --check`, `clippy --all-targets -D warnings`
      (+ `--features napi`), `cargo test --workspace` (60), `tsc --noEmit`,
      `npm test` (26: + attach.integration)
