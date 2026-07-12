# PR: Phase 3A — Mesh Link Layer

**Phase:** 3A only (ENGINEERING.md §13.1; implements frozen RFC 0004 §4.4/§4.6/§4.7).
Branch: `phase-3a-mesh-link`, from `master` (dcc3070). Adds one new crate,
`crates/mesh` — pure Rust, **no NAPI** (same rule as core). Nothing in
`core`/`node`/the SDK changes: the crate is a leaf with no reverse dependencies,
so the existing 112-test suite is untouched by construction. Core attaches to it
behind the Engine facade in 3D, not here.

This is the link *transport* a mesh is built on: framing, the
HELLO→CHALLENGE→AUTH handshake, version/feature negotiation with
sender-suppression, the coalesced link writer, and per-peer byte-bounded
drop-and-count queues. **No SWIM (3B), no interest routing (3C), no relay verbs
or engine integration (3D).** The frame kinds those phases need
(`MEMBERSHIP`/`INTEREST*`/`RELAY_*`) exist so negotiation can classify them; the
crate carries and counts them but never interprets them.

## What's in it

| Module | Responsibility |
|---|---|
| `frame` | `[len u32 LE][kind u8][flags u8][body]` codec; golden-bytes tests; close-on-oversize with **no resync** (§4.4) |
| `hello` | HELLO body codec — append-only, trailing-byte tolerant (§4.4 body-evolution); the transcript source of truth |
| `crypto` | Vendored HMAC-SHA256 + SHA-256 (FIPS 180-4 / RFC 4231 KATs) + a `/dev/urandom` nonce source |
| `handshake` | Sans-IO state machine: transcript-pinned, role-bound MACs; version window; feature intersection; `Negotiated::may_emit` (sender suppression) |
| `queue` | The Rule 5 star: byte-bounded, drop-newest-and-count, per-peer pressure gauge, non-blocking `push`, coalescing `drain` |
| `link` | Async lifecycle: connect/accept, auth timeout, the **coalesced writer**, framed reader, idle PING/PONG, clean close, reconnect-backoff seam |
| `config` / `counters` | Per-link config + defaults (RFC/spike constants cited); lock-free counters incl. `unknownFrames`, `authFailures`, `oversizeCloses`, per-peer `relayDrops`/pressure |

### The handshake (§4.7), stated precisely

`HELLO(both) → responder CHALLENGE(nonce) → AUTH in both directions`, then the
link is up. The MAC is `HMAC(secret, role_label ‖ responder_nonce ‖ transcript)`
where **transcript = the two HELLO bodies, bit-exact as received**, initiator's
first. Two properties fall out and each has its own gate:

- **Transcript pinned** → a MITM edit to any negotiated HELLO field
  (feature bit, version, max-frame) changes one side's transcript, so the AUTH
  MAC fails. The **downgrade-tamper test** proves it.
- **Role-bound** (`bsmh-initiator` / `bsmh-responder` labels, plus the
  responder's fresh nonce) → an attacker cannot reflect a node's own AUTH back
  at it; the labels differ, so the MAC does not verify. The **reflection test**
  proves it.

Cluster-name mismatch and an out-of-window version are refused **at HELLO,
before any challenge** — each a distinct `RefuseReason` (a distinct link-state,
logged with both numbers), never a silent retry loop. Only `AuthFailed` earns a
backoff-retry (§4.7); version/cluster/id mismatches are terminal.

### Sender suppression (§4.4 — the load-bearing rule)

Feature bits are an **intersection** (`local & peer`), computed at handshake.
`Negotiated::may_emit(kind)` gates the send path: attempting to emit an
un-negotiated kind is **counted (`suppressed_emits`) and, in debug builds,
`debug_assert`'d — never a wire write**. `LinkHandle::try_send` returns
`Err(Suppressed)` so this is observable in tests without panicking;
`LinkHandle::send` is the production wrapper with the assert. On receive,
`unknownFrames` counts anything a suppressed sender could never have sent
(unknown kind, or a known-but-un-negotiated feature kind) and skips it
(self-delimiting) — a **bug detector that reads zero in correct operation**.

### The coalesced writer (spike finding #1, `0004-results.md`)

`COALESCE_CAP_BYTES = 128 KiB` and the 1 MiB default queue HWM are cited in the
code to `0004-results.md` ("What the spike changed" #1): per-frame writes
measured **3.8 ms p99** at 100k msgs/s; coalescing everything queued into one
`write` per wakeup (cap 128 KiB) cut that **5.5× to 680 µs**. The writer drains
the per-peer queue in one buffer per wakeup — one syscall per batch, not per
frame. "The syscall, not the byte, is the expensive unit" — the RFC 0001 bridge
lesson, one layer down. Constants re-derive on real HW; the decision to coalesce
does not.

## §13.1 gate tests

Handshake gates are driven at the **sans-IO** layer (deterministic, no sockets);
link gates run over **real loopback TCP**. Attack tests are named as in the RFC
freeze note.

| Gate | Test | Where |
|---|---|---|
| N/N−1 interop matrix (same / one-step / two-step-refused+logged) | `interop.rs` | sans-IO |
| **downgrade-tamper** (feature bit AND version) → AUTH MAC fails | `security.rs::downgrade_tamper_*` | sans-IO MITM |
| **reflection** (replay initiator AUTH back at it) → refused | `security.rs::reflection_of_own_auth_is_refused` | sans-IO |
| cross-cluster-name refused at HELLO, before auth | `security.rs::cross_cluster_name_refused_at_hello_before_auth` | sans-IO |
| saturation: slow reader → drop-and-count, gauge rises, enqueuer never blocks, recovers | `saturation.rs` | real TCP |
| `unknownFrames == 0` under mixed-feature load; `> 0` only from a misbehaving peer | `suppression.rs` | real TCP |
| relay microbench: coalesced writer sustains 100k × 64 B, zero drops | `relay_bench.rs` (CI) + `#[ignore]`d `<1 ms p99` (pinned box) | real TCP |
| oversize frame → link closed, counted, no resync | `oversize.rs` | real TCP |

Plus per-module unit tests: frame golden bytes + oversize/malformed; crypto
KATs (FIPS 180-4 SHA-256, RFC 4231 HMAC); HELLO round-trip + append-only
tolerance; handshake happy-path + version/cluster/collision refusals + feature
intersection; queue drop/gauge/coalesce; config validation + backoff schedule.

## Rules audit (§1 carries PR-rejection weight)

- **Rule 5 — every queue bounded, every drop counted, every link observable (the
  star).** `PeerQueue` is byte-bounded (not frame-bounded — a 64 KiB frame is
  not one 64 B frame, §4.6), overflow is drop-newest-and-count (`relayDrops`),
  `push` never blocks the enqueuer, and every link exposes a `0..1` pressure
  gauge plus the full counter set. The one internal channel-free structure is a
  `Mutex<VecDeque>` + `Notify` + atomic byte counter — genuinely bounded, no
  unbounded channel anywhere.
- **Rule 4 — per-peer memory cost stated.** Per peer: one TCP link + the bounded
  data queue (`queue_hwm_bytes`, default **1 MiB**) + fixed handshake/lifecycle
  overhead (two 32-byte MAC scratch buffers, a 128 KiB writer scratch, counters,
  an `IdleClock`) ≈ **~1.15 MiB worst case**. Times N ≤ 50 peers ≈ **~58 MiB**
  data-plane ceiling — the mesh's Rule 4 envelope (RFC §8).
- **Rule 3 — works behind infrastructure.** The mesh authenticates every peer
  by shared-secret HMAC regardless of network position; it trusts its network
  boundary for confidentiality only (§4.7, cleartext until mTLS on RFC 0003's
  seam), stated plainly.
- **Rule 2 — no global lock on a hot path.** State is per-link and lock-local;
  the only lock is the per-peer queue's, held for microseconds and **never
  across an `.await`** (the writer releases it before `write_all`).
- **Rule 1 — no per-message JS.** This crate never touches the bridge or a
  runtime; there is no JS boundary to cross here.

**Bridge constants and all existing suites untouched.** The only edits outside
`crates/mesh/` are `Cargo.toml` (add the workspace member) and `Cargo.lock` (the
new package entry — it pulls in only `tokio`, already locked, so no dependency
tree churn).

## Deviations, honest (RFC 0001 rule)

- **Vendored HMAC-SHA256 instead of `hmac`/`sha2`.** Rationale: keeps the crate
  std-plus-tokio only — offline-buildable, and adding a mesh does not drag the
  RustCrypto tree into the workspace lockfile. Correctness is the FIPS 180-4 and
  RFC 4231 known-answer vectors, byte for byte (`crypto::tests`). Swapping to the
  audited crates is a one-line dependency change; every caller takes `&[u8]` in,
  `[u8; 32]` out. **Flagged for reviewer preference.**
- **Mutual auth uses the responder's single fresh nonce** (as §4.7 specifies),
  not a nonce from each side. Reflection is prevented by the role labels, replay
  across sessions by the fresh nonce + transcript binding. If review wants
  initiator-nonce freshness too, it is an additive HELLO/CHALLENGE change behind
  the append-only rule.
- **Idle-liveness `idle_dead_after` close** is a clean local close in 3A; 3B
  feeds link death into suspicion. The reconnect loop is a stub — `Backoff` and
  `Link::connect` are the named seam, tested (`config::tests`), not yet driven.
- **The `<1 ms p99` relay gate is `#[ignore]`d** (run on the pinned box:
  `cargo test -- --ignored`). A shared sandbox cannot be trusted on absolute
  latency — the same reason the RFC's soak gate is CI/pinned-box only. The
  non-ignored throughput test guards the coalescing path in CI.

## Verification (run green)

Toolchain: stable 1.97.0. Commands (mesh crate + whole workspace):

- `cargo fmt -p beamsocket-mesh --check` — clean.
- `cargo clippy -p beamsocket-mesh --all-targets -- -D warnings` — clean.
- `cargo test -p beamsocket-mesh` — **55 passed, 0 failed, 1 ignored** (the
  `#[ignore]`d pinned-box p99 gate). 41 lib unit tests + 14 integration.
- `cargo test --workspace` — **126 passed, 0 failed, 4 ignored**: the pre-
  existing 71 Rust tests (3 pre-existing ignored soak/HW gates) unchanged and
  green, plus the mesh crate's 55. The 41 JS tests are untouched (no JS change),
  so the 112-test suite stays green.

The vendored SHA-256/HMAC were additionally cross-checked by porting the exact
algorithm to a reference implementation and matching `hashlib`/`hmac` across
edge sizes (0, 55, 56, 64, 1000+ bytes); the KAT expected values are the FIPS
180-4 / RFC 4231 standard vectors.

## Exit gates (DoD §13.1)

| Gate | Status |
|---|---|
| `crates/mesh` standalone: fmt, clippy `--all-targets -D warnings`, `cargo test`; workspace green end-to-end | ✅ all green (see Verification) |
| All §13.1 gate tests present and green; attack tests named exactly as the RFC freeze note | ✅ `interop`/`security`(downgrade-tamper, reflection)/`saturation`/`suppression`/`oversize`/`relay_bench` |
| Coalesced-writer constants cited to `0004-results.md` | ✅ `link::COALESCE_CAP_BYTES`, `config::DEFAULT_QUEUE_HWM_BYTES` |
| Sender-suppression rule not weakened | ✅ intersection + `may_emit` on the send path (counted + `debug_assert`), receive-side `unknownFrames` |
| Existing 112 tests untouched | ✅ additive crate; no existing source file changed; 71 Rust + 41 JS green |
| PR notes in house format; deviations honest | ✅ this document |
