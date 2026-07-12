# PR: Phase 3B — SWIM Membership

**Phase:** 3B only (ENGINEERING.md §13.2; implements frozen RFC 0004 §4.2 + the
§4.4 UDP freeze). Branch: `phase-3b-swim`, stacked on `phase-3a-mesh-link`. All
new code lands in `crates/mesh`; the crate **still has no reverse
dependencies** (core/node/SDK/bridge untouched — engine wiring is 3D).

This adds failure detection + membership dissemination: the SWIM detector
graduated from `spike/mesh/src/swim.rs`, split across the two planes the §4.4
freeze mandates, and assembled into a runnable node. **No interest routing
(3C), no relay verbs or engine integration (3D).**

## What's in it

| Module | Responsibility |
|---|---|
| `swim` | The graduated detector core (transport-agnostic): member table, precedence/merge, incarnation refutation, bounded gossip list, probe scheduler, counters. `SwimParams::tuned()` is the shipped default, cited to `0004-results.md`. |
| `probe` | The **UDP probe plane** — PING/ACK/PING-REQ **only**, frozen HMAC packet format (version-stamped, append-only, golden-bytes), `(inc, seq)` replay guard. Carries **no member state**. |
| `membership_sync` | The **TCP dissemination plane** — the MEMBERSHIP-frame codec + merge logic: push-pull Sync, incremental Gossip, anti-entropy Digest. Pure over the table (unit-testable). |
| `node` | The assembly: binds UDP + TCP on one address, manages one link per peer (dial rule + seed bootstrap), routes MEMBERSHIP frames, spreads gossip/digest, feeds link death into suspicion, and injects partitions for the heal gate. |
| `link` (3A) | Additive hooks only — an inbound data-frame handler and a link-death callback (`LinkHooks`). `connect`/`accept` are unchanged (default hooks = exact 3A behavior); 3B uses `connect_with`/`accept_with`. |

### The two planes, and why (the §4.4 freeze, faithfully)

The spike put everything on JSON-over-UDP. The RFC froze UDP as **probe-only**
(review hit 3: UDP packets carry no negotiated context), so this PR splits them:

- **UDP detects.** A probe packet is `magic ‖ version ‖ kind ‖ from ‖ seq ‖
  inc ‖ (addrs) ‖ HMAC`. There is no `Update`, no member list — the `ProbePacket`
  type itself is the guarantee that membership never rides UDP (a no-reply PING
  is a fixed 53 bytes regardless of cluster size, asserted). Every packet is
  HMAC-authenticated (a forged `dead` is a remote kick, §4.7); a per-sender
  `(inc, seq)` high-water mark drops replays. The format is **frozen** — the
  `golden_ping_bytes_are_frozen` test fails if any byte moves.
- **TCP disseminates.** Suspect/Dead decisions and joins spread as MEMBERSHIP
  frames (kind 0x04) over the 3A negotiated links: push-pull **Sync**,
  incremental **Gossip**, anti-entropy **Digest**.

### Push-pull is load-bearing (spike fix #2), reproduced end-to-end

The stuck-entry failure: after a partition, each island marks the other Dead at
some incarnation; a pull-only join heals one side and leaves the other stuck
behind equal-incarnation Dead-beats-Alive. The fix is the **push** half — the
joiner pushes its state so the contacted node *sees* the "you are dead" claim
about itself and refutes with a bumped incarnation. `membership_sync::apply`
merges the push **before** composing the pull reply, so the reply already
carries the refutation. Proven at three levels: the table
(`swim::refutes_suspicion_of_self`), the sync layer
(`membership_sync::push_pull_triggers_self_refutation`), and end-to-end
(`membership::partition_islands_then_heal_zero_stuck`).

### Tuned row ships (cite-or-fail)

`SwimParams::default() == SwimParams::tuned()` is asserted (T=500 ms, probe
timeout 250 ms, k=3, suspicion 2.5 s) — the row that passed the kill gate at
4.8 s. The literature row (which failed at 8.9 s, `0004-results.md`) stays
selectable for jittery networks but is explicitly not the default. Retuning
these needs a new results entry (the RFC 0001 rule), enforced by the test.

## §13.2 gate tests

| Gate | Test |
|---|---|
| Cold-start convergence < 2 s, 5 nodes (real UDP+TCP) | `membership::cold_start_convergence_under_2s` |
| kill -9 detection < 5 s at the tuned row | `membership::kill_detection_under_5s_tuned` (+ cite-or-fail `swim::tuned_is_the_default...`) |
| Partition → island → heal, **zero stuck** (permanent regression) | `membership::partition_islands_then_heal_zero_stuck` |
| Equal-incarnation Dead/Alive resolves via push-pull | `swim::equal_incarnation_dead_beats_alive`, `membership_sync::push_pull_triggers_self_refutation`, end-to-end in the heal test |
| UDP probe golden-bytes (frozen format) | `probe::golden_ping_bytes_are_frozen` |
| Forged (bad HMAC) probe ignored + counted | `probe::forged_hmac_is_rejected`, `membership::forged_and_replayed_probes_ignored_and_counted` |
| Replayed (stale seq) probe ignored + counted | `membership::forged_and_replayed_probes_ignored_and_counted` |
| Membership never rides UDP (no member-state payload) | `probe::probe_packet_size_is_independent_of_membership` + the `ProbePacket` type |
| Soak chunk, zero false positives | `membership::soak_chunk_no_false_positives` (full 30-min loaded → pinned box) |

Fault injection is at the mesh layer (`MeshNode::set_partition` deny-set), not
iptables, so the heal gate runs in CI.

## Rules audit

- **Rule 5 — no new wire queue type.** MEMBERSHIP dissemination egresses through
  the 3A per-peer `PeerQueue` (byte-bounded, drop-and-count) via `try_send` —
  reused, not reinvented. The SWIM gossip list is a bounded in-memory retransmit
  buffer (one entry per id, capped at `GOSSIP_CAP = 256`), not a socket queue.
  The probe `pending`/`replay` maps are bounded by N; the UDP receive buffer is a
  fixed 2 KiB. No unbounded channel anywhere.
- **Rule 4 — per-member memory cost stated.** Per member: one table entry
  (`Member`: addr + state + inc + an `Instant` ≈ 48 B), at most one gossip-list
  entry (≈ 32 B), and probe map entries (≈ 24 B). That is **~100 B of
  membership-specific state per peer**; the dominant per-peer cost remains the
  3A TCP link (~1.15 MiB, unchanged). Times N ≤ 50 the membership overhead is
  ~5 KiB — negligible beside the link ceiling. The `soak_chunk` test asserts the
  table size never leaks past `N-1` (the RSS-flat proxy).
- **Rule 3 / §4.7 — authenticated regardless of position.** Both planes
  authenticate every peer by the shared secret: TCP via the 3A HMAC handshake,
  UDP via the per-packet HMAC. Confidentiality is still out of scope (cleartext
  until mTLS on RFC 0003's seam).
- **Rules 1 & 2** — no JS boundary in this crate; membership state is one lock
  held for microseconds, never across an `.await`.

## Deviations, honest

- **UDP is now probe-only; dissemination moved to TCP.** This is a change *from
  the spike* (which piggybacked updates on UDP), made to honor the §4.4 freeze
  (review hit 3). Faithful to the frozen RFC, not to the throwaway spike.
- **The 3A `link.rs` gained additive hooks** (`LinkHooks`, `connect_with`/
  `accept_with`, a `ReaderCtx`). `connect`/`accept` and all 3A behavior are
  unchanged — the existing 3A suite (interop/oversize/saturation/suppression/
  relay) passes untouched, which is the proof. The work order invited this ("the
  hook 3A left for you").
- **Link death → immediate Suspect.** A dead TCP link marks the peer Suspect at
  once (recovery via a probe ACK or fresh sync revives it), which sharpens kill
  detection below the gate. It is Suspect, never a direct eviction — the
  suspicion timer still governs Dead.
- **Digest is full-`(id, inc)` summary + full-state Sync anti-entropy**, not a
  compressed hash. For N ≤ 50 this is cheap and robust; a hash-based digest is a
  bandwidth optimization deferred with 3C's interest digest.

## Verification (run green)

Toolchain: stable 1.97.0.

- `cargo fmt -p beamsocket-mesh --check` — clean.
- `cargo clippy -p beamsocket-mesh --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — **148 passed, 0 failed, 4 ignored**: the pre-
  existing 71 Rust tests unchanged and green (3 pre-existing ignored HW gates),
  the 3A crate's 63, and 3B's new 14 (unit + the 5 integration gates). The 41 JS
  tests are untouched. The `membership` integration gates ran stable across
  repeated runs (~3.65 s wall each; convergence and kill comfortably inside
  their windows).

## Exit gates (DoD §13.2)

| Gate | Status |
|---|---|
| Tuned row shipped, cited to `0004-results.md`; cite-or-fail test | ✅ |
| Push-pull join; equal-incarnation conflict resolved by it | ✅ unit + end-to-end |
| UDP probe-only, frozen HMAC format, golden-bytes, replay-hardened | ✅ |
| MEMBERSHIP over TCP only; membership never on UDP | ✅ asserted |
| Link death → suspicion; reconnect drives the 3A Backoff seam | ✅ |
| Membership counters + table exposed for 3D | ✅ `MeshNode::{member_table, membership_counters, probe_counters}` |
| All §13.2 gates green; heal is a permanent CI regression | ✅ |
| Rule 4 stated; Rule 5 = 3A queues, no new type | ✅ |
| fmt, clippy `--all-targets -D warnings`, `cargo test --workspace` green | ✅ 148 passed |
| Existing 126-test baseline untouched | ✅ additive; no existing file changed except 3A `link.rs` (additive hooks, 3A suite green) |
