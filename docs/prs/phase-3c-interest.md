# PR: Phase 3C — Interest Routing

**Phase:** 3C only (ENGINEERING.md §13.3; implements RFC 0004 §4.3 + the §4.4
INTEREST/INTEREST_DIGEST catalog). Branch: `phase-3c-interest`, stacked on
`phase-3b-swim`. All code in `crates/mesh`; the crate **still has no reverse
dependencies** — this decides *where* a relay would go, 3D wires the engine that
*sends* it. No relay verbs fire cross-node payloads here, no ConnectionId
node-prefix, no SDK.

This is the efficiency phase: the mesh stops flooding and starts routing. A
targeted message goes only to the peers that host the target; the spike's
byte-reduction cell reproduces (~40× here vs the spike's 22× — same direction,
absolute is directional).

## What's in it

| Module | Responsibility |
|---|---|
| `interest` (new) | The **pure** interest core + wire codec: the `origin → {rooms, users}` table, edge-triggered `local_set`, per-origin seq discipline, the anti-entropy digest, the routing decision `interested_peers`, and the flood lever. No IO — so routing is model-checkable. |
| `node` | Owns an `InterestState`; routes INTEREST (0x05) / INTEREST_DIGEST (0x06) frames over the 3A links, disseminates edges on local transitions, runs the interest digest on a timer, sweeps evicted peers' interest, and exposes the routing seam + counters. |
| `link` (3A) | Links now advertise `features::INTEREST_ROUTING`, so INTEREST frames pass the 3A feature-intersection + sender-suppression checks. No 3A behavior change otherwise (its suite is green). |

### The three disciplines (§4.3), and the seam for 3D

- **Edge-triggered.** `InterestState::local_set(target, hosting)` returns an edge
  **only** on a real 0→1 / 1→0 transition; join #2..#n of a room returns `None`
  and puts nothing on the wire. Interest chatter is independent of per-room
  membership churn.
- **Per-origin seq.** Every edge carries the origin's monotonic seq; a receiver
  applies only strictly-newer seqs and drops stale / duplicate / reordered ones
  (counted `seq_drops`). The edge stream is **lossy by design**.
- **Anti-entropy digest.** A periodic `(origin, seq, set-hash)` digest — reusing
  the `membership_sync` digest shape — detects a divergence (a dropped edge, a
  gap) and repairs it with a full snapshot resync within one cycle. This is the
  self-healing net that makes the lossy edge stream safe. The hash is a
  deterministic FNV-1a over the sorted set (not `DefaultHasher`, which is
  per-process seeded — two nodes must agree on the hash).

**The seam 3D consumes:** `MeshNode::interested_peers(&Target) -> Vec<NodeId>` —
the remote peers to relay a room/user to (empty = no relay). Unreachable peers
are excluded. Local interest input is `MeshNode::set_local_interest(target,
hosting)` — the engine calls it on room/identity 0→1/1→0 transitions; in 3C a
test double calls it (core is not wired until 3D).

### The flood lever (`cluster.routing`)

`Routing::Interest` is the default; `Routing::Flood` makes `interested_peers`
return **all** live peers, ignoring the table. It exists as the operational
escape hatch — if interest state is ever suspected wrong in production, choose
correctness over efficiency on demand. It is documented and **never** the
default (`MeshConfig::routing` defaults to `Interest`, asserted by the type).

## §13.3 gate tests

| Gate | Test |
|---|---|
| **Routing correctness vs a flood reference model, under proptest churn** (add/remove/**partition/heal** with dropped edges) | `interest::interested_peers_never_under_delivers_vs_flood` — 200 cases; after convergence `interested_peers(R)` equals the flood ground truth on every node. Under-delivery is a hard failure. |
| Byte-reduction cell re-measured (50 rooms/node, 10% cross-node) | `interest::byte_reduction_vs_flood_cell` — **40.0×** (interest 256 KB vs flood 10.24 MB); the P3 `>5×` claim holds, direction reproduces the spike's 22×. |
| Digest repairs a deliberately dropped edge | `interest::digest_repairs_a_dropped_edge` (unit) + exercised on every proptest case (the model drops edges and relies on the digest) |
| Seq discipline (reorder/dup/stale dropped + counted) | `interest::seq_discipline_drops_stale_reordered_dup` |
| Partition: no relay to unreachable, no stuck interest | `interest::partition_ages_out_interest_no_relay_to_unreachable` (real mesh) + `evicted_peer_interest_is_swept` (unit) |
| Flood lever returns all peers | `interest::flood_lever_relays_to_all_peers` (real mesh) + `flood_lever_returns_all_peers_ignoring_interest` (unit) |
| Interest propagates + routes end-to-end | `interest::interest_propagates_and_routes_over_a_real_mesh` |

**The correctness gate is model-based:** a synchronous cluster of `InterestState`
tables with a lossy wire (edges dropped by a proptest-chosen mask, partitions
that split reachability), settled with digests, asserted against ground truth.
Because the interest core is pure, the model check is deterministic and shrinks —
no async flakiness in the correctness oracle.

## Rules audit

- **Rule 5 — no new queue type.** INTEREST and INTEREST_DIGEST frames egress
  through the 3A per-peer `PeerQueue` via `try_send` (the `membership_sync`
  precedent). The interest table is a `HashMap`/`BTreeSet`, not a wire queue.
- **Rule 4 — interest-table memory stated.** Per remote origin: one
  `OriginInterest` (a seq + two `BTreeSet`s of the room/user *names* it hosts).
  Cost is bounded by real hosted state — `(rooms + users advertised) × peers`,
  not by membership churn (edge-triggered). `InterestState::table_size()` is the
  gauge; evicted peers are swept (no stuck entries, the 3B lesson).
- **Rule 2 — lock discipline.** The interest lock is held for microseconds
  (apply an edge / build a digest / answer a query) and **never across an
  `.await`** — the async send happens after the lock is dropped.
- **§4.4 invariants intact** — UDP stays probe-only and frozen (untouched);
  sender-suppression is honored (INTEREST is feature-gated behind
  `INTEREST_ROUTING`, negotiated on every link).

## Deviations, honest

- **Byte-reduction reads 40×, not 22×.** The spike measured real wire bytes
  (framing + ACKs); this cell is a pure payload model over the routing
  decisions. Same direction, larger absolute — the work order calls the absolute
  directional in-sandbox; the pinned box confirms. Published, losses and all
  (local-only rooms cost zero inter-node bytes; only the 25 cross-node rooms do).
- **Digest snapshot authority.** A node re-sends a snapshot of an origin only
  when it is the origin (authoritative) or strictly ahead — this prevents two
  non-origin caches from ping-ponging equal-seq snapshots. Equal-seq divergence
  is repaired by the origin re-sending its own set.
- **Users are modeled but the correctness proptest exercises rooms.** The
  room/user paths are the same code (`Target`); the proptest covers rooms, unit
  tests cover the user path via the codec + `local_set`.

## Verification (run green)

Toolchain: stable 1.97.0.

- `cargo fmt -p beamsocket-mesh --check` — clean.
- `cargo clippy -p beamsocket-mesh --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — **162 passed, 0 failed, 4 ignored**: the pre-
  existing 71 Rust tests unchanged, the mesh crate's 3A+3B green, and 3C's new
  additions (8 interest unit tests + 5 interest integration incl. the 200-case
  proptest). The 41 JS tests are untouched.

`proptest` is added as a dev-dependency (already in the workspace lockfile via
core's dev-dep — no new downloads).

## Exit gates (DoD)

| Gate | Status |
|---|---|
| `interested_peers` correctness == flood model under proptest | ✅ 200 cases, join/leave/partition/heal + edge loss |
| Byte-reduction cell published, cited to `0004-results.md` | ✅ 40.0× (>5× P3 claim; spike direction reproduced) |
| Digest-repair + seq-discipline + partition tests green | ✅ |
| Flood fallback lever works, documented as non-default | ✅ `Routing::Flood`, default is `Interest` |
| fmt, clippy `--all-targets -D warnings`, `cargo test --workspace` | ✅ 162 passed |
| PR notes in house format; deviations honest | ✅ this document |
| Existing 148-test baseline untouched | ✅ additive; only 3A `link.rs` gained a features line (3A suite green) |
