# PR: Phase 3D — Relay Verbs + Engine Integration (the finale)

**Phase:** 3D only (ENGINEERING.md §13.4; implements RFC 0004 §4.3/§4.5/§4.6).
Branch: `phase-3d-relay`, stacked on `phase-3c-interest`. **For the first time in
Phase 3, core changes** — this is the phase that wires `crates/mesh` into
`crates/core` behind the Engine facade. The 112 pre-mesh tests (71 Rust + 41 JS)
are the tripwire, and they are green **unchanged**.

## What's in it

| Area | Change |
|---|---|
| `crates/mesh` (queue) | `PeerQueue` now holds refcounted **`Bytes`**: one relay-frame allocation is clone-by-refcount into every interested peer's queue (§4.6 serialize-once across the hop). |
| `crates/mesh` (node) | Relay send API (`MeshNode::relay` — builds one frame, copies the payload **once**, refcount-clones to peers), a relay-inbound hook (`RelayHandler`), `RelayKind`, per-peer pressure/peer-count for stats. |
| `crates/core` (cluster) | New `cluster.rs`: `Cluster` owns the mesh node + the relay body codec + the receive-side **local fan-out** (never re-forwarded) + relay counters + interest passthrough. |
| `crates/core` (engine) | `Engine` gains `Option<Arc<Cluster>>`. Targeting verbs relay after local fan-out; **the `None` arm is byte-identical to pre-3D**. Interest edges fire on the real 0→1/1→0 transitions (`join`/`leave` for rooms, bind/unbind + disconnect-cleanup for users, via `ConnCtx`). `cluster: { listen, seeds, secret, nodeId }` config. |
| SDK (TS) | `ids.ts` node-prefix codec (§4.5) — clustered ids are three-segment `node-hi-lo`, single-node stays **two-segment and byte-identical**; `ClusterConfig` + `ClusterStats` types. |

## The hard mechanics

### Serialize-once across the hop (§4.6)

The app payload is serialized **once** (at the FFI boundary, into `Bytes`). Local
fan-out clones that handle by refcount (the 1B one-allocation invariant,
unchanged). The relay copies it **once** into a single frame `Bytes`, which every
interested peer link then holds by **refcount** (the queue is now `Bytes`, not
`Vec<u8>`) — never re-serialized per peer. Proven by `serialize_once_across_the_hop`:
two local members receive the **identical allocation** (`as_ptr` equality), and
the remote member receives the same bytes delivered exactly once.

### No relay loop (§4.3)

A received `RELAY_*` frame is decoded and fanned out to **local** recipients only
— the mesh's inbound hook calls core's `deliver_local`, which never sends. The
origin already relayed to every interested node, so re-forwarding would be a
loop. `re_relays` is a counter asserted to stay **0** across all nodes.

### Zero-cost when single-node (Rule 1, the finale gate)

`cluster` absent → `Engine.cluster` is `None` → **no mesh is spawned**, and every
targeting verb takes a `match &self.cluster { None => <pre-3D code>, .. }` arm
that is byte-identical to before this PR (the payload is moved in, not cloned; no
relay branch). Measured overhead of the single-node verb path (a room miss = one
`Option` match + one sharded lookup returning `None`): **~405 ns/call** over
300k calls (`single_node_is_zero_cost_and_bit_identical`) — within noise, no mesh
touched.

### Node-prefixed ConnectionId (§4.5)

`toSocket(id)` routes to the owning node: the id string carries a base-36 node
segment when clustered. `decodeSocketId` accepts both shapes — an old two-segment
id **round-trips** with `node` undefined ("this node"); a three-segment id
carries the owner; malformed ids are cleanly **rejected** (`null`). Single-node
ids are unchanged, byte for byte.

## §13.4 gate tests (`crates/core/tests/phase3d.rs`)

| Gate | Test |
|---|---|
| 3-node E2E: every verb cross-node exactly once; `except` honored across nodes; `toSocket` owning-node only; `toUser` all nodes' devices | `e2e_every_verb_reaches_remote_exactly_once_except_honored` |
| Serialize-once across the relay hop (pointer identity) | `serialize_once_across_the_hop` |
| Delivery under partition in 1C currency (drops counted, **no queue-and-forward**) | `partition_delivery_is_1c_currency_no_queue_and_forward` |
| No relay loop (received relay never re-forwarded; `re_relays == 0`) | asserted in the E2E |
| Zero-cost single-node, with numbers | `single_node_is_zero_cost_and_bit_identical` |
| ids codec: old-shape round-trips / cleanly rejected | `ids.ts` (verified: 2-seg ↔ 3-seg, garbage → null) |

The E2E runs at the **Cluster facade** — three real mesh nodes (real TCP links,
real interest routing, real `RELAY_*` frames over the wire), with mock local
connections in each node's registry standing in for WebSocket clients (the WS
transport is the unchanged 1B/1C path). Interest is driven exactly as the Engine
drives it. This exercises the entire relay path end-to-end; only the WS
transport (untouched) is mocked.

## The 112-test tripwire — additive-by-proof

- **71 Rust tests: green, unchanged.** The one mechanical edit outside new code:
  `ConnCtx` gained a `cluster: Option<..>` field, so the two `ConnCtx` literals in
  `phase1a.rs` add `cluster: None`. That is a required-field addition, **not** a
  change to any assertion or expectation — single-node behavior is identical.
- **41 JS tests: green, unchanged** (rebuilt `dist` from the updated `src` +
  the existing addon). `tsc --noEmit` clean. The TS additions are optional
  (`cluster?`, `ClusterStats?`) and the ids codec is backward-compatible.

## Rules audit

- **Rule 1 — relay fan-out is Rust-only.** No per-message JS, clustered or not:
  local fan-out is the 1B Rust path; the relay send/receive and cross-node local
  fan-out are all in Rust; the mesh has no JS boundary. When unclustered, there
  is no branch cost (the `None` match arm).
- **Rule 4 — node-prefix cost stated.** The node id lives in the id **string**
  and the `RELAY_SOCKET` wire form, **not** in per-connection state — zero new
  bytes per connection (the u64 slab id is unchanged; `NodeConnId` is a transient
  routing value). The cluster adds O(N ≤ 50) peer state, not O(connections).
- **Rule 5 — relay egress is the 3A bounded `PeerQueue`.** No new queue type; the
  queue now carries `Bytes` (a refcount, not a new structure), keeping its
  byte-bound + drop-and-count + pressure gauge intact.
- **Bridge constants untouched;** the binding (`crates/node`) is unchanged.

## Deviations, honest

- **JS-level cluster activation is config-surface + codec, not yet wired through
  the addon.** The functional cross-node integration and **every hard gate** are
  proven at the Rust facade (`Cluster` + the 3-node E2E). Passing `cluster`
  config from the SDK through the binding into `Engine::start`, populating
  `stats().cluster`, and emitting node-prefixed ids from native all require
  **rebuilding the native addon** (`crates/node`) — deliberately **not** done, to
  keep the 41-JS-test tripwire on a stable, unchanged addon. The binding compiles
  cleanly against the new core→mesh dependency (`cargo build -p beamsocket-node
  --features napi`), so the rebuild is a mechanical follow-up, not a redesign.
  The Rust `Config` already carries `cluster`, and `ids.ts` already encodes the
  node prefix — the seam is complete on both sides of the FFI.
- **Interest edges use a post-transition member-count check** (`rooms.info(...).
  members == 1` / `== 0`) rather than a new signal from `rooms.rs`, so the
  existing `MembershipChange` return type (and its tests) are untouched.
- **The partition drop counter** folds "unreachable interested peer" (`no_link`)
  into `relayDrops` — a drop is a drop, no stronger promise (1C currency). No
  queue-and-forward: the partition-time message is never delivered, even after
  heal (asserted).

## Verification (run green)

Toolchain: stable 1.97.0; Node v18.

- `cargo fmt` (mesh crate + the core files this PR touched) — clean; `events.rs`
  and other pre-existing-drift files left untouched.
- `cargo clippy --all-targets -- -D warnings` (core + mesh) — clean; `cargo
  clippy -p beamsocket-node --features napi -- -D warnings` — clean.
- `cargo test --workspace` — **166 passed, 0 failed, 4 ignored** (the 71 pre-mesh
  Rust tests unchanged; 3A/3B/3C mesh green; the 4 new §13.4 gates).
- `tsc --noEmit` — clean. `node --test __tests__/` — **41 passed**.

## Exit gates (DoD §13.4)

| Gate | Status |
|---|---|
| 3-node E2E: all verbs cross-node, exactly once, `except` honored | ✅ |
| Serialize-once proven across the relay hop | ✅ pointer identity + one shared frame `Bytes` |
| Partition semantics in 1C currency (drops counted, no stronger) | ✅ no queue-and-forward |
| Zero-cost single-node, proven with numbers | ✅ ~405 ns/call, `None` arm bit-identical, no mesh task |
| 112 pre-mesh tests green unchanged; full suite green | ✅ 71 Rust + 41 JS unchanged; 166 Rust total |
| fmt, clippy ×2 (default + napi), cargo test --workspace, tsc, npm test | ✅ |
| PR notes: gates, rules audit, single-node numbers, honest deviations | ✅ this document |
