# Architecture review and adaptive fan-out exclusions

## Scope and baseline

This is a source-based review of revision `d961332`, not a replacement for the
historical Phase 1 proposal in `docs/ARCHITECTURE.md`. The goal is one measurable,
compatible improvement, not an unvalidated runtime rewrite. Existing untracked
`__agent__/` and `benchmarks/oci/` files are outside this work.

Baseline verification: `cargo test --workspace` passes all non-ignored tests;
a freshly built native addon and SDK pass all 50 JS tests. Hardware-sensitive
ignored tests and the real-hardware release gates remain separate.

## The implemented architecture

```text
Node application
  packages/beamsocket/src/{server,socket,rooms}.ts
    synchronous NAPI commands             decoded event batches
              |                                    ^
  crates/node/src/binding.rs                bridge.rs + buffers.rs
              |                                    ^
  crates/core/src/engine.rs                 bounded event channel
     |        |        |                            ^
  registry  rooms   identity                connection read tasks
     |        \       /                            ^
     |       broadcast.rs                          |
     |            |                          WebSocket transport
     +----> bounded per-connection mailbox -------> writer tasks
     |
     +---- optional cluster.rs --> crates/mesh --> remote local fan-out
```

- **SDK:** `BeamSocket` owns lifecycle, socket proxies, authorization correlation,
  and event dispatch. `Target` encodes local and remote excluded IDs separately.
  Each room/user/all `.send()` makes one native call; application message
  handlers still run on Node's single JS thread. The native fan-out loop is
  synchronous on that calling thread: Rust ownership does **not** make its
  enqueue work event-loop-free. Only subsequent socket I/O runs asynchronously.
- **Binding:** `binding.rs` translates IDs, config and buffers. Outbound payloads
  become Rust-owned `Bytes`. `bridge.rs` batches Rust events into one flat buffer
  for the TSFN; its adaptive low-load flush is already implemented. The old
  architecture document's unconditional 1 ms wait is historical, not current.
- **Core lifecycle:** `Engine` owns a Tokio runtime and sharded registries.
  Standalone listeners and Unix HTTP fd handoff converge on connection setup.
  Admission, identity, ping/pong, cleanup and administrative actions live here.
- **Connections:** a reader and supervised writer task per connection, a bounded
  control channel, first-signal-wins close watch, and a mutex-protected,
  payload-byte-accounted mailbox. Slow recipients have individual policies.
  IDs include shard, slab slot, and generation to prevent stale-slot aliasing.
- **Membership:** rooms and users use sharded DashMaps; connections use 16
  mutex-protected slabs. Room membership is bidirectional. Mutations acquire
  connection shard then room map. Fan-out copies membership and drops the room
  guard *before* obtaining connection handles. This lock order must not change.
- **Fan-out:** room and user targets snapshot IDs; all-target snapshots handles.
  Each recipient receives a reference-counted clone of one payload allocation.
  `FanoutReport` counts queue acceptance, pressure and missing connections, not
  successful network delivery. Room messages are recorded even if all members
  are excluded.
- **Mesh:** `cluster.rs` adds local delivery plus an optional inter-node relay at
  the engine facade. `crates/mesh` supplies authenticated links, SWIM membership,
  interest routing and bounded/coalesced writes. Relays filter node-tagged
  exclusions into local full-width IDs before calling the same `broadcast`.
- **Diagnostics:** atomics plus an optional background rate sampler; read-side
  snapshots and bounded top-N queries. No new instrumentation belongs in the
  message hot path for this change.
- **Subscription caveat:** the current core reader submits all inbound data
  frames to the bridge, and the SDK creates a proxy for every Opened event.
  Historical documentation describing subscription-gated inbound work or lazy
  proxy creation is aspirational, not what these paths currently implement.
- **Tests:** Rust unit/property/integration tests cover registries, backpressure,
  sockets and mesh; JS tests cover the rebuilt native binding and SDK.
  `spike/` is historical bridge experimentation, not the production engine.

## Candidate improvements and decision

These are project-specific design proposals, not claims of algorithmic novelty.

1. **Recipient-aware exclusion indexing (validated and implemented).** At the
   baseline both fan-out loops call `except.contains(&id)`: worst-case
   O(recipients × excludes).
   Keep borrowed linear scans for small workloads; build a temporary index only
   when enough recipient probes amortize setup. No persistent cache, new lock,
   public API or wire-format change. Relayed broadcasts benefit automatically.
2. **Generation-aware cached room snapshots (deferred).** Cache immutable member
   arrays between membership mutations to avoid repeated HashSet iteration and
   allocations in stable hot rooms. Requires a precise invalidation protocol,
   churn benchmarks and a memory retention bound; writes may become more costly.
3. **Shard-grouped recipient lookup (deferred).** Group an existing member
   snapshot by the 16 connection shards, resolving batches of handles under one
   lock per shard. This could amortize lock acquisition for large rooms, but
   sorting/bucketing adds work and longer lock holds can hurt admission/churn.
   Never push mailboxes while holding a registry shard.

### Exclusion alternatives to measure

| Design | Setup | Lookup | Scratch memory | Main risk |
|---|---|---|---|---|
| Current borrowed slice | none | O(E) | none | O(R×E) for large lists |
| Always hashed | O(E) expected | O(1) expected | O(E), hash-table overhead | penalizes empty/sender-only/tiny targets |
| Sorted copy | O(E log E) | O(log E) | 8×E bytes | sorting is wasted on tiny targets |
| Adaptive index | chosen by R and E | scan or indexed | none on small path | threshold portability |
| Slot bitmap | O(E) | O(1) | depends on slot range | stale generations alias live IDs unless extra state is stored |

Use full `ConnectionId` equality in every variant. Do not use a slot-only bitmap,
change caller ordering, deduplicate recipients, or mutate caller exclusions.
Randomized standard-library hashing avoids introducing an adversarial custom
hasher. Sorted indexing is another deterministic, allocation-bounded option.

## Experiment contract (written before changing production)

1. Compare linear, sorted and hashed membership, **including per-broadcast index
   construction and destruction**, with no/one/many exclusions and tiny/large
   recipient sets. Test hits, misses, duplicate and generation-distinct IDs.
2. Keep the old `broadcast.rs` as a benchmark-only reference. Compare complete
   core fan-out (snapshot, lookup, filtering, enqueue) against the chosen
   implementation in the same process; alternate variant order and retain raw
   samples. Drain mailboxes outside the timed interval to avoid pressure effects.
3. Accept only if large exclusion workloads improve at least 2× in median core
   fan-out time, while representative empty/single-exclusion and tiny-target
   cases have no repeatable regression above 10%. If a small-path timing is
   noisy, repeat and publish the variability rather than relaxing the gate.
4. Differential/property tests must preserve recipient sets, exact report
   accounting, payload sharing, text/binary flags, duplicate exclusions, stale
   generations, missing targets, and backpressure behavior across all three
   target kinds. Existing cross-node exclusion tests must remain green.
5. Run fmt, clippy (workspace/all targets and native feature), workspace tests,
   rebuilt native+TypeScript, and JS tests after integration.

This is a local CPU/core benchmark, **not** a network latency or pinned-box
100k-fan-out result. It cannot close the existing hardware release blockers.

## Prototype selection

The membership-only experiment favored sorting over randomized hashing in most
cells: for example 1,024 recipients/256 mixed exclusions was roughly 14× faster
than linear scanning in the first exploratory run. Hashing won at the largest
16,384/4,096 cell, but sorting uses a smaller, deterministic scratch allocation
and avoids a second strategy/crossover. These are lookup-only results, not yet
full fan-out gains.

The prototype uses sorting only when `E >= 32`, `R >= 64`, and `R >= E`.
Otherwise it borrows the original slice. The last guard deliberately forgoes
some large-miss-list wins: huge exclusion lists can have early hits for all
members of a tiny target, where sorting the unused tail is wasteful. It also
bounds scratch to `8 × E <= 8 × R` bytes, per call, with no retained cache.
Duplicate exclusions are harmless; no deduplication pass is needed.

## Other findings, not silently included in this patch

- **Benchmark validity:** `benchmarks/driver.mjs` installs a `once` listener for
  every pipelined echo. One incoming reply resolves multiple outstanding
  promises (also on the Socket.IO path). Historical pipelined throughput and
  16-KiB throughput numbers need remeasurement after fixing correlation. This
  report uses neither as an acceptance gate. The new core benchmark checks
  actual mailbox payloads, once per selected recipient. Sequential echo latency
  does not have that particular accounting bug.
- Remote `toSocket` relay delivery ignores `PushOutcome::Disconnect`, unlike
  local send and broadcast. Its 1013-close behavior needs a separate fix/test.
- Zero-byte frames consume mailbox entries but no payload-byte budget; a
  separate item bound or accounting policy is needed for a strict memory bound.
- Clustered presence/user-query IDs and admin disconnect node scoping need a
  separate correctness review. These are independent of exclusion lookup.
- The rejected 1-KiB codec buffer remains rejected in code; do not reverse that
  decision without corrected throughput measurements and a new proposal.

## Results and integration decision

**Accepted and integrated on 2026-09-30.** Production changes are confined to
`crates/core/src/broadcast.rs`; no SDK API, mesh format, queue, payload, or lock
changes. The indexed branch costs O(E log E + R log E), with O(E) temporary
memory. The borrowed branch remains O(R×E). The algorithm does not promise a
speedup for every possible exclusion distribution or machine.

### Measured complete core fan-out

Same machine: Intel i5-1135G7 (4 cores/8 logical CPUs), Linux 7.0.0-31-generic,
Rust 1.97.1, release profile. No CPU pinning or governor changes. Node 24.15.0
was used for JS regressions, not the core timing. Measurements were sequential,
without concurrent builds/tests. Baseline and candidate run in the same process
on the same registries; both source modules are compiled locally to avoid a
cross-crate inlining asymmetry. The frozen reference is
`crates/core/benches/support/broadcast_linear.rs` (the `d961332` implementation).

Each cell has five warm-up calls per variant, then seven rounds with alternating
order. Iteration counts target about 3 ms of timed work per sample, clamped to
2–500 iterations. Reported values are the median of the seven sample means,
not per-call percentiles. Every call checks report counts and drains/checks
actual selected mailboxes outside the timer. Snapshot allocation, exclusion
index construction/destruction and enqueuing are inside the timer. Payload is
64 bytes; all benchmark recipients are healthy. Pressure is tested separately.

Representative **mixed-hit** results from `fanout-exclusions-integrated.csv`:

| Target | Recipients / exclusions | Linear baseline | Integrated | Speedup |
|---|---:|---:|---:|---:|
| Room | 1,024 / 0 | 81.83 µs | 80.75 µs | 1.01× |
| Room | 1,024 / 1 | 81.09 µs | 79.82 µs | 1.02× |
| Room | 128 / 128 | 10.00 µs | 6.09 µs | 1.64× |
| All | 128 / 128 | 11.87 µs | 8.61 µs | 1.38× |
| Room | 1,024 / 32 | 82.06 µs | 80.49 µs | 1.02× |
| Room | 1,024 / 256 | 168.82 µs | 74.95 µs | 2.25× |
| User | 1,024 / 256 | 174.13 µs | 76.46 µs | 2.28× |
| All | 1,024 / 256 | 175.38 µs | 77.33 µs | 2.27× |
| Room | 4,096 / 1,024 | 2.036 ms | 0.377 ms | 5.40× |
| User | 4,096 / 1,024 | 2.013 ms | 0.404 ms | 4.98× |
| All | 4,096 / 1,024 | 2.153 ms | 0.366 ms | 5.88× |
| Room | 16,384 / 4,096 | 33.481 ms | 6.361 ms | 5.26× |
| All | 16,384 / 4,096 | 34.139 ms | 5.420 ms | 6.30× |

The 2× gate holds for indexed cells with E >= 256 (minimum 2.07× across all
36 such cells). The index also engages for smaller lists, where the gain is
smaller: 128/128 ranges 1.24–2.70× by target/distribution, and 64/32 and
1,024/32 are neutral (0.98–1.08×) — no loss, but no win either.

All 108 cells (9 cardinality pairs × 4 distributions × 3 targets) and their raw
samples are retained. Distributions: stale-generation misses, alternating
hits/misses, duplicate-only lists, and lists containing recipient IDs. The
`all-hit` label means exclusion entries hit recipients; it excludes *every*
recipient only when E >= R. At E < R it is a hit-only subset.

**Losses/noise are included:** the first prototype run had one tiny-user-target
outlier: 8 recipients/4,096 hit entries, 89.32 ns → 104.85 ns (+17.4%). It did
not repeat. The second prototype run's worst common/tiny-target median delta
was +5.9%; the integrated run's was +7.0%. Neither of those runs had any cell
above +10%. Common empty/single-exclusion sends are essentially unchanged, not
claimed as a performance win. Large-exclusion cells met the 2× gate before
integration. The largest cells are cache-sensitive and should not be used to
extrapolate network fan-out latency.

### Artifacts and reproduction

- `benchmarks/results/exclusion-filter.csv`: membership strategy exploration.
- `benchmarks/results/fanout-exclusions-prototype.csv`: first full prototype A/B.
- `benchmarks/results/fanout-exclusions-prototype-r2.csv`: repeated prototype A/B.
- `benchmarks/results/fanout-exclusions-integrated.csv`: integrated source A/B.

From repository root (redirects below create **new** files, not overwrite the
checked-in evidence):

```sh
cargo bench -p beamsocket-core --bench exclusion_filter > /tmp/exclusion-filter.csv
cargo bench -p beamsocket-core --bench fanout_exclusions > /tmp/fanout-exclusions.csv
cargo test -p beamsocket-core --test fanout_exclusions
cargo test -p beamsocket-core --lib broadcast
```

Group CSV rows by all dimensions except `round`, `iterations` and
`ns_per_broadcast`; take the median of `ns_per_broadcast` per variant, then divide
baseline median by candidate median. Both benchmark commands use only existing
workspace dependencies and stable Rust. Performance gates are deliberately not
assertions in shared CI.

### Correctness and final validation

- Two new unit/property tests check threshold boundaries, borrowed small paths,
  arbitrary full-width IDs, and membership equivalence without caller mutation.
- Two new Rust integration tests compare the actual production fan-out against
  the frozen baseline: all target kinds, 10 exclusion sizes, duplicate/stale IDs,
  actual slot recycling, missing/closed connections, all three pressure
  policies, queue contents, shared payload pointers, text/binary flags, close
  signals, counters and all-excluded room accounting.
- One new JS integration test uses 80 real WebSocket clients and 41 chained
  exclusions through room and user targeting. Per-socket FIFO barrier messages
  establish exact delivery without sleep-based negative assertions.
- `cargo fmt --all --check`: pass.
- `cargo clippy --workspace --all-targets -- -D warnings`: pass.
- `cargo clippy -p beamsocket-node --features napi --all-targets -- -D warnings`: pass.
- `cargo test --workspace`: **174 passed, 4 intentionally ignored**.
- Fresh native release build and TypeScript build: pass.
- `npm test -w beamsocket`: **51/51 passed**, including existing cluster/remote
  exclusion regressions.

The installed CodeGraph CLI rejects the documented `affected --git-diff` option;
explicit-file analysis returned no affected tests despite the known callers.
The index was synced, but full workspace and JS suites were used rather than
trusting that false-negative affected-test result. No hardware release gate,
long soak, or Autobahn run is claimed here.
