# RFC 0001 — Event Bridge Spike

**Status:** FROZEN — accepted as the gate for all further runtime work
**Gate scope:** No rooms, presence, user targeting, clustering, or transport abstraction work until results land in `0001-results.md`
**Depends on:** nothing (this is deliberately the first thing built)

> The project currently has exactly one unanswered question: **can the Rust ↔ JS boundary sustain the architecture we want?** This RFC is the fastest way to answer it.

---

## 1. Why this is the critical unknown

Every architectural claim BeamSocket makes rests on one path:

```
Rust engine → event queue → ThreadsafeFunction → V8 callback → app handler
```

Three properties make it the highest-risk component:

1. **It cannot be benchmarked in pure Rust.** `core` components are criterion-benchmarkable in isolation; the bridge only exists with napi + V8 + libuv in the loop. Uncertainty is structural, not incidental.
2. **Its ceiling is the product's ceiling** for any app that subscribes to `message`. If the bridge sustains 200k events/sec, that number — not connection density — is what message-heavy users experience.
3. **Its saturation policy is API semantics.** What happens when JS can't keep up (drop, coalesce, backpressure-to-socket, disconnect) is observable behavior we must document and commit to. Building rooms first means building on delivery semantics we haven't validated.

## 2. Questions the spike must answer

1. What is the sustained event throughput ceiling (events/sec into JS handlers) per bridge design?
2. What p50/p99/p999 latency does batching add at low, medium, and saturated load?
3. Where is the copy-vs-external-buffer crossover? External (zero-copy) buffers carry GC-finalizer cost; small payloads may be cheaper copied into V8. Find the threshold (hypothesis: 1–4 KB).
4. What GC pressure does each design create (allocations per event, pause frequency at 1M events/sec)?
5. What actually happens at saturation — and is it observable via metrics?
6. Is the JS→Rust direction (`send`, `join` as plain napi calls) ever a bottleneck, or can we ignore it? (Hypothesis: ignore; verify cheaply.)

## 3. Candidate designs

| | Design | Mechanism | Expectation |
|---|---|---|---|
| A | Naive TSFN | One TSFN call per event | Baseline. Expected to lose badly; measured to quantify *why* batching exists |
| B | Batched objects | TSFN per flush; events as JS array of objects (flush at N=256 or 1 ms) | The ARCHITECTURE.md default. Simple, GC cost = per-event object allocation |
| C | Batched flat encoding | TSFN per flush; events encoded into one contiguous Buffer, decoded by a JS cursor reader | Near-zero native-side allocation; JS creates subarray views lazily. More code, less GC |
| D | SAB ring + doorbell | SharedArrayBuffer ring written by Rust; TSFN used only as a wake signal | Highest ceiling, highest complexity. Only pursued if C fails the gates |

All designs use a **bounded** queue between engine and bridge; overflow policy is part of the measurement, not an afterthought.

## 4. Measurement matrix

Synthetic event generator in Rust (no real sockets — isolate the bridge):

- **Payload sizes:** 64 B, 512 B, 4 KB, 64 KB (text and binary)
- **Offered load:** 10k, 100k, 500k, 1M, 2M events/sec, plus ramp-to-failure
- **Consumer profiles:** no-op handler; 10 µs synthetic work; **typical JSON handler** (`JSON.parse(payload)` + `JSON.stringify({ id, payload })` — informational, not gated: this is what most real users will write, so its numbers headline the results doc); pathological (5 ms stall every 100 ms) to exercise saturation policy
- **Batch parameters:** N ∈ {64, 256, 1024}, timer ∈ {0.25, 1, 4} ms — validate or replace the defaults in ARCHITECTURE.md §2.2

**Recorded per cell:** sustained events/sec, p50/p99/p999 end-to-end latency (Rust enqueue → JS handler entry, hrtime-correlated), CPU per 1M events, RSS, GC stats (`--trace-gc` + `perf_hooks`), events dropped/coalesced at saturation.

## 5. Pass/fail gates

### Primary gate — survival, not speed

Run the pathological consumer at 2× the design's measured throughput ceiling for 10 minutes. Pass requires **all** of:

- the engine↔bridge queue stays bounded (no unbounded memory growth, RSS flat)
- `bridgePressure` metric rises and is queryable while saturated
- drops/coalesces are counted and visible, never silent
- the runtime recovers to normal latency within seconds of load subsiding

A design that wins every throughput cell and fails this gate is **rejected**. Rationale: someone will run 200k connections with `await db.insert()` inside their message handler. The answer to "what happens next" must be *queue bounded, metric rises, drops visible, runtime stable* — never *memory grows forever*. That is the difference between a benchmark winner and production software.

### Performance gates

On an 8-core reference box (pinned in `benchmarks/README`):

- ≥ **1M events/sec** sustained at 512 B payloads with a no-op handler
- ≤ **2 ms p99** added latency at 100k events/sec (the common case must not pay for the extreme case)
- **Zero unbounded growth** in RSS or GC pause times over a 10-minute soak at 80% of ceiling

## 6. Decision mapping

- B passes all gates → ship B; C/D noted as future headroom.
- B fails throughput but C passes → ship C; the extra decode complexity is justified by measurement, not taste.
- C fails → D is designed in a follow-up RFC before proceeding. **Rooms/presence do not start until a bridge design passes.**
- The copy/external-buffer crossover threshold becomes a constant in `buffers.rs` with the benchmark cited in a comment.

## 7. Pre-registered predictions

Recorded before any code exists; `0001-results.md` must confront each one, confirmed or refuted. This keeps our "benchmarks must be transparent and honest" rule pointed at ourselves first.

| Design | Prediction |
|---|---|
| A | Dies immediately. Useful only as the baseline that quantifies why batching exists |
| B | Likely passes — TSFN + 256-event batches + 1 ms flush is probably enough, and better than expected |
| C | Wins on large payloads and high event rates, where GC pressure becomes the dominant cost |
| D | Never gets built — and that is the *good* outcome. If B or C pass, D becomes interesting instead of required |

## 8. Spike scope

Deliberately throwaway-grade, ~3 crates/packages:

```
spike/
├── bridge-core/     # event generator + bounded queue (no sockets)
├── bridge-node/     # napi binding, designs A–C behind a flag
└── harness/         # JS consumer, load driver, stats collection, report output
```

Estimated: days, not weeks. Results land in `docs/rfcs/0001-results.md`; the winning design and its constants graduate into `crates/node/src/bridge.rs`.

## 9. What this deliberately ignores

WebSocket framing, TLS, rooms, identity, real network I/O — all of it. Any of those in the spike contaminates the measurement. The bridge is benchmarked against a synthetic generator precisely so the number we get is the bridge's number.
