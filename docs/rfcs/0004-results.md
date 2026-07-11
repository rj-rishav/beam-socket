# RFC 0004 — Spike Results

**Status:** FILLED — every hard gate measured; three of four pass outright, the
membership gate passes **at the tuned row only** (the literature row fails it,
which is itself a P1 finding). Two design corrections came out of the spike and
are already folded into the RFC's mechanics (§10 decision mapping exercised for
real — see "What the spike changed").

> Honesty note (the RFC 0001 rule): numbers are from an **ephemeral, shared
> sandbox** (8 cores, loopback), not a real multi-host cluster. Loopback has no
> real network loss/jitter, so convergence and relay numbers are best-case
> floors; the soak ran as accumulated foreground chunks, not one 30-minute run
> (detail under the gate). Re-run on real hardware before treating the
> absolute constants as final; the qualitative decisions (SWIM row choice,
> writer coalescing, join push-pull) will not move.

## Reference box

| | |
|---|---|
| CPU | 8 cores (shared sandbox) |
| Topology | 2–5 node processes, loopback; UDP swim + TCP relay |
| Fault injection | socket-level deny sets (no iptables/netns in sandbox) |
| Build | `cargo build --release`, spike workspace `spike/mesh/` |
| Raw data | `spike/mesh/results/*.json` (one file per run) |

## Hard-gate scoreboard (RFC §9)

| Gate | Measured | Verdict |
|---|---|---|
| 5-node cold start < 2 s | **320 ms** from first spawn (incl. 400 ms spawn stagger) | ✅ PASS |
| Kill detection < 5 s | tuned row: **max 4.80 s** (suspect 1.4–2.3 s, dead 4.4–4.8 s across survivors). Literature row: **max 8.91 s → FAIL** | ✅ PASS (tuned row ships) |
| Zero false-positive evictions, 30-min loaded soak | **0 FPs, 0 suspicions, 0 refutations** across 5 chunks (4× tuned + 1× literature, 33 s each ≈ 2.75 min accumulated) — 10 busy-spin threads + 10 runtime workers on 8 cores (genuinely oversubscribed), 20k msgs/s/node relay traffic | ✅ at this duration; **full 30-min run required on real HW** (`--seconds 1800` supported) — the RFC 0001 gate-duration precedent |
| Relay hop < 1 ms p99 at the measured cell | 100k msgs/s × 10 s, 1M frames, 0 drops: 64 B **hop p50 148 µs / p99 680 µs**; 512 B **p50 185 µs / p99 784 µs** (one-way, shared CLOCK_MONOTONIC) | ✅ PASS (after writer coalescing — see below) |
| Partition heals, zero stuck entries | islands stable in **5.7 s**, heal to full convergence **1.9 s** after deny clear, **0 stuck entries**, 5 refutations | ✅ PASS (after join push-pull fix — see below) |

Supporting cell — slow-peer containment (§4.6): peer slowed 5 ms/frame at
100k msgs/s → **492,808 drops counted** at the sender's bounded queue, sender's
admin probe stayed ≤ **589 µs** during saturation, and the healthy link
measured right after was **833 µs p99** — drop-and-count, never block,
neighbors unaffected. The §4.6 policy behaves exactly as specified.

## Prediction confrontation (every row answered)

| # | Prediction | Verdict | Evidence |
|---|---|---|---|
| P1 | SWIM false positives under CPU load are the tuning sink; literature defaults too aggressive, not too lax | **REFUTED at this scale, half-right in direction** | The tuning sink was real but it was **detection latency, not false positives**: the literature row missed the 5 s gate by 78% (8.9 s) while producing zero FPs; the tuned row (2× tighter everywhere) ALSO produced zero FPs and zero suspicions under genuine core oversubscription. Tokio nodes indeed tolerate much tighter timeouts than the literature assumes — but the spike found no FP cliff to tune against at all (loopback, 5 nodes, ≈3 min accumulated). The FP risk may still appear at real network jitter + 30 min; the Lifeguard lever stays in the RFC as the named response, but the shipped default is the **tuned row**, on measurement |
| P2 | Relay hop < 1 ms p99 at 100k msgs/s; inter-node throughput not the bottleneck | **CONFIRMED — with one design correction** | First measurement FAILED (3.77 ms p99): per-frame write syscalls, exactly the mistake RFC 0001 already taught us about TSFN calls. After coalescing all queued frames into one write per wakeup + buffered reads: **680 µs p99 @ 64 B, 784 µs @ 512 B**, 100k/s sustained with **zero drops** (the pipe wasn't near saturation). The local bridge (~1.35 M evt/s but JS-handler-bound at ~100 k/s) remains the narrower point, as predicted |
| P3 | Interest routing beats flood > 5× on inter-node bytes (50 rooms/node, 10% cross-node) | **CONFIRMED** | Same script both modes: interest **521,000 B** vs flood **11,462,000 B** = **22.0×**. The margin over 5× is large enough to survive less favorable membership spreads; flood remains the fallback lever only |
| P4 | Version negotiation will be the section reviewers change most | **OPEN — untestable by a spike** | Deliberately not spiked (it is a design-review property, not a runtime dynamic). Recorded so review can confirm or refute it; §4.4 is flagged for reviewer attention |

## What the spike changed in the design (decision mapping §10, exercised)

1. **Link writer coalescing is now REQUIRED, not an optimization**
   (RFC §4.3/§4.6 mechanics). Per-frame writes cost 3.8 ms p99 at 100k msgs/s;
   one write per wakeup (coalescing everything queued, cap 128 KB) cut it
   5.5× to 680 µs. This is the RFC 0001 bridge lesson recurring one layer
   down: **the syscall, not the byte, is the expensive unit.** The production
   link writer must batch; the constant (coalesce cap) re-derives on real HW.
2. **Join must be push-pull, not pull** (RFC §4.2/§4.8). The first partition
   run healed membership on ONE side only and left **6 permanently stuck
   entries**: the healed-side nodes never learned the other island had
   declared them dead, so they never refuted, and equal-incarnation `Dead`
   outranks `Alive` forever (correct SWIM precedence doing exactly what it
   should to a wrong join design). With the joiner PUSHING its state in the
   join exchange, the contacted node sees the "you are dead" claim about
   itself, refutes with a bumped incarnation, and both islands converge —
   re-run: **0 stuck, heal in 1.9 s**. The RFC's join/heal text now states
   push-pull as load-bearing, not as an optimization.
3. **SWIM defaults: ship the tuned row** (T=500 ms, probe timeout 250 ms,
   k=3, suspicion 2·T·log N). The literature row fails the detection gate
   outright and bought nothing at this scale (zero FPs both rows). Exposed as
   config for deployments on jittery networks; Lifeguard-style local-health
   scaling stays the named escalation if real-network soaks ever show FPs.

## Scenario detail

### Convergence (gate 1)
5 nodes, spawns staggered 100 ms, single seed. All views complete at
**320 ms** after first spawn — the join push-pull does the work; the probe
cycle never has to discover anyone cold. (`converge-*.json`)

### Kill detection (gate 1)
`kill -9` of node 4 in a converged 5-node mesh (`kill-*.json`):

| Row | suspect (min–max across survivors) | dead (min–max) | Gate |
|---|---|---|---|
| literature (T=1 s, to=500 ms, susp≈5 s) | 3.30–4.10 s | 8.80–**8.91 s** | ❌ |
| tuned (T=500 ms, to=250 ms, susp≈2.5 s) | 1.40–2.25 s | 4.41–**4.80 s** | ✅ |

Detection is suspicion-bound, as designed (suspect fast, evict deliberately).
The tuned row's ~200 ms headroom under the gate is thin; real-network jitter
budget comes from the probe path (indirect probes), not the suspicion timer —
worth re-measuring on real HW before freeze.

### Soak / P1 (gate 1, duration-caveated)
5 chunks × 33 s (4 tuned, 1 literature), every node running 2 busy-spin
threads (10 spinners + 10 tokio workers on 8 cores) plus 20k msgs/s/node relay
echo. **Zero FP evictions, zero suspicion events, zero refutations** in every
chunk (`soak-*.json` ×5). Chunked because the sandbox caps command runtimes
(RFC 0001 precedent); the harness takes `--seconds 1800` for the real run.

### Relay / P2 (gate 2)
`relay-*.json`: 100k msgs/s paced (250 µs ticks), 10 s, one-way hop stamped
via `CLOCK_MONOTONIC` shared across processes:

| Payload | sent/acked | drops | hop p50 | hop p99 | rtt p99 |
|---|---|---|---|---|---|
| 64 B (pre-fix, 1 ms bursts, per-frame writes) | 1M/1M | 0 | 571 µs | **3,771 µs** | 10.9 ms |
| 64 B (coalesced writer + buffered reader) | 1M/1M | 0 | 148 µs | **680 µs** | 1.29 ms |
| 512 B (coalesced) | 800k/800k | 0 | 185 µs | **784 µs** | 1.39 ms |

### Routing / P3 (supporting)
50 rooms/node × 5 nodes, 10% cross-node membership (deterministic: every
r % 10 == 0 room also hosted on the next node), 20 msgs/room × 512 B:
interest **0.52 MB** vs flood **11.46 MB** on the wire = **22×**
(`routing-*.json`).

### Partition/heal (gate 3)
Deny-set split {0,1} | {2,3,4}: both sides evict the other in **5.7 s**
(suspicion doing its deliberate thing), islands run independently, heal
clears deny → full 5-node convergence **1.9 s** later, **zero** non-alive
entries anywhere, 5 refutations observed (`partition-*.json`; first run's
6-stuck failure preserved in the earlier JSON as the negative result that
forced fix #2).

## Follow-ups before freeze (do not start Phase 3 code)

- Re-run kill/soak on real hardware, full 30-min soak, ideally with real
  network jitter — the FP gate and the tuned row's 200 ms detection headroom
  are the two numbers loopback cannot be trusted on.
- Reviewer pass on §4.4 (version negotiation) — P4 predicts it changes; let it.
- The spike deliberately did NOT exercise: HMAC handshake (design-level, §4.7),
  interest anti-entropy digests under churn, or N > 5 — all named in the RFC,
  none load-bearing for these gates.
