# RFC 0001 — Results

**Status:** EMPTY — Phase 0 not complete. This file existing is not the gate;
this file being FILLED is the gate.

## Reference box

(pin hardware/OS/Node/Rust versions here)

## Prediction confrontation (RFC §7 — every row must be answered)

| Design | Prediction | Verdict | Evidence |
|---|---|---|---|
| A | Dies immediately; baseline only | TBD | |
| B | Passes; TSFN + 256/1ms is enough | TBD | |
| C | Wins on large payloads / high rates (GC-dominant) | TBD | |
| D | Never gets built | TBD | |

## Primary gate — survival at 2× ceiling (RFC §5)

| Design | Queue bounded | RSS flat | Pressure visible | Recovery | PASS/FAIL |
|---|---|---|---|---|---|

## Performance matrix

(harness `--matrix` output: sustained events/sec, p50/p99/p999, CPU/1M events,
RSS, GC pauses — per payload × rate × profile × design)

## Decision

Winning design: TBD
Constants graduating to `crates/node/src/bridge.rs`: TBD
Copy/external-buffer crossover for `buffers.rs`: TBD
