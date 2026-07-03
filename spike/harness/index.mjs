// RFC 0001 harness — load driver + stats collector.
// Usage: see spike/README.md. Spec: RFC 0001 §4 (matrix), §5 (gates).

// Consumer profiles (RFC 0001 §4). JSON profile is informational but
// headlines the results doc — it's what real users write.
export const profiles = {
  noop: () => {},
  work10us: () => {
    const end = process.hrtime.bigint() + 10_000n;
    while (process.hrtime.bigint() < end);
  },
  json: (payload) => JSON.stringify({ id: 1, payload: JSON.parse(payload) }),
  pathological: (() => {
    let last = 0;
    return () => {
      const now = Date.now();
      if (now - last >= 100) {
        last = now;
        const end = now + 5;
        while (Date.now() < end); // 5 ms stall every 100 ms
      }
    };
  })(),
};

// TODO(Phase 0, steps 3–6 — ENGINEERING.md §4):
// - parse CLI: --design A|B|C --rate --payload --profile --matrix --gate
// - load ../bridge-node/*.node, start with selected design
// - record: sustained events/sec, p50/p99/p999 enqueue→handler latency,
//   CPU/1M events, RSS, GC stats (perf_hooks), drops
// - --matrix: run RFC §4 cells, write results/*.json
// - --gate: pathological @ 2× ceiling for 10 min; assert RSS flat,
//   pressure visible, recovery after load stops
console.error('Harness not implemented yet — Phase 0, docs/ENGINEERING.md §4');
process.exit(1);
