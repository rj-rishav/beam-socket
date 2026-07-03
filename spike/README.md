# RFC 0001 Bridge Spike

**This is the current work item. Nothing else starts until this finishes.**

- Spec: [../docs/rfcs/0001-event-bridge.md](../docs/rfcs/0001-event-bridge.md)
- How-to: [../docs/ENGINEERING.md §4](../docs/ENGINEERING.md)
- Results go in: `../docs/rfcs/0001-results.md`

## Run

```bash
cargo test                                                        # generator bounds, encoder round-trip
npm --prefix harness install
node harness/index.mjs --design B --rate 100000 --payload 512 --profile noop
node harness/index.mjs --matrix                                   # full RFC §4 matrix → results/*.json
node harness/index.mjs --design B --gate                          # primary gate: 2× ceiling, 10 min, pathological
```

## Reminders

- Latency = Rust enqueue → JS handler entry (hrtime-correlated), nothing else.
- No sockets, no WebSocket framing, no TLS in here — they contaminate the measurement.
- The queue is bounded. Measure the overflow, don't prevent it.
