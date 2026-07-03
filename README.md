# BeamSocket

**The high-performance networking runtime for Node.js.**

Rust data plane, JavaScript control plane. Maximum connections, minimum overhead.

**Status:** pre-alpha — Phase 1A (echo server). Phase 0 gate met: RFC 0001 [results](docs/rfcs/0001-results.md) — Design C graduated.

| Doc | Purpose |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design and the rules that govern it |
| [docs/rfcs/0001-event-bridge.md](docs/rfcs/0001-event-bridge.md) | The frozen RFC gating all runtime work |
| [docs/ENGINEERING.md](docs/ENGINEERING.md) | What to build, in what order, and how to know you're done |

## Layout

- `crates/core` — Rust engine (no NAPI, ever)
- `crates/node` — NAPI-RS binding
- `packages/beamsocket` — the npm package (TypeScript SDK)
- `spike/` — RFC 0001 bridge spike (current work)
