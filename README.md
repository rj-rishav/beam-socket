# BeamSocket

**The high-performance networking runtime for Node.js.**

Rust data plane, JavaScript control plane. Maximum connections, minimum overhead.

**Status:** `0.1.0-alpha.0` — **Phase 1 + 1.1 merged to master** (echo, rooms,
broadcast, identity, admission limits, presence, metrics, graceful close, HTTP
attach), all gates closed in sequence; 60 Rust + 26 JS tests green on the merged
tree. Connections, rooms, users, and admission control all run in Rust; the
whole per-message data plane stays off the JS event loop. Phase 0 gate met: RFC
0001 [results](docs/rfcs/0001-results.md) — Design C graduated. **Phase 2
(runtime maturity) is complete and merged**: the full observability read surface
(`stats`, `topRooms`, `backpressureReport`, `memoryUsage`, Prometheus export —
zero hot-path cost, proven) plus the admin verbs (`disconnectSocket`,
`disconnectUser`, `closeRoom`). **Phase 3 (cluster mesh) is in progress** — RFC 0004 is FROZEN (soak-conditional) and implementation follows ENGINEERING §13 (3A–3D)
— see [ENGINEERING.md §12](docs/ENGINEERING.md).

**Release blockers before the alpha goes public** (need real hardware /
credentials, not closable in a sandbox): pinned-box bridge-constant
re-confirmation (`--gate-seconds 600`), pinned-box benchmark suite (100k fan-out
gate, Socket.IO ≥25k, echo p99), the full 10-minute soak, npm publish +
per-platform install test, and the darwin CI run that proves the macOS attach
row. **Before any release claims cluster support:** RFC 0004's 30-minute
real-hardware mesh soak, and swapping the mesh's vendored HMAC/SHA-256 for the
audited `hmac`/`sha2` crates (auth-path crypto ships audited, not hand-rolled). **Parked backlog** (deliberately after Phase 2): RFC 0003 engine-side TLS
(`listen(443, { cert })`, rustls) and the Windows fd-handoff spike
(`WSADuplicateSocket`).

## Attach to an existing Express/Fastify server (Phase 1.1)

Run BeamSocket on your existing HTTP server — one port, one deployment. See
[`examples/express-attach`](examples/express-attach).

```ts
const httpServer = app.listen(3000);           // your Express/Fastify server
const io = new BeamSocket({ server: httpServer, path: '/ws' }); // no io.listen()
io.on('connection', (socket) => socket.on('message', (d) => socket.send(d)));
process.on('SIGTERM', async () => { await io.close(); httpServer.close(); }); // drain WS first
```

**Support matrix (RFC 0002).** TLS terminates at your load balancer (attach is
plaintext); `{ server }` throws with a fallback pointer where unsupported.

| Platform | Plaintext `http.Server` | `https.Server` |
|---|---|---|
| Linux | ✅ fd handoff | ❌ throws → TLS-at-LB / standalone port |
| macOS | ✅ fd handoff (CI-gated) | ❌ throws |
| Windows | ❌ throws → standalone `listen()` port | ❌ throws |

## Quickstart

```ts
import { BeamSocket } from 'beamsocket';

const io = new BeamSocket({
  limits: { maxConnectionsPerIp: 100, maxRoomsPerConnection: 100 },
  trustProxy: ['10.0.0.0/8'], // honor X-Forwarded-For only from your LB
});

// One connection-time JS hook (never per message). Return a userId to bind a
// first-class User; toUser() then reaches every device that user has.
io.authorize(async (req) => {
  const user = await verify(req.headers.authorization);
  return user ? { accept: true, userId: user.id, metadata: { plan: user.plan } }
              : { accept: false, code: 4401 };
});

io.on('connection', (socket) => {
  socket.join('lobby');                          // rooms
  socket.on('message', (data) => socket.send(data)); // echo
});

// Targeting — each is ONE FFI call; fan-out happens in Rust.
io.toRoom('lobby').except(someId).send('hello room');
io.toUser('user-123').send('hi, all your devices');
io.broadcast('hello everyone');

const members = await io.presence('lobby').list(); // [{ id, userId, metadata }]
const m = io.metrics();                            // { connections, users, bytesIn, bridgePressure, … }

await io.listen(8080);
process.on('SIGTERM', () => io.close({ timeoutMs: 30_000 })); // drain, then exit
```

| Doc | Purpose |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design and the rules that govern it |
| [docs/rfcs/0001-event-bridge.md](docs/rfcs/0001-event-bridge.md) | The frozen RFC gating all runtime work |
| [docs/ENGINEERING.md](docs/ENGINEERING.md) | What to build, in what order, and how to know you're done |
| [CHANGELOG.md](CHANGELOG.md) | Release notes |

## Layout

- `crates/core` — Rust engine (no NAPI, ever)
- `crates/node` — NAPI-RS binding
- `packages/beamsocket` — the npm package (TypeScript SDK)
- `benchmarks/` — honest comparisons vs ws / Socket.IO / uWebSockets.js
- `spike/` — RFC 0001 bridge spike (throwaway; the winner graduated into `crates/node`)
