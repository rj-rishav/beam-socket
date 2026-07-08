# BeamSocket

**The high-performance networking runtime for Node.js.**

Rust data plane, JavaScript control plane. Maximum connections, minimum overhead.

**Status:** `0.1.0-alpha.0` — Phase 1D complete (presence, metrics, graceful
close) + Phase 1.1 HTTP-server attach (RFC 0002). Connections, rooms, users, and
admission control all run in Rust; the whole per-message data plane stays off
the JS event loop. Phase 0 gate met: RFC 0001
[results](docs/rfcs/0001-results.md) — Design C graduated. Alpha caveats:
single-process (no clustering yet), and the headline benchmark + constants are
still pending their pinned-box confirmation (see
[benchmarks](benchmarks/README.md)).

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
