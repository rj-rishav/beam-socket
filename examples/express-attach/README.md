# express-attach — BeamSocket on your Express server (Phase 1.1)

One port for Express HTTP routes **and** BeamSocket WebSockets — the RFC 0002
adoption story. BeamSocket attaches to the Express `http.Server`'s upgrade path
(`/ws`) via fd handoff; the WebSocket data plane stays entirely in Rust.

```bash
cd examples/express-attach
npm install
npm --prefix ../../packages/beamsocket run build:native   # build the Rust addon
npm --prefix ../../packages/beamsocket run build           # build the SDK dist
node server.mjs
```

Then:

```bash
curl localhost:3000/health           # Express route → {"ok":true}
# WebSocket (any client) → ws://localhost:3000/ws  (echoes; join 'lobby')
```

Notes:

- **One port, one TLS setup.** Terminate TLS at your load balancer and attach to
  a plaintext `http.Server` — attach refuses an `https.Server` (RFC 0002 §6).
- **Coexistence.** `path: '/ws'` claims only `/ws`; other upgrade listeners
  (e.g. a second library) still get their paths.
- **Graceful shutdown.** On `SIGTERM` the app drains WebSockets with
  `io.close()` (1001), *then* `httpServer.close()` — in that order, because
  `httpServer.close()` does not drain the engine-owned WebSockets.
- **Windows / TLS attach** are not supported in 1.1 (`{ server }` throws with a
  pointer to the standalone-port fallback / TLS-at-LB — RFC 0002 §6/§7).
