// Phase 1.1 — attach BeamSocket to an existing Express server (RFC 0002).
// Express HTTP routes AND BeamSocket WebSockets at /ws share ONE port, one
// deployment, one TLS setup (terminate TLS at your load balancer — attach is
// plaintext, RFC 0002 §6).
//
//   cd examples/express-attach && npm install
//   npm --prefix ../../packages/beamsocket run build   # build the SDK dist
//   node server.mjs        # http://localhost:3000  +  ws://localhost:3000/ws
import express from 'express';
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

const app = express();
app.get('/', (_req, res) => res.send('Express + BeamSocket on one port. WebSockets at /ws'));
app.get('/health', (_req, res) => res.json({ ok: true }));

// Express owns the port. Do NOT call io.listen() in attached mode.
const httpServer = app.listen(Number(process.env.PORT ?? 3000), () => {
  console.log(`listening on :${httpServer.address().port} — HTTP routes + WS at /ws`);
});

const io = new BeamSocket({
  server: httpServer,
  path: '/ws', // claim only /ws; other upgrade listeners coexist
  // Behind a load balancer, honor its X-Forwarded-For (and terminate TLS there):
  // trustProxy: ['10.0.0.0/8'],
});

// Connection-time auth — one JS round-trip per connection, never per message.
io.authorize((req) => {
  const user = req.headers['x-user'];
  return user ? { accept: true, userId: String(user) } : { accept: true };
});

io.on('connection', (socket) => {
  socket.join('lobby');
  socket.on('message', (data, isBinary) => socket.send(isBinary ? data : data.toString())); // echo
});

// Graceful shutdown (RFC 0002 §10.4): drain the Rust-owned WebSockets FIRST
// (1001 going away), then stop the HTTP server. httpServer.close() alone does
// NOT drain the upgraded WebSockets — they belong to the engine now.
process.on('SIGTERM', async () => {
  await io.close({ timeoutMs: 30_000 });
  httpServer.close();
});
