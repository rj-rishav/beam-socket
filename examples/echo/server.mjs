// Phase 1A echo server — the whole point of the phase in ~15 lines.
// Also the target for the Autobahn protocol suite in CI (.github/workflows).
//
//   node examples/echo/server.mjs            # listens on 9001
//   PORT=8080 node examples/echo/server.mjs
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

const io = new BeamSocket({
  // Autobahn's limit cases (9.x) send multi-MB messages; allow them when
  // asked. Default stays at the production 1 MB.
  limits: process.env.BEAMSOCKET_MAX_PAYLOAD
    ? { maxPayloadBytes: Number(process.env.BEAMSOCKET_MAX_PAYLOAD) }
    : undefined,
});

io.on('connection', (socket) => {
  socket.on('message', (data, isBinary) => {
    socket.send(isBinary ? data : data.toString('utf8'));
  });
});

const port = await io.listen(Number(process.env.PORT ?? 9001));
console.log(`beamsocket echo listening on :${port}`);
