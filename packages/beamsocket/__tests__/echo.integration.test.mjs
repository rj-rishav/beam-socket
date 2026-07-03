// Phase 1A integration test (docs/ENGINEERING.md §5): a stock `ws` client
// echoes text + binary through the real stack (TS SDK → NAPI → engine →
// Tokio → socket and back), and the close handshake is clean in BOTH
// directions. Requires the built addon (npm run build:native) and dist/
// (npm run build).

import { test } from 'node:test';
import assert from 'node:assert';
import { once } from 'node:events';
import WebSocket from 'ws';

import { BeamSocket } from '../dist/index.js';

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms).unref(),
    ),
  ]);
}

test('echo + clean close, both directions, via stock ws client', async () => {
  const io = new BeamSocket({});
  const serverEvents = { opens: 0, closes: [] };
  io.on('connection', (socket) => {
    serverEvents.opens += 1;
    socket.on('message', (data, isBinary) => {
      // Echo: Buffer view (zero-copy subarray) goes straight back. Text
      // frames must be echoed as text.
      socket.send(isBinary ? data : data.toString('utf8'));
    });
    socket.on('close', (code, reason) => serverEvents.closes.push({ code, reason }));
  });
  const port = await io.listen(0);
  assert.ok(port > 0, 'ephemeral port bound');

  // --- client-initiated close direction ---
  const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
  await withTimeout(once(ws, 'open'), 5000, 'client open');

  // Text echo.
  ws.send('hello beamsocket');
  const [textMsg, textIsBinary] = await withTimeout(once(ws, 'message'), 5000, 'text echo');
  assert.equal(textIsBinary, false, 'text frame echoed as text');
  assert.equal(textMsg.toString('utf8'), 'hello beamsocket');

  // Binary echo (includes NUL and high bytes).
  const blob = Buffer.from([0, 1, 2, 250, 255, 42]);
  ws.send(blob);
  const [binMsg, binIsBinary] = await withTimeout(once(ws, 'message'), 5000, 'binary echo');
  assert.equal(binIsBinary, true, 'binary frame echoed as binary');
  assert.deepEqual(Buffer.from(binMsg), blob);

  // Client initiates close; server must complete the handshake.
  ws.close(1000, 'client done');
  const [clientCloseCode] = await withTimeout(once(ws, 'close'), 5000, 'client close');
  assert.equal(clientCloseCode, 1000);

  // --- server-initiated close direction ---
  const conn2Promise = once(io, 'connection'); // registered BEFORE connecting
  const ws2 = new WebSocket(`ws://127.0.0.1:${port}/`);
  await withTimeout(once(ws2, 'open'), 5000, 'client2 open');
  const [socket2] = await withTimeout(conn2Promise, 5000, 'server connection 2');
  assert.equal(serverEvents.opens, 2, 'server observed both connections');
  socket2.close(4001, 'server says bye');

  const [code2, reasonBuf] = await withTimeout(once(ws2, 'close'), 5000, 'server-initiated close');
  assert.equal(code2, 4001);
  assert.equal(reasonBuf.toString('utf8'), 'server says bye');

  // Server-side close events observed for both connections with sane codes.
  await withTimeout(
    (async () => {
      while (serverEvents.closes.length < 2) {
        await new Promise((r) => setTimeout(r, 20));
      }
    })(),
    5000,
    'server-side close events',
  );
  const codes = serverEvents.closes.map((c) => c.code).sort();
  assert.deepEqual(codes, [1000, 4001], `close codes: ${JSON.stringify(serverEvents.closes)}`);

  await io.close();
});
