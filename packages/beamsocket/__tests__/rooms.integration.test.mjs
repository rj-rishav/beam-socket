// Phase 1B integration (docs/ENGINEERING.md §6): rooms + broadcast through
// the whole stack with stock `ws` clients. Fan-out never enters JS — the SDK
// makes ONE native call per targeting verb.
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

async function connect(io, port) {
  const connP = once(io, 'connection');
  const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
  await withTimeout(once(ws, 'open'), 5000, 'client open');
  const [socket] = await withTimeout(connP, 5000, 'server connection');
  return { ws, socket };
}

function nextMessage(ws, label) {
  return withTimeout(once(ws, 'message'), 5000, label);
}

test('rooms: join/leave, toRoom().except(), toSocket, io.broadcast', async () => {
  const io = new BeamSocket({});
  io.on('connection', () => {});
  const port = await io.listen(0);

  const a = await connect(io, port);
  const b = await connect(io, port);
  const c = await connect(io, port);

  // a, b join; c stays out.
  a.socket.join('lobby');
  b.socket.join('lobby');

  // Room broadcast: a and b get it, c must not. (Waiters registered BEFORE
  // sending — frames can arrive before a later once() would attach.)
  let aP = nextMessage(a.ws, 'a room text');
  let bP = nextMessage(b.ws, 'b room text');
  io.toRoom('lobby').send('room text');
  const [aMsg, aBin] = await aP;
  const [bMsg] = await bP;
  assert.equal(aBin, false, 'text frame stays text');
  assert.equal(aMsg.toString(), 'room text');
  assert.equal(bMsg.toString(), 'room text');

  // except(): only b receives.
  bP = nextMessage(b.ws, 'b except blob');
  io.toRoom('lobby').except(a.socket.id).send(Buffer.from([9, 9, 9]));
  const [bBlob, bIsBin] = await bP;
  assert.equal(bIsBin, true);
  assert.deepEqual(Buffer.from(bBlob), Buffer.from([9, 9, 9]));

  // toSocket(): exactly one recipient.
  const cP = nextMessage(c.ws, 'c direct');
  io.toSocket(c.socket.id).send('just for c');
  const [cMsg] = await cP;
  assert.equal(cMsg.toString(), 'just for c');

  // io.broadcast(): everyone, including the non-member.
  const all = [a, b, c].map(({ ws }) => nextMessage(ws, 'broadcast'));
  io.broadcast('to everyone');
  for (const p of all) {
    const [m] = await p;
    assert.equal(m.toString(), 'to everyone');
  }

  // c must never have received room traffic: its next message would have
  // been the direct one already consumed; queue must now be silent. Verify
  // leave() stops delivery for a too.
  a.socket.leave('lobby');
  bP = nextMessage(b.ws, 'b after leave');
  io.toRoom('lobby').send('after leave');
  const [bAfter] = await bP;
  assert.equal(bAfter.toString(), 'after leave');
  let aGotExtra = false;
  a.ws.once('message', () => (aGotExtra = true));
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(aGotExtra, false, 'a received room traffic after leave()');

  // Disconnect cleanup: b leaves by closing; the room auto-destroys, and a
  // fresh broadcast to it reaches nobody (no crash, no ghosts).
  b.ws.close(1000);
  await withTimeout(once(b.ws, 'close'), 5000, 'b close');
  await new Promise((r) => setTimeout(r, 100)); // server-side cleanup settles
  io.toRoom('lobby').send('into the void');
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(aGotExtra, false);

  a.ws.close(1000);
  c.ws.close(1000);
  await io.close();
});

test('large chained exclusions reach only intended room/user members, exactly once', async (t) => {
  const io = new BeamSocket({});
  io.authorize(() => ({ accept: true, userId: 'exclusion-user' }));
  const clients = [];
  t.after(async () => {
    for (const { ws } of clients) ws.terminate();
    await io.close({ timeoutMs: 2000 });
  });
  const port = await io.listen(0);
  // Both R >= 64 and E >= 32: exercise the indexed path through the real FFI.
  for (let i = 0; i < 80; i++) {
    const client = await connect(io, port);
    client.socket.join('indexed');
    clients.push(client);
  }
  for (const kind of ['room', 'user']) {
    const marker = `barrier-${kind}`;
    const seen = clients.map(() => []);
    const listeners = [];
    const barriers = clients.map(({ ws }, i) => withTimeout(new Promise((resolve) => {
      const listener = (data, binary) => {
        if (!binary && data.toString() === marker) resolve();
        else seen[i].push([Buffer.from(data), binary]);
      };
      listeners.push(listener);
      ws.on('message', listener);
    }), 5000, `${kind} delivery barrier ${i}`));
    const target = kind === 'room' ? io.toRoom('indexed') : io.toUser('exclusion-user');
    for (let i = 0; i < 40; i++) target.except(clients[i].socket.id);
    target.except(clients[0].socket.id); // duplicate exclusions do not change delivery
    const payload = kind === 'room' ? Buffer.from([0, 42, 255]) : 'indexed text';
    target.send(payload);
    // Per-socket FIFO markers prove all earlier frames were observed without
    // a sleep-based negative assertion or counting duplicates as recipients.
    for (const { socket } of clients) io.toSocket(socket.id).send(marker);
    await Promise.all(barriers);
    for (let i = 0; i < clients.length; i++) {
      clients[i].ws.off('message', listeners[i]);
      assert.deepEqual(seen[i], i < 40 ? [] : [[Buffer.from(payload), kind === 'room']],
        `${kind} recipient ${i}`);
    }
  }
});
