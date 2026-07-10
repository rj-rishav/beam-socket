// Phase 2B admin actions (ENGINEERING.md §12.2) through the whole stack with
// stock `ws` clients: the three verbs, their counts, the close codes landing
// on real clients, drain-time no-ops, and code validation.
import { test } from 'node:test';
import assert from 'node:assert';
import { once } from 'node:events';
import WebSocket from 'ws';

import { BeamSocket, AdminCloseCode } from '../dist/index.js';

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms).unref(),
    ),
  ]);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function awaitTrue(label, fn) {
  const deadline = Date.now() + 5000;
  while (!fn()) {
    assert.ok(Date.now() < deadline, `timed out waiting for: ${label}`);
    await sleep(20);
  }
}

/** Server with an authorize hook binding userId from the x-user header. */
async function startServer() {
  const io = new BeamSocket({}).authorize((req) => ({
    accept: true,
    userId: req.headers['x-user'],
  }));
  const port = await io.listen(0);
  return { io, port };
}

async function connectAs(io, port, user) {
  const connP = once(io, 'connection');
  const ws = new WebSocket(`ws://127.0.0.1:${port}/`, {
    headers: user ? { 'x-user': user } : {},
  });
  await withTimeout(once(ws, 'open'), 5000, 'client open');
  const [socket] = await withTimeout(connP, 5000, 'server connection');
  return { ws, socket };
}

/** The ws client's close event → its code. */
function closeCode(ws) {
  return withTimeout(
    new Promise((resolve) => ws.once('close', (code) => resolve(code))),
    5000,
    'client close',
  );
}

test('disconnectUser: 3 devices all closed with the code, identity entry gone, toUser reaches 0', async () => {
  const { io, port } = await startServer();
  const a = await connectAs(io, port, 'alice');
  const b = await connectAs(io, port, 'alice');
  const c = await connectAs(io, port, 'alice');
  assert.equal(io.user('alice').connections().length, 3);

  const codes = Promise.all([closeCode(a.ws), closeCode(b.ws), closeCode(c.ws)]);
  assert.deepEqual(io.disconnectUser('alice', 4005), { closed: 3 });

  // §12.2: the code lands on EVERY device's client.
  assert.deepEqual(await codes, [4005, 4005, 4005]);

  // Identity entry gone (the 1C auto-destroy invariant) → toUser reaches 0.
  await awaitTrue('identity gone', () => io.user('alice').connections().length === 0);
  await awaitTrue('users gauge 0', () => io.stats().users === 0);
  io.toUser('alice').send('anyone?'); // must reach nobody, and not throw
  assert.equal(io.stats().adminDisconnects, 3, 'one count per device closed');

  // Gone user → { closed: 0 }, no error.
  assert.deepEqual(io.disconnectUser('alice', 4005), { closed: 0 });
  await io.close();
});

test('disconnectSocket: default 1000 and 4000-range codes land; stale id → 0', async () => {
  const { io, port } = await startServer();

  const a = await connectAs(io, port, 'amy');
  const aCode = closeCode(a.ws);
  assert.deepEqual(io.disconnectSocket(a.socket.id), { closed: 1 });
  assert.equal(await aCode, AdminCloseCode.NORMAL, 'default code is 1000');

  const b = await connectAs(io, port, 'bob');
  b.socket.join('ops');
  const bCode = closeCode(b.ws);
  assert.deepEqual(io.disconnectSocket(b.socket.id, 4001), { closed: 1 });
  assert.equal(await bCode, 4001);

  // Full 1C/1D cleanup via the existing path: registry, identity, rooms.
  await awaitTrue('all cleaned up', () => {
    const s = io.stats();
    return s.connections === 0 && s.users === 0 && s.rooms === 0;
  });

  // The now-stale id and a foreign id both report 0, never an error.
  assert.deepEqual(io.disconnectSocket(b.socket.id), { closed: 0 });
  assert.deepEqual(io.disconnectSocket('not-a-real-id'), { closed: 0 });
  assert.equal(io.stats().adminDisconnects, 2);
  await io.close();
});

test('closeRoom: members stay alive, room gone, second room untouched', async () => {
  const { io, port } = await startServer();
  const a = await connectAs(io, port, 'u1');
  const b = await connectAs(io, port, 'u2');
  a.socket.join('lobby');
  b.socket.join('lobby');
  a.socket.join('other');
  assert.equal(io.room('lobby').info().members, 2);

  assert.deepEqual(io.closeRoom('lobby'), { removed: 2 });

  // Room gone, immediately (the sweep is synchronous)…
  assert.equal(io.room('lobby').info().exists, false);
  assert.equal(io.room('other').info().members, 1, 'other room untouched');

  // …but the members' CONNECTIONS are alive (disconnect-free): both still
  // receive, and neither client saw a close.
  const aMsg = withTimeout(once(a.ws, 'message'), 5000, 'a receives');
  const bMsg = withTimeout(once(b.ws, 'message'), 5000, 'b receives');
  io.broadcast('still here');
  assert.equal((await aMsg)[0].toString(), 'still here');
  assert.equal((await bMsg)[0].toString(), 'still here');
  assert.equal(io.connectionCount(), 2);
  assert.equal(io.stats().adminRoomCloses, 1);

  // Gone/never-existed room → { removed: 0 }, no error, not counted again.
  assert.deepEqual(io.closeRoom('lobby'), { removed: 0 });
  assert.deepEqual(io.closeRoom('never-existed'), { removed: 0 });
  assert.equal(io.stats().adminRoomCloses, 1);

  a.ws.close();
  b.ws.close();
  await io.close();
});

test('code validation: only 1000 and 4000-4999 pass; anything else throws RangeError before any FFI', async () => {
  const { io, port } = await startServer();
  const { ws, socket } = await connectAs(io, port, 'carl');

  for (const bad of [999, 1001, 1006, 3999, 5000, 4000.5, NaN]) {
    assert.throws(() => io.disconnectSocket(socket.id, bad), RangeError, `code ${bad}`);
    assert.throws(() => io.disconnectUser('carl', bad), RangeError, `code ${bad}`);
    assert.throws(() => io.closeRoom('lobby', bad), RangeError, `code ${bad}`);
  }
  // The throws happened before any close was initiated.
  assert.equal(io.connectionCount(), 1);
  assert.equal(io.stats().adminDisconnects, 0);

  // Boundary values pass validation.
  assert.deepEqual(io.disconnectSocket(socket.id, 4999), { closed: 1 });
  ws.close();
  await io.close();
});

test('verbs during close() drain are safe no-ops: counts 0, no throw', async () => {
  const { io, port } = await startServer();
  const { socket } = await connectAs(io, port, 'dora');
  socket.join('lobby');

  const closing = io.close(); // drain begins; do NOT await yet
  assert.deepEqual(io.disconnectSocket(socket.id), { closed: 0 });
  assert.deepEqual(io.disconnectUser('dora', 4001), { closed: 0 });
  assert.deepEqual(io.closeRoom('lobby'), { removed: 0 });
  await closing;

  // Still no-ops after the drain completes.
  assert.deepEqual(io.disconnectSocket(socket.id), { closed: 0 });
  assert.deepEqual(io.disconnectUser('dora'), { closed: 0 });
  assert.deepEqual(io.closeRoom('lobby'), { removed: 0 });
});

test('metricsText exposes the admin counters', async () => {
  const { io, port } = await startServer();
  const { socket } = await connectAs(io, port, 'eve');
  socket.join('lobby');
  io.closeRoom('lobby');
  io.disconnectSocket(socket.id);
  await awaitTrue('drained', () => io.connectionCount() === 0);

  const text = io.metricsText();
  assert.match(text, /^beamsocket_admin_disconnects_total 1$/m);
  assert.match(text, /^beamsocket_admin_room_closes_total 1$/m);
  await io.close();
});
