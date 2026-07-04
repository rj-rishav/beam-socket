// Phase 1C integration (docs/ENGINEERING.md §7): identity + admission limits
// through the whole stack with stock `ws` clients.
//  - authorize() binds userId/metadata; toUser() fans out in Rust
//  - authorize reject / thrown handler close with the documented codes
//  - maxConnectionsPerIp rejects at the upgrade (429), direct AND behind a
//    trusted proxy (Rule 3)
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function nextMessage(ws, label) {
  return withTimeout(once(ws, 'message'), 5000, label);
}

async function connectAs(io, port, headers) {
  const connP = once(io, 'connection');
  const ws = new WebSocket(`ws://127.0.0.1:${port}/`, { headers });
  await withTimeout(once(ws, 'open'), 5000, 'client open');
  const [socket] = await withTimeout(connP, 5000, 'server connection');
  return { ws, socket };
}

/** Resolve how a handshake settled: 'open' | <httpStatus> | 'error'. */
function settleWs(ws) {
  return new Promise((resolve) => {
    ws.once('open', () => resolve('open'));
    ws.once('unexpected-response', (_req, res) => resolve(res.statusCode));
    ws.once('error', () => resolve('error'));
  });
}

test('authorize accept binds userId/metadata; toUser fans out to every device', async () => {
  const io = new BeamSocket({});
  io.authorize((req) => {
    const user = req.headers['x-user'];
    return user
      ? { accept: true, userId: String(user), metadata: { plan: 'pro', ip: req.ip } }
      : { accept: false, code: 4401 };
  });
  io.on('connection', () => {});
  const port = await io.listen(0);

  const a1 = await connectAs(io, port, { 'x-user': 'alice' });
  const a2 = await connectAs(io, port, { 'x-user': 'alice' });
  const b1 = await connectAs(io, port, { 'x-user': 'bob' });

  // socket.userId / socket.metadata come from the authorize hook.
  assert.equal(a1.socket.userId, 'alice');
  assert.equal(a1.socket.metadata.plan, 'pro');
  assert.equal(typeof a1.socket.metadata.ip, 'string');
  assert.equal(b1.socket.userId, 'bob');

  // toUser('alice') reaches both alice devices, not bob.
  const p1 = nextMessage(a1.ws, 'a1');
  const p2 = nextMessage(a2.ws, 'a2');
  let bobGot = false;
  b1.ws.once('message', () => (bobGot = true));
  io.toUser('alice').send('hey alice');
  assert.equal((await p1)[0].toString(), 'hey alice');
  assert.equal((await p2)[0].toString(), 'hey alice');
  await sleep(150);
  assert.equal(bobGot, false, 'bob must not receive alice traffic');

  // Disconnect one alice device → toUser still reaches the other.
  a1.ws.close(1000);
  await withTimeout(once(a1.ws, 'close'), 5000, 'a1 close');
  await sleep(100);
  const p3 = nextMessage(a2.ws, 'a2 again');
  io.toUser('alice').send('still here');
  assert.equal((await p3)[0].toString(), 'still here');

  a2.ws.close(1000);
  b1.ws.close(1000);
  await io.close();
});

test('authorize reject closes with the app-supplied code', async () => {
  const io = new BeamSocket({});
  io.authorize((req) => (req.headers['x-user'] ? { accept: true } : { accept: false, code: 4401 }));
  io.on('connection', () => {});
  const port = await io.listen(0);

  const ws = new WebSocket(`ws://127.0.0.1:${port}/`); // no x-user → rejected
  const [code] = await withTimeout(once(ws, 'close'), 5000, 'close');
  assert.equal(code, 4401);
  await io.close();
});

test('a throwing authorize handler rejects (1011), never hangs', async () => {
  const io = new BeamSocket({});
  io.authorize(() => {
    throw new Error('boom');
  });
  io.on('connection', () => {});
  const port = await io.listen(0);

  const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
  const [code] = await withTimeout(once(ws, 'close'), 5000, 'close');
  assert.equal(code, 1011);
  await io.close();
});

test('maxConnectionsPerIp rejects the N+1th at the upgrade (direct topology)', async () => {
  const io = new BeamSocket({ limits: { maxConnectionsPerIp: 2 } });
  io.on('connection', () => {});
  const port = await io.listen(0);

  const a = new WebSocket(`ws://127.0.0.1:${port}/`);
  await withTimeout(once(a, 'open'), 5000, 'a open');
  const b = new WebSocket(`ws://127.0.0.1:${port}/`);
  await withTimeout(once(b, 'open'), 5000, 'b open');

  // 3rd from the same peer → HTTP 429, no WebSocket.
  const status = await withTimeout(settleWs(new WebSocket(`ws://127.0.0.1:${port}/`)), 5000, 'c');
  assert.ok(status === 429 || status === 'error', `expected rejection, got ${status}`);

  a.close(1000);
  b.close(1000);
  await io.close();
});

test('trustProxy keys the per-IP limit on the forwarded client (Rule 3)', async () => {
  const io = new BeamSocket({
    trustProxy: ['127.0.0.0/8'],
    limits: { maxConnectionsPerIp: 1 },
  });
  io.on('connection', () => {});
  const port = await io.listen(0);

  // Loopback is a trusted proxy → the per-IP limit keys on X-Forwarded-For.
  const a = new WebSocket(`ws://127.0.0.1:${port}/`, { headers: { 'x-forwarded-for': '9.9.9.9' } });
  assert.equal(await withTimeout(settleWs(a), 5000, 'a'), 'open');

  // Same forwarded client → over the cap of 1.
  const b = new WebSocket(`ws://127.0.0.1:${port}/`, { headers: { 'x-forwarded-for': '9.9.9.9' } });
  const bStatus = await withTimeout(settleWs(b), 5000, 'b');
  assert.ok(bStatus === 429 || bStatus === 'error', `expected rejection, got ${bStatus}`);

  // A different forwarded client has its own budget.
  const c = new WebSocket(`ws://127.0.0.1:${port}/`, { headers: { 'x-forwarded-for': '8.8.8.8' } });
  assert.equal(await withTimeout(settleWs(c), 5000, 'c'), 'open');

  a.close(1000);
  c.close(1000);
  await io.close();
});
