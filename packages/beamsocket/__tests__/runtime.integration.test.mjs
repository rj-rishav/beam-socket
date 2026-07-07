// Phase 1D integration (docs/ENGINEERING.md §8): presence, metrics, and
// graceful close through the whole stack with stock `ws` clients — plus the
// clean-process-exit proof (the TSFN-release trap).
import { test } from 'node:test';
import assert from 'node:assert';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';

import { BeamSocket } from '../dist/index.js';

const pkgDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms).unref(),
    ),
  ]);
}

async function connect(io, port, headers) {
  const connP = once(io, 'connection');
  const ws = new WebSocket(`ws://127.0.0.1:${port}/`, { headers });
  await withTimeout(once(ws, 'open'), 5000, 'client open');
  const [socket] = await withTimeout(connP, 5000, 'server connection');
  return { ws, socket };
}

function settleWs(ws) {
  return new Promise((resolve) => {
    ws.once('open', () => resolve('open'));
    ws.once('unexpected-response', (_req, res) => resolve(res.statusCode));
    ws.once('error', () => resolve('error'));
  });
}

test('presence(room).list() → {id, userId, metadata}, joined SDK-side', async () => {
  const io = new BeamSocket({});
  io.authorize((req) => {
    const u = req.headers['x-user'];
    return u ? { accept: true, userId: String(u), metadata: { plan: 'pro' } } : { accept: true };
  });
  io.on('connection', (socket) => socket.join('lobby'));
  const port = await io.listen(0);

  const a = await connect(io, port, { 'x-user': 'alice' });
  const b = await connect(io, port, {}); // anonymous — opens with {} metadata
  await sleep(80); // let the joins settle

  const list = await io.presence('lobby').list();
  assert.equal(list.length, 2);

  const alice = list.find((e) => e.userId === 'alice');
  assert.ok(alice, 'alice present');
  assert.deepEqual(alice.metadata, { plan: 'pro' });
  assert.equal(typeof alice.id, 'string');

  const anon = list.find((e) => e.userId === undefined);
  assert.ok(anon, 'anonymous member present');
  assert.deepEqual(anon.metadata, {}, 'no-metadata member joins as {}');

  // A room nobody is in is empty, never an error.
  assert.deepEqual(await io.presence('void').list(), []);

  a.ws.close(1000);
  b.ws.close(1000);
  await io.close();
});

test('metrics() moves under the workload that should move each field', async () => {
  const io = new BeamSocket({});
  io.authorize((req) => ({ accept: true, userId: String(req.headers['x-user'] ?? 'anon') }));
  io.on('connection', (socket) => {
    socket.join('room');
    socket.on('message', (data) => socket.send(data));
  });
  const port = await io.listen(0);

  assert.deepEqual(
    [io.metrics().connections, io.metrics().rooms, io.metrics().users],
    [0, 0, 0],
  );

  const { ws } = await connect(io, port, { 'x-user': 'u1' });
  await sleep(60);
  let m = io.metrics();
  assert.equal(m.connections, 1, 'connections moved');
  assert.equal(m.users, 1, 'users moved');
  assert.equal(m.rooms, 1, 'rooms moved');

  // Echo a frame → messagesIn/Out + bytesIn/Out move.
  const got = once(ws, 'message');
  ws.send('hello');
  await withTimeout(got, 5000, 'echo');
  await sleep(60);
  m = io.metrics();
  assert.ok(m.messagesIn >= 1, 'messagesIn moved');
  assert.ok(m.messagesOut >= 1, 'messagesOut moved');
  assert.ok(m.bytesIn >= 5, 'bytesIn moved');
  assert.ok(m.bytesOut >= 5, 'bytesOut moved');

  // Every documented field exists and is a number (no undocumented counters).
  for (const k of [
    'connections', 'users', 'rooms', 'messagesIn', 'messagesOut', 'bytesIn', 'bytesOut',
    'backpressureDrops', 'bridgePressure', 'bridgeDropped', 'admissionRejectedIp',
    'authorizeRejected', 'authorizeTimedOut', 'pendingOverflow', 'authMetadataEvicted',
  ]) {
    assert.equal(typeof m[k], 'number', `metrics().${k} present`);
  }

  ws.close(1000);
  await io.close();
});

test('metrics().admissionRejectedIp moves when a per-IP reject fires', async () => {
  const io = new BeamSocket({ limits: { maxConnectionsPerIp: 1 } });
  io.on('connection', () => {});
  const port = await io.listen(0);
  const a = new WebSocket(`ws://127.0.0.1:${port}/`);
  await withTimeout(once(a, 'open'), 5000, 'a open');
  // 2nd from the same IP → 429; the counter must move.
  await withTimeout(settleWs(new WebSocket(`ws://127.0.0.1:${port}/`)), 5000, 'b');
  await sleep(40);
  assert.ok(io.metrics().admissionRejectedIp >= 1, 'admissionRejectedIp moved');
  a.close(1000);
  await io.close();
});

test('close(): new upgrades get 503 during the drain', async () => {
  const io = new BeamSocket({});
  io.on('connection', (socket) => socket.on('message', (d) => socket.send(d)));
  const port = await io.listen(0);
  const { ws } = await connect(io, port, {});

  // Begin a graceful close with a comfortable window, then probe mid-drain.
  const closing = io.close({ timeoutMs: 3000 });
  await sleep(50); // draining flag is set
  const status = await withTimeout(
    settleWs(new WebSocket(`ws://127.0.0.1:${port}/`)),
    5000,
    'drain probe',
  );
  assert.ok(status === 503 || status === 'error', `expected 503 during drain, got ${status}`);

  // The existing socket is closed by the drain; close() resolves.
  await withTimeout(closing, 5000, 'close resolves');
  assert.equal(ws.readyState, WebSocket.CLOSED, 'draining socket was closed');
});

test('a server that opens then closes exits the process by itself (TSFN released)', async () => {
  // The definitive clean-exit proof: a child that never calls process.exit().
  // If close() failed to release the ThreadsafeFunction, the event loop would
  // stay referenced and the child would hang until this timeout.
  const script = `
    import { BeamSocket } from './dist/index.js';
    import WebSocket from 'ws';
    const io = new BeamSocket({});
    io.on('connection', (s) => s.on('message', (d) => s.send(d)));
    const port = await io.listen(0);
    const ws = new WebSocket('ws://127.0.0.1:' + port + '/');
    await new Promise((r) => ws.on('open', r));
    const echoed = new Promise((r) => ws.on('message', r));
    ws.send('hi');
    await echoed;
    await io.close({ timeoutMs: 2000 });
    // Intentionally NO process.exit(): a clean release lets Node exit on its own.
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script], {
    cwd: pkgDir,
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  const [code] = await withTimeout(once(child, 'exit'), 10000, 'child clean exit');
  assert.equal(code, 0, 'process must exit cleanly on its own after close()');
});
