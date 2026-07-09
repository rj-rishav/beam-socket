// Phase 2A observability integration (ENGINEERING.md §12.1) — the read surface
// through the whole stack, plus the perf-regression guard (the zero-hot-path
// proof).
import { test } from 'node:test';
import assert from 'node:assert';
import { once } from 'node:events';
import WebSocket from 'ws';

import { BeamSocket } from '../dist/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function withTimeout(p, ms, label) {
  return Promise.race([
    p,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout: ${label}`)), ms).unref()),
  ]);
}
const echoConn = (s) => s.on('message', (d, isBin) => s.send(isBin ? d : d.toString('utf8')));

async function connect(io, port, headers) {
  const connP = once(io, 'connection');
  const ws = new WebSocket(`ws://127.0.0.1:${port}/`, { headers });
  await withTimeout(once(ws, 'open'), 5000, 'open');
  const [socket] = await withTimeout(connP, 5000, 'server connection');
  return { ws, socket };
}

test('stats(): rates rise under echo load and decay toward zero after it stops', async () => {
  const io = new BeamSocket({ observability: { samplerMs: 100 } });
  io.on('connection', echoConn);
  const port = await io.listen(0);
  const { ws } = await connect(io, port, {});

  // Drive echo load for ~1.2 s.
  let stop = false;
  const pump = (async () => {
    while (!stop) {
      ws.send('x');
      await once(ws, 'message');
    }
  })();
  await sleep(1200);
  const loaded = io.stats().rates;
  assert.ok(loaded, 'rates present while sampler on');
  assert.ok(loaded.messagesIn.perSec1s > 3, `1s in-rate rose, got ${loaded.messagesIn.perSec1s}`);
  assert.ok(loaded.messagesOut.perSec1s > 3, `1s out-rate rose`);

  stop = true;
  await pump;
  const peak = loaded.messagesIn.perSec1s;
  await sleep(3200); // ~3 one-second windows
  const decayed = io.stats().rates.messagesIn.perSec1s;
  assert.ok(decayed < peak * 0.3, `1s rate decayed (${decayed} vs peak ${peak})`);

  ws.close();
  await io.close();
});

test('sampler off (samplerMs: 0): stats().rates is null, nothing crashes', async () => {
  const io = new BeamSocket({ observability: { samplerMs: 0 } });
  io.on('connection', echoConn);
  const port = await io.listen(0);
  const { ws } = await connect(io, port, {});
  ws.send('x');
  await once(ws, 'message');
  const s = io.stats();
  assert.strictEqual(s.rates, null, 'rates absent when sampler disabled');
  assert.ok(s.uptimeMs >= 0 && typeof s.connections === 'number');
  ws.close();
  await io.close();
});

test('topRooms + room().info() + connectionCount', async () => {
  const io = new BeamSocket({});
  io.on('connection', (s) => {
    s.on('message', (m) => {
      s.join(m.toString());
    });
  });
  const port = await io.listen(0);
  // 3 clients: 2 in "big", 1 in "small".
  const a = await connect(io, port, {});
  const b = await connect(io, port, {});
  const c = await connect(io, port, {});
  a.ws.send('big');
  b.ws.send('big');
  c.ws.send('small');
  await sleep(120);
  io.toRoom('big').send('hello'); // bump big's message counter
  await sleep(60);

  assert.equal(io.connectionCount(), 3);
  const top = io.topRooms(10);
  assert.equal(top[0].room, 'big');
  assert.equal(top[0].members, 2);
  assert.ok(top[0].messages >= 1, 'big room message counter moved');

  const info = io.room('big').info();
  assert.equal(info.exists, true);
  assert.equal(info.members, 2);
  const gone = io.room('does-not-exist').info();
  assert.equal(gone.exists, false);
  assert.equal(gone.members, 0);

  for (const x of [a, b, c]) x.ws.close();
  await io.close();
});

test('topRooms cap: 1e9 clamps to <=100; 0 and negative throw', async () => {
  const io = new BeamSocket({});
  io.on('connection', echoConn);
  const port = await io.listen(0);
  assert.ok(io.topRooms(1e9).length <= 100, 'clamped to the hard cap');
  assert.throws(() => io.topRooms(0), /positive/);
  assert.throws(() => io.topRooms(-5), /positive/);
  assert.throws(() => io.backpressureReport({ topN: 0 }), /positive/);
  await io.close();
});

test('user(id).connections() agrees with multi-device identity', async () => {
  const io = new BeamSocket({});
  io.authorize((req) => ({ accept: true, userId: String(req.headers['x-user']) }));
  io.on('connection', () => {});
  const port = await io.listen(0);
  const a1 = await connect(io, port, { 'x-user': 'alice' });
  const a2 = await connect(io, port, { 'x-user': 'alice' });
  await sleep(80);

  const conns = io.user('alice').connections();
  assert.equal(conns.length, 2, 'both alice devices listed');
  assert.deepEqual(new Set(conns), new Set([a1.socket.id, a2.socket.id]));
  assert.deepEqual(io.user('nobody').connections(), []);

  a1.ws.close();
  await withTimeout(once(a1.ws, 'close'), 5000, 'a1 close');
  await sleep(80);
  assert.equal(io.user('alice').connections().length, 1, 'drops to one device');

  a2.ws.close();
  await io.close();
});

test('backpressureReport surfaces the slowed consumer as top offender, with userId', async () => {
  const HWM = 32 * 1024 * 1024;
  const io = new BeamSocket({
    // Large HWM so bytes sit in the mailbox (once the writer blocks on a full
    // kernel buffer) instead of being dropped before we can observe them.
    backpressure: { highWaterMark: HWM, policy: 'drop-oldest' },
  });
  io.authorize((req) => ({ accept: true, userId: String(req.headers['x-user']) }));
  io.on('connection', () => {});
  const port = await io.listen(0);

  const slow = await connect(io, port, { 'x-user': 'laggard' });
  const fast = await connect(io, port, { 'x-user': 'speedy' });
  // Stop the slow client from reading so its server-side writer blocks on a
  // full socket buffer and the mailbox backs up.
  slow.ws._socket.pause();
  await sleep(30);

  // Push ~12 MB, far more than loopback kernel buffers absorb, so the writer
  // blocks and the surplus is held in this connection's mailbox.
  const big = Buffer.alloc(8192, 7);
  for (let i = 0; i < 1500; i++) io.toSocket(slow.socket.id).send(big);
  await sleep(150);

  const report = io.backpressureReport({ topN: 5 });
  assert.ok(report.mailboxes.length >= 1, 'at least one mailbox reported');
  const top = report.mailboxes[0];
  assert.equal(top.socketId, slow.socket.id, 'slowed consumer is the top offender');
  assert.equal(top.userId, 'laggard', 'offender userId attached');
  assert.ok(top.depthBytes > 0 && top.hwmPercent > 0, 'depth + HWM% reported');
  assert.equal(typeof report.totalDrops, 'number');

  slow.ws.terminate();
  fast.ws.close();
  await io.close();
});

test('memoryUsage(): scales with counts and is labeled estimated', async () => {
  const io = new BeamSocket({});
  io.on('connection', echoConn);
  const port = await io.listen(0);
  const base = io.memoryUsage();
  assert.equal(base.estimated, true);
  assert.equal(base.connections, 0);

  const a = await connect(io, port, {});
  await sleep(50);
  const one = io.memoryUsage();
  assert.equal(one.connections, 1);
  assert.ok(one.estimatedHeapBytes > base.estimatedHeapBytes, 'model grows with connections');
  assert.ok(one.mailboxBytesInFlight >= 0);

  a.ws.close();
  await io.close();
});

test('metricsText(): valid Prometheus exposition that a strict parser accepts', async () => {
  const io = new BeamSocket({ observability: { samplerMs: 100 } });
  io.on('connection', echoConn);
  const port = await io.listen(0);
  const { ws } = await connect(io, port, {});
  ws.send('x');
  await once(ws, 'message');
  await sleep(150);

  const text = io.metricsText();
  // Strict structural checks: every metric line references a name declared by a
  // preceding # TYPE, HELP/TYPE precede samples, and sample values are numeric.
  const declared = new Set();
  for (const line of text.split('\n')) {
    if (line === '' || line.startsWith('# HELP')) continue;
    if (line.startsWith('# TYPE')) {
      const m = line.match(/^# TYPE (\S+) (counter|gauge)$/);
      assert.ok(m, `well-formed TYPE line: ${line}`);
      declared.add(m[1]);
      continue;
    }
    const m = line.match(/^([a-zA-Z_][a-zA-Z0-9_]*)(\{[^}]*\})? (-?[0-9.eE+]+)$/);
    assert.ok(m, `well-formed sample line: ${line}`);
    assert.ok(declared.has(m[1]), `sample ${m[1]} has a preceding TYPE`);
    assert.ok(Number.isFinite(Number(m[3])), `numeric value in: ${line}`);
  }
  assert.match(text, /beamsocket_connections \d/);
  assert.match(text, /beamsocket_messages_in_total \d/);
  assert.match(text, /beamsocket_messages_in_per_second\{window="1s"\}/);

  ws.close();
  await io.close();
});

test('PERF GUARD: sampler ON vs OFF — echo throughput/p99 within noise (zero-hot-path)', async () => {
  async function measure(samplerMs) {
    const io = new BeamSocket({ observability: { samplerMs } });
    io.on('connection', echoConn);
    const port = await io.listen(0);
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    await once(ws, 'open');
    // Warm up.
    for (let i = 0; i < 200; i++) {
      ws.send('x');
      await once(ws, 'message');
    }
    // Sequential RTT sample for p99 + throughput.
    const N = 3000;
    const rtts = new Float64Array(N);
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < N; i++) {
      const s = process.hrtime.bigint();
      ws.send('x');
      await once(ws, 'message');
      rtts[i] = Number(process.hrtime.bigint() - s) / 1e6; // ms
    }
    const totalMs = Number(process.hrtime.bigint() - t0) / 1e6;
    rtts.sort();
    ws.close();
    await io.close();
    return { throughput: (N / totalMs) * 1000, p99: rtts[Math.floor(N * 0.99)] };
  }

  const off = await measure(0); // the 1D baseline (no sampler)
  const on = await measure(1000); // sampler running
  console.log(
    `perf guard: throughput off=${off.throughput.toFixed(0)}/s on=${on.throughput.toFixed(0)}/s; ` +
      `p99 off=${off.p99.toFixed(3)}ms on=${on.p99.toFixed(3)}ms`,
  );
  // Hard gate (generous for sandbox noise; catches gross regressions): the
  // sampler is off-thread and the echo path has no room counter, so ON must not
  // be materially slower than OFF.
  assert.ok(on.throughput > off.throughput * 0.7, `throughput within noise (off ${off.throughput.toFixed(0)}, on ${on.throughput.toFixed(0)})`);
  assert.ok(on.p99 < off.p99 * 3 + 1, `p99 within noise (off ${off.p99.toFixed(3)}, on ${on.p99.toFixed(3)})`);
});
