// 0.2.0 integration (docs/ENGINEERING.md §14, work order "0.2.0 — Clustering
// Reaches JavaScript"): a 3-node mesh formed and driven ENTIRELY from JS
// config — no addon internals, no manual wire-protocol knowledge. Three
// `BeamSocket` instances in this one process, each with its own `cluster`
// config on loopback, mirrors the multi-process production topology closely
// enough for the required verb/except/secret gates; `cluster-process.
// integration.test.mjs` covers what genuinely needs separate OS processes
// (kill -9 survival, clean process exit with a mesh running).
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

/** Poll `f()` until it returns true or the timeout elapses. */
async function pollUntil(f, timeoutMs, label) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (f()) return true;
    await sleep(25);
  }
  assert.fail(`timed out waiting for ${label}`);
}

let nextMeshPort = 27100;
function meshPort() {
  return nextMeshPort++;
}

/** Boot `count` nodes seeded off node 0, wait for full mesh convergence
 * (every node sees `count - 1` peers), and return `{ ios, ports }`. */
async function spawnCluster(count, { secret = 'test-cluster-secret', clusterName } = {}) {
  const meshPorts = Array.from({ length: count }, () => meshPort());
  const ios = [];
  const ports = [];
  for (let i = 0; i < count; i++) {
    const io = new BeamSocket({
      cluster: {
        nodeId: i + 1,
        listen: `127.0.0.1:${meshPorts[i]}`,
        seeds: i === 0 ? [] : [`127.0.0.1:${meshPorts[0]}`],
        secret,
        clusterName,
      },
    });
    io.on('connection', () => {});
    const port = await io.listen(0);
    ios.push(io);
    ports.push(port);
  }
  await pollUntil(
    () => ios.every((io) => (io.stats().cluster?.peers ?? 0) === count - 1),
    10_000,
    `${count}-node mesh convergence`,
  );
  return { ios, ports };
}

async function closeAll(ios) {
  await Promise.all(ios.map((io) => io.close({ timeoutMs: 1000 })));
}

test('3-node cluster forms from JS config; membership visible in stats().cluster on all three', async () => {
  const { ios } = await spawnCluster(3);
  for (const io of ios) {
    const s = io.stats();
    assert.ok(s.cluster, 'stats().cluster must be present when clustered');
    assert.equal(s.cluster.peers, 2);
    assert.ok(s.cluster.nodeId >= 1 && s.cluster.nodeId <= 3);
  }
  await closeAll(ios);
});

// Regression: `socket.id` under cluster config is node-prefixed
// (`encodeSocketId(hi, lo, nodeId)`, 3 segments), but the server's internal
// connection lookup once used a second, independent 2-segment key builder
// for the native bridge's onMessage/onClose/presence-metadata paths. The
// keys never matched under cluster config, so `socket.on('message', ...)`
// silently never fired for any client-sent message — invisible to every
// other test in this file because they all trigger fan-out via
// io.toRoom()/toUser()/toSocket().send() (server-initiated), never via a
// client message hitting this exact lookup. Found running examples/cluster
// by hand; fixed by deriving the lookup key from the same encodeSocketId()
// that builds socket.id.
test('a client-sent message reaches socket.on("message") when cluster is configured', async () => {
  const { ios, ports } = await spawnCluster(1);
  const [io1] = ios;

  const m1 = await connectAs(io1, ports[0]);
  const receivedP = withTimeout(
    new Promise((res) => m1.socket.on('message', (data) => res(data.toString()))),
    5000,
    'server socket.on("message")',
  );
  m1.ws.send('ping from client');
  assert.equal(await receivedP, 'ping from client');

  await closeAll(ios);
});

test('toRoom crosses nodes exactly once; except honored across nodes', async () => {
  const { ios, ports } = await spawnCluster(3);
  const [io1, , io3] = ios;

  const m1 = await connectAs(io1, ports[0]);
  const m3a = await connectAs(io3, ports[2]);
  const m3b = await connectAs(io3, ports[2]);
  m1.socket.join('lobby');
  m3a.socket.join('lobby');
  m3b.socket.join('lobby');
  await sleep(300); // let interest propagate (edge-triggered gossip)

  // Plain send: every member on every node gets it, exactly once.
  const p1 = nextMessage(m1.ws, 'm1');
  const pa = nextMessage(m3a.ws, 'm3a');
  const pb = nextMessage(m3b.ws, 'm3b');
  io1.toRoom('lobby').send('hello lobby');
  assert.equal((await p1)[0].toString(), 'hello lobby');
  assert.equal((await pa)[0].toString(), 'hello lobby');
  assert.equal((await pb)[0].toString(), 'hello lobby');

  // except() naming a REMOTE socket (m3a, on node 3) must be honored on node 3
  // — m3b (same node, not excepted) still gets it; m3a gets nothing.
  let m3aGot = false;
  m3a.ws.once('message', () => (m3aGot = true));
  const pb2 = nextMessage(m3b.ws, 'm3b second');
  io1.toRoom('lobby').except(m3a.socket.id).send('second');
  assert.equal((await pb2)[0].toString(), 'second');
  await sleep(300);
  assert.equal(m3aGot, false, 'except() must be honored across the node hop');

  await closeAll(ios);
});

test('toUser reaches devices on different nodes, exactly once each', async () => {
  // authorize() must be registered before listen() (Phase 1C), so this test
  // builds its own pair of nodes directly rather than reusing spawnCluster
  // (which listens with no authorize hook).
  const meshA = meshPort();
  const meshB = meshPort();
  const secret = 'test-cluster-secret';
  const a = new BeamSocket({
    cluster: { nodeId: 1, listen: `127.0.0.1:${meshA}`, seeds: [], secret },
  });
  a.authorize((req) => ({ accept: true, userId: String(req.headers['x-user'] ?? '') }));
  a.on('connection', () => {});
  const portA = await a.listen(0);

  const b = new BeamSocket({
    cluster: { nodeId: 2, listen: `127.0.0.1:${meshB}`, seeds: [`127.0.0.1:${meshA}`], secret },
  });
  b.authorize((req) => ({ accept: true, userId: String(req.headers['x-user'] ?? '') }));
  b.on('connection', () => {});
  const portB = await b.listen(0);

  await pollUntil(
    () => (a.stats().cluster?.peers ?? 0) === 1 && (b.stats().cluster?.peers ?? 0) === 1,
    10_000,
    '2-node mesh convergence',
  );

  const devA = await connectAs(a, portA, { 'x-user': 'alice' });
  const devB = await connectAs(b, portB, { 'x-user': 'alice' });
  await sleep(300); // interest propagation for the user

  const pA = nextMessage(devA.ws, 'alice@node1');
  const pB = nextMessage(devB.ws, 'alice@node2');
  a.toUser('alice').send('ping every device');
  assert.equal((await pA)[0].toString(), 'ping every device');
  assert.equal((await pB)[0].toString(), 'ping every device');

  await closeAll([a, b]);
});

test('broadcast reaches every node; toSocket(remote id) routes to the owning node only', async () => {
  const { ios, ports } = await spawnCluster(3);
  const [io1, io2, io3] = ios;

  const m1 = await connectAs(io1, ports[0]);
  const m2 = await connectAs(io2, ports[1]);
  const m3 = await connectAs(io3, ports[2]);

  const p1 = nextMessage(m1.ws, 'm1');
  const p2 = nextMessage(m2.ws, 'm2');
  const p3 = nextMessage(m3.ws, 'm3');
  io1.broadcast('everyone');
  assert.equal((await p1)[0].toString(), 'everyone');
  assert.equal((await p2)[0].toString(), 'everyone');
  assert.equal((await p3)[0].toString(), 'everyone');

  // toSocket(remote id): m3.socket.id is node-prefixed (three-segment) since
  // io3 is clustered. io1.toSocket(that id) must reach ONLY m3, not m1/m2.
  assert.match(m3.socket.id, /^[0-9a-z]+-[0-9a-z]+-[0-9a-z]+$/, 'clustered id is 3-segment');
  let m1Got = false;
  let m2Got = false;
  m1.ws.once('message', () => (m1Got = true));
  m2.ws.once('message', () => (m2Got = true));
  const p3b = nextMessage(m3.ws, 'm3 direct');
  io1.toSocket(m3.socket.id).send('direct to m3');
  assert.equal((await p3b)[0].toString(), 'direct to m3');
  await sleep(300);
  assert.equal(m1Got, false);
  assert.equal(m2Got, false);

  await closeAll(ios);
});

test('wrong secret: node refused, JS-observable via stats, no crash', async () => {
  const meshA = meshPort();
  const meshB = meshPort();
  const a = new BeamSocket({
    cluster: { nodeId: 1, listen: `127.0.0.1:${meshA}`, seeds: [], secret: 'correct-secret' },
  });
  a.on('connection', () => {});
  await a.listen(0);

  const b = new BeamSocket({
    cluster: {
      nodeId: 2,
      listen: `127.0.0.1:${meshB}`,
      seeds: [`127.0.0.1:${meshA}`],
      secret: 'WRONG-secret',
    },
  });
  b.on('connection', () => {});
  await b.listen(0);

  // Give the handshake attempt time to fail; it must fail, not hang or crash
  // the process (this test file continuing to run other tests IS part of the
  // "no crash" proof).
  await sleep(1000);
  assert.equal(a.stats().cluster?.peers ?? 0, 0, 'mismatched secret must never join');
  assert.equal(b.stats().cluster?.peers ?? 0, 0, 'mismatched secret must never join');

  await closeAll([a, b]);
});

test('single-node mode: no cluster config → stats().cluster is undefined', async () => {
  const io = new BeamSocket({});
  io.on('connection', () => {});
  await io.listen(0);
  assert.equal(io.stats().cluster, undefined);
  await io.close();
});
