// 0.2.0 integration — the two required-test-matrix gates that only mean
// something across REAL OS process boundaries: `kill -9` survival and a
// clean process exit with a mesh running. Spawns
// __tests__/helpers/cluster-worker.mjs as genuine child processes (not
// in-process BeamSocket instances — see cluster.integration.test.mjs for the
// formation/verb/except/secret gates, which don't need separate processes).
import { test } from 'node:test';
import assert from 'node:assert';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
import WebSocket from 'ws';

const WORKER = fileURLToPath(new URL('./helpers/cluster-worker.mjs', import.meta.url));

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms).unref(),
    ),
  ]);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Spawn one cluster-worker child process; returns a handle with parsed
 * stdout events and a `waitFor(predicate)` helper. */
function spawnWorker({ nodeId, meshPort, seeds, secret }) {
  const child = spawn(process.execPath, [WORKER], {
    env: {
      ...process.env,
      NODE_ID: String(nodeId),
      WS_PORT: '0',
      MESH_PORT: String(meshPort),
      SEEDS: (seeds ?? []).join(','),
      SECRET: secret ?? 'cluster-process-test-secret',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const events = [];
  const listeners = [];
  const rl = createInterface({ input: child.stdout });
  rl.on('line', (line) => {
    let evt;
    try {
      evt = JSON.parse(line);
    } catch {
      return;
    }
    events.push(evt);
    for (const l of listeners) l(evt);
  });
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d.toString()));

  return {
    child,
    events,
    getStderr: () => stderr,
    latestStats: () => [...events].reverse().find((e) => e.type === 'stats'),
    waitFor(predicate, ms, label) {
      const already = events.find(predicate);
      if (already) return Promise.resolve(already);
      return withTimeout(
        new Promise((resolve) => {
          const l = (evt) => {
            if (predicate(evt)) {
              listeners.splice(listeners.indexOf(l), 1);
              resolve(evt);
            }
          };
          listeners.push(l);
        }),
        ms,
        label,
      );
    },
  };
}

let nextMeshPort = 27300;
function meshPort() {
  return nextMeshPort++;
}

test('kill -9 one node: the other two keep serving; dead peer leaves stats().cluster within the detection window', async () => {
  const secret = 'kill-test-secret';
  const p1 = meshPort();
  const p2 = meshPort();
  const p3 = meshPort();

  const n1 = spawnWorker({ nodeId: 1, meshPort: p1, seeds: [], secret });
  const n2 = spawnWorker({ nodeId: 2, meshPort: p2, seeds: [`127.0.0.1:${p1}`], secret });
  const n3 = spawnWorker({ nodeId: 3, meshPort: p3, seeds: [`127.0.0.1:${p1}`], secret });

  try {
    const [r1, r2, r3] = await Promise.all([
      n1.waitFor((e) => e.type === 'ready', 10_000, 'n1 ready'),
      n2.waitFor((e) => e.type === 'ready', 10_000, 'n2 ready'),
      n3.waitFor((e) => e.type === 'ready', 10_000, 'n3 ready'),
    ]);

    await Promise.all([
      n1.waitFor((e) => e.type === 'stats' && e.peers === 2, 10_000, 'n1 sees 2 peers'),
      n2.waitFor((e) => e.type === 'stats' && e.peers === 2, 10_000, 'n2 sees 2 peers'),
      n3.waitFor((e) => e.type === 'stats' && e.peers === 2, 10_000, 'n3 sees 2 peers'),
    ]);

    // A client on node1 still works before the kill (baseline).
    const ws1 = new WebSocket(`ws://127.0.0.1:${r1.port}/`);
    await withTimeout(new Promise((r) => ws1.once('open', r)), 5000, 'ws1 open');

    // kill -9 node3.
    assert.ok(n3.child.kill('SIGKILL'));
    await withTimeout(
      new Promise((resolve) => n3.child.once('exit', resolve)),
      5000,
      'n3 process exit',
    );

    // n1 and n2 must notice within the SWIM detection window and keep serving.
    await Promise.all([
      n1.waitFor((e) => e.type === 'stats' && e.peers === 1, 15_000, 'n1 drops to 1 peer'),
      n2.waitFor((e) => e.type === 'stats' && e.peers === 1, 15_000, 'n2 drops to 1 peer'),
    ]);

    // node1 still serves a live client after the kill.
    const ws1b = new WebSocket(`ws://127.0.0.1:${r1.port}/`);
    await withTimeout(new Promise((r) => ws1b.once('open', r)), 5000, 'ws1b open');
    ws1.close();
    ws1b.close();
  } finally {
    n1.child.kill('SIGKILL');
    n2.child.kill('SIGKILL');
    n3.child.kill('SIGKILL');
  }
});

test('clean process exit with a mesh running (TSFN + mesh threads all join)', async () => {
  const secret = 'exit-test-secret';
  const p1 = meshPort();
  const p2 = meshPort();

  const n1 = spawnWorker({ nodeId: 1, meshPort: p1, seeds: [], secret });
  const n2 = spawnWorker({ nodeId: 2, meshPort: p2, seeds: [`127.0.0.1:${p1}`], secret });

  await Promise.all([
    n1.waitFor((e) => e.type === 'ready', 10_000, 'n1 ready'),
    n2.waitFor((e) => e.type === 'ready', 10_000, 'n2 ready'),
  ]);
  await Promise.all([
    n1.waitFor((e) => e.type === 'stats' && e.peers === 1, 10_000, 'n1 sees peer'),
    n2.waitFor((e) => e.type === 'stats' && e.peers === 1, 10_000, 'n2 sees peer'),
  ]);

  // SIGTERM both; the worker's handler calls io.close() and does NOT call
  // process.exit() — if anything (a mesh thread, the TSFN) fails to release,
  // the process hangs and this test times out.
  n1.child.kill('SIGTERM');
  n2.child.kill('SIGTERM');

  const [code1, code2] = await Promise.all([
    withTimeout(
      new Promise((resolve) => n1.child.once('exit', (code) => resolve(code))),
      8000,
      'n1 exits by itself',
    ),
    withTimeout(
      new Promise((resolve) => n2.child.once('exit', (code) => resolve(code))),
      8000,
      'n2 exits by itself',
    ),
  ]);
  assert.equal(code1, 0, `n1 stderr:\n${n1.getStderr()}`);
  assert.equal(code2, 0, `n2 stderr:\n${n2.getStderr()}`);
});
