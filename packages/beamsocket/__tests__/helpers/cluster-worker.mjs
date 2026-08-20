// Test-only helper (0.2.0): one cluster node run as a REAL child process, so
// cluster-process.integration.test.mjs can exercise things that only mean
// something across process boundaries — `kill -9` survival and a clean
// process exit with a mesh running (the 1D "process exits by itself" proof,
// extended to a node with mesh threads).
//
// Protocol (stdout, one JSON line per event — the parent test parses these):
//   {"type":"ready","port":N}                     — WS listening
//   {"type":"stats","peers":N,"nodeId":N}          — printed every 200ms
//   {"type":"message","from":"<socketId>","data":"..."}  — a relayed message
//
// Env: NODE_ID, WS_PORT (0 = ephemeral), MESH_PORT, SEEDS (comma-separated,
// may be empty), SECRET.
import { BeamSocket } from '../../dist/index.js';

const nodeId = Number(process.env.NODE_ID);
const wsPort = Number(process.env.WS_PORT ?? 0);
const meshPort = Number(process.env.MESH_PORT);
const seeds = (process.env.SEEDS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
const secret = process.env.SECRET ?? 'cluster-process-test-secret';

const io = new BeamSocket({
  cluster: {
    nodeId,
    listen: `127.0.0.1:${meshPort}`,
    seeds,
    secret,
    clusterName: 'cluster-process-test',
  },
});

io.on('connection', (socket) => {
  socket.join('lobby');
  socket.on('message', (data) => {
    const text = data.toString('utf8');
    process.stdout.write(JSON.stringify({ type: 'message', from: socket.id, data: text }) + '\n');
    io.toRoom('lobby').except(socket.id).send(`relayed:${text}`);
  });
});

const port = await io.listen(wsPort);
process.stdout.write(JSON.stringify({ type: 'ready', port }) + '\n');

const statsTimer = setInterval(() => {
  const s = io.stats();
  process.stdout.write(
    JSON.stringify({ type: 'stats', peers: s.cluster?.peers ?? -1, nodeId: s.cluster?.nodeId ?? -1 }) +
      '\n',
  );
}, 200);
statsTimer.unref();

// Graceful path (SIGTERM): drain and let the process exit by itself — proves
// the TSFN AND the mesh's threads all join (the 1D proof, extended).
process.on('SIGTERM', async () => {
  clearInterval(statsTimer);
  await io.close({ timeoutMs: 2000 });
  // Deliberately no process.exit(): a lingering handle here would mean the
  // "exits by itself" proof failed.
});
