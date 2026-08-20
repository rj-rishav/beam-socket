// 0.2.0 — cluster mesh reaches JavaScript. Boots ONE node of a 3-node mesh;
// run this three times (see README.md) to form the cluster. Every targeting
// verb (`toRoom`, `toUser`, `broadcast`, `toSocket`) fans out locally AND
// relays to the other nodes that host the target — from plain JS config,
// no addon-internals knowledge required.
//
//   NODE_ID=1 WS_PORT=9101 MESH_PORT=7101 SEEDS=127.0.0.1:7102,127.0.0.1:7103 \
//     node examples/cluster/node.mjs
//
// Env vars:
//   NODE_ID    required — unique small integer per node (0..65535)
//   WS_PORT    required — this node's WebSocket listen port (clients connect here)
//   MESH_PORT  required — this node's mesh (TCP link + UDP SWIM) listen port
//   SEEDS      optional — comma-separated host:port list of other nodes' mesh
//              ports (any live member works; the first node in a fresh
//              cluster can start with no seeds)
//   SECRET     optional — shared cluster HMAC secret (default below is for
//              local demo use ONLY — set a real secret in production)
//   CLUSTER_NAME  optional — defaults to 'beamsocket-cluster-example'
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

function required(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is required — see examples/cluster/README.md`);
  return v;
}

const nodeId = Number(required('NODE_ID'));
const wsPort = Number(required('WS_PORT'));
const meshPort = Number(required('MESH_PORT'));
const seeds = (process.env.SEEDS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
const secret = process.env.SECRET ?? 'beamsocket-cluster-example-demo-secret-do-not-use-in-prod';
const clusterName = process.env.CLUSTER_NAME ?? 'beamsocket-cluster-example';

const io = new BeamSocket({
  cluster: {
    nodeId,
    listen: `127.0.0.1:${meshPort}`,
    seeds,
    secret,
    clusterName,
  },
});

io.on('connection', (socket) => {
  socket.join('lobby');
  console.log(`[node ${nodeId}] connection ${socket.id} joined lobby`);

  socket.on('message', (data, isBinary) => {
    const text = isBinary ? data.toString('base64') : data.toString('utf8');
    console.log(`[node ${nodeId}] message from ${socket.id}: ${text}`);
    // Fans out to EVERY member of 'lobby' across ALL three nodes, exactly
    // once each, with the sender excluded — one call, Rust does the rest
    // (locally AND across the mesh relay).
    io.toRoom('lobby').except(socket.id).send(`[node ${nodeId} relayed] ${text}`);
  });

  socket.on('close', (code, reason) => {
    console.log(`[node ${nodeId}] connection ${socket.id} closed (${code}) ${reason}`);
  });
});

const bound = await io.listen(wsPort);
console.log(`[node ${nodeId}] listening ws://127.0.0.1:${bound}, mesh 127.0.0.1:${meshPort}, seeds=[${seeds.join(', ')}]`);

// Print cluster membership every 2s so it's visible from the terminal without
// a separate tool — the same stats().cluster a real app would poll.
setInterval(() => {
  const s = io.stats();
  if (s.cluster) {
    console.log(
      `[node ${nodeId}] cluster: peers=${s.cluster.peers} relayIn=${s.cluster.relayIn} ` +
        `relayOut=${s.cluster.relayOut} relayDrops=${s.cluster.relayDrops}`,
    );
  }
}, 2000).unref();

process.on('SIGINT', async () => {
  console.log(`[node ${nodeId}] shutting down`);
  await io.close({ timeoutMs: 2000 });
  process.exit(0);
});
