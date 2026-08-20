# cluster — a 3-node BeamSocket mesh, from plain JS (0.2.0)

Three BeamSocket processes on loopback, seeded into one cluster, each with its
own WebSocket clients. A message sent to node 1's client shows up on node 2's
and node 3's clients — `io.toRoom('lobby').send()` fans out locally **and**
relays across the mesh, exactly once per member, entirely from
`new BeamSocket({ cluster: {...} })` config. See ENGINEERING.md §13.4 / RFC
0004 for the mechanics; this example only exercises the JS surface.

## Setup

```bash
cd examples/cluster
npm install
npm --prefix ../../packages/beamsocket run build:native   # build the Rust addon (needs Rust)
npm --prefix ../../packages/beamsocket run build           # build the SDK dist
```

## Run the 3-node cluster

Open **three terminals**, one per node. Node 1 starts with no seeds (it's the
first member); nodes 2 and 3 seed off node 1's mesh port so all three
converge into one cluster (SWIM gossip fills in the rest — nodes 2 and 3
learn about each other too, not just node 1).

**Terminal 1:**
```bash
NODE_ID=1 WS_PORT=9101 MESH_PORT=7101 \
  node examples/cluster/node.mjs
```

**Terminal 2:**
```bash
NODE_ID=2 WS_PORT=9102 MESH_PORT=7102 SEEDS=127.0.0.1:7101 \
  node examples/cluster/node.mjs
```

**Terminal 3:**
```bash
NODE_ID=3 WS_PORT=9103 MESH_PORT=7103 SEEDS=127.0.0.1:7101 \
  node examples/cluster/node.mjs
```

Each node logs `cluster: peers=N ...` every 2s — once convergence finishes,
every node should show `peers=2`.

## Connect clients and cross-node chat

Open **three more terminals**, one WebSocket client per node:

```bash
node examples/cluster/client.mjs 9101   # connects to node 1
node examples/cluster/client.mjs 9102   # connects to node 2
node examples/cluster/client.mjs 9103   # connects to node 3
```

Then, in a fourth terminal, send one message through node 1's client:

```bash
node examples/cluster/client.mjs 9101 "hello from node 1"
```

The client connected to node 1 sees nothing back (it's the sender, excepted
via `.except(socket.id)` — honored across the mesh, not just locally); the
clients connected to nodes 2 and 3 each print exactly one
`[node 1 relayed] hello from node 1`. Try sending from the node 2 and node 3
clients too — every direction works the same way.

## What this proves

- A 3-node cluster forms from JS config alone (`seeds` + a shared `secret`) —
  no addon internals, no manual wire-protocol knowledge.
- `toRoom` fans out locally and relays cross-node, exactly once per member.
- `except()` is honored across nodes (the sender never gets its own relayed
  message back, even though the exclusion has to survive a network hop).
- `io.stats().cluster` gives you peer count and relay counters for free.

## Failure behavior (worth trying by hand)

- **Wrong secret:** start a fourth node with a different `SECRET` — it never
  joins (`peers` on the other three never counts it, and the node's own logs
  show it stuck at `peers=0`). The mesh authenticates every peer by shared
  secret (RFC 0004 §4.7); a mismatched node is refused, not silently ignored.
- **Kill one node:** `Ctrl-C` (or `kill -9`) node 3. Nodes 1 and 2 keep
  serving their own clients and each other; the dead peer drops out of their
  `stats().cluster.peers` count within the SWIM detection window (a few
  seconds), and messages to `lobby` still reach whichever of nodes 1/2 host
  members.

## Single-node reminder

None of this costs anything if you don't use it — omit `cluster` from the
config (see `examples/chat` or `examples/echo`) and BeamSocket runs exactly
as it did before 0.2.0.
