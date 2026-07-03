// Phase 1A informational gate: JS→Rust call cost for the sync command path
// (`send`), accepted as a cheap follow-up in docs/rfcs/0001-results.md
// §"JS→Rust direction" (hypothesis: sub-µs, cannot bottleneck vs the batched
// callback path).
//
//   node benchmarks/send-microbench.mjs [iters=200000]
//
// Three numbers:
//  - stale-id: napi call + sharded-registry miss (the FFI floor, no mailbox)
//  - live socket binary/text: napi call + registry hit + bounded-mailbox
//    push with a real client draining — the hot path apps pay per send()
import { BeamSocket } from '../packages/beamsocket/dist/index.js';
import { loadNative } from '../packages/beamsocket/dist/native.js';
import WebSocket from 'ws';
import { once } from 'node:events';

const ITERS = Number(process.argv[2] ?? 200000);

const io = new BeamSocket({ backpressure: { policy: 'drop-newest' } });
let serverSocket;
io.on('connection', (s) => (serverSocket = s));
const port = await io.listen(0);

const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
ws.on('message', () => {}); // drain
await once(ws, 'open');
while (!serverSocket) await new Promise((r) => setTimeout(r, 5));

const payload = Buffer.from('x'.repeat(64));
const text = 'x'.repeat(64);

function bench(label, fn) {
  fn(); // warm
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < ITERS; i++) fn();
  const ns = Number(process.hrtime.bigint() - t0) / ITERS;
  console.log(`${label}: ${ns.toFixed(0)} ns/call (${(1e9 / ns / 1e6).toFixed(2)} M calls/s)`);
}

// FFI floor: separate raw engine, id that cannot exist → registry miss.
const floor = loadNative().BeamEngine.start({}, () => {});
bench('send (stale id — FFI+registry floor)', () => floor.send(7, 7, payload, true));
floor.shutdown();

bench('socket.send (live, 64 B binary)     ', () => serverSocket.send(payload));
bench('socket.send (live, 64 B text)       ', () => serverSocket.send(text));

await new Promise((r) => setTimeout(r, 300));
ws.close();
await io.close();
process.exit(0);
