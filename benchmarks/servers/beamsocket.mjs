// BeamSocket server adapter — single-node (cluster deferred to the addon
// rebuild). Room fan-out via io.toRoom('bench').send(), which fans out entirely
// in Rust off the JS event loop. Same echo + "GO" trigger contract as the others.
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

const port = Number(process.argv[2]);
const PAYLOAD = Buffer.alloc(512, 0x61);

// Match the other servers' send buffering: ws buffers without limit and uws
// sets maxBackpressure: 0. BeamSocket's default (64 KiB, Disconnect) would
// cut every pipelined 16 KiB echo client with 1013 within a second — 20 in
// flight x 16 KiB = 320 KiB — so throughput16k would measure dead sockets.
const io = new BeamSocket({ backpressure: { highWaterMark: 16 * 1024 * 1024 } });

io.on('connection', (s) => {
  s.join('bench');
  s.on('message', (data) => {
    const b = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (b.length === 2 && b[0] === 0x47 && b[1] === 0x4f) {
      io.toRoom('bench').send(PAYLOAD);
    } else {
      s.send(b);
    }
  });
});

await io.listen(port);
process.stdout.write(`READY ${port}\n`);
