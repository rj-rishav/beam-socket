// BeamSocket server adapter — single-node (cluster deferred to the addon
// rebuild). Room fan-out via io.toRoom('bench').send(), which fans out entirely
// in Rust off the JS event loop. Same echo + "GO" trigger contract as the others.
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

const port = Number(process.argv[2]);
const PAYLOAD = Buffer.alloc(512, 0x61);

const io = new BeamSocket({});

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
