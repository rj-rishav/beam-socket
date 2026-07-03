// Uniform bench-server contract (see ../fanout.mjs):
//   stdout: "PORT <n>" once listening
//   stdin:  "rss" -> "RSS <bytes>" | "count" -> "COUNT <n>"
//         | "bcast <seq> <bytes>" -> broadcast binary [u32 seq][zeros]
//         | "exit"
import readline from 'node:readline';
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

const io = new BeamSocket({});
let conns = 0;
io.on('connection', (socket) => {
  conns++;
  socket.join('bench');
  socket.on('close', () => conns--);
});
const port = await io.listen(0);
console.log(`PORT ${port}`);

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  const [cmd, a, b] = line.split(' ');
  if (cmd === 'rss') {
    global.gc?.();
    console.log(`RSS ${process.memoryUsage().rss}`);
  } else if (cmd === 'count') {
    console.log(`COUNT ${conns}`);
  } else if (cmd === 'bcast') {
    const buf = Buffer.alloc(Number(b));
    buf.writeUInt32LE(Number(a), 0);
    io.toRoom('bench').send(buf); // ONE FFI call; fan-out in Rust
    console.log(`SENT ${a}`);
  } else if (cmd === 'exit') {
    process.exit(0);
  }
});
