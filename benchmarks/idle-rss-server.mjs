// Child process for idle-rss.mjs: a BeamSocket server that answers simple
// stdin commands with lines on stdout. Not a benchmark itself.
import { BeamSocket } from '../packages/beamsocket/dist/index.js';
import readline from 'node:readline';

const io = new BeamSocket({});
io.on('connection', () => {}); // subscribe so opens flow (echo not needed)
const port = await io.listen(0);
console.log(`PORT ${port}`);

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  if (line === 'rss') {
    // Self-reported RSS from /proc (Linux); includes V8 + engine + buffers.
    console.log(`RSS ${process.memoryUsage().rss}`);
  } else if (line === 'gc') {
    global.gc?.();
    console.log('GC ok');
  } else if (line === 'exit') {
    process.exit(0);
  }
});
