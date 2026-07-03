// `ws` reference server for the fan-out/density suites (same contract as
// beamsocket-server.mjs). Broadcast = the canonical wss.clients loop.
import readline from 'node:readline';
import { WebSocketServer } from 'ws';

const wss = new WebSocketServer({ port: 0, perMessageDeflate: false });
wss.on('listening', () => console.log(`PORT ${wss.address().port}`));
wss.on('connection', (ws) => ws.on('message', () => {}));

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  const [cmd, a, b] = line.split(' ');
  if (cmd === 'rss') {
    global.gc?.();
    console.log(`RSS ${process.memoryUsage().rss}`);
  } else if (cmd === 'count') {
    console.log(`COUNT ${wss.clients.size}`);
  } else if (cmd === 'bcast') {
    const buf = Buffer.alloc(Number(b));
    buf.writeUInt32LE(Number(a), 0);
    for (const c of wss.clients) {
      c.send(buf, { binary: true });
    }
    console.log(`SENT ${a}`);
  } else if (cmd === 'exit') {
    process.exit(0);
  }
});
