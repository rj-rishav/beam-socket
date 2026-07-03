// uWebSockets.js reference server (same contract). Broadcast = pub/sub
// publish — uWS's strongest configuration, per the benchmark honesty rules.
import readline from 'node:readline';
import { createRequire } from 'node:module';
const uWS = createRequire(import.meta.url)('uWebSockets.js');

let conns = 0;
let listenSocket;
const app = uWS
  .App()
  .ws('/*', {
    compression: uWS.DISABLED,
    maxPayloadLength: 1 << 20,
    open: (ws) => {
      conns++;
      ws.subscribe('bench');
    },
    close: () => conns--,
    message: () => {},
  })
  .listen(0, (token) => {
    if (!token) {
      console.error('uws listen failed');
      process.exit(1);
    }
    listenSocket = token;
    console.log(`PORT ${uWS.us_socket_local_port(token)}`);
  });

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
    app.publish('bench', buf, true);
    console.log(`SENT ${a}`);
  } else if (cmd === 'exit') {
    if (listenSocket) uWS.us_listen_socket_close(listenSocket);
    process.exit(0);
  }
});
