// uWebSockets.js server adapter — echo + room fan-out via its NATIVE pub/sub
// (topics), which is uWS's idiomatic and fastest broadcast path. Every socket
// subscribes to "bench"; trigger "GO" → ws.publish fans out in C++.
import uWS from 'uWebSockets.js';

const port = Number(process.argv[2]);
const PAYLOAD = Buffer.alloc(512, 0x61);

uWS.App()
  .ws('/*', {
    compression: uWS.DISABLED,
    idleTimeout: 0,
    maxBackpressure: 0,
    maxPayloadLength: 16 * 1024 * 1024,
    open: (ws) => {
      ws.subscribe('bench');
    },
    message: (ws, msg, isBinary) => {
      const b = Buffer.from(msg);
      if (b.length === 2 && b[0] === 0x47 && b[1] === 0x4f) {
        ws.publish('bench', PAYLOAD, true);
      } else {
        ws.send(msg, isBinary);
      }
    },
  })
  .listen(port, (tok) => {
    if (tok) process.stdout.write(`READY ${port}\n`);
    else {
      process.stdout.write('LISTEN_FAILED\n');
      process.exit(1);
    }
  });
