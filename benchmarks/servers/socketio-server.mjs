// Socket.IO reference server (same contract), websocket transport only,
// compression off — its fastest fair configuration.
import readline from 'node:readline';
import { createServer } from 'node:http';
import { Server } from 'socket.io';

const http = createServer();
const io = new Server(http, {
  transports: ['websocket'],
  perMessageDeflate: false,
  httpCompression: false,
});
let conns = 0;
io.on('connection', (socket) => {
  conns++;
  socket.join('bench');
  socket.on('disconnect', () => conns--);
});
http.listen(0, () => console.log(`PORT ${http.address().port}`));

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
    io.to('bench').emit('m', buf);
    console.log(`SENT ${a}`);
  } else if (cmd === 'exit') {
    process.exit(0);
  }
});
