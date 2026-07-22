// Socket.IO server adapter — driven by its own client (socketio.client.mjs),
// the only fair way to measure it. WebSocket transport only, deflate off, so
// the comparison isolates the library, not a polling fallback. Echo via an
// 'echo' event; room fan-out via io.to('bench').emit on a 'go' trigger.
import { Server } from 'socket.io';

const port = Number(process.argv[2]);
const PAYLOAD = Buffer.alloc(512, 0x61);

const io = new Server(port, {
  perMessageDeflate: false,
  transports: ['websocket'],
  maxHttpBufferSize: 16 * 1024 * 1024,
});

io.on('connection', (s) => {
  s.join('bench');
  s.on('echo', (d) => s.emit('echo', d));
  s.on('go', () => io.to('bench').emit('bcast', PAYLOAD));
});

process.stdout.write(`READY ${port}\n`);
