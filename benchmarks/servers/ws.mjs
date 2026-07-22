// ws server adapter — echo + room fan-out. Room = a Set the server loops over
// (the canonical ws broadcast pattern). Trigger frame "GO" → fan the 512 B
// payload to every socket in the room.
import { WebSocketServer } from 'ws';

const port = Number(process.argv[2]);
const PAYLOAD = Buffer.alloc(512, 0x61);
const room = new Set();

const wss = new WebSocketServer({ port, perMessageDeflate: false }, () => {
  process.stdout.write(`READY ${port}\n`);
});

wss.on('connection', (ws) => {
  room.add(ws);
  ws.on('message', (data, isBinary) => {
    if (data.length === 2 && data[0] === 0x47 && data[1] === 0x4f) {
      for (const c of room) if (c.readyState === 1) c.send(PAYLOAD);
    } else {
      ws.send(data, { binary: isBinary });
    }
  });
  ws.on('close', () => room.delete(ws));
  ws.on('error', () => {});
});
