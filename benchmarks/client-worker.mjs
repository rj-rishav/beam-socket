// Fan-out client worker (forked by fanout.mjs, talks over IPC).
//   argv: <url> <proto ws|sio> <count>
// Parent → child: {expect: seq} — start counting frames tagged seq.
// Child → parent: {ready}, {expecting: seq}, {done: seq, t: ms-epoch}
import WebSocket from 'ws';
import { io as sioClient } from 'socket.io-client';

const [url, proto, countArg] = process.argv.slice(2);
const COUNT = Number(countArg);
const WAVE = 250;

let expectSeq = -1;
let got = 0;

function onPayload(buf) {
  if (buf.length >= 4 && buf.readUInt32LE(0) === expectSeq) {
    got++;
    if (got === COUNT) {
      process.send({ done: expectSeq, t: Date.now() });
    }
  }
}

process.on('message', (m) => {
  if (typeof m.expect === 'number') {
    expectSeq = m.expect;
    got = 0;
    process.send({ expecting: expectSeq });
  }
});

const sockets = [];
async function connectAll() {
  for (let done = 0; done < COUNT; done += WAVE) {
    const batch = Math.min(WAVE, COUNT - done);
    await Promise.all(
      Array.from({ length: batch }, () => {
        return new Promise((resolve, reject) => {
          if (proto === 'sio') {
            const s = sioClient(url, {
              transports: ['websocket'],
              forceNew: true,
              reconnection: false,
            });
            s.on('m', (data) => onPayload(Buffer.from(data)));
            s.on('connect', resolve);
            s.on('connect_error', reject);
            sockets.push(s);
          } else {
            const s = new WebSocket(url, { perMessageDeflate: false });
            s.on('message', (data) => onPayload(data));
            s.on('open', resolve);
            s.on('error', reject);
            sockets.push(s);
          }
        });
      }),
    );
  }
}

connectAll().then(
  () => process.send({ ready: COUNT }),
  (e) => {
    console.error('worker connect failed:', e?.message ?? e);
    process.exit(1);
  },
);
