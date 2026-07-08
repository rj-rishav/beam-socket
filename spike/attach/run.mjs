// RFC 0002 attach spike runner — THROWAWAY, Linux + plaintext only.
//
// Starts an http.Server, hands each /ws upgrade's fd to Rust (dup → adopt →
// 101 → head replay → echo), then runs two clients:
//   T1 — a stock `ws` client: proves dup/detach/adopt/handshake/echo.
//   T2 — a raw client that writes its FIRST WS frame COALESCED with the upgrade
//        request (single TCP write): proves §8.3 head-byte replay (the frame
//        lands in Node's `head` and must still be echoed).
//
//   node run.mjs
import http from 'node:http';
import net from 'node:net';
import crypto from 'node:crypto';
import { once } from 'node:events';
import { createRequire } from 'node:module';
import WebSocket from 'ws';

const require = createRequire(import.meta.url);
const addon = require('./attach.node');

const server = http.createServer((_req, res) => {
  res.writeHead(426);
  res.end('upgrade required');
});

let attachErrors = 0;
server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/ws') {
    socket.destroy();
    return;
  }
  // §8.1: pause libuv reads, read the fd, hand the dup to Rust, THEN detach.
  socket.pause();
  const handle = socket._handle;
  const fd = handle && typeof handle.fd === 'number' ? handle.fd : -1;
  if (fd < 0) {
    attachErrors++;
    console.error('FAIL: socket._handle.fd unavailable on this Node — attach needs it');
    socket.destroy();
    return;
  }
  const key = req.headers['sec-websocket-key'];
  try {
    addon.adoptAndEcho(fd, key, head); // Rust dup()s synchronously before returning
  } catch (e) {
    attachErrors++;
    console.error('FAIL: adoptAndEcho threw:', e.message);
    socket.destroy();
    return;
  }
  // Detach Node WITHOUT closing the connection: destroy() closes Node's fd; the
  // Rust dup keeps the TCP connection alive.
  socket.destroy();
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// A masked client text frame (RFC 6455 §5 — client frames MUST be masked).
function maskedTextFrame(text) {
  const payload = Buffer.from(text, 'utf8');
  const mask = crypto.randomBytes(4);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
  // FIN=1, opcode=1 (text); MASK=1, len (assume <126 for the spike)
  const header = Buffer.from([0x81, 0x80 | payload.length]);
  return Buffer.concat([header, mask, masked]);
}

// Parse a single unmasked server text frame's payload (spike: len < 126).
function serverTextPayload(buf) {
  // buf[0] = 0x81, buf[1] = len (no mask bit from server)
  const len = buf[1] & 0x7f;
  return buf.slice(2, 2 + len).toString('utf8');
}

async function testWsClient(port) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await once(ws, 'open');
  const got = once(ws, 'message');
  ws.send('hello-via-ws');
  const [msg] = await Promise.race([
    got,
    sleep(3000).then(() => {
      throw new Error('T1 timeout: no echo');
    }),
  ]);
  ws.close();
  const text = msg.toString();
  if (text !== 'hello-via-ws') throw new Error(`T1 wrong echo: ${text}`);
  return 'T1 ws-client echo: PASS';
}

async function testCoalescedFirstFrame(port) {
  // Raw TCP: write the upgrade request AND the first WS frame in ONE write, so
  // the frame is read by Node into `head`. If head replay is broken, the frame
  // is silently lost and we time out.
  const sock = net.connect(port, '127.0.0.1');
  await once(sock, 'connect');
  const key = crypto.randomBytes(16).toString('base64');
  const upgrade =
    `GET /ws HTTP/1.1\r\n` +
    `Host: 127.0.0.1:${port}\r\n` +
    `Upgrade: websocket\r\n` +
    `Connection: Upgrade\r\n` +
    `Sec-WebSocket-Key: ${key}\r\n` +
    `Sec-WebSocket-Version: 13\r\n\r\n`;
  const firstFrame = maskedTextFrame('coalesced-first-frame');
  sock.write(Buffer.concat([Buffer.from(upgrade, 'utf8'), firstFrame])); // ONE write

  // Collect bytes until we've seen the 101 response AND an echoed frame.
  let buf = Buffer.alloc(0);
  const deadline = Date.now() + 3000;
  let echo = null;
  while (Date.now() < deadline) {
    const chunk = await Promise.race([
      once(sock, 'data').then(([c]) => c),
      sleep(200).then(() => null),
    ]);
    if (chunk) buf = Buffer.concat([buf, chunk]);
    const headerEnd = buf.indexOf('\r\n\r\n');
    if (headerEnd >= 0 && buf.length > headerEnd + 4) {
      const framePart = buf.slice(headerEnd + 4);
      if (framePart.length >= 2 && framePart[0] === 0x81) {
        echo = serverTextPayload(framePart);
        break;
      }
    }
  }
  sock.destroy();
  if (echo === null) throw new Error('T2 timeout: coalesced first frame was lost (head replay broken)');
  if (echo !== 'coalesced-first-frame') throw new Error(`T2 wrong echo: ${echo}`);
  const status = buf.slice(0, buf.indexOf('\r\n')).toString();
  if (!status.includes('101')) throw new Error(`T2 missing 101: ${status}`);
  return 'T2 coalesced-head replay echo: PASS';
}

async function main() {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  console.log(`[spike] attached http.Server on :${port}`);

  const results = [];
  results.push(await testWsClient(port));
  results.push(await testCoalescedFirstFrame(port));

  // A connection must survive the handoff long enough to prove liveness.
  results.forEach((r) => console.log('  ' + r));

  server.close();
  if (attachErrors > 0) {
    console.log('RESULT: FAIL (attach errors)');
    process.exit(1);
  }
  console.log('RESULT: PASS — fd handoff + head replay proven (Linux, plaintext)');
  process.exit(0);
}

main().catch((e) => {
  console.error('RESULT: FAIL —', e.message);
  server.close();
  process.exit(1);
});
