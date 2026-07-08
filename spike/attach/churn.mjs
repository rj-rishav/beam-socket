// RFC 0002 attach spike — fd-hygiene churn (THROWAWAY). Open/echo/close N
// connections through the attached server; the dup'd fds must be released on
// close so the process fd count stays flat (no leak — §9). Not a double-close
// proof (that needs the RAII guard the RFC specifies), just a leak sanity check.
//   node churn.mjs [N]
import http from 'node:http';
import { once } from 'node:events';
import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import WebSocket from 'ws';

const require = createRequire(import.meta.url);
const addon = require('./attach.node');
const N = Number(process.argv[2] ?? 300);

const server = http.createServer((_q, r) => r.end());
server.on('upgrade', (req, socket, head) => {
  socket.pause();
  const fd = socket._handle?.fd ?? -1;
  if (fd >= 0) addon.adoptAndEcho(fd, req.headers['sec-websocket-key'], head);
  socket.destroy();
});

const fdCount = () => {
  try {
    return readdirSync('/proc/self/fd').length;
  } catch {
    return -1;
  }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function oneConn(port) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await once(ws, 'open');
  const got = once(ws, 'message');
  ws.send('x');
  await got;
  ws.close();
  await once(ws, 'close');
}

async function main() {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;

  // Warm up, then measure fd count across the churn.
  for (let i = 0; i < 20; i++) await oneConn(port);
  await sleep(100);
  const before = fdCount();

  for (let i = 0; i < N; i++) await oneConn(port);
  await sleep(200); // let Rust echo threads finish + drop their streams
  const after = fdCount();

  server.close();
  console.log(`fd count: before=${before} after=${after} (Δ ${after - before}) over ${N} connections`);
  // A per-connection fd leak would grow ~N; allow small slack for async settle.
  if (after - before > 20) {
    console.log('RESULT: FAIL — fd leak suspected');
    process.exit(1);
  }
  console.log('RESULT: PASS — fd count flat across churn (no leak)');
  process.exit(0);
}

main().catch((e) => {
  console.error('RESULT: FAIL —', e.message);
  process.exit(1);
});
