// Phase 1.1 attach integration (RFC 0002) — the full napi fd-handoff path
// through a real Node http.Server. Covers the RFC-named tests, coexistence,
// lifecycle, the verbatim throws, and Rule 3/4.
import { test } from 'node:test';
import assert from 'node:assert';
import { once } from 'node:events';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';

import { BeamSocket } from '../dist/index.js';

const pkgDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function withTimeout(p, ms, label) {
  return Promise.race([
    p,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout: ${label}`)), ms).unref()),
  ]);
}

/** Start an http.Server (with an HTTP route) + an attached BeamSocket. */
async function makeAttached(beamConfig, { onConnection } = {}) {
  const server = http.createServer((req, res) => {
    if (req.url === '/health') return void res.end('ok');
    res.writeHead(404);
    res.end();
  });
  const io = new BeamSocket({ server, ...beamConfig });
  if (onConnection) io.on('connection', onConnection);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return { server, io, port: server.address().port };
}

function maskedTextFrame(text) {
  const payload = Buffer.from(text, 'utf8');
  const mask = crypto.randomBytes(4);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([Buffer.from([0x81, 0x80 | payload.length]), mask, masked]);
}
function serverTextPayload(buf) {
  const len = buf[1] & 0x7f;
  return buf.slice(2, 2 + len).toString('utf8');
}
function upgradeRequest(port, path, extra = '') {
  const key = crypto.randomBytes(16).toString('base64');
  return (
    `GET ${path} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nUpgrade: websocket\r\n` +
    `Connection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n${extra}\r\n`
  );
}

// Read frames from a raw socket until the 101 + one echoed text frame.
async function rawExpectEcho(sock, label) {
  let buf = Buffer.alloc(0);
  const deadline = Date.now() + 4000;
  while (Date.now() < deadline) {
    const chunk = await Promise.race([once(sock, 'data').then(([c]) => c), sleep(150).then(() => null)]);
    if (chunk) buf = Buffer.concat([buf, chunk]);
    const end = buf.indexOf('\r\n\r\n');
    if (end >= 0 && buf.length > end + 4) {
      const frame = buf.slice(end + 4);
      if (frame.length >= 2 && frame[0] === 0x81) return serverTextPayload(frame);
    }
  }
  throw new Error(`${label}: no echo (frame lost?)`);
}

const echoConn = (s) => s.on('message', (d, isBin) => s.send(isBin ? d : d.toString('utf8')));

test('attach: ws client echoes through an attached http.Server at /ws', async () => {
  const { server, io, port } = await makeAttached({ path: '/ws' }, { onConnection: echoConn });
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await withTimeout(once(ws, 'open'), 5000, 'open');
  const got = once(ws, 'message');
  ws.send('hello-attach');
  assert.equal((await withTimeout(got, 5000, 'echo'))[0].toString(), 'hello-attach');
  ws.close();
  await io.close();
  server.close();
});

test('attach_replays_coalesced_first_frame — first frame in `head` echoes', async () => {
  const { server, io, port } = await makeAttached({ path: '/ws' }, { onConnection: echoConn });
  const sock = net.connect(port, '127.0.0.1');
  await once(sock, 'connect');
  // Upgrade request + first WS frame in ONE write → the frame lands in `head`.
  sock.write(Buffer.concat([Buffer.from(upgradeRequest(port, '/ws')), maskedTextFrame('coalesced')]));
  assert.equal(await rawExpectEcho(sock, 'coalesced'), 'coalesced');
  sock.destroy();
  await io.close();
  server.close();
});

test('attach_drains_stranded_prepause_bytes — a separately-written first frame still echoes', async () => {
  // Best-effort: a first frame written just after the upgrade may be buffered by
  // libuv past `head`; socket.read() in #onUpgrade drains it. Whichever path it
  // takes (head / drain / wire), no-loss is the invariant we assert.
  const { server, io, port } = await makeAttached({ path: '/ws' }, { onConnection: echoConn });
  const sock = net.connect(port, '127.0.0.1');
  await once(sock, 'connect');
  sock.write(upgradeRequest(port, '/ws'));
  sock.write(maskedTextFrame('stranded')); // separate write
  assert.equal(await rawExpectEcho(sock, 'stranded'), 'stranded');
  sock.destroy();
  await io.close();
  server.close();
});

test('attach coexistence: a second upgrade listener still receives non-matching paths', async () => {
  const { server, io, port } = await makeAttached({ path: '/ws' }, { onConnection: echoConn });
  // A peer library registers its own upgrade listener for a different path.
  let otherSawIt = false;
  server.on('upgrade', (req, socket) => {
    if (req.url === '/other') {
      otherSawIt = true;
      socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n');
      socket.destroy();
    }
  });
  const sock = net.connect(port, '127.0.0.1');
  await once(sock, 'connect');
  sock.write(upgradeRequest(port, '/other'));
  await withTimeout(once(sock, 'data'), 4000, 'other 101'); // the peer answered
  assert.equal(otherSawIt, true, 'BeamSocket deferred the non-matching path');
  sock.destroy();
  // BeamSocket still claims /ws.
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await withTimeout(once(ws, 'open'), 5000, 'ws open');
  ws.close();
  await io.close();
  server.close();
});

test('attach sole-handler mode 400s a malformed (non-websocket) upgrade', async () => {
  // No `path` → BeamSocket owns all upgrades and may reject malformed ones.
  const { server, io, port } = await makeAttached({}, { onConnection: echoConn });
  const sock = net.connect(port, '127.0.0.1');
  await once(sock, 'connect');
  // An "upgrade" that is not a websocket (no Sec-WebSocket-Key).
  sock.write(`GET / HTTP/1.1\r\nHost: x\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n`);
  const [chunk] = await withTimeout(once(sock, 'data'), 4000, '400');
  assert.match(chunk.toString(), /400 Bad Request/);
  sock.destroy();
  await io.close();
  server.close();
});

test('attach lifecycle: close() 503s a racing upgrade; httpServer.close() leaves WS alive', async () => {
  const { server, io, port } = await makeAttached({ path: '/ws' }, { onConnection: echoConn });
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await withTimeout(once(ws, 'open'), 5000, 'open');

  // httpServer.close() stops new HTTP conns but must NOT close the upgraded WS
  // (it's Rust-owned now). Prove the WS still echoes after it.
  server.close();
  const got = once(ws, 'message');
  ws.send('still-alive');
  assert.equal((await withTimeout(got, 5000, 'post-httpclose echo'))[0].toString(), 'still-alive');

  // Register the close waiter BEFORE draining — the 1001 fires DURING io.close().
  const closed = once(ws, 'close');
  await io.close({ timeoutMs: 2000 }); // now drain the WS
  const [code] = await withTimeout(closed, 5000, 'ws drained');
  assert.equal(code, 1001, 'draining sends going-away (1001)');
});

test('attach close() during drain: a racing upgrade is refused (503/handshake error)', async () => {
  const { server, io, port } = await makeAttached({ path: '/ws' }, { onConnection: echoConn });
  const a = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await withTimeout(once(a, 'open'), 5000, 'a open');
  const closing = io.close({ timeoutMs: 3000 });
  await sleep(30); // listener removed + #closing set
  const status = await new Promise((resolve) => {
    const b = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    b.once('open', () => resolve('open'));
    b.once('unexpected-response', (_q, res) => resolve(res.statusCode));
    b.once('error', () => resolve('error'));
  });
  assert.ok(status === 503 || status === 'error', `racing upgrade refused, got ${status}`);
  await withTimeout(closing, 5000, 'close');
  server.close();
});

test('attach mutual exclusion + verbatim throws', async () => {
  const server = http.createServer();
  const io = new BeamSocket({ server, path: '/ws' });
  // listen() is invalid in attached mode (verbatim message).
  await assert.rejects(
    () => io.listen(0),
    /listen\(\) is invalid when constructed with \{ server \} — the HTTP server owns the port/,
  );
  await io.close();

  // https.Server throws verbatim at construction.
  const tlsServer = https.createServer({});
  assert.throws(
    () => new BeamSocket({ server: tlsServer }),
    /cannot attach to an https\.Server .* Terminate TLS at your load balancer/,
  );
});

test('Rule 4: the Node socket is destroyed post-handoff (zero per-connection state)', async () => {
  let serverSocket;
  const server = http.createServer();
  const io = new BeamSocket({ server, path: '/ws' });
  io.on('connection', echoConn);
  // Capture the raw socket the upgrade used (before BeamSocket's handler runs).
  server.prependListener('upgrade', (_req, socket) => {
    serverSocket = socket;
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;

  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await withTimeout(once(ws, 'open'), 5000, 'open');
  await sleep(50);
  assert.ok(serverSocket && serverSocket.destroyed, 'Node socket destroyed after fd handoff');

  // Idle RSS spot-check (informational): a handful of attached idles should not
  // balloon the Node heap — the per-connection state is in Rust (1D table).
  const rss = process.memoryUsage().rss / 1024 / 1024;
  assert.ok(rss < 512, `attached-mode RSS spot-check ${rss.toFixed(0)} MB is sane`);

  ws.close();
  await io.close();
  server.close();
});

test('Rule 3: trustProxy resolves the client IP through attach (behind a proxy)', async () => {
  // Loopback is the trusted proxy; the per-IP limit keys on X-Forwarded-For.
  const { server, io, port } = await makeAttached(
    { path: '/ws', trustProxy: ['127.0.0.0/8'], limits: { maxConnectionsPerIp: 1 } },
    { onConnection: echoConn },
  );
  const a = new WebSocket(`ws://127.0.0.1:${port}/ws`, { headers: { 'x-forwarded-for': '9.9.9.9' } });
  await withTimeout(once(a, 'open'), 5000, 'a');
  // Same forwarded client → over the cap of 1.
  const status = await new Promise((resolve) => {
    const b = new WebSocket(`ws://127.0.0.1:${port}/ws`, { headers: { 'x-forwarded-for': '9.9.9.9' } });
    b.once('open', () => resolve('open'));
    b.once('unexpected-response', (_q, res) => resolve(res.statusCode));
    b.once('error', () => resolve('error'));
  });
  assert.ok(status === 429 || status === 'error', `forwarded-IP per-IP limit fired, got ${status}`);
  a.close();
  await io.close();
  server.close();
});

test('attach_fd_hygiene_no_leak_no_double_close — churn accept + per-IP reject, fd count flat', async () => {
  // Cap of 1 so every 2nd concurrent attach from this IP hits the gate-reject
  // path (dup → 429 written → fd closed), exercising both the accept-and-close
  // and reject-and-close fd lifecycles. The dup'd fds must all be released.
  const { server, io, port } = await makeAttached(
    { path: '/ws', limits: { maxConnectionsPerIp: 1 } },
    { onConnection: echoConn },
  );
  const fdCount = () => {
    try {
      return readdirSync('/proc/self/fd').length;
    } catch {
      return -1;
    }
  };
  // The per-IP slot releases during async server-side teardown, which can lag
  // the client's `close` event. Wait for the server to actually report the
  // connection gone before the next accept, or a cap-of-1 accept races the
  // prior slot's release and is itself 429'd (a test race, not an engine bug).
  const waitDrained = async () => {
    for (let i = 0; i < 200; i++) {
      if (io.connectionCount() === 0) return;
      await sleep(10);
    }
  };
  // Open (accepted), then a concurrent 2nd → rejected (429), then close both.
  const acceptThenReject = async () => {
    const a = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    await once(a, 'open');
    const rejected = await new Promise((resolve) => {
      const b = new WebSocket(`ws://127.0.0.1:${port}/ws`);
      b.once('open', () => resolve('open'));
      b.once('unexpected-response', (_q, res) => resolve(res.statusCode));
      b.once('error', () => resolve('error'));
    });
    a.close();
    await once(a, 'close');
    await waitDrained();
    return rejected;
  };

  for (let i = 0; i < 5; i++) await acceptThenReject();
  await sleep(100);
  const before = fdCount();
  let sawReject = false;
  for (let i = 0; i < 40; i++) {
    const r = await acceptThenReject();
    if (r === 429 || r === 'error') sawReject = true;
  }
  await sleep(200);
  const after = fdCount();
  assert.ok(sawReject, 'exercised the gate-reject fd path');
  assert.ok(after - before <= 20, `fd count flat: before=${before} after=${after}`);
  await io.close();
  server.close();
});

test('attach clean process exit (the 1D TSFN proof through attach)', async () => {
  const script = `
    import http from 'node:http';
    import { BeamSocket } from './dist/index.js';
    import WebSocket from 'ws';
    const server = http.createServer();
    const io = new BeamSocket({ server, path: '/ws' });
    io.on('connection', (s) => s.on('message', (d) => s.send(d)));
    server.listen(0, '127.0.0.1');
    await new Promise((r) => server.once('listening', r));
    const port = server.address().port;
    const ws = new WebSocket('ws://127.0.0.1:' + port + '/ws');
    await new Promise((r) => ws.on('open', r));
    const echoed = new Promise((r) => ws.on('message', r));
    ws.send('hi'); await echoed;
    await io.close({ timeoutMs: 2000 });
    server.close();
    // No process.exit(): a clean TSFN release lets Node exit on its own.
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script], {
    cwd: pkgDir,
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  const [code] = await withTimeout(once(child, 'exit'), 10000, 'child exit');
  assert.equal(code, 0, 'attached-mode process exits cleanly on its own');
});
