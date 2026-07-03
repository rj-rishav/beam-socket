// Phase 1A informational gate (docs/ENGINEERING.md §5): RSS with N idle
// connections, target context <20 KB/conn including kernel buffers.
//
//   node benchmarks/idle-rss.mjs [connections=10000] [wave=1000]
//
// Clients are raw TCP sockets doing a minimal WebSocket upgrade (no `ws`
// client objects — keeps the CLIENT process light so the measurement is
// about the server). Prints baseline RSS, loaded RSS, and delta per conn.
import { spawn } from 'node:child_process';
import net from 'node:net';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';

const N = Number(process.argv[2] ?? 10000);
const WAVE = Number(process.argv[3] ?? 1000);

const serverPath = fileURLToPath(new URL('./idle-rss-server.mjs', import.meta.url));
const child = spawn(process.execPath, ['--expose-gc', serverPath], {
  stdio: ['pipe', 'pipe', 'inherit'],
});
const lines = [];
const waiters = [];
let buf = '';
child.stdout.on('data', (d) => {
  buf += d;
  let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i);
    buf = buf.slice(i + 1);
    const w = waiters.shift();
    if (w) w(line);
    else lines.push(line);
  }
});
function nextLine() {
  const l = lines.shift();
  return l !== undefined ? Promise.resolve(l) : new Promise((r) => waiters.push(r));
}
async function ask(cmd) {
  child.stdin.write(cmd + '\n');
  return nextLine();
}

const portLine = await nextLine();
const port = Number(portLine.split(' ')[1]);
console.log(`server pid=${child.pid} port=${port}`);

async function rss() {
  await ask('gc');
  const l = await ask('rss');
  return Number(l.split(' ')[1]);
}

function upgrade(sock) {
  return new Promise((resolve, reject) => {
    sock.setNoDelay(true);
    let resp = '';
    const onData = (d) => {
      resp += d;
      if (resp.includes('\r\n\r\n')) {
        sock.off('data', onData);
        resp.startsWith('HTTP/1.1 101')
          ? resolve()
          : reject(new Error(`bad upgrade: ${resp.slice(0, 80)}`));
      }
    };
    sock.on('data', onData);
    sock.on('error', reject);
    sock.write(
      'GET / HTTP/1.1\r\n' +
        `Host: 127.0.0.1:${port}\r\n` +
        'Connection: Upgrade\r\n' +
        'Upgrade: websocket\r\n' +
        'Sec-WebSocket-Version: 13\r\n' +
        'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n',
    );
  });
}

const baseline = await rss();
console.log(`baseline RSS: ${(baseline / 1048576).toFixed(1)} MB`);

const sockets = [];
const t0 = Date.now();
for (let done = 0; done < N; done += WAVE) {
  const batch = Math.min(WAVE, N - done);
  const conns = await Promise.all(
    Array.from({ length: batch }, async () => {
      const s = net.connect(port, '127.0.0.1');
      await once(s, 'connect');
      await upgrade(s);
      return s;
    }),
  );
  sockets.push(...conns);
}
console.log(`${sockets.length} idle connections up in ${Date.now() - t0} ms`);

// Let the engine settle, then measure.
await new Promise((r) => setTimeout(r, 1500));
const loaded = await rss();
const perConn = (loaded - baseline) / N;
console.log(`loaded RSS:   ${(loaded / 1048576).toFixed(1)} MB`);
console.log(
  `delta: ${((loaded - baseline) / 1048576).toFixed(1)} MB over ${N} conns = ${(perConn / 1024).toFixed(2)} KB/conn (target context: <20 KB/conn)`,
);

for (const s of sockets) s.destroy();
child.stdin.write('exit\n');
child.kill();
process.exit(0);
