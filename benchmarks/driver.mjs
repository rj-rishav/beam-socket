// Cross-library benchmark driver.
//
// Boots one server (as a child process, so its RSS is measured in isolation),
// then runs four metrics against it, all timed from THIS process (one clock):
//   1. idle memory per connection   — (server VmRSS with N conns − baseline) / N
//   2. echo round-trip latency      — p50 / p99 / mean over many serial samples
//   3. broadcast fan-out completion — trigger → all N room members received
//   4. echo throughput              — sustained echoes/sec across C connections
//
// Raw-WebSocket libs (ws, uws, beamsocket) share the `ws` client. Socket.IO is
// driven by socket.io-client — the only honest way to measure it. Metrics are
// equivalent (client-observed); the transport under each is that library's own.
//
// Usage: node driver.mjs --lib <ws|uws|socketio|beamsocket> [--out results/x.json]
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { writeFileSync } from 'node:fs';
import net from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket } from 'ws';
import { io as ioClient } from 'socket.io-client';

const args = Object.fromEntries(
  process.argv.slice(2).flatMap((a, i, arr) => (a.startsWith('--') ? [[a.slice(2), arr[i + 1]]] : [])),
);
const LIB = args.lib;
const OUT = args.out;
const isSio = LIB === 'socketio';

// ---- config (sandbox-feasible scale; the pinned box scales these up) ----
const MEM_CONNS = Number(args.memConns ?? 3000);
const FANOUT_SIZES = (args.fanout ?? '1000,3000,5000').split(',').map(Number);
const FANOUT_REPS = 5;
const LAT_CONNS = 50;
const LAT_SAMPLES = 2000;
const TPUT_CONNS = 8;
const TPUT_SECS = 3;
const PAYLOAD64 = Buffer.alloc(64, 0x62);
const GO = Buffer.from('GO');

const SERVER = { ws: 'servers/ws.mjs', uws: 'servers/uws.mjs', socketio: 'servers/socketio.mjs', beamsocket: 'servers/beamsocket.mjs' }[LIB];
const NODE = process.execPath;

function freePort() {
  return new Promise((res, rej) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => res(p));
    });
    s.on('error', rej);
  });
}

function rssKB(pid) {
  try {
    const status = readFileSync(`/proc/${pid}/status`, 'utf8');
    const m = status.match(/VmRSS:\s+(\d+)\s+kB/);
    return m ? Number(m[1]) : null;
  } catch {
    return null;
  }
}

async function bootServer(port) {
  const child = spawn(NODE, [SERVER, String(port)], { cwd: process.cwd(), stdio: ['ignore', 'pipe', 'pipe'] });
  child.stderr.on('data', (d) => process.stderr.write(`[srv] ${d}`));
  await new Promise((res, rej) => {
    const to = setTimeout(() => rej(new Error('server READY timeout')), 15000);
    child.stdout.on('data', (d) => {
      if (d.toString().includes('READY')) {
        clearTimeout(to);
        res();
      }
    });
    child.on('exit', (c) => rej(new Error(`server exited ${c}`)));
  });
  return child;
}

// ---- client abstraction ----
// open(port) -> { onMsg(cb), echo(buf)->Promise, trigger(), close(), waitOpen }
function openRaw(port) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`, { perMessageDeflate: false });
  ws.setMaxListeners(0);
  let msgCb = null;
  ws.on('message', (data) => msgCb && msgCb(data));
  ws.on('error', () => {});
  return {
    ws,
    waitOpen: new Promise((res, rej) => {
      ws.on('open', res);
      ws.on('error', rej);
    }),
    onMsg(cb) { msgCb = cb; },
    echo(buf) {
      return new Promise((res) => {
        ws.once('message', () => res());
        ws.send(buf);
      });
    },
    trigger() { ws.send(GO); },
    close() { try { ws.terminate(); } catch {} },
  };
}

function openSio(port) {
  const sock = ioClient(`ws://127.0.0.1:${port}`, { transports: ['websocket'], reconnection: false });
  let bcastCb = null;
  sock.on('bcast', (d) => bcastCb && bcastCb(d));
  return {
    sock,
    waitOpen: new Promise((res, rej) => {
      sock.on('connect', res);
      sock.on('connect_error', rej);
    }),
    onMsg(cb) { bcastCb = cb; },
    echo(buf) {
      return new Promise((res) => {
        sock.once('echo', () => res());
        sock.emit('echo', buf);
      });
    },
    trigger() { sock.emit('go'); },
    close() { try { sock.disconnect(); } catch {} },
  };
}

const open = isSio ? openSio : openRaw;

async function openMany(port, n, batch = 500) {
  const clients = [];
  for (let i = 0; i < n; i += batch) {
    const chunk = [];
    for (let j = i; j < Math.min(i + batch, n); j++) chunk.push(open(port));
    await Promise.all(chunk.map((c) => c.waitOpen));
    clients.push(...chunk);
  }
  return clients;
}

function pct(sorted, p) {
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

// ---- metrics ----
async function measureMemory(port, pid) {
  const base = rssKB(pid);
  const clients = await openMany(port, MEM_CONNS);
  await sleep(2000); // settle
  const withConns = rssKB(pid);
  const perConn = (withConns - base) / MEM_CONNS;
  clients.forEach((c) => c.close());
  await sleep(500);
  return { conns: MEM_CONNS, baseRssKB: base, loadedRssKB: withConns, bytesPerConn: Math.round(perConn * 1024) };
}

async function measureLatency(port) {
  const clients = await openMany(port, LAT_CONNS);
  const rtts = [];
  const perConn = Math.ceil(LAT_SAMPLES / LAT_CONNS);
  // warmup
  await Promise.all(clients.map((c) => c.echo(PAYLOAD64)));
  await Promise.all(
    clients.map(async (c) => {
      for (let i = 0; i < perConn; i++) {
        const t = process.hrtime.bigint();
        await c.echo(PAYLOAD64);
        rtts.push(Number(process.hrtime.bigint() - t) / 1e6);
      }
    }),
  );
  clients.forEach((c) => c.close());
  await sleep(300);
  rtts.sort((a, b) => a - b);
  return {
    samples: rtts.length,
    p50ms: +pct(rtts, 50).toFixed(3),
    p99ms: +pct(rtts, 99).toFixed(3),
    meanms: +(rtts.reduce((a, b) => a + b, 0) / rtts.length).toFixed(3),
  };
}

async function measureFanout(port) {
  const out = {};
  for (const size of FANOUT_SIZES) {
    // size+1 conns: clients[0] is the dedicated trigger; the other `size` are the
    // counted receivers. Counting only non-trigger sockets makes the metric
    // immune to whether a lib delivers a publish back to its own sender.
    const clients = await openMany(port, size + 1);
    const trigger = clients[0];
    const receivers = clients.slice(1);
    const times = [];
    for (let rep = 0; rep < FANOUT_REPS; rep++) {
      let received = 0;
      let resolve;
      const done = new Promise((res) => { resolve = res; });
      receivers.forEach((c) => c.onMsg(() => { if (++received >= size) resolve(); }));
      const t = process.hrtime.bigint();
      trigger.trigger();
      await done;
      times.push(Number(process.hrtime.bigint() - t) / 1e6);
      await sleep(50);
    }
    clients.forEach((c) => c.close());
    await sleep(500);
    times.sort((a, b) => a - b);
    out[size] = { recipients: size, bestMs: +times[0].toFixed(2), medianMs: +pct(times, 50).toFixed(2) };
  }
  return out;
}

async function measureThroughput(port) {
  const clients = await openMany(port, TPUT_CONNS);
  let echoes = 0;
  let running = true;
  const WINDOW = 20; // in-flight messages per connection
  const loops = clients.map(async (c) => {
    // pipeline: keep WINDOW messages in flight
    let inFlight = 0;
    const pump = () => {
      while (running && inFlight < WINDOW) {
        inFlight++;
        c.echo(PAYLOAD64).then(() => {
          echoes++;
          inFlight--;
          if (running) pump();
        });
      }
    };
    pump();
  });
  await Promise.all(loops);
  await sleep(TPUT_SECS * 1000);
  running = false;
  await sleep(300);
  clients.forEach((c) => c.close());
  return { conns: TPUT_CONNS, seconds: TPUT_SECS, msgsPerSec: Math.round(echoes / TPUT_SECS) };
}

async function main() {
  const port = await freePort();
  const server = await bootServer(port);
  const result = { lib: LIB, node: process.version, ts: new Date().toISOString() };
  try {
    result.throughput = await measureThroughput(port);
    result.latency = await measureLatency(port);
    result.fanout = await measureFanout(port);
    result.memory = await measureMemory(port, server.pid);
  } finally {
    server.kill('SIGKILL');
  }
  const json = JSON.stringify(result, null, 2);
  if (OUT) writeFileSync(OUT, json);
  process.stdout.write(json + '\n');
}

main().then(() => process.exit(0)).catch((e) => {
  process.stderr.write(`DRIVER ERROR: ${e.stack}\n`);
  process.exit(1);
});
