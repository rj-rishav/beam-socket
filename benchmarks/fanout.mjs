// Fan-out + density suite (ENGINEERING.md §6).
//
//   node benchmarks/fanout.mjs --server beamsocket|ws|uws|socketio \
//        [--members 10000] [--workers 4] [--rounds 5] [--payload 512] \
//        [--out benchmarks/results/fanout.jsonl]
//
// Measures, per round: wall time from the broadcast command until EVERY
// member has received the tagged frame (client-observed completion, the
// honest number). Also records server RSS at 0 and N idle connections
// (the density suite datapoint). Appends one JSON line per run.
import { spawn, fork } from 'node:child_process';
import { appendFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const args = Object.fromEntries(
  process.argv.slice(2).map((a, i, all) => (a.startsWith('--') ? [a.slice(2), all[i + 1]] : [])).filter((x) => x.length),
);
const SERVER = args.server ?? 'beamsocket';
const MEMBERS = Number(args.members ?? 10000);
const WORKERS = Number(args.workers ?? 4);
const ROUNDS = Number(args.rounds ?? 5);
const PAYLOAD = Number(args.payload ?? 512);
const OUT = args.out ?? fileURLToPath(new URL('./results/fanout.jsonl', import.meta.url));

const here = path.dirname(fileURLToPath(import.meta.url));
const serverPath = path.join(here, 'servers', `${SERVER}-server.mjs`);
const proto = SERVER === 'socketio' ? 'sio' : 'ws';

// ---- server child ----------------------------------------------------------
const server = spawn(process.execPath, ['--expose-gc', serverPath], {
  stdio: ['pipe', 'pipe', 'inherit'],
});
const pending = [];
let sbuf = '';
server.stdout.on('data', (d) => {
  sbuf += d;
  let i;
  while ((i = sbuf.indexOf('\n')) >= 0) {
    const line = sbuf.slice(0, i);
    sbuf = sbuf.slice(i + 1);
    pending.shift()?.(line);
  }
});
const serverLine = () => new Promise((r) => pending.push(r));
async function ask(cmd) {
  const p = serverLine();
  server.stdin.write(cmd + '\n');
  return p;
}

const port = Number((await serverLine()).split(' ')[1]);
async function rss() {
  return Number((await ask('rss')).split(' ')[1]);
}
const rssBase = await rss();

// ---- client workers ---------------------------------------------------------
const per = Math.ceil(MEMBERS / WORKERS);
const counts = Array.from({ length: WORKERS }, (_, i) =>
  Math.max(0, Math.min(per, MEMBERS - i * per)),
).filter((c) => c > 0);
const url = proto === 'sio' ? `http://127.0.0.1:${port}` : `ws://127.0.0.1:${port}/`;
const t0conn = Date.now();
const workers = counts.map((c) =>
  fork(path.join(here, 'client-worker.mjs'), [url, proto, String(c)], {
    execPath: process.execPath,
  }),
);
function eachWorker(pred) {
  return Promise.all(
    workers.map(
      (w) =>
        new Promise((resolve) => {
          const h = (m) => {
            const v = pred(m);
            if (v !== undefined) {
              w.off('message', h);
              resolve(v);
            }
          };
          w.on('message', h);
        }),
    ),
  );
}

await eachWorker((m) => (m.ready !== undefined ? m.ready : undefined));
const connectMs = Date.now() - t0conn;
await new Promise((r) => setTimeout(r, 1000)); // settle
const rssLoaded = await rss();

// ---- rounds ------------------------------------------------------------------
const rounds = [];
for (let seq = 1; seq <= ROUNDS; seq++) {
  const armed = eachWorker((m) => (m.expecting === seq ? true : undefined));
  for (const w of workers) w.send({ expect: seq });
  await armed;
  const done = eachWorker((m) => (m.done === seq ? m.t : undefined));
  const t0 = Date.now();
  await ask(`bcast ${seq} ${PAYLOAD}`); // SENT ack = server-side call issued
  const times = await done;
  rounds.push(Math.max(...times) - t0);
  await new Promise((r) => setTimeout(r, 150));
}

const result = {
  server: SERVER,
  node: process.version,
  members: MEMBERS,
  payload: PAYLOAD,
  connectMs,
  rssBaseMB: +(rssBase / 1048576).toFixed(1),
  rssLoadedMB: +(rssLoaded / 1048576).toFixed(1),
  rssPerConnKB: +((rssLoaded - rssBase) / MEMBERS / 1024).toFixed(2),
  fanoutMs: rounds,
  fanoutBestMs: Math.min(...rounds),
  fanoutMedianMs: rounds.slice().sort((a, b) => a - b)[rounds.length >> 1],
};
mkdirSync(path.dirname(OUT), { recursive: true });
appendFileSync(OUT, JSON.stringify(result) + '\n');
console.log(JSON.stringify(result, null, 2));

for (const w of workers) w.kill();
server.stdin.write('exit\n');
server.kill();
process.exit(0);
