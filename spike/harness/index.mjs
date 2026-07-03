// RFC 0001 harness — load driver + stats collector.
// Usage: see spike/README.md. Spec: RFC 0001 §4 (matrix), §5 (gates).
//
// Latency = Rust enqueue → JS handler entry, hrtime-correlated (README).
// The bridge stamps each event with `rel_ns` (CLOCK_MONOTONIC − run epoch).
// We align Node's hrtime to that frame ONCE per run, then handler-entry time
// is `rustRef + Number(hrtime() - jsRef)` — no per-event FFI.

import { createRequire } from 'node:module';
import { spawnSync } from 'node:child_process';
import { PerformanceObserver } from 'node:perf_hooks';
import { existsSync, mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const require = createRequire(import.meta.url);
const __dirname = dirname(fileURLToPath(import.meta.url));
const SPIKE = dirname(__dirname);
const RESULTS_DIR = join(SPIKE, 'results');

function loadAddon() {
  const candidates = [
    join(__dirname, 'bridge.node'),
    join(SPIKE, 'bridge.node'),
    process.env.BRIDGE_NODE,
  ].filter(Boolean);
  for (const p of candidates) if (existsSync(p)) return require(p);
  throw new Error(
    `bridge.node not found. Build it: cargo build -p bridge-node --release, then copy ` +
      `target/release/libbridge_node.so to spike/harness/bridge.node. Looked in:\n  ${candidates.join('\n  ')}`,
  );
}

// ---- Consumer profiles (RFC 0001 §4) -------------------------------------
// The JSON profile is informational but headlines the results doc — it's what
// real users write.
export const profiles = {
  noop: () => {},
  work10us: () => {
    const end = process.hrtime.bigint() + 10_000n;
    while (process.hrtime.bigint() < end);
  },
  json: (payload) => JSON.stringify({ id: 1, payload: JSON.parse(payload) }),
  pathological: (() => {
    let last = 0;
    return () => {
      const now = Date.now();
      if (now - last >= 100) {
        last = now;
        const end = now + 5;
        while (Date.now() < end); // 5 ms stall every 100 ms
      }
    };
  })(),
};

// ---- Latency reservoir (Algorithm R — unbiased, bounded memory) -----------
const RES_CAP = 1_000_000;
class Reservoir {
  constructor() {
    this.buf = new Float64Array(RES_CAP);
    this.n = 0;
  }
  reset() {
    this.n = 0;
  }
  add(x) {
    if (this.n < RES_CAP) this.buf[this.n] = x;
    else {
      const j = Math.floor(Math.random() * (this.n + 1));
      if (j < RES_CAP) this.buf[j] = x;
    }
    this.n++;
  }
  percentiles() {
    const len = Math.min(this.n, RES_CAP);
    if (len === 0) return { p50: 0, p99: 0, p999: 0, count: 0 };
    const s = this.buf.slice(0, len).sort();
    const at = (q) => s[Math.min(len - 1, Math.floor(q * len))];
    return { p50: at(0.5), p99: at(0.99), p999: at(0.999), count: len };
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- Single run ----------------------------------------------------------
// Returns a metrics object for one (design, rate, payload, profile, …) cell.
async function runCell(opts) {
  const {
    design,
    rate,
    payload,
    profile,
    batch = 256,
    timer = 1,
    external = false,
    queue = 8192,
    warmupMs = 700,
    measureMs = 2500,
    sampleEvery = 1,
  } = opts;

  const { Bridge } = loadAddon();
  const profileFn = profiles[profile];
  if (!profileFn) throw new Error(`unknown profile ${profile}`);

  const res = new Reservoir();
  let recording = false;
  let seen = 0; // for sampleEvery
  let bridge = null;
  let jsRef = 0n;
  let rustRef = 0;
  const relNow = () => rustRef + Number(process.hrtime.bigint() - jsRef);

  const handleOne = (t, payloadBuf, entry) => {
    if (recording) {
      if (sampleEvery === 1 || seen % sampleEvery === 0) res.add(entry - t);
      seen++;
    }
    profileFn(payloadBuf);
  };

  // Window management is a state machine driven ENTIRELY from inside the
  // handler (warmup → measure → done), never by a driver timer. Under
  // saturation the drain keeps the TSFN queue perpetually full, so libuv never
  // leaves the async phase to fire a setTimeout — driver timers (even the
  // warmup sleep) would be starved and never wake. The handler always runs, so
  // it owns the clock: it opens the window after warmup, closes it after the
  // measure window, and on close calls bridge.stop() to halt the feed and let
  // the event loop recover. A last-resort driver timeout covers the degenerate
  // "no events at all" case (loop not starved).
  let phase = 'warmup';
  let startTime = 0;
  let windowEnd = Infinity;
  let cpu0 = null, t0 = 0n, p0 = null;
  let pressureMax = 0;
  let flushSeen = 0;
  let snap = null;
  let resolveWindow;
  const windowDone = new Promise((r) => (resolveWindow = r));

  const openWindow = (now) => {
    res.reset();
    seen = 0;
    gcCount = 0;
    gcPauseMs = 0;
    cpu0 = process.cpuUsage();
    t0 = process.hrtime.bigint();
    p0 = bridge.pressure();
    recording = true;
    windowEnd = now + measureMs;
    phase = 'measure';
  };
  const closeWindow = () => {
    snap = {
      t1: process.hrtime.bigint(),
      cpu1: process.cpuUsage(cpu0),
      p1: bridge.pressure(),
      rssMb: process.memoryUsage().rss / 1048576,
    };
    recording = false;
    phase = 'done';
    bridge.stop(); // stop the feed → event loop's timer phase recovers
    resolveWindow();
  };
  const tick = () => {
    if (phase === 'done') return;
    const now = Date.now();
    if (phase === 'warmup') {
      if (now - startTime >= warmupMs) openWindow(now);
      return;
    }
    // measure
    if ((flushSeen++ & 511) === 0) {
      const pr = bridge.pressure();
      if (pr.bridgePressure > pressureMax) pressureMax = pr.bridgePressure;
    }
    if (now >= windowEnd) closeWindow();
  };

  // Design-specific TSFN callbacks (all unary).
  const cbA = (ev) => {
    const entry = relNow();
    handleOne(ev.t, ev.payload, entry);
    tick();
  };
  const cbB = (arr) => {
    const entry = relNow();
    for (let i = 0; i < arr.length; i++) handleOne(arr[i].t, arr[i].payload, entry);
    tick();
  };
  const cbC = (buf) => {
    const entry = relNow();
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const count = dv.getUint32(0, true);
    let off = 4;
    for (let i = 0; i < count; i++) {
      const t = dv.getFloat64(off + 4, true);
      const len = dv.getUint32(off + 12, true);
      const p = buf.subarray(off + 16, off + 16 + len);
      off += 16 + len;
      handleOne(t, p, entry);
    }
    tick();
  };
  const cb = design === 'A' ? cbA : design === 'B' ? cbB : cbC;

  // GC observer for the window.
  let gcCount = 0;
  let gcPauseMs = 0;
  const gcObs = new PerformanceObserver((list) => {
    if (!recording) return;
    for (const e of list.getEntries()) {
      gcCount++;
      gcPauseMs += e.duration;
    }
  });
  gcObs.observe({ entryTypes: ['gc'] });

  const cfg = {
    design,
    eventsPerSec: rate,
    payloadBytes: payload,
    durationSecs: 0, // JS controls timing via stop()
    queueCapacity: queue,
    batch,
    timerMs: timer,
    external,
  };

  bridge = Bridge.start(cfg, cb);
  // Align clocks immediately after start.
  jsRef = process.hrtime.bigint();
  rustRef = bridge.relNowNs();
  startTime = Date.now();

  // Last-resort close: only reached if the loop is NOT starved yet the handler
  // never fires (e.g. no events produced at all). Under saturation this timer
  // is starved, but then the handler drives the state machine itself.
  const fallback = setTimeout(() => {
    if (phase === 'done') return;
    if (phase === 'warmup') openWindow(Date.now() - measureMs); // window already elapsed
    if (!snap) closeWindow();
  }, warmupMs + measureMs + 3000);
  await windowDone;
  clearTimeout(fallback);
  gcObs.disconnect();

  const t1 = snap.t1;
  const cpu1 = snap.cpu1;
  const p1 = snap.p1;
  const rssMb = snap.rssMb;
  const pf = bridge.pressure();

  const windowSec = Math.max(1e-3, Number(t1 - t0) / 1e9);
  const deliveredWin = Math.max(0, p1.delivered - p0.delivered);
  const sustained = deliveredWin / windowSec;
  const cpuUs = cpu1.user + cpu1.system;
  const cpuMsPer1M = deliveredWin > 0 ? cpuUs / 1000 / (deliveredWin / 1e6) : 0;
  const pct = res.percentiles();
  const dropPct = pf.produced > 0 ? (pf.dropped / pf.produced) * 100 : 0;

  return {
    design,
    rate,
    payload,
    profile,
    batch,
    timer,
    external,
    queue,
    sustainedEventsPerSec: Math.round(sustained),
    p50Ms: pct.p50 / 1e6,
    p99Ms: pct.p99 / 1e6,
    p999Ms: pct.p999 / 1e6,
    latencySamples: pct.count,
    cpuMsPer1M,
    rssMb,
    gcCount,
    gcPauseMs,
    produced: pf.produced,
    dropped: pf.dropped,
    delivered: pf.delivered,
    dropPct,
    bridgePressureMax: pressureMax,
  };
}

// ---- Matrix (RFC 0001 §4) ------------------------------------------------
const PAYLOADS = [64, 512, 4096, 65536];
const RATES = [10_000, 100_000, 500_000, 1_000_000, 2_000_000, 0]; // 0 = ramp-to-failure / ceiling
const PROFILES = ['noop', 'work10us', 'json', 'pathological'];
const DESIGNS = ['A', 'B', 'C'];

function cellId(c) {
  return `${c.design}_p${c.payload}_r${c.rate}_${c.profile}_b${c.batch}_t${c.timer}_${c.external ? 'ext' : 'copy'}`;
}

// Run one cell in a *child process* for memory/GC isolation, return its JSON.
function runCellIsolated(cell) {
  const args = [
    join(__dirname, 'index.mjs'),
    '--single',
    '--design', cell.design,
    '--rate', String(cell.rate),
    '--payload', String(cell.payload),
    '--profile', cell.profile,
    '--batch', String(cell.batch),
    '--timer', String(cell.timer),
    '--queue', String(cell.queue ?? 8192),
    '--warmup', String(cell.warmupMs ?? 400),
    '--measure', String(cell.measureMs ?? 1500),
    ...(cell.external ? ['--external'] : []),
  ];
  const r = spawnSync(process.execPath, args, {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    timeout: 120_000,
  });
  const out = (r.stdout || '') + (r.stderr || '');
  const m = out.match(/__RESULT__(.*)/);
  if (!m) {
    return { ...cell, error: (r.stderr || r.stdout || 'no result').slice(0, 400) };
  }
  return JSON.parse(m[1]);
}

async function runMatrix(budgetSeconds = 0) {
  mkdirSync(RESULTS_DIR, { recursive: true });
  const started = new Date().toISOString();
  const budgetMs = budgetSeconds > 0 ? budgetSeconds * 1000 : Infinity;
  const runStart = Date.now();
  const cells = [];

  // Main matrix: default batch/timer (B/C), copy buffers.
  for (const design of DESIGNS)
    for (const payload of PAYLOADS)
      for (const rate of RATES)
        for (const profile of PROFILES)
          cells.push({ design, payload, rate, profile, batch: 256, timer: 1, external: false });

  // Batch-parameter sweep (RFC §4): B and C at 512 B / noop, ceiling offer.
  for (const design of ['B', 'C'])
    for (const batch of [64, 256, 1024])
      for (const timer of [0.25, 1, 4])
        cells.push({ design, payload: 512, rate: 0, profile: 'noop', batch, timer, external: false, sweep: 'batch' });

  // Copy-vs-external crossover (RFC §2 Q3): B and C, noop, ceiling offer,
  // across payload sizes, both buffer strategies.
  for (const design of ['B', 'C'])
    for (const payload of PAYLOADS)
      for (const external of [false, true])
        cells.push({ design, payload, rate: 0, profile: 'noop', batch: 256, timer: 1, external, sweep: 'crossover' });

  const results = [];
  const total = cells.length;
  const progressFile = join(RESULTS_DIR, 'matrix-progress.json');
  console.error(`Matrix: ${total} cells starting at ${started}`);
  let ran = 0;
  for (let i = 0; i < cells.length; i++) {
    const c = cells[i];
    // Resume support: if this cell already has a valid result file (from a
    // previous run interrupted by a sandbox recycle), reuse it.
    const cellFile = join(RESULTS_DIR, `cell-${cellId(c)}.json`);
    let r;
    if (existsSync(cellFile)) {
      try {
        r = JSON.parse(readFileSync(cellFile, 'utf8'));
        if (r && (r.error || typeof r.sustainedEventsPerSec === 'number')) {
          r.sweep = c.sweep || 'main';
          results.push(r);
          console.error(`[${i + 1}/${total}] ${cellId(c)}  →  (cached)`);
          writeFileSync(progressFile, JSON.stringify({ started, done: i + 1, total, results }, null, 2));
          continue;
        }
      } catch {
        /* fall through and re-run */
      }
    }
    // Time budget: exit cleanly so a follow-up invocation resumes (used to fit
    // inside a short shell window when background processes aren't durable).
    if (ran > 0 && Date.now() - runStart > budgetMs) {
      console.error(`budget ${budgetSeconds}s reached at cell ${i + 1}/${total}; exiting to resume later`);
      return { results, done: false };
    }
    r = runCellIsolated(c);
    ran++;
    r.sweep = c.sweep || 'main';
    results.push(r);
    // Persist per-cell + running progress (crash resilience).
    writeFileSync(join(RESULTS_DIR, `cell-${cellId(c)}.json`), JSON.stringify(r, null, 2));
    writeFileSync(progressFile, JSON.stringify({ started, done: i + 1, total, results }, null, 2));
    const rr = r.error ? `ERROR ${r.error}` :
      `${(r.sustainedEventsPerSec / 1e3).toFixed(0)}k ev/s  p99=${r.p99Ms.toFixed(3)}ms  drop=${r.dropPct.toFixed(1)}%`;
    console.error(`[${i + 1}/${total}] ${cellId(c)}  →  ${rr}`);
  }
  const outFile = join(RESULTS_DIR, 'matrix.json');
  writeFileSync(outFile, JSON.stringify({ started, finished: new Date().toISOString(), results }, null, 2));
  console.error(`Matrix done → ${outFile}`);
  return { results, done: true };
}

// ---- Primary gate (RFC 0001 §5) ------------------------------------------
// Pathological consumer at 2× the design's measured ceiling for N minutes.
// Watch RSS (must stay flat), bridgePressure (must rise, be queryable), drops
// (counted, visible), and recovery after load subsides.
async function runGate(design, ceilingEventsPerSec, gateSeconds) {
  mkdirSync(RESULTS_DIR, { recursive: true });
  const { Bridge } = loadAddon();
  const rate = Math.max(1, Math.round(ceilingEventsPerSec * 2));
  const series = [];
  const seriesFile = join(RESULTS_DIR, `gate-${design}-series.json`);

  // Saturation phase — pathological handler at 2× ceiling.
  const bridge = Bridge.start(
    { design, eventsPerSec: rate, payloadBytes: 512, durationSecs: 0, queueCapacity: 8192, batch: 256, timerMs: 1, external: false },
    (arg) => {
      // Minimal pathological consumer (stall 5 ms every 100 ms). We don't
      // record latency here; the gate is about survival, not speed.
      profiles.pathological();
      void arg;
    },
  );

  const t0 = Date.now();
  let lastLog = 0;
  while (Date.now() - t0 < gateSeconds * 1000) {
    await sleep(1000);
    const pr = bridge.pressure();
    const rssMb = process.memoryUsage().rss / 1048576;
    const point = { tSec: Math.round((Date.now() - t0) / 1000), rssMb, ...pr };
    series.push(point);
    writeFileSync(seriesFile, JSON.stringify({ design, rate, series }, null, 2));
    if (point.tSec - lastLog >= 30) {
      lastLog = point.tSec;
      console.error(
        `gate ${design} t=${point.tSec}s rss=${rssMb.toFixed(0)}MB pressure=${pr.bridgePressure.toFixed(2)} dropped=${(pr.dropped / 1e6).toFixed(1)}M delivered=${(pr.delivered / 1e6).toFixed(1)}M`,
      );
    }
  }
  const satPressure = bridge.pressure();
  const satRss = process.memoryUsage().rss / 1048576;
  bridge.stop();

  // Recovery phase — fresh low-rate no-op run; latency must return to normal.
  await sleep(500);
  const recover = await runCell({
    design,
    rate: Math.min(50_000, Math.max(10_000, Math.round(ceilingEventsPerSec * 0.1))),
    payload: 512,
    profile: 'noop',
    warmupMs: 500,
    measureMs: 2000,
  });

  // RSS flatness: the criterion is "no UNBOUNDED growth", i.e. RSS reaches a
  // plateau. V8 grows its heap in one-time steps under GC pressure, so a whole-
  // run linear slope is misleading; the honest test is whether the FINAL THIRD
  // has flattened out (bounded) versus still climbing (leak).
  const lastThird = series.slice(Math.floor((series.length * 2) / 3));
  const ltSlope = linregSlope(lastThird.map((p) => p.tSec), lastThird.map((p) => p.rssMb));
  const ltMin = Math.min(...lastThird.map((p) => p.rssMb));
  const ltMax = Math.max(...lastThird.map((p) => p.rssMb));
  const rssMin = Math.min(...series.map((p) => p.rssMb));
  const rssMax = Math.max(...series.map((p) => p.rssMb));

  const verdict = {
    design,
    gateSeconds,
    offeredRate: rate,
    ceilingEventsPerSec,
    // Plateaued in the final third: small spread and no upward trend.
    queueBounded: ltMax - ltMin < 12 && ltSlope < 0.25,
    rssFlat: { min: rssMin, max: rssMax, lastThirdMin: ltMin, lastThirdMax: ltMax, lastThirdSlopeMbPerSec: ltSlope },
    // Pressure must rise and be queryable at some point during saturation; the
    // peak over the run is the right statistic (a single end-of-run sample can
    // land in the trough between the pathological consumer's stalls).
    pressureVisible: Math.max(...series.map((p) => p.bridgePressure)) > 0.5,
    pressurePeak: Math.max(...series.map((p) => p.bridgePressure)),
    finalPressure: satPressure.bridgePressure,
    dropsVisible: satPressure.dropped > 0,
    droppedTotal: satPressure.dropped,
    deliveredTotal: satPressure.delivered,
    recoveredP99Ms: recover.p99Ms,
    recovered: recover.p99Ms < 5, // back to normal within a couple seconds
    satPressure,
    satRssMb: satRss,
  };
  verdict.PASS =
    verdict.queueBounded && verdict.pressureVisible && verdict.dropsVisible && verdict.recovered;
  writeFileSync(join(RESULTS_DIR, `gate-${design}.json`), JSON.stringify(verdict, null, 2));
  console.error(`gate ${design}: ${verdict.PASS ? 'PASS' : 'FAIL'} (bounded=${verdict.queueBounded} pressure=${verdict.pressureVisible} drops=${verdict.dropsVisible} recovered=${verdict.recovered})`);
  return verdict;
}

function linregSlope(xs, ys) {
  const n = xs.length;
  if (n < 2) return 0;
  const mx = xs.reduce((a, b) => a + b, 0) / n;
  const my = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) {
    num += (xs[i] - mx) * (ys[i] - my);
    den += (xs[i] - mx) ** 2;
  }
  return den === 0 ? 0 : num / den;
}

// ---- CLI -----------------------------------------------------------------
function parseArgs(argv) {
  const a = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const t = argv[i];
    if (t.startsWith('--')) {
      const k = t.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith('--')) a[k] = true;
      else { a[k] = next; i++; }
    } else a._.push(t);
  }
  return a;
}

async function main() {
  const a = parseArgs(process.argv.slice(2));

  if (a.single) {
    const r = await runCell({
      design: a.design || 'B',
      rate: Number(a.rate ?? 100000),
      payload: Number(a.payload ?? 512),
      profile: a.profile || 'noop',
      batch: Number(a.batch ?? 256),
      timer: Number(a.timer ?? 1),
      external: !!a.external,
      queue: Number(a.queue ?? 8192),
      warmupMs: Number(a.warmup ?? 700),
      measureMs: Number(a.measure ?? 2500),
    });
    // Machine-readable line for the matrix parent; human summary on stderr.
    console.log('__RESULT__' + JSON.stringify(r));
    console.error(
      `${r.design} p=${r.payload} rate=${r.rate} ${r.profile}: ` +
        `${(r.sustainedEventsPerSec / 1e3).toFixed(0)}k ev/s  ` +
        `p50=${r.p50Ms.toFixed(3)} p99=${r.p99Ms.toFixed(3)} p999=${r.p999Ms.toFixed(3)} ms  ` +
        `drop=${r.dropPct.toFixed(1)}%  rss=${r.rssMb.toFixed(0)}MB  gc=${r.gcCount}/${r.gcPauseMs.toFixed(0)}ms`,
    );
    return;
  }

  if (a.matrix) {
    const out = await runMatrix(Number(a.budget ?? 0));
    // Signal completion state to the shell driver via exit code: 0 = fully
    // done, 3 = budget hit, more cells remain (call again to resume).
    process.exit(out.done ? 0 : 3);
  }

  if (a.gate) {
    // Ceiling comes from the matrix (512 B, no-op, design's best sustained).
    const design = a.design || 'B';
    const gateSeconds = Number(a['gate-seconds'] ?? 600);
    let ceiling = a.ceiling ? Number(a.ceiling) : null;
    if (!ceiling) {
      const mp = join(RESULTS_DIR, 'matrix.json');
      if (existsSync(mp)) {
        const m = JSON.parse(readFileSync(mp, 'utf8')).results;
        ceiling = Math.max(
          0,
          ...m.filter((r) => r.design === design && r.payload === 512 && r.profile === 'noop' && !r.external)
            .map((r) => r.sustainedEventsPerSec),
        );
      }
    }
    if (!ceiling) throw new Error('no ceiling available; run --matrix first or pass --ceiling N');
    await runGate(design, ceiling, gateSeconds);
    return;
  }

  // Default: single cell from flags (interactive smoke test).
  const r = await runCell({
    design: a.design || 'B',
    rate: Number(a.rate ?? 100000),
    payload: Number(a.payload ?? 512),
    profile: a.profile || 'noop',
    external: !!a.external,
  });
  console.error(JSON.stringify(r, null, 2));
}

main().catch((e) => {
  console.error(e.stack || String(e));
  process.exit(1);
});
