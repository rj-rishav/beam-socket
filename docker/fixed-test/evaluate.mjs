// Evaluate a fixed-resource run: previous ("baseline") vs current tree.
//
//   node evaluate.mjs verify <out>   correctness matrix + core micro A/B
//   node evaluate.mjs bench  <out>   end-to-end paired A/B (driver.mjs)
//
// Each mode writes its own JSON (<out>/verify.json, <out>/bench.json) and then
// regenerates <out>/evaluation.md from whatever JSON exists. Exit status is 1
// when the current tree fails a step, a micro cell is missing, or an
// end-to-end metric breaches its regression gate.
import fs from 'node:fs';
import path from 'node:path';

const [mode, out] = process.argv.slice(2);
if (!['verify', 'bench'].includes(mode) || !out) {
  console.error('usage: evaluate.mjs verify|bench <results-dir>');
  process.exit(2);
}

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length === 0 ? null : s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const pct = (x) => (x === null || Number.isNaN(x) ? 'n/a' : `${x >= 0 ? '+' : ''}${x.toFixed(1)}%`);
const read = (p) => fs.readFileSync(p, 'utf8');
const exists = (p) => fs.existsSync(p);

// ---------------------------------------------------------------- verify

function testCounts(log) {
  if (!exists(log)) return null;
  const text = read(log);
  let passed = 0, failed = 0, ignored = 0, seen = false;
  for (const m of text.matchAll(/test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored/g)) {
    passed += +m[1]; failed += +m[2]; ignored += +m[3]; seen = true;
  }
  const js = { pass: /ℹ pass (\d+)/.exec(text), fail: /ℹ fail (\d+)/.exec(text) };
  if (js.pass) { passed += +js.pass[1]; failed += +(js.fail?.[1] ?? 0); seen = true; }
  return seen ? { passed, failed, ignored } : null;
}

function microEval(csvPath) {
  if (!exists(csvPath)) return { error: 'micro-bench CSV missing' };
  const lines = read(csvPath).trim().split(/\r?\n/);
  const head = lines.shift()?.split(',') ?? [];
  const col = Object.fromEntries(head.map((h, i) => [h, i]));
  if (!('ns_per_broadcast' in col)) return { error: 'micro-bench CSV has no data' };
  const groups = new Map();
  for (const line of lines) {
    const c = line.split(',');
    const key = `${c[col.target]}|${c[col.recipients]}|${c[col.exclusions]}|${c[col.pattern]}`;
    const g = groups.get(key) ?? { baseline: [], candidate: [] };
    g[c[col.variant]].push(+c[col.ns_per_broadcast]);
    groups.set(key, g);
  }
  const cells = [];
  for (const [key, g] of groups) {
    const [target, recipients, exclusions, pattern] = key.split('|');
    if (!g.baseline.length || !g.candidate.length) return { error: `incomplete cell ${key}` };
    const b = median(g.baseline), c = median(g.candidate);
    cells.push({ target, recipients: +recipients, exclusions: +exclusions, pattern,
      baselineNs: b, currentNs: c, speedup: b / c, samples: g.baseline.length });
  }
  const indexed = cells.filter((x) => x.exclusions >= 32 && x.recipients >= 64 && x.recipients >= x.exclusions);
  const large = indexed.filter((x) => x.exclusions >= 256);
  const worst = [...cells].sort((a, b) => a.speedup - b.speedup)[0];
  return {
    cells: cells.length,
    largeExclusionMinSpeedup: large.length ? Math.min(...large.map((x) => x.speedup)) : null,
    largeExclusionMaxSpeedup: large.length ? Math.max(...large.map((x) => x.speedup)) : null,
    worstCell: worst,
    // Same gate as the exclusions report: >=2x for large lists, and no cell
    // more than 10% slower.
    gatePass: large.every((x) => x.speedup >= 2) && cells.every((x) => x.speedup >= 1 / 1.1),
    all: cells,
  };
}

function verify() {
  const rows = read(path.join(out, 'verify/status.tsv')).trim().split('\n').slice(1)
    .map((l) => l.split('\t'))
    .map(([tree, step, status, seconds, log]) => ({
      tree, step, status, seconds: +seconds, counts: testCounts(path.join(out, 'verify', log)),
    }));
  const microCsv = path.join(out, 'micro/fanout-exclusions.csv');
  const micro = exists(microCsv) ? microEval(microCsv) : { skipped: 'current tree has no fanout_exclusions bench' };
  const currentFailed = rows.some((r) => r.tree === 'current' && r.status !== 'pass');
  // The micro gate is reported, not enforced: one run cannot show that a
  // >10% cell is *repeatable* (the exclusions report's criterion), and
  // ~100 ns cells move that much on scheduler noise alone.
  const result = { generatedAt: new Date().toISOString(), steps: rows, micro,
    pass: !currentFailed && !micro.error };
  fs.writeFileSync(path.join(out, 'verify.json'), `${JSON.stringify(result, null, 2)}\n`);
  return result.pass;
}

// ----------------------------------------------------------------- bench

// dir: +1 = higher is better, -1 = lower is better. gate: worst acceptable
// paired-median change, in "better" orientation (negative = allowed loss).
const METRICS = [
  { id: 'throughput64', label: 'Echo throughput, 64 B (msg/s)', get: (r) => r.throughput?.msgsPerSec, dir: +1, gate: -10 },
  { id: 'throughput16k', label: 'Echo throughput, 16 KiB (msg/s)', get: (r) => r.throughput16k?.msgsPerSec, dir: +1, gate: -10 },
  { id: 'latencyP50', label: 'Echo latency p50 (ms)', get: (r) => r.latency?.p50ms, dir: -1, gate: -15 },
  { id: 'latencyP99', label: 'Echo latency p99 (ms)', get: (r) => r.latency?.p99ms, dir: -1, gate: -15 },
  { id: 'memory', label: 'Idle memory per connection (bytes)', get: (r) => r.memory?.bytesPerConn, dir: -1, gate: -10 },
];

function bench() {
  const rawDir = path.join(out, 'bench/raw');
  const runs = {};
  for (const f of fs.readdirSync(rawDir).filter((f) => f.endsWith('.json'))) {
    const m = /^(baseline|current|reference)-r(\d+)\.json$/.exec(f);
    if (m) (runs[m[1]] ??= {})[+m[2]] = JSON.parse(read(path.join(rawDir, f)));
  }
  const rounds = Object.keys(runs.current ?? {}).map(Number)
    .filter((r) => runs.baseline?.[r]).sort((a, b) => a - b);
  const fanoutSizes = Object.keys(runs.current?.[rounds[0]]?.fanout ?? {});
  const metrics = [
    ...METRICS,
    ...fanoutSizes.map((n) => ({ id: `fanout${n}`, label: `Room fan-out to ${n} (median ms)`,
      get: (r) => r.fanout?.[n]?.medianMs, dir: -1, gate: -15 })),
  ];

  const results = metrics.filter((m) => rounds.some((r) => typeof m.get(runs.current[r]) === 'number')).map((m) => {
    // Paired per-round change, oriented so positive = current is better.
    const deltas = [], base = [], cur = [];
    for (const r of rounds) {
      const b = m.get(runs.baseline[r]), c = m.get(runs.current[r]);
      if (typeof b !== 'number' || typeof c !== 'number' || b === 0) continue;
      base.push(b); cur.push(c);
      deltas.push(m.dir * (c / b - 1) * 100);
    }
    const ref = Object.values(runs.reference ?? {}).map(m.get).filter((x) => typeof x === 'number');
    const refMed = median(ref);
    // Noise floor: the control library's median absolute deviation from its
    // own median, as a percentage — how much an unchanged program moves here.
    const noise = ref.length >= 3
      ? median(ref.map((x) => Math.abs(x / refMed - 1) * 100)) : null;
    const med = median(deltas);
    const better = deltas.filter((d) => d > 0).length;
    const consistent = deltas.length >= 3 &&
      Math.max(better, deltas.length - better) >= Math.ceil(0.8 * deltas.length);
    const beyondNoise = med !== null && Math.abs(med) > Math.max(2, noise ?? 0);
    // A variant whose OWN rounds spread > 1.5x is multi-modal or heavy-tailed
    // here (observed: BeamSocket echo p50 flips between ~0.7 ms and ~2.7 ms run
    // to run, in both trees). A paired median over such data mostly measures
    // which mode each round happened to land in, so it is not judged.
    const spread = (xs) => (xs.length && Math.min(...xs) > 0 ? Math.max(...xs) / Math.min(...xs) : 1);
    const spreadX = Math.max(spread(base), spread(cur));
    const unstable = spreadX > 1.5;
    let verdict = 'no clear change';
    if (unstable) verdict = `unstable metric (${spreadX.toFixed(1)}× spread within a variant)`;
    else if (med !== null && consistent && beyondNoise) verdict = med > 0 ? 'improved' : 'slower';
    // A gate breach must be repeatable: median beyond the gate AND >= 80% of
    // rounds worse (the same criterion as the micro A/B), on a stable metric.
    const gatePass = !(med !== null && med < m.gate && consistent && better < deltas.length / 2 && !unstable);
    if (!gatePass) verdict = 'REGRESSION (gate)';
    return { id: m.id, label: m.label, rounds: deltas.length, spreadX, unstable,
      baselineMedian: median(base), currentMedian: median(cur), referenceMedian: refMed,
      pairedMedianPct: med, minPct: deltas.length ? Math.min(...deltas) : null,
      maxPct: deltas.length ? Math.max(...deltas) : null, roundsBetter: better,
      noiseFloorPct: noise, gatePct: m.gate, gatePass, verdict };
  });
  const result = { generatedAt: new Date().toISOString(), rounds, metrics: results,
    pass: rounds.length > 0 && results.every((r) => r.gatePass) };
  fs.writeFileSync(path.join(out, 'bench.json'), `${JSON.stringify(result, null, 2)}\n`);
  return result.pass;
}

// ---------------------------------------------------------------- report

const fmt = (x) => (x === null || x === undefined ? 'n/a'
  : Math.abs(x) >= 1000 ? Math.round(x).toLocaleString('en-US') : +x.toFixed(3));

function report() {
  const L = ['# Fixed-resource evaluation: previous vs current', ''];
  for (const [phase, file] of [['verify', 'verify/environment.txt'], ['bench', 'bench/environment.txt']]) {
    if (exists(path.join(out, file))) {
      L.push(`<details><summary>${phase} environment</summary>`, '', '```', read(path.join(out, file)).trim(), '```', '', '</details>', '');
    }
  }
  if (exists(path.join(out, 'verify.json'))) {
    const v = JSON.parse(read(path.join(out, 'verify.json')));
    L.push('## Correctness matrix', '', '| Step | Previous | Current |', '|---|---|---|');
    const steps = [...new Set(v.steps.map((s) => s.step))];
    const cell = (tree, step) => {
      const s = v.steps.find((x) => x.tree === tree && x.step === step);
      if (!s) return '—';
      const c = s.counts ? ` (${s.counts.passed} passed, ${s.counts.failed} failed${s.counts.ignored ? `, ${s.counts.ignored} ignored` : ''})` : '';
      return `${s.status === 'pass' ? '✅' : '❌'} ${s.seconds}s${c}`;
    };
    for (const s of steps) L.push(`| ${s} | ${cell('baseline', s)} | ${cell('current', s)} |`);
    L.push('');
    const m = v.micro;
    L.push('## Core fan-out micro A/B (same process, frozen previous implementation)', '');
    if (m.skipped) L.push(`— skipped: ${m.skipped}`, '');
    else if (m.error) L.push(`❌ ${m.error}`, '');
    else {
      L.push(`- ${m.cells} cells; large-exclusion (E ≥ 256, indexed) speedup **${m.largeExclusionMinSpeedup?.toFixed(2)}×–${m.largeExclusionMaxSpeedup?.toFixed(2)}×**`,
        `- Worst cell: ${m.worstCell.target} ${m.worstCell.recipients}/${m.worstCell.exclusions} ${m.worstCell.pattern} — ${m.worstCell.speedup.toFixed(2)}×`,
        `- Gate (≥ 2× for E ≥ 256, no cell > 10% slower): ${m.gatePass ? '✅ pass' : '❌ fail'}`, '');
    }
  }
  if (exists(path.join(out, 'bench.json'))) {
    const b = JSON.parse(read(path.join(out, 'bench.json')));
    L.push(`## End-to-end A/B (${b.rounds.length} paired rounds, alternating order)`, '',
      'Δ is the per-round paired change, oriented so **positive = current is better**. ',
      'Noise floor = the control library\'s median deviation across rounds in this same container.', '',
      '| Metric | Previous | Current | Paired Δ (median) | Range | Rounds better | Noise floor | Gate | Verdict |',
      '|---|---:|---:|---:|---|---:|---:|---:|---|');
    for (const r of b.metrics) {
      L.push(`| ${r.label} | ${fmt(r.baselineMedian)} | ${fmt(r.currentMedian)} | **${pct(r.pairedMedianPct)}** | ${pct(r.minPct)} … ${pct(r.maxPct)} | ${r.roundsBetter}/${r.rounds} | ${r.noiseFloorPct === null ? 'n/a' : `±${r.noiseFloorPct.toFixed(1)}%`} | ${r.gatePct}% | ${r.gatePass ? '' : '❌ '}${r.verdict} |`);
    }
    L.push('', `Overall end-to-end gate: ${b.pass ? '✅ pass' : '❌ fail'}`, '');
  }
  fs.writeFileSync(path.join(out, 'evaluation.md'), `${L.join('\n')}\n`);
  console.log(L.join('\n'));
}

const ok = mode === 'verify' ? verify() : bench();
report();
process.exit(ok ? 0 : 1);
