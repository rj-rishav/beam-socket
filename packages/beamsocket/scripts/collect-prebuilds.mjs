// Phase 1D prebuild helper: after `download-artifact`, gather every staged
// `beamsocket.*.node` from the artifacts tree into its matching npm/ package
// dir, ready for `npm publish`.
//   node scripts/collect-prebuilds.mjs <artifacts-dir>
import { copyFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TARGETS } from './stage-prebuild.mjs';

// napi node filename → destination npm/ dir.
const FILE_TO_DIR = new Map(Object.values(TARGETS).map(([dir, nodeName]) => [nodeName, dir]));

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const p = path.join(dir, name);
    if (statSync(p).isDirectory()) out.push(...walk(p));
    else if (/^beamsocket\..*\.node$/.test(name)) out.push(p);
  }
  return out;
}

const artifactsDir = process.argv[2];
if (!artifactsDir) {
  console.error('usage: collect-prebuilds.mjs <artifacts-dir>');
  process.exit(1);
}
const pkgRoot = path.dirname(fileURLToPath(new URL('.', import.meta.url)));
let count = 0;
for (const src of walk(artifactsDir)) {
  const dir = FILE_TO_DIR.get(path.basename(src));
  if (!dir) continue;
  const dest = path.join(pkgRoot, 'npm', dir, path.basename(src));
  copyFileSync(src, dest);
  console.log(`collected ${src} -> ${dest}`);
  count++;
}
if (count === 0) {
  console.error(`no beamsocket.*.node artifacts found under ${artifactsDir}`);
  process.exit(1);
}
