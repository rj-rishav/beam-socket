// Phase 1D prebuild helper: copy the cargo-built addon for a given Rust target
// triple into its per-platform npm/ package dir under the napi filename.
//   node scripts/stage-prebuild.mjs <target-triple>
import { copyFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// target triple → [npm dir, napi node filename, cargo library filename]
export const TARGETS = {
  'x86_64-unknown-linux-gnu': ['linux-x64-gnu', 'beamsocket.linux-x64-gnu.node', 'libbeamsocket_node.so'],
  'x86_64-unknown-linux-musl': ['linux-x64-musl', 'beamsocket.linux-x64-musl.node', 'libbeamsocket_node.so'],
  'aarch64-unknown-linux-gnu': ['linux-arm64-gnu', 'beamsocket.linux-arm64-gnu.node', 'libbeamsocket_node.so'],
  'aarch64-unknown-linux-musl': ['linux-arm64-musl', 'beamsocket.linux-arm64-musl.node', 'libbeamsocket_node.so'],
  'aarch64-apple-darwin': ['darwin-arm64', 'beamsocket.darwin-arm64.node', 'libbeamsocket_node.dylib'],
  'x86_64-pc-windows-msvc': ['win32-x64-msvc', 'beamsocket.win32-x64-msvc.node', 'beamsocket_node.dll'],
};

if (import.meta.url === `file://${process.argv[1]}`) {
  const target = process.argv[2];
  const entry = TARGETS[target];
  if (!entry) {
    console.error(`unknown target ${target}; known: ${Object.keys(TARGETS).join(', ')}`);
    process.exit(1);
  }
  const [dir, nodeName, libName] = entry;
  const pkgRoot = path.dirname(fileURLToPath(new URL('.', import.meta.url)));
  const repoRoot = path.resolve(pkgRoot, '..', '..');
  const targetDir = process.env.CARGO_TARGET_DIR ?? path.join(repoRoot, 'target');
  const src = path.join(targetDir, target, 'release', libName);
  if (!existsSync(src)) {
    console.error(`built library not found at ${src} — run cargo build --target ${target} first`);
    process.exit(1);
  }
  const dest = path.join(pkgRoot, 'npm', dir, nodeName);
  copyFileSync(src, dest);
  console.log(`staged ${src} -> ${dest}`);
}
