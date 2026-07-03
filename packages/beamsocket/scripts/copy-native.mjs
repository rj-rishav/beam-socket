// Copies the cargo-built addon to where the loader expects it
// (native/beamsocket.node). Respects CARGO_TARGET_DIR.
import { copyFileSync, mkdirSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const pkgRoot = path.dirname(fileURLToPath(new URL('.', import.meta.url)));
const repoRoot = path.resolve(pkgRoot, '..', '..');
const targetDir = process.env.CARGO_TARGET_DIR ?? path.join(repoRoot, 'target');
const candidates = [
  path.join(targetDir, 'release', 'libbeamsocket_node.so'), // linux
  path.join(targetDir, 'release', 'libbeamsocket_node.dylib'), // macos
  path.join(targetDir, 'release', 'beamsocket_node.dll'), // windows
];
const src = candidates.find(existsSync);
if (!src) {
  console.error(`no built addon found in ${targetDir}/release — run cargo build first`);
  process.exit(1);
}
const dest = path.join(pkgRoot, 'native', 'beamsocket.node');
mkdirSync(path.dirname(dest), { recursive: true });
copyFileSync(src, dest);
console.log(`copied ${src} -> ${dest}`);
