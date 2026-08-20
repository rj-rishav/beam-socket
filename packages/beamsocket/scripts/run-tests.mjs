// `node --test`'s own file-discovery is not stable across Node versions: a
// bare directory throws MODULE_NOT_FOUND on some (v24.15.0), and a quoted
// "**/*.test.mjs" glob string finds nothing on others (v20, CI's pinned
// version) — neither form is safe to depend on. This enumerates the test
// files ourselves with a long-stable fs API and passes explicit paths, which
// every supported Node version handles identically.
import { readdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const testsDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '__tests__');
const files = readdirSync(testsDir, { recursive: true })
  .filter((f) => f.endsWith('.test.mjs'))
  .map((f) => path.join(testsDir, f))
  .sort();

if (files.length === 0) {
  console.error(`no *.test.mjs files found under ${testsDir}`);
  process.exit(1);
}

const result = spawnSync(process.execPath, ['--test', ...files], { stdio: 'inherit' });
process.exit(result.status ?? 1);
