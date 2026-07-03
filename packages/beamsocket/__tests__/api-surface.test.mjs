// Smoke test: the public surface exists and unimplemented methods fail
// loudly with a phase pointer (not silently). Runs against dist/ — build first.
import { test } from 'node:test';
import assert from 'node:assert';

test('API surface', async () => {
  const { BeamSocket } = await import('../dist/index.js');
  const io = new BeamSocket({});
  for (const m of ['authorize', 'on', 'toSocket', 'toUser', 'toRoom', 'broadcast', 'metrics']) {
    assert.equal(typeof io[m], 'function', `io.${m} missing`);
  }
  assert.throws(() => io.broadcast('x'), /Phase 1B/);
});
