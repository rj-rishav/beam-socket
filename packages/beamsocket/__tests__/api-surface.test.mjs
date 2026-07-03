// Smoke test: the public surface exists, unimplemented methods fail loudly
// with a phase pointer, and implemented-but-unusable states fail loudly too.
// Runs against dist/ — build first.
import { test } from 'node:test';
import assert from 'node:assert';

test('API surface', async () => {
  const { BeamSocket } = await import('../dist/index.js');
  const io = new BeamSocket({});
  for (const m of ['authorize', 'on', 'toSocket', 'toUser', 'toRoom', 'broadcast', 'metrics']) {
    assert.equal(typeof io[m], 'function', `io.${m} missing`);
  }
  // Phase 1B is implemented: broadcasting before listen() fails loudly…
  assert.throws(() => io.broadcast('x'), /listen\(\)/);
  assert.throws(() => io.toRoom('lobby'), /listen\(\)/);
  // …while future phases still point at their phase.
  assert.throws(() => io.toUser('u1'), /Phase 1C/);
  assert.throws(() => io.metrics(), /Phase 1D/);
});
