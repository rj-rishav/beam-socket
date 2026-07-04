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
  // Phase 1B/1C targeting is implemented: using it before listen() fails loudly…
  assert.throws(() => io.broadcast('x'), /listen\(\)/);
  assert.throws(() => io.toRoom('lobby'), /listen\(\)/);
  assert.throws(() => io.toUser('u1'), /listen\(\)/);
  // authorize() is chainable and registered before listen() (Phase 1C).
  assert.equal(
    io.authorize(() => ({ accept: true })),
    io,
    'authorize() should be chainable',
  );
  // …while future phases still point at their phase.
  assert.throws(() => io.metrics(), /Phase 1D/);
});
