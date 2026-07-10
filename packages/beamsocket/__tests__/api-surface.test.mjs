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
  // Phase 1D targeting/queries also require a running server.
  assert.throws(() => io.metrics(), /listen\(\)/);
  assert.throws(() => io.presence('lobby'), /listen\(\)/);
  // Phase 2B admin verbs exist and fail loudly before listen() (a server that
  // never started is an operator typo, not a drain — server.ts #adminEngine).
  for (const m of ['disconnectSocket', 'disconnectUser', 'closeRoom']) {
    assert.equal(typeof io[m], 'function', `io.${m} missing`);
  }
  assert.throws(() => io.disconnectSocket('x-y'), /listen\(\)/);
  assert.throws(() => io.disconnectUser('u1'), /listen\(\)/);
  assert.throws(() => io.closeRoom('lobby'), /listen\(\)/);
  // Code validation runs BEFORE the engine check (never a partial effect).
  assert.throws(() => io.disconnectSocket('x-y', 1006), RangeError);
  // authorize() is chainable and registered before listen() (Phase 1C).
  assert.equal(
    io.authorize(() => ({ accept: true })),
    io,
    'authorize() should be chainable',
  );
});
