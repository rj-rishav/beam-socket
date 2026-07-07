// Phase 1D unit test: the authorize-metadata correlation map is FIFO-bounded.
// Filling it past cap evicts the OLDEST entry (metadata only), counts the
// eviction, and leaves newer entries — and any fresh one — still resolvable.
// Runs against dist/ — build first.
import { test } from 'node:test';
import assert from 'node:assert';

import { BoundedMetaMap } from '../dist/correlation.js';

test('BoundedMetaMap: FIFO evict-oldest over cap, counted; take() consumes once', () => {
  const m = new BoundedMetaMap(3);
  m.set('a', { userId: 'a' });
  m.set('b', { userId: 'b' });
  m.set('c', { userId: 'c' });
  assert.equal(m.size, 3);
  assert.equal(m.evicted, 0);

  // Fourth entry overflows the cap → the oldest ('a') is evicted.
  m.set('d', { userId: 'd' });
  assert.equal(m.size, 3, 'stays at cap');
  assert.equal(m.evicted, 1, 'eviction counted');
  assert.equal(m.take('a'), undefined, 'oldest was evicted (metadata lost)');

  // Newer entries — and a brand-new one — still resolve with their metadata.
  assert.deepEqual(m.take('d'), { userId: 'd' });
  assert.equal(m.size, 2, 'take() consumes');
  assert.equal(m.take('d'), undefined, 'consumed exactly once');

  // Continued overflow keeps evicting oldest and incrementing the counter.
  m.set('e', { userId: 'e' }); // {b,c,e}
  m.set('f', { userId: 'f' }); // over cap → evict 'b'
  assert.equal(m.evicted, 2);
  assert.equal(m.take('b'), undefined);
  assert.deepEqual(m.take('f'), { userId: 'f' });
});
