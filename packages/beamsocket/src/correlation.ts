/**
 * FIFO-bounded correlation store — Phase 1C/1D.
 *
 * Holds the userId/metadata an accepted `authorize` produced, keyed by
 * request_id, until the connection's `Opened` event consumes it. Bounded (Rule
 * 5 on the JS side): on overflow the OLDEST entry is evicted (FIFO). Eviction
 * costs at most that connection's metadata (it opens with `{}`), never its
 * identity — the userId was already bound in Rust and drives `toUser`
 * regardless. Evict-oldest degrades gracefully where reject-new would deny a
 * working feature to a healthy connection to keep stale junk. Evictions are
 * counted for `metrics().authMetadataEvicted`.
 */
export class BoundedMetaMap<V> {
  #map = new Map<string, V>();
  #cap: number;
  #evicted = 0;

  constructor(cap: number) {
    this.#cap = cap;
  }

  /** Insert; evict the oldest entry if now over cap. */
  set(key: string, value: V): void {
    this.#map.set(key, value);
    if (this.#map.size > this.#cap) {
      const oldest = this.#map.keys().next().value;
      if (oldest !== undefined) {
        this.#map.delete(oldest);
        this.#evicted++;
      }
    }
  }

  /** Get-and-remove (an entry is consumed exactly once, on `Opened`). */
  take(key: string): V | undefined {
    const value = this.#map.get(key);
    if (value !== undefined) this.#map.delete(key);
    return value;
  }

  clear(): void {
    this.#map.clear();
  }

  /** Total FIFO evictions (surfaced via metrics().authMetadataEvicted). */
  get evicted(): number {
    return this.#evicted;
  }

  get size(): number {
    return this.#map.size;
  }
}
