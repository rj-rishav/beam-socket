/**
 * Presence — Phase 1D.
 *
 * Rust owns the room's `(connectionId, userId)` pairs; **metadata lives in JS**
 * (Phase 1C consequence), so `list()` makes ONE native call for the pairs and
 * joins metadata locally from the live `Socket` objects. A member whose
 * metadata was evicted from the authorize correlation map — or, in Phase 4,
 * whose socket lives on another node — joins as `{}` (see server.ts
 * `PENDING_AUTH_CAP`).
 *
 * Async because Phase 4 turns this into a distributed query.
 */

import { encodeSocketId } from './ids.js';
import type { NativeEngine } from './native.js';
import type { PresenceEntry } from './types.js';

export class Presence {
  #native: NativeEngine;
  #room: string;
  #metadataOf: (idHi: number, idLo: number) => Record<string, unknown>;

  /** @internal Constructed by BeamSocket.presence(). */
  constructor(
    native: NativeEngine,
    room: string,
    metadataOf: (idHi: number, idLo: number) => Record<string, unknown>,
  ) {
    this.#native = native;
    this.#room = room;
    this.#metadataOf = metadataOf;
  }

  async list(): Promise<PresenceEntry[]> {
    // One FFI hop for the whole room; fan-out of the join stays in JS heap.
    return this.#native.presenceList(this.#room).map((row) => ({
      id: encodeSocketId(row.idHi, row.idLo),
      userId: row.hasUserId ? row.userId : undefined,
      metadata: this.#metadataOf(row.idHi, row.idLo),
    }));
  }
}
