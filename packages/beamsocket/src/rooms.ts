/**
 * Fluent targeting — Phase 1B for sockets/rooms/broadcast, 1C for users.
 * Every terminal .send() is ONE FFI call — fan-out happens in Rust (Rule 1).
 */

import { decodeSocketId } from './ids.js';
import type { NativeEngine } from './native.js';

export type TargetKind =
  | { type: 'socket'; hi: number; lo: number }
  | { type: 'room'; room: string }
  | { type: 'user'; userId: string }
  | { type: 'all' };

export class Target {
  #native: NativeEngine;
  #kind: TargetKind;
  #excluded: number[] = []; // flat [hi, lo, …] pairs

  /** @internal Constructed by BeamSocket.toSocket/toRoom/broadcast. */
  constructor(native: NativeEngine, kind: TargetKind) {
    this.#native = native;
    this.#kind = kind;
  }

  except(socketId: string): this {
    const parsed = decodeSocketId(socketId);
    if (parsed) {
      this.#excluded.push(parsed.hi, parsed.lo);
    }
    // Unparseable ids can't identify a live socket — nothing to exclude.
    return this;
  }

  /** Queue the frame(s). Strings go as text, Buffers as binary. */
  send(data: Buffer | string): void {
    const k = this.#kind;
    switch (k.type) {
      case 'socket': {
        // except() on a single-socket target: an excluded self means no send.
        for (let i = 0; i < this.#excluded.length; i += 2) {
          if (this.#excluded[i] === k.hi && this.#excluded[i + 1] === k.lo) {
            return;
          }
        }
        if (typeof data === 'string') {
          this.#native.sendText(k.hi, k.lo, data);
        } else {
          this.#native.send(k.hi, k.lo, data, true);
        }
        return;
      }
      case 'room': {
        const except = Uint32Array.from(this.#excluded);
        if (typeof data === 'string') {
          this.#native.broadcastTextRoom(k.room, data, except);
        } else {
          this.#native.broadcastRoom(k.room, data, true, except);
        }
        return;
      }
      case 'user': {
        const except = Uint32Array.from(this.#excluded);
        if (typeof data === 'string') {
          this.#native.broadcastTextUser(k.userId, data, except);
        } else {
          this.#native.broadcastUser(k.userId, data, true, except);
        }
        return;
      }
      case 'all': {
        const except = Uint32Array.from(this.#excluded);
        if (typeof data === 'string') {
          this.#native.broadcastTextAll(data, except);
        } else {
          this.#native.broadcastAll(data, true, except);
        }
        return;
      }
    }
  }
}
