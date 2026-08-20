/**
 * Fluent targeting — Phase 1B for sockets/rooms/broadcast, 1C for users.
 * Every terminal .send() is ONE FFI call — fan-out happens in Rust (Rule 1).
 *
 * 0.2.0 (cluster mesh): a target/except may name a socket on ANOTHER node.
 * `#selfNode` (this server's own `cluster.nodeId`, `undefined` when
 * single-node) is how `.except()` and `toSocket()` tell "local" from
 * "remote" — a local except stays on the existing flat [hi,lo,…] array
 * (byte-identical to pre-3D), a remote except goes on a SEPARATE
 * [node,hi,lo,…] array that is empty (and free) whenever nothing remote is
 * excepted, which is always true in single-node mode.
 */

import { decodeSocketId } from './ids.js';
import type { NativeEngine } from './native.js';

const EMPTY_U32 = new Uint32Array(0);

export type TargetKind =
  | { type: 'socket'; node?: number; hi: number; lo: number }
  | { type: 'room'; room: string }
  | { type: 'user'; userId: string }
  | { type: 'all' };

export class Target {
  #native: NativeEngine;
  #kind: TargetKind;
  #selfNode?: number;
  #excluded: number[] = []; // flat [hi, lo, …] LOCAL pairs
  #excludedRemote: number[] = []; // flat [node, hi, lo, …] triples

  /**
   * @internal Constructed by BeamSocket.toSocket/toRoom/toUser/broadcast.
   * `selfNode` is this server's own cluster node id (undefined = single-node
   * or clustering not configured) — how except()/toSocket() classify a
   * decoded id as local vs. remote.
   */
  constructor(native: NativeEngine, kind: TargetKind, selfNode?: number) {
    this.#native = native;
    this.#kind = kind;
    this.#selfNode = selfNode;
  }

  except(socketId: string): this {
    const parsed = decodeSocketId(socketId);
    if (!parsed) {
      return this; // unparseable ids can't identify a live socket — nothing to exclude
    }
    if (parsed.node !== undefined && parsed.node !== this.#selfNode) {
      this.#excludedRemote.push(parsed.node, parsed.hi, parsed.lo);
    } else {
      this.#excluded.push(parsed.hi, parsed.lo);
    }
    return this;
  }

  /** Queue the frame(s). Strings go as text, Buffers as binary. */
  send(data: Buffer | string): void {
    const k = this.#kind;
    switch (k.type) {
      case 'socket': {
        const isRemote = k.node !== undefined && k.node !== this.#selfNode;
        // except() on a single-socket target: an excluded self means no send.
        if (isRemote) {
          for (let i = 0; i < this.#excludedRemote.length; i += 3) {
            if (
              this.#excludedRemote[i] === k.node &&
              this.#excludedRemote[i + 1] === k.hi &&
              this.#excludedRemote[i + 2] === k.lo
            ) {
              return;
            }
          }
          const buf = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
          this.#native.sendNode(k.node as number, k.hi, k.lo, buf, typeof data !== 'string');
          return;
        }
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
        const remoteExcept = this.#excludedRemote.length
          ? Uint32Array.from(this.#excludedRemote)
          : EMPTY_U32;
        if (typeof data === 'string') {
          this.#native.broadcastTextRoom(k.room, data, except, remoteExcept);
        } else {
          this.#native.broadcastRoom(k.room, data, true, except, remoteExcept);
        }
        return;
      }
      case 'user': {
        const except = Uint32Array.from(this.#excluded);
        const remoteExcept = this.#excludedRemote.length
          ? Uint32Array.from(this.#excludedRemote)
          : EMPTY_U32;
        if (typeof data === 'string') {
          this.#native.broadcastTextUser(k.userId, data, except, remoteExcept);
        } else {
          this.#native.broadcastUser(k.userId, data, true, except, remoteExcept);
        }
        return;
      }
      case 'all': {
        const except = Uint32Array.from(this.#excluded);
        const remoteExcept = this.#excludedRemote.length
          ? Uint32Array.from(this.#excludedRemote)
          : EMPTY_U32;
        if (typeof data === 'string') {
          this.#native.broadcastTextAll(data, except, remoteExcept);
        } else {
          this.#native.broadcastAll(data, true, except, remoteExcept);
        }
        return;
      }
    }
  }
}
