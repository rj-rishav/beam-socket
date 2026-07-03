/**
 * Lightweight JS proxy around a connection ID. There is NO per-socket native
 * handle — methods delegate to flat native calls with the ID halves, so JS
 * heap cost per connection is one small object (plus EventEmitter state only
 * if the app subscribes).
 */

import { EventEmitter } from 'node:events';
import { encodeSocketId } from './ids.js';
import type { NativeEngine } from './native.js';

export interface SocketEvents {
  message: (data: Buffer, isBinary: boolean) => void;
  close: (code: number, reason: string) => void;
}

export class Socket extends EventEmitter {
  /** Opaque. Do not parse — the encoding will change in Phase 3. */
  readonly id: string;
  readonly userId?: string;
  readonly metadata: Record<string, unknown>;

  /** @internal */ readonly _idHi: number;
  /** @internal */ readonly _idLo: number;
  #native: NativeEngine;
  #closed = false;

  /** @internal Constructed by the server's demux on ConnectionOpened. */
  constructor(native: NativeEngine, idHi: number, idLo: number) {
    super();
    this.#native = native;
    this._idHi = idHi;
    this._idLo = idLo;
    this.id = encodeSocketId(idHi, idLo);
    this.metadata = {};
  }

  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Queue a frame. Resolving to the send queue is the whole Phase 1 delivery
   * promise (ARCHITECTURE.md §4). One synchronous FFI call; strings go as
   * text frames, Buffers/views as binary.
   */
  send(data: Buffer | string): void {
    if (this.#closed) {
      return; // frame-delivery semantics: sending to a closed socket is a no-op
    }
    if (typeof data === 'string') {
      this.#native.sendText(this._idHi, this._idLo, data);
    } else {
      this.#native.send(this._idHi, this._idLo, data, true);
    }
  }

  /** Join a room (auto-created on first join). One sync FFI call. */
  join(room: string): void {
    if (!this.#closed) {
      this.#native.join(this._idHi, this._idLo, room);
    }
  }

  /** Leave a room (auto-destroyed on last leave). One sync FFI call. */
  leave(room: string): void {
    if (!this.#closed) {
      this.#native.leave(this._idHi, this._idLo, room);
    }
  }

  /** Initiate the close handshake (handled entirely in Rust — Rule 1). */
  close(code = 1000, reason = ''): void {
    if (!this.#closed) {
      this.#native.closeConnection(this._idHi, this._idLo, code, reason);
    }
  }

  override on<E extends keyof SocketEvents>(event: E, handler: SocketEvents[E]): this {
    return super.on(event, handler as (...args: unknown[]) => void);
  }

  /** @internal */
  _handleClose(code: number, reason: string): void {
    this.#closed = true;
    this.emit('close', code, reason);
  }
}
