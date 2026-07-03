/**
 * Native binding loader — Phase 1A.
 * Resolution order: BEAMSOCKET_NATIVE env override → local build at
 * native/beamsocket.node (put there by `npm run build:native`). The napi-rs
 * per-platform optionalDependencies layout under npm/ lands with the Phase 1D
 * prebuilds.
 */

import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const requireNative = createRequire(import.meta.url);

export interface NativeStats {
  connections: number;
  messagesIn: number;
  messagesOut: number;
  bytesIn: number;
  bytesOut: number;
  backpressureDrops: number;
  bridgeDropped: number;
}

export interface NativeConfig {
  maxPayloadBytes?: number;
  highWaterMark?: number;
  backpressurePolicy?: string;
  pingIntervalMs?: number;
  pongTimeoutMs?: number;
}

/** Send status codes from crates/node/src/binding.rs. */
export const SEND_QUEUED = 0;
export const SEND_BACKPRESSURE = 1;
export const SEND_NOT_FOUND = 2;

/** Membership result codes (crates/node/src/binding.rs). */
export const MEMBERSHIP_CHANGED = 0;
export const MEMBERSHIP_NOOP = 1;
export const MEMBERSHIP_NOT_FOUND = 2;

/** Fan-out accounting — informational, frame-delivery semantics. */
export interface NativeFanout {
  attempted: number;
  queued: number;
  backpressured: number;
  missing: number;
}

export interface NativeEngine {
  listen(port: number): number;
  send(idHi: number, idLo: number, data: Buffer, isBinary: boolean): number;
  sendText(idHi: number, idLo: number, data: string): number;
  closeConnection(idHi: number, idLo: number, code: number, reason: string): boolean;
  connectionCount(): number;
  stats(): NativeStats;
  shutdown(): void;
  // Phase 1B — each call is ONE FFI hop; fan-out runs in Rust.
  join(idHi: number, idLo: number, room: string): number;
  leave(idHi: number, idLo: number, room: string): number;
  /** `except`: flat [hi, lo, hi, lo, …] id pairs. */
  broadcastRoom(room: string, data: Buffer, isBinary: boolean, except: Uint32Array): NativeFanout;
  broadcastTextRoom(room: string, data: string, except: Uint32Array): NativeFanout;
  broadcastAll(data: Buffer, isBinary: boolean, except: Uint32Array): NativeFanout;
  broadcastTextAll(data: string, except: Uint32Array): NativeFanout;
  roomCount(): number;
}

export interface NativeModule {
  BeamEngine: {
    start(cfg: NativeConfig, onFlush: (buf: Buffer) => void): NativeEngine;
  };
}

export function nativeCandidates(): string[] {
  const local = fileURLToPath(new URL('../native/beamsocket.node', import.meta.url));
  return process.env.BEAMSOCKET_NATIVE ? [process.env.BEAMSOCKET_NATIVE, local] : [local];
}

export function loadNative(): NativeModule {
  const candidates = nativeCandidates();
  for (const p of candidates) {
    if (existsSync(p)) {
      return requireNative(p) as NativeModule;
    }
  }
  throw new Error(
    `beamsocket native addon not found (looked at: ${candidates.join(', ')}). ` +
      'Build with `cargo build -p beamsocket-node --release --features napi`, then copy ' +
      'target/release/libbeamsocket_node.so to packages/beamsocket/native/beamsocket.node ' +
      '(or set BEAMSOCKET_NATIVE to the .node file).',
  );
}
