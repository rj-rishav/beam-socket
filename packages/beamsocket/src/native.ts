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

export interface NativeEngine {
  listen(port: number): number;
  send(idHi: number, idLo: number, data: Buffer, isBinary: boolean): number;
  sendText(idHi: number, idLo: number, data: string): number;
  closeConnection(idHi: number, idLo: number, code: number, reason: string): boolean;
  connectionCount(): number;
  stats(): NativeStats;
  shutdown(): void;
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
