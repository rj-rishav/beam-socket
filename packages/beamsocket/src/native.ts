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

/**
 * napi maps snake_case fields to camelCase, capitalizing the first letter after
 * a digit boundary: `messages_in_1s` → `messagesIn1S` (note the capital S).
 */
export interface NativeRates {
  messagesIn1S: number;
  messagesIn10S: number;
  messagesOut1S: number;
  messagesOut10S: number;
  bytesIn1S: number;
  bytesIn10S: number;
  bytesOut1S: number;
  bytesOut10S: number;
}

export interface NativeStats {
  connections: number;
  users: number;
  rooms: number;
  messagesIn: number;
  messagesOut: number;
  bytesIn: number;
  bytesOut: number;
  backpressureDrops: number;
  bridgePressure: number;
  bridgeDropped: number;
  admissionRejectedIp: number;
  authorizeRejected: number;
  authorizeTimedOut: number;
  pendingOverflow: number;
  // Phase 2B
  adminDisconnects: number;
  adminRoomCloses: number;
  // Phase 2A
  uptimeMs: number;
  rates: NativeRates | null;
  // 0.2.0 — cluster mesh; `null` when single-node (mirrors the `rates`
  // Option<T> pattern already used here).
  cluster: NativeClusterStats | null;
}

/** One `stats().cluster.peerPressures[]` row — `[nodeId, pressure]` on the TS
 * side; napi objects don't cross as tuples, so this is the wire shape. */
export interface NativePeerPressure {
  nodeId: number;
  pressure: number;
}

/** `stats().cluster` (0.2.0) — mirrors `engine.cluster_summary()` +
 * `engine.cluster_peer_pressures()` (crates/core/src/engine.rs), already
 * computed and waiting on the core side since Phase 3D. */
export interface NativeClusterStats {
  nodeId: number;
  peers: number;
  relayIn: number;
  relayOut: number;
  relayDrops: number;
  peerPressures: NativePeerPressure[];
}

export interface NativeRoomStat {
  room: string;
  members: number;
  messages: number;
  exists: boolean;
}

export interface NativeMemoryUsage {
  connections: number;
  rooms: number;
  users: number;
  estimatedHeapBytes: number;
  mailboxBytesInFlight: number;
  estimated: boolean;
}

export interface NativeMailbox {
  idHi: number;
  idLo: number;
  userId: string;
  hasUserId: boolean;
  depthBytes: number;
  hwmBytes: number;
  hwmPercent: number;
}

export interface NativeBackpressureReport {
  totalDrops: number;
  mailboxes: NativeMailbox[];
}

export interface NativeConfig {
  maxPayloadBytes?: number;
  highWaterMark?: number;
  backpressurePolicy?: string;
  pingIntervalMs?: number;
  pongTimeoutMs?: number;
  // Phase 1C
  maxConnectionsPerIp?: number;
  maxRoomsPerConnection?: number;
  /** "never" | "always" | "cidrs" — the SDK maps `false | true | string[]`. */
  trustProxyMode?: string;
  trustProxyCidrs?: string[];
  authorizeTimeoutMs?: number;
  maxPendingAuthorizations?: number;
  // Phase 2A
  samplerMs?: number;
  // 0.2.0 — cluster mesh (RFC 0004, wired through from Phase 3D's core-only
  // support). Absent = single-node: `to_config` in binding.rs leaves
  // `Config.cluster` as `None`, so this field costs nothing when unused.
  cluster?: NativeClusterConfig;
}

/** Native shape of `ClusterConfig` (types.ts) — `secret` crosses as a UTF-8
 * string; Rust turns it into the HMAC key bytes. */
export interface NativeClusterConfig {
  nodeId: number;
  listen: string;
  seeds: string[];
  secret: string;
  clusterName?: string;
}

/** Send status codes from crates/node/src/binding.rs. */
export const SEND_QUEUED = 0;
export const SEND_BACKPRESSURE = 1;
export const SEND_NOT_FOUND = 2;

/** Membership result codes (crates/node/src/binding.rs). */
export const MEMBERSHIP_CHANGED = 0;
export const MEMBERSHIP_NOOP = 1;
export const MEMBERSHIP_NOT_FOUND = 2;
/** Phase 1C: join refused by `maxRoomsPerConnection`. */
export const MEMBERSHIP_LIMIT_EXCEEDED = 3;

/** Fan-out accounting — informational, frame-delivery semantics. */
export interface NativeFanout {
  attempted: number;
  queued: number;
  backpressured: number;
  missing: number;
}

/** One room-presence row (Phase 1D). Metadata is joined SDK-side. */
export interface NativePresenceEntry {
  idHi: number;
  idLo: number;
  userId: string;
  hasUserId: boolean;
}

export interface NativeEngine {
  listen(port: number): number;
  send(idHi: number, idLo: number, data: Buffer, isBinary: boolean): number;
  sendText(idHi: number, idLo: number, data: string): number;
  closeConnection(idHi: number, idLo: number, code: number, reason: string): boolean;
  connectionCount(): number;
  stats(): NativeStats;
  /** Graceful drain + release the TSFN; resolves when the runtime is down. */
  close(timeoutMs: number): Promise<void>;
  shutdown(): void;
  // Phase 1B — each call is ONE FFI hop; fan-out runs in Rust.
  join(idHi: number, idLo: number, room: string): number;
  leave(idHi: number, idLo: number, room: string): number;
  /**
   * `except`: flat [hi, lo, hi, lo, …] LOCAL id pairs — unchanged since 1B,
   * byte-identical wire shape in single-node builds (the zero-cost proof).
   * `remoteExcept` (0.2.0): flat [node, hi, lo, node, hi, lo, …] triples for
   * excepting a socket that lives on ANOTHER node — empty in single-node
   * calls (the SDK passes a shared empty array, no per-call allocation).
   */
  broadcastRoom(
    room: string,
    data: Buffer,
    isBinary: boolean,
    except: Uint32Array,
    remoteExcept: Uint32Array,
  ): NativeFanout;
  broadcastTextRoom(
    room: string,
    data: string,
    except: Uint32Array,
    remoteExcept: Uint32Array,
  ): NativeFanout;
  broadcastAll(
    data: Buffer,
    isBinary: boolean,
    except: Uint32Array,
    remoteExcept: Uint32Array,
  ): NativeFanout;
  broadcastTextAll(data: string, except: Uint32Array, remoteExcept: Uint32Array): NativeFanout;
  roomCount(): number;
  // Phase 1C — identity + authorize.
  /** JS's reply to an authorize request (reqHi/reqLo = request_id halves). */
  resolveAuthorize(
    reqHi: number,
    reqLo: number,
    accept: boolean,
    userId: string,
    hasUserId: boolean,
    code: number,
  ): void;
  broadcastUser(
    userId: string,
    data: Buffer,
    isBinary: boolean,
    except: Uint32Array,
    remoteExcept: Uint32Array,
  ): NativeFanout;
  broadcastTextUser(
    userId: string,
    data: string,
    except: Uint32Array,
    remoteExcept: Uint32Array,
  ): NativeFanout;
  // 0.2.0 — cluster mesh: `toSocket(id)` when `id` names another node (§4.5).
  // Not on the hot path (cross-node is inherently a relay hop already), so
  // there is no text fast path — the SDK encodes strings to a Buffer first.
  sendNode(node: number, idHi: number, idLo: number, data: Buffer, isBinary: boolean): number;
  // Phase 1D — presence. One FFI call returns the room's (id, userId) pairs.
  presenceList(room: string): NativePresenceEntry[];
  // Phase 2A — observability read surface (one FFI call each).
  topRooms(n: number): NativeRoomStat[];
  roomInfo(room: string): NativeRoomStat;
  memoryUsage(): NativeMemoryUsage;
  backpressureReport(topN: number): NativeBackpressureReport;
  /** Flat [hi0, lo0, hi1, lo1, …] device id pairs. */
  userConnections(userId: string): Uint32Array;
  metricsText(): string;
  // Phase 2B — admin actions (one FFI call each; sweeps run in Rust). Each
  // returns a count and is a safe 0-returning no-op while draining/shut down.
  disconnectSocket(idHi: number, idLo: number, code: number): number;
  disconnectUser(userId: string, code: number): number;
  closeRoom(room: string): number;
  // Phase 1.1 — HTTP attach (Unix only; the SDK throws on Windows before here).
  // `headersFlat` is [k0,v0,k1,v1,…] (e.g. req.rawHeaders).
  attach(
    fd: number,
    remoteAddr: string,
    method: string,
    url: string,
    headersFlat: string[],
    head: Buffer,
  ): NativeAttachResult;
}

/** Result of a native attach (Phase 1.1). */
export interface NativeAttachResult {
  /** true → handoff in progress (Rust owns the connection). */
  accepted: boolean;
  /** HTTP status Rust wrote to the fd on reject (0 when accepted). */
  status: number;
}

export interface NativeModule {
  BeamEngine: {
    /** `hasAuthorize` tells Rust whether to run the authorize round-trip at
     * all — false means accept-all, no JS round-trip (Rule 1). */
    start(cfg: NativeConfig, hasAuthorize: boolean, onFlush: (buf: Buffer) => void): NativeEngine;
  };
}

/** Local candidate .node file paths (env override → local dev build). */
export function nativeCandidates(): string[] {
  const local = fileURLToPath(new URL('../native/beamsocket.node', import.meta.url));
  return process.env.BEAMSOCKET_NATIVE ? [process.env.BEAMSOCKET_NATIVE, local] : [local];
}

/**
 * The napi-rs per-platform prebuild package name(s) for this host (Phase 1D).
 * Resolves to one of the `optionalDependencies` installed by npm for the
 * running platform. Linux returns both libc variants (gnu, then musl) so we
 * don't depend on flaky musl detection — whichever is installed wins.
 */
export function platformPackages(): string[] {
  const { platform, arch } = process;
  if (platform === 'linux') {
    return [`beamsocket-linux-${arch}-gnu`, `beamsocket-linux-${arch}-musl`];
  }
  if (platform === 'darwin') return [`beamsocket-darwin-${arch}`];
  if (platform === 'win32') return [`beamsocket-win32-${arch}-msvc`];
  return [];
}

export function loadNative(): NativeModule {
  // 1. Env override / local dev build (native/beamsocket.node).
  for (const p of nativeCandidates()) {
    if (existsSync(p)) return requireNative(p) as NativeModule;
  }
  // 2. Published per-platform prebuild package (optionalDependencies).
  for (const pkg of platformPackages()) {
    try {
      return requireNative(pkg) as NativeModule;
    } catch {
      // not installed for this platform — try the next candidate
    }
  }
  throw new Error(
    'beamsocket native addon not found. For local development build it with ' +
      '`npm run build:native -w beamsocket` (or set BEAMSOCKET_NATIVE to the .node file). ' +
      `For a published install, the prebuild package for ${process.platform}-${process.arch} ` +
      `(${platformPackages().join(' / ') || 'unsupported platform'}) was not resolvable.`,
  );
}
