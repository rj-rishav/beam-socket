/**
 * Public types. Mirrors crates/core/src/config.rs — keep the two in sync.
 * Full API contract: docs/ARCHITECTURE.md §4.
 */

/** Admission limits — enforced in Rust before any JS runs. (Phase 1C) */
export interface Limits {
  maxPayloadBytes?: number;
  /** Requires trustProxy behind a load balancer, or it misfires. */
  maxConnectionsPerIp?: number;
  maxRoomsPerConnection?: number;
}

export type BackpressurePolicy = 'disconnect' | 'drop-newest' | 'drop-oldest';

export interface Backpressure {
  highWaterMark?: number;
  policy?: BackpressurePolicy;
}

/**
 * false (default) | true | CIDR allowlist. Security boundary: bare `true`
 * trusts any peer's X-Forwarded-For and is only safe when the runtime is
 * unreachable except through the proxy. Prefer the CIDR form.
 */
export type TrustProxy = boolean | string[];

/**
 * Server-side keepalive — runs entirely in Rust (Rule 1). The engine pings a
 * connection idle for `pingIntervalMs`; no pong (nor any frame) within
 * `pongTimeoutMs` tears the connection down (close event code 1006).
 */
export interface Keepalive {
  pingIntervalMs?: number;
  pongTimeoutMs?: number;
}

export interface BeamSocketConfig {
  limits?: Limits;
  backpressure?: Backpressure;
  keepalive?: Keepalive;
  trustProxy?: TrustProxy;
}

export interface AuthorizeRequest {
  headers: Record<string, string | string[] | undefined>;
  /** Peer address, or the XFF-derived client IP when trustProxy allows it. */
  ip: string;
  url: string;
}

export type AuthorizeResult =
  | { accept: true; userId?: string; metadata?: Record<string, unknown> }
  | { accept: false; code?: number };

/** Snapshot from lock-free Rust counters. (Phase 1D) */
export interface Metrics {
  connections: number;
  users: number;
  rooms: number;
  messagesIn: number;
  messagesOut: number;
  bytesIn: number;
  bytesOut: number;
  backpressureDrops: number;
  /** Rises when the Rust→JS bridge saturates. Watch this in production. */
  bridgePressure: number;
}

export interface PresenceEntry {
  id: string;
  userId?: string;
  metadata?: Record<string, unknown>;
}

export interface CloseOptions {
  timeoutMs?: number;
}
