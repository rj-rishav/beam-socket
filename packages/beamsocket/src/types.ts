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

/**
 * The `authorize` hook's Rust-side safety rails (Phase 1C). The hook itself is
 * registered with `io.authorize(fn)`; these bound its cost.
 */
export interface AuthorizeConfig {
  /**
   * How long to wait for the `authorize` promise to settle before the
   * connection is rejected-and-cleaned (never left hanging). Default 10000 ms.
   */
  timeoutMs?: number;
  /**
   * Upper bound on concurrently-pending authorizations (Rule 5). Overflow →
   * reject at the door; unauthenticated handshakes are a DoS surface. Default
   * 8192.
   */
  maxPending?: number;
}

export interface BeamSocketConfig {
  limits?: Limits;
  backpressure?: Backpressure;
  keepalive?: Keepalive;
  trustProxy?: TrustProxy;
  authorize?: AuthorizeConfig;
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

/**
 * Every close code / HTTP status a client can receive from admission control
 * and `authorize` (Phase 1C — DoD requires each to be named in the types).
 *
 * - `HTTP_TOO_MANY_REQUESTS` is an HTTP handshake status: `maxConnectionsPerIp`
 *   rejects at the upgrade, BEFORE a WebSocket exists, so the client sees a
 *   failed handshake (HTTP 429), not a close frame.
 * - The rest are WebSocket close codes delivered on the (already-upgraded)
 *   socket's `close` event.
 *
 * (Existing engine close codes reused elsewhere: 1006 keepalive/abnormal
 * teardown, 1009 payload over `maxPayloadBytes`, 1013 backpressure disconnect.)
 */
export const RejectCode = {
  /** `maxConnectionsPerIp` hit at the HTTP upgrade — HTTP 429, no WebSocket. */
  TOO_MANY_CONNECTIONS_PER_IP: 429,
  /** New upgrade during a graceful `close()` drain — HTTP 503, no WebSocket. */
  SERVICE_UNAVAILABLE: 503,
  /** `authorize` returned `{ accept: false }` with no `code`: default policy
   * rejection (WebSocket close 1008). Apps may return any code, including the
   * 4000–4999 application range (e.g. 4401 unauthenticated, 4403 forbidden). */
  UNAUTHORIZED: 1008,
  /** `authorize` timed out or the pending-upgrade table was full: transient,
   * retry (WebSocket close 1013). */
  AUTH_UNAVAILABLE: 1013,
  /** The `authorize` handler threw: the SDK rejects with WebSocket close 1011
   * (internal error) rather than hang the connection. */
  AUTH_ERROR: 1011,
} as const;

export type RejectCode = (typeof RejectCode)[keyof typeof RejectCode];

/**
 * One-FFI-call snapshot from lock-free Rust atomics (Phase 1D). Every field is
 * named here — there are no undocumented counters. Per-connection metrics cost
 * is ~zero: these are process-global atomics, not per-connection state.
 */
export interface Metrics {
  /** Live connections (gauge). */
  connections: number;
  /** Distinct users with ≥1 live connection (gauge). */
  users: number;
  /** Live rooms (gauge; empty rooms auto-destroy). */
  rooms: number;
  messagesIn: number;
  messagesOut: number;
  bytesIn: number;
  bytesOut: number;
  /** Frames dropped / connections cut by a send-queue backpressure policy. */
  backpressureDrops: number;
  /**
   * Rust→JS bridge saturation: in-flight depth ÷ capacity of the bounded
   * engine→bridge queue (0..1). Rises when the JS consumer falls behind — watch
   * this in production; sustained highs precede `bridgeDropped` climbing.
   */
  bridgePressure: number;
  /** `message` events shed at the bounded engine→bridge queue (drop-newest). */
  bridgeDropped: number;
  /** Handshakes rejected by `maxConnectionsPerIp` (HTTP 429). */
  admissionRejectedIp: number;
  /** Connections closed by `authorize` returning `{ accept: false }`. */
  authorizeRejected: number;
  /** Connections closed because `authorize` never settled within its timeout. */
  authorizeTimedOut: number;
  /** Handshakes shed because the bounded pending-upgrade table was full. */
  pendingOverflow: number;
  /**
   * FIFO evictions from the JS-side authorize-metadata correlation map
   * (server.ts `PENDING_AUTH_CAP`). A nonzero, growing value means some
   * accepted connections opened with `{}` metadata; identity is unaffected.
   */
  authMetadataEvicted: number;
}

export interface PresenceEntry {
  id: string;
  userId?: string;
  metadata?: Record<string, unknown>;
}

export interface CloseOptions {
  timeoutMs?: number;
}
