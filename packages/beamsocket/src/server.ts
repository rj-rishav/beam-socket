import { EventEmitter } from 'node:events';

import type { AuthorizeRequest, AuthorizeResult, BeamSocketConfig, CloseOptions, Metrics } from './types.js';
import { RejectCode } from './types.js';
import { BoundedMetaMap } from './correlation.js';
import { demux } from './events.js';
import { decodeSocketId } from './ids.js';
import { loadNative, type NativeConfig, type NativeEngine } from './native.js';
import { Presence } from './presence.js';
import { Target } from './rooms.js';
import { Socket } from './socket.js';

export interface ServerEvents {
  connection: (socket: Socket) => void;
}

type AuthorizeFn = (req: AuthorizeRequest) => AuthorizeResult | Promise<AuthorizeResult>;

/** userId/metadata produced by an accepted `authorize`, awaiting its Opened. */
interface PendingAuth {
  userId?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Bound on the JS-side authorize correlation map. Entries are consumed within
 * a bridge flush of the accept, so this only guards the pathological
 * accepted-but-never-opened case (the connection dropped in the microseconds
 * between). Keeping it bounded is the same Rule 5 discipline as the Rust side.
 *
 * On `PENDING_AUTH_CAP` overflow the oldest entry is evicted (FIFO); eviction
 * costs at most that connection's `socket.metadata` (it opens with `{}`), never
 * its identity, which Rust owns — the `userId` was already bound in the engine
 * and drives `toUser` regardless. Evict-oldest degrades gracefully; rejecting
 * the new entry would instead deny a working feature to a healthy connection to
 * preserve stale junk. Evictions are counted (`metrics().authMetadataEvicted`).
 */
const PENDING_AUTH_CAP = 16384;

function reqKey(hi: number, lo: number): string {
  return `${hi}:${lo}`;
}

function toNativeConfig(config: BeamSocketConfig): NativeConfig {
  // Map trustProxy `false | true | string[]` onto the native mode+cidrs pair.
  const tp = config.trustProxy;
  let trustProxyMode = 'never';
  let trustProxyCidrs: string[] | undefined;
  if (tp === true) {
    trustProxyMode = 'always';
  } else if (Array.isArray(tp)) {
    trustProxyMode = 'cidrs';
    trustProxyCidrs = tp;
  }
  return {
    maxPayloadBytes: config.limits?.maxPayloadBytes,
    highWaterMark: config.backpressure?.highWaterMark,
    backpressurePolicy: config.backpressure?.policy,
    pingIntervalMs: config.keepalive?.pingIntervalMs,
    pongTimeoutMs: config.keepalive?.pongTimeoutMs,
    maxConnectionsPerIp: config.limits?.maxConnectionsPerIp,
    maxRoomsPerConnection: config.limits?.maxRoomsPerConnection,
    trustProxyMode,
    trustProxyCidrs,
    authorizeTimeoutMs: config.authorize?.timeoutMs,
    maxPendingAuthorizations: config.authorize?.maxPending,
  };
}

/**
 * The control plane. Every method here is a thin wrapper over a flat native
 * call; per-message work stays in Rust (Rule 1). Events arrive as one flat
 * Buffer per bridge flush (design C, RFC 0001) and are demultiplexed here to
 * per-socket EventEmitter listeners.
 *
 * API contract: docs/ARCHITECTURE.md §4. Phase map: docs/ENGINEERING.md §3.
 */
export class BeamSocket extends EventEmitter {
  #config: BeamSocketConfig;
  #engine?: NativeEngine;
  #sockets = new Map<string, Socket>();
  #listening = false;
  #authorizeFn?: AuthorizeFn;
  /**
   * request_id → the userId/metadata to attach when its Opened arrives.
   * FIFO-bounded (evict-oldest); see BoundedMetaMap / PENDING_AUTH_CAP.
   */
  #pendingAuth = new BoundedMetaMap<PendingAuth>(PENDING_AUTH_CAP);

  constructor(config: BeamSocketConfig = {}) {
    super();
    this.#config = config;
  }

  /**
   * Register the connection-time auth hook. Runs in JS ONCE per connection at
   * upgrade time (Rule 1: never per message) — the first request/response
   * round-trip across the bridge. Returning `{ accept: true, userId }` binds
   * the connection to a first-class User; `{ accept: false, code }` rejects it
   * with that WebSocket close code (default 1008). Must be called before
   * `listen()`. (Phase 1C)
   */
  authorize(fn: AuthorizeFn): this {
    if (this.#listening) {
      throw new Error('authorize() must be registered before listen()');
    }
    this.#authorizeFn = fn;
    return this;
  }

  override on<E extends keyof ServerEvents>(event: E, handler: ServerEvents[E]): this {
    return super.on(event, handler as (...args: unknown[]) => void);
  }

  /** Single-socket target. Unknown/stale ids no-op at send time. */
  toSocket(socketId: string): Target {
    const parsed = decodeSocketId(socketId);
    return new Target(this.#requireEngine('toSocket'), {
      type: 'socket',
      hi: parsed?.hi ?? 0xffffffff, // guaranteed miss for foreign ids
      lo: parsed?.lo ?? 0xffffffff,
    });
  }

  /** All devices of a user. Fan-out runs entirely in Rust. (Phase 1C) */
  toUser(userId: string): Target {
    return new Target(this.#requireEngine('toUser'), { type: 'user', userId });
  }

  /** Room target; fan-out (with .except()) runs entirely in Rust. */
  toRoom(room: string): Target {
    return new Target(this.#requireEngine('toRoom'), { type: 'room', room });
  }

  /** Every live connection. One FFI call regardless of connection count. */
  broadcast(data: Buffer | string): void {
    new Target(this.#requireEngine('broadcast'), { type: 'all' }).send(data);
  }

  /** Room presence — `[{ id, userId, metadata }]`. (Phase 1D) */
  presence(room: string): Presence {
    return new Presence(this.#requireEngine('presence'), room, (hi, lo) => {
      // Metadata lives on the live Socket; absent (evicted / remote) → {}.
      return this.#sockets.get(socketKey(hi, lo))?.metadata ?? {};
    });
  }

  /** One-FFI-call snapshot of the runtime counters (Phase 1D). */
  metrics(): Metrics {
    const s = this.#requireEngine('metrics').stats();
    return {
      connections: s.connections,
      users: s.users,
      rooms: s.rooms,
      messagesIn: s.messagesIn,
      messagesOut: s.messagesOut,
      bytesIn: s.bytesIn,
      bytesOut: s.bytesOut,
      backpressureDrops: s.backpressureDrops,
      bridgePressure: s.bridgePressure,
      bridgeDropped: s.bridgeDropped,
      admissionRejectedIp: s.admissionRejectedIp,
      authorizeRejected: s.authorizeRejected,
      authorizeTimedOut: s.authorizeTimedOut,
      pendingOverflow: s.pendingOverflow,
      // JS-owned counter (the correlation map lives here, not in Rust).
      authMetadataEvicted: this.#pendingAuth.evicted,
    };
  }

  /**
   * Boot the Rust engine (its own Tokio threads — the Node loop is never
   * blocked) and start accepting. The returned promise resolves once the
   * port is bound.
   */
  async listen(port: number): Promise<number> {
    if (this.#listening) {
      throw new Error('listen() may only be called once per BeamSocket (Phase 1A)');
    }
    const native = loadNative();
    this.#engine = native.BeamEngine.start(
      toNativeConfig(this.#config),
      this.#authorizeFn !== undefined,
      (batch) => this.#onFlush(batch),
    );
    const bound = this.#engine.listen(port);
    this.#listening = true;
    return bound;
  }

  /**
   * Graceful shutdown (Phase 1D): stop accepting (new upgrades get HTTP 503),
   * drain in-flight sockets, force-close stragglers at `timeoutMs` (default
   * 30 s) with 1001, then stop the engine and release the bridge. Resolves once
   * the runtime is down — after which the Node process can exit on its own.
   */
  async close(opts: CloseOptions = {}): Promise<void> {
    const engine = this.#engine;
    if (!engine) return;
    // Prevent re-entrancy and drop references so nothing races the drain.
    this.#engine = undefined;
    this.#sockets.clear();
    this.#pendingAuth.clear();
    await engine.close(opts.timeoutMs ?? 30_000);
  }

  #requireEngine(method: string): NativeEngine {
    if (!this.#engine) {
      throw new Error(`io.${method}() requires a running server — call listen() first`);
    }
    return this.#engine;
  }

  /** One flat Buffer per bridge flush (design C): decode + dispatch. */
  #onFlush(batch: Buffer): void {
    demux(batch, {
      onOpen: (hi, lo, authReq) => {
        const engine = this.#engine;
        if (!engine) return;
        // Attach the userId/metadata the authorize hook produced for this
        // connection (correlated by request_id); absent for accept-all.
        let info: PendingAuth | undefined;
        if (authReq) {
          info = this.#pendingAuth.take(reqKey(authReq.hi, authReq.lo));
        }
        const socket = new Socket(engine, hi, lo, info?.userId, info?.metadata);
        this.#sockets.set(socket.id, socket);
        this.emit('connection', socket);
      },
      onMessage: (hi, lo, payload, isBinary) => {
        // Zero-copy view straight through to the app handler.
        this.#sockets.get(socketKey(hi, lo))?.emit('message', payload, isBinary);
      },
      onClose: (hi, lo, code, reason) => {
        const key = socketKey(hi, lo);
        const socket = this.#sockets.get(key);
        if (socket) {
          this.#sockets.delete(key);
          socket._handleClose(code, reason);
        }
      },
      onAuthorize: (reqHi, reqLo, req) => this.#handleAuthorize(reqHi, reqLo, req),
    });
  }

  /**
   * Run the app's `authorize` hook for one pending upgrade and reply to Rust.
   * ALWAYS resolves exactly once: an accept stashes userId/metadata for the
   * upcoming Opened; a reject sends the code; a thrown/rejected handler sends
   * AUTH_ERROR (1011) — the connection is rejected, never left hanging (Rule 5).
   */
  #handleAuthorize(reqHi: number, reqLo: number, req: AuthorizeRequest): void {
    const engine = this.#engine;
    if (!engine) return;
    const fn = this.#authorizeFn;
    if (!fn) {
      // No hook (shouldn't get an Authorize event, but stay safe): accept-all.
      engine.resolveAuthorize(reqHi, reqLo, true, '', false, 0);
      return;
    }
    Promise.resolve()
      .then(() => fn(req))
      .then((result) => {
        if (result.accept) {
          // FIFO-bounded internally; over-cap evicts the oldest (metadata only).
          this.#pendingAuth.set(reqKey(reqHi, reqLo), {
            userId: result.userId,
            metadata: result.metadata,
          });
          engine.resolveAuthorize(
            reqHi,
            reqLo,
            true,
            result.userId ?? '',
            result.userId !== undefined,
            0,
          );
        } else {
          engine.resolveAuthorize(reqHi, reqLo, false, '', false, result.code ?? RejectCode.UNAUTHORIZED);
        }
      })
      .catch(() => {
        // A throwing/ rejecting hook must reject the connection, not hang it.
        engine.resolveAuthorize(reqHi, reqLo, false, '', false, RejectCode.AUTH_ERROR);
      });
  }
}

function socketKey(hi: number, lo: number): string {
  return `${hi.toString(36)}-${lo.toString(36)}`;
}
