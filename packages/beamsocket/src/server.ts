import { EventEmitter } from 'node:events';

import type {
  AdminCloseRoomResult,
  AdminDisconnectResult,
  AuthorizeRequest,
  AuthorizeResult,
  BackpressureReport,
  BeamSocketConfig,
  CloseOptions,
  MemoryUsage,
  Metrics,
  RoomHandle,
  RoomStat,
  Stats,
  UserHandle,
} from './types.js';
import { AdminCloseCode, RejectCode } from './types.js';
import { BoundedMetaMap } from './correlation.js';
import { demux } from './events.js';
import { decodeSocketId, encodeSocketId } from './ids.js';
import { loadNative, type NativeConfig, type NativeEngine, type NativeStats } from './native.js';
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

/** Verbatim throw messages (RFC 0002 §6/§7/§10.1) — tests assert these exactly. */
const HTTPS_ATTACH_MSG =
  'BeamSocket cannot attach to an https.Server (Node owns the decrypted stream; the raw fd is ciphertext). Terminate TLS at your load balancer and attach to a plaintext http.Server, or run BeamSocket on its own TLS port (engine-side TLS is RFC 0003).';
const WINDOWS_ATTACH_MSG =
  'BeamSocket cannot attach to an HTTP server on Windows yet (fd handoff needs WSADuplicateSocket, not shipped in 1.1). Run BeamSocket on its own port with listen() alongside your HTTP server, behind the same load balancer.';
const LISTEN_MUTEX_MSG =
  'listen() is invalid when constructed with { server } — the HTTP server owns the port';

/**
 * Validate `cluster` config at construction (0.2.0) — fail loudly before any
 * FFI call, same discipline as `Config::validate` on the Rust side
 * (ENGINEERING.md §5). TypeScript already makes `secret`/`nodeId`/`listen`
 * non-optional on `ClusterConfig`, but this guards plain-JS callers who
 * bypass the type checker.
 */
function validateClusterConfig(cluster: NonNullable<BeamSocketConfig['cluster']>): void {
  if (typeof cluster.secret !== 'string' || cluster.secret.length === 0) {
    throw new Error(
      'cluster.secret is required when cluster config is present — a mesh with no shared ' +
        'secret cannot authenticate its peers (RFC 0004 §4.7). Use the same secret on every node.',
    );
  }
  if (!Number.isInteger(cluster.nodeId) || cluster.nodeId < 0 || cluster.nodeId > 0xffff) {
    throw new Error(`cluster.nodeId must be an integer in 0..65535, got ${cluster.nodeId}`);
  }
  if (typeof cluster.listen !== 'string' || cluster.listen.length === 0) {
    throw new Error('cluster.listen is required when cluster config is present (e.g. "127.0.0.1:7946")');
  }
}

/**
 * Validate an admin close code (Phase 2B): 1000 (default) or the 4000–4999
 * application range. Throws BEFORE any FFI call — the other registered codes
 * belong to the engine/RFC 6455 and would lie to the client (types.ts
 * `AdminCloseCode`).
 */
function adminCode(code: number | undefined): number {
  const c = code ?? AdminCloseCode.NORMAL;
  if (
    !Number.isInteger(c) ||
    (c !== AdminCloseCode.NORMAL &&
      (c < AdminCloseCode.APP_RANGE_MIN || c > AdminCloseCode.APP_RANGE_MAX))
  ) {
    throw new RangeError(
      `admin close code must be 1000 or in the application range 4000-4999, got ${c}`,
    );
  }
  return c;
}

/** The request path without query (for `path` claim routing). */
function pathnameOf(url: string | undefined): string {
  const u = url ?? '/';
  const q = u.indexOf('?');
  return q < 0 ? u : u.slice(0, q);
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
    samplerMs: config.observability?.samplerMs,
    cluster: config.cluster
      ? {
          nodeId: config.cluster.nodeId,
          listen: config.cluster.listen,
          seeds: config.cluster.seeds ?? [],
          secret: config.cluster.secret,
          clusterName: config.cluster.clusterName,
        }
      : undefined,
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
  // ── Phase 1.1 attach (RFC 0002) ──
  #server?: BeamSocketConfig['server'];
  #path?: string;
  #upgradeHandler?: (req: any, socket: any, head: Buffer) => void;
  #closing = false;
  // ── 0.2.0 cluster mesh ──
  /** This server's own cluster node id — undefined in single-node mode. */
  #nodeId?: number;

  constructor(config: BeamSocketConfig = {}) {
    super();
    this.#config = config;
    if (config.cluster) {
      validateClusterConfig(config.cluster);
      this.#nodeId = config.cluster.nodeId;
    }
    if (config.server) {
      // §6: https.Server hands us ciphertext — refuse loudly (verbatim message).
      if (typeof (config.server as any).setSecureContext === 'function') {
        throw new Error(HTTPS_ATTACH_MSG);
      }
      // §7: Windows fd handoff is deferred — refuse loudly (verbatim message).
      if (process.platform === 'win32') {
        throw new Error(WINDOWS_ATTACH_MSG);
      }
      this.#server = config.server;
      this.#path = config.path;
      this.#upgradeHandler = (req, socket, head) => this.#onUpgrade(req, socket, head);
      // Register now; the engine starts lazily on the first claimed upgrade.
      this.#server.on('upgrade', this.#upgradeHandler);
    }
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

  /**
   * Single-socket target. Unknown/stale ids no-op at send time. A node-
   * prefixed id (0.2.0) naming another cluster member routes through the
   * mesh relay (`engine.sendNode`); a same-node or unprefixed id sends
   * locally, unchanged since 1B.
   */
  toSocket(socketId: string): Target {
    const parsed = decodeSocketId(socketId);
    return new Target(
      this.#requireEngine('toSocket'),
      {
        type: 'socket',
        node: parsed?.node,
        hi: parsed?.hi ?? 0xffffffff, // guaranteed miss for foreign ids
        lo: parsed?.lo ?? 0xffffffff,
      },
      this.#nodeId,
    );
  }

  /** All devices of a user. Fan-out runs entirely in Rust. (Phase 1C) */
  toUser(userId: string): Target {
    return new Target(this.#requireEngine('toUser'), { type: 'user', userId }, this.#nodeId);
  }

  /** Room target; fan-out (with .except()) runs entirely in Rust. */
  toRoom(room: string): Target {
    return new Target(this.#requireEngine('toRoom'), { type: 'room', room }, this.#nodeId);
  }

  /** Every live connection. One FFI call regardless of connection count. */
  broadcast(data: Buffer | string): void {
    new Target(this.#requireEngine('broadcast'), { type: 'all' }, this.#nodeId).send(data);
  }

  /** Room presence — `[{ id, userId, metadata }]`. (Phase 1D) */
  presence(room: string): Presence {
    return new Presence(this.#requireEngine('presence'), room, (hi, lo) => {
      // Metadata lives on the live Socket; absent (evicted / remote) → {}.
      return this.#sockets.get(encodeSocketId(hi, lo, this.#nodeId))?.metadata ?? {};
    });
  }

  #mapMetrics(s: NativeStats): Metrics {
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
      adminDisconnects: s.adminDisconnects,
      adminRoomCloses: s.adminRoomCloses,
      // JS-owned counter (the correlation map lives here, not in Rust).
      authMetadataEvicted: this.#pendingAuth.evicted,
    };
  }

  /** One-FFI-call snapshot of the runtime counters (Phase 1D). */
  metrics(): Metrics {
    return this.#mapMetrics(this.#requireEngine('metrics').stats());
  }

  /** Full runtime snapshot: counters + uptime + sampler rates (Phase 2A) +
   * cluster mesh state (0.2.0 — absent when single-node). */
  stats(): Stats {
    const s = this.#requireEngine('stats').stats();
    return {
      ...this.#mapMetrics(s),
      uptimeMs: s.uptimeMs,
      rates: s.rates
        ? {
            messagesIn: { perSec1s: s.rates.messagesIn1S, perSec10s: s.rates.messagesIn10S },
            messagesOut: { perSec1s: s.rates.messagesOut1S, perSec10s: s.rates.messagesOut10S },
            bytesIn: { perSec1s: s.rates.bytesIn1S, perSec10s: s.rates.bytesIn10S },
            bytesOut: { perSec1s: s.rates.bytesOut1S, perSec10s: s.rates.bytesOut10S },
          }
        : null,
      cluster: s.cluster
        ? {
            nodeId: s.cluster.nodeId,
            peers: s.cluster.peers,
            relayIn: s.cluster.relayIn,
            relayOut: s.cluster.relayOut,
            relayDrops: s.cluster.relayDrops,
            peerPressures: s.cluster.peerPressures.map(
              (p): [number, number] => [p.nodeId, p.pressure],
            ),
          }
        : undefined,
    };
  }

  /** Live connection count (Phase 2A). */
  connectionCount(): number {
    return this.#requireEngine('connectionCount').connectionCount();
  }

  /** The `n` most-populated rooms, highest first (Phase 2A). `n` (default 10) is
   * clamped to a hard cap of 100; a non-positive `n` throws. */
  topRooms(n = 10): RoomStat[] {
    return this.#requireEngine('topRooms')
      .topRooms(n)
      .map((r) => ({ room: r.room, members: r.members, messages: r.messages }));
  }

  /** Structural memory model + measured mailbox bytes-in-flight (Phase 2A). */
  memoryUsage(): MemoryUsage {
    const u = this.#requireEngine('memoryUsage').memoryUsage();
    return {
      connections: u.connections,
      rooms: u.rooms,
      users: u.users,
      estimatedHeapBytes: u.estimatedHeapBytes,
      mailboxBytesInFlight: u.mailboxBytesInFlight,
      estimated: true,
    };
  }

  /** The `topN` deepest send queues + the global drop total (Phase 2A). `topN`
   * (default 10) is clamped to 100; non-positive throws. */
  backpressureReport({ topN = 10 }: { topN?: number } = {}): BackpressureReport {
    const r = this.#requireEngine('backpressureReport').backpressureReport(topN);
    return {
      totalDrops: r.totalDrops,
      mailboxes: r.mailboxes.map((m) => ({
        socketId: encodeSocketId(m.idHi, m.idLo),
        userId: m.hasUserId ? m.userId : undefined,
        depthBytes: m.depthBytes,
        hwmBytes: m.hwmBytes,
        hwmPercent: m.hwmPercent,
      })),
    };
  }

  /** Prometheus text exposition of the counters + rates (Phase 2A). */
  metricsText(): string {
    return this.#requireEngine('metricsText').metricsText();
  }

  /** Query handle for a user (Phase 2A): `io.user(id).connections()`. */
  user(userId: string): UserHandle {
    const engine = this.#requireEngine('user');
    return {
      connections: () => {
        const flat = engine.userConnections(userId);
        const out: string[] = [];
        for (let i = 0; i + 1 < flat.length; i += 2) out.push(encodeSocketId(flat[i]!, flat[i + 1]!));
        return out;
      },
    };
  }

  /** Query handle for a room (Phase 2A): `io.room(id).info()`. */
  room(roomId: string): RoomHandle {
    const engine = this.#requireEngine('room');
    return {
      info: () => {
        const s = engine.roomInfo(roomId);
        return { room: s.room, members: s.members, messages: s.messages, exists: s.exists };
      },
    };
  }

  // ── Phase 2B: admin actions (§12.2). One FFI call each; the sweep runs in
  // Rust over the EXISTING close/cleanup paths (no new teardown logic). While
  // a graceful close() is draining, each verb is a safe no-op reporting 0 —
  // same answer as a nonexistent target — instead of a throw: an operator
  // script racing shutdown should not crash. Before the server ever started,
  // they fail loudly like every other verb. ──

  /**
   * Close one connection (Phase 2B). `code` defaults to 1000; the 4000–4999
   * application range is allowed (any other code throws a RangeError). Full
   * 1C/1D cleanup — rooms, identity, presence, metrics — runs in Rust on the
   * same path as `socket.close()`.
   */
  disconnectSocket(socketId: string, code?: number): AdminDisconnectResult {
    const c = adminCode(code);
    const engine = this.#adminEngine('disconnectSocket');
    if (!engine) return { closed: 0 };
    const parsed = decodeSocketId(socketId);
    if (!parsed) return { closed: 0 }; // never one of ours → same as stale
    return { closed: engine.disconnectSocket(parsed.hi, parsed.lo, c) };
  }

  /**
   * Close every device of one user (Phase 2B). Same code rules as
   * `disconnectSocket`. The identity entry is gone once the last device's
   * close completes (the 1C invariant). Returns how many devices were closed.
   */
  disconnectUser(userId: string, code?: number): AdminDisconnectResult {
    const c = adminCode(code);
    const engine = this.#adminEngine('disconnectUser');
    if (!engine) return { closed: 0 };
    return { closed: engine.disconnectUser(userId, c) };
  }

  /**
   * Destroy a room by removing every member (Phase 2B). Disconnect-free: the
   * members' connections stay alive and no close frame is sent — which is why,
   * unlike the disconnect verbs, this takes no close code (a parameter with no
   * wire effect is a footgun, not symmetry). Returns how many memberships were
   * removed.
   */
  closeRoom(room: string): AdminCloseRoomResult {
    const engine = this.#adminEngine('closeRoom');
    if (!engine) return { removed: 0 };
    return { removed: engine.closeRoom(room) };
  }

  /** Engine for an admin verb: null while draining (safe no-op), throw when
   * the server never started (an operator typo should fail loudly). */
  #adminEngine(method: string): NativeEngine | null {
    if (this.#engine) return this.#engine;
    if (this.#closing) return null; // close() drain in progress/complete
    throw new Error(`io.${method}() requires a running server — call listen() first`);
  }

  /**
   * Boot the Rust engine (its own Tokio threads — the Node loop is never
   * blocked) and start accepting. The returned promise resolves once the
   * port is bound.
   */
  async listen(port: number): Promise<number> {
    if (this.#server) {
      throw new Error(LISTEN_MUTEX_MSG); // §10.1 — attached mode owns no port
    }
    if (this.#listening) {
      throw new Error('listen() may only be called once per BeamSocket (Phase 1A)');
    }
    const engine = this.#ensureStarted();
    const bound = engine.listen(port);
    this.#listening = true;
    return bound;
  }

  /** Boot the Rust engine + bridge exactly once (shared by listen() + attach). */
  #ensureStarted(): NativeEngine {
    if (!this.#engine) {
      const native = loadNative();
      this.#engine = native.BeamEngine.start(
        toNativeConfig(this.#config),
        this.#authorizeFn !== undefined,
        (batch) => this.#onFlush(batch),
      );
    }
    return this.#engine;
  }

  /**
   * `'upgrade'` handler for attached mode (RFC 0002 §8.1, §10.2). Claims only
   * matching-path upgrades; defers non-matches to other listeners (never
   * destroys them). Sole-handler mode (no `path`) 400s malformed upgrades. On a
   * claim: pause, drain stranded pre-pause bytes into `head` (Rider 1), hand the
   * fd to Rust, then detach the Node socket.
   */
  #onUpgrade(req: any, socket: any, head: Buffer): void {
    // §10.2 path routing: only claim our path; defer the rest.
    if (this.#path !== undefined && pathnameOf(req.url) !== this.#path) {
      return;
    }
    // §10.4: 503 an upgrade that raced close()'s listener removal.
    if (this.#closing) {
      socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    // Must be a WebSocket upgrade.
    const isWs =
      String(req.headers?.upgrade ?? '').toLowerCase() === 'websocket' &&
      typeof req.headers?.['sec-websocket-key'] === 'string';
    if (!isWs) {
      // Sole-handler mode → we own all upgrades, so reject malformed ones (400).
      // Path mode → not ours to reject; defer.
      if (this.#path === undefined) {
        socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
        socket.destroy();
      }
      return;
    }

    const engine = this.#ensureStarted();

    // §8.1 step 1 + Rider 1: pause, then drain bytes libuv already buffered past
    // the request into `head` — otherwise a coalesced first frame is stranded.
    socket.pause();
    let full = head;
    for (let chunk = socket.read(); chunk !== null; chunk = socket.read()) {
      full = Buffer.concat([full, chunk]);
    }

    // §8.1 step 2: read the fd (private field; absence → 500 + detach).
    const fd = typeof socket._handle?.fd === 'number' ? socket._handle.fd : -1;
    if (fd < 0) {
      socket.write('HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }

    // rawHeaders is the flat [name, value, …] list (Rust lowercases the names).
    // Accept vs reject are both handled Rust-side (dup kept alive / status
    // written); the SDK's job is identical either way — detach the Node socket.
    engine.attach(fd, socket.remoteAddress ?? '', req.method ?? 'GET', req.url ?? '/', req.rawHeaders ?? [], full);

    // §8.1 step 4: detach. After this, Node holds zero per-connection state.
    socket.destroy();
  }

  /**
   * Graceful shutdown (Phase 1D): stop accepting (new upgrades get HTTP 503),
   * drain in-flight sockets, force-close stragglers at `timeoutMs` (default
   * 30 s) with 1001, then stop the engine and release the bridge. Resolves once
   * the runtime is down — after which the Node process can exit on its own.
   */
  async close(opts: CloseOptions = {}): Promise<void> {
    // Attached mode (§10.4): `#closing` makes #onUpgrade answer 503 to any
    // matching-path upgrade from now on — the handler STAYS registered so a
    // racing upgrade gets a real 503 whether it dispatches during or after the
    // drain (removing the handler would route matching upgrades to the app's
    // request handler or hang them, never a 503). The server is shutting down,
    // so a lingering 503-only listener is harmless. Non-matching paths are still
    // deferred to other listeners.
    this.#closing = true;
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
        const socket = new Socket(engine, hi, lo, info?.userId, info?.metadata, this.#nodeId);
        this.#sockets.set(socket.id, socket);
        this.emit('connection', socket);
      },
      onMessage: (hi, lo, payload, isBinary) => {
        // Zero-copy view straight through to the app handler.
        this.#sockets.get(encodeSocketId(hi, lo, this.#nodeId))?.emit('message', payload, isBinary);
      },
      onClose: (hi, lo, code, reason) => {
        const key = encodeSocketId(hi, lo, this.#nodeId);
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
