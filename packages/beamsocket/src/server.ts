import { EventEmitter } from 'node:events';

import type {
  AuthorizeRequest,
  AuthorizeResult,
  BeamSocketConfig,
  CloseOptions,
  Metrics,
} from './types.js';
import { demux } from './events.js';
import { loadNative, type NativeConfig, type NativeEngine } from './native.js';
import { Presence } from './presence.js';
import { Target } from './rooms.js';
import { Socket } from './socket.js';

export interface ServerEvents {
  connection: (socket: Socket) => void;
}

function toNativeConfig(config: BeamSocketConfig): NativeConfig {
  return {
    maxPayloadBytes: config.limits?.maxPayloadBytes,
    highWaterMark: config.backpressure?.highWaterMark,
    backpressurePolicy: config.backpressure?.policy,
    pingIntervalMs: config.keepalive?.pingIntervalMs,
    pongTimeoutMs: config.keepalive?.pongTimeoutMs,
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

  constructor(config: BeamSocketConfig = {}) {
    super();
    this.#config = config;
  }

  /** Runs in JS once per connection, at upgrade time. (Phase 1C) */
  authorize(_fn: (req: AuthorizeRequest) => AuthorizeResult | Promise<AuthorizeResult>): this {
    throw new Error('Not implemented until Phase 1C — docs/ENGINEERING.md §7');
  }

  override on<E extends keyof ServerEvents>(event: E, handler: ServerEvents[E]): this {
    return super.on(event, handler as (...args: unknown[]) => void);
  }

  toSocket(_socketId: string): Target {
    throw new Error('Not implemented until Phase 1B — docs/ENGINEERING.md §6');
  }

  /** All devices of a user. (Phase 1C) */
  toUser(_userId: string): Target {
    throw new Error('Not implemented until Phase 1C — docs/ENGINEERING.md §7');
  }

  toRoom(_room: string): Target {
    throw new Error('Not implemented until Phase 1B — docs/ENGINEERING.md §6');
  }

  broadcast(_data: Buffer | string): void {
    throw new Error('Not implemented until Phase 1B — docs/ENGINEERING.md §6');
  }

  presence(room: string): Presence {
    return new Presence(room);
  }

  metrics(): Metrics {
    throw new Error('Not implemented until Phase 1D — docs/ENGINEERING.md §8');
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
    this.#engine = native.BeamEngine.start(toNativeConfig(this.#config), (batch) =>
      this.#onFlush(batch),
    );
    const bound = this.#engine.listen(port);
    this.#listening = true;
    return bound;
  }

  /**
   * Phase 1A teardown: stop accepting, close every connection (1001), stop
   * the engine. Graceful drain semantics (`timeoutMs`) land in Phase 1D.
   */
  async close(_opts: CloseOptions = {}): Promise<void> {
    this.#engine?.shutdown();
    this.#engine = undefined;
  }

  /** One flat Buffer per bridge flush (design C): decode + dispatch. */
  #onFlush(batch: Buffer): void {
    demux(batch, {
      onOpen: (hi, lo) => {
        const engine = this.#engine;
        if (!engine) return;
        const socket = new Socket(engine, hi, lo);
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
    });
  }
}

function socketKey(hi: number, lo: number): string {
  return `${hi.toString(36)}-${lo.toString(36)}`;
}
