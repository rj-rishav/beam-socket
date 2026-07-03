import type {
  AuthorizeRequest,
  AuthorizeResult,
  BeamSocketConfig,
  CloseOptions,
  Metrics,
} from './types.js';
import { Presence } from './presence.js';
import { Target } from './rooms.js';
import type { Socket } from './socket.js';

export interface ServerEvents {
  connection: (socket: Socket) => void;
}

/**
 * The control plane. Every method here is a thin wrapper over a flat native
 * call; per-message work stays in Rust (Rule 1).
 *
 * API contract: docs/ARCHITECTURE.md §4. Phase map: docs/ENGINEERING.md §3.
 */
export class BeamSocket {
  constructor(_config: BeamSocketConfig = {}) {
    // Phase 1A: validate config, hand to native Engine::start.
  }

  /** Runs in JS once per connection, at upgrade time. (Phase 1C) */
  authorize(_fn: (req: AuthorizeRequest) => AuthorizeResult | Promise<AuthorizeResult>): this {
    throw new Error('Not implemented until Phase 1C — docs/ENGINEERING.md §7');
  }

  on<E extends keyof ServerEvents>(_event: E, _handler: ServerEvents[E]): this {
    throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
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

  async listen(_port: number): Promise<void> {
    throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
  }

  /** Stop accepting → drain → flush pending writes → close. (Phase 1D) */
  async close(_opts: CloseOptions = {}): Promise<void> {
    throw new Error('Not implemented until Phase 1D — docs/ENGINEERING.md §8');
  }
}
