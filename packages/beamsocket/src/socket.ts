/**
 * Lightweight JS proxy around a connection ID. There is NO per-socket native
 * handle — methods delegate to flat native calls with the ID, so JS heap cost
 * per connection is one small object, allocated only if the app touches it.
 */
export interface SocketEvents {
  message: (data: Buffer | string, isBinary: boolean) => void;
  close: (code: number, reason: string) => void;
}

export class Socket {
  /** Opaque. Do not parse — the encoding will change in Phase 3. */
  readonly id: string;
  readonly userId?: string;
  readonly metadata: Record<string, unknown>;

  constructor(id: string, userId?: string, metadata: Record<string, unknown> = {}) {
    this.id = id;
    this.userId = userId;
    this.metadata = metadata;
  }

  send(_data: Buffer | string): void {
    throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
  }

  join(_room: string): void {
    throw new Error('Not implemented until Phase 1B — docs/ENGINEERING.md §6');
  }

  leave(_room: string): void {
    throw new Error('Not implemented until Phase 1B — docs/ENGINEERING.md §6');
  }

  on<E extends keyof SocketEvents>(_event: E, _handler: SocketEvents[E]): this {
    throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
  }

  close(_code?: number, _reason?: string): void {
    throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
  }
}
