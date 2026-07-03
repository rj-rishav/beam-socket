/**
 * Fluent targeting. Every terminal .send() is ONE FFI call — fan-out happens
 * in Rust (Rule 1). Phase 1B for sockets/rooms, 1C for users.
 */
export class Target {
  #excluded: string[] = [];

  except(socketId: string): this {
    this.#excluded.push(socketId);
    return this;
  }

  send(_data: Buffer | string): void {
    throw new Error('Not implemented until Phase 1B — docs/ENGINEERING.md §6');
  }
}
