import type { PresenceEntry } from './types.js';

/** Phase 1D. Async because Phase 4 makes this a distributed query. */
export class Presence {
  constructor(private readonly room: string) {}

  async list(): Promise<PresenceEntry[]> {
    throw new Error('Not implemented until Phase 1D — docs/ENGINEERING.md §8');
  }
}
