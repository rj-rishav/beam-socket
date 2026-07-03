/**
 * Batch demultiplexer — Phase 1A (graduated from the RFC 0001 spike).
 *
 * Receives one native batch per bridge flush and dispatches to per-socket and
 * server-level listeners. Decode format depends on the winning spike design
 * (B: array of objects, C: flat Buffer + cursor reader).
 */
export function demux(_batch: unknown): void {
  throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
}
