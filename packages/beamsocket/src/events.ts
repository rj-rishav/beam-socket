/**
 * Batch demultiplexer — Phase 1A (graduated from the RFC 0001 spike).
 *
 * Receives one native batch per bridge flush and dispatches to per-socket and
 * server-level listeners. Decode format: Design C won (RFC 0001 results) —
 * one flat Buffer per flush, decoded by a cursor reader into zero-copy
 * subarray views. Zero per-message allocation is the contract; keep it.
 */
export function demux(_batch: unknown): void {
  throw new Error('Not implemented until Phase 1A — docs/ENGINEERING.md §5');
}
