/**
 * Batch demultiplexer — Phase 1A (graduated from the RFC 0001 spike).
 *
 * Receives one native Buffer per bridge flush and decodes it with a cursor
 * reader (design C — RFC 0001 winner). Wire format (little-endian, mirrored
 * by the Rust round-trip tests in crates/node/src/bridge.rs::flat):
 *
 *   [u32 count] then per event: [u64 connId][u8 kind][u32 len][payload]
 *   kind: 0 text, 1 binary, 2 opened (len 0), 3 closed ([u16 code][reason])
 *
 * Message payloads are exposed as ZERO-COPY `subarray` views into the flush
 * buffer — zero per-message allocation is the contract; keep it.
 */

export const KIND_TEXT = 0;
export const KIND_BINARY = 1;
export const KIND_OPEN = 2;
export const KIND_CLOSE = 3;

export interface DemuxHandlers {
  onOpen(idHi: number, idLo: number): void;
  /** `payload` is a zero-copy view into the flush buffer. */
  onMessage(idHi: number, idLo: number, payload: Buffer, isBinary: boolean): void;
  onClose(idHi: number, idLo: number, code: number, reason: string): void;
}

export function demux(batch: Buffer, handlers: DemuxHandlers): void {
  const count = batch.readUInt32LE(0);
  let off = 4;
  for (let i = 0; i < count; i++) {
    // u64 connId, little-endian: low 4 bytes first.
    const idLo = batch.readUInt32LE(off);
    const idHi = batch.readUInt32LE(off + 4);
    const kind = batch[off + 8]!;
    const len = batch.readUInt32LE(off + 9);
    off += 13;
    switch (kind) {
      case KIND_TEXT:
      case KIND_BINARY:
        handlers.onMessage(idHi, idLo, batch.subarray(off, off + len), kind === KIND_BINARY);
        break;
      case KIND_OPEN:
        handlers.onOpen(idHi, idLo);
        break;
      case KIND_CLOSE: {
        const code = batch.readUInt16LE(off);
        const reason = len > 2 ? batch.toString('utf8', off + 2, off + len) : '';
        handlers.onClose(idHi, idLo, code, reason);
        break;
      }
      default:
        throw new Error(`corrupt flush buffer: unknown event kind ${kind}`);
    }
    off += len;
  }
}
