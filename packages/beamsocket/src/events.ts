/**
 * Batch demultiplexer — Phase 1A (graduated from the RFC 0001 spike),
 * extended in Phase 1C with the authorize request leg.
 *
 * Receives one native Buffer per bridge flush and decodes it with a cursor
 * reader (design C — RFC 0001 winner). Wire format (little-endian, mirrored
 * by the Rust round-trip tests in crates/node/src/bridge.rs::flat):
 *
 *   [u32 count] then per event: [u64 connId][u8 kind][u32 len][payload]
 *   kind: 0 text, 1 binary,
 *         2 opened   (payload: 8-byte authorize request_id, or empty),
 *         3 closed   ([u16 code][reason]),
 *         4 authorize(connId slot = request_id; payload =
 *                     [u32 ipLen][ip][u32 urlLen][url][u32 hCount]{[u32 k][u32 v]…})
 *
 * Message payloads are exposed as ZERO-COPY `subarray` views into the flush
 * buffer — zero per-message allocation is the contract; keep it. Authorize is
 * rare (once per connection), so its strings are decoded eagerly.
 */

export const KIND_TEXT = 0;
export const KIND_BINARY = 1;
export const KIND_OPEN = 2;
export const KIND_CLOSE = 3;
export const KIND_AUTHORIZE = 4;

/** Decoded authorize request, ready to hand to the app's `authorize` hook. */
export interface AuthorizeWire {
  ip: string;
  url: string;
  headers: Record<string, string | string[]>;
}

export interface DemuxHandlers {
  /** `authReq` is the originating authorize request_id halves, or null for a
   * connection admitted with no `authorize` hook. */
  onOpen(idHi: number, idLo: number, authReq: { hi: number; lo: number } | null): void;
  /** `payload` is a zero-copy view into the flush buffer. */
  onMessage(idHi: number, idLo: number, payload: Buffer, isBinary: boolean): void;
  onClose(idHi: number, idLo: number, code: number, reason: string): void;
  /** `reqHi`/`reqLo` are the request_id halves to pass back to resolveAuthorize. */
  onAuthorize(reqHi: number, reqLo: number, req: AuthorizeWire): void;
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
        // Optional 8-byte authorize request_id (LE lo, hi).
        handlers.onOpen(
          idHi,
          idLo,
          len === 8 ? { lo: batch.readUInt32LE(off), hi: batch.readUInt32LE(off + 4) } : null,
        );
        break;
      case KIND_CLOSE: {
        const code = batch.readUInt16LE(off);
        const reason = len > 2 ? batch.toString('utf8', off + 2, off + len) : '';
        handlers.onClose(idHi, idLo, code, reason);
        break;
      }
      case KIND_AUTHORIZE: {
        // The connId slot carried the request_id (reqHi/reqLo).
        let p = off;
        const readStr = (): string => {
          const n = batch.readUInt32LE(p);
          p += 4;
          const s = batch.toString('utf8', p, p + n);
          p += n;
          return s;
        };
        const ip = readStr();
        const url = readStr();
        const hCount = batch.readUInt32LE(p);
        p += 4;
        const headers: Record<string, string | string[]> = {};
        for (let h = 0; h < hCount; h++) {
          const name = readStr();
          const value = readStr();
          const existing = headers[name];
          if (existing === undefined) {
            headers[name] = value;
          } else if (Array.isArray(existing)) {
            existing.push(value);
          } else {
            headers[name] = [existing, value];
          }
        }
        handlers.onAuthorize(idHi, idLo, { ip, url, headers });
        break;
      }
      default:
        throw new Error(`corrupt flush buffer: unknown event kind ${kind}`);
    }
    off += len;
  }
}
