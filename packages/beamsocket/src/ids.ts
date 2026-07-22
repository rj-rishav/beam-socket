/**
 * Socket-id codec — SDK-internal. Applications must treat `socket.id` as
 * opaque; the SDK defined it, so it may parse it to route flat native calls.
 *
 * Phase 3D (RFC 0004 §4.5): the encoding grew a **node-id prefix** so
 * `toSocket(id)` can route to the owning node — the payoff of keeping the id
 * opaque since Phase 1A. Single-node ids stay **two-segment and byte-identical**
 * to pre-3D; a clustered id is three-segment (`node-hi-lo`), all base-36.
 * `decodeSocketId` accepts both: a two-segment (pre-3D / single-node) id
 * round-trips with `node` undefined ("this node").
 */

function nonNeg(x: number): boolean {
  return Number.isInteger(x) && x >= 0;
}

/**
 * Encode a socket id. Omit `node` (single-node) for the pre-3D two-segment
 * form; pass it (clustered) for the three-segment form.
 */
export function encodeSocketId(hi: number, lo: number, node?: number): string {
  const base = `${hi.toString(36)}-${lo.toString(36)}`;
  return node === undefined ? base : `${node.toString(36)}-${base}`;
}

/**
 * Returns null for ids that were never ours (wrong shape). A two-segment id
 * decodes with `node` undefined (this node); a three-segment id carries the
 * owning node.
 */
export function decodeSocketId(id: string): { node?: number; hi: number; lo: number } | null {
  const parts = id.split('-');
  if (parts.length === 2) {
    const hi = parseInt(parts[0]!, 36);
    const lo = parseInt(parts[1]!, 36);
    if (!nonNeg(hi) || !nonNeg(lo)) return null;
    return { hi, lo };
  }
  if (parts.length === 3) {
    const node = parseInt(parts[0]!, 36);
    const hi = parseInt(parts[1]!, 36);
    const lo = parseInt(parts[2]!, 36);
    if (!nonNeg(node) || !nonNeg(hi) || !nonNeg(lo)) return null;
    return { node, hi, lo };
  }
  return null;
}
