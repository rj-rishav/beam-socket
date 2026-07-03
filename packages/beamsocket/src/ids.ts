/**
 * Socket-id codec — SDK-internal. Applications must treat `socket.id` as
 * opaque (the encoding changes in Phase 3); the SDK itself defined it, so it
 * may parse it to route flat native calls.
 */

export function encodeSocketId(hi: number, lo: number): string {
  return `${hi.toString(36)}-${lo.toString(36)}`;
}

/** Returns null for ids that were never ours (wrong shape). */
export function decodeSocketId(id: string): { hi: number; lo: number } | null {
  const dash = id.indexOf('-');
  if (dash <= 0) return null;
  const hi = parseInt(id.slice(0, dash), 36);
  const lo = parseInt(id.slice(dash + 1), 36);
  if (!Number.isInteger(hi) || !Number.isInteger(lo) || hi < 0 || lo < 0) return null;
  return { hi, lo };
}
