/**
 * Native binding loader — Phase 1A/1D.
 * Standard napi-rs layout: optionalDependencies per platform under npm/,
 * falling back to a local debug build for development.
 */
export function loadNative(): never {
  throw new Error('Native binding not built yet — Phase 1A, docs/ENGINEERING.md §5');
}
