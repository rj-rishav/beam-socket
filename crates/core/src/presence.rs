//! Presence — Phase 1D.
//!
//! Per-connection metadata + per-room presence views returning
//! { id, userId, metadata }. Local PresenceStore now; the trait is the seam
//! for distributed presence in Phase 4 (ARCHITECTURE.md §6).
