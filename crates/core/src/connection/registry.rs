//! Sharded slab registry — Phase 1A.
//!
//! ConnectionId encodes shard index + slab key: lookup is an array index
//! within a shard, IDs recycle, no hashing on the hot path, and no global
//! lock (Rule 2).
//!
//! Required tests (ENGINEERING.md §5): insert/remove/recycle under
//! concurrency; lookups against recycled IDs miss safely.
