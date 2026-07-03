//! Opaque identifiers. Public API exposes these as strings so the internal
//! encoding can grow a node-ID prefix in Phase 3 (clustering) without a break.

/// Internally: shard index + slab key. O(1) registry lookup. (Phase 1A)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Bound via `authorize` returning a userId. (Phase 1C)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomId(pub String);
