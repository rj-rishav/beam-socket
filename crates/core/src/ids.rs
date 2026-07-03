//! Opaque identifiers. Public API exposes these as strings so the internal
//! encoding can grow a node-ID prefix in Phase 3 (clustering) without a break.

/// Internally: shard index + generation + slab key. O(1) registry lookup.
///
/// Layout (u64, little-endian when it crosses the bridge):
///
/// ```text
/// [ shard: 8 bits ][ generation: 24 bits ][ slab key: 32 bits ]
/// ```
///
/// The generation counter is bumped every time a slab slot is recycled, so a
/// stale ID held by JS (e.g. a `socket.send()` racing a disconnect) can never
/// address the *next* connection that reuses the slot — the generation check
/// in the registry misses instead (ENGINEERING.md §5 "IDs recycled", made
/// safe). 24 bits of generation per slot wraps only after 16.7M reuses of the
/// same slot; Phase 3's cluster-wide IDs redo this encoding anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

pub const GENERATION_BITS: u32 = 24;
pub const GENERATION_MASK: u32 = (1 << GENERATION_BITS) - 1;

impl ConnectionId {
    #[inline]
    pub fn new(shard: u8, generation: u32, key: u32) -> Self {
        let g = (generation & GENERATION_MASK) as u64;
        ConnectionId(((shard as u64) << 56) | (g << 32) | key as u64)
    }

    #[inline]
    pub fn shard(self) -> u8 {
        (self.0 >> 56) as u8
    }

    #[inline]
    pub fn generation(self) -> u32 {
        ((self.0 >> 32) as u32) & GENERATION_MASK
    }

    #[inline]
    pub fn key(self) -> u32 {
        self.0 as u32
    }
}

/// Bound via `authorize` returning a userId. (Phase 1C)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomId(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_round_trips_fields() {
        let id = ConnectionId::new(17, 0xABCDEF, 0xDEAD_BEEF);
        assert_eq!(id.shard(), 17);
        assert_eq!(id.generation(), 0xABCDEF);
        assert_eq!(id.key(), 0xDEAD_BEEF);
    }

    #[test]
    fn generation_wraps_at_24_bits() {
        let id = ConnectionId::new(0, GENERATION_MASK + 5, 1);
        assert_eq!(id.generation(), 4);
    }
}
