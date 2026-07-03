//! Room registry — Phase 1B (do not start before 1A's exit gate).
//!
//! Sharded DashMap<RoomId, RoomShard>. Membership is BIDIRECTIONAL
//! (room→conns and conn→rooms) so disconnect cleanup is O(rooms of that
//! connection). Auto-create on first join, auto-destroy on last leave.
//!
//! Required property test (ENGINEERING.md §6): after any join/leave/disconnect
//! sequence, both membership views agree and no empty room survives.
