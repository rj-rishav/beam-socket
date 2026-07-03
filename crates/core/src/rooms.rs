//! Room registry — Phase 1B.
//!
//! Sharded `DashMap<RoomId, RoomShard>` (Rule 2: no global lock). Membership
//! is BIDIRECTIONAL — room→conns lives here, conn→rooms lives in the
//! connection registry entry — so disconnect cleanup is O(rooms of that
//! connection). Rooms auto-create on first join and auto-destroy on last
//! leave: an empty room must never survive.
//!
//! ## Lock-order invariant (deadlock freedom)
//!
//! Membership mutations (`join`/`leave`) run inside the connection's shard
//! lock and touch the room map from there: **conn-shard → room-map**, never
//! the reverse. Readers that need both (fan-out) copy the member list out of
//! the room map and RELEASE the room guard before touching any conn shard —
//! so no thread ever holds room-map while waiting on conn-shard. The conn
//! shard lock is thereby the serializer for a connection's membership, which
//! is what makes the join-vs-disconnect race safe: a join that won the shard
//! lock is in the set `remove_full` hands to `disconnect_cleanup`; a join
//! that lost finds the entry gone and no-ops.
//!
//! Memory cost (Rule 4): per room ≈ one DashMap entry (RoomId string + HashSet
//! overhead ≈ 100 B + 48 B·capacity); per membership ≈ one ConnectionId (8 B)
//! in the room set + one RoomId clone (~room-name bytes + 24 B) in the conn
//! entry — measured number in the PR notes.

use std::collections::HashSet;

use dashmap::DashMap;

use crate::connection::registry::Registry;
use crate::ids::{ConnectionId, RoomId};

#[derive(Default)]
pub struct RoomRegistry {
    rooms: DashMap<RoomId, HashSet<ConnectionId>>,
}

/// Outcome of a join/leave, surfaced to the SDK (frame-delivery-grade
/// signals, not errors).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipChange {
    Changed,
    /// Join of a room already joined / leave of a room not joined.
    NoOp,
    /// Unknown or stale connection id.
    NotFound,
}

impl RoomRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of live rooms (auto-destroyed rooms excluded by construction).
    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// Snapshot of a room's members: copied out under the room guard, guard
    /// released before the caller touches any conn shard (lock invariant).
    /// `None` = room doesn't exist.
    pub fn members(&self, room: &RoomId) -> Option<Vec<ConnectionId>> {
        self.rooms
            .get(room)
            .map(|set| set.iter().copied().collect())
    }

    /// Membership size without copying (diagnostics/tests).
    pub fn member_count(&self, room: &RoomId) -> usize {
        self.rooms.get(room).map_or(0, |set| set.len())
    }

    /// Join `id` to `room`. Runs under the connection's shard lock
    /// (conn-shard → room-map order).
    pub fn join(&self, conns: &Registry, id: ConnectionId, room: RoomId) -> MembershipChange {
        match conns.with_rooms(id, |set| {
            if set.insert(room.clone()) {
                // Auto-create on first join.
                self.rooms.entry(room).or_default().insert(id);
                MembershipChange::Changed
            } else {
                MembershipChange::NoOp
            }
        }) {
            Some(change) => change,
            None => MembershipChange::NotFound,
        }
    }

    /// Leave `room`. Same locking discipline as `join`.
    pub fn leave(&self, conns: &Registry, id: ConnectionId, room: &RoomId) -> MembershipChange {
        match conns.with_rooms(id, |set| {
            if set.remove(room) {
                self.remove_member(room, id);
                MembershipChange::Changed
            } else {
                MembershipChange::NoOp
            }
        }) {
            Some(change) => change,
            None => MembershipChange::NotFound,
        }
    }

    /// O(rooms of the connection) cleanup with the set taken out of the
    /// registry by `remove_full` — the entry is already gone, so no new join
    /// for this id can race the sweep.
    pub fn disconnect_cleanup(&self, id: ConnectionId, rooms: HashSet<RoomId>) {
        for room in rooms {
            self.remove_member(&room, id);
        }
    }

    /// Remove one member; auto-destroy the room when it empties.
    fn remove_member(&self, room: &RoomId, id: ConnectionId) {
        if let Some(mut set) = self.rooms.get_mut(room) {
            set.remove(&id);
            if set.is_empty() {
                drop(set); // release the entry guard before removal
                           // remove_if re-checks emptiness under the map lock, so a
                           // concurrent join between drop and here is not lost.
                self.rooms.remove_if(room, |_, set| set.is_empty());
            }
        }
    }
}
