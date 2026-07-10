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

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::AtomicU64;

use dashmap::DashMap;

use crate::connection::registry::Registry;
use crate::ids::{ConnectionId, RoomId};
use crate::metrics::Metrics;

/// A room's state: its members plus a cumulative message counter (Phase 2A).
/// The counter is `+8 B/room` (Rule 4) and is bumped ONLY on the existing Room
/// fan-out path, where the room is already resolved — no new lookup, no
/// per-message work anywhere else (§12 rule 1).
#[derive(Default)]
struct RoomEntry {
    members: HashSet<ConnectionId>,
    messages: AtomicU64,
}

/// One row of `topRooms` / `room().info()` (Phase 2A). `Ord` ranks rooms:
/// member count, then message count, then room name ASC — a total, deterministic
/// order so the top-N agrees with the reference model under churn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomStat {
    pub room: String,
    pub members: usize,
    pub messages: u64,
}

impl Ord for RoomStat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.members
            .cmp(&other.members)
            .then(self.messages.cmp(&other.messages))
            // A smaller room name ranks HIGHER, so it must compare as `Greater`.
            .then_with(|| other.room.cmp(&self.room))
    }
}

impl PartialOrd for RoomStat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
pub struct RoomRegistry {
    rooms: DashMap<RoomId, RoomEntry>,
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
    /// Join refused: the connection is already in `maxRoomsPerConnection`
    /// rooms (Phase 1C — enforced in Rust before any per-message work).
    LimitExceeded,
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
            .map(|e| e.members.iter().copied().collect())
    }

    /// Broadcast path (Phase 2A): copy the members out AND bump the room's
    /// message counter in the SAME `get` — this REPLACES the `members()` lookup
    /// the Room fan-out already did, so it adds no new lookup and no per-message
    /// work outside the one fan-out that was going to happen anyway.
    pub fn record_and_members(&self, room: &RoomId) -> Option<Vec<ConnectionId>> {
        self.rooms.get(room).map(|e| {
            Metrics::add(&e.messages, 1);
            e.members.iter().copied().collect()
        })
    }

    /// Membership size without copying (diagnostics/tests).
    pub fn member_count(&self, room: &RoomId) -> usize {
        self.rooms.get(room).map_or(0, |e| e.members.len())
    }

    /// `room().info()` (Phase 2A): `(members, messages, exists)`.
    pub fn info(&self, room: &RoomId) -> RoomStat {
        match self.rooms.get(room) {
            Some(e) => RoomStat {
                room: room.0.clone(),
                members: e.members.len(),
                messages: Metrics::get(&e.messages),
            },
            None => RoomStat {
                room: room.0.clone(),
                members: 0,
                messages: 0,
            },
        }
    }

    /// `topRooms(n)` (Phase 2A): the n rooms with the most members (ties by
    /// message count, then name), highest first. Bounded output; copy-out
    /// discipline — each entry is read under its own shard lock (via `iter`),
    /// the bounded top-N heap is held OUTSIDE any lock, and no two shard locks
    /// are ever held at once (§12 rules 2/3). `n == 0` yields an empty vec (the
    /// binding rejects non-positive requests and clamps above the cap).
    pub fn top_rooms(&self, n: usize) -> Vec<RoomStat> {
        if n == 0 {
            return Vec::new();
        }
        // A min-heap of the current top-N: push, then evict the lowest-ranked
        // once over capacity — never more than n+1 entries retained.
        let mut heap: BinaryHeap<Reverse<RoomStat>> = BinaryHeap::with_capacity(n + 1);
        for entry in self.rooms.iter() {
            let stat = RoomStat {
                room: entry.key().0.clone(),
                members: entry.value().members.len(),
                messages: Metrics::get(&entry.value().messages),
            };
            heap.push(Reverse(stat));
            if heap.len() > n {
                heap.pop(); // drop the lowest-ranked of the kept set
            }
        }
        let mut out: Vec<RoomStat> = heap.into_iter().map(|Reverse(s)| s).collect();
        out.sort_by(|a, b| b.cmp(a)); // highest rank first
        out
    }

    /// Join `id` to `room`. Runs under the connection's shard lock
    /// (conn-shard → room-map order). `max_rooms` (0 = unlimited) is enforced
    /// here under that same lock (Phase 1C `maxRoomsPerConnection`): the check
    /// and the insert are atomic w.r.t. this connection's membership, so a
    /// racing join cannot slip past the cap.
    pub fn join(
        &self,
        conns: &Registry,
        id: ConnectionId,
        room: RoomId,
        max_rooms: u32,
    ) -> MembershipChange {
        match conns.with_rooms(id, |set| {
            if set.contains(&room) {
                return MembershipChange::NoOp; // already joined — idempotent, no cost
            }
            if max_rooms != 0 && set.len() as u64 >= max_rooms as u64 {
                return MembershipChange::LimitExceeded;
            }
            set.insert(room.clone());
            // Auto-create on first join (fresh RoomEntry: empty set, 0 counter).
            self.rooms.entry(room).or_default().members.insert(id);
            MembershipChange::Changed
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

    /// `closeRoom` sweep (Phase 2B §12.2): copy the members out (room guard
    /// released before any conn shard is touched — the 1B discipline), then
    /// run the EXISTING `leave` path per member. The last leave auto-destroys
    /// the room — no new teardown logic, by design: the sweep is literally
    /// `members()` + `leave()` in a loop. Connections stay alive.
    ///
    /// Returns `Some(removed)` (memberships actually removed), or `None` when
    /// the room doesn't exist. A member that disconnects between the copy-out
    /// and its leave is a benign `NotFound`/`NoOp` (not counted); a join that
    /// races the sweep may keep the room alive with the new member — the room
    /// was closed as of the snapshot, which is all a sweep can promise.
    pub fn close_room(&self, conns: &Registry, room: &RoomId) -> Option<usize> {
        let members = self.members(room)?;
        let mut removed = 0;
        for id in members {
            if self.leave(conns, id, room) == MembershipChange::Changed {
                removed += 1;
            }
        }
        Some(removed)
    }

    /// O(rooms of the connection) cleanup with the set taken out of the
    /// registry by `remove_full` — the entry is already gone, so no new join
    /// for this id can race the sweep.
    pub fn disconnect_cleanup(&self, id: ConnectionId, rooms: HashSet<RoomId>) {
        for room in rooms {
            self.remove_member(&room, id);
        }
    }

    /// Remove one member; auto-destroy the room when it empties. Destroying the
    /// entry also drops its message counter — an idle room costs nothing, and a
    /// re-created room starts its counter fresh (documented behavior).
    fn remove_member(&self, room: &RoomId, id: ConnectionId) {
        if let Some(mut e) = self.rooms.get_mut(room) {
            e.members.remove(&id);
            if e.members.is_empty() {
                drop(e); // release the entry guard before removal
                         // remove_if re-checks emptiness under the map lock, so a
                         // concurrent join between drop and here is not lost.
                self.rooms.remove_if(room, |_, e| e.members.is_empty());
            }
        }
    }
}
