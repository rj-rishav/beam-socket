//! Presence — Phase 1D.
//!
//! A per-room presence view: for each live member, `(ConnectionId, userId)`.
//! Rust owns the id and the userId; **metadata lives in JS** (the Phase 1C
//! consequence — Rust never serializes an arbitrary JS object), so the SDK
//! joins metadata from its own store after this returns. Members whose metadata
//! was evicted (or, in Phase 4, live on another node) join as `{}`.
//!
//! `PresenceStore` is trait-shaped so Phase 4 can swap a distributed
//! implementation — gossip- or control-plane-backed — without touching the call
//! sites (ARCHITECTURE §2.1/§6). The local implementation reuses the same
//! lock discipline as fan-out: copy the room's member list out (room guard
//! released), THEN read each connection's registry entry.
//!
//! Memory cost (Rule 4): presence adds NO per-connection state of its own — the
//! conn→userId it reads already lives in the connection registry entry (see
//! registry.rs `Entry.user`); presence is a pure read over rooms + registry.

use crate::connection::registry::Registry;
use crate::ids::{ConnectionId, RoomId, UserId};
use crate::rooms::RoomRegistry;

/// One presence entry as Rust knows it. Metadata is joined SDK-side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceEntry {
    pub id: ConnectionId,
    pub user: Option<UserId>,
}

/// The seam for distributed presence (Phase 4). Local now.
pub trait PresenceStore {
    fn room_presence(
        &self,
        rooms: &RoomRegistry,
        conns: &Registry,
        room: &RoomId,
    ) -> Vec<PresenceEntry>;
}

/// Single-node presence: read straight from the room + connection registries.
pub struct LocalPresence;

impl PresenceStore for LocalPresence {
    fn room_presence(
        &self,
        rooms: &RoomRegistry,
        conns: &Registry,
        room: &RoomId,
    ) -> Vec<PresenceEntry> {
        // Copy the member list out under the room guard, release it, then read
        // each live connection's userId (lock invariant: room-map → conn-shard
        // is never held simultaneously). A member that disconnected between the
        // snapshot and the read is skipped (`user_of` returns the outer `None`).
        let Some(members) = rooms.members(room) else {
            return Vec::new();
        };
        members
            .into_iter()
            .filter_map(|id| conns.user_of(id).map(|user| PresenceEntry { id, user }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackpressurePolicy;
    use crate::connection::backpressure::Mailbox;
    use crate::connection::{CloseSignal, ConnHandle, Control, CONTROL_QUEUE_CAPACITY};
    use crate::metrics::Metrics;
    use std::sync::Arc;

    fn handle(metrics: &Arc<Metrics>) -> ConnHandle {
        let (control, _rx) = tokio::sync::mpsc::channel::<Control>(CONTROL_QUEUE_CAPACITY);
        let (close, _close_rx) = CloseSignal::new();
        ConnHandle {
            mailbox: Mailbox::new(1024, BackpressurePolicy::DropNewest, metrics.clone()),
            control,
            close,
        }
    }

    #[test]
    fn room_presence_reports_ids_and_users_skips_the_gone() {
        let metrics = Arc::new(Metrics::default());
        let conns = Registry::new();
        let rooms = RoomRegistry::new();
        let room = RoomId("lobby".into());

        let alice = conns.insert(handle(&metrics), Some(UserId("alice".into())));
        let anon = conns.insert(handle(&metrics), None);
        let gone = conns.insert(handle(&metrics), Some(UserId("ghost".into())));
        for id in [alice, anon, gone] {
            rooms.join(&conns, id, room.clone(), 0);
        }
        // `gone` disconnects: removed from the registry, but suppose a stale
        // membership snapshot still lists it — presence must skip it.
        conns.remove_full(gone);

        let mut view = LocalPresence.room_presence(&rooms, &conns, &room);
        view.sort_by_key(|e| e.id.0);
        let mut expected = vec![
            PresenceEntry {
                id: alice,
                user: Some(UserId("alice".into())),
            },
            PresenceEntry {
                id: anon,
                user: None,
            },
        ];
        expected.sort_by_key(|e| e.id.0);
        assert_eq!(view, expected);

        // Unknown room → empty view, never a panic.
        assert!(LocalPresence
            .room_presence(&rooms, &conns, &RoomId("nope".into()))
            .is_empty());
    }
}
