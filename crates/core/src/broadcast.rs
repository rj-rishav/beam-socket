//! Fan-out engine — Phase 1B.
//!
//! The payload is serialized ONCE into `Bytes` (at the FFI boundary — the
//! single unavoidable JS→Rust copy); fan-out clones the refcounted handle
//! into each recipient's bounded mailbox. One allocation regardless of
//! recipient count, and the codec writes the same allocation to every socket
//! (tungstenite ≥0.26 Bytes payloads). Fan-out never enters JS (Rule 1).
//!
//! Locking: the member list is copied out of the room map and the room guard
//! is released BEFORE any conn shard is touched (see rooms.rs lock-order
//! invariant). Slow members hit their own backpressure policy; nobody else
//! is affected — pushes are non-blocking `Mailbox::push` calls.

use bytes::Bytes;

use crate::connection::backpressure::{OutboundFrame, PushOutcome};
use crate::connection::registry::Registry;
use crate::connection::ConnHandle;
use crate::identity::IdentityRegistry;
use crate::ids::{ConnectionId, RoomId, UserId};
use crate::rooms::RoomRegistry;

/// Where a broadcast goes.
pub enum FanoutTarget<'a> {
    /// Every live connection (`io.broadcast()`).
    All,
    /// One room (`io.toRoom(...)`).
    Room(&'a RoomId),
    /// Every device of one user (`io.toUser(...)`, Phase 1C). Fan-out runs
    /// entirely in Rust over the sharded identity index, reusing this same
    /// serialize-once path — one allocation regardless of device count.
    User(&'a UserId),
}

/// Fan-out accounting, surfaced for tests/metrics. Not a delivery receipt —
/// Phase 1 semantics are frame delivery (queued ≠ delivered).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FanoutReport {
    /// Recipients addressed (after `except` filtering).
    pub attempted: u64,
    /// Frames accepted into recipient mailboxes.
    pub queued: u64,
    /// Recipients whose overflow policy fired (drops already counted in
    /// `metrics.backpressure_drops`; Disconnect policy also initiated close).
    pub backpressured: u64,
    /// Stale/vanished ids (disconnected between listing and push) — benign.
    pub missing: u64,
}

/// Fan a payload out to the target, skipping `except`. The `Bytes` clone per
/// recipient is a refcount bump, never a copy.
pub fn broadcast(
    conns: &Registry,
    rooms: &RoomRegistry,
    identity: &IdentityRegistry,
    target: FanoutTarget<'_>,
    payload: Bytes,
    is_binary: bool,
    except: &[ConnectionId],
) -> FanoutReport {
    let mut report = FanoutReport::default();
    match target {
        FanoutTarget::Room(room) => {
            // Copy out + release the room guard before touching conn shards.
            // `record_and_members` also bumps the room's Phase 2A message counter
            // in this SAME lookup — no new lookup, no per-message work elsewhere.
            let Some(members) = rooms.record_and_members(room) else {
                return report;
            };
            fan_out_ids(conns, members, &payload, is_binary, except, &mut report);
        }
        FanoutTarget::User(user) => {
            // Same discipline as rooms: copy the device list out of the identity
            // shard, release the guard, THEN push into conn mailboxes.
            let Some(devices) = identity.connections(user) else {
                return report;
            };
            fan_out_ids(conns, devices, &payload, is_binary, except, &mut report);
        }
        FanoutTarget::All => {
            // Snapshot of live handles; collected under shard locks, pushed
            // outside them (Registry::handles contract).
            let handles = conns.handles();
            let except = Exclusions::new(except, handles.len());
            for (id, handle) in handles {
                if except.contains(id) {
                    continue;
                }
                push_one(&handle, &payload, is_binary, &mut report);
                report.attempted += 1;
            }
        }
    }
    report
}

/// Push `payload` into each id's mailbox (skipping `except`), tallying the
/// report. Shared by the Room and User targets — the member/device list has
/// already been copied out and its source guard released (lock invariant).
fn fan_out_ids(
    conns: &Registry,
    ids: Vec<ConnectionId>,
    payload: &Bytes,
    is_binary: bool,
    except: &[ConnectionId],
    report: &mut FanoutReport,
) {
    let except = Exclusions::new(except, ids.len());
    for id in ids {
        if except.contains(id) {
            continue;
        }
        match conns.get(id) {
            Some(handle) => push_one(&handle, payload, is_binary, report),
            None => report.missing += 1,
        }
        report.attempted += 1;
    }
}

/// Temporary, full-ID membership index. Keep tiny lists/targets allocation-free.
/// The recipient guard also bounds scratch to at most 8 bytes per recipient
/// and avoids sorting huge lists for a few probes (especially early hits).
/// Crossover evidence: docs/reports/adaptive-fanout-exclusions.md.
enum Exclusions<'a> {
    Linear(&'a [ConnectionId]),
    Sorted(Vec<ConnectionId>),
}

impl<'a> Exclusions<'a> {
    fn new(except: &'a [ConnectionId], recipients: usize) -> Self {
        if except.len() >= 32 && recipients >= 64 && recipients >= except.len() {
            let mut sorted = except.to_vec();
            sorted.sort_unstable_by_key(|id| id.0);
            Self::Sorted(sorted)
        } else {
            Self::Linear(except)
        }
    }

    fn contains(&self, id: ConnectionId) -> bool {
        match self {
            Self::Linear(except) => except.contains(&id),
            Self::Sorted(except) => except.binary_search_by_key(&id.0, |x| x.0).is_ok(),
        }
    }
}

fn push_one(handle: &ConnHandle, payload: &Bytes, is_binary: bool, report: &mut FanoutReport) {
    match handle.push(OutboundFrame {
        data: payload.clone(), // refcount bump — THE point of this module
        is_binary,
    }) {
        PushOutcome::Queued => report.queued += 1,
        PushOutcome::DroppedNewest | PushOutcome::DroppedOldest | PushOutcome::Disconnect => {
            report.backpressured += 1
        }
        PushOutcome::Closed => report.missing += 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn index_only_allocates_when_both_cardinalities_justify_it() {
        for exclusions in [0, 1, 31, 32, 33, 63, 64, 65, 127, 128] {
            let except = vec![ConnectionId::new(1, 2, 3); exclusions];
            for recipients in [0, 1, 31, 32, 63, 64, 65, 127, 128] {
                let index = Exclusions::new(&except, recipients);
                let should_sort = exclusions >= 32 && recipients >= 64 && recipients >= exclusions;
                assert_eq!(matches!(index, Exclusions::Sorted(_)), should_sort);
                if let Exclusions::Linear(slice) = index {
                    assert_eq!(slice.as_ptr(), except.as_ptr(), "small path must borrow");
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn adaptive_membership_matches_slice_for_full_width_ids(
            raw in proptest::collection::vec(any::<u64>(), 0..256),
            probes in proptest::collection::vec(any::<u64>(), 0..128),
        ) {
            let except: Vec<_> = raw.iter().copied().map(ConnectionId).collect();
            for recipients in [0, 1, 63, 64, 128, 256, 4096] {
                let index = Exclusions::new(&except, recipients);
                for id in raw.iter().chain(&probes).copied().map(ConnectionId) {
                    prop_assert_eq!(index.contains(id), except.contains(&id));
                }
            }
            prop_assert_eq!(except.iter().map(|id| id.0).collect::<Vec<_>>(), raw);
        }
    }
}
