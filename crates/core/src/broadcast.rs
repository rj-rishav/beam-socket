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
use crate::connection::{ConnHandle, CLOSE_BACKPRESSURE};
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
            let Some(members) = rooms.members(room) else {
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
            for (id, handle) in conns.handles() {
                if except.contains(&id) {
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
    for id in ids {
        if except.contains(&id) {
            continue;
        }
        match conns.get(id) {
            Some(handle) => push_one(&handle, payload, is_binary, report),
            None => report.missing += 1,
        }
        report.attempted += 1;
    }
}

fn push_one(handle: &ConnHandle, payload: &Bytes, is_binary: bool, report: &mut FanoutReport) {
    match handle.mailbox.push(OutboundFrame {
        data: payload.clone(), // refcount bump — THE point of this module
        is_binary,
    }) {
        PushOutcome::Queued => report.queued += 1,
        PushOutcome::DroppedNewest | PushOutcome::DroppedOldest => report.backpressured += 1,
        PushOutcome::Disconnect => {
            handle.initiate_close(CLOSE_BACKPRESSURE, "backpressure", true);
            report.backpressured += 1;
        }
        PushOutcome::Closed => report.missing += 1,
    }
}
