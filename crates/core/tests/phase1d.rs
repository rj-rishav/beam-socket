//! Phase 1D required tests (docs/ENGINEERING.md §8):
//! - Presence ⇄ room membership agreement after any churn (the property test
//!   shared with 1B — presence must report exactly the live members of a room,
//!   each with the userId it was bound to, and never a disconnected one).
//! - Soak (ignored by default; run with `--ignored`): a churn+broadcast
//!   workload for a bounded window, recording RSS + counter stability. The full
//!   10-minute soak is a pinned-box release blocker.
//!
//! close()/drain and the clean-process-exit proof are exercised end-to-end from
//! JS (packages/beamsocket/__tests__/runtime.integration.test.mjs) — they need
//! the real napi ThreadsafeFunction, which does not exist in a pure-Rust test.

// Tests build a default Config and mutate individual fields on purpose.
#![allow(clippy::field_reassign_with_default)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use beamsocket_core::config::BackpressurePolicy;
use beamsocket_core::connection::backpressure::Mailbox;
use beamsocket_core::connection::registry::Registry;
use beamsocket_core::connection::{CloseSignal, ConnHandle, Control, CONTROL_QUEUE_CAPACITY};
use beamsocket_core::ids::{ConnectionId, RoomId, UserId};
use beamsocket_core::metrics::Metrics;
use beamsocket_core::presence::{LocalPresence, PresenceStore};
use beamsocket_core::rooms::RoomRegistry;

use proptest::prelude::*;
use tokio::sync::mpsc;

fn mk_handle(metrics: &Arc<Metrics>) -> ConnHandle {
    let (control, _rx) = mpsc::channel::<Control>(CONTROL_QUEUE_CAPACITY);
    let (close, _close_rx) = CloseSignal::new();
    ConnHandle {
        mailbox: Mailbox::new(1024, BackpressurePolicy::DropNewest, metrics.clone()),
        control,
        close,
    }
}

fn room(n: u8) -> RoomId {
    RoomId(format!("room-{n}"))
}

#[derive(Debug, Clone)]
enum Op {
    Join(u8, u8),
    Leave(u8, u8),
    Disconnect(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..8u8, 0..5u8).prop_map(|(c, r)| Op::Join(c, r)),
        (0..8u8, 0..5u8).prop_map(|(c, r)| Op::Leave(c, r)),
        (0..8u8).prop_map(Op::Disconnect),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn presence_agrees_with_membership_and_identity(ops in proptest::collection::vec(op_strategy(), 0..200)) {
        let metrics = Arc::new(Metrics::default());
        let conns = Registry::new();
        let rooms = RoomRegistry::new();

        // 8 connections; even ids are bound to a user, odd ids are anonymous.
        let mut users: HashMap<ConnectionId, Option<UserId>> = HashMap::new();
        let mut ids: Vec<Option<ConnectionId>> = (0..8u8)
            .map(|i| {
                let user = (i % 2 == 0).then(|| UserId(format!("user-{i}")));
                let id = conns.insert(mk_handle(&metrics), user.clone());
                users.insert(id, user);
                Some(id)
            })
            .collect();

        for op in &ops {
            match *op {
                Op::Join(c, r) => {
                    if let Some(id) = ids[c as usize] {
                        rooms.join(&conns, id, room(r), 0);
                    }
                }
                Op::Leave(c, r) => {
                    if let Some(id) = ids[c as usize] {
                        rooms.leave(&conns, id, &room(r));
                    }
                }
                Op::Disconnect(c) => {
                    if let Some(id) = ids[c as usize].take() {
                        let (_, joined) = conns.remove_full(id).expect("live conn");
                        rooms.disconnect_cleanup(id, joined);
                    }
                }
            }
        }

        // For every room, presence must equal exactly its live members, each
        // carrying the userId it was bound to.
        for r in 0..5u8 {
            let rid = room(r);
            let presence = LocalPresence.room_presence(&rooms, &conns, &rid);

            // Build the expected view from room membership ∩ live conns.
            let expected: HashSet<(ConnectionId, Option<UserId>)> = match rooms.members(&rid) {
                Some(members) => members
                    .into_iter()
                    // A member still in the room map but removed from the
                    // registry (should not happen post-cleanup, but the filter
                    // is the presence contract) is excluded.
                    .filter(|id| conns.user_of(*id).is_some())
                    .map(|id| (id, users.get(&id).cloned().flatten()))
                    .collect(),
                None => HashSet::new(),
            };

            let got: HashSet<(ConnectionId, Option<UserId>)> =
                presence.into_iter().map(|e| (e.id, e.user)).collect();

            prop_assert_eq!(got.clone(), expected, "presence disagrees for {:?}", rid);

            // No presence entry may name a disconnected connection.
            for (id, _) in &got {
                prop_assert!(conns.user_of(*id).is_some(), "presence lists a gone conn {:?}", id);
            }
        }
    }
}

// ─────────────────────────────── soak (ignored) ───────────────────────────

/// A churn + broadcast workload for a bounded window (default ~15 s here; the
/// harness accepts `BEAM_SOAK_SECS`). Records RSS at start/end and asserts the
/// registry drains to empty — a leak would show as monotonic RSS growth AND a
/// nonzero residual count. The full 10-minute soak at 80% ceiling on the pinned
/// box is a release blocker (ENGINEERING.md §8).
#[test]
#[ignore = "soak — run with `--ignored --nocapture` (set BEAM_SOAK_SECS to lengthen)"]
fn soak_churn_and_broadcast_rss_stable() {
    let secs: u64 = std::env::var("BEAM_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);
    let metrics = Arc::new(Metrics::default());
    let conns = Registry::new();
    let rooms = RoomRegistry::new();
    let room0 = RoomId("soak".into());

    // Warm a steady population so RSS is past its initial reservations.
    let steady: Vec<ConnectionId> = (0..2_000)
        .map(|_| {
            let id = conns.insert(mk_handle(&metrics), Some(UserId("steady".into())));
            rooms.join(&conns, id, room0.clone(), 0);
            id
        })
        .collect();

    let rss_start = rss_kb();
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut churned: u64 = 0;
    while Instant::now() < deadline {
        // Broadcast into the steady room (fan-out over 2k members). The members'
        // mailboxes are bounded (HWM 1024, DropNewest), so with no writer task
        // draining them they cap-and-drop rather than grow — exactly the Rule 5
        // behavior a soak should confirm stays flat.
        let report = beamsocket_core::broadcast::broadcast(
            &conns,
            &rooms,
            &beamsocket_core::identity::IdentityRegistry::new(),
            beamsocket_core::broadcast::FanoutTarget::Room(&room0),
            bytes::Bytes::from_static(b"soak payload of a few dozen bytes...."),
            false,
            &[],
        );
        std::hint::black_box(report);

        // Churn: a batch of short-lived connections join/leave/disconnect.
        for _ in 0..500 {
            let id = conns.insert(mk_handle(&metrics), Some(UserId("churn".into())));
            rooms.join(&conns, id, room0.clone(), 0);
            let (_, joined) = conns.remove_full(id).unwrap();
            rooms.disconnect_cleanup(id, joined);
            churned += 1;
        }
    }
    let rss_end = rss_kb();

    // Tear down the steady population; the room must auto-destroy.
    for id in steady {
        if let Some((_, joined)) = conns.remove_full(id) {
            rooms.disconnect_cleanup(id, joined);
        }
    }
    println!(
        "soak {secs}s: churned {churned} conns; RSS {rss_start}→{rss_end} KB (Δ {} KB); rooms left {}",
        rss_end as i64 - rss_start as i64,
        rooms.room_count()
    );
    assert_eq!(conns.len(), 0, "steady population leaked");
    assert_eq!(rooms.room_count(), 0, "rooms leaked");
}

/// Measured anchor for the benchmarks/README memory table: the Rust-side
/// per-connection BOOKKEEPING heap — the registry entry + `ConnHandle`
/// (mailbox Arc, control channel, close watch) + a bound identity entry. Does
/// NOT include the codec read buffer, the Tokio task future, or kernel socket
/// buffers (those need a real socket; see the density benchmark).
#[test]
#[ignore = "measurement — run with `--ignored --nocapture` for the memory-table anchor"]
fn engine_bookkeeping_memory_cost() {
    use beamsocket_core::identity::IdentityRegistry;

    const N: u64 = 200_000;
    let metrics = Arc::new(Metrics::default());
    let conns = Registry::new();
    let identity = IdentityRegistry::new();

    let before = rss_kb();
    let mut ids = Vec::with_capacity(N as usize);
    for i in 0..N {
        let user = UserId(format!("user-{i}"));
        let id = conns.insert(mk_handle(&metrics), Some(user.clone()));
        identity.bind(user, id);
        ids.push(id);
    }
    let after = rss_kb();
    std::hint::black_box(&ids);
    println!(
        "engine bookkeeping: {N} conns → {:.0} B/conn (registry entry + ConnHandle + identity entry, distinct users)",
        (after - before) as f64 * 1024.0 / N as f64
    );
}

fn rss_kb() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    pages * 4 // 4 KB pages → KB
}
