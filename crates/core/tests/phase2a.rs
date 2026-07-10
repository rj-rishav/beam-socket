//! Phase 2A observability — core tests (ENGINEERING.md §12.1).
//!
//! `topRooms` vs a reference model under churn (extends the 1B strategy),
//! `room().info()`, and the sampler on/off wiring. The engine-level queries that
//! need live connections (`backpressureReport`, `memoryUsage`, rate rise/decay,
//! caps, `metricsText`, and the perf-regression guard) live in the JS
//! integration test, which can drive real sockets + a slowed consumer.

#![allow(clippy::field_reassign_with_default)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use beamsocket_core::config::{BackpressurePolicy, Config};
use beamsocket_core::connection::backpressure::Mailbox;
use beamsocket_core::connection::registry::Registry;
use beamsocket_core::connection::{CloseSignal, ConnHandle, Control, CONTROL_QUEUE_CAPACITY};
use beamsocket_core::engine::Engine;
use beamsocket_core::ids::{ConnectionId, RoomId};
use beamsocket_core::metrics::Metrics;
use beamsocket_core::rooms::{RoomRegistry, RoomStat};

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

/// Reference top-N: rank by members desc, then messages desc, then room name
/// ASC — the same total order `RoomStat: Ord` defines.
fn reference_top(model: &HashMap<RoomId, (HashSet<ConnectionId>, u64)>, n: usize) -> Vec<RoomStat> {
    let mut v: Vec<RoomStat> = model
        .iter()
        .map(|(r, (members, messages))| RoomStat {
            room: r.0.clone(),
            members: members.len(),
            messages: *messages,
        })
        .collect();
    v.sort_by(|a, b| {
        b.members
            .cmp(&a.members)
            .then(b.messages.cmp(&a.messages))
            .then(a.room.cmp(&b.room))
    });
    v.truncate(n);
    v
}

#[derive(Debug, Clone)]
enum Op {
    Join(u8, u8),
    Leave(u8, u8),
    Disconnect(u8),
    Broadcast(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..8u8, 0..6u8).prop_map(|(c, r)| Op::Join(c, r)),
        (0..8u8, 0..6u8).prop_map(|(c, r)| Op::Leave(c, r)),
        (0..8u8).prop_map(Op::Disconnect),
        (0..6u8).prop_map(Op::Broadcast),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn top_rooms_matches_reference(
        ops in proptest::collection::vec(op_strategy(), 0..300),
        n in 1usize..12,
    ) {
        let metrics = Arc::new(Metrics::default());
        let conns = Registry::new();
        let rooms = RoomRegistry::new();
        let mut ids: Vec<Option<ConnectionId>> =
            (0..8).map(|_| Some(conns.insert(mk_handle(&metrics), None))).collect();

        // Reference: room -> (members, cumulative messages). Empty rooms are
        // removed (mirrors auto-destroy → a re-created room resets its counter).
        let mut model: HashMap<RoomId, (HashSet<ConnectionId>, u64)> = HashMap::new();

        for op in &ops {
            match *op {
                Op::Join(c, r) => {
                    if let Some(id) = ids[c as usize] {
                        rooms.join(&conns, id, room(r), 0);
                        model.entry(room(r)).or_default().0.insert(id);
                    }
                }
                Op::Leave(c, r) => {
                    if let Some(id) = ids[c as usize] {
                        rooms.leave(&conns, id, &room(r));
                        if let Some(e) = model.get_mut(&room(r)) {
                            e.0.remove(&id);
                            if e.0.is_empty() {
                                model.remove(&room(r));
                            }
                        }
                    }
                }
                Op::Disconnect(c) => {
                    if let Some(id) = ids[c as usize].take() {
                        let (_, joined) = conns.remove_full(id).unwrap();
                        rooms.disconnect_cleanup(id, joined);
                        model.retain(|_, e| {
                            e.0.remove(&id);
                            !e.0.is_empty()
                        });
                    }
                }
                Op::Broadcast(r) => {
                    // record_and_members bumps the counter IFF the room exists;
                    // the model mirrors that condition.
                    let hit = rooms.record_and_members(&room(r)).is_some();
                    if let Some(e) = model.get_mut(&room(r)) {
                        prop_assert!(hit, "registry has room {:?} but model bump missed", room(r));
                        e.1 += 1;
                    } else {
                        prop_assert!(!hit, "registry bumped a room the model thinks is gone");
                    }
                }
            }
        }

        let got = rooms.top_rooms(n);
        let want = reference_top(&model, n);
        prop_assert_eq!(got, want);
    }
}

#[test]
fn room_info_reports_members_messages_and_existence() {
    let metrics = Arc::new(Metrics::default());
    let conns = Registry::new();
    let rooms = RoomRegistry::new();
    let a = conns.insert(mk_handle(&metrics), None);
    let b = conns.insert(mk_handle(&metrics), None);
    rooms.join(&conns, a, room(1), 0);
    rooms.join(&conns, b, room(1), 0);
    rooms.record_and_members(&room(1)); // +1 message
    rooms.record_and_members(&room(1)); // +1 message

    let info = rooms.info(&room(1));
    assert_eq!(info.members, 2);
    assert_eq!(info.messages, 2);

    // A room nobody is in does not exist (auto-destroyed) → zeroes.
    let gone = rooms.info(&room(9));
    assert_eq!(gone.members, 0);
    assert_eq!(gone.messages, 0);
}

#[test]
fn top_rooms_zero_is_empty() {
    let rooms = RoomRegistry::new();
    assert!(rooms.top_rooms(0).is_empty());
}

#[test]
fn sampler_on_off_controls_rates_presence() {
    // Default (sampler_ms = 1000) → rates present.
    let (engine_on, _rx) = Engine::start(Config::default(), 1024, false).unwrap();
    assert!(engine_on.rates().is_some(), "sampler on → rates present");
    drop(engine_on);

    // sampler_ms = 0 → no sampler task, no rates.
    let mut cfg = Config::default();
    cfg.observability.sampler_ms = 0;
    let (engine_off, _rx) = Engine::start(cfg, 1024, false).unwrap();
    assert!(engine_off.rates().is_none(), "sampler off → no rates");
    drop(engine_off);
}
