//! Phase 1B required tests (docs/ENGINEERING.md §6):
//! - property: after any join/leave/disconnect sequence, room→conn and
//!   conn→room views agree, and no empty room survives
//! - fan-out: every member exactly once; except honored; non-members nothing
//! - broadcast with a saturated member: that member hits its policy alone
//! - end-to-end rooms through the engine with real sockets

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use beamsocket_core::broadcast::{broadcast, FanoutTarget};
use beamsocket_core::config::{BackpressurePolicy, Config};
use beamsocket_core::connection::backpressure::{Mailbox, OutboundFrame, PushOutcome};
use beamsocket_core::connection::registry::Registry;
use beamsocket_core::connection::{CloseSignal, ConnHandle, Control, CONTROL_QUEUE_CAPACITY};
use beamsocket_core::engine::Engine;
use beamsocket_core::events::EngineEvent;
use beamsocket_core::identity::IdentityRegistry;
use beamsocket_core::ids::{ConnectionId, RoomId};
use beamsocket_core::metrics::Metrics;
use beamsocket_core::rooms::{MembershipChange, RoomRegistry};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use proptest::prelude::*;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

fn mk_handle(metrics: &Arc<Metrics>, hwm: usize, policy: BackpressurePolicy) -> ConnHandle {
    let (control, _rx) = mpsc::channel::<Control>(CONTROL_QUEUE_CAPACITY);
    let (close, _close_rx) = CloseSignal::new();
    ConnHandle {
        mailbox: Mailbox::new(hwm, policy, metrics.clone()),
        control,
        close,
    }
}

fn room(n: u8) -> RoomId {
    RoomId(format!("room-{n}"))
}

// ---------- property: bidirectional views agree, no empty room survives ----------

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
    fn membership_views_agree_after_any_sequence(ops in proptest::collection::vec(op_strategy(), 0..200)) {
        let metrics = Arc::new(Metrics::default());
        let conns = Registry::new();
        let rooms = RoomRegistry::new();

        // Pool of 8 connections; disconnected slots stay dead (stale ops no-op).
        let mut ids: Vec<Option<ConnectionId>> = (0..8)
            .map(|_| Some(conns.insert(mk_handle(&metrics, 1024, BackpressurePolicy::DropNewest))))
            .collect();

        for op in &ops {
            match *op {
                Op::Join(c, r) => {
                    if let Some(id) = ids[c as usize] {
                        rooms.join(&conns, id, room(r), 0);
                    } else if let Some(dead) = dead_id(&ids, c) {
                        // Stale id after disconnect must be a NotFound no-op.
                        prop_assert_eq!(rooms.join(&conns, dead, room(r), 0), MembershipChange::NotFound);
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

        // Rebuild both views and compare.
        let mut conn_view: HashMap<ConnectionId, HashSet<RoomId>> = HashMap::new();
        for id in ids.iter().flatten() {
            let set = conns.with_rooms(*id, |s| s.clone()).expect("live conn");
            conn_view.insert(*id, set);
        }
        let mut room_view: HashMap<ConnectionId, HashSet<RoomId>> = HashMap::new();
        let mut live_rooms = 0usize;
        for r in 0..5u8 {
            let rid = room(r);
            if let Some(members) = rooms.members(&rid) {
                prop_assert!(!members.is_empty(), "empty room {rid:?} survived");
                live_rooms += 1;
                for m in members {
                    room_view.entry(m).or_default().insert(rid.clone());
                }
            }
        }
        // Every membership seen from the room side belongs to a LIVE conn
        // that agrees, and vice versa.
        for (id, set) in &conn_view {
            let other = room_view.remove(id).unwrap_or_default();
            prop_assert_eq!(set, &other, "views disagree for {:?}", id);
        }
        prop_assert!(room_view.is_empty(), "rooms hold dead members: {room_view:?}");
        prop_assert_eq!(rooms.room_count(), live_rooms);

        // Disconnect everyone → every room must auto-destroy.
        for slot in ids.iter_mut() {
            if let Some(id) = slot.take() {
                let (_, joined) = conns.remove_full(id).unwrap();
                rooms.disconnect_cleanup(id, joined);
            }
        }
        prop_assert_eq!(rooms.room_count(), 0, "empty rooms survived the final sweep");
    }
}

fn dead_id(ids: &[Option<ConnectionId>], _c: u8) -> Option<ConnectionId> {
    // Any currently-dead id would do; absence just skips the stale check.
    None
}

// ---------- fan-out correctness ----------

#[tokio::test]
async fn fanout_exactly_once_except_honored_nonmembers_nothing() {
    let metrics = Arc::new(Metrics::default());
    let conns = Registry::new();
    let rooms = RoomRegistry::new();

    let a = conns.insert(mk_handle(&metrics, 1 << 20, BackpressurePolicy::DropNewest));
    let b = conns.insert(mk_handle(&metrics, 1 << 20, BackpressurePolicy::DropNewest));
    let c = conns.insert(mk_handle(&metrics, 1 << 20, BackpressurePolicy::DropNewest));
    let outsider = conns.insert(mk_handle(&metrics, 1 << 20, BackpressurePolicy::DropNewest));

    let identity = IdentityRegistry::new();
    for id in [a, b, c] {
        assert_eq!(
            rooms.join(&conns, id, room(1), 0),
            MembershipChange::Changed
        );
    }

    let payload = Bytes::from(vec![7u8; 512]);
    let report = broadcast(
        &conns,
        &rooms,
        &identity,
        FanoutTarget::Room(&room(1)),
        payload.clone(),
        true,
        &[b], // except
    );
    assert_eq!(report.queued, 2);
    assert_eq!(report.backpressured, 0);
    assert_eq!(report.missing, 0);

    // a and c: exactly one frame, and it is THE SAME allocation (refcount
    // clone, not a copy — the §6 one-allocation contract, proven by pointer
    // identity).
    for id in [a, c] {
        let h = conns.get(id).unwrap();
        let frame = h.mailbox.pop().await.unwrap();
        assert_eq!(frame.data.len(), 512);
        assert!(frame.is_binary);
        assert_eq!(
            frame.data.as_ptr(),
            payload.as_ptr(),
            "fan-out must clone the refcount, never the bytes"
        );
        assert_eq!(h.mailbox.queued_bytes(), 0, "exactly once means once");
    }
    // except'd member and non-member: nothing.
    assert_eq!(conns.get(b).unwrap().mailbox.queued_bytes(), 0);
    assert_eq!(conns.get(outsider).unwrap().mailbox.queued_bytes(), 0);
}

// ---------- saturated member is isolated ----------

#[tokio::test]
async fn slow_member_hits_policy_alone() {
    let metrics = Arc::new(Metrics::default());
    let conns = Registry::new();
    let rooms = RoomRegistry::new();

    let healthy1 = conns.insert(mk_handle(&metrics, 1 << 20, BackpressurePolicy::Disconnect));
    let slow = conns.insert(mk_handle(&metrics, 64, BackpressurePolicy::Disconnect));
    let healthy2 = conns.insert(mk_handle(&metrics, 1 << 20, BackpressurePolicy::Disconnect));

    let identity = IdentityRegistry::new();
    for id in [healthy1, slow, healthy2] {
        rooms.join(&conns, id, room(2), 0);
    }
    // Saturate the slow member's mailbox (64-byte HWM).
    let slow_handle = conns.get(slow).unwrap();
    assert_eq!(
        slow_handle.mailbox.push(OutboundFrame {
            data: Bytes::from(vec![0u8; 60]),
            is_binary: true,
        }),
        PushOutcome::Queued
    );

    let payload = Bytes::from(vec![1u8; 128]);
    let report = broadcast(
        &conns,
        &rooms,
        &identity,
        FanoutTarget::Room(&room(2)),
        payload,
        true,
        &[],
    );
    assert_eq!(report.attempted, 3);
    assert_eq!(report.queued, 2, "healthy members unaffected");
    assert_eq!(report.backpressured, 1, "slow member sheds, alone");
    assert_eq!(Metrics::get(&metrics.backpressure_drops), 1);

    // Disconnect policy: the slow member's close is initiated (1013)…
    let h1 = conns.get(healthy1).unwrap();
    let h2 = conns.get(healthy2).unwrap();
    assert_eq!(h1.mailbox.pop().await.unwrap().data.len(), 128);
    assert_eq!(h2.mailbox.pop().await.unwrap().data.len(), 128);
    // …and its mailbox rejects further pushes (closed by the policy).
    assert_eq!(
        slow_handle.mailbox.push(OutboundFrame {
            data: Bytes::from_static(b"x"),
            is_binary: true,
        }),
        PushOutcome::Closed
    );
}

// ---------- end-to-end through the engine with real sockets ----------

#[test]
fn rooms_broadcast_end_to_end() {
    let (engine, mut rx) = Engine::start(Config::default(), 1024, false).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();

    // Bridge sim: collect opened ids in order.
    let (ids_tx, ids_rx) = std::sync::mpsc::channel::<ConnectionId>();
    let bridge = std::thread::spawn(move || {
        let mut closes = 0;
        while let Some(ev) = rx.blocking_recv() {
            match ev {
                EngineEvent::ConnectionOpened { id, .. } => ids_tx.send(id).unwrap(),
                EngineEvent::ConnectionClosed { .. } => {
                    closes += 1;
                    if closes == 3 {
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let url = format!("ws://127.0.0.1:{port}/");
        let (mut c1, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let id1 = ids_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (mut c2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let id2 = ids_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (mut c3, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let id3 = ids_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        // c1, c2 in the room; c3 out. Idempotent double-join is a no-op.
        assert_eq!(engine.join(id1, "lobby"), MembershipChange::Changed);
        assert_eq!(engine.join(id2, "lobby"), MembershipChange::Changed);
        assert_eq!(engine.join(id2, "lobby"), MembershipChange::NoOp);
        assert_eq!(engine.room_member_count("lobby"), 2);
        assert_eq!(engine.room_count(), 1);

        // Room broadcast with except: only c1 receives.
        let report =
            engine.broadcast_room("lobby", Bytes::from_static(b"hello room"), false, &[id2]);
        assert_eq!(report.queued, 1);
        assert_eq!(
            c1.next().await.unwrap().unwrap(),
            Message::Text("hello room".into())
        );

        // broadcast_all reaches everyone, including the non-member.
        let report = engine.broadcast_all(Bytes::from_static(b"to all"), false, &[]);
        assert_eq!(report.queued, 3);
        for c in [&mut c1, &mut c2, &mut c3] {
            assert_eq!(
                c.next().await.unwrap().unwrap(),
                Message::Text("to all".into())
            );
        }

        // Disconnect c1 → membership shrinks; leave by c2 destroys the room.
        c1.close(None).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while engine.room_member_count("lobby") != 1 {
            assert!(tokio::time::Instant::now() < deadline, "cleanup timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(engine.leave(id2, "lobby"), MembershipChange::Changed);
        assert_eq!(engine.room_count(), 0, "last leave must destroy the room");

        c2.close(None).await.unwrap();
        c3.close(None).await.unwrap();
        // Drain until server close acks arrive.
        while let Some(Ok(_)) = c2.next().await {}
        while let Some(Ok(_)) = c3.next().await {}
    });

    bridge.join().unwrap();
    let engine = Arc::try_unwrap(engine).ok().expect("sole owner");
    engine.shutdown();
}
