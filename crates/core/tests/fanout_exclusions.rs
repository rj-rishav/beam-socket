//! Differential regression tests against the frozen pre-optimization fan-out.
//! Pressure, stale entries and missing targets intentionally go beyond benches.
use std::sync::Arc;

use beamsocket_core::{connection, identity, ids, metrics, rooms};
#[path = "../benches/support/broadcast_linear.rs"]
mod baseline;

use beamsocket_core::broadcast::{broadcast, FanoutTarget};
use beamsocket_core::config::BackpressurePolicy;
use bytes::Bytes;
use connection::backpressure::{Mailbox, OutboundFrame, PushOutcome};
use connection::registry::Registry;
use connection::{CloseCmd, CloseSignal, ConnHandle, Control};
use identity::IdentityRegistry;
use ids::{ConnectionId, RoomId, UserId};
use metrics::Metrics;
use rooms::RoomRegistry;
use tokio::sync::watch;

type CloseState = Option<(u16, String, bool)>;
type QueuedFrames = Vec<(Bytes, bool)>;

struct Fixture {
    conns: Registry,
    rooms: RoomRegistry,
    identity: IdentityRegistry,
    metrics: Arc<Metrics>,
    ids: Vec<ConnectionId>,
    handles: Vec<ConnHandle>,
    closes: Vec<watch::Receiver<Option<CloseCmd>>>,
    room: RoomId,
    user: UserId,
}

impl Fixture {
    fn new(pressure: bool) -> Self {
        let mut f = Self {
            conns: Registry::new(),
            rooms: RoomRegistry::new(),
            identity: IdentityRegistry::new(),
            metrics: Arc::new(Metrics::default()),
            ids: Vec::new(),
            handles: Vec::new(),
            closes: Vec::new(),
            room: RoomId("members".into()),
            user: UserId("devices".into()),
        };
        for i in 0..256 {
            let policy = match i % 3 {
                0 => BackpressurePolicy::DropNewest,
                1 => BackpressurePolicy::DropOldest,
                _ => BackpressurePolicy::Disconnect,
            };
            let (control, _) = tokio::sync::mpsc::channel::<Control>(4);
            let (close, rx) = CloseSignal::new();
            let handle = ConnHandle {
                mailbox: Mailbox::new(8, policy, f.metrics.clone()),
                control,
                close,
            };
            let id = f.conns.insert(handle.clone(), None);
            if i < 128 {
                f.rooms.join(&f.conns, id, f.room.clone(), 0);
                f.identity.bind(f.user.clone(), id);
            }
            if pressure && i % 5 == 0 {
                assert_eq!(
                    handle.mailbox.push(OutboundFrame {
                        data: Bytes::from_static(b"prefill"),
                        is_binary: false,
                    }),
                    PushOutcome::Queued
                );
            }
            if pressure && i % 17 == 0 {
                handle.mailbox.close();
            }
            f.ids.push(id);
            f.handles.push(handle);
            f.closes.push(rx);
        }
        // Simulate a member that disappeared between snapshot and resolution.
        // Intentionally skip normal room/user cleanup to retain a stale entry.
        f.conns.remove(f.ids[0]).unwrap();
        f.conns.remove(f.ids[1]).unwrap();
        // Round-robin returns to shard 0: a real slot reuse with a new generation.
        let (control, _) = tokio::sync::mpsc::channel::<Control>(4);
        let (close, rx) = CloseSignal::new();
        let handle = ConnHandle {
            mailbox: Mailbox::new(8, BackpressurePolicy::DropNewest, f.metrics.clone()),
            control,
            close,
        };
        let id = f.conns.insert(handle.clone(), None);
        assert_eq!(id.key(), f.ids[0].key());
        assert_eq!(id.shard(), f.ids[0].shard());
        assert_ne!(id.generation(), f.ids[0].generation());
        f.rooms.join(&f.conns, id, f.room.clone(), 0);
        f.identity.bind(f.user.clone(), id);
        f.ids.push(id);
        f.handles.push(handle);
        f.closes.push(rx);
        f
    }

    fn run(
        &self,
        target: &str,
        except: &[ConnectionId],
        payload: &Bytes,
        binary: bool,
        old: bool,
    ) -> [u64; 4] {
        let missing_room = RoomId("absent".into());
        let missing_user = UserId("absent".into());
        if old {
            let target = match target {
                "room" => baseline::FanoutTarget::Room(&self.room),
                "user" => baseline::FanoutTarget::User(&self.user),
                "missing-room" => baseline::FanoutTarget::Room(&missing_room),
                "missing-user" => baseline::FanoutTarget::User(&missing_user),
                _ => baseline::FanoutTarget::All,
            };
            let r = baseline::broadcast(
                &self.conns,
                &self.rooms,
                &self.identity,
                target,
                payload.clone(),
                binary,
                except,
            );
            [r.attempted, r.queued, r.backpressured, r.missing]
        } else {
            let target = match target {
                "room" => FanoutTarget::Room(&self.room),
                "user" => FanoutTarget::User(&self.user),
                "missing-room" => FanoutTarget::Room(&missing_room),
                "missing-user" => FanoutTarget::User(&missing_user),
                _ => FanoutTarget::All,
            };
            let r = broadcast(
                &self.conns,
                &self.rooms,
                &self.identity,
                target,
                payload.clone(),
                binary,
                except,
            );
            [r.attempted, r.queued, r.backpressured, r.missing]
        }
    }

    async fn snapshot(&self, payload: &Bytes) -> Vec<(QueuedFrames, CloseState)> {
        let mut out = Vec::new();
        for (h, rx) in self.handles.iter().zip(&self.closes) {
            let mut frames = Vec::new();
            h.mailbox.close(); // finite drain, including closed/empty queues
            while let Some(frame) = h.mailbox.pop().await {
                if frame.data == *payload {
                    assert_eq!(frame.data.as_ptr(), payload.as_ptr(), "payload copied");
                }
                frames.push((frame.data, frame.is_binary));
            }
            let close = rx
                .borrow()
                .as_ref()
                .map(|c| (c.code, c.reason.clone(), c.graceful));
            out.push((frames, close));
        }
        out
    }
}

#[tokio::test]
async fn all_targets_match_linear_reference_with_pressure_stale_ids_and_duplicates() {
    for target in ["room", "user", "all", "missing-room", "missing-user"] {
        for size in [0, 1, 31, 32, 63, 64, 128, 129, 256, 1024] {
            for pressure in [false, true] {
                let old = Fixture::new(pressure);
                let new = Fixture::new(pressure);
                assert_eq!(old.ids, new.ids);
                let except: Vec<_> = (0..size)
                    .map(|i| match i % 4 {
                        0 => old.ids[i % 256],
                        1 => old.ids[0], // repeated STALE id must not hide its replacement
                        2 => ConnectionId::new(255, 0, i as u32),
                        _ => ConnectionId::new(0, 9, i as u32),
                    })
                    .collect();
                let before = except.clone();
                let payload = Bytes::from(vec![42; 8]);
                let a = old.run(target, &except, &payload, pressure, true);
                let b = new.run(target, &except, &payload, pressure, false);
                assert_eq!(a, b, "{target} E={size} pressure={pressure}");
                assert_eq!(a[0], a[1] + a[2] + a[3]);
                assert_eq!(except, before, "caller list was mutated");
                let expected = old.snapshot(&payload).await;
                let actual = new.snapshot(&payload).await;
                assert_eq!(expected, actual, "{target} E={size} pressure={pressure}");
                if !target.starts_with("missing") {
                    assert_eq!(
                        actual.last().unwrap().0,
                        vec![(payload.clone(), pressure)],
                        "stale exclusion hid replacement"
                    );
                } else {
                    assert_eq!(b, [0; 4]);
                }
                assert_eq!(
                    Metrics::get(&old.metrics.backpressure_drops),
                    Metrics::get(&new.metrics.backpressure_drops)
                );
                assert_eq!(old.rooms.info(&old.room), new.rooms.info(&new.room));
            }
        }
    }
}

#[tokio::test]
async fn excluding_every_member_still_records_one_room_message() {
    let f = Fixture::new(false);
    let payload = Bytes::from_static(b"data");
    for target in ["room", "user", "all"] {
        let except = if target == "all" {
            f.conns.handles().into_iter().map(|(id, _)| id).collect()
        } else {
            f.rooms.members(&f.room).unwrap()
        };
        assert_eq!(f.run(target, &except, &payload, true, false), [0; 4]);
    }
    assert_eq!(f.rooms.info(&f.room).messages, 1);
    assert!(f
        .snapshot(&payload)
        .await
        .iter()
        .all(|(frames, _)| frames.is_empty()));
}
