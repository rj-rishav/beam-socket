//! Same-process A/B of frozen linear fan-out and current production code.
//! Setup, snapshots, filtering, queue pushes and temporary index destruction
//! are timed. Mailbox draining/validation is outside the timed interval.
//! Run: cargo bench -p beamsocket-core --bench fanout_exclusions
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

// Both copies compile as local modules to avoid a cross-crate inlining bias.
use beamsocket_core::{connection, identity, ids, metrics, rooms};
#[path = "support/broadcast_linear.rs"]
mod baseline;
// harness=false still sets cfg(test), but drops #[test] functions in the
// included production module; their imports are consequently unused here.
#[allow(unused_imports)]
#[path = "../src/broadcast.rs"]
mod candidate;

use beamsocket_core::config::BackpressurePolicy;
use connection::backpressure::Mailbox;
use connection::registry::Registry;
use connection::{CloseSignal, ConnHandle, Control};
use identity::IdentityRegistry;
use ids::{ConnectionId, RoomId, UserId};
use metrics::Metrics;
use rooms::RoomRegistry;

struct Fixture {
    conns: Registry,
    rooms: RoomRegistry,
    identity: IdentityRegistry,
    ids: Vec<ConnectionId>,
    handles: Vec<ConnHandle>,
    room: RoomId,
    user: UserId,
    payload: bytes::Bytes,
}

impl Fixture {
    fn new(n: usize) -> Self {
        let conns = Registry::new();
        let rooms = RoomRegistry::new();
        let identity = IdentityRegistry::new();
        let room = RoomId("bench".into());
        let user = UserId("bench".into());
        let metrics = Arc::new(Metrics::default());
        let mut ids = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..n {
            let (control, _) = tokio::sync::mpsc::channel::<Control>(4);
            let (close, _) = CloseSignal::new();
            let handle = ConnHandle {
                mailbox: Mailbox::new(65536, BackpressurePolicy::DropNewest, metrics.clone()),
                control,
                close,
            };
            let id = conns.insert(handle.clone(), Some(user.clone()));
            rooms.join(&conns, id, room.clone(), 0);
            identity.bind(user.clone(), id);
            ids.push(id);
            handles.push(handle);
        }
        Self {
            conns,
            rooms,
            identity,
            ids,
            handles,
            room,
            user,
            payload: bytes::Bytes::from(vec![42; 64]),
        }
    }

    fn run(&self, target: &str, except: &[ConnectionId], old: bool) -> [u64; 4] {
        if old {
            let target = match target {
                "room" => baseline::FanoutTarget::Room(&self.room),
                "user" => baseline::FanoutTarget::User(&self.user),
                _ => baseline::FanoutTarget::All,
            };
            let r = baseline::broadcast(
                &self.conns,
                &self.rooms,
                &self.identity,
                target,
                self.payload.clone(),
                true,
                except,
            );
            [r.attempted, r.queued, r.backpressured, r.missing]
        } else {
            let target = match target {
                "room" => candidate::FanoutTarget::Room(&self.room),
                "user" => candidate::FanoutTarget::User(&self.user),
                _ => candidate::FanoutTarget::All,
            };
            let r = candidate::broadcast(
                &self.conns,
                &self.rooms,
                &self.identity,
                target,
                self.payload.clone(),
                true,
                except,
            );
            [r.attempted, r.queued, r.backpressured, r.missing]
        }
    }

    async fn drain(&self, selected: &[bool]) {
        for (handle, &selected) in self.handles.iter().zip(selected) {
            assert_eq!(handle.mailbox.queued_bytes(), if selected { 64 } else { 0 });
            if selected {
                let frame = handle.mailbox.pop().await.unwrap();
                assert_eq!(frame.data.as_ptr(), self.payload.as_ptr());
                assert!(frame.is_binary);
                assert_eq!(handle.mailbox.queued_bytes(), 0);
            }
        }
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    println!("target,recipients,exclusions,pattern,variant,round,iterations,ns_per_broadcast");
    for (n, e) in [
        (8, 4096),
        (64, 32),
        (128, 128),
        (1024, 0),
        (1024, 1),
        (1024, 32),
        (1024, 256),
        (4096, 1024),
        (16384, 4096),
    ] {
        let f = Fixture::new(n);
        for pattern in ["miss", "mixed", "duplicates", "all-hit"] {
            let except: Vec<_> = (0..e)
                .map(|i| match pattern {
                    "mixed" if i % 2 == 0 => f.ids[(i * 17) % n],
                    "duplicates" => f.ids[0],
                    "all-hit" => f.ids[i % n],
                    _ => ConnectionId::new((i % 16) as u8, 1, (i / 16) as u32),
                })
                .collect();
            let selected: Vec<_> = f.ids.iter().map(|id| !except.contains(id)).collect();
            let count = selected.iter().filter(|&&b| b).count() as u64;
            for target in ["room", "user", "all"] {
                let mut iterations = [0; 2];
                // Warm both variants and all mailbox allocations before measuring.
                for (v, old) in [true, false].into_iter().enumerate() {
                    let mut ns = 0;
                    for _ in 0..5 {
                        let start = Instant::now();
                        let r = f.run(target, black_box(&except), old);
                        ns += start.elapsed().as_nanos() as usize;
                        assert_eq!(r, [count, count, 0, 0]);
                        rt.block_on(f.drain(&selected));
                    }
                    iterations[v] = (3_000_000 / (ns / 5).max(1)).clamp(2, 500);
                }
                for round in 0..7 {
                    for step in 0..2 {
                        let v = (round + step) % 2;
                        let old = v == 0;
                        let mut ns = 0u128;
                        for _ in 0..iterations[v] {
                            let start = Instant::now();
                            let r = f.run(target, black_box(&except), old);
                            ns += start.elapsed().as_nanos();
                            assert_eq!(black_box(r), [count, count, 0, 0]);
                            rt.block_on(f.drain(&selected));
                        }
                        let variant = if old { "baseline" } else { "candidate" };
                        println!(
                            "{target},{n},{e},{pattern},{variant},{round},{},{:.2}",
                            iterations[v],
                            ns as f64 / iterations[v] as f64
                        );
                    }
                }
            }
        }
    }
}
