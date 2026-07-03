//! Bounded send queue ("mailbox") + high-water mark — Phase 1A.
//!
//! Every connection's outbound path goes through this queue. It is bounded in
//! BYTES by `config.backpressure.high_water_mark` (Rule 5) and the overflow
//! behavior is the configured `BackpressurePolicy`. Every overflow increments
//! `metrics.backpressure_drops` — never silent (RFC 0001 primary-gate
//! philosophy).
//!
//! Implementation note: a mutex-guarded VecDeque + Notify rather than an mpsc
//! channel because `DropOldest` requires popping from the producer side,
//! which channel APIs cannot do. Contention is two parties only (the JS
//! thread pushing, the writer task popping) — no hot-path global lock
//! (Rule 2: the lock is per-connection).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Notify;

use crate::config::BackpressurePolicy;
use crate::metrics::Metrics;

/// An outbound item for the writer task. `data` is refcounted: a broadcast
/// enqueues N clones of ONE allocation (ENGINEERING.md §6).
#[derive(Debug, Clone)]
pub struct OutboundFrame {
    pub data: Bytes,
    pub is_binary: bool,
}

/// Result of a push, so the caller can apply the policy's side effect
/// (`Disconnect` → initiate close; drops are already counted here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Queued,
    /// Policy `DropNewest`: this frame was discarded (counted).
    DroppedNewest,
    /// Policy `DropOldest`: older frame(s) were discarded (counted) and this
    /// frame was queued.
    DroppedOldest,
    /// Policy `Disconnect`: frame discarded (counted); caller must close the
    /// connection (close code 1013, "try again later").
    Disconnect,
    /// Mailbox already closed (connection tearing down).
    Closed,
}

struct Inner {
    queue: Mutex<Q>,
    notify: Notify,
    high_water_mark: usize,
    policy: BackpressurePolicy,
    metrics: Arc<Metrics>,
}

struct Q {
    items: VecDeque<OutboundFrame>,
    bytes: usize,
    closed: bool,
}

/// Cheaply clonable handle (Arc inside).
#[derive(Clone)]
pub struct Mailbox {
    inner: Arc<Inner>,
}

impl Mailbox {
    pub fn new(high_water_mark: usize, policy: BackpressurePolicy, metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Arc::new(Inner {
                queue: Mutex::new(Q {
                    items: VecDeque::new(),
                    bytes: 0,
                    closed: false,
                }),
                notify: Notify::new(),
                high_water_mark,
                policy,
                metrics,
            }),
        }
    }

    /// Push a frame, applying the overflow policy at the high-water mark.
    ///
    /// A single frame larger than the HWM is allowed through an EMPTY queue
    /// (otherwise it could never be sent at all); `limits.max_payload_bytes`
    /// is the real cap on frame size.
    pub fn push(&self, frame: OutboundFrame) -> PushOutcome {
        let inner = &self.inner;
        let mut q = inner.queue.lock().unwrap();
        if q.closed {
            return PushOutcome::Closed;
        }
        let len = frame.data.len();
        if q.bytes + len > inner.high_water_mark && !q.items.is_empty() {
            match inner.policy {
                BackpressurePolicy::Disconnect => {
                    Metrics::add(&inner.metrics.backpressure_drops, 1);
                    q.closed = true; // no further sends; writer drains what's queued
                    drop(q);
                    inner.notify.notify_one();
                    return PushOutcome::Disconnect;
                }
                BackpressurePolicy::DropNewest => {
                    Metrics::add(&inner.metrics.backpressure_drops, 1);
                    return PushOutcome::DroppedNewest;
                }
                BackpressurePolicy::DropOldest => {
                    let mut dropped = 0u64;
                    while q.bytes + len > inner.high_water_mark {
                        match q.items.pop_front() {
                            Some(old) => {
                                q.bytes -= old.data.len();
                                dropped += 1;
                            }
                            None => break,
                        }
                    }
                    Metrics::add(&inner.metrics.backpressure_drops, dropped);
                    q.bytes += len;
                    q.items.push_back(frame);
                    drop(q);
                    inner.notify.notify_one();
                    return PushOutcome::DroppedOldest;
                }
            }
        }
        q.bytes += len;
        q.items.push_back(frame);
        drop(q);
        inner.notify.notify_one();
        PushOutcome::Queued
    }

    /// Writer side: next frame, or `None` once closed AND drained.
    pub async fn pop(&self) -> Option<OutboundFrame> {
        loop {
            {
                let mut q = self.inner.queue.lock().unwrap();
                if let Some(f) = q.items.pop_front() {
                    q.bytes -= f.data.len();
                    return Some(f);
                }
                if q.closed {
                    return None;
                }
            }
            self.inner.notify.notified().await;
        }
    }

    /// Tear-down: no more pushes accepted; `pop` returns queued items then None.
    pub fn close(&self) {
        self.inner.queue.lock().unwrap().closed = true;
        self.inner.notify.notify_one();
    }

    pub fn queued_bytes(&self) -> usize {
        self.inner.queue.lock().unwrap().bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mailbox(policy: BackpressurePolicy, hwm: usize) -> (Mailbox, Arc<Metrics>) {
        let m = Arc::new(Metrics::default());
        (Mailbox::new(hwm, policy, m.clone()), m)
    }

    fn frame(n: usize) -> OutboundFrame {
        OutboundFrame {
            data: Bytes::from(vec![0u8; n]),
            is_binary: true,
        }
    }

    #[test]
    fn disconnect_policy_fires_and_counts() {
        let (mb, m) = mailbox(BackpressurePolicy::Disconnect, 100);
        assert_eq!(mb.push(frame(60)), PushOutcome::Queued);
        assert_eq!(mb.push(frame(60)), PushOutcome::Disconnect);
        assert_eq!(Metrics::get(&m.backpressure_drops), 1);
        // Closed after the policy fired.
        assert_eq!(mb.push(frame(1)), PushOutcome::Closed);
    }

    #[test]
    fn drop_newest_counts_and_keeps_old() {
        let (mb, m) = mailbox(BackpressurePolicy::DropNewest, 100);
        assert_eq!(mb.push(frame(60)), PushOutcome::Queued);
        assert_eq!(mb.push(frame(60)), PushOutcome::DroppedNewest);
        assert_eq!(mb.push(frame(60)), PushOutcome::DroppedNewest);
        assert_eq!(Metrics::get(&m.backpressure_drops), 2);
        assert_eq!(mb.queued_bytes(), 60);
    }

    #[test]
    fn drop_oldest_evicts_until_fit() {
        let (mb, m) = mailbox(BackpressurePolicy::DropOldest, 100);
        assert_eq!(mb.push(frame(40)), PushOutcome::Queued);
        assert_eq!(mb.push(frame(40)), PushOutcome::Queued);
        assert_eq!(mb.push(frame(80)), PushOutcome::DroppedOldest);
        assert_eq!(Metrics::get(&m.backpressure_drops), 2);
        assert_eq!(mb.queued_bytes(), 80);
    }

    #[test]
    fn oversized_frame_allowed_on_empty_queue() {
        let (mb, m) = mailbox(BackpressurePolicy::Disconnect, 10);
        assert_eq!(mb.push(frame(1000)), PushOutcome::Queued);
        assert_eq!(Metrics::get(&m.backpressure_drops), 0);
    }

    #[tokio::test]
    async fn pop_drains_then_ends_after_close() {
        let (mb, _) = mailbox(BackpressurePolicy::DropNewest, 1000);
        mb.push(frame(10));
        mb.push(frame(20));
        mb.close();
        assert_eq!(mb.pop().await.unwrap().data.len(), 10);
        assert_eq!(mb.pop().await.unwrap().data.len(), 20);
        assert!(mb.pop().await.is_none());
    }
}
