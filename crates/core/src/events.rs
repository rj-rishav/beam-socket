//! Engine → bridge events. Only events the app subscribed to are emitted
//! (Rule 1); the bridge batches them toward JS (see crates/node/src/bridge.rs).
//!
//! The engine→bridge channel is BOUNDED (Rule 5). Capacity comes from the
//! caller (crates/node passes `ENGINE_BRIDGE_QUEUE_CAPACITY`, graduated from
//! RFC 0001 — the constant and its citation live in crates/node/src/bridge.rs
//! so core never hard-codes a benchmark number).
//!
//! Overflow policy, split by event class:
//! - `Message`: **drop-newest + counter** (`metrics.bridge_dropped`) — the RFC
//!   0001 graduation. Phase 1 semantics are frame delivery, not message
//!   delivery (ARCHITECTURE.md §4), so shedding under a saturated JS consumer
//!   is correct as long as it is counted and visible.
//! - `ConnectionOpened` / `ConnectionClosed`: **lossless**. Dropping one would
//!   permanently desync SDK state (a Socket that never fires `close`). These
//!   are rare, so the connection task `send().await`s — briefly back-pressuring
//!   that one connection, never the JS thread.

use crate::ids::ConnectionId;
use crate::metrics::Metrics;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum EngineEvent {
    /// The FIRST request/response round-trip across the bridge (Phase 1C): the
    /// engine asks JS to authorize a pending upgrade. JS replies out-of-band
    /// via the `resolveAuthorize` command (crates/node), keyed by `request_id`.
    /// Lossless like the other control events — a dropped request would hang a
    /// connection until its `authorize.timeout` (identity.rs) rather than fail
    /// fast, so this rides the awaited `control` path, never `try_message`.
    Authorize {
        request_id: u64,
        /// Resolved client IP (already through `trustProxy`) — the value the
        /// app should treat as authoritative, matching `maxConnectionsPerIp`.
        client_ip: String,
        /// Request target (the upgrade request's URI path+query).
        url: String,
        /// Request headers, names lowercased (transport/websocket.rs).
        headers: Vec<(String, String)>,
    },
    /// A connection passed admission + authorize and is live. (Phase 1A)
    ///
    /// Phase 1C: for a connection admitted by an `authorize` hook, `auth_request`
    /// carries the originating `request_id` so the SDK can attach the `userId` /
    /// `metadata` it produced; `None` for connections admitted with no hook.
    ConnectionOpened {
        id: ConnectionId,
        auth_request: Option<u64>,
    },
    /// App subscribed to `message` on this connection. (Phase 1A; payload is
    /// a refcounted view of the codec read buffer since 1B — no copy here.)
    Message {
        id: ConnectionId,
        payload: Bytes,
        is_binary: bool,
    },
    /// Close handshake finished or connection dropped. (Phase 1A)
    ConnectionClosed {
        id: ConnectionId,
        code: u16,
        reason: String,
    },
}

/// Sending half of the bounded engine→bridge channel, with the overflow
/// policy baked in so no call site can bypass it.
#[derive(Clone)]
pub struct EventSender {
    tx: mpsc::Sender<EngineEvent>,
    metrics: Arc<Metrics>,
}

impl EventSender {
    pub fn bounded(capacity: usize, metrics: Arc<Metrics>) -> (Self, mpsc::Receiver<EngineEvent>) {
        assert!(
            capacity > 0,
            "engine→bridge queue must be bounded and non-empty"
        );
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx, metrics }, rx)
    }

    /// Hot path: drop-newest on a full queue, counted, never silent.
    #[inline]
    pub fn try_message(&self, id: ConnectionId, payload: Bytes, is_binary: bool) {
        match self.tx.try_send(EngineEvent::Message {
            id,
            payload,
            is_binary,
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                Metrics::add(&self.metrics.bridge_dropped, 1);
            }
            // Bridge gone (shutdown) — nothing to notify.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Lossless control event (open/close). Awaits queue space; only ever
    /// blocks the emitting connection task.
    pub async fn control(&self, ev: EngineEvent) {
        debug_assert!(
            !matches!(ev, EngineEvent::Message { .. }),
            "Message events must go through try_message (drop-newest policy)"
        );
        let _ = self.tx.send(ev).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn message_overflow_drops_newest_and_counts() {
        let metrics = Arc::new(Metrics::default());
        let (tx, mut rx) = EventSender::bounded(2, metrics.clone());
        tx.try_message(ConnectionId(1), Bytes::from_static(b"a"), false);
        tx.try_message(ConnectionId(2), Bytes::from_static(b"b"), false);
        tx.try_message(ConnectionId(3), Bytes::from_static(b"c"), false); // full → dropped
        assert_eq!(Metrics::get(&metrics.bridge_dropped), 1);

        // The two oldest survived (drop-newest, not drop-oldest).
        match rx.recv().await.unwrap() {
            EngineEvent::Message { id, .. } => assert_eq!(id, ConnectionId(1)),
            other => panic!("unexpected event: {other:?}"),
        }
        match rx.recv().await.unwrap() {
            EngineEvent::Message { id, .. } => assert_eq!(id, ConnectionId(2)),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn control_events_are_lossless() {
        let metrics = Arc::new(Metrics::default());
        let (tx, mut rx) = EventSender::bounded(1, metrics.clone());
        // Fill the queue, then send a control event concurrently: it must
        // arrive once the consumer makes room — never be dropped.
        tx.try_message(ConnectionId(1), Bytes::new(), false);
        let tx2 = tx.clone();
        let sender = tokio::spawn(async move {
            tx2.control(EngineEvent::ConnectionClosed {
                id: ConnectionId(7),
                code: 1000,
                reason: String::new(),
            })
            .await;
        });
        // Drain the message; the awaited control send now completes.
        assert!(matches!(
            rx.recv().await.unwrap(),
            EngineEvent::Message { .. }
        ));
        match rx.recv().await.unwrap() {
            EngineEvent::ConnectionClosed { id, code, .. } => {
                assert_eq!(id, ConnectionId(7));
                assert_eq!(code, 1000);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        sender.await.unwrap();
        assert_eq!(Metrics::get(&metrics.bridge_dropped), 0);
    }
}
