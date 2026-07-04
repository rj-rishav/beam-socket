//! User → connections index + the authorize round-trip — Phase 1C.
//!
//! Two pieces live here, both "User is a first-class primitive" (ARCHITECTURE
//! §1):
//!
//! 1. [`IdentityRegistry`] — a sharded `DashMap<UserId, HashSet<ConnectionId>>`
//!    (Rule 2: `toUser` is a hot path, and so are tomorrow's `disconnectUser`
//!    / per-user presence — no global lock). Bound at authorize-accept, unbound
//!    at disconnect. Backs `io.toUser()` fan-out, which runs entirely in Rust.
//!
//!    Memory cost (Rule 4): one `ConnectionId` (8 B) per connection in its
//!    user's set, plus amortized `HashSet`/`DashMap` slot overhead — target
//!    ~24–40 B/conn, measured and published in the PR notes. An idle user (no
//!    live connections) costs nothing: the entry auto-destroys on last unbind,
//!    the same discipline rooms use.
//!
//! 2. [`Authorizer`] — the FIRST request/response round-trip across the bridge
//!    (1A/1B events are fire-and-forget). The engine emits an `Authorize` event
//!    to JS and awaits a `resolve` keyed by `request_id`. Two Rule 5 hazards are
//!    designed out: the pending table is BOUNDED (overflow → reject; an
//!    unauthenticated handshake flood is a DoS surface), and an `authorize`
//!    promise that never settles is rejected-and-cleaned at `timeout`, never
//!    leaked.
//!
//! Rule 1: `authorize` runs once per CONNECTION at upgrade time, never per
//! message. It is the one sanctioned connection-time JS hook (ARCHITECTURE §4).

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::connection::CLOSE_AUTH_UNAVAILABLE;
use crate::events::{EngineEvent, EventSender};
use crate::ids::{ConnectionId, UserId};
use crate::metrics::Metrics;

// ───────────────────────────── identity index ─────────────────────────────

/// Sharded user index. `DashMap` is internally sharded, so `toUser` and
/// disconnect-unbind contend only within a shard (Rule 2).
#[derive(Default)]
pub struct IdentityRegistry {
    users: DashMap<UserId, HashSet<ConnectionId>>,
}

impl IdentityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a connection to a user (authorize accept). Auto-creates the user
    /// entry on the first device.
    pub fn bind(&self, user: UserId, id: ConnectionId) {
        self.users.entry(user).or_default().insert(id);
    }

    /// Unbind on disconnect; auto-destroy the user entry when its last device
    /// goes (an empty user must never survive — same rule as empty rooms).
    pub fn unbind(&self, user: &UserId, id: ConnectionId) {
        if let Some(mut set) = self.users.get_mut(user) {
            set.remove(&id);
            if set.is_empty() {
                drop(set); // release the entry guard before removal
                           // remove_if re-checks emptiness under the map lock, so a
                           // concurrent bind between drop and here is not lost.
                self.users.remove_if(user, |_, set| set.is_empty());
            }
        }
    }

    /// Every live connection (device) of a user: copied out under the shard
    /// guard, which is released before the caller touches any conn shard —
    /// same lock discipline as `rooms::members` (broadcast.rs relies on it).
    /// `None` = the user has no live connections.
    pub fn connections(&self, user: &UserId) -> Option<Vec<ConnectionId>> {
        self.users
            .get(user)
            .map(|set| set.iter().copied().collect())
    }

    /// Device count for a user (diagnostics/tests).
    pub fn device_count(&self, user: &UserId) -> usize {
        self.users.get(user).map_or(0, |set| set.len())
    }

    /// Distinct users with ≥1 live connection (the `metrics.users` gauge feeds
    /// off this; Phase 1D surfaces it).
    pub fn user_count(&self) -> usize {
        self.users.len()
    }
}

// ─────────────────────────────── authorize ────────────────────────────────

/// What JS sends back through `resolveAuthorize`. Metadata is intentionally
/// absent: it stays in JS (the SDK correlates it to the socket by
/// `request_id`), so Rust never serializes an arbitrary JS object.
#[derive(Debug, Clone)]
pub enum AuthorizeOutcome {
    Accept { user_id: Option<String> },
    Reject { code: u16 },
}

/// What `authorize` resolves to for `setup_connection`.
#[derive(Debug, Clone)]
pub enum AuthorizeResolution {
    /// Admit. `request_id` is threaded into `ConnectionOpened` so the SDK can
    /// attach the userId/metadata it produced.
    Accept {
        user_id: Option<UserId>,
        request_id: u64,
    },
    /// Reject: close the (already-upgraded) socket with this code + reason.
    Reject { code: u16, reason: &'static str },
}

/// Decrements the in-flight counter on drop — so every `authorize` call, on
/// any exit path (accept, reject, timeout, panic), frees exactly one slot.
struct InFlightPermit<'a> {
    count: &'a AtomicUsize,
}

impl Drop for InFlightPermit<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The authorize request/response coordinator. Emits `Authorize` events to the
/// bridge and matches `resolve` calls back to the awaiting connection setup.
pub struct Authorizer {
    events: EventSender,
    /// request_id → the oneshot that wakes the awaiting `setup_connection`.
    /// `DashMap` (Rule 2), though authorize is once-per-connection, not
    /// per-message.
    pending: DashMap<u64, oneshot::Sender<AuthorizeOutcome>>,
    next_id: AtomicU64,
    /// Concurrently-pending authorizations (the BOUNDED table's occupancy).
    in_flight: AtomicUsize,
    max_pending: usize,
    timeout: Duration,
    metrics: Arc<Metrics>,
}

impl Authorizer {
    pub fn new(
        events: EventSender,
        max_pending: usize,
        timeout: Duration,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            events,
            pending: DashMap::new(),
            next_id: AtomicU64::new(1),
            in_flight: AtomicUsize::new(0),
            max_pending,
            timeout,
            metrics,
        }
    }

    /// Run the round-trip for one pending upgrade. Bounded (Rule 5): if the
    /// pending table is full, reject immediately without emitting — the flood
    /// is shed at the door. Otherwise emit `Authorize`, then await the JS reply
    /// up to `timeout`; a promise that never settles is rejected-and-cleaned.
    pub async fn authorize(
        &self,
        client_ip: IpAddr,
        url: String,
        headers: Vec<(String, String)>,
    ) -> AuthorizeResolution {
        // Reserve a pending slot. Over cap → shed now (do not emit, do not wait).
        let occupancy = self.in_flight.fetch_add(1, Ordering::AcqRel);
        if occupancy >= self.max_pending {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            Metrics::add(&self.metrics.pending_overflow, 1);
            return AuthorizeResolution::Reject {
                code: CLOSE_AUTH_UNAVAILABLE,
                reason: "pending-upgrade table full",
            };
        }
        let _permit = InFlightPermit {
            count: &self.in_flight,
        };

        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(request_id, tx);

        // Lossless, awaited: back-pressures only THIS connection's setup if the
        // engine→bridge queue is full, never the JS thread (events.rs control()).
        self.events
            .control(EngineEvent::Authorize {
                request_id,
                client_ip: client_ip.to_string(),
                url,
                headers,
            })
            .await;

        let resolution = match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(AuthorizeOutcome::Accept { user_id })) => AuthorizeResolution::Accept {
                user_id: user_id.map(UserId),
                request_id,
            },
            Ok(Ok(AuthorizeOutcome::Reject { code })) => {
                Metrics::add(&self.metrics.authorize_rejected, 1);
                AuthorizeResolution::Reject {
                    code,
                    reason: "unauthorized",
                }
            }
            // Sender dropped without sending (shutdown, or resolve raced our
            // timeout-cleanup): reject, never hang.
            Ok(Err(_)) => AuthorizeResolution::Reject {
                code: CLOSE_AUTH_UNAVAILABLE,
                reason: "authorize unavailable",
            },
            Err(_elapsed) => {
                Metrics::add(&self.metrics.authorize_timed_out, 1);
                AuthorizeResolution::Reject {
                    code: CLOSE_AUTH_UNAVAILABLE,
                    reason: "authorize timed out",
                }
            }
        };

        // Idempotent: `resolve` may already have taken it. Guarantees no pending
        // entry survives a timeout (no leak). `_permit` frees the slot on return.
        self.pending.remove(&request_id);
        resolution
    }

    /// JS replied (via the `resolveAuthorize` command). Wakes the awaiting
    /// connection setup. An unknown/duplicate `request_id` (already timed out
    /// and cleaned) is a benign no-op — never a panic, never a hang.
    pub fn resolve(&self, request_id: u64, outcome: AuthorizeOutcome) {
        if let Some((_, tx)) = self.pending.remove(&request_id) {
            let _ = tx.send(outcome); // receiver gone (timed out) → benign drop
        }
    }

    /// Occupancy of the bounded pending table (tests/metrics).
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(n: u64) -> ConnectionId {
        ConnectionId(n)
    }

    fn user(s: &str) -> UserId {
        UserId(s.to_owned())
    }

    // ---------- identity index ----------

    #[test]
    fn multi_device_bind_and_unbind() {
        let idx = IdentityRegistry::new();
        idx.bind(user("u1"), conn(1));
        idx.bind(user("u1"), conn(2));
        idx.bind(user("u1"), conn(3));
        assert_eq!(idx.device_count(&user("u1")), 3);
        assert_eq!(idx.user_count(), 1);

        // toUser sees all three devices.
        let mut devices = idx.connections(&user("u1")).unwrap();
        devices.sort_by_key(|c| c.0);
        assert_eq!(devices, vec![conn(1), conn(2), conn(3)]);

        idx.unbind(&user("u1"), conn(2));
        assert_eq!(idx.device_count(&user("u1")), 2);

        // Last two leave → the user entry is gone (no empty user survives).
        idx.unbind(&user("u1"), conn(1));
        idx.unbind(&user("u1"), conn(3));
        assert_eq!(idx.user_count(), 0);
        assert!(idx.connections(&user("u1")).is_none());
    }

    #[test]
    fn unbind_unknown_is_noop() {
        let idx = IdentityRegistry::new();
        idx.unbind(&user("ghost"), conn(9)); // must not panic
        idx.bind(user("u1"), conn(1));
        idx.unbind(&user("u1"), conn(999)); // wrong conn, right user
        assert_eq!(idx.device_count(&user("u1")), 1);
    }

    // ---------- authorize round-trip ----------

    fn authorizer(
        max_pending: usize,
        timeout: Duration,
    ) -> (
        Arc<Authorizer>,
        tokio::sync::mpsc::Receiver<EngineEvent>,
        Arc<Metrics>,
    ) {
        let metrics = Arc::new(Metrics::default());
        let (events, rx) = EventSender::bounded(64, metrics.clone());
        (
            Arc::new(Authorizer::new(
                events,
                max_pending,
                timeout,
                metrics.clone(),
            )),
            rx,
            metrics,
        )
    }

    fn any_ip() -> IpAddr {
        "203.0.113.7".parse().unwrap()
    }

    #[tokio::test]
    async fn accept_binds_user_and_carries_request_id() {
        let (auth, mut rx, _m) = authorizer(16, Duration::from_secs(5));
        let a = auth.clone();
        let task = tokio::spawn(async move { a.authorize(any_ip(), "/".into(), vec![]).await });

        // The engine emitted an Authorize event; reply as JS would.
        let request_id = match rx.recv().await.unwrap() {
            EngineEvent::Authorize { request_id, .. } => request_id,
            other => panic!("expected Authorize, got {other:?}"),
        };
        auth.resolve(
            request_id,
            AuthorizeOutcome::Accept {
                user_id: Some("alice".into()),
            },
        );

        match task.await.unwrap() {
            AuthorizeResolution::Accept {
                user_id,
                request_id: rid,
            } => {
                assert_eq!(user_id, Some(user("alice")));
                assert_eq!(rid, request_id);
            }
            other => panic!("expected Accept, got {other:?}"),
        }
        assert_eq!(auth.in_flight(), 0, "slot freed after resolve");
    }

    #[tokio::test]
    async fn reject_carries_code_and_counts() {
        let (auth, mut rx, metrics) = authorizer(16, Duration::from_secs(5));
        let a = auth.clone();
        let task = tokio::spawn(async move { a.authorize(any_ip(), "/".into(), vec![]).await });
        let request_id = match rx.recv().await.unwrap() {
            EngineEvent::Authorize { request_id, .. } => request_id,
            other => panic!("got {other:?}"),
        };
        auth.resolve(request_id, AuthorizeOutcome::Reject { code: 4403 });
        match task.await.unwrap() {
            AuthorizeResolution::Reject { code, .. } => assert_eq!(code, 4403),
            other => panic!("expected Reject, got {other:?}"),
        }
        assert_eq!(Metrics::get(&metrics.authorize_rejected), 1);
        assert_eq!(auth.in_flight(), 0);
    }

    #[tokio::test]
    async fn timeout_rejects_and_cleans_never_hangs() {
        let (auth, mut rx, metrics) = authorizer(16, Duration::from_millis(40));
        let a = auth.clone();
        let task = tokio::spawn(async move { a.authorize(any_ip(), "/".into(), vec![]).await });
        // Consume the event but NEVER resolve — simulate a JS promise that
        // never settles.
        let _ = rx.recv().await.unwrap();
        match tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("authorize must not hang past its own timeout")
            .unwrap()
        {
            AuthorizeResolution::Reject { code, .. } => assert_eq!(code, CLOSE_AUTH_UNAVAILABLE),
            other => panic!("expected timeout Reject, got {other:?}"),
        }
        assert_eq!(Metrics::get(&metrics.authorize_timed_out), 1);
        assert_eq!(auth.in_flight(), 0, "timed-out slot must be freed");
        assert!(
            auth.pending.is_empty(),
            "no pending entry may survive a timeout"
        );
    }

    #[tokio::test]
    async fn pending_table_overflow_rejects_at_the_door() {
        // Cap of 2. Two never-resolved authorizations fill the table; the third
        // must be shed immediately (no event emitted, no wait).
        let (auth, mut rx, metrics) = authorizer(2, Duration::from_secs(30));
        let a1 = auth.clone();
        let a2 = auth.clone();
        let t1 = tokio::spawn(async move { a1.authorize(any_ip(), "/".into(), vec![]).await });
        let t2 = tokio::spawn(async move { a2.authorize(any_ip(), "/".into(), vec![]).await });

        // Both are in flight once their events have been emitted.
        let _ = rx.recv().await.unwrap();
        let _ = rx.recv().await.unwrap();
        assert_eq!(auth.in_flight(), 2);

        // Third overflows synchronously.
        match auth.authorize(any_ip(), "/".into(), vec![]).await {
            AuthorizeResolution::Reject { code, reason } => {
                assert_eq!(code, CLOSE_AUTH_UNAVAILABLE);
                assert!(reason.contains("full"), "reason: {reason}");
            }
            other => panic!("expected overflow Reject, got {other:?}"),
        }
        assert_eq!(Metrics::get(&metrics.pending_overflow), 1);
        assert_eq!(auth.in_flight(), 2, "overflow must not consume a slot");

        // The overflow emitted no extra event.
        assert!(
            rx.try_recv().is_err(),
            "overflow must not emit an Authorize event"
        );

        // Clean up the two parked tasks.
        t1.abort();
        t2.abort();
    }
}
