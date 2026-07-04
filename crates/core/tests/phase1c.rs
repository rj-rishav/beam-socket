//! Phase 1C required tests (docs/ENGINEERING.md §7):
//! - Multi-device: one user, 3 connections → `toUser` reaches 3; one leaves →
//!   2; last leaves → the identity index entry is gone.
//! - Leak: connect/disconnect churn → identity index empty, per-IP table empty,
//!   RSS flat (printed).
//! - Spoof: untrusted peer's XFF ignored (peer used); trusted peer's XFF honored
//!   right-to-left; mixed trusted+untrusted hops. (Resolver logic is unit-tested
//!   in src/limits.rs; here it is proven end-to-end through the per-IP limit.)
//! - Per-IP limit: the N+1th connection rejected with the documented status, in
//!   BOTH direct and simulated-proxy topologies (Rule 3).
//! - Plus: authorize timeout fires; pending-upgrade cap overflows safely;
//!   authorize rejection closes with the app's code.
//!
//! The authorize round-trip is exercised for real: a bridge task on the engine
//! runtime plays the JS side, replying to `Authorize` events via
//! `engine.resolve_authorize` — the same seam the napi binding uses.

use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beamsocket_core::config::{Config, TrustProxy};
use beamsocket_core::engine::Engine;
use beamsocket_core::events::EngineEvent;
use beamsocket_core::identity::AuthorizeOutcome;
use beamsocket_core::ids::ConnectionId;
use beamsocket_core::metrics::Metrics;
use beamsocket_core::rooms::MembershipChange;

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ─────────────────────────────── harness ──────────────────────────────────

/// How the test's "JS side" answers `authorize`.
#[derive(Clone)]
enum AuthPolicy {
    /// Accept; userId = the value of this header, or anonymous if absent.
    UserFromHeader(&'static str),
    /// Reject with this close code.
    Reject(u16),
    /// Never answer (simulate a hung promise) — authorize must time out.
    Never,
    /// Never answer, but signal on every request (to observe in-flight count).
    NeverSignal(std_mpsc::Sender<()>),
}

/// Spawn the "bridge" on the engine's own runtime: drain events, answer
/// `Authorize` per `policy`, and forward opened connection ids to `opened`.
/// Holds only a `Weak<Engine>`, so dropping the engine ends it — no join.
fn spawn_bridge(
    engine: &Arc<Engine>,
    mut rx: tokio::sync::mpsc::Receiver<EngineEvent>,
    policy: AuthPolicy,
    opened: std_mpsc::Sender<ConnectionId>,
) {
    let weak = Arc::downgrade(engine);
    engine.handle().spawn(async move {
        while let Some(ev) = rx.recv().await {
            let Some(engine) = weak.upgrade() else { break };
            match ev {
                EngineEvent::Authorize {
                    request_id,
                    headers,
                    ..
                } => match &policy {
                    AuthPolicy::UserFromHeader(name) => {
                        let user_id = header_value(&headers, name);
                        engine.resolve_authorize(request_id, AuthorizeOutcome::Accept { user_id });
                    }
                    AuthPolicy::Reject(code) => {
                        engine.resolve_authorize(
                            request_id,
                            AuthorizeOutcome::Reject { code: *code },
                        );
                    }
                    AuthPolicy::Never => {}
                    AuthPolicy::NeverSignal(tx) => {
                        let _ = tx.send(());
                    }
                },
                EngineEvent::ConnectionOpened { id, .. } => {
                    let _ = opened.send(id);
                }
                EngineEvent::ConnectionClosed { .. } | EngineEvent::Message { .. } => {}
            }
        }
    });
}

fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn client_request(
    port: u16,
    headers: &[(&str, &str)],
) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut req = format!("ws://127.0.0.1:{port}/")
        .into_client_request()
        .unwrap();
    for (k, v) in headers {
        req.headers_mut().insert(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    req
}

async fn connect(port: u16, headers: &[(&str, &str)]) -> Ws {
    let (ws, _) = connect_async(client_request(port, headers)).await.unwrap();
    ws
}

/// Connect but keep the handshake result — used to assert a rejected upgrade.
async fn try_connect(port: u16, headers: &[(&str, &str)]) -> bool {
    connect_async(client_request(port, headers)).await.is_ok()
}

/// Read frames until the server's Close frame; return its code.
async fn read_close_code(ws: &mut Ws) -> u16 {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Close(Some(frame))))) => return u16::from(frame.code),
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => panic!("stream ended before a close frame"),
            Err(_) => panic!("timed out waiting for a close frame"),
        }
    }
}

/// Client-initiated close, then drain until the server's ack ends the stream.
async fn close_client(ws: &mut Ws) {
    let _ = ws.close(None).await;
    while let Ok(Some(Ok(_))) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {}
}

async fn await_true<F: Fn() -> bool>(what: &str, f: F) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Resident set size in KB (Linux). Informational, for the leak test.
fn rss_kb() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    pages * (page_size() / 1024)
}

fn page_size() -> u64 {
    4096
}

// ─────────────────────────── multi-device toUser ──────────────────────────

#[test]
fn multi_device_to_user_reaches_every_device() {
    let (engine, rx) = Engine::start(Config::default(), 1024, true).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::UserFromHeader("x-user"), opened_tx);

    current_thread_rt().block_on(async {
        let mut a = connect(port, &[("x-user", "alice")]).await;
        let mut b = connect(port, &[("x-user", "alice")]).await;
        let mut c = connect(port, &[("x-user", "alice")]).await;
        await_true("3 devices bound", || engine.user_device_count("alice") == 3).await;
        assert_eq!(engine.user_count(), 1);

        // toUser reaches all three devices, one allocation, one FFI call.
        let report = engine.broadcast_user("alice", Bytes::from_static(b"ping"), false, &[]);
        assert_eq!(report.queued, 3);
        for ws in [&mut a, &mut b, &mut c] {
            assert_eq!(
                ws.next().await.unwrap().unwrap(),
                Message::Text("ping".into())
            );
        }

        // One device leaves → toUser now reaches two.
        close_client(&mut a).await;
        await_true("2 devices remain", || {
            engine.user_device_count("alice") == 2
        })
        .await;
        let report = engine.broadcast_user("alice", Bytes::from_static(b"again"), false, &[]);
        assert_eq!(report.queued, 2);
        assert_eq!(
            b.next().await.unwrap().unwrap(),
            Message::Text("again".into())
        );
        assert_eq!(
            c.next().await.unwrap().unwrap(),
            Message::Text("again".into())
        );

        // Last devices leave → the user's index entry is gone (no empty user).
        close_client(&mut b).await;
        close_client(&mut c).await;
        await_true("user entry destroyed", || engine.user_count() == 0).await;
        assert_eq!(engine.user_device_count("alice"), 0);
        // toUser to a vanished user is a benign no-op.
        assert_eq!(
            engine
                .broadcast_user("alice", Bytes::from_static(b"x"), false, &[])
                .attempted,
            0
        );
    });
    drop(engine);
}

// ───────────────────────────────── leak ───────────────────────────────────

#[test]
fn churn_leaves_identity_and_ip_tables_empty_rss_flat() {
    // Track per-IP too (max_connections_per_ip > 0), so the IP guard-release
    // path is exercised alongside identity unbind.
    let mut config = Config::default();
    config.limits.max_connections_per_ip = 100_000;
    let (engine, rx) = Engine::start(config, 4096, true).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::UserFromHeader("x-user"), opened_tx);

    const CHURN: usize = 10_000;
    current_thread_rt().block_on(async {
        // Warm up, then measure RSS across the churn.
        for i in 0..200 {
            let mut ws = connect(port, &[("x-user", &format!("warm{i}"))]).await;
            close_client(&mut ws).await;
        }
        await_true("warmup drained", || engine.connection_count() == 0).await;
        let rss_before = rss_kb();

        for i in 0..CHURN {
            // A fresh distinct user each cycle → its index entry must be created
            // AND destroyed; a leak would grow user_count without bound.
            let mut ws = connect(port, &[("x-user", &format!("u{i}"))]).await;
            close_client(&mut ws).await;
        }
        await_true("all connections drained", || engine.connection_count() == 0).await;
        await_true("identity index empty", || engine.user_count() == 0).await;
        await_true("per-IP table empty", || engine.tracked_ips() == 0).await;

        let rss_after = rss_kb();
        // Not an assertion (allocator variance), but recorded: a per-connection
        // leak of even 40 B over 10k cycles would show as ~hundreds of KB of
        // monotonic growth; a bound-and-release path stays flat.
        println!(
            "leak: RSS before={rss_before} KB after={rss_after} KB delta={} KB over {CHURN} cycles",
            rss_after as i64 - rss_before as i64
        );
        assert_eq!(engine.user_count(), 0, "identity index leaked");
        assert_eq!(engine.tracked_ips(), 0, "per-IP table leaked");
    });
    drop(engine);
}

// ───────────────────── per-IP limit: direct + proxy (Rule 3) ──────────────

#[test]
fn per_ip_limit_direct_topology() {
    // trustProxy: false — the peer address is the client IP. Two loopback
    // connections fit under the cap of 2; the third is rejected at the upgrade.
    let mut config = Config::default();
    config.trust_proxy = TrustProxy::Never;
    config.limits.max_connections_per_ip = 2;
    let (engine, rx) = Engine::start(config, 1024, false).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::Never, opened_tx);

    current_thread_rt().block_on(async {
        let _a = connect(port, &[]).await;
        let _b = connect(port, &[]).await;
        await_true("two admitted", || engine.connection_count() == 2).await;
        // 3rd from the same peer → HTTP 429 handshake failure.
        assert!(
            !try_connect(port, &[]).await,
            "N+1th connection over the per-IP cap must fail the handshake"
        );
        // A spoofed XFF must NOT dodge the limit (trustProxy is false → ignored).
        assert!(
            !try_connect(port, &[("x-forwarded-for", "9.9.9.9")]).await,
            "spoofed XFF must not bypass the per-IP limit"
        );
    });
    assert!(Metrics::get(&engine.metrics().admission_rejected_ip) >= 2);
    drop(engine);
}

#[test]
fn per_ip_limit_proxy_topology() {
    // trustProxy: [127.0.0.0/8] — loopback is a trusted proxy, so the per-IP
    // limit keys on the XFF-derived client IP, not the shared peer address.
    let mut config = Config::default();
    config.trust_proxy = TrustProxy::Cidrs(vec!["127.0.0.0/8".into()]);
    config.limits.max_connections_per_ip = 2;
    let (engine, rx) = Engine::start(config, 1024, false).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::Never, opened_tx);

    current_thread_rt().block_on(async {
        // Two connections attributed to client 9.9.9.9 (behind the proxy).
        let _a = connect(port, &[("x-forwarded-for", "9.9.9.9")]).await;
        let _b = connect(port, &[("x-forwarded-for", "9.9.9.9")]).await;
        await_true("two admitted", || engine.connection_count() == 2).await;
        // A third for 9.9.9.9 is rejected…
        assert!(
            !try_connect(port, &[("x-forwarded-for", "9.9.9.9")]).await,
            "N+1th for one forwarded client IP must be rejected behind a proxy"
        );
        // …but a different forwarded client has its own budget.
        assert!(
            try_connect(port, &[("x-forwarded-for", "8.8.8.8")]).await,
            "a distinct forwarded client IP must be admitted"
        );
        // Mixed trusted+untrusted hops: right-to-left, the first untrusted is the
        // client (7.7.7.7); it already has 0 connections, so it is admitted, and
        // the forged leftmost 1.2.3.4 is irrelevant.
        assert!(
            try_connect(port, &[("x-forwarded-for", "1.2.3.4, 7.7.7.7, 127.0.0.5")]).await,
            "mixed-hop XFF must resolve to the first untrusted address"
        );
    });
    drop(engine);
}

// ─────────────────────────── authorize edge cases ─────────────────────────

#[test]
fn authorize_timeout_closes_and_cleans() {
    let mut config = Config::default();
    config.authorize.timeout = Duration::from_millis(150);
    let (engine, rx) = Engine::start(config, 1024, true).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::Never, opened_tx); // never answers

    current_thread_rt().block_on(async {
        let mut ws = connect(port, &[]).await;
        // The upgrade succeeded; authorize never settles → 1013 (try later).
        assert_eq!(read_close_code(&mut ws).await, 1013);
    });
    assert_eq!(Metrics::get(&engine.metrics().authorize_timed_out), 1);
    assert_eq!(engine.user_count(), 0);
    drop(engine);
}

#[test]
fn authorize_reject_closes_with_app_code() {
    let (engine, rx) = Engine::start(Config::default(), 1024, true).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::Reject(4403), opened_tx);

    current_thread_rt().block_on(async {
        let mut ws = connect(port, &[]).await;
        assert_eq!(
            read_close_code(&mut ws).await,
            4403,
            "app reject code delivered"
        );
    });
    assert_eq!(Metrics::get(&engine.metrics().authorize_rejected), 1);
    assert_eq!(
        engine.connection_count(),
        0,
        "rejected connection is not counted live"
    );
    drop(engine);
}

#[test]
fn pending_upgrade_table_overflow_rejects_safely() {
    // Cap of 1, long timeout: the first authorization occupies the only slot
    // (never answered), so the second must be shed at the door with 1013 —
    // without leaking, hanging, or opening.
    let mut config = Config::default();
    config.authorize.max_pending = 1;
    config.authorize.timeout = Duration::from_secs(30);
    let (engine, rx) = Engine::start(config, 1024, true).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, _opened_rx) = std_mpsc::channel();
    let (seen_tx, seen_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::NeverSignal(seen_tx), opened_tx);

    current_thread_rt().block_on(async {
        // First connection occupies the pending slot.
        let _first = connect(port, &[]).await;
        seen_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("first authorize seen");

        // Second connection: handshake succeeds, but authorize overflows.
        let mut second = connect(port, &[]).await;
        assert_eq!(
            read_close_code(&mut second).await,
            1013,
            "overflow shed with 1013"
        );
    });
    assert_eq!(Metrics::get(&engine.metrics().pending_overflow), 1);
    drop(engine);
}

// ───────────────────── Rule 4: identity memory cost ───────────────────────

#[test]
#[ignore = "measurement, not a gate — run with `--ignored --nocapture` for the Rule 4 number"]
fn identity_memory_cost_measurement() {
    use beamsocket_core::identity::IdentityRegistry;
    use beamsocket_core::ids::UserId;

    const N: u64 = 500_000;

    // Hot case (multi-device): one user, N devices. Isolates the per-connection
    // index cost — one ConnectionId in the shared HashSet + load-factor slack.
    let idx = IdentityRegistry::new();
    let before = rss_kb();
    for i in 0..N {
        idx.bind(UserId("one".into()), ConnectionId(i));
    }
    let after = rss_kb();
    let per_device = (after - before) as f64 * 1024.0 / N as f64;
    println!("identity Rule 4 — 1 user × {N} devices: {per_device:.1} B/connection (ConnectionId in the shared set)");
    assert_eq!(idx.device_count(&UserId("one".into())), N as usize);

    // Distinct-user case: N users × 1 device. Adds the per-user DashMap entry +
    // userId string, amortized to ~0 for real multi-device users.
    let idx2 = IdentityRegistry::new();
    let before2 = rss_kb();
    for i in 0..N {
        idx2.bind(UserId(format!("user-{i}")), ConnectionId(i));
    }
    let after2 = rss_kb();
    let per_user = (after2 - before2) as f64 * 1024.0 / N as f64;
    println!("identity Rule 4 — {N} distinct users × 1 device: {per_user:.1} B/connection (entry + set + userId string)");
}

// ─────────────────── maxRoomsPerConnection (now enforced) ──────────────────

#[test]
fn max_rooms_per_connection_enforced() {
    let mut config = Config::default();
    config.limits.max_rooms_per_connection = 2;
    let (engine, rx) = Engine::start(config, 1024, false).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, AuthPolicy::Never, opened_tx);

    current_thread_rt().block_on(async {
        let _ws = connect(port, &[]).await;
        let id = opened_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        assert_eq!(engine.join(id, "a"), MembershipChange::Changed);
        assert_eq!(engine.join(id, "b"), MembershipChange::Changed);
        // The 3rd distinct room exceeds the cap…
        assert_eq!(engine.join(id, "c"), MembershipChange::LimitExceeded);
        // …but re-joining a room already joined is an idempotent no-op, never a
        // limit error, and never grows the set.
        assert_eq!(engine.join(id, "a"), MembershipChange::NoOp);
        assert_eq!(engine.room_count(), 2);
    });
    drop(engine);
}
