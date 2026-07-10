//! Phase 2B required tests (docs/ENGINEERING.md §12.2):
//! - disconnectUser: 3 devices → all closed with the given code, the identity
//!   entry is GONE (auto-destroy on last device — the 1C invariant), and
//!   toUser reaches 0.
//! - closeRoom: members' connections stay alive, the room is gone; the
//!   bidirectional-views proptest is extended with a CloseRoom op in
//!   phase1b.rs (the §12.2 "extend the 1B proptest" requirement).
//! - Verbs on nonexistent/stale targets: count 0, never an error.
//! - Each verb's close code lands on the client (asserted from a real ws
//!   client; the SDK-level equivalent lives in admin.integration.test.mjs).
//! - Churn: 1k admin disconnect sweeps → registries empty, RSS flat.
//!
//! The review lens (§12.2 smell rule): every assertion here is satisfied by
//! the verbs CALLING the existing 1C/1D close/cleanup paths — there is no
//! admin-specific teardown to test, only that the sweeps drive the proven
//! paths and report honest counts.

use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beamsocket_core::config::Config;
use beamsocket_core::engine::Engine;
use beamsocket_core::events::EngineEvent;
use beamsocket_core::identity::AuthorizeOutcome;
use beamsocket_core::ids::ConnectionId;
use beamsocket_core::metrics::Metrics;

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ─────────────────────────────── harness ──────────────────────────────────
// Same shape as the 1C/1D harnesses: a bridge task on the engine runtime
// plays the JS side (answers `Authorize` from the `x-user` header, forwards
// opened ids), and stock tungstenite clients play the network.

fn spawn_bridge(
    engine: &Arc<Engine>,
    mut rx: tokio::sync::mpsc::Receiver<EngineEvent>,
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
                } => {
                    let user_id = headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("x-user"))
                        .map(|(_, v)| v.clone());
                    engine.resolve_authorize(request_id, AuthorizeOutcome::Accept { user_id });
                }
                EngineEvent::ConnectionOpened { id, .. } => {
                    let _ = opened.send(id);
                }
                EngineEvent::ConnectionClosed { .. } | EngineEvent::Message { .. } => {}
            }
        }
    });
}

fn start(config: Config) -> (Arc<Engine>, u16, std_mpsc::Receiver<ConnectionId>) {
    let (engine, rx) = Engine::start(config, 4096, true).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let (opened_tx, opened_rx) = std_mpsc::channel();
    spawn_bridge(&engine, rx, opened_tx);
    (engine, port, opened_rx)
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn connect(port: u16, user: &str) -> Ws {
    let mut req = format!("ws://127.0.0.1:{port}/")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        HeaderName::from_static("x-user"),
        HeaderValue::from_str(user).unwrap(),
    );
    let (ws, _) = connect_async(req).await.unwrap();
    ws
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

/// Drain the stream to its end (post-close bookkeeping).
async fn drain(ws: &mut Ws) {
    while let Ok(Some(Ok(_))) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {}
}

async fn await_true<F: Fn() -> bool>(what: &str, f: F) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn recv_id(rx: &std_mpsc::Receiver<ConnectionId>) -> ConnectionId {
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

/// Resident set size in KB (Linux). Informational, for the churn test.
fn rss_kb() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    pages * 4
}

// ─────────────── disconnectUser: all devices, identity GONE ────────────────

#[test]
fn disconnect_user_closes_all_devices_and_destroys_the_identity_entry() {
    let (engine, port, opened) = start(Config::default());

    current_thread_rt().block_on(async {
        let mut a = connect(port, "alice").await;
        let mut b = connect(port, "alice").await;
        let mut c = connect(port, "alice").await;
        for _ in 0..3 {
            recv_id(&opened);
        }
        await_true("3 devices bound", || engine.user_device_count("alice") == 3).await;

        // The sweep: one call, three devices, the given (4000-range) code.
        assert_eq!(engine.admin_disconnect_user("alice", 4005), 3);

        // §12.2: the code lands on EVERY device's client.
        for ws in [&mut a, &mut b, &mut c] {
            assert_eq!(read_close_code(ws).await, 4005);
            drain(ws).await;
        }

        // The identity entry is GONE (auto-destroy on last device — the 1C
        // invariant, reached through the EXISTING unbind path)…
        await_true("identity entry gone", || {
            engine.user_device_count("alice") == 0 && engine.user_count() == 0
        })
        .await;
        await_true("registry empty", || engine.connection_count() == 0).await;

        // …so toUser reaches 0.
        let report = engine.broadcast_user("alice", Bytes::from_static(b"gone?"), false, &[]);
        assert_eq!(report.attempted, 0, "toUser must reach 0 after the sweep");

        // Counted: one per device closed.
        assert_eq!(Metrics::get(&engine.metrics().admin_disconnects), 3);

        // Idempotent from the operator's view: the user is gone → 0, no error.
        assert_eq!(engine.admin_disconnect_user("alice", 4005), 0);
    });

    Arc::try_unwrap(engine).ok().expect("sole owner").shutdown();
}

// ─────────────── disconnectSocket: code lands, full cleanup ────────────────

#[test]
fn disconnect_socket_delivers_code_and_runs_full_cleanup() {
    let (engine, port, opened) = start(Config::default());

    current_thread_rt().block_on(async {
        // Default-code path (1000).
        let mut a = connect(port, "amy").await;
        let a_id = recv_id(&opened);
        assert_eq!(engine.admin_disconnect_socket(a_id, 1000), 1);
        assert_eq!(read_close_code(&mut a).await, 1000);
        drain(&mut a).await;

        // Admin-code path (4000 range) with room + identity in play: the one
        // call must unwind membership, identity, and presence — through the
        // EXISTING disconnect path, nothing admin-specific.
        let mut b = connect(port, "bob").await;
        let b_id = recv_id(&opened);
        engine.join(b_id, "ops");
        assert_eq!(engine.room_member_count("ops"), 1);

        assert_eq!(engine.admin_disconnect_socket(b_id, 4008), 1);
        assert_eq!(read_close_code(&mut b).await, 4008);
        drain(&mut b).await;

        await_true("full cleanup", || {
            engine.connection_count() == 0
                && engine.room_count() == 0
                && engine.user_count() == 0
                && engine.presence_list("ops").is_empty()
        })
        .await;

        // The now-stale id reports 0, never an error.
        assert_eq!(engine.admin_disconnect_socket(b_id, 1000), 0);
        assert_eq!(Metrics::get(&engine.metrics().admin_disconnects), 2);
    });

    Arc::try_unwrap(engine).ok().expect("sole owner").shutdown();
}

// ──────────────── closeRoom: members alive, room gone ─────────────────────

#[test]
fn close_room_removes_members_destroys_room_keeps_connections_alive() {
    let (engine, port, opened) = start(Config::default());

    current_thread_rt().block_on(async {
        let mut a = connect(port, "u1").await;
        let mut b = connect(port, "u2").await;
        let mut c = connect(port, "u3").await;
        let ids: Vec<_> = (0..3).map(|_| recv_id(&opened)).collect();
        for id in &ids {
            engine.join(*id, "lobby");
        }
        // One member of a second room proves the sweep is scoped to ONE room.
        engine.join(ids[0], "other");
        assert_eq!(engine.room_member_count("lobby"), 3);

        assert_eq!(engine.admin_close_room("lobby"), 3);

        // Room gone, immediately (the sweep is synchronous)…
        assert_eq!(engine.room_member_count("lobby"), 0);
        assert_eq!(engine.room_count(), 1, "only 'other' survives");
        // …bidirectional views agree: no connection still claims 'lobby'
        // (broadcast to it reaches nobody), while 'other' is untouched.
        let report = engine.broadcast_room("lobby", Bytes::from_static(b"x"), false, &[]);
        assert_eq!(report.attempted, 0);
        assert_eq!(engine.room_member_count("other"), 1);

        // Connections STAY ALIVE (disconnect-free): all three still reachable.
        assert_eq!(engine.connection_count(), 3);
        let report = engine.broadcast_all(Bytes::from_static(b"alive"), false, &[]);
        assert_eq!(report.queued, 3);
        for ws in [&mut a, &mut b, &mut c] {
            let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(msg, Message::Text("alive".into()));
        }

        assert_eq!(Metrics::get(&engine.metrics().admin_room_closes), 1);

        // Gone room → 0, no error, not counted again.
        assert_eq!(engine.admin_close_room("lobby"), 0);
        assert_eq!(Metrics::get(&engine.metrics().admin_room_closes), 1);
    });

    Arc::try_unwrap(engine).ok().expect("sole owner").shutdown();
}

// ─────────────── nonexistent targets: 0, never an error ───────────────────

#[test]
fn verbs_on_nonexistent_targets_report_zero() {
    let (engine, _port, _opened) = start(Config::default());

    // No connections at all: every verb answers 0 and touches nothing.
    assert_eq!(
        engine.admin_disconnect_socket(ConnectionId(u64::MAX), 1000),
        0
    );
    assert_eq!(engine.admin_disconnect_user("ghost", 1000), 0);
    assert_eq!(engine.admin_close_room("nowhere"), 0);
    assert_eq!(Metrics::get(&engine.metrics().admin_disconnects), 0);
    assert_eq!(Metrics::get(&engine.metrics().admin_room_closes), 0);

    Arc::try_unwrap(engine).ok().expect("sole owner").shutdown();
}

// ──────────────── churn: 1k admin sweeps, registries empty ─────────────────

#[test]
fn admin_disconnect_churn_leaves_registries_empty_rss_flat() {
    // Per-IP tracking on, so the guard-release path is exercised too.
    let mut config = Config::default();
    config.limits.max_connections_per_ip = 100_000;
    let (engine, port, opened) = start(config);

    const CHURN: usize = 1_000;
    current_thread_rt().block_on(async {
        // Warm up allocator/tables, then measure.
        for i in 0..100 {
            let mut ws = connect(port, &format!("warm{i}")).await;
            let id = recv_id(&opened);
            engine.join(id, "churn-room");
            assert_eq!(engine.admin_disconnect_socket(id, 1000), 1);
            drain(&mut ws).await;
        }
        await_true("warmup drained", || engine.connection_count() == 0).await;
        let rss_before = rss_kb();

        for i in 0..CHURN {
            // Fresh user + room membership each cycle: the sweep must unwind
            // identity AND membership through the existing paths, every time.
            let mut ws = connect(port, &format!("u{i}")).await;
            let id = recv_id(&opened);
            engine.join(id, "churn-room");
            assert_eq!(engine.admin_disconnect_socket(id, 1000), 1);
            drain(&mut ws).await;
        }

        await_true("registry empty", || engine.connection_count() == 0).await;
        await_true("identity empty", || engine.user_count() == 0).await;
        await_true("rooms empty", || engine.room_count() == 0).await;
        await_true("per-IP table empty", || engine.tracked_ips() == 0).await;

        let rss_after = rss_kb();
        // Recorded, not asserted (allocator variance) — same discipline as the
        // 1C leak test: monotonic growth here would be a sweep leak.
        println!(
            "admin churn: RSS before={rss_before} KB after={rss_after} KB delta={} KB over {CHURN} sweeps",
            rss_after as i64 - rss_before as i64
        );
        assert_eq!(
            Metrics::get(&engine.metrics().admin_disconnects),
            (100 + CHURN) as u64
        );
    });

    Arc::try_unwrap(engine).ok().expect("sole owner").shutdown();
}
