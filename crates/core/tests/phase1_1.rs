//! Phase 1.1 attach — core-seam integration (RFC 0002 §5/§8.3).
//!
//! Exercises `engine.attach` end-to-end WITHOUT the napi fd-handoff layer (that
//! is Node-specific and covered by the JS integration test): a raw TCP pair
//! stands in for the dup'd upgrade fd. Proves the `adopt` handshake completion,
//! head-byte replay, the shared authorize + lifecycle, and the gate reject
//! path. The full fd handoff is proven separately (spike/attach + the JS
//! attach.integration test).

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use beamsocket_core::config::Config;
use beamsocket_core::engine::{AttachOutcome, Engine, ParsedUpgrade};
use beamsocket_core::events::EngineEvent;
use beamsocket_core::identity::AuthorizeOutcome;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ─────────────────────────────── harness ──────────────────────────────────

/// Bridge on the engine runtime: accept every authorize (userId from `x-user`)
/// AND echo every message back (there is no JS app in a core test).
fn spawn_bridge(engine: &Arc<Engine>, mut rx: tokio::sync::mpsc::Receiver<EngineEvent>) {
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
                        .find(|(k, _)| k == "x-user")
                        .map(|(_, v)| v.clone());
                    engine.resolve_authorize(request_id, AuthorizeOutcome::Accept { user_id });
                }
                EngineEvent::Message {
                    id,
                    payload,
                    is_binary,
                } => {
                    engine.send(id, payload, is_binary);
                }
                _ => {}
            }
        }
    });
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// A masked client text frame (RFC 6455 §5 — client frames MUST be masked;
/// spike-grade: payload < 126 bytes).
fn masked_text_frame(text: &str) -> Vec<u8> {
    let payload = text.as_bytes();
    let mask = [0x12u8, 0x34, 0x56, 0x78];
    let mut out = vec![0x81, 0x80 | (payload.len() as u8)];
    out.extend_from_slice(&mask);
    for (i, b) in payload.iter().enumerate() {
        out.push(b ^ mask[i % 4]);
    }
    out
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read the 101 response and then one server (unmasked) text frame's payload.
async fn read_101_and_text(client: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut tmp))
            .await
            .expect("timed out reading echo")
            .expect("read error");
        assert!(n > 0, "eof before echo");
        buf.extend_from_slice(&tmp[..n]);
        if let Some(hdr_end) = find(&buf, b"\r\n\r\n") {
            assert!(
                buf[..hdr_end].windows(3).any(|w| w == b"101"),
                "expected a 101 status"
            );
            let frame = &buf[hdr_end + 4..];
            if frame.len() >= 2 && frame[0] == 0x81 {
                let len = (frame[1] & 0x7f) as usize;
                if frame.len() >= 2 + len {
                    return String::from_utf8_lossy(&frame[2..2 + len]).to_string();
                }
            }
        }
    }
}

fn parsed(url: &str, extra: &[(&str, &str)]) -> ParsedUpgrade {
    let mut headers = vec![
        (
            "sec-websocket-key".to_string(),
            "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
        ),
        ("upgrade".to_string(), "websocket".to_string()),
    ];
    for (k, v) in extra {
        headers.push((k.to_string(), v.to_string()));
    }
    ParsedUpgrade {
        method: "GET".into(),
        url: url.into(),
        headers,
    }
}

/// Accept a raw TCP connection as a nonblocking std stream (what the napi layer
/// would produce from the dup'd fd).
fn accept_std(listener: &std::net::TcpListener) -> (std::net::TcpStream, std::net::IpAddr) {
    let (s, addr) = listener.accept().unwrap();
    s.set_nonblocking(true).unwrap();
    (s, addr.ip())
}

// ─────────────────────────── coalesced head replay ────────────────────────

#[test]
fn attach_replays_coalesced_first_frame() {
    let (engine, rx) = Engine::start(Config::default(), 1024, true).unwrap();
    let engine = Arc::new(engine);
    spawn_bridge(&engine, rx);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    current_thread_rt().block_on(async {
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (server_std, peer) = accept_std(&listener);

        // The first frame is in `head` (Node read it coalesced with the upgrade);
        // the client does NOT send it on the wire.
        let head = bytes::Bytes::from(masked_text_frame("coalesced-first-frame"));
        let outcome = engine.attach(
            server_std,
            peer,
            parsed("/ws", &[("x-user", "alice")]),
            head,
        );
        assert_eq!(outcome, AttachOutcome::Accepted);

        assert_eq!(
            read_101_and_text(&mut client).await,
            "coalesced-first-frame"
        );
    });
    drop(engine);
}

#[test]
fn attach_empty_head_normal_first_frame() {
    let (engine, rx) = Engine::start(Config::default(), 1024, true).unwrap();
    let engine = Arc::new(engine);
    spawn_bridge(&engine, rx);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    current_thread_rt().block_on(async {
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (server_std, peer) = accept_std(&listener);

        // Empty head: the client sends its first frame on the wire AFTER the
        // handoff — the codec reads it from the socket (prefix is a no-op).
        let outcome = engine.attach(
            server_std,
            peer,
            parsed("/ws", &[("x-user", "bob")]),
            bytes::Bytes::new(),
        );
        assert_eq!(outcome, AttachOutcome::Accepted);

        client
            .write_all(&masked_text_frame("wire-first-frame"))
            .await
            .unwrap();
        assert_eq!(read_101_and_text(&mut client).await, "wire-first-frame");
    });
    drop(engine);
}

// ─────────────────────── gate reject through attach ────────────────────────

#[test]
fn attach_gate_reject_returns_http_status() {
    // maxConnectionsPerIp = 1: the second attach from the same peer is rejected
    // with 429 (the same admission gate as own-port), synchronously.
    let mut config = Config::default();
    config.limits.max_connections_per_ip = 1;
    let (engine, rx) = Engine::start(config, 1024, false).unwrap();
    let engine = Arc::new(engine);
    spawn_bridge(&engine, rx);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    current_thread_rt().block_on(async {
        let _c1 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (s1, peer1) = accept_std(&listener);
        assert_eq!(
            engine.attach(s1, peer1, parsed("/ws", &[]), bytes::Bytes::new()),
            AttachOutcome::Accepted
        );

        let _c2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (s2, peer2) = accept_std(&listener);
        assert_eq!(
            engine.attach(s2, peer2, parsed("/ws", &[]), bytes::Bytes::new()),
            AttachOutcome::Rejected(429),
            "N+1th from one IP is 429 through attach too (Rule 3)"
        );
    });
    assert!(beamsocket_core::metrics::Metrics::get(&engine.metrics().admission_rejected_ip) >= 1);
    drop(engine);
}

// The named JS-side companions (attach_drains_stranded_prepause_bytes,
// attach_fd_hygiene_no_leak_no_double_close, coexistence/lifecycle/throws,
// Rule 4) live in packages/beamsocket/__tests__/attach.integration.test.mjs —
// they need Node's http.Server + the real fd handoff.
