//! §13.1 gate: **unknownFrames == 0 under mixed-feature-bit load** (the sender-
//! suppression proof), and **> 0 only from a deliberately-misbehaving peer**.
//!
//! Two real loopback links with different feature sets. A well-behaved sender
//! never emits an un-negotiated kind (§4.4), so the receiver's `unknownFrames`
//! stays zero. A raw peer that bypasses suppression and writes an unknown kind
//! is the only way to move that counter — which is exactly its job as a bug
//! detector.

mod common;

use std::time::Duration;

use beamsocket_mesh::handshake::features;
use beamsocket_mesh::link::Suppressed;
use beamsocket_mesh::{Frame, FrameKind, Link, Role};
use common::{connected_pair, manual_handshake, poll_until, test_cfg, write_raw_frame};

use tokio::net::{TcpListener, TcpStream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_link_establishes_over_tcp() {
    // The async path works at all: connect + accept complete the mutual HMAC
    // handshake over a real socket and agree on the peer ids.
    let (a, b) = connected_pair(test_cfg(1, "prod", 1, 0), test_cfg(2, "prod", 1, 0)).await;
    assert_eq!(a.peer_node_id(), 2);
    assert_eq!(b.peer_node_id(), 1);
    assert_eq!(a.negotiated().protocol_version, 1);
    a.close();
    b.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_frames_stay_zero_under_mixed_feature_load() {
    // A advertises INTEREST + RELAY; B advertises INTEREST only → the link
    // negotiates INTEREST. A well-behaved A emits only negotiated kinds.
    let cfg_a = test_cfg(1, "prod", 1, features::INTEREST_ROUTING | features::RELAY);
    let cfg_b = test_cfg(2, "prod", 1, features::INTEREST_ROUTING);
    let (a, b) = connected_pair(cfg_a, cfg_b).await;

    // Negotiated + control kinds go through: one MEMBERSHIP (always) + 5 INTEREST.
    a.try_send(Frame::new(FrameKind::Membership, vec![1, 2, 3]))
        .unwrap();
    for i in 0..5u8 {
        a.try_send(Frame::new(FrameKind::Interest, vec![i]))
            .unwrap();
    }

    // RELAY is not on both sides → the send path refuses it (counted, never a
    // wire write). Observed here via `try_send`; production `send` also
    // `debug_assert`s.
    let refused = a.try_send(Frame::new(FrameKind::RelayRoom, vec![9]));
    assert!(matches!(refused, Err(Suppressed(FrameKind::RelayRoom))));
    assert_eq!(a.counters().snapshot().suppressed_emits, 1);

    // B receives the six legitimate frames and counts ZERO unknown frames.
    let got = poll_until(
        || b.counters().snapshot().frames_in >= 6,
        Duration::from_secs(3),
    )
    .await;
    assert!(
        got,
        "B did not receive the negotiated frames: {:?}",
        b.counters().snapshot()
    );
    assert_eq!(
        b.counters().snapshot().unknown_frames,
        0,
        "sender suppression proof: no un-negotiated frame ever reached the peer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_frames_positive_only_from_a_misbehaving_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_cfg = test_cfg(2, "prod", 1, 0);
    let accept = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        Link::accept(s, server_cfg).await.unwrap()
    });

    // A raw client that completes the handshake, then ignores sender suppression
    // and writes a frame with a kind outside the catalog (0x7F).
    let mut client = TcpStream::connect(addr).await.unwrap();
    manual_handshake(&mut client, Role::Initiator, test_cfg(1, "prod", 1, 0))
        .await
        .unwrap();
    let server = accept.await.unwrap();

    write_raw_frame(&mut client, 2, 0x7F, 0, &[]).await.unwrap();

    let counted = poll_until(
        || server.counters().snapshot().unknown_frames >= 1,
        Duration::from_secs(3),
    )
    .await;
    assert!(
        counted,
        "the misbehaving peer's unknown kind must be counted"
    );
    // Self-delimiting: the unknown frame was skipped, not treated as a legit
    // frame, and the link is not torn down by it.
    assert_eq!(server.counters().snapshot().frames_in, 0);
    drop(client);
}
