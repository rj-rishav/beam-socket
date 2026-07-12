//! §13.1 gate: **relay microbench — the coalesced writer reproduces the spike's
//! cell.** The spike (`0004-results.md`, "What the spike changed" #1) showed
//! per-frame writes at 3.8 ms p99 and the coalesced writer at 680 µs — the 5.5×
//! that made coalescing a requirement, not an optimization.
//!
//! Two tests:
//! - a throughput/functional guard that runs in CI: 100k × 64 B, directional,
//!   zero drops, and a throughput floor a per-frame-syscall writer could not
//!   reach on a shared box;
//! - the strict `<1 ms p99` hop gate, `#[ignore]`d because a shared sandbox
//!   cannot be trusted on absolute latency (the same reason the RFC's soak gate
//!   is CI/pinned-box only). Run it with `cargo test -- --ignored` on the pinned
//!   box.

mod common;

use std::time::{Duration, Instant};

use beamsocket_mesh::{Frame, FrameKind, Link, Role};
use common::{connected_pair, manual_handshake, poll_until, read_frame, test_cfg};

use tokio::net::TcpListener;

const N: u64 = 100_000;
const CELL_BYTES: usize = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coalesced_writer_sustains_100k_small_frames_zero_drops() {
    // Large HWM so this measures the coalescing throughput path, not the drop
    // path; B is a real link draining as fast as it can.
    let mut a_cfg = test_cfg(1, "prod", 1, 0);
    a_cfg.queue_hwm_bytes = 32 * 1024 * 1024;
    let (a, b) = connected_pair(a_cfg, test_cfg(2, "prod", 1, 0)).await;

    let payload = vec![0x42u8; CELL_BYTES];
    let start = Instant::now();
    for _ in 0..N {
        let _ = a.try_send(Frame::new(FrameKind::Membership, payload.clone()));
    }
    let received = poll_until(
        || b.counters().snapshot().frames_in >= N,
        Duration::from_secs(30),
    )
    .await;
    let elapsed = start.elapsed();
    let snap = b.counters().snapshot();
    assert!(
        received,
        "B received {}/{N} frames in {elapsed:?}",
        snap.frames_in
    );
    assert_eq!(
        a.drops(),
        0,
        "no drops with a large HWM and a keeping-up reader"
    );

    let rate = N as f64 / elapsed.as_secs_f64();
    eprintln!("relay throughput: {rate:.0} msgs/s ({N} × {CELL_BYTES} B in {elapsed:?})");
    // A generous floor for a shared sandbox — the point is the writer is NOT
    // per-frame-syscall-bound (that path could not clear this on a busy box).
    assert!(
        rate >= 20_000.0,
        "throughput {rate:.0} msgs/s below the floor"
    );

    a.close();
    b.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing-sensitive relay-hop p99; run on the pinned box: cargo test -- --ignored"]
async fn relay_hop_p99_under_1ms_pinned_box() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // A shared monotonic epoch: both the sender's stamp and the receiver's
    // arrival read are `epoch.elapsed()`, so hop = recv_ns − send_ns is a true
    // one-way latency within this process (the spike's method).
    let epoch = Instant::now();

    let recv = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        manual_handshake(&mut s, Role::Responder, test_cfg(2, "prod", 1, 0))
            .await
            .unwrap();
        let mut hops = Vec::with_capacity(N as usize);
        for _ in 0..N {
            let f = read_frame(&mut s).await.unwrap();
            let send_ns = u64::from_le_bytes(f.body[0..8].try_into().unwrap());
            let recv_ns = epoch.elapsed().as_nanos() as u64;
            hops.push(recv_ns.saturating_sub(send_ns));
        }
        hops
    });

    let a = Link::connect(addr, test_cfg(1, "prod", 1, 0))
        .await
        .unwrap();

    // Pace at ~100k msgs/s in small batches (batch-level sleep keeps us off the
    // 1 ms timer floor while still averaging the target rate).
    let tokio_epoch = tokio::time::Instant::now();
    const BATCH: u64 = 200;
    for i in 0..N {
        if i % BATCH == 0 {
            let target = tokio_epoch + Duration::from_micros(i * 10);
            tokio::time::sleep_until(target).await;
        }
        let send_ns = epoch.elapsed().as_nanos() as u64;
        let mut body = Vec::with_capacity(CELL_BYTES);
        body.extend_from_slice(&send_ns.to_le_bytes());
        body.resize(CELL_BYTES, 0x42);
        let _ = a.try_send(Frame::new(FrameKind::Membership, body));
    }

    let mut hops = recv.await.unwrap();
    hops.sort_unstable();
    let p50 = hops[(hops.len() * 50) / 100];
    let p99 = hops[(hops.len() * 99) / 100];
    eprintln!("relay hop p50 {} µs / p99 {} µs", p50 / 1000, p99 / 1000);
    assert!(
        p99 < 1_000_000,
        "relay hop p99 {} µs exceeded the 1 ms cell",
        p99 / 1000
    );
    a.close();
}
