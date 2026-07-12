//! §13.1 gate: **link saturation.** A slow reader must make the *sender's*
//! per-peer queue drop-and-count, drive its pressure gauge up, and — the
//! load-bearing property — **never block the enqueuer** (§4.6, Rule 5). When the
//! reader catches up, the queue drains and pressure falls.

mod common;

use std::time::Duration;

use beamsocket_mesh::{Frame, FrameKind, Link, Role};
use common::{manual_handshake, poll_until, test_cfg};

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_reader_drops_and_counts_never_blocks_then_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // B is a raw responder we control the read pace of: it completes the
    // handshake, then deliberately stops reading (the slow/stalled peer).
    let b_cfg = test_cfg(2, "prod", 1, 0);
    let b_join = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        manual_handshake(&mut s, Role::Responder, b_cfg)
            .await
            .unwrap();
        s // hand the stalled stream back; not a single byte read yet
    });

    // A is a real link with a small HWM so overflow is quick and obvious.
    let mut a_cfg = test_cfg(1, "prod", 1, 0);
    a_cfg.queue_hwm_bytes = 16 * 1024;
    let a = Link::connect(addr, a_cfg).await.unwrap();
    let mut b_stream = b_join.await.unwrap();

    // Blast far more than the socket buffers + HWM can hold. `try_send` is
    // synchronous and non-blocking, so this loop returns promptly even though
    // the peer is not reading at all.
    let payload = vec![0x42u8; 200];
    let start = std::time::Instant::now();
    for _ in 0..20_000 {
        let _ = a.try_send(Frame::new(FrameKind::Membership, payload.clone()));
    }
    let enqueue_elapsed = start.elapsed();
    assert!(
        enqueue_elapsed < Duration::from_secs(2),
        "the enqueuer must not block on a slow peer (took {enqueue_elapsed:?})"
    );

    // Overflow: drops counted, pressure gauge high.
    let dropped = poll_until(|| a.drops() > 0, Duration::from_secs(2)).await;
    assert!(
        dropped,
        "a stalled peer must make the sender drop-and-count"
    );
    assert!(
        a.pressure() > 0.5,
        "pressure gauge should be high while the peer is stalled (was {})",
        a.pressure()
    );

    // Recovery: let B drain. The writer unblocks and the queue empties.
    let drain = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        loop {
            match tokio::time::timeout_at(deadline, b_stream.read(&mut buf)).await {
                Ok(Ok(0)) | Err(_) => break, // EOF or deadline
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
            }
        }
    });

    let recovered = poll_until(|| a.pressure() < 0.1, Duration::from_secs(4)).await;
    assert!(
        recovered,
        "queue should drain once the reader catches up (pressure {})",
        a.pressure()
    );

    a.close();
    let _ = drain.await;
}
