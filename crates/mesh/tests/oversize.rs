//! §13.1 gate: **oversize frame → link closed, counted, no resync.** A length
//! prefix beyond the negotiated max is a protocol error on a stream we no longer
//! trust; the reader closes and counts it once, and does **not** try to find the
//! next frame boundary (§4.4). A valid frame sent after the oversize is never
//! processed — proof there is no resync.

mod common;

use std::time::Duration;

use beamsocket_mesh::{Link, LinkState, Role};
use common::{manual_handshake, poll_until, test_cfg, write_raw_frame};

use tokio::net::{TcpListener, TcpStream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversize_frame_closes_link_counted_no_resync() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Both ends declare a 1 KiB max frame → negotiated max is 1 KiB.
    let mut server_cfg = test_cfg(2, "prod", 1, 0);
    server_cfg.max_frame = 1024;
    let accept = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        Link::accept(s, server_cfg).await.unwrap()
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut client_cfg = test_cfg(1, "prod", 1, 0);
    client_cfg.max_frame = 1024;
    manual_handshake(&mut client, Role::Initiator, client_cfg)
        .await
        .unwrap();
    let server = accept.await.unwrap();

    // Oversize length prefix (2000 > 1024). The server rejects on the prefix
    // alone — it never reads the body.
    write_raw_frame(&mut client, 2000, 0x04, 0, &[])
        .await
        .unwrap();
    // A perfectly valid frame right after. It must NOT be processed: the stream
    // is closed, and there is no resync. How fast the server tears down its
    // end after the oversize close is a race with this write: a broken pipe /
    // connection reset here means the peer was already gone, which is itself
    // evidence the link closed promptly — not a test failure. Only an error
    // that ISN'T "peer already gone" is unexpected.
    if let Err(e) = write_raw_frame(&mut client, 2, 0x04, 0, &[]).await {
        let io_err = e
            .downcast_ref::<std::io::Error>()
            .expect("write_raw_frame only ever returns an io::Error");
        assert!(
            matches!(
                io_err.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ),
            "unexpected error writing the post-oversize frame: {io_err}"
        );
    }

    let closed = poll_until(
        || server.state() == LinkState::Closed,
        Duration::from_secs(3),
    )
    .await;
    assert!(closed, "an oversize frame must close the link");

    let snap = server.counters().snapshot();
    assert_eq!(
        snap.oversize_closes, 1,
        "the oversize close is counted once"
    );
    assert_eq!(
        snap.frames_in, 0,
        "no resync: the valid frame after the oversize is never processed"
    );

    drop(client);
}
