//! Phase 1A required tests (docs/ENGINEERING.md §5):
//! - registry recycling under concurrency → unit tests in registry.rs
//! - send-queue overflow policy fires → unit tests in backpressure.rs
//! - a deliberately panicking connection task doesn't kill the engine → here
//! - real-socket echo + clean close both directions through the engine → here
//!   (the JS-client version lives in packages/beamsocket/__tests__)

use std::sync::Arc;
use std::time::Duration;

use beamsocket_core::config::Config;
use beamsocket_core::connection::backpressure::Mailbox;
use beamsocket_core::connection::{
    run_connection, CloseSignal, ConnCtx, ConnHandle, CLOSE_INTERNAL_ERROR, CONTROL_QUEUE_CAPACITY,
};
use beamsocket_core::engine::{Engine, SendStatus};
use beamsocket_core::events::{EngineEvent, EventSender};
use beamsocket_core::ids::ConnectionId;
use beamsocket_core::metrics::Metrics;
use beamsocket_core::transport::{FrameSink, FrameSource, InFrame, OutFrame, TransportError};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

// ---------- mock transport (lets tests inject frames and panics) ----------

enum Feed {
    Frame(InFrame),
    Panic,
}

struct MockSource(mpsc::Receiver<Feed>);

impl FrameSource for MockSource {
    async fn next_frame(&mut self) -> Option<Result<InFrame, TransportError>> {
        match self.0.recv().await? {
            Feed::Frame(f) => Some(Ok(f)),
            Feed::Panic => panic!("deliberate test panic in connection read loop"),
        }
    }
}

struct MockSink(mpsc::Sender<OutFrame>); // bounded, like everything (Rule 5)

impl FrameSink for MockSink {
    async fn send_frame(&mut self, frame: OutFrame) -> Result<(), TransportError> {
        let _ = self.0.try_send(frame); // tests never fill the collector
        Ok(())
    }

    async fn shutdown(&mut self) {}
}

struct MockConn {
    feed: mpsc::Sender<Feed>,
    written: mpsc::Receiver<OutFrame>,
    handle: ConnHandle,
    task: tokio::task::JoinHandle<(u16, String)>,
}

fn spawn_mock_conn(id: u64, ctx: Arc<ConnCtx>) -> MockConn {
    let (feed_tx, feed_rx) = mpsc::channel(64);
    let (written_tx, written_rx) = mpsc::channel(1024);
    let mailbox = Mailbox::new(
        ctx.config.backpressure.high_water_mark,
        ctx.config.backpressure.policy,
        ctx.metrics.clone(),
    );
    let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
    let (close, close_rx) = CloseSignal::new();
    let handle = ConnHandle {
        mailbox,
        control: control_tx,
        close,
    };
    let task = tokio::spawn(run_connection(
        ConnectionId(id),
        MockSource(feed_rx),
        MockSink(written_tx),
        handle.clone(),
        control_rx,
        close_rx,
        ctx,
    ));
    MockConn {
        feed: feed_tx,
        written: written_rx,
        handle,
        task,
    }
}

async fn recv_until<F: Fn(&EngineEvent) -> bool>(
    rx: &mut mpsc::Receiver<EngineEvent>,
    pred: F,
) -> EngineEvent {
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for engine event")
            .expect("event channel closed");
        if pred(&ev) {
            return ev;
        }
    }
}

// ---------- panic containment ----------

#[tokio::test]
async fn panicking_connection_task_is_contained() {
    let metrics = Arc::new(Metrics::default());
    let (events, mut rx) = EventSender::bounded(256, metrics.clone());
    let ctx = Arc::new(ConnCtx {
        config: Arc::new(Config::default()),
        metrics,
        events,
    });

    let doomed = spawn_mock_conn(1, ctx.clone());
    let mut survivor = spawn_mock_conn(2, ctx.clone());

    // Both connections are up.
    recv_until(
        &mut rx,
        |e| matches!(e, EngineEvent::ConnectionOpened { id } if *id == ConnectionId(1)),
    )
    .await;
    recv_until(
        &mut rx,
        |e| matches!(e, EngineEvent::ConnectionOpened { id } if *id == ConnectionId(2)),
    )
    .await;

    // Blow up connection 1's read loop.
    doomed.feed.send(Feed::Panic).await.unwrap();

    // The panic is contained: cleanup ran and Closed was emitted with 1011.
    let ev = recv_until(
        &mut rx,
        |e| matches!(e, EngineEvent::ConnectionClosed { id, .. } if *id == ConnectionId(1)),
    )
    .await;
    match ev {
        EngineEvent::ConnectionClosed { code, .. } => assert_eq!(code, CLOSE_INTERNAL_ERROR),
        _ => unreachable!(),
    }
    let (code, _) = doomed
        .task
        .await
        .expect("run_connection itself must not panic");
    assert_eq!(code, CLOSE_INTERNAL_ERROR);

    // The runtime and the OTHER connection are unaffected: it still reads…
    survivor
        .feed
        .send(Feed::Frame(InFrame::Text("still alive".into())))
        .await
        .unwrap();
    let ev = recv_until(
        &mut rx,
        |e| matches!(e, EngineEvent::Message { id, .. } if *id == ConnectionId(2)),
    )
    .await;
    match ev {
        EngineEvent::Message {
            payload, is_binary, ..
        } => {
            assert_eq!(payload, b"still alive");
            assert!(!is_binary);
        }
        _ => unreachable!(),
    }
    // …and still writes.
    survivor
        .handle
        .mailbox
        .push(beamsocket_core::connection::backpressure::OutboundFrame {
            data: b"echo".to_vec(),
            is_binary: true,
        });
    match tokio::time::timeout(Duration::from_secs(5), survivor.written.recv())
        .await
        .unwrap()
        .unwrap()
    {
        OutFrame::Binary(b) => assert_eq!(b, b"echo"),
        other => panic!("expected binary frame, got {other:?}"),
    }
}

// ---------- keepalive (Rule 1: no JS involved anywhere here) ----------

#[tokio::test]
async fn keepalive_pings_and_times_out_dead_peer() {
    let mut config = Config::default();
    config.keepalive.ping_interval = Duration::from_millis(50);
    config.keepalive.pong_timeout = Duration::from_millis(80);
    let metrics = Arc::new(Metrics::default());
    let (events, mut rx) = EventSender::bounded(64, metrics.clone());
    let ctx = Arc::new(ConnCtx {
        config: Arc::new(config),
        metrics,
        events,
    });

    let mut conn = spawn_mock_conn(9, ctx);
    // Peer never answers: expect a Ping on the wire, then a 1006 teardown.
    let ping = tokio::time::timeout(Duration::from_secs(5), conn.written.recv())
        .await
        .expect("no ping sent")
        .unwrap();
    assert!(matches!(ping, OutFrame::Ping(_)));
    let ev = recv_until(
        &mut rx,
        |e| matches!(e, EngineEvent::ConnectionClosed { id, .. } if *id == ConnectionId(9)),
    )
    .await;
    match ev {
        EngineEvent::ConnectionClosed { code, reason, .. } => {
            assert_eq!(code, 1006);
            assert!(reason.contains("keepalive"), "reason: {reason}");
        }
        _ => unreachable!(),
    }
    conn.task.await.unwrap();
}

// ---------- real sockets end-to-end through the engine ----------

fn echo_bridge(
    mut rx: mpsc::Receiver<EngineEvent>,
    engine: Arc<Engine>,
    stop_after_closes: usize,
) -> std::thread::JoinHandle<Vec<EngineEvent>> {
    // Simulates what the JS bridge does in Phase 1A: subscribe to Message and
    // send the payload back. Runs off-runtime like the real TSFN consumer.
    std::thread::spawn(move || {
        let mut log = Vec::new();
        let mut closes = 0;
        while let Some(ev) = rx.blocking_recv() {
            if let EngineEvent::Message {
                id,
                payload,
                is_binary,
            } = &ev
            {
                assert_eq!(
                    engine.send(*id, payload.clone(), *is_binary),
                    SendStatus::Queued
                );
            }
            if matches!(ev, EngineEvent::ConnectionClosed { .. }) {
                closes += 1;
            }
            log.push(ev);
            if closes >= stop_after_closes {
                break;
            }
        }
        log
    })
}

#[test]
fn engine_echoes_and_closes_cleanly_both_directions() {
    let (engine, rx) = Engine::start(Config::default(), 1024).unwrap();
    let engine = Arc::new(engine);
    let port = engine.listen(0).unwrap();
    let bridge = echo_bridge(rx, engine.clone(), 2);

    let client_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    client_rt.block_on(async {
        // --- connection 1: echo text + binary, then CLIENT-initiated close.
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/"))
            .await
            .expect("client connect");
        ws.send(Message::Text("hello".into())).await.unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Text("hello".into())
        );
        ws.send(Message::Binary(vec![1, 2, 3, 0, 255]))
            .await
            .unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Binary(vec![1, 2, 3, 0, 255])
        );
        // Client initiates the close handshake; the server must ack.
        ws.close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        }))
        .await
        .unwrap();
        let mut acked = false;
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Close(_)) => acked = true,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        assert!(acked, "server never acked the client-initiated close");

        // --- connection 2: SERVER-initiated close.
        let (mut ws2, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/"))
            .await
            .expect("client 2 connect");
        ws2.send(Message::Text("who am i".into())).await.unwrap();
        assert_eq!(
            ws2.next().await.unwrap().unwrap(),
            Message::Text("who am i".into())
        );
        // Exactly one connection is live: close it with an explicit code.
        assert_eq!(engine.connection_count(), 1);
        for live_id in engine.live_connection_ids() {
            assert!(engine.close_connection(live_id, 4001, "server says bye"));
        }
        let mut got = None;
        while let Some(msg) = ws2.next().await {
            match msg {
                Ok(Message::Close(frame)) => {
                    got = frame;
                    // tungstenite auto-acks; keep polling until the stream ends.
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let frame = got.expect("no close frame from server");
        assert_eq!(u16::from(frame.code), 4001);
        assert_eq!(frame.reason, "server says bye");
    });

    // Wait for the server to observe both closes.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.connection_count() > 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(engine.connection_count(), 0);

    // Bridge exits after observing both closes; then we own the engine again.
    let log = bridge.join().unwrap();
    let engine = Arc::try_unwrap(engine).ok().expect("sole owner");
    engine.shutdown();

    // Opened twice, closed twice; conn 1 closed with the client's 1000,
    // conn 2 with our 4001.
    let closes: Vec<_> = log
        .iter()
        .filter_map(|e| match e {
            EngineEvent::ConnectionClosed { code, .. } => Some(*code),
            _ => None,
        })
        .collect();
    assert_eq!(closes.len(), 2, "log: {log:?}");
    assert!(closes.contains(&1000), "closes: {closes:?}");
    assert!(closes.contains(&4001), "closes: {closes:?}");
}
