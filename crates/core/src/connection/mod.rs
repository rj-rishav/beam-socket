//! The per-connection task — Phase 1A. This is the BEAM-inspired part:
//! each connection is an isolated Tokio task with its own bounded mailbox.
//!
//! Responsibilities: read loop, write loop, bounded send queue, ping/pong
//! keepalive, close handshake. A panic tears down ONE connection, never the
//! runtime: the read loop runs under `catch_unwind` so cleanup (registry
//! removal, Closed event) happens even on panic, and the writer is a separate
//! supervised task whose panic aborts only its own connection.
//!
//! Rule 1 reminder: ping/pong and close bookkeeping NEVER call into JS.
//!
//! Per-connection memory budget (Rule 4, informational — the measured number
//! lives in the PR notes): mailbox (Arc + mutex + empty VecDeque ≈ 150 B),
//! control channel (cap 4 ≈ 200 B), close watch (≈ 150 B), task stacks are
//! lazy. Idle-connection RSS including kernel + tungstenite read buffers is
//! measured by the 10k-idle gate (<20 KB/conn target context).

pub mod backpressure;
pub mod registry;

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep_until, Instant};

use crate::config::Config;
use crate::events::{EngineEvent, EventSender};
use crate::ids::ConnectionId;
use crate::metrics::Metrics;
use crate::transport::{FrameSink, FrameSource, InFrame, OutFrame};
use backpressure::{Mailbox, OutboundFrame};

/// Close codes the engine itself produces.
pub const CLOSE_NORMAL: u16 = 1000;
/// Server going away (engine shutdown sweep).
pub const CLOSE_GOING_AWAY: u16 = 1001;
/// Reported (never sent) for abnormal teardown: EOF without close frame,
/// keepalive timeout, write failure.
pub const CLOSE_ABNORMAL: u16 = 1006;
/// A connection task panicked; the panic was contained to this connection.
pub const CLOSE_INTERNAL_ERROR: u16 = 1011;
/// Backpressure policy `Disconnect` fired ("try again later").
pub const CLOSE_BACKPRESSURE: u16 = 1013;

/// How long a graceful close may wait for the peer's close frame before the
/// connection is torn down anyway.
pub const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// Control-plane commands to the writer task. Bounded at
/// `CONTROL_QUEUE_CAPACITY` (Rule 5); overflow is a benign `try_send` miss:
/// a skipped ping retries next tick, and every close command is mirrored in
/// the close watch, which cannot be lost.
#[derive(Debug)]
pub enum Control {
    Ping(Vec<u8>),
    Close { code: u16, reason: String },
}

pub const CONTROL_QUEUE_CAPACITY: usize = 4;

/// A close request. `graceful: true` = send a close frame and wait (bounded
/// by CLOSE_GRACE) for the peer's reply; `false` = tear down immediately.
#[derive(Debug, Clone)]
pub struct CloseCmd {
    pub code: u16,
    pub reason: String,
    pub graceful: bool,
}

/// First-signal-wins close latch, watchable by the read loop.
pub struct CloseSignal {
    tx: watch::Sender<Option<CloseCmd>>,
}

impl CloseSignal {
    pub fn new() -> (Arc<Self>, watch::Receiver<Option<CloseCmd>>) {
        let (tx, rx) = watch::channel(None);
        (Arc::new(Self { tx }), rx)
    }

    /// Records the FIRST close reason; later signals are ignored so e.g. a
    /// write-error abort cannot overwrite the JS-initiated code already being
    /// reported.
    pub fn signal(&self, cmd: CloseCmd) {
        self.tx.send_if_modified(|v| {
            if v.is_none() {
                *v = Some(cmd);
                true
            } else {
                false
            }
        });
    }
}

/// What the registry stores per connection: everything the JS thread needs
/// for `send` / `close` without touching the task (all cheap clones).
#[derive(Clone)]
pub struct ConnHandle {
    pub mailbox: Mailbox,
    pub control: mpsc::Sender<Control>,
    pub close: Arc<CloseSignal>,
}

/// Everything a connection task shares with the engine.
pub struct ConnCtx {
    pub config: Arc<Config>,
    pub metrics: Arc<Metrics>,
    pub events: EventSender,
}

/// Drive one connection to completion: emit Opened, run writer + reader,
/// contain panics, and ALWAYS emit Closed exactly once at the end. The caller
/// (engine) removes the registry entry via the returned future's completion —
/// see `Engine`'s `setup_connection`.
///
/// Returns the (code, reason) reported in the Closed event.
pub async fn run_connection<Src, Snk>(
    id: ConnectionId,
    source: Src,
    sink: Snk,
    handle: ConnHandle,
    control_rx: mpsc::Receiver<Control>,
    close_rx: watch::Receiver<Option<CloseCmd>>,
    ctx: Arc<ConnCtx>,
) -> (u16, String)
where
    Src: FrameSource,
    Snk: FrameSink,
{
    ctx.events
        .control(EngineEvent::ConnectionOpened { id })
        .await;

    // Writer: separate task so a slow socket never blocks reads. Its panic is
    // contained by catch_unwind and aborts only this connection.
    let writer = {
        let mailbox = handle.mailbox.clone();
        let close = handle.close.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move {
            let res = AssertUnwindSafe(write_loop(sink, mailbox, control_rx, metrics))
                .catch_unwind()
                .await;
            match res {
                Ok(()) => {}
                Err(_panic) => close.signal(CloseCmd {
                    code: CLOSE_INTERNAL_ERROR,
                    reason: "connection writer panicked".into(),
                    graceful: false,
                }),
            }
        })
    };

    // Reader: run under catch_unwind so cleanup below happens even on panic.
    let read = AssertUnwindSafe(read_loop(id, source, &handle, close_rx, &ctx))
        .catch_unwind()
        .await;
    let (code, reason) = match read {
        Ok(outcome) => outcome,
        Err(_panic) => (CLOSE_INTERNAL_ERROR, "connection task panicked".into()),
    };

    // Tear down the writer: close the mailbox (drains, then ends). The
    // control channel ends when the last sender (registry entry) drops.
    handle.mailbox.close();
    let _ = writer.await; // panic already contained inside the writer task

    ctx.events
        .control(EngineEvent::ConnectionClosed {
            id,
            code,
            reason: reason.clone(),
        })
        .await;
    (code, reason)
}

async fn write_loop<Snk: FrameSink>(
    mut sink: Snk,
    mailbox: Mailbox,
    mut control_rx: mpsc::Receiver<Control>,
    metrics: Arc<Metrics>,
) {
    let mut close_sent = false;
    loop {
        tokio::select! {
            biased;
            ctrl = control_rx.recv() => match ctrl {
                Some(Control::Ping(payload)) => {
                    if !close_sent && sink.send_frame(OutFrame::Ping(payload)).await.is_err() {
                        break;
                    }
                }
                Some(Control::Close { code, reason }) => {
                    if !close_sent {
                        // A failure here is benign: the codec may already
                        // have answered the peer's close itself.
                        let _ = sink.send_frame(OutFrame::Close { code, reason }).await;
                        close_sent = true;
                    }
                }
                None => break,
            },
            frame = mailbox.pop() => match frame {
                // After a close frame, queued data is discarded (RFC 6455: no
                // data after Close) — but the mailbox keeps getting drained so
                // its closure always ends the writer (no exit deadlock).
                Some(_) if close_sent => {}
                Some(OutboundFrame { data, is_binary }) => {
                    let len = data.len() as u64;
                    let out = if is_binary {
                        OutFrame::Binary(data)
                    } else {
                        // JS strings arrive as valid UTF-8; a non-UTF-8 "text"
                        // send is a caller bug — fall back to binary rather
                        // than poison the connection.
                        match String::from_utf8(data) {
                            Ok(s) => OutFrame::Text(s),
                            Err(e) => OutFrame::Binary(e.into_bytes()),
                        }
                    };
                    if sink.send_frame(out).await.is_err() {
                        break;
                    }
                    Metrics::add(&metrics.messages_out, 1);
                    Metrics::add(&metrics.bytes_out, len);
                }
                None => break, // mailbox closed and drained
            },
        }
    }
    // Best-effort: flush any pending close handshake bytes.
    sink.shutdown().await;
}

async fn read_loop<Src: FrameSource>(
    id: ConnectionId,
    mut source: Src,
    handle: &ConnHandle,
    mut close_rx: watch::Receiver<Option<CloseCmd>>,
    ctx: &ConnCtx,
) -> (u16, String) {
    let keepalive = ctx.config.keepalive.clone();
    let mut ping_timer = tokio::time::interval_at(
        Instant::now() + keepalive.ping_interval,
        keepalive.ping_interval,
    );
    ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_activity = Instant::now();
    let mut pong_deadline: Option<Instant> = None;
    let mut close_deadline: Option<Instant> = None;
    // Set when we initiate a graceful close: reported when the peer replies.
    let mut initiated: Option<(u16, String)> = None;

    // A close may have been signalled before we started watching.
    if let Some(cmd) = close_rx.borrow_and_update().clone() {
        if !cmd.graceful {
            return (cmd.code, cmd.reason);
        }
        close_deadline = Some(Instant::now() + CLOSE_GRACE);
        initiated = Some((cmd.code, cmd.reason));
    }

    loop {
        let deadline = match (pong_deadline, close_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        tokio::select! {
            biased;
            changed = close_rx.changed() => {
                if changed.is_err() {
                    return (CLOSE_ABNORMAL, "engine dropped".into());
                }
                if let Some(cmd) = close_rx.borrow_and_update().clone() {
                    if !cmd.graceful {
                        return (cmd.code, cmd.reason);
                    }
                    close_deadline = Some(Instant::now() + CLOSE_GRACE);
                    initiated = Some((cmd.code, cmd.reason));
                }
            }
            frame = source.next_frame() => match frame {
                None => {
                    return initiated.unwrap_or((CLOSE_ABNORMAL, "connection reset without close frame".into()));
                }
                Some(Err(e)) => return (e.close_code, e.message),
                Some(Ok(InFrame::Text(s))) => {
                    last_activity = Instant::now();
                    pong_deadline = None;
                    Metrics::add(&ctx.metrics.messages_in, 1);
                    Metrics::add(&ctx.metrics.bytes_in, s.len() as u64);
                    // Hot path: drop-newest into the bounded bridge queue.
                    ctx.events.try_message(id, s.into_bytes(), false);
                }
                Some(Ok(InFrame::Binary(b))) => {
                    last_activity = Instant::now();
                    pong_deadline = None;
                    Metrics::add(&ctx.metrics.messages_in, 1);
                    Metrics::add(&ctx.metrics.bytes_in, b.len() as u64);
                    ctx.events.try_message(id, b, true);
                }
                Some(Ok(InFrame::Pong)) => {
                    last_activity = Instant::now();
                    pong_deadline = None;
                }
                Some(Ok(InFrame::Close { code, reason })) => {
                    // Peer-initiated (or reply to ours). Ask the writer to
                    // answer — benign no-op if the codec already replied or
                    // we sent the first close frame ourselves.
                    let _ = handle.control.try_send(Control::Close {
                        code: if code == 1005 { CLOSE_NORMAL } else { code },
                        reason: String::new(),
                    });
                    // Report OUR code if we initiated, else the peer's.
                    return initiated.unwrap_or((code, reason));
                }
            },
            _ = ping_timer.tick() => {
                if Instant::now().duration_since(last_activity) >= keepalive.ping_interval
                    && pong_deadline.is_none()
                {
                    let _ = handle.control.try_send(Control::Ping(Vec::new()));
                    pong_deadline = Some(Instant::now() + keepalive.pong_timeout);
                }
            }
            _ = maybe_sleep(deadline), if deadline.is_some() => {
                if close_deadline.is_some_and(|d| Instant::now() >= d) {
                    let (code, _) = initiated.clone().unwrap_or((CLOSE_ABNORMAL, String::new()));
                    return (code, "close handshake timed out".into());
                }
                return (CLOSE_ABNORMAL, "keepalive timeout: no pong".into());
            }
        }
    }
}

async fn maybe_sleep(deadline: Option<Instant>) {
    match deadline {
        Some(d) => sleep_until(d).await,
        None => std::future::pending().await,
    }
}
