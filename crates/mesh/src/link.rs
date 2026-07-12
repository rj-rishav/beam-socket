//! Link lifecycle (RFC 0004 §4.4/§4.6/§4.7): connect/accept, the async
//! handshake driver, the **coalesced writer**, the framed reader, idle
//! PING/PONG, clean close, and the reconnect-with-backoff seam (3B).
//!
//! This is the only async module in the crate. It is deliberately thin: the
//! handshake is [`crate::handshake`]'s sans-IO machine driven over a
//! `TcpStream`, and the send path is [`crate::queue::PeerQueue`]. What lives
//! here is the wiring — spawning the writer and reader, enforcing the
//! auth timeout, translating a length prefix into a bounded read, and turning a
//! send request into a suppression check plus an enqueue.
//!
//! The writer is the spike-forced requirement: it drains the per-peer queue
//! **coalesced**, one `write` per wakeup (see [`COALESCE_CAP_BYTES`]).

use std::io::ErrorKind;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::Notify;

use crate::config::LinkConfig;
use crate::counters::LinkCounters;
use crate::frame::{decode_len, Flags, Frame, FrameError, FrameKind, HEADER_LEN};
use crate::handshake::{ping_frame, Handshake, Negotiated, RefuseReason, Role};
use crate::queue::{PeerQueue, PushOutcome};

/// The coalesce cap: the writer packs up to this many bytes of queued frames
/// into a single `write` per wakeup. **128 KiB is the spike's constant**
/// (`0004-results.md`, "What the spike changed" #1): per-frame writes measured
/// 3.8 ms p99 at 100k msgs/s; coalescing everything queued into one write per
/// wakeup, capped at 128 KiB, cut that 5.5× to 680 µs. The constant re-derives
/// on real hardware (the spike ran on a shared, loopback sandbox); the
/// *decision* to coalesce does not.
pub const COALESCE_CAP_BYTES: usize = 128 * 1024;

/// A live link's coarse state — a distinct value per terminal outcome so
/// metrics (and 3B's reconnect loop) can tell "refused, do not retry" from
/// "authed, then closed" (§4.4: refusals are a distinct link-state, never a
/// silent retry loop). Stored as a `u8` for lock-free reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LinkState {
    Up = 1,
    Closing = 2,
    Closed = 3,
    Refused = 4,
}

impl LinkState {
    fn from_u8(v: u8) -> LinkState {
        match v {
            1 => LinkState::Up,
            2 => LinkState::Closing,
            4 => LinkState::Refused,
            _ => LinkState::Closed,
        }
    }
}

/// The reason a `send` was suppressed: the mesh's own code tried to emit a kind
/// the peer never advertised (§4.4). Never a wire event — the frame is dropped
/// before the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Suppressed(pub FrameKind);

impl std::fmt::Display for Suppressed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sender-suppression: {:?} is not negotiated on this link",
            self.0
        )
    }
}

/// Called by the reader for each inbound **data-plane** frame (MEMBERSHIP,
/// INTEREST*, RELAY*) with the peer's node id. 3B uses it to feed MEMBERSHIP
/// frames to the dissemination plane; 3A's PING/PONG and control frames never
/// reach it. Runs on the link's reader task — keep it cheap and non-blocking.
pub type InboundHandler = Arc<dyn Fn(u16, &Frame) + Send + Sync>;

/// Called once when the link's reader exits (peer closed, IO error, oversize, or
/// a local `close()`), with the peer's node id. 3B wires this to suspicion —
/// a dead TCP link is evidence the peer may be gone.
pub type CloseHandler = Arc<dyn Fn(u16) + Send + Sync>;

/// Optional per-link callbacks. `Default` (both `None`) reproduces exact 3A
/// behavior, so the existing `connect`/`accept` API is unchanged.
#[derive(Clone, Default)]
pub struct LinkHooks {
    pub inbound: Option<InboundHandler>,
    pub on_close: Option<CloseHandler>,
}

/// A handle to a live link. Cheap to clone (all shared state is `Arc`); holds
/// the send queue, the counters, and the negotiated parameters. The reader,
/// writer, and keepalive run as background tasks; dropping every handle does
/// **not** close the link — call [`LinkHandle::close`] for that (the tasks own
/// the sockets).
#[derive(Clone)]
pub struct LinkHandle {
    negotiated: Negotiated,
    queue: Arc<PeerQueue>,
    counters: Arc<LinkCounters>,
    state: Arc<AtomicU8>,
    shutdown: Arc<Notify>,
    peer_node_id: u16,
}

impl LinkHandle {
    pub fn peer_node_id(&self) -> u16 {
        self.peer_node_id
    }

    pub fn negotiated(&self) -> &Negotiated {
        &self.negotiated
    }

    pub fn counters(&self) -> &Arc<LinkCounters> {
        &self.counters
    }

    /// Per-peer pressure gauge (§4.6), `0.0..=1.0`.
    pub fn pressure(&self) -> f64 {
        self.queue.pressure()
    }

    /// Per-peer `relayDrops` (§4.6).
    pub fn drops(&self) -> u64 {
        self.queue.drops()
    }

    pub fn state(&self) -> LinkState {
        LinkState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Enqueue a frame for the peer, enforcing **sender suppression** (§4.4):
    /// an un-negotiated kind is refused (counted, never written) and returned as
    /// `Err(Suppressed)`. On success the frame is byte-bounded-queued — the
    /// return distinguishes `Enqueued` from a queue-overflow `Dropped`, neither
    /// of which blocks the caller.
    pub fn try_send(&self, frame: Frame) -> Result<PushOutcome, Suppressed> {
        if !self.negotiated.may_emit(frame.kind) {
            LinkCounters::add(&self.counters.suppressed_emits, 1);
            return Err(Suppressed(frame.kind));
        }
        Ok(self.queue.push(frame.encode()))
    }

    /// The production send wrapper: a suppression violation is a **bug** in the
    /// caller (it tried to emit something un-negotiated), so it is a
    /// `debug_assert` in dev builds and a counted no-op in release — exactly the
    /// §4.4 rule ("debug_assert + counted error, not a wire write"). Tests that
    /// want to *observe* the counted refusal without panicking call
    /// [`LinkHandle::try_send`].
    pub fn send(&self, frame: Frame) -> PushOutcome {
        match self.try_send(frame) {
            Ok(outcome) => outcome,
            Err(s) => {
                debug_assert!(false, "{s}");
                PushOutcome::Dropped
            }
        }
    }

    /// Begin a clean close: stop the reader and keepalive, and let the writer
    /// drain what is queued and exit. Idempotent.
    pub fn close(&self) {
        self.state
            .store(LinkState::Closing as u8, Ordering::Release);
        self.shutdown.notify_waiters();
        self.queue.close();
    }
}

/// Errors from establishing a link. Not `PartialEq` (it wraps `io::Error`);
/// callers match on the variant.
#[derive(Debug)]
pub enum LinkError {
    /// Socket-level failure during connect/handshake IO.
    Io(std::io::Error),
    /// The handshake was refused; `reason` is the distinct link-state.
    Refused(RefuseReason),
    /// AUTH was not reached within `auth_timeout` (counted `authTimeouts`).
    AuthTimeout,
    /// A length prefix exceeded the (pre-negotiation, local) max frame during
    /// the handshake — closed, no resync.
    Oversize { len: u32, max: u32 },
    /// A malformed frame during the handshake (len < 2).
    Protocol(&'static str),
    /// The peer closed before the handshake completed.
    Eof,
}

impl std::fmt::Display for LinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkError::Io(e) => write!(f, "link io error: {e}"),
            LinkError::Refused(r) => write!(f, "link refused: {r}"),
            LinkError::AuthTimeout => write!(f, "handshake auth timeout"),
            LinkError::Oversize { len, max } => {
                write!(f, "oversize frame during handshake: {len} > {max}")
            }
            LinkError::Protocol(s) => write!(f, "protocol error during handshake: {s}"),
            LinkError::Eof => write!(f, "peer closed before handshake completed"),
        }
    }
}

impl std::error::Error for LinkError {}

impl From<std::io::Error> for LinkError {
    fn from(e: std::io::Error) -> Self {
        LinkError::Io(e)
    }
}

/// Namespace for link construction. `connect` dials (initiator role); `accept`
/// takes an already-accepted `TcpStream` (responder role).
pub struct Link;

impl Link {
    /// Dial `addr`, run the handshake as the **initiator**, and spawn the link
    /// tasks. Returns once the link is `Up`.
    pub async fn connect(
        addr: impl ToSocketAddrs,
        cfg: LinkConfig,
    ) -> Result<LinkHandle, LinkError> {
        Self::connect_with(addr, cfg, LinkHooks::default()).await
    }

    /// [`Link::connect`] with inbound/close hooks (3B wires the dissemination
    /// plane + suspicion here).
    pub async fn connect_with(
        addr: impl ToSocketAddrs,
        cfg: LinkConfig,
        hooks: LinkHooks,
    ) -> Result<LinkHandle, LinkError> {
        let stream = TcpStream::connect(addr).await?;
        establish(stream, cfg, Role::Initiator, hooks).await
    }

    /// Run the handshake as the **responder** on an accepted stream, and spawn
    /// the link tasks.
    pub async fn accept(stream: TcpStream, cfg: LinkConfig) -> Result<LinkHandle, LinkError> {
        Self::accept_with(stream, cfg, LinkHooks::default()).await
    }

    /// [`Link::accept`] with inbound/close hooks.
    pub async fn accept_with(
        stream: TcpStream,
        cfg: LinkConfig,
        hooks: LinkHooks,
    ) -> Result<LinkHandle, LinkError> {
        establish(stream, cfg, Role::Responder, hooks).await
    }

    /// The reconnect backoff seam (§4.7 "backoff, not retry storms"). 3B's
    /// reconnect loop consumes this; 3A only exposes the schedule so the policy
    /// is decided (and tested) here, not reinvented there.
    pub fn reconnect_delay(cfg: &LinkConfig, attempt: u32) -> std::time::Duration {
        cfg.backoff.next_delay(attempt)
    }
}

/// Shared idle clock: the reader stamps the last inbound frame; the keepalive
/// reads it to choose "quiet, PING" vs "dead, close". Millis since link start.
#[derive(Clone)]
struct IdleClock {
    epoch: Instant,
    last_in_ms: Arc<AtomicU64>,
}

impl IdleClock {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            last_in_ms: Arc::new(AtomicU64::new(0)),
        }
    }
    fn stamp(&self) {
        self.last_in_ms
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }
    fn idle_ms(&self) -> u64 {
        (self.epoch.elapsed().as_millis() as u64)
            .saturating_sub(self.last_in_ms.load(Ordering::Relaxed))
    }
}

async fn establish(
    stream: TcpStream,
    cfg: LinkConfig,
    role: Role,
    hooks: LinkHooks,
) -> Result<LinkHandle, LinkError> {
    stream.set_nodelay(true).ok();
    let counters = Arc::new(LinkCounters::default());
    let (mut rd, mut wr) = stream.into_split();

    // Bound the whole handshake by auth_timeout: a peer that connects and stalls
    // is closed and counted, never left holding a task.
    let negotiated = match tokio::time::timeout(
        cfg.auth_timeout,
        run_handshake(role, &cfg, &mut rd, &mut wr, &counters),
    )
    .await
    {
        Ok(result) => result?,
        Err(_elapsed) => {
            LinkCounters::add(&counters.auth_timeouts, 1);
            return Err(LinkError::AuthTimeout);
        }
    };

    // Handshake done → steady state. Spawn the coalesced writer, the framed
    // reader, and the idle keepalive.
    let queue = Arc::new(PeerQueue::new(cfg.queue_hwm_bytes));
    let state = Arc::new(AtomicU8::new(LinkState::Up as u8));
    let shutdown = Arc::new(Notify::new());
    let peer_node_id = negotiated.peer_node_id;
    let idle = IdleClock::new();
    let rctx = Arc::new(ReaderCtx {
        queue: queue.clone(),
        counters: counters.clone(),
        negotiated: negotiated.clone(),
        hooks,
    });

    tokio::spawn(writer_loop(queue.clone(), wr, counters.clone()));
    tokio::spawn(reader_loop(
        rd,
        rctx,
        state.clone(),
        shutdown.clone(),
        idle.clone(),
    ));
    tokio::spawn(keepalive_loop(
        queue.clone(),
        counters.clone(),
        state.clone(),
        shutdown.clone(),
        cfg.clone(),
        idle,
    ));

    Ok(LinkHandle {
        negotiated,
        queue,
        counters,
        state,
        shutdown,
        peer_node_id,
    })
}

/// Drive the sans-IO handshake over the split socket.
async fn run_handshake(
    role: Role,
    cfg: &LinkConfig,
    rd: &mut OwnedReadHalf,
    wr: &mut OwnedWriteHalf,
    counters: &Arc<LinkCounters>,
) -> Result<Negotiated, LinkError> {
    use crate::handshake::HandshakeStep;

    let mut hs = Handshake::new(role, cfg.clone());
    let opening = hs.start();
    write_frame(wr, &opening).await?;

    // Pre-negotiation, bound reads by our own declared max frame.
    let max = cfg.max_frame;
    loop {
        let (kind_byte, flags, body) = read_raw_frame(rd, max).await?;
        let kind = match FrameKind::from_u8(kind_byte) {
            Some(k) => k,
            // An unknown kind mid-handshake is not a compatibility case — the
            // peer is not speaking the protocol. Refuse, do not skip.
            None => return Err(LinkError::Protocol("unknown frame kind during handshake")),
        };
        let frame = Frame::with_flags(kind, Flags(flags), body);
        match hs.on_frame(&frame) {
            HandshakeStep::Continue(send) => {
                for f in send {
                    write_frame(wr, &f).await?;
                }
            }
            HandshakeStep::Established { send, negotiated } => {
                for f in send {
                    write_frame(wr, &f).await?;
                }
                return Ok(negotiated);
            }
            HandshakeStep::Refused(reason) => {
                if matches!(reason, RefuseReason::AuthFailed) {
                    LinkCounters::add(&counters.auth_failures, 1);
                }
                return Err(LinkError::Refused(reason));
            }
        }
    }
}

/// The coalesced writer (§4.6). One `write` per wakeup, draining everything
/// queued up to [`COALESCE_CAP_BYTES`]. Exits when the queue is closed and
/// drained.
async fn writer_loop(queue: Arc<PeerQueue>, mut wr: OwnedWriteHalf, counters: Arc<LinkCounters>) {
    let mut scratch = Vec::with_capacity(COALESCE_CAP_BYTES);
    loop {
        // Drain to empty, coalescing each batch into a single write.
        loop {
            scratch.clear();
            let n = queue.drain_coalesced(&mut scratch, COALESCE_CAP_BYTES);
            if n == 0 {
                break;
            }
            if wr.write_all(&scratch).await.is_err() {
                return; // peer gone; the reader will observe the close too
            }
            LinkCounters::add(&counters.bytes_out, scratch.len() as u64);
            LinkCounters::add(&counters.frames_out, n as u64);
        }
        if queue.is_closed() && queue.is_empty() {
            let _ = wr.shutdown().await;
            return;
        }
        queue.wait().await;
        if queue.is_closed() && queue.is_empty() {
            let _ = wr.shutdown().await;
            return;
        }
    }
}

/// The framed reader. Dispatches known kinds, answers PINGs, counts
/// `unknownFrames` for anything a well-behaved peer under sender-suppression
/// would never send, and closes (counted) on an oversize length prefix — no
/// resync (§4.4).
/// Everything the reader task needs, bundled so its arity stays sane.
struct ReaderCtx {
    queue: Arc<PeerQueue>,
    counters: Arc<LinkCounters>,
    negotiated: Negotiated,
    hooks: LinkHooks,
}

async fn reader_loop(
    mut rd: OwnedReadHalf,
    ctx: Arc<ReaderCtx>,
    state: Arc<AtomicU8>,
    shutdown: Arc<Notify>,
    idle: IdleClock,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => break,
            r = read_raw_frame(&mut rd, ctx.negotiated.max_frame) => match r {
                Ok((kind_byte, flags, body)) => {
                    idle.stamp();
                    LinkCounters::add(&ctx.counters.bytes_in, (HEADER_LEN + body.len()) as u64);
                    dispatch(&ctx, kind_byte, flags, &body);
                }
                Err(LinkError::Oversize { .. }) => {
                    // The close IS the response. Count once, do not hunt for the
                    // next boundary on a stream we no longer trust.
                    LinkCounters::add(&ctx.counters.oversize_closes, 1);
                    break;
                }
                Err(_) => break, // EOF / io / malformed → link down
            },
        }
    }
    state.store(LinkState::Closed as u8, Ordering::Release);
    ctx.queue.close();
    // Wire link death into 3B suspicion: a dead link is evidence the peer may
    // be gone (the reconnect loop and probe plane decide what to do with it).
    if let Some(on_close) = &ctx.hooks.on_close {
        on_close(ctx.negotiated.peer_node_id);
    }
}

fn dispatch(ctx: &ReaderCtx, kind_byte: u8, flags: u8, body: &[u8]) {
    let Some(kind) = FrameKind::from_u8(kind_byte) else {
        // Truly unknown kind: skip and count (§4.4 defense-in-depth). Frames are
        // self-delimiting, so skipping is safe; under sender suppression this
        // never fires.
        LinkCounters::add(&ctx.counters.unknown_frames, 1);
        return;
    };

    // A KNOWN but non-negotiated feature kind should never arrive under sender
    // suppression either — treat it the same way (a bug detector, not a parse).
    if is_feature_gated(kind) && !ctx.negotiated.may_emit(kind) {
        LinkCounters::add(&ctx.counters.unknown_frames, 1);
        return;
    }

    LinkCounters::add(&ctx.counters.frames_in, 1);

    match kind {
        FrameKind::Ping => {
            if Flags(flags).has(Flags::PONG) {
                LinkCounters::add(&ctx.counters.pongs_recv, 1);
            } else {
                // Reply with a PONG. It rides the same bounded queue as data; a
                // dropped PONG just means the peer PINGs again next interval.
                let _ = ctx.queue.push(ping_frame(true).encode());
            }
        }
        // Data-plane frames: hand to the inbound hook if wired (3B's MEMBERSHIP
        // dissemination, 3C/3D later); with no hook this is exactly 3A — counted
        // and dropped.
        FrameKind::Membership
        | FrameKind::Interest
        | FrameKind::InterestDigest
        | FrameKind::RelayRoom
        | FrameKind::RelayUser
        | FrameKind::RelayAll
        | FrameKind::RelaySocket => {
            if let Some(inbound) = &ctx.hooks.inbound {
                let frame = Frame::with_flags(kind, Flags(flags), body.to_vec());
                inbound(ctx.negotiated.peer_node_id, &frame);
            }
        }
        // Handshake kinds after the handshake are a protocol quirk; count as
        // frames_in and ignore (a peer re-HELLOing does not restart auth).
        FrameKind::Hello | FrameKind::Challenge | FrameKind::Auth => {}
    }
}

/// Idle liveness (§4.4): PING on a quiet link; if nothing arrives for
/// `idle_dead_after`, declare the link dead and close it. Distinct from SWIM
/// UDP probes (3B) — this is TCP-link liveness only.
async fn keepalive_loop(
    queue: Arc<PeerQueue>,
    counters: Arc<LinkCounters>,
    state: Arc<AtomicU8>,
    shutdown: Arc<Notify>,
    cfg: LinkConfig,
    idle: IdleClock,
) {
    let dead_after_ms = cfg.idle_dead_after.as_millis() as u64;
    let mut ticker = tokio::time::interval(cfg.idle_ping_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately; consume it so we do not PING the instant
    // the link comes up.
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => break,
            _ = ticker.tick() => {
                if queue.is_closed() {
                    break;
                }
                // Nothing inbound for `idle_dead_after` → the link is dead. Close
                // it (3B will feed this into suspicion; here it is a clean local
                // close). The clock reads 0 until the first inbound frame, so it
                // is measured from link start, which is correct.
                if idle.idle_ms() > dead_after_ms {
                    state.store(LinkState::Closed as u8, Ordering::Release);
                    shutdown.notify_waiters();
                    queue.close();
                    break;
                }
                // Otherwise PING. Control frames are always emittable.
                if queue.push(ping_frame(false).encode()) == PushOutcome::Enqueued {
                    LinkCounters::add(&counters.pings_sent, 1);
                }
            }
        }
    }
}

// ── low-level frame IO ──

async fn write_frame(wr: &mut OwnedWriteHalf, frame: &Frame) -> Result<(), LinkError> {
    wr.write_all(&frame.encode()).await?;
    Ok(())
}

/// Read one frame's `(kind, flags, body)`, validating the length prefix against
/// `max_frame` **before** allocating the body (a hostile length never becomes a
/// large allocation). EOF before a full frame is `LinkError::Eof`.
async fn read_raw_frame(
    rd: &mut OwnedReadHalf,
    max_frame: u32,
) -> Result<(u8, u8, Vec<u8>), LinkError> {
    let mut len4 = [0u8; 4];
    match rd.read_exact(&mut len4).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Err(LinkError::Eof),
        Err(e) => return Err(LinkError::Io(e)),
    }
    let len = u32::from_le_bytes(len4);
    let body_len = match decode_len(len, max_frame) {
        Ok(n) => n,
        Err(FrameError::Oversize { len, max }) => return Err(LinkError::Oversize { len, max }),
        Err(_) => return Err(LinkError::Protocol("malformed length prefix")),
    };
    let mut buf = vec![0u8; body_len];
    match rd.read_exact(&mut buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Err(LinkError::Eof),
        Err(e) => return Err(LinkError::Io(e)),
    }
    // body_len ≥ 2 is guaranteed by decode_len.
    let kind = buf[0];
    let flags = buf[1];
    let body = buf[2..].to_vec();
    Ok((kind, flags, body))
}

/// The data/feature kinds whose presence, un-negotiated, is a sender-suppression
/// violation by the peer (a bug detector on receive).
fn is_feature_gated(kind: FrameKind) -> bool {
    matches!(
        kind,
        FrameKind::Interest
            | FrameKind::InterestDigest
            | FrameKind::RelayRoom
            | FrameKind::RelayUser
            | FrameKind::RelayAll
            | FrameKind::RelaySocket
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_state_round_trips_through_u8() {
        for s in [
            LinkState::Up,
            LinkState::Closing,
            LinkState::Closed,
            LinkState::Refused,
        ] {
            assert_eq!(LinkState::from_u8(s as u8), s);
        }
    }

    #[test]
    fn feature_gated_classification() {
        assert!(is_feature_gated(FrameKind::RelayRoom));
        assert!(is_feature_gated(FrameKind::Interest));
        assert!(!is_feature_gated(FrameKind::Ping));
        assert!(!is_feature_gated(FrameKind::Membership));
    }
}
