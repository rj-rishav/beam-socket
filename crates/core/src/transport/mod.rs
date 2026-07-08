//! Transport abstraction. Everything above this module operates on frames
//! and ConnectionIds, never raw sockets — that's what makes TCP/MQTT/SSE/QUIC
//! (Phase 5) additive instead of a rewrite.
//!
//! Kept deliberately minimal until a second transport forces generalization:
//! one accept/handshake entry point and split read/write frame halves. The
//! connection task (connection/mod.rs) is generic over these traits, which is
//! also what lets unit tests inject mock (and deliberately panicking)
//! transports without a socket.
//!
//! Payloads are `bytes::Bytes` in BOTH directions (Phase 1B): inbound frames
//! borrow the codec's read buffer refcount instead of copying; outbound
//! frames are refcounted so a broadcast enqueues N handle clones of ONE
//! allocation (ENGINEERING.md §6).

use std::future::Future;
use std::net::IpAddr;

use bytes::Bytes;
use tokio::net::TcpStream;

use crate::config::Config;
use crate::limits::{AdmittedUpgrade, Gate};

/// A frame arriving from the peer, already decoded by the transport.
/// Ping is intentionally absent: replying to pings is codec bookkeeping and
/// is handled inside the transport (Rule 1 — never JS, and not the engine's
/// business either).
#[derive(Debug)]
pub enum InFrame {
    /// UTF-8 validated by the codec.
    Text(Bytes),
    Binary(Bytes),
    /// Peer answered a keepalive ping (or sent an unsolicited pong).
    Pong,
    /// Peer initiated (or acknowledged) the close handshake.
    Close {
        code: u16,
        reason: String,
    },
}

/// A frame the engine asks the transport to write.
#[derive(Debug)]
pub enum OutFrame {
    /// Must be valid UTF-8 (JS strings always are); the WebSocket transport
    /// re-validates and falls back to Binary rather than poison the stream.
    Text(Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Close {
        code: u16,
        reason: String,
    },
}

/// Transport-level failure. `close_code` is what we *report* to the app
/// (1002 protocol error, 1007 bad UTF-8, 1009 too big, 1006 abnormal);
/// RFC-compliant close frames on the wire are the codec's job
/// (tokio-tungstenite handles this for WebSocket).
#[derive(Debug)]
pub struct TransportError {
    pub close_code: u16,
    pub message: String,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transport error ({}): {}", self.close_code, self.message)
    }
}

impl std::error::Error for TransportError {}

/// Read half of an accepted connection.
pub trait FrameSource: Send + Unpin + 'static {
    /// `None` = EOF (peer went away without a close frame).
    fn next_frame(
        &mut self,
    ) -> impl Future<Output = Option<Result<InFrame, TransportError>>> + Send;
}

/// Write half of an accepted connection.
pub trait FrameSink: Send + Unpin + 'static {
    fn send_frame(
        &mut self,
        frame: OutFrame,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Best-effort teardown: flush anything pending (including a close frame
    /// the codec queued itself) before the socket drops. Errors are moot.
    fn shutdown(&mut self) -> impl Future<Output = ()> + Send;
}

/// A completed, admitted upgrade: the split frame halves plus the resolved
/// client IP, captured request metadata (for `authorize`), and the RAII per-IP
/// admission slot (Phase 1C).
pub struct Accepted<Snk, Src> {
    pub sink: Snk,
    pub source: Src,
    pub upgrade: AdmittedUpgrade,
}

/// Why an accept did not yield a live connection. Either variant means "return,
/// don't proceed" for the caller; any per-IP slot reserved by the gate is
/// released internally before this is returned, so the caller has no cleanup.
#[derive(Debug)]
pub enum AcceptError {
    /// The gate rejected the upgrade before it completed (e.g. the per-IP cap —
    /// an HTTP 429 was sent), or the request was malformed before the gate ran.
    /// Nothing was admitted; no JS ever runs for this attempt.
    Rejected,
    /// The protocol handshake failed AFTER admission (rare). The admission slot
    /// has already been released.
    Handshake(TransportError),
}

/// Implemented by transport/websocket.rs in Phase 1A (Phase 1C: gated accept).
pub trait Transport: Send + Sync + 'static {
    type Source: FrameSource;
    type Sink: FrameSink;

    /// Perform the protocol handshake on an accepted TCP stream. The `gate`
    /// runs INSIDE the handshake (Phase 1C): it resolves the client IP from
    /// `peer` + `X-Forwarded-For` per `trustProxy` and enforces
    /// `maxConnectionsPerIp` BEFORE the upgrade completes — a rejected
    /// connection never becomes a WebSocket. On success the split frame halves
    /// come back alongside the resolved IP, captured headers, and the per-IP
    /// admission guard (see [`Accepted`]).
    fn accept(
        io: TcpStream,
        peer: IpAddr,
        config: &Config,
        gate: &Gate,
    ) -> impl Future<Output = Result<Accepted<Self::Sink, Self::Source>, AcceptError>> + Send;

    /// **Attach path (Phase 1.1, RFC 0002 §5/§8.3).** The second producer into
    /// the connection lifecycle: the HTTP request was already parsed by Node
    /// (there is nothing left to read for the handshake), and the admission
    /// `Gate` has already run in `engine.attach` — so this only COMPLETES the
    /// WebSocket 101 (`Sec-WebSocket-Accept` from the pre-parsed `ws_key`) and
    /// starts framing over a stream that REPLAYS `head` before the wire, so a
    /// first frame coalesced with the upgrade is not lost. Returns the same
    /// split frame halves `accept` does, so downstream (`run_admitted`) is
    /// byte-for-byte shared.
    fn adopt(
        io: TcpStream,
        ws_key: &str,
        head: Bytes,
        config: &Config,
    ) -> impl Future<Output = Result<(Self::Sink, Self::Source), TransportError>> + Send;
}

pub mod websocket;

pub use websocket::WebSocketTransport;
