//! WebSocket transport — Phase 1A (payloads Bytes-refcounted since 1B).
//!
//! Codec: tokio-tungstenite first (correctness, Autobahn-proven), behind the
//! `Transport` trait so a fastwebsockets swap stays contained here
//! (ARCHITECTURE.md §7 "codec choice regret").
//!
//! permessage-deflate is OFF in Phase 1 (memory blowup risk, ~300 KB/conn) —
//! tungstenite does not negotiate it, so no extension is ever accepted.
//!
//! Codec bookkeeping stays inside the codec (Rule 1): tungstenite
//! auto-replies to Ping with Pong and answers the peer's Close frame itself;
//! oversized messages (`max_payload_bytes`) and protocol violations produce
//! the RFC-correct close frames on the wire. The engine only sees the
//! transport-neutral `InFrame`/`TransportError` view.

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

use crate::config::Config;
use crate::transport::{FrameSink, FrameSource, InFrame, OutFrame, Transport, TransportError};

/// Initial codec read-buffer size. tungstenite's default (128 KiB) is tuned
/// for throughput on few sockets; BeamSocket's density target wants small
/// initial buffers that grow on demand (ARCHITECTURE.md §5: "tune read
/// buffers to 4 KB initial"). Re-validated by the 10k-idle RSS gate.
const READ_BUFFER_SIZE: usize = 4 * 1024;

pub struct WebSocketTransport;

pub struct WsSource(SplitStream<WebSocketStream<TcpStream>>);
pub struct WsSink(SplitSink<WebSocketStream<TcpStream>, Message>);

fn map_err(e: WsError) -> TransportError {
    let (close_code, message) = match &e {
        WsError::Capacity(_) => (1009, e.to_string()),
        WsError::Protocol(_) => (1002, e.to_string()),
        WsError::Utf8(_) => (1007, e.to_string()),
        // Includes Io, AlreadyClosed, ConnectionClosed after an unclean drop…
        _ => (1006, e.to_string()),
    };
    TransportError {
        close_code,
        message,
    }
}

impl Transport for WebSocketTransport {
    type Source = WsSource;
    type Sink = WsSink;

    async fn accept(
        io: TcpStream,
        config: &Config,
    ) -> Result<(Self::Sink, Self::Source), TransportError> {
        // Admission limit enforced in Rust before any JS runs: a frame or
        // message over the cap is rejected by the codec with close 1009.
        let ws_cfg = WebSocketConfig::default()
            .max_message_size(Some(config.limits.max_payload_bytes))
            .max_frame_size(Some(config.limits.max_payload_bytes))
            .read_buffer_size(READ_BUFFER_SIZE);

        let ws = tokio_tungstenite::accept_async_with_config(io, Some(ws_cfg))
            .await
            .map_err(map_err)?;
        let (sink, stream) = ws.split();
        Ok((WsSink(sink), WsSource(stream)))
    }
}

impl FrameSource for WsSource {
    async fn next_frame(&mut self) -> Option<Result<InFrame, TransportError>> {
        loop {
            match self.0.next().await? {
                // Utf8Bytes → Bytes is a refcount move, not a copy; validity
                // was already checked by the codec.
                Ok(Message::Text(s)) => return Some(Ok(InFrame::Text(Bytes::from(s)))),
                Ok(Message::Binary(b)) => return Some(Ok(InFrame::Binary(b))),
                // tungstenite already queued the Pong (codec bookkeeping);
                // pings otherwise carry no engine-visible information.
                Ok(Message::Ping(_)) => continue,
                Ok(Message::Pong(_)) => return Some(Ok(InFrame::Pong)),
                Ok(Message::Close(frame)) => {
                    let (code, reason) = match frame {
                        Some(CloseFrame { code, reason }) => {
                            (u16::from(code), reason.as_str().to_owned())
                        }
                        None => (1005, String::new()), // no status present
                    };
                    return Some(Ok(InFrame::Close { code, reason }));
                }
                // Raw frames only surface with capability flags we don't set.
                Ok(Message::Frame(_)) => continue,
                Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => return None,
                Err(e) => return Some(Err(map_err(e))),
            }
        }
    }
}

impl FrameSink for WsSink {
    async fn send_frame(&mut self, frame: OutFrame) -> Result<(), TransportError> {
        let msg = match frame {
            // The clone is a refcount bump; Utf8Bytes wraps the SAME
            // allocation on success (zero-copy validation).
            OutFrame::Text(b) => match Utf8Bytes::try_from(b.clone()) {
                Ok(s) => Message::Text(s),
                // JS strings are always valid UTF-8; this is a non-JS caller
                // bug — degrade to binary rather than poison the connection.
                Err(_) => Message::Binary(b),
            },
            OutFrame::Binary(b) => Message::Binary(b),
            OutFrame::Ping(p) => Message::Ping(p),
            OutFrame::Close { code, reason } => Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            })),
        };
        self.0.send(msg).await.map_err(map_err)
    }

    async fn shutdown(&mut self) {
        // poll_close on the underlying WebSocketStream sends a close frame if
        // one hasn't been sent and flushes pending bytes (incl. a queued ack).
        let _ = self.0.close().await;
    }
}
