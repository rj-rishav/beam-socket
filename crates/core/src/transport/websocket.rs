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

use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

use crate::config::Config;
use crate::limits::{AdmittedUpgrade, Gate};
use crate::transport::{
    AcceptError, Accepted, FrameSink, FrameSource, InFrame, OutFrame, Transport, TransportError,
};

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

    // The handshake callback's `Err` type (`ErrorResponse` = an HTTP response)
    // is tungstenite's API, not ours — we can't box it. Allowed narrowly.
    #[allow(clippy::result_large_err)]
    async fn accept(
        io: TcpStream,
        peer: IpAddr,
        config: &Config,
        gate: &Gate,
    ) -> Result<Accepted<Self::Sink, Self::Source>, AcceptError> {
        // max_payload_bytes enforced in Rust before any JS runs: a frame or
        // message over the cap is rejected by the codec with close 1009.
        let ws_cfg = WebSocketConfig::default()
            .max_message_size(Some(config.limits.max_payload_bytes))
            .max_frame_size(Some(config.limits.max_payload_bytes))
            .read_buffer_size(READ_BUFFER_SIZE);

        // The gate runs INSIDE the handshake callback (sync): resolve the
        // client IP and enforce maxConnectionsPerIp BEFORE the upgrade
        // completes, so a rejected connection is a plain HTTP 429 and never
        // becomes a WebSocket. The admitted upgrade (with its per-IP guard) is
        // handed out through this slot because the callback is `FnOnce`.
        let slot: Arc<Mutex<Option<AdmittedUpgrade>>> = Arc::new(Mutex::new(None));
        let slot_cb = slot.clone();
        let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
            // Capture headers (names lowercased) + target for authorize.
            let mut headers = Vec::with_capacity(req.headers().len());
            for (name, value) in req.headers().iter() {
                headers.push((
                    name.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                ));
            }
            let url = req
                .uri()
                .path_and_query()
                .map(|pq| pq.as_str().to_owned())
                .unwrap_or_else(|| req.uri().to_string());

            match gate.admit(peer, headers, url) {
                Ok(upgrade) => {
                    *slot_cb.lock().unwrap() = Some(upgrade);
                    Ok(resp)
                }
                Err(status) => {
                    // Reject the upgrade with an HTTP status — no WebSocket is
                    // ever created (cheaper than close-after-handshake).
                    let mut err = ErrorResponse::new(Some(
                        "connection rejected: per-IP connection limit reached".to_owned(),
                    ));
                    *err.status_mut() =
                        StatusCode::from_u16(status).unwrap_or(StatusCode::TOO_MANY_REQUESTS);
                    Err(err)
                }
            }
        };

        match tokio_tungstenite::accept_hdr_async_with_config(io, callback, Some(ws_cfg)).await {
            Ok(ws) => {
                let upgrade = slot
                    .lock()
                    .unwrap()
                    .take()
                    .expect("gate stored the admitted upgrade on success");
                let (sink, stream) = ws.split();
                Ok(Accepted {
                    sink: WsSink(sink),
                    source: WsSource(stream),
                    upgrade,
                })
            }
            Err(e) => match slot.lock().unwrap().take() {
                // Admit happened, then the handshake failed: dropping `upgrade`
                // here releases the per-IP slot (its guard's Drop).
                Some(_upgrade) => Err(AcceptError::Handshake(map_err(e))),
                // Gate rejected (429) or the request was malformed before the
                // gate ran — nothing was admitted.
                None => Err(AcceptError::Rejected),
            },
        }
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
