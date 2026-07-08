//! RFC 0002 de-risking spike — THROWAWAY, Linux + plaintext only.
//!
//! Proves the Node-specific unknown from §8.1/§8.3: take a raw fd out of a Node
//! `http.Server` upgrade socket, `dup()` it so Rust owns an independent handle,
//! adopt it as a Tokio `TcpStream`, complete the WebSocket 101 ourselves,
//! REPLAY the `head` bytes Node already read (so a first frame coalesced with
//! the upgrade is not lost), and echo. If a `ws` client — and a raw client that
//! coalesces its first frame with the upgrade — both echo through an attached
//! Express/http server, design A (fd handoff) is proven on Linux.
//!
//! Not production code. No error-path RAII, no metrics, no gate/authorize — the
//! RFC specifies those; the spike only de-risks the mechanic.

use std::io;
use std::os::fd::FromRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{SinkExt, StreamExt};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Reads `head` first, then the socket; writes go straight to the socket.
/// `TcpStream` + `Vec`/`usize` are all `Unpin`, so no pin-projection needed.
struct PrefixedStream {
    head: Vec<u8>,
    pos: usize,
    inner: TcpStream,
}

impl AsyncRead for PrefixedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if me.pos < me.head.len() {
            let remaining = &me.head[me.pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            me.pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut me.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Adopt the dup of `fd`, finish the 101 for `ws_key`, replay `head`, and echo.
/// Fire-and-forget: runs the connection on its own thread + current-thread
/// runtime (spike-grade; the real engine adopts onto its existing runtime).
#[napi]
pub fn adopt_and_echo(fd: i32, ws_key: String, head: Buffer) -> Result<()> {
    // §8.1 step 3: dup → Rust owns an independent fd; the connection survives
    // Node closing its original (POSIX dup semantics).
    let dup = unsafe { libc::dup(fd) };
    if dup < 0 {
        return Err(Error::from_reason(format!(
            "dup(fd={fd}) failed: {}",
            io::Error::last_os_error()
        )));
    }
    let head = head.to_vec();

    std::thread::Builder::new()
        .name("attach-spike-conn".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("[spike] runtime build failed: {e}");
                    unsafe { libc::close(dup) };
                    return;
                }
            };
            rt.block_on(async move {
                if let Err(e) = run(dup, ws_key, head).await {
                    eprintln!("[spike] connection error: {e}");
                }
            });
        })
        .map_err(|e| Error::from_reason(format!("spawn failed: {e}")))?;
    Ok(())
}

async fn run(dup_fd: i32, ws_key: String, head: Vec<u8>) -> io::Result<()> {
    // §8.1 step 7: rebuild a Tokio TcpStream from the dup'd fd.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(dup_fd) };
    std_stream.set_nonblocking(true)?;
    let mut stream = TcpStream::from_std(std_stream)?;
    stream.set_nodelay(true).ok();

    // §8.3: write the 101 ourselves (Sec-WebSocket-Accept from the Node-parsed
    // key) BEFORE framing starts.
    let accept = derive_accept_key(ws_key.as_bytes());
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;

    // §8.3: frame over head-then-wire so a coalesced first frame is not lost.
    let prefixed = PrefixedStream {
        head,
        pos: 0,
        inner: stream,
    };
    let mut ws = WebSocketStream::from_raw_socket(prefixed, Role::Server, None).await;

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                ws.send(Message::Text(t)).await.ok();
            }
            Ok(Message::Binary(b)) => {
                ws.send(Message::Binary(b)).await.ok();
            }
            Ok(Message::Close(_)) => {
                let _ = ws.close(None).await;
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    Ok(())
}
