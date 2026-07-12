//! Shared harness for the §13.1 gate tests.
//!
//! Two flavors: a **sans-IO driver** (`drive`) that runs two [`Handshake`]
//! machines against each other with an optional MITM transform — deterministic,
//! no sockets — used by the interop/tamper/reflection/cluster gates; and a
//! **manual link handshake** (`manual_handshake`) over a real `TcpStream`, used
//! by the gates that must then inject raw bytes a well-behaved [`LinkHandle`]
//! would never send (unknown kinds, oversize frames).

#![allow(dead_code)] // each test binary uses a different subset of this module

use std::error::Error;

use beamsocket_mesh::frame::{Flags, Frame, FrameKind};
use beamsocket_mesh::handshake::HandshakeStep;
use beamsocket_mesh::{Handshake, Link, LinkConfig, LinkHandle, Negotiated, RefuseReason, Role};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A test config: fixed shared secret, timeouts short enough to keep the suite
/// fast, and keepalive intervals long enough that PINGs do not perturb the
/// frame counts a test asserts on. Callers mutate fields (queue HWM, versions)
/// as a gate needs.
pub fn test_cfg(node_id: u16, cluster: &str, version: u16, features: u32) -> LinkConfig {
    let mut c = LinkConfig::new(node_id, cluster, b"cluster-secret".to_vec());
    c.protocol_version = version;
    c.features = features;
    c.auth_timeout = std::time::Duration::from_secs(2);
    // Push idle liveness far out so a short test never sees a spontaneous PING.
    c.idle_ping_interval = std::time::Duration::from_secs(60);
    c.idle_dead_after = std::time::Duration::from_secs(120);
    c
}

/// Drive an initiator and a responder against each other. `mitm(role, frame)`
/// may mutate each frame in flight, tagged by the role that authored it. Returns
/// each side's terminal outcome.
pub fn drive(
    mut init: Handshake,
    mut resp: Handshake,
    mut mitm: impl FnMut(Role, &mut Frame),
) -> (
    Result<Negotiated, RefuseReason>,
    Result<Negotiated, RefuseReason>,
) {
    let mut to_resp: Vec<Frame> = vec![init.start()];
    let mut to_init: Vec<Frame> = vec![resp.start()];
    let mut init_done: Option<Result<Negotiated, RefuseReason>> = None;
    let mut resp_done: Option<Result<Negotiated, RefuseReason>> = None;

    for _ in 0..16 {
        let mut next_to_init = Vec::new();
        for mut f in to_resp.drain(..) {
            mitm(Role::Initiator, &mut f);
            match resp.on_frame(&f) {
                HandshakeStep::Continue(out) => next_to_init.extend(out),
                HandshakeStep::Established { send, negotiated } => {
                    next_to_init.extend(send);
                    resp_done.get_or_insert(Ok(negotiated));
                }
                HandshakeStep::Refused(r) => {
                    resp_done.get_or_insert(Err(r));
                }
            }
        }
        let mut next_to_resp = Vec::new();
        for mut f in to_init.drain(..) {
            mitm(Role::Responder, &mut f);
            match init.on_frame(&f) {
                HandshakeStep::Continue(out) => next_to_resp.extend(out),
                HandshakeStep::Established { send, negotiated } => {
                    next_to_resp.extend(send);
                    init_done.get_or_insert(Ok(negotiated));
                }
                HandshakeStep::Refused(r) => {
                    init_done.get_or_insert(Err(r));
                }
            }
        }
        to_init = next_to_init;
        to_resp = next_to_resp;
        if to_init.is_empty() && to_resp.is_empty() {
            break;
        }
    }

    (
        init_done.unwrap_or(Err(RefuseReason::ProtocolViolation("no outcome"))),
        resp_done.unwrap_or(Err(RefuseReason::ProtocolViolation("no outcome"))),
    )
}

/// No-op MITM.
pub fn passthrough(_: Role, _: &mut Frame) {}

/// Poll `f` every 5 ms until it returns true or `timeout` elapses. Async link
/// tests observe counters that a background reader/writer updates, so they wait
/// on a condition rather than a fixed sleep.
pub async fn poll_until(mut f: impl FnMut() -> bool, timeout: std::time::Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if f() {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Bind a loopback listener and establish a real link pair: `cfg_a` dials
/// (initiator), `cfg_b` accepts (responder). Returns both live handles.
pub async fn connected_pair(cfg_a: LinkConfig, cfg_b: LinkConfig) -> (LinkHandle, LinkHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        Link::accept(stream, cfg_b).await.unwrap()
    });
    let a = Link::connect(addr, cfg_a).await.unwrap();
    let b = accept.await.unwrap();
    (a, b)
}

// ── manual link handshake over a real socket (for raw-injection gates) ──

/// Byte offset of the feature-bits field inside a HELLO body:
/// magic(4)+version(2)+node_id(2)+incarnation(8)+max_frame(4) = 20.
pub const HELLO_FEATURES_OFFSET: usize = 20;

/// Drive `role`'s handshake to completion over `stream`, leaving the stream
/// positioned right after AUTH so the caller can inject raw post-handshake
/// bytes. Returns the negotiated parameters (the caller needs `max_frame`).
pub async fn manual_handshake(
    stream: &mut TcpStream,
    role: Role,
    cfg: LinkConfig,
) -> Result<Negotiated, Box<dyn Error + Send + Sync>> {
    let mut hs = Handshake::new(role, cfg);
    let opening = hs.start();
    write_frame(stream, &opening).await?;
    loop {
        let frame = read_frame(stream).await?;
        match hs.on_frame(&frame) {
            HandshakeStep::Continue(send) => {
                for f in send {
                    write_frame(stream, &f).await?;
                }
            }
            HandshakeStep::Established { send, negotiated } => {
                for f in send {
                    write_frame(stream, &f).await?;
                }
                return Ok(negotiated);
            }
            HandshakeStep::Refused(r) => return Err(format!("handshake refused: {r}").into()),
        }
    }
}

pub async fn write_frame(
    stream: &mut TcpStream,
    frame: &Frame,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    stream.write_all(&frame.encode()).await?;
    Ok(())
}

/// Read one whole frame. Trusts the peer's lengths (this is a test helper for
/// the handshake phase, not the production reader).
pub async fn read_frame(stream: &mut TcpStream) -> Result<Frame, Box<dyn Error + Send + Sync>> {
    let mut len4 = [0u8; 4];
    stream.read_exact(&mut len4).await?;
    let len = u32::from_le_bytes(len4) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let kind = FrameKind::from_u8(buf[0]).ok_or("unknown kind in test read_frame")?;
    Ok(Frame::with_flags(kind, Flags(buf[1]), buf[2..].to_vec()))
}

/// Write a raw, hand-encoded frame: `[len u32 LE][kind][flags][body]`. Used to
/// inject frames the typed API forbids (an unknown kind, or an oversize `len`).
pub async fn write_raw_frame(
    stream: &mut TcpStream,
    len: u32,
    kind: u8,
    flags: u8,
    body: &[u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = Vec::with_capacity(4 + body.len() + 2);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.push(kind);
    buf.push(flags);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await?;
    Ok(())
}
