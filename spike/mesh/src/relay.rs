//! Relay links (RFC 0004 §4.3/§4.6): one TCP link per peer pair,
//! length-prefixed binary frames, per-peer BOUNDED byte-capped outbound
//! queue with drop-and-count overflow. The latency-sensitive path is
//! hand-rolled binary (JSON stays on the low-rate swim plane).
//!
//! Frames: [len u32 LE][kind u8][body]
//!   1 HELLO     [node_id u16]
//!   2 ECHO_REQ  [seq u64][send_ns u64][payload...]
//!   3 ECHO_ACK  [seq u64][send_ns u64][recv_ns u64]
//!   4 ROOM      [room u32][payload...]           (routing-cell traffic)
//!   5 INTEREST  [count u32][room u32 × count]    (advertised hosted rooms)

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::swim::{MState, Swim};
use crate::{mono_ns, Reservoir};

/// Per-link outbound HWM in bytes (§4.6: byte-capped, not frame-capped).
pub const LINK_HWM_BYTES: usize = 1 << 20;

pub struct PeerLink {
    pub tx: mpsc::UnboundedSender<Vec<u8>>,
    pub queued_bytes: Arc<AtomicUsize>,
}

#[derive(Default)]
pub struct Counters {
    pub bytes_out: AtomicU64,
    pub bytes_in: AtomicU64,
    pub frames_out: AtomicU64,
    pub frames_in: AtomicU64,
    pub drops: AtomicU64,
}

pub struct BenchState {
    pub hop: Reservoir,
    pub rtt: Reservoir,
    pub acked: u64,
    pub sent: u64,
    pub done: bool,
}

pub struct Relay {
    pub self_id: u16,
    pub peers: Mutex<HashMap<u16, PeerLink>>,
    pub counters: Counters,
    pub deny: Arc<Mutex<HashSet<u16>>>,
    /// ms of artificial stall per inbound frame ("slow peer" cell).
    pub slow_ms: AtomicU64,
    /// room → peers that advertised interest in it.
    pub interest: Mutex<HashMap<u32, HashSet<u16>>>,
    /// Live echo-bench sampling (one bench at a time is fine for the spike).
    pub bench: Mutex<BenchState>,
    swim: Arc<Swim>,
}

impl Relay {
    pub async fn start(
        self_id: u16,
        bind: SocketAddr,
        swim: Arc<Swim>,
        deny: Arc<Mutex<HashSet<u16>>>,
    ) -> Arc<Self> {
        let relay = Arc::new(Self {
            self_id,
            peers: Mutex::new(HashMap::new()),
            counters: Counters::default(),
            deny,
            slow_ms: AtomicU64::new(0),
            interest: Mutex::new(HashMap::new()),
            bench: Mutex::new(BenchState {
                hop: Reservoir::new(100_000),
                rtt: Reservoir::new(100_000),
                acked: 0,
                sent: 0,
                done: true,
            }),
            swim,
        });
        let listener = TcpListener::bind(bind).await.expect("relay bind");
        tokio::spawn(relay.clone().accept_loop(listener));
        tokio::spawn(relay.clone().dial_loop());
        relay
    }

    // ── frame builders ──

    pub fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut f = Vec::with_capacity(5 + body.len());
        f.extend_from_slice(&(1 + body.len() as u32).to_le_bytes());
        f.push(kind);
        f.extend_from_slice(body);
        f
    }

    pub fn echo_req(seq: u64, payload_len: usize) -> Vec<u8> {
        let mut body = Vec::with_capacity(16 + payload_len);
        body.extend_from_slice(&seq.to_le_bytes());
        body.extend_from_slice(&mono_ns().to_le_bytes());
        body.resize(16 + payload_len, 0x42);
        Self::frame(2, &body)
    }

    pub fn room_frame(room: u32, payload_len: usize) -> Vec<u8> {
        let mut body = Vec::with_capacity(4 + payload_len);
        body.extend_from_slice(&room.to_le_bytes());
        body.resize(4 + payload_len, 0x42);
        Self::frame(4, &body)
    }

    /// Bounded push (§4.6): over the byte HWM → drop-and-count, NEVER block.
    pub fn push(&self, peer: u16, frame: Vec<u8>) -> bool {
        let peers = self.peers.lock().unwrap();
        let Some(link) = peers.get(&peer) else {
            self.counters.drops.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        let queued = link.queued_bytes.load(Ordering::Relaxed);
        if queued + frame.len() > LINK_HWM_BYTES {
            self.counters.drops.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        link.queued_bytes.fetch_add(frame.len(), Ordering::Relaxed);
        self.counters
            .bytes_out
            .fetch_add(frame.len() as u64, Ordering::Relaxed);
        self.counters.frames_out.fetch_add(1, Ordering::Relaxed);
        link.tx.send(frame).is_ok()
    }

    pub fn peers_up(&self) -> Vec<u16> {
        self.peers.lock().unwrap().keys().copied().collect()
    }

    // ── link lifecycle ──

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let relay = self.clone();
            tokio::spawn(async move {
                relay.run_link(stream, None).await;
            });
        }
    }

    /// Dial rule (§4.1, one link per pair): the HIGHER id dials the lower.
    async fn dial_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let targets: Vec<(u16, SocketAddr)> = {
                let m = self.swim.membership.lock().unwrap();
                m.members
                    .iter()
                    .filter(|(id, mem)| **id < self.self_id && mem.state != MState::Dead)
                    .map(|(id, mem)| {
                        // relay port = swim port + 1 (lib.rs layout)
                        let mut a = mem.addr;
                        a.set_port(a.port() + 1);
                        (*id, a)
                    })
                    .collect()
            };
            for (id, addr) in targets {
                let already = self.peers.lock().unwrap().contains_key(&id);
                let denied = self.deny.lock().unwrap().contains(&id);
                if already || denied {
                    continue;
                }
                if let Ok(stream) = TcpStream::connect(addr).await {
                    let relay = self.clone();
                    tokio::spawn(async move {
                        relay.run_link(stream, Some(id)).await;
                    });
                }
            }
        }
    }

    /// One live link: HELLO exchange, bounded writer, framed reader.
    async fn run_link(self: Arc<Self>, stream: TcpStream, dialed: Option<u16>) {
        stream.set_nodelay(true).ok();
        let (rd, mut wr) = stream.into_split();
        // Buffered reads: 2 read syscalls per frame is the other half of the
        // first measurement's latency tail.
        let mut rd = tokio::io::BufReader::with_capacity(256 * 1024, rd);

        // HELLO both ways; learn the peer id when accepting.
        let hello = Self::frame(1, &self.self_id.to_le_bytes());
        if wr.write_all(&hello).await.is_err() {
            return;
        }
        let Ok(peer_id) = read_hello(&mut rd).await else {
            return;
        };
        if let Some(expect) = dialed {
            if peer_id != expect {
                return;
            }
        }
        if self.deny.lock().unwrap().contains(&peer_id) {
            return;
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        // NOTE on boundedness: the channel type is unbounded but every push
        // goes through `push()` which enforces the byte HWM — the byte cap
        // IS the bound (frame-count caps lie when frames vary 64 B..64 KB).
        {
            let mut peers = self.peers.lock().unwrap();
            if peers.contains_key(&peer_id) {
                return; // one link per pair; a racing dial loses
            }
            peers.insert(
                peer_id,
                PeerLink {
                    tx,
                    queued_bytes: queued_bytes.clone(),
                },
            );
        }

        // Writer: drains the bounded queue onto the socket, COALESCING all
        // currently-queued frames into one write (the RFC 0001 bridge lesson,
        // §10 decision mapping: per-frame syscalls are what put the first
        // measurement at 3.8 ms p99; one write per wakeup put it under 1 ms).
        let qb = queued_bytes.clone();
        let writer = tokio::spawn(async move {
            let mut out = Vec::with_capacity(256 * 1024);
            while let Some(frame) = rx.recv().await {
                qb.fetch_sub(frame.len(), Ordering::Relaxed);
                out.clear();
                out.extend_from_slice(&frame);
                while out.len() < 128 * 1024 {
                    match rx.try_recv() {
                        Ok(f) => {
                            qb.fetch_sub(f.len(), Ordering::Relaxed);
                            out.extend_from_slice(&f);
                        }
                        Err(_) => break,
                    }
                }
                if wr.write_all(&out).await.is_err() {
                    return;
                }
            }
        });

        // Reader: framed dispatch until EOF/error or deny.
        let mut buf = Vec::new();
        loop {
            let mut len4 = [0u8; 4];
            if rd.read_exact(&mut len4).await.is_err() {
                break;
            }
            let len = u32::from_le_bytes(len4) as usize;
            if len == 0 || len > 32 << 20 {
                break; // protocol error → close (§4.4)
            }
            buf.resize(len, 0);
            if rd.read_exact(&mut buf).await.is_err() {
                break;
            }
            if self.deny.lock().unwrap().contains(&peer_id) {
                break; // partition injection severs the link
            }
            let slow = self.slow_ms.load(Ordering::Relaxed);
            if slow > 0 {
                tokio::time::sleep(Duration::from_millis(slow)).await;
            }
            self.counters
                .bytes_in
                .fetch_add((4 + len) as u64, Ordering::Relaxed);
            self.counters.frames_in.fetch_add(1, Ordering::Relaxed);
            self.dispatch(peer_id, &buf);
        }

        self.peers.lock().unwrap().remove(&peer_id);
        writer.abort();
    }

    fn dispatch(&self, peer_id: u16, frame: &[u8]) {
        match frame[0] {
            2 => {
                // ECHO_REQ → stamp receive time, ack back (hop = recv - send).
                let recv_ns = mono_ns();
                let seq = &frame[1..9];
                let send_ns = &frame[9..17];
                let mut body = Vec::with_capacity(24);
                body.extend_from_slice(seq);
                body.extend_from_slice(send_ns);
                body.extend_from_slice(&recv_ns.to_le_bytes());
                self.push(peer_id, Self::frame(3, &body));
            }
            3 => {
                // ECHO_ACK → record one-way hop + rtt.
                let send_ns = u64::from_le_bytes(frame[9..17].try_into().unwrap());
                let recv_ns = u64::from_le_bytes(frame[17..25].try_into().unwrap());
                let now = mono_ns();
                let mut b = self.bench.lock().unwrap();
                b.hop.push(recv_ns.saturating_sub(send_ns));
                b.rtt.push(now.saturating_sub(send_ns));
                b.acked += 1;
            }
            4 => {
                // ROOM traffic: counted (bytes_in above), then discarded —
                // the routing cell measures wire bytes, not app delivery.
            }
            5 => {
                let count = u32::from_le_bytes(frame[1..5].try_into().unwrap()) as usize;
                let mut rooms = HashSet::new();
                for i in 0..count {
                    let off = 5 + i * 4;
                    if off + 4 <= frame.len() {
                        rooms.insert(u32::from_le_bytes(frame[off..off + 4].try_into().unwrap()));
                    }
                }
                let mut interest = self.interest.lock().unwrap();
                for r in rooms {
                    interest.entry(r).or_default().insert(peer_id);
                }
            }
            _ => {} // unknown kind: skip (the §4.4 rule, even in the spike)
        }
    }
}

async fn read_hello<R: tokio::io::AsyncRead + Unpin>(rd: &mut R) -> Result<u16, ()> {
    let mut len4 = [0u8; 4];
    rd.read_exact(&mut len4).await.map_err(|_| ())?;
    let len = u32::from_le_bytes(len4) as usize;
    if len != 3 {
        return Err(());
    }
    let mut body = [0u8; 3];
    rd.read_exact(&mut body).await.map_err(|_| ())?;
    if body[0] != 1 {
        return Err(());
    }
    Ok(u16::from_le_bytes([body[1], body[2]]))
}
