//! Rust → JS event bridge. THE component RFC 0001 exists to validate.
//!
//! Graduation rules (do not violate):
//! - The design and its constants (batch size, flush timer) come from
//!   docs/rfcs/0001-results.md. Cite the result in a comment next to each
//!   constant. Constants without citations get the PR rejected.
//! - The engine↔bridge queue is bounded; overflow increments bridge_pressure
//!   and applies the documented policy. Never silent.
//!
//! ---
//!
//! Phase 0 winner: **design C — batched flat encoding** (RFC 0001, decided in
//! `docs/rfcs/0001-results.md`). One `ThreadsafeFunction` call per *flush*;
//! events are packed into one contiguous buffer and decoded by a JS cursor
//! reader that hands each message to the app as a zero-copy subarray view. This
//! module graduates the winner's **wire format + constants** (tested here); the
//! napi/`ThreadsafeFunction` wiring and the engine↔bridge channel are switched
//! on in **Phase 1A** (see the "Phase 1A wiring" note below), because they need
//! the engine's `EngineEvent` stream, which does not exist until then.

use std::time::Duration;

use beamsocket_core::events::EngineEvent;
use beamsocket_core::ids::ConnectionId;

/// Flush a batch when it reaches this many events. Validated in
/// `docs/rfcs/0001-results.md` §"Batch-parameter sweep": N=256 sits within ~10 %
/// of the best-throughput cell for design C while keeping a good latency tail;
/// it confirms the ARCHITECTURE.md §2.2 default. Latency-sensitive deployments
/// may lower this to 64 (≈10 % less peak throughput, markedly lower p99).
pub const BRIDGE_BATCH: usize = 256;

/// Flush a partially-filled batch after this long (measured from its first
/// event). Validated alongside `BRIDGE_BATCH` in 0001-results.md; 1 ms bounds
/// the added latency at low load without fragmenting batches at high load.
pub const BRIDGE_FLUSH_INTERVAL: Duration = Duration::from_millis(1);

/// Capacity of the bounded engine↔bridge queue (Rule 5). Overflow is
/// drop-newest and counted in `bridge_pressure` — never silent. The value is a
/// starting point graduated from the spike's harness (`queue_capacity = 8192`,
/// the depth at which pressure/latency behaved well); re-confirm on the pinned
/// reference box per 0001-results.md "Follow-ups".
pub const ENGINE_BRIDGE_QUEUE_CAPACITY: usize = 8192;

/// Design C flat wire format — the winner from RFC 0001. One contiguous buffer
/// per flush, little-endian, decoded by a JS cursor reader. Layout:
///
/// ```text
/// [u32 count]
/// repeated count times:
///   [u64 conn_id][u8 kind][u32 payload_len][payload bytes]
/// ```
///
/// `kind`: 0 = text message, 1 = binary message, 2 = connection opened
/// (empty payload), 3 = connection closed (payload = [u16 close_code][reason
/// utf-8]). This is the Phase 1A production form of the spike's proven
/// encoder (`spike/bridge-core/src/lib.rs::flat`): the spike carried a
/// latency timestamp per event and only messages; production replaces the
/// timestamp slot with `conn_id` and folds the rare open/close control events
/// into the same batched stream via the kind byte — same size, same layout,
/// no second channel. The JS side creates one `Buffer` over the whole flush
/// and exposes each `payload` as a zero-copy `subarray` — no per-message
/// allocation (RFC 0001 §"Copy vs external buffers").
pub mod flat {
    use super::*;

    const HEADER: usize = 4;
    const PER_EVENT_OVERHEAD: usize = 8 + 1 + 4; // conn_id + kind + len

    pub const KIND_TEXT: u8 = 0;
    pub const KIND_BINARY: u8 = 1;
    pub const KIND_OPEN: u8 = 2;
    pub const KIND_CLOSE: u8 = 3;

    fn event_payload_len(ev: &EngineEvent) -> usize {
        match ev {
            EngineEvent::Message { payload, .. } => payload.len(),
            EngineEvent::ConnectionOpened { .. } => 0,
            EngineEvent::ConnectionClosed { reason, .. } => 2 + reason.len(),
        }
    }

    /// Exact encoded size of a batch, so the flush buffer allocates exactly
    /// once per flush.
    pub fn encoded_len(batch: &[EngineEvent]) -> usize {
        HEADER
            + batch
                .iter()
                .map(|e| PER_EVENT_OVERHEAD + event_payload_len(e))
                .sum::<usize>()
    }

    /// Encode a batch of engine events into one contiguous flush buffer.
    pub fn encode_batch(batch: &[EngineEvent]) -> Vec<u8> {
        let mut out = Vec::with_capacity(encoded_len(batch));
        out.extend_from_slice(&(batch.len() as u32).to_le_bytes());
        for ev in batch {
            match ev {
                EngineEvent::Message {
                    id,
                    payload,
                    is_binary,
                } => {
                    out.extend_from_slice(&id.0.to_le_bytes());
                    out.push(if *is_binary { KIND_BINARY } else { KIND_TEXT });
                    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                    out.extend_from_slice(payload);
                }
                EngineEvent::ConnectionOpened { id } => {
                    out.extend_from_slice(&id.0.to_le_bytes());
                    out.push(KIND_OPEN);
                    out.extend_from_slice(&0u32.to_le_bytes());
                }
                EngineEvent::ConnectionClosed { id, code, reason } => {
                    out.extend_from_slice(&id.0.to_le_bytes());
                    out.push(KIND_CLOSE);
                    out.extend_from_slice(&((2 + reason.len()) as u32).to_le_bytes());
                    out.extend_from_slice(&code.to_le_bytes());
                    out.extend_from_slice(reason.as_bytes());
                }
            }
        }
        out
    }

    /// One decoded event (payload borrowed from the flush buffer — the JS
    /// side does the equivalent with a `subarray`, zero-copy).
    #[derive(Debug, PartialEq, Eq)]
    pub enum DecodedEvent<'a> {
        Message {
            conn_id: ConnectionId,
            is_binary: bool,
            payload: &'a [u8],
        },
        Opened {
            conn_id: ConnectionId,
        },
        Closed {
            conn_id: ConnectionId,
            code: u16,
            reason: &'a [u8],
        },
    }

    /// Mirror of the JS cursor reader (packages/beamsocket/src/events.ts) —
    /// kept in Rust so the round-trip is covered by `cargo test`.
    pub fn decode(buf: &[u8]) -> Vec<DecodedEvent<'_>> {
        let mut out = Vec::new();
        if buf.len() < HEADER {
            return out;
        }
        let count = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut off = HEADER;
        for _ in 0..count {
            let conn_id = ConnectionId(u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
            off += 8;
            let kind = buf[off];
            off += 1;
            let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let payload = &buf[off..off + len];
            off += len;
            out.push(match kind {
                KIND_TEXT | KIND_BINARY => DecodedEvent::Message {
                    conn_id,
                    is_binary: kind == KIND_BINARY,
                    payload,
                },
                KIND_OPEN => DecodedEvent::Opened { conn_id },
                KIND_CLOSE => DecodedEvent::Closed {
                    conn_id,
                    code: u16::from_le_bytes(payload[0..2].try_into().unwrap()),
                    reason: &payload[2..],
                },
                other => panic!("corrupt flush buffer: unknown kind {other}"),
            });
        }
        out
    }
}

/// The drain loop (Phase 1A wiring of the spike's design-C reference loop,
/// spike/bridge-node/src/lib.rs): accumulate up to `BRIDGE_BATCH` events or
/// `BRIDGE_FLUSH_INTERVAL` from the first event of a batch, whichever first,
/// then hand ONE encoded flush buffer to `deliver`.
///
/// `deliver` is the TSFN call in production (crates/node/src/binding.rs,
/// Blocking mode + small bounded TSFN queue so back-pressure reaches the
/// bounded engine→bridge mpsc and RSS stays flat — RFC 0001 survival gate);
/// tests pass a closure, which keeps this loop `cargo test`-able without
/// linking Node.
///
/// Runs on its own current-thread Tokio runtime (NOT the engine's — engine
/// shutdown must never strand the drain mid-`block_on`; the channel closing
/// is the drain's exit signal).
pub async fn drain_loop<F: FnMut(Vec<u8>)>(
    mut rx: tokio::sync::mpsc::Receiver<EngineEvent>,
    mut deliver: F,
) {
    let mut batch: Vec<EngineEvent> = Vec::with_capacity(BRIDGE_BATCH);
    'outer: loop {
        // Wait for the first event of a batch (no timer running while idle).
        match rx.recv().await {
            Some(ev) => batch.push(ev),
            None => break,
        }
        // Fill until batch size or the flush timer fires (measured from the
        // first event — the oldest event's wait bounds the added latency).
        let deadline = tokio::time::sleep(BRIDGE_FLUSH_INTERVAL);
        tokio::pin!(deadline);
        while batch.len() < BRIDGE_BATCH {
            tokio::select! {
                r = rx.recv() => match r {
                    Some(ev) => batch.push(ev),
                    None => {
                        flush(&mut batch, &mut deliver);
                        break 'outer;
                    }
                },
                _ = &mut deadline => break,
            }
        }
        flush(&mut batch, &mut deliver);
    }
    flush(&mut batch, &mut deliver);
}

fn flush<F: FnMut(Vec<u8>)>(batch: &mut Vec<EngineEvent>, deliver: &mut F) {
    if batch.is_empty() {
        return;
    }
    deliver(flat::encode_batch(batch));
    batch.clear();
}

#[cfg(test)]
mod tests {
    use super::flat::*;
    use super::*;
    use beamsocket_core::ids::ConnectionId;

    #[test]
    fn mixed_batch_round_trips() {
        let batch = vec![
            EngineEvent::ConnectionOpened {
                id: ConnectionId(u64::MAX),
            },
            EngineEvent::Message {
                id: ConnectionId(1),
                payload: b"hello".to_vec(),
                is_binary: false,
            },
            EngineEvent::Message {
                id: ConnectionId(9_999_999_999),
                payload: vec![0u8; 4096],
                is_binary: true,
            },
            EngineEvent::Message {
                id: ConnectionId(0),
                payload: Vec::new(),
                is_binary: false,
            },
            EngineEvent::ConnectionClosed {
                id: ConnectionId(42),
                code: 4001,
                reason: "server says bye".into(),
            },
            EngineEvent::ConnectionClosed {
                id: ConnectionId(43),
                code: 1000,
                reason: String::new(),
            },
        ];
        let buf = encode_batch(&batch);
        assert_eq!(
            buf.len(),
            encoded_len(&batch),
            "flush buffer must allocate exactly once"
        );

        let decoded = decode(&buf);
        assert_eq!(decoded.len(), batch.len());
        assert_eq!(
            decoded[0],
            DecodedEvent::Opened {
                conn_id: ConnectionId(u64::MAX)
            }
        );
        assert_eq!(
            decoded[1],
            DecodedEvent::Message {
                conn_id: ConnectionId(1),
                is_binary: false,
                payload: b"hello"
            }
        );
        assert_eq!(
            decoded[2],
            DecodedEvent::Message {
                conn_id: ConnectionId(9_999_999_999),
                is_binary: true,
                payload: &[0u8; 4096]
            }
        );
        assert_eq!(
            decoded[3],
            DecodedEvent::Message {
                conn_id: ConnectionId(0),
                is_binary: false,
                payload: &[]
            }
        );
        assert_eq!(
            decoded[4],
            DecodedEvent::Closed {
                conn_id: ConnectionId(42),
                code: 4001,
                reason: b"server says bye"
            }
        );
        assert_eq!(
            decoded[5],
            DecodedEvent::Closed {
                conn_id: ConnectionId(43),
                code: 1000,
                reason: b""
            }
        );
    }

    #[test]
    fn empty_batch_is_just_a_zero_count() {
        let buf = encode_batch(&[]);
        assert_eq!(buf, vec![0, 0, 0, 0]);
        assert!(decode(&buf).is_empty());
    }

    #[tokio::test]
    async fn drain_loop_batches_by_size_and_flushes_rest_on_close() {
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        for i in 0..(BRIDGE_BATCH + 3) as u64 {
            tx.send(EngineEvent::Message {
                id: ConnectionId(i),
                payload: b"x".to_vec(),
                is_binary: false,
            })
            .await
            .unwrap();
        }
        drop(tx);
        let mut flushes: Vec<usize> = Vec::new();
        drain_loop(rx, |buf| flushes.push(decode(&buf).len())).await;
        assert_eq!(flushes, vec![BRIDGE_BATCH, 3]);
    }
}
