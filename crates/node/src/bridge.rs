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
///   [u64 conn_id][u8 is_binary][u32 payload_len][payload bytes]
/// ```
///
/// This is the production form of the spike's proven encoder
/// (`spike/bridge-core/src/lib.rs::flat`): the spike carried a latency
/// timestamp per event for measurement; production carries `conn_id` +
/// `is_binary` instead. The JS side creates one `Buffer` over the whole flush
/// and exposes each `payload` as a zero-copy `subarray` — no per-message
/// allocation (RFC 0001 §"Copy vs external buffers").
pub mod flat {
    use super::*;

    const HEADER: usize = 4;
    const PER_EVENT_OVERHEAD: usize = 8 + 1 + 4; // conn_id + is_binary + len

    /// Exact encoded size for a batch of `(payload_len)`s, so the flush buffer
    /// allocates exactly once per flush.
    pub fn encoded_len(payload_lens: impl Iterator<Item = usize>) -> usize {
        HEADER + payload_lens.map(|l| PER_EVENT_OVERHEAD + l).sum::<usize>()
    }

    /// Encode a batch of `Message` events into one contiguous buffer. Non-message
    /// events (open/close) travel on the same batched path but are encoded by
    /// their own control frames (Phase 1A); this hot-path encoder covers the
    /// `message` fan-in that RFC 0001 measured.
    pub fn encode_messages(batch: &[(ConnectionId, bool, &[u8])]) -> Vec<u8> {
        let cap = encoded_len(batch.iter().map(|(_, _, p)| p.len()));
        let mut out = Vec::with_capacity(cap);
        out.extend_from_slice(&(batch.len() as u32).to_le_bytes());
        for (conn, is_binary, payload) in batch {
            out.extend_from_slice(&conn.0.to_le_bytes());
            out.push(*is_binary as u8);
            out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            out.extend_from_slice(payload);
        }
        out
    }

    /// One decoded message (payload borrowed from the flush buffer — the JS side
    /// does the equivalent with a `subarray`, zero-copy).
    #[derive(Debug, PartialEq, Eq)]
    pub struct DecodedMessage<'a> {
        pub conn_id: ConnectionId,
        pub is_binary: bool,
        pub payload: &'a [u8],
    }

    /// Mirror of the JS cursor reader — kept in Rust so the round-trip is
    /// covered by `cargo test`.
    pub fn decode(buf: &[u8]) -> Vec<DecodedMessage<'_>> {
        let mut out = Vec::new();
        if buf.len() < HEADER {
            return out;
        }
        let count = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut off = HEADER;
        for _ in 0..count {
            let conn_id = ConnectionId(u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
            off += 8;
            let is_binary = buf[off] != 0;
            off += 1;
            let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let payload = &buf[off..off + len];
            off += len;
            out.push(DecodedMessage {
                conn_id,
                is_binary,
                payload,
            });
        }
        out
    }

    /// Extract the `(conn_id, is_binary, payload)` tuple from an `EngineEvent`
    /// if it is a `Message` (the batched hot path). Open/close are handled
    /// separately in Phase 1A.
    pub fn message_parts(ev: &EngineEvent) -> Option<(ConnectionId, bool, &[u8])> {
        match ev {
            EngineEvent::Message {
                id,
                payload,
                is_binary,
            } => Some((*id, *is_binary, payload.as_slice())),
            _ => None,
        }
    }
}

// ── Phase 1A wiring (not yet active — needs the engine's EngineEvent stream) ──
//
// When Phase 1A starts (ENGINEERING.md §5):
//   1. In crates/node/Cargo.toml: switch `crate-type = ["cdylib"]`, enable the
//      napi/napi-derive deps + napi-build (they are staged, commented, there).
//   2. Bridge a BOUNDED mpsc of ENGINE_BRIDGE_QUEUE_CAPACITY between the engine
//      and this module (drop-newest + bridge_pressure counter on overflow).
//   3. Drain it on a non-JS thread: accumulate until BRIDGE_BATCH or
//      BRIDGE_FLUSH_INTERVAL, then `flat::encode_messages(...)` and deliver the
//      buffer via ONE ThreadsafeFunction call (Blocking mode, small bounded
//      queue, so back-pressure reaches the bounded mpsc and RSS stays flat).
//   4. Buffer strategy from `buffers::should_externalize` (16 KB crossover);
//      flush buffers are large, so external/zero-copy in practice.
//   5. Export `bridge_pressure` to metrics.rs (Phase 1D).
// The spike's reference implementation of this exact loop is in
// spike/bridge-node/src/lib.rs (design C path).

#[cfg(test)]
mod tests {
    use super::flat::*;
    use beamsocket_core::ids::ConnectionId;

    #[test]
    fn message_batch_round_trips() {
        let a = b"hello".to_vec();
        let b = vec![0u8; 4096];
        let c = Vec::new();
        let batch: Vec<(ConnectionId, bool, &[u8])> = vec![
            (ConnectionId(1), false, a.as_slice()),
            (ConnectionId(9_999_999_999), true, b.as_slice()),
            (ConnectionId(0), false, c.as_slice()),
        ];
        let cap = encoded_len(batch.iter().map(|(_, _, p)| p.len()));
        let buf = encode_messages(&batch);
        assert_eq!(buf.len(), cap, "flush buffer must allocate exactly once");

        let decoded = decode(&buf);
        assert_eq!(decoded.len(), 3);
        for (d, (conn, bin, pay)) in decoded.iter().zip(batch.iter()) {
            assert_eq!(d.conn_id, *conn);
            assert_eq!(d.is_binary, *bin);
            assert_eq!(d.payload, *pay);
        }
    }

    #[test]
    fn empty_batch_is_just_a_zero_count() {
        let buf = encode_messages(&[]);
        assert_eq!(buf, vec![0, 0, 0, 0]);
        assert!(decode(&buf).is_empty());
    }
}
