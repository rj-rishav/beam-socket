//! Wire framing (RFC 0004 §4.4).
//!
//! Every frame is `[len: u32 LE][kind: u8][flags: u8][body]`, where `len`
//! covers **kind + flags + body** (so the minimum `len` is 2, an empty-body
//! control frame). The maximum `len` is a handshake-declared constant; a reader
//! that sees `len > negotiated max` **closes the link** — a corrupted or
//! oversize stream is a protocol error, and there are deliberately **no resync
//! heuristics** (§4.4: never corrupt, refuse loudly).
//!
//! The frame catalog is the §4.4 table. This crate *defines* every kind so
//! negotiation can reason about it, but Phase 3A only *drives* the control
//! kinds — MEMBERSHIP/INTEREST*/RELAY_* are carried and counted, never
//! interpreted here (3B–3D own their semantics).

/// Header bytes before the body: `len` (4) + `kind` (1) + `flags` (1).
pub const HEADER_LEN: usize = 6;

/// The smallest a negotiated max-frame may be. `len` counts kind+flags+body, so
/// a floor of 64 bytes leaves room for the largest control frame (AUTH is 32 B
/// of MAC + 2 B header = 34 B; CHALLENGE is 34 B) with margin. A peer declaring
/// a max below this is misconfigured and is refused at the handshake, not left
/// to wedge on its first control frame.
pub const MIN_FRAME_FLOOR: u32 = 64;

/// The largest a negotiated max-frame may be (defensive ceiling, 64 MiB). The
/// default is 16 MiB (§4.4, "must exceed maxPayloadBytes + envelope"); this
/// ceiling only stops a malicious HELLO from declaring a 4 GiB frame and
/// turning the length prefix into an allocation oracle.
pub const MAX_FRAME_CEILING: u32 = 64 << 20;

/// Frame kinds (§4.4 catalog). Stored on the wire as one `u8`.
///
/// `Membership`, `Interest*`, and `Relay*` are defined here so negotiation and
/// the unknown-kind counter can classify them; their *handlers* land in 3B–3D.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FrameKind {
    /// magic `BSMH`, version, node id, cluster name, incarnation, max frame,
    /// feature bits — the negotiation frame ([`crate::hello::Hello`]).
    Hello = 0x01,
    /// 32-byte challenge nonce (§4.7).
    Challenge = 0x02,
    /// `HMAC-SHA256(secret, role ‖ nonce ‖ transcript)` (§4.7).
    Auth = 0x03,
    /// Piggybacked SWIM updates — TCP links only (3B).
    Membership = 0x04,
    /// Edge-triggered interest add/remove (3C, feature-gated).
    Interest = 0x05,
    /// Interest anti-entropy hash (3C, feature-gated).
    InterestDigest = 0x06,
    /// Room-targeted relayed payload (3D).
    RelayRoom = 0x07,
    /// User-targeted relayed payload (3D).
    RelayUser = 0x08,
    /// Broadcast relayed payload (3D).
    RelayAll = 0x09,
    /// ConnectionId-targeted relayed payload (3D, feature-gated example).
    RelaySocket = 0x0A,
    /// Idle-link liveness (§4.4). PING vs PONG is the [`Flags::PONG`] bit — one
    /// catalog kind, faithful to the table.
    Ping = 0x0B,
}

impl FrameKind {
    /// Classify a wire byte. `None` = a kind not in the catalog → the receiver
    /// counts it (`unknownFrames`) and skips it (§4.4 defense-in-depth). Under
    /// sender suppression this never fires; a nonzero count is a bug detector,
    /// not a compatibility mechanism.
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0x01 => Self::Hello,
            0x02 => Self::Challenge,
            0x03 => Self::Auth,
            0x04 => Self::Membership,
            0x05 => Self::Interest,
            0x06 => Self::InterestDigest,
            0x07 => Self::RelayRoom,
            0x08 => Self::RelayUser,
            0x09 => Self::RelayAll,
            0x0A => Self::RelaySocket,
            0x0B => Self::Ping,
            _ => return None,
        })
    }

    #[inline]
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// The per-frame `flags` byte. Bit meanings are **per kind** (§4.4 leaves it a
/// reserved byte); we assign only bit 0 so far, so an unknown flag bit is
/// simply ignored (append-only, like the body).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Flags(pub u8);

impl Flags {
    pub const NONE: Flags = Flags(0);
    /// On a `Relay*` frame: the payload is binary (vs UTF-8 text).
    pub const BINARY: u8 = 0x01;
    /// On a `Ping` frame: this is the PONG reply, not the probe.
    pub const PONG: u8 = 0x01;

    #[inline]
    pub fn has(self, bit: u8) -> bool {
        self.0 & bit != 0
    }
}

/// A decoded frame. The body is owned bytes; the coalesced writer never sees
/// this type (it works on already-encoded buffers) — `Frame` is the codec's
/// and the handshake's currency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub flags: Flags,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn new(kind: FrameKind, body: Vec<u8>) -> Self {
        Self {
            kind,
            flags: Flags::NONE,
            body,
        }
    }

    pub fn with_flags(kind: FrameKind, flags: Flags, body: Vec<u8>) -> Self {
        Self { kind, flags, body }
    }

    /// Bytes on the wire (`len` counts kind+flags+body).
    #[inline]
    pub fn wire_len(&self) -> usize {
        HEADER_LEN + self.body.len()
    }

    /// Encode to `[len][kind][flags][body]`. `len` = `2 + body.len()`.
    pub fn encode(&self) -> Vec<u8> {
        let len = (2 + self.body.len()) as u32;
        let mut out = Vec::with_capacity(self.wire_len());
        out.extend_from_slice(&len.to_le_bytes());
        out.push(self.kind.as_u8());
        out.push(self.flags.0);
        out.extend_from_slice(&self.body);
        out
    }

    /// Append the encoding to an existing buffer (the writer's coalescing path —
    /// one allocation per flush, not per frame).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let len = (2 + self.body.len()) as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.push(self.kind.as_u8());
        out.push(self.flags.0);
        out.extend_from_slice(&self.body);
    }

    /// Parse one whole frame from a complete `[len][kind][flags][body]` buffer.
    /// Used by golden-bytes tests and any caller that already has the bytes; the
    /// async link uses [`decode_len`] + a body read instead.
    ///
    /// `Oversize` when `len > max_frame` — the link's contract is to close on
    /// this, never to hunt for the next boundary.
    pub fn parse(buf: &[u8], max_frame: u32) -> Result<Frame, FrameError> {
        if buf.len() < 4 {
            return Err(FrameError::Truncated);
        }
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        decode_len(len, max_frame)?;
        let total = 4 + len as usize;
        if buf.len() < total {
            return Err(FrameError::Truncated);
        }
        let kind = FrameKind::from_u8(buf[4]).ok_or(FrameError::UnknownKind(buf[4]))?;
        let flags = Flags(buf[5]);
        let body = buf[HEADER_LEN..total].to_vec();
        Ok(Frame { kind, flags, body })
    }
}

/// Validate a length prefix against the negotiated maximum. Split out so the
/// async reader validates **before** it allocates a body buffer — a hostile
/// `len` never turns into a large allocation.
///
/// `len == 0 || len == 1` is a malformed frame (a frame must carry at least
/// kind+flags); `len > max_frame` is the oversize protocol error.
#[inline]
pub fn decode_len(len: u32, max_frame: u32) -> Result<usize, FrameError> {
    if len < 2 {
        return Err(FrameError::Malformed);
    }
    if len > max_frame {
        return Err(FrameError::Oversize {
            len,
            max: max_frame,
        });
    }
    Ok(len as usize)
}

/// Codec-level errors. Every variant means **close the link** — none is
/// recoverable mid-stream (§4.4: no resync on a corrupted stream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Buffer does not yet hold a full frame (sync `parse` only; the async
    /// reader turns a short read into an EOF close).
    Truncated,
    /// `len < 2` — a frame with no room for kind+flags.
    Malformed,
    /// `len > negotiated max` — the oversize protocol error. Carries both
    /// numbers so the close is logged with the offending size and the cap.
    Oversize { len: u32, max: u32 },
    /// A `kind` byte outside the catalog. In `parse` this is an error; in the
    /// live reader it is instead *counted* (`unknownFrames`) and skipped —
    /// self-delimiting frames make skipping safe (§4.4).
    UnknownKind(u8),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Truncated => write!(f, "frame truncated"),
            FrameError::Malformed => write!(f, "frame len < 2 (no room for kind+flags)"),
            FrameError::Oversize { len, max } => {
                write!(f, "oversize frame: len {len} > negotiated max {max}")
            }
            FrameError::UnknownKind(k) => write!(f, "unknown frame kind 0x{k:02X}"),
        }
    }
}

impl std::error::Error for FrameError {}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- golden bytes: the encoding is a wire contract ----------

    #[test]
    fn golden_empty_control_frame() {
        // An empty-body HELLO-kind frame: len = 2 (kind+flags), LE.
        let f = Frame::new(FrameKind::Hello, vec![]);
        assert_eq!(f.encode(), vec![0x02, 0x00, 0x00, 0x00, 0x01, 0x00]);
    }

    #[test]
    fn golden_frame_with_body_and_flags() {
        // kind=RelayRoom(0x07), flags=BINARY(0x01), body=[0xAA,0xBB,0xCC].
        // len = 2 + 3 = 5.
        let f = Frame::with_flags(
            FrameKind::RelayRoom,
            Flags(Flags::BINARY),
            vec![0xAA, 0xBB, 0xCC],
        );
        assert_eq!(
            f.encode(),
            vec![0x05, 0x00, 0x00, 0x00, 0x07, 0x01, 0xAA, 0xBB, 0xCC]
        );
    }

    #[test]
    fn golden_ping_vs_pong_is_the_flag_bit() {
        let ping = Frame::new(FrameKind::Ping, vec![]);
        let pong = Frame::with_flags(FrameKind::Ping, Flags(Flags::PONG), vec![]);
        assert_eq!(ping.encode(), vec![0x02, 0x00, 0x00, 0x00, 0x0B, 0x00]);
        assert_eq!(pong.encode(), vec![0x02, 0x00, 0x00, 0x00, 0x0B, 0x01]);
    }

    #[test]
    fn encode_into_matches_encode() {
        let f = Frame::with_flags(FrameKind::Auth, Flags::NONE, vec![9; 32]);
        let mut buf = Vec::new();
        // Coalesce two frames into one buffer (the writer's path).
        f.encode_into(&mut buf);
        f.encode_into(&mut buf);
        let mut expect = f.encode();
        expect.extend(f.encode());
        assert_eq!(buf, expect);
    }

    // ---------- round-trip ----------

    #[test]
    fn round_trip_every_kind() {
        for k in [
            FrameKind::Hello,
            FrameKind::Challenge,
            FrameKind::Auth,
            FrameKind::Membership,
            FrameKind::Interest,
            FrameKind::InterestDigest,
            FrameKind::RelayRoom,
            FrameKind::RelayUser,
            FrameKind::RelayAll,
            FrameKind::RelaySocket,
            FrameKind::Ping,
        ] {
            let f = Frame::new(k, vec![1, 2, 3, 4]);
            let bytes = f.encode();
            let back = Frame::parse(&bytes, MAX_FRAME_CEILING).unwrap();
            assert_eq!(back, f, "round-trip failed for {k:?}");
            assert_eq!(FrameKind::from_u8(k.as_u8()), Some(k));
        }
    }

    // ---------- oversize / malformed: close, never resync ----------

    #[test]
    fn oversize_len_is_rejected_before_body() {
        // decode_len must reject on the prefix alone — the reader never
        // allocates the (huge) body.
        let err = decode_len(1024, 512).unwrap_err();
        assert_eq!(
            err,
            FrameError::Oversize {
                len: 1024,
                max: 512
            }
        );
    }

    #[test]
    fn zero_and_one_length_are_malformed() {
        assert_eq!(decode_len(0, 4096), Err(FrameError::Malformed));
        assert_eq!(decode_len(1, 4096), Err(FrameError::Malformed));
        assert_eq!(decode_len(2, 4096), Ok(2)); // minimum legal frame
    }

    #[test]
    fn unknown_kind_surfaces_in_parse() {
        // 0x7F is not in the catalog. parse() errors; the *live reader* would
        // instead count-and-skip (see the suppression gate test).
        let buf = vec![0x02, 0x00, 0x00, 0x00, 0x7F, 0x00];
        assert_eq!(Frame::parse(&buf, 4096), Err(FrameError::UnknownKind(0x7F)));
    }

    #[test]
    fn truncated_buffer_is_not_a_frame() {
        assert_eq!(
            Frame::parse(&[0x02, 0x00], 4096),
            Err(FrameError::Truncated)
        );
        // len says 10 body bytes follow, buffer has 3.
        let buf = vec![0x0A, 0x00, 0x00, 0x00, 0x07, 0x00, 1, 2, 3];
        assert_eq!(Frame::parse(&buf, 4096), Err(FrameError::Truncated));
    }
}
