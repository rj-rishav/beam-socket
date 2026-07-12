//! The HELLO body (RFC 0004 §4.4 frame catalog + §4.7 transcript).
//!
//! HELLO opens every link and carries everything negotiation and
//! authentication depend on: `magic`, `protocol_version`, `node_id`,
//! `incarnation`, `max_frame`, `feature bits`, and `cluster_name`. Two
//! properties make this codec load-bearing:
//!
//! - **Append-only body (§4.4 body-evolution rule).** Fields are encoded in a
//!   fixed order; a decoder tolerates a *longer* body than it knows (trailing
//!   bytes ignored). That is how the feature-bit space and later fields extend
//!   without a version bump.
//! - **Bit-exact transcript (§4.7).** The handshake MAC covers **both HELLO
//!   bodies exactly as received**, so any MITM edit to a negotiated field
//!   breaks auth. Callers therefore keep the *raw* body bytes, not a
//!   re-encoding — [`Hello::decode`] returns the parsed view; the raw bytes are
//!   whatever arrived on the wire.

/// The parsed contents of a HELLO body. Encoding is deterministic and
/// append-only; see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub protocol_version: u16,
    pub node_id: u16,
    pub incarnation: u64,
    pub max_frame: u32,
    pub features: u32,
    pub cluster_name: String,
}

/// Fixed-size prefix before the variable-length cluster name:
/// magic(4) + version(2) + node_id(2) + incarnation(8) + max_frame(4) +
/// features(4) + name_len(2).
const FIXED_PREFIX: usize = 4 + 2 + 2 + 8 + 4 + 4 + 2;

/// A cluster name is an operator label, not a payload. Cap it so a hostile
/// HELLO cannot make the name field an allocation lever, and so the whole HELLO
/// body stays comfortably inside the frame floor.
pub const MAX_CLUSTER_NAME_LEN: usize = 255;

impl Hello {
    /// Encode the HELLO body (the frame body, without the `[len][kind][flags]`
    /// header — [`crate::frame::Frame`] adds that).
    pub fn encode(&self) -> Vec<u8> {
        let name = self.cluster_name.as_bytes();
        let mut out = Vec::with_capacity(FIXED_PREFIX + name.len());
        out.extend_from_slice(&crate::MESH_MAGIC);
        out.extend_from_slice(&self.protocol_version.to_le_bytes());
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.incarnation.to_le_bytes());
        out.extend_from_slice(&self.max_frame.to_le_bytes());
        out.extend_from_slice(&self.features.to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
        out
    }

    /// Decode a HELLO body. Tolerates trailing bytes beyond the known fields
    /// (append-only rule). The caller retains the raw bytes for the transcript;
    /// this only interprets them.
    pub fn decode(body: &[u8]) -> Result<Hello, HelloError> {
        if body.len() < FIXED_PREFIX {
            return Err(HelloError::Truncated);
        }
        if body[0..4] != crate::MESH_MAGIC {
            return Err(HelloError::BadMagic);
        }
        let protocol_version = u16::from_le_bytes([body[4], body[5]]);
        let node_id = u16::from_le_bytes([body[6], body[7]]);
        let incarnation = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let max_frame = u32::from_le_bytes(body[16..20].try_into().unwrap());
        let features = u32::from_le_bytes(body[20..24].try_into().unwrap());
        let name_len = u16::from_le_bytes([body[24], body[25]]) as usize;

        if name_len > MAX_CLUSTER_NAME_LEN {
            return Err(HelloError::ClusterNameTooLong(name_len));
        }
        let name_end = FIXED_PREFIX + name_len;
        // A body SHORTER than the declared name is malformed. A body LONGER than
        // `name_end` is fine — trailing bytes are a future field (append-only).
        if body.len() < name_end {
            return Err(HelloError::Truncated);
        }
        let cluster_name = std::str::from_utf8(&body[FIXED_PREFIX..name_end])
            .map_err(|_| HelloError::ClusterNameNotUtf8)?
            .to_string();

        Ok(Hello {
            protocol_version,
            node_id,
            incarnation,
            max_frame,
            features,
            cluster_name,
        })
    }
}

/// HELLO parse failures. Every one refuses the link **before auth** — a peer
/// that cannot present a well-formed HELLO in our cluster never reaches the
/// challenge (§4.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HelloError {
    /// First frame did not open with `BSMH` — not a mesh peer.
    BadMagic,
    /// Body ends before a declared field.
    Truncated,
    /// Declared cluster-name length exceeds [`MAX_CLUSTER_NAME_LEN`].
    ClusterNameTooLong(usize),
    /// Cluster name bytes are not valid UTF-8.
    ClusterNameNotUtf8,
}

impl std::fmt::Display for HelloError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HelloError::BadMagic => write!(f, "HELLO missing BSMH magic (not a mesh peer)"),
            HelloError::Truncated => write!(f, "HELLO body truncated"),
            HelloError::ClusterNameTooLong(n) => {
                write!(f, "HELLO cluster name too long: {n} bytes")
            }
            HelloError::ClusterNameNotUtf8 => write!(f, "HELLO cluster name not UTF-8"),
        }
    }
}

impl std::error::Error for HelloError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Hello {
        Hello {
            protocol_version: 1,
            node_id: 7,
            incarnation: 42,
            max_frame: 16 << 20,
            features: 0b101,
            cluster_name: "prod".to_string(),
        }
    }

    #[test]
    fn round_trip() {
        let h = sample();
        let body = h.encode();
        assert_eq!(&body[0..4], b"BSMH");
        assert_eq!(Hello::decode(&body).unwrap(), h);
    }

    #[test]
    fn trailing_bytes_are_tolerated() {
        // A future protocol version appends a field. An N-reader must ignore the
        // tail, not reject the peer (append-only, §4.4).
        let mut body = sample().encode();
        body.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(Hello::decode(&body).unwrap(), sample());
    }

    #[test]
    fn bad_magic_refused() {
        let mut body = sample().encode();
        body[0] = b'X';
        assert_eq!(Hello::decode(&body), Err(HelloError::BadMagic));
    }

    #[test]
    fn truncated_refused() {
        let body = sample().encode();
        assert_eq!(Hello::decode(&body[..10]), Err(HelloError::Truncated));
        // Declared name longer than the body present.
        let mut short = sample().encode();
        short.truncate(FIXED_PREFIX + 1); // name_len says 4, only 1 present
        assert_eq!(Hello::decode(&short), Err(HelloError::Truncated));
    }

    #[test]
    fn empty_cluster_name_round_trips() {
        let h = Hello {
            cluster_name: String::new(),
            ..sample()
        };
        assert_eq!(Hello::decode(&h.encode()).unwrap(), h);
    }
}
