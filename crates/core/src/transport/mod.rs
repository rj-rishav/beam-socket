//! Transport abstraction. Everything above this module operates on frames
//! and ConnectionIds, never raw sockets — that's what makes TCP/MQTT/SSE/QUIC
//! (Phase 5) additive instead of a rewrite.

/// Implemented by transport/websocket.rs in Phase 1A.
/// Kept deliberately minimal until a second transport forces generalization.
pub trait Transport {
    // Phase 1A: accept loop, handshake, frame read/write halves.
}

pub mod websocket;
