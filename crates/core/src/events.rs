//! Engine → bridge events. Only events the app subscribed to are emitted
//! (Rule 1); the bridge batches them toward JS (see crates/node/src/bridge.rs).

use crate::ids::ConnectionId;

#[derive(Debug)]
pub enum EngineEvent {
    /// A connection passed admission + authorize and is live. (Phase 1A)
    ConnectionOpened { id: ConnectionId },
    /// App subscribed to `message` on this connection. (Phase 1A)
    Message {
        id: ConnectionId,
        payload: Vec<u8>,
        is_binary: bool,
    },
    /// Close handshake finished or connection dropped. (Phase 1A)
    ConnectionClosed {
        id: ConnectionId,
        code: u16,
        reason: String,
    },
}

// Phase 1A: the engine→bridge channel is BOUNDED (Rule 5). Its capacity and
// overflow policy come from the RFC 0001 spike results — do not guess them.
