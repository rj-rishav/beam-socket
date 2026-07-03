//! WebSocket transport — Phase 1A.
//!
//! Codec: tokio-tungstenite first (correctness, Autobahn-proven), behind this
//! module boundary so a fastwebsockets swap stays contained here
//! (ARCHITECTURE.md §7 "codec choice regret").
//!
//! permessage-deflate is OFF in Phase 1 (memory blowup risk, ~300 KB/conn).
