//! beamsocket-mesh — the cluster **link layer** (RFC 0004, Phase 3A).
//!
//! Scope of THIS crate (docs/ENGINEERING.md §13.1): wire framing, the
//! HELLO→CHALLENGE→AUTH handshake, version/feature negotiation with
//! sender-suppression, the coalesced link writer, and per-peer byte-bounded
//! drop-and-count queues. That is the transport a mesh is built ON — it moves
//! authenticated, framed bytes between two nodes and nothing more.
//!
//! Explicitly NOT here (later sub-phases, so the seams stay honest):
//! - SWIM membership (3B) — this crate carries MEMBERSHIP frames but never
//!   interprets them; the frame kind exists, the gossip engine does not.
//! - Interest routing (3C) — the INTEREST/INTEREST_DIGEST kinds and their
//!   feature bit exist so negotiation can gate them; the router does not.
//! - Relay verbs + engine integration (3D) — RELAY_* kinds exist for the same
//!   reason; nothing in core/node/SDK depends on this crate yet.
//!
//! Ground rules (docs/ENGINEERING.md §1), enforced across the wire exactly as
//! within a node:
//! 1. No per-message JS — this crate never touches the bridge or a runtime.
//! 2. No global lock on a hot path — link state is per-peer, lock-local.
//! 3. Safety features work behind infrastructure — the mesh trusts its network
//!    boundary (§4.7) but authenticates every peer regardless.
//! 4. Per-peer state documents its memory cost — see [`config::LinkConfig`] and
//!    the PR notes (queue HWM × peers, N ≤ 50).
//! 5. **Every queue is bounded.** [`queue::PeerQueue`] is byte-bounded with a
//!    drop-newest-and-count overflow policy and a per-peer pressure gauge — the
//!    star rule of this PR (§4.6).
//!
//! Like `beamsocket-core`, this crate must NEVER depend on napi. Core attaches
//! to it behind the Engine facade in 3D; nothing crosses the Rust↔JS boundary
//! here.

pub mod config;
pub mod counters;
pub mod crypto;
pub mod frame;
pub mod handshake;
pub mod hello;
pub mod link;
pub mod membership_sync;
pub mod node;
pub mod probe;
pub mod queue;
pub mod swim;

pub use config::LinkConfig;
pub use counters::LinkCounters;
pub use frame::{Flags, Frame, FrameError, FrameKind, MAX_FRAME_CEILING, MIN_FRAME_FLOOR};
pub use handshake::{Handshake, HandshakeError, HandshakeStep, Negotiated, RefuseReason, Role};
pub use hello::Hello;
pub use link::{Link, LinkError, LinkHandle, LinkHooks, LinkState};
pub use node::{MeshConfig, MeshNode};
pub use queue::PeerQueue;
pub use swim::{MState, MemberInfo, Membership, MembershipCounters, SwimParams};

/// The mesh wire protocol version (RFC 0004 §4.4). Bumped **only** for
/// incompatible changes; additive changes ride feature bits ([`hello::Hello`]).
///
/// The compatibility promise is **N interoperates with N−1** — a link speaks
/// `min(local, remote)`; a peer outside `{N, N−1}` is refused loudly, never
/// retried (see [`handshake`]). At v1 there is no N−1, so a v0 or v2 peer is
/// the "two-step / out-of-window" refusal the interop matrix exercises.
pub const PROTOCOL_VERSION: u16 = 1;

/// The four ASCII bytes that open every HELLO body (RFC 0004 §4.4 frame
/// catalog). A first frame that does not start with this is not a BeamSocket
/// mesh peer; the link is refused before any allocation on its behalf.
pub const MESH_MAGIC: [u8; 4] = *b"BSMH";
