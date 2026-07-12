//! The mesh handshake and negotiation (RFC 0004 §4.4 + §4.7), as a **sans-IO
//! state machine**.
//!
//! Why sans-IO: the security-critical logic — transcript pinning, role-bound
//! MACs, version windows, feature intersection — must be exhaustively testable
//! without sockets, timers, or a runtime. [`Handshake`] consumes decoded
//! control [`Frame`]s and produces frames to send plus a terminal outcome; the
//! async [`crate::link`] is a thin driver that moves bytes between this machine
//! and a `TcpStream`. Every §13.1 handshake gate (interop matrix,
//! downgrade-tamper, reflection, cross-cluster) drives this type directly.
//!
//! The flow, both directions (§4.7):
//! ```text
//!   initiator (dialer)                 responder (accepter)
//!   ── HELLO ─────────────────────────▶
//!   ◀───────────────────────── HELLO ──
//!   ◀──────────────────── CHALLENGE(n) ──   (responder picks fresh nonce n)
//!   ── AUTH_i = MAC("bsmh-initiator" ‖ n ‖ transcript) ─▶
//!                                          (verify AUTH_i, then:)
//!   ◀── AUTH_r = MAC("bsmh-responder" ‖ n ‖ transcript) ──
//!   (verify AUTH_r)                        (established)
//!   (established)
//! ```
//! `transcript` = the two HELLO **bodies, bit-exact as received**, initiator's
//! first. The MAC covers a **role label** and the responder's **fresh nonce**,
//! so (a) tampering with any negotiated HELLO field breaks the MAC
//! [downgrade-tamper], and (b) an attacker cannot reflect one side's AUTH back
//! at it [reflection] — the labels differ.

use crate::config::LinkConfig;
use crate::crypto::{constant_time_eq, hmac_sha256, random_nonce};
use crate::frame::{Flags, Frame, FrameKind, MIN_FRAME_FLOOR};
use crate::hello::{Hello, HelloError};

/// Feature bits (§4.4 reserved u32). Defined here because this module is what
/// interprets them into "which frames may exist on this link." A feature bit
/// gates *which frames exist*, **never how an existing frame parses** — the
/// §4.4 invariant that keeps body-evolution and feature-gating orthogonal.
pub mod features {
    /// Interest routing (3C): gates `INTEREST` / `INTEREST_DIGEST`.
    pub const INTEREST_ROUTING: u32 = 1 << 0;
    /// Relay data plane (3D): gates all `RELAY_*` kinds. New data-plane kinds
    /// are never additive (§4.4) — they live behind this bit or a version bump.
    pub const RELAY: u32 = 1 << 1;
}

/// Role labels bound into each direction's MAC (§4.7). Distinct labels are what
/// make a reflected AUTH fail to verify.
pub const INITIATOR_LABEL: &[u8] = b"bsmh-initiator";
/// See [`INITIATOR_LABEL`].
pub const RESPONDER_LABEL: &[u8] = b"bsmh-responder";

/// Which end of the link this machine is. The dialer is the initiator; the
/// accepter is the responder (§4.7). Determines transcript ordering and MAC
/// labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator,
    Responder,
}

impl Role {
    fn my_label(self) -> &'static [u8] {
        match self {
            Role::Initiator => INITIATOR_LABEL,
            Role::Responder => RESPONDER_LABEL,
        }
    }
    fn peer_label(self) -> &'static [u8] {
        match self {
            Role::Initiator => RESPONDER_LABEL,
            Role::Responder => INITIATOR_LABEL,
        }
    }
}

/// The outcome of feeding one frame to the machine.
#[derive(Debug)]
pub enum HandshakeStep {
    /// Emit these frames (possibly empty) and keep waiting for input.
    Continue(Vec<Frame>),
    /// Handshake complete. Emit `send` (the responder's AUTH, or nothing for the
    /// initiator), then the link is authenticated and `negotiated` governs it.
    Established {
        send: Vec<Frame>,
        negotiated: Negotiated,
    },
    /// Handshake refused. Close the link; `reason` selects a distinct link
    /// state and log line. Never followed by more frames.
    Refused(RefuseReason),
}

/// The negotiated properties of an established link. `may_emit` on this type is
/// the **sender-suppression** enforcement point (§4.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiated {
    /// `min(local, peer)` — the version both sides speak.
    pub protocol_version: u16,
    /// `local & peer` — features usable only when BOTH advertised them.
    pub features: u32,
    /// `min(local, peer)` — the max frame either side will accept.
    pub max_frame: u32,
    /// Which role this node played (affects nothing post-handshake except
    /// diagnostics; kept for the stats shape).
    pub role: Role,
    pub peer_node_id: u16,
    pub peer_incarnation: u64,
}

impl Negotiated {
    /// **Sender suppression (§4.4):** may this node emit `kind` on this link?
    /// Control and base kinds always; feature-gated kinds only when the gating
    /// feature is in the negotiated intersection. The link's send path consults
    /// this and refuses (never writes) on `false` — attempting to emit an
    /// unadvertised kind is a bug, not a wire event.
    pub fn may_emit(&self, kind: FrameKind) -> bool {
        use FrameKind::*;
        match kind {
            // Control plane + membership dissemination + liveness: always.
            Hello | Challenge | Auth | Ping | Membership => true,
            // Interest routing (3C).
            Interest | InterestDigest => self.features & features::INTEREST_ROUTING != 0,
            // Relay data plane (3D) — feature-gated, never additive.
            RelayRoom | RelayUser | RelayAll | RelaySocket => self.features & features::RELAY != 0,
        }
    }
}

/// Why a link was refused. Each variant is a **distinct link state** (§4.4:
/// "visible in metrics as a distinct link-state, never a silent retry loop")
/// and carries what the log line needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefuseReason {
    /// First frame was not a HELLO, or a later frame was out of sequence.
    ProtocolViolation(&'static str),
    /// HELLO missing `BSMH` magic — not a mesh peer at all.
    NotMeshPeer,
    /// HELLO present but malformed.
    MalformedHello(HelloError),
    /// Peer is in a different cluster — refused at HELLO, **before auth** (§4.4).
    ClusterMismatch { local: String, peer: String },
    /// Peer version is more than one step away — outside `{N, N−1}`. Logged with
    /// both numbers; no retry (§4.4). Same-version and one-step interoperate.
    IncompatibleVersion { local: u16, peer: u16 },
    /// Peer claims this node's own id (§4.5) — a loud config error, not
    /// auto-resolved.
    NodeIdCollision(u16),
    /// The negotiated max frame fell below the floor — a misconfigured peer.
    MaxFrameTooSmall { negotiated: u32, floor: u32 },
    /// A peer's AUTH MAC did not verify: wrong secret, tampered transcript, or a
    /// reflected AUTH. Counted as `authFailures`; the only reason that earns a
    /// backoff-retry rather than a terminal refusal.
    AuthFailed,
}

impl RefuseReason {
    /// Auth failures get backoff (§4.7: "repeated failures get backoff, not
    /// retry storms"); every other refusal is terminal — a version/cluster/id
    /// mismatch will not fix itself by dialing again, so we do not (§4.4: "never
    /// a silent retry loop").
    pub fn should_backoff_retry(&self) -> bool {
        matches!(self, RefuseReason::AuthFailed)
    }
}

impl std::fmt::Display for RefuseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefuseReason::ProtocolViolation(s) => write!(f, "protocol violation: {s}"),
            RefuseReason::NotMeshPeer => write!(f, "not a mesh peer (no BSMH magic)"),
            RefuseReason::MalformedHello(e) => write!(f, "malformed HELLO: {e}"),
            RefuseReason::ClusterMismatch { local, peer } => {
                write!(f, "cluster mismatch: local {local:?} != peer {peer:?}")
            }
            RefuseReason::IncompatibleVersion { local, peer } => write!(
                f,
                "incompatible protocol version: local {local}, peer {peer} (window is one step)"
            ),
            RefuseReason::NodeIdCollision(id) => write!(f, "node id collision: {id}"),
            RefuseReason::MaxFrameTooSmall { negotiated, floor } => {
                write!(f, "negotiated max frame {negotiated} below floor {floor}")
            }
            RefuseReason::AuthFailed => write!(f, "auth failed (MAC mismatch)"),
        }
    }
}

/// A refusal that is also an error (for the async link's `Result`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeError(pub RefuseReason);

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "handshake refused: {}", self.0)
    }
}

impl std::error::Error for HandshakeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Sent my HELLO; waiting for the peer's HELLO.
    AwaitingHello,
    /// (Initiator) validated the peer HELLO; waiting for CHALLENGE.
    AwaitingChallenge,
    /// Waiting for the peer's AUTH (responder waits for AUTH_i, initiator for
    /// AUTH_r — both are an AUTH frame verified against the peer's role label).
    AwaitingAuth,
    /// Terminal.
    Done,
    Refused,
}

/// The handshake state machine for one link. Construct with [`Handshake::new`],
/// take the opening frame from [`Handshake::start`], then feed every received
/// control frame to [`Handshake::on_frame`] until it returns `Established` or
/// `Refused`.
pub struct Handshake {
    role: Role,
    cfg: LinkConfig,
    state: State,
    my_hello_raw: Vec<u8>,
    peer_hello_raw: Option<Vec<u8>>,
    /// The responder's fresh nonce (generated by the responder, learned by the
    /// initiator from CHALLENGE).
    nonce: Option<[u8; 32]>,
    negotiated: Option<Negotiated>,
}

impl Handshake {
    pub fn new(role: Role, cfg: LinkConfig) -> Self {
        Self {
            role,
            cfg,
            state: State::AwaitingHello,
            my_hello_raw: Vec::new(),
            peer_hello_raw: None,
            nonce: None,
            negotiated: None,
        }
    }

    /// The opening frame: this node's HELLO. Both roles send it immediately, so
    /// the two HELLO bodies (the transcript) are on the wire before any
    /// challenge. Call exactly once, before [`Handshake::on_frame`].
    pub fn start(&mut self) -> Frame {
        let hello = Hello {
            protocol_version: self.cfg.protocol_version,
            node_id: self.cfg.node_id,
            incarnation: self.cfg.incarnation,
            max_frame: self.cfg.max_frame,
            features: self.cfg.features,
            // 3A review fixup — initiator freshness: a per-attempt random nonce
            // makes every transcript unique, so a full recorded-session replay
            // can never verify (the MAC covers both HELLO bodies bit-exact).
            fresh: random_nonce(),
            cluster_name: self.cfg.cluster_name.clone(),
        };
        self.my_hello_raw = hello.encode();
        Frame::new(FrameKind::Hello, self.my_hello_raw.clone())
    }

    /// The two HELLO bodies, initiator's first, bit-exact as each side holds
    /// them. Requires both HELLOs to be present.
    fn transcript(&self) -> Vec<u8> {
        let peer = self.peer_hello_raw.as_deref().unwrap_or_default();
        let (first, second) = match self.role {
            Role::Initiator => (self.my_hello_raw.as_slice(), peer),
            Role::Responder => (peer, self.my_hello_raw.as_slice()),
        };
        let mut t = Vec::with_capacity(first.len() + second.len());
        t.extend_from_slice(first);
        t.extend_from_slice(second);
        t
    }

    fn auth_mac(&self, label: &[u8]) -> [u8; 32] {
        let nonce = self.nonce.expect("nonce set before AUTH");
        let transcript = self.transcript();
        // input = role_label ‖ responder_nonce ‖ transcript
        let mut input = Vec::with_capacity(label.len() + 32 + transcript.len());
        input.extend_from_slice(label);
        input.extend_from_slice(&nonce);
        input.extend_from_slice(&transcript);
        hmac_sha256(&self.cfg.secret, &input)
    }

    /// Validate the peer's HELLO and compute the negotiated parameters. All
    /// refusals here happen **before** any challenge — the cluster-name and
    /// version barriers sit ahead of auth by construction.
    fn validate_peer_hello(&mut self, body: &[u8]) -> Result<Negotiated, RefuseReason> {
        let hello = match Hello::decode(body) {
            Ok(h) => h,
            Err(HelloError::BadMagic) => return Err(RefuseReason::NotMeshPeer),
            Err(e) => return Err(RefuseReason::MalformedHello(e)),
        };

        // Cross-cluster barrier — first, and before auth (§4.4).
        if hello.cluster_name != self.cfg.cluster_name {
            return Err(RefuseReason::ClusterMismatch {
                local: self.cfg.cluster_name.clone(),
                peer: hello.cluster_name,
            });
        }

        // Two nodes claiming one id is a config error, loudly fatal (§4.5).
        if hello.node_id == self.cfg.node_id {
            return Err(RefuseReason::NodeIdCollision(hello.node_id));
        }

        // Version window is one step in either direction: same or ±1 interoperate
        // (speaking min); two or more apart is refused and logged (§4.4).
        let local = self.cfg.protocol_version;
        let peer = hello.protocol_version;
        if local.abs_diff(peer) > 1 {
            return Err(RefuseReason::IncompatibleVersion { local, peer });
        }

        let max_frame = self.cfg.max_frame.min(hello.max_frame);
        if max_frame < MIN_FRAME_FLOOR {
            return Err(RefuseReason::MaxFrameTooSmall {
                negotiated: max_frame,
                floor: MIN_FRAME_FLOOR,
            });
        }

        Ok(Negotiated {
            protocol_version: local.min(peer),
            features: self.cfg.features & hello.features,
            max_frame,
            role: self.role,
            peer_node_id: hello.node_id,
            peer_incarnation: hello.incarnation,
        })
    }

    /// Feed one received control frame to the machine.
    pub fn on_frame(&mut self, frame: &Frame) -> HandshakeStep {
        match self.state {
            State::AwaitingHello => self.on_hello(frame),
            State::AwaitingChallenge => self.on_challenge(frame),
            State::AwaitingAuth => self.on_auth(frame),
            State::Done | State::Refused => self.refuse(RefuseReason::ProtocolViolation(
                "frame after handshake terminal",
            )),
        }
    }

    fn on_hello(&mut self, frame: &Frame) -> HandshakeStep {
        if frame.kind != FrameKind::Hello {
            return self.refuse(RefuseReason::ProtocolViolation("expected HELLO"));
        }
        self.peer_hello_raw = Some(frame.body.clone());
        let negotiated = match self.validate_peer_hello(&frame.body) {
            Ok(n) => n,
            Err(reason) => return self.refuse(reason),
        };
        self.negotiated = Some(negotiated);

        match self.role {
            // Responder challenges once it accepts the HELLO.
            Role::Responder => {
                let nonce = random_nonce();
                self.nonce = Some(nonce);
                self.state = State::AwaitingAuth;
                HandshakeStep::Continue(vec![Frame::new(FrameKind::Challenge, nonce.to_vec())])
            }
            // Initiator waits for the challenge; nothing to send yet.
            Role::Initiator => {
                self.state = State::AwaitingChallenge;
                HandshakeStep::Continue(vec![])
            }
        }
    }

    fn on_challenge(&mut self, frame: &Frame) -> HandshakeStep {
        if frame.kind != FrameKind::Challenge {
            return self.refuse(RefuseReason::ProtocolViolation("expected CHALLENGE"));
        }
        if frame.body.len() != 32 {
            return self.refuse(RefuseReason::ProtocolViolation(
                "CHALLENGE nonce must be 32 bytes",
            ));
        }
        let mut nonce = [0u8; 32];
        nonce.copy_from_slice(&frame.body);
        self.nonce = Some(nonce);

        // Prove our knowledge of the secret over the responder's nonce +
        // transcript, bound to our initiator label.
        let mac = self.auth_mac(self.role.my_label());
        self.state = State::AwaitingAuth;
        HandshakeStep::Continue(vec![Frame::new(FrameKind::Auth, mac.to_vec())])
    }

    fn on_auth(&mut self, frame: &Frame) -> HandshakeStep {
        if frame.kind != FrameKind::Auth {
            return self.refuse(RefuseReason::ProtocolViolation("expected AUTH"));
        }
        if frame.body.len() != 32 {
            return self.refuse(RefuseReason::ProtocolViolation("AUTH MAC must be 32 bytes"));
        }
        // Verify the PEER's AUTH against the PEER's role label. A reflected AUTH
        // (our own frame bounced back) carries our label and fails here.
        let expected = self.auth_mac(self.role.peer_label());
        if !constant_time_eq(&frame.body, &expected) {
            return self.refuse(RefuseReason::AuthFailed);
        }

        let negotiated = self.negotiated.clone().expect("negotiated set at HELLO");
        self.state = State::Done;
        match self.role {
            // Responder authenticated the initiator; now prove itself and close
            // the handshake.
            Role::Responder => {
                let mac = self.auth_mac(RESPONDER_LABEL);
                HandshakeStep::Established {
                    send: vec![Frame::new(FrameKind::Auth, mac.to_vec())],
                    negotiated,
                }
            }
            // Initiator verified the responder's AUTH; done, nothing to send.
            Role::Initiator => HandshakeStep::Established {
                send: vec![],
                negotiated,
            },
        }
    }

    fn refuse(&mut self, reason: RefuseReason) -> HandshakeStep {
        self.state = State::Refused;
        HandshakeStep::Refused(reason)
    }
}

/// Build a `Ping` or `Pong` frame (§4.4 liveness). Kept next to the handshake
/// so the one control-plane frame the link emits post-handshake lives with its
/// siblings.
pub fn ping_frame(is_pong: bool) -> Frame {
    let flags = if is_pong {
        Flags(Flags::PONG)
    } else {
        Flags::NONE
    };
    Frame::with_flags(FrameKind::Ping, flags, Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(node_id: u16, cluster: &str, version: u16, features: u32) -> LinkConfig {
        let mut c = LinkConfig::new(node_id, cluster, b"shared-secret".to_vec());
        c.protocol_version = version;
        c.features = features;
        c
    }

    /// Drive two machines against each other with an optional transform applied
    /// to each frame in flight (the MITM hook). Returns both outcomes.
    fn run(
        mut a: Handshake,
        mut b: Handshake,
        mut mitm: impl FnMut(Role, &mut Frame),
    ) -> (
        Result<Negotiated, RefuseReason>,
        Result<Negotiated, RefuseReason>,
    ) {
        // a is the initiator, b the responder.
        let mut a_out = Some(a.start());
        let mut b_out = Some(b.start());
        let mut a_done: Option<Result<Negotiated, RefuseReason>> = None;
        let mut b_done: Option<Result<Negotiated, RefuseReason>> = None;
        // Queues of frames from A→B and B→A.
        let mut to_b: Vec<Frame> = a_out.take().into_iter().collect();
        let mut to_a: Vec<Frame> = b_out.take().into_iter().collect();

        for _ in 0..16 {
            // Deliver to B (frames authored by A, the initiator).
            let mut next_to_a = Vec::new();
            for mut f in to_b.drain(..) {
                mitm(Role::Initiator, &mut f);
                match b.on_frame(&f) {
                    HandshakeStep::Continue(out) => next_to_a.extend(out),
                    HandshakeStep::Established { send, negotiated } => {
                        next_to_a.extend(send);
                        b_done.get_or_insert(Ok(negotiated));
                    }
                    HandshakeStep::Refused(r) => {
                        b_done.get_or_insert(Err(r));
                    }
                }
            }
            // Deliver to A (frames authored by B, the responder).
            let mut next_to_b = Vec::new();
            for mut f in to_a.drain(..) {
                mitm(Role::Responder, &mut f);
                match a.on_frame(&f) {
                    HandshakeStep::Continue(out) => next_to_b.extend(out),
                    HandshakeStep::Established { send, negotiated } => {
                        next_to_b.extend(send);
                        a_done.get_or_insert(Ok(negotiated));
                    }
                    HandshakeStep::Refused(r) => {
                        a_done.get_or_insert(Err(r));
                    }
                }
            }
            to_a = next_to_a;
            to_b = next_to_b;
            if to_a.is_empty() && to_b.is_empty() {
                break;
            }
        }
        (
            a_done.unwrap_or(Err(RefuseReason::ProtocolViolation("no outcome"))),
            b_done.unwrap_or(Err(RefuseReason::ProtocolViolation("no outcome"))),
        )
    }

    fn noop(_: Role, _: &mut Frame) {}

    #[test]
    fn happy_path_both_sides_establish() {
        let a = Handshake::new(Role::Initiator, cfg(1, "prod", 1, 0));
        let b = Handshake::new(Role::Responder, cfg(2, "prod", 1, 0));
        let (ra, rb) = run(a, b, noop);
        let na = ra.expect("initiator establishes");
        let nb = rb.expect("responder establishes");
        assert_eq!(na.peer_node_id, 2);
        assert_eq!(nb.peer_node_id, 1);
        assert_eq!(na.protocol_version, 1);
    }

    #[test]
    fn feature_bits_are_intersected() {
        let a = Handshake::new(
            Role::Initiator,
            cfg(1, "prod", 1, features::INTEREST_ROUTING | features::RELAY),
        );
        let b = Handshake::new(
            Role::Responder,
            cfg(2, "prod", 1, features::INTEREST_ROUTING),
        );
        let (ra, _rb) = run(a, b, noop);
        let na = ra.unwrap();
        assert_eq!(na.features, features::INTEREST_ROUTING, "intersection only");
        assert!(na.may_emit(FrameKind::Interest));
        assert!(
            !na.may_emit(FrameKind::RelayRoom),
            "RELAY not on both sides"
        );
        assert!(na.may_emit(FrameKind::Ping), "control always allowed");
    }

    #[test]
    fn version_matrix_same_and_one_step_ok_two_step_refused() {
        // same
        let (ra, _) = run(
            Handshake::new(Role::Initiator, cfg(1, "prod", 5, 0)),
            Handshake::new(Role::Responder, cfg(2, "prod", 5, 0)),
            noop,
        );
        assert_eq!(ra.unwrap().protocol_version, 5);
        // one step: 5 vs 4 → speak 4
        let (ra, _) = run(
            Handshake::new(Role::Initiator, cfg(1, "prod", 5, 0)),
            Handshake::new(Role::Responder, cfg(2, "prod", 4, 0)),
            noop,
        );
        assert_eq!(ra.unwrap().protocol_version, 4);
        // two step: 5 vs 3 → refused with both numbers, no auth reached
        let (ra, rb) = run(
            Handshake::new(Role::Initiator, cfg(1, "prod", 5, 0)),
            Handshake::new(Role::Responder, cfg(2, "prod", 3, 0)),
            noop,
        );
        assert!(matches!(
            rb,
            Err(RefuseReason::IncompatibleVersion { local: 3, peer: 5 })
        ));
        assert!(ra.is_err(), "initiator never completes against a refuser");
    }

    #[test]
    fn cluster_mismatch_refused_before_auth() {
        let a = Handshake::new(Role::Initiator, cfg(1, "staging", 1, 0));
        let b = Handshake::new(Role::Responder, cfg(2, "prod", 1, 0));
        let (_ra, rb) = run(a, b, noop);
        assert!(matches!(rb, Err(RefuseReason::ClusterMismatch { .. })));
    }

    #[test]
    fn node_id_collision_refused() {
        let a = Handshake::new(Role::Initiator, cfg(7, "prod", 1, 0));
        let b = Handshake::new(Role::Responder, cfg(7, "prod", 1, 0));
        let (_ra, rb) = run(a, b, noop);
        assert_eq!(rb, Err(RefuseReason::NodeIdCollision(7)));
    }
}
