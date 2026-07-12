//! §13.1 security gates, named exactly as the RFC freeze note (§4.7):
//! - **downgrade-tamper test** — a MITM edits a negotiated HELLO field between
//!   the two HELLOs; because the AUTH MAC pins the transcript bit-exact, the
//!   MAC fails and the link is refused.
//! - **reflection test** — replaying the initiator's own AUTH back at it is
//!   refused, because each direction's MAC carries a distinct role label.
//! - **cross-cluster-name** — a staging node dialing prod is refused at HELLO,
//!   before any challenge.

mod common;

use beamsocket_mesh::handshake::{features, HandshakeStep};
use beamsocket_mesh::{Frame, Handshake, RefuseReason, Role};
use common::{drive, test_cfg, HELLO_FEATURES_OFFSET};

fn init(node_id: u16, cluster: &str, version: u16, feats: u32) -> Handshake {
    Handshake::new(Role::Initiator, test_cfg(node_id, cluster, version, feats))
}
fn resp(node_id: u16, cluster: &str, version: u16, feats: u32) -> Handshake {
    Handshake::new(Role::Responder, test_cfg(node_id, cluster, version, feats))
}

fn one_frame(step: HandshakeStep) -> Frame {
    match step {
        HandshakeStep::Continue(mut v) => {
            assert_eq!(v.len(), 1, "expected exactly one frame");
            v.pop().unwrap()
        }
        other => panic!("expected Continue(1 frame), got {other:?}"),
    }
}

// ---------- downgrade-tamper test ----------

#[test]
fn downgrade_tamper_feature_bit_breaks_auth() {
    // MITM clears a feature bit in the initiator's HELLO on its way to the
    // responder. Both sides still reach AUTH (features do not gate the version
    // window), but their transcripts now differ by one byte, so the MAC fails.
    let mitm = |role: Role, f: &mut Frame| {
        if role == Role::Initiator && f.kind == beamsocket_mesh::FrameKind::Hello {
            f.body[HELLO_FEATURES_OFFSET] ^= 0x01; // flip INTEREST_ROUTING bit
        }
    };
    let (a, b) = drive(
        init(1, "prod", 2, features::INTEREST_ROUTING),
        resp(2, "prod", 2, features::INTEREST_ROUTING),
        mitm,
    );
    assert_eq!(b, Err(RefuseReason::AuthFailed), "tamper must fail the MAC");
    assert!(a.is_err(), "initiator never completes against the refuser");
}

#[test]
fn downgrade_tamper_version_also_breaks_auth() {
    // The other tampering the RFC names: lowering the version. 3→2 stays in the
    // one-step window (so no version refusal), but the transcript byte differs →
    // AUTH fails. Proof that a silent downgrade cannot slip past auth.
    let mitm = |role: Role, f: &mut Frame| {
        if role == Role::Initiator && f.kind == beamsocket_mesh::FrameKind::Hello {
            // version field is at offset 4 (LE u16), just after the 4-byte magic.
            f.body[4] = 2; // was 3
        }
    };
    let (_, b) = drive(init(1, "prod", 3, 0), resp(2, "prod", 3, 0), mitm);
    assert_eq!(b, Err(RefuseReason::AuthFailed));
}

// ---------- reflection test ----------

#[test]
fn reflection_of_own_auth_is_refused() {
    // Walk the handshake by hand up to the initiator's AUTH, then feed that same
    // AUTH back to the initiator (as an attacker reflecting it would). The
    // initiator verifies incoming AUTH against the RESPONDER label; its own AUTH
    // carries the INITIATOR label, so the MAC cannot match.
    let mut i = init(1, "prod", 1, 0);
    let mut r = resp(2, "prod", 1, 0);

    let hello_i = i.start();
    let hello_r = r.start();

    // Exchange HELLOs.
    assert!(matches!(i.on_frame(&hello_r), HandshakeStep::Continue(_)));
    let challenge = one_frame(r.on_frame(&hello_i));

    // Initiator answers the challenge with AUTH_i.
    let auth_i = one_frame(i.on_frame(&challenge));

    // Reflect AUTH_i back at the initiator instead of delivering AUTH_r.
    match i.on_frame(&auth_i) {
        HandshakeStep::Refused(RefuseReason::AuthFailed) => {}
        other => panic!("reflected AUTH must be refused, got {other:?}"),
    }
}

// ---------- full-session replay (3A review fixup: initiator freshness) ----------

#[test]
fn full_recorded_session_replay_is_refused() {
    // An attacker records a COMPLETE past session (both HELLOs, challenge, both
    // AUTHs) and, posing as the responder, replays the recorded responder frames
    // at a brand-new initiator whose static config is byte-identical to the
    // recorded one. Without initiator freshness this would verify. With the
    // per-attempt `fresh` nonce in HELLO, the new initiator's transcript differs
    // from the recorded one, so the replayed responder-AUTH fails its MAC.
    let mut rec_i = init(1, "prod", 1, 0);
    let mut rec_r = resp(2, "prod", 1, 0);
    let rec_hello_i = rec_i.start();
    let rec_hello_r = rec_r.start();
    assert!(matches!(
        rec_i.on_frame(&rec_hello_r),
        HandshakeStep::Continue(_)
    ));
    let rec_challenge = one_frame(rec_r.on_frame(&rec_hello_i));
    let _rec_auth_i = one_frame(rec_i.on_frame(&rec_challenge));
    // (rec_r would now emit AUTH_r; capture it.)
    let rec_auth_r = match rec_r.on_frame(&_rec_auth_i) {
        HandshakeStep::Established { send, .. } => send.into_iter().next().unwrap(),
        HandshakeStep::Continue(v) => v.into_iter().next().unwrap(),
        other => panic!("recorded responder should finish, got {other:?}"),
    };

    // New session: fresh initiator, same static config. Attacker replays the
    // recorded responder frames (its HELLO carries the OLD `fresh`).
    let mut new_i = init(1, "prod", 1, 0);
    let _new_hello_i = new_i.start();
    assert!(matches!(
        new_i.on_frame(&rec_hello_r),
        HandshakeStep::Continue(_)
    ));
    let new_challenge_from_replay = new_i.on_frame(&rec_challenge);
    // The initiator will produce AUTH against the replayed challenge, then be
    // handed the recorded AUTH_r — which must fail (transcripts diverge on fresh).
    let _ = one_frame(new_challenge_from_replay);
    match new_i.on_frame(&rec_auth_r) {
        HandshakeStep::Refused(RefuseReason::AuthFailed) => {}
        other => panic!("recorded-session replay must be refused, got {other:?}"),
    }
}

// ---------- cross-cluster-name ----------

#[test]
fn cross_cluster_name_refused_at_hello_before_auth() {
    // A staging node dials a prod node. The prod responder must refuse the very
    // first frame (the HELLO) — it must NOT emit a CHALLENGE, which would mean
    // it had begun authenticating a foreign-cluster peer.
    let mut i = init(1, "staging", 1, 0);
    let mut r = resp(2, "prod", 1, 0);

    let hello_i = i.start();
    let _hello_r = r.start();

    match r.on_frame(&hello_i) {
        HandshakeStep::Refused(RefuseReason::ClusterMismatch { local, peer }) => {
            assert_eq!(local, "prod");
            assert_eq!(peer, "staging");
        }
        HandshakeStep::Continue(frames) => panic!(
            "prod responder must not proceed to challenge a staging peer; emitted {} frame(s)",
            frames.len()
        ),
        other => panic!("expected ClusterMismatch refusal, got {other:?}"),
    }
    // And the barrier is terminal, not a retry.
    assert!(!RefuseReason::ClusterMismatch {
        local: "prod".into(),
        peer: "staging".into()
    }
    .should_backoff_retry());
}
