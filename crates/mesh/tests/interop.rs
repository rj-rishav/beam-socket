//! §13.1 gate: **N / N−1 interop matrix.** A mixed-version cluster during a
//! rolling deploy must interoperate (speaking the lower version) or refuse
//! loudly and log — never corrupt (§4.4). Three rows: same-version, one-step,
//! two-step-refused.
//!
//! Driven at the handshake layer (sans-IO), which is where negotiation lives —
//! deterministic, no sockets.

mod common;

use beamsocket_mesh::handshake::features;
use beamsocket_mesh::{FrameKind, Handshake, RefuseReason, Role};
use common::{drive, passthrough, test_cfg};

fn hs(role: Role, node_id: u16, version: u16, features_bits: u32) -> Handshake {
    Handshake::new(role, test_cfg(node_id, "prod", version, features_bits))
}

#[test]
fn same_version_interoperates() {
    let (a, b) = drive(
        hs(Role::Initiator, 1, 2, 0),
        hs(Role::Responder, 2, 2, 0),
        passthrough,
    );
    let na = a.expect("initiator establishes");
    let nb = b.expect("responder establishes");
    assert_eq!(na.protocol_version, 2);
    assert_eq!(nb.protocol_version, 2);
    assert_eq!(na.peer_node_id, 2);
    assert_eq!(nb.peer_node_id, 1);
}

#[test]
fn one_step_apart_speaks_the_lower_version() {
    // v3 (new) meets v2 (old) → both speak v2, in either dial direction.
    for (iv, rv) in [(3, 2), (2, 3)] {
        let (a, b) = drive(
            hs(Role::Initiator, 1, iv, 0),
            hs(Role::Responder, 2, rv, 0),
            passthrough,
        );
        assert_eq!(a.unwrap().protocol_version, 2, "iv={iv} rv={rv}");
        assert_eq!(b.unwrap().protocol_version, 2, "iv={iv} rv={rv}");
    }
}

#[test]
fn two_steps_apart_is_refused_and_logged() {
    // v3 meets v1: two steps → refused. Skipping a version in one deploy is not
    // the supported path (§4.4).
    let (a, b) = drive(
        hs(Role::Initiator, 1, 3, 0),
        hs(Role::Responder, 2, 1, 0),
        passthrough,
    );
    // The responder sees local=1, peer=3 and refuses with BOTH numbers.
    match b {
        Err(RefuseReason::IncompatibleVersion { local, peer }) => {
            assert_eq!((local, peer), (1, 3));
            // "logged" = the refusal renders both versions for the operator.
            let line = RefuseReason::IncompatibleVersion { local, peer }.to_string();
            assert!(line.contains('1') && line.contains('3'), "log line: {line}");
        }
        other => panic!("expected IncompatibleVersion, got {other:?}"),
    }
    // The initiator never completes against a refuser (no silent success).
    assert!(a.is_err());
    // And it is terminal — a version gap does not earn a retry (§4.4).
    assert!(!RefuseReason::IncompatibleVersion { local: 1, peer: 3 }.should_backoff_retry());
}

#[test]
fn feature_bits_are_the_intersection_across_the_matrix() {
    // Negotiation of the version does not disturb feature intersection: a v2/v2
    // link where one side lacks RELAY yields no RELAY, both sides agreeing.
    let (a, b) = drive(
        hs(
            Role::Initiator,
            1,
            2,
            features::INTEREST_ROUTING | features::RELAY,
        ),
        hs(Role::Responder, 2, 2, features::INTEREST_ROUTING),
        passthrough,
    );
    let na = a.unwrap();
    let nb = b.unwrap();
    assert_eq!(na.features, features::INTEREST_ROUTING);
    assert_eq!(nb.features, features::INTEREST_ROUTING);
    // Both sides agree on what may be emitted — the basis of sender suppression.
    assert!(na.may_emit(FrameKind::Interest) && nb.may_emit(FrameKind::Interest));
    assert!(!na.may_emit(FrameKind::RelayRoom) && !nb.may_emit(FrameKind::RelayRoom));
}
