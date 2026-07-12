//! TCP membership **dissemination** (RFC 0004 §4.2 + §4.4 freeze): the codec and
//! merge logic for MEMBERSHIP frames, which ride **negotiated TCP links only**.
//!
//! Three message shapes share the `Membership` frame kind (0x04), tagged by a
//! sub-type byte:
//! - **Sync** — push-pull full state. The join half-that-is-load-bearing (spike
//!   fix #2): a joiner pushes its state (so the peer sees any "you are dead"
//!   claim and refutes) and the peer pulls back its own state. `reply` marks a
//!   request (expects a state back) vs the answering push (does not).
//! - **Gossip** — incremental piggybacked updates (epidemic spread of a change).
//! - **Digest** — anti-entropy summary `(id, inc)`; the peer answers with the
//!   updates it holds that the digest shows are missing or stale.
//!
//! [`apply`] is pure over the [`Membership`] table and returns an optional
//! response, so the merge rules are unit-testable without sockets. The wire
//! egress is the 3A per-peer [`crate::queue::PeerQueue`] — **no new queue type**
//! (Rule 5).

use crate::probe::{get_addr, put_addr};
use crate::swim::{MState, Membership, Update};

const SUB_SYNC: u8 = 1;
const SUB_GOSSIP: u8 = 2;
const SUB_DIGEST: u8 = 3;

/// A decoded MEMBERSHIP message (the body of a `FrameKind::Membership` frame).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipMsg {
    /// Push-pull full-state sync. `reply = true` is a request (answer with your
    /// state); `false` is the answering push.
    Sync { reply: bool, updates: Vec<Update> },
    /// Incremental gossip — merge, no response.
    Gossip(Vec<Update>),
    /// Anti-entropy digest: what the sender knows, as `(id, inc)`.
    Digest(Vec<(u16, u32)>),
}

fn state_u8(s: MState) -> u8 {
    match s {
        MState::Alive => 0,
        MState::Suspect => 1,
        MState::Dead => 2,
    }
}

fn u8_state(b: u8) -> Option<MState> {
    Some(match b {
        0 => MState::Alive,
        1 => MState::Suspect,
        2 => MState::Dead,
        _ => return None,
    })
}

fn put_update(buf: &mut Vec<u8>, u: &Update) {
    buf.extend_from_slice(&u.id.to_le_bytes());
    buf.push(state_u8(u.state));
    buf.extend_from_slice(&u.inc.to_le_bytes());
    put_addr(buf, u.addr);
}

fn get_update(buf: &[u8], off: &mut usize) -> Option<Update> {
    let id = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?);
    *off += 2;
    let state = u8_state(*buf.get(*off)?)?;
    *off += 1;
    let inc = u32::from_le_bytes(buf.get(*off..*off + 4)?.try_into().ok()?);
    *off += 4;
    let addr = get_addr(buf, off)?;
    Some(Update {
        id,
        addr,
        state,
        inc,
    })
}

fn put_updates(buf: &mut Vec<u8>, updates: &[Update]) {
    buf.extend_from_slice(&(updates.len() as u16).to_le_bytes());
    for u in updates {
        put_update(buf, u);
    }
}

fn get_updates(buf: &[u8], off: &mut usize) -> Option<Vec<Update>> {
    let count = u16::from_le_bytes(buf.get(*off..*off + 2)?.try_into().ok()?) as usize;
    *off += 2;
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(get_update(buf, off)?);
    }
    Some(v)
}

impl MembershipMsg {
    /// Encode the frame body (`[subtype][payload]`).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            MembershipMsg::Sync { reply, updates } => {
                buf.push(SUB_SYNC);
                buf.push(*reply as u8);
                put_updates(&mut buf, updates);
            }
            MembershipMsg::Gossip(updates) => {
                buf.push(SUB_GOSSIP);
                put_updates(&mut buf, updates);
            }
            MembershipMsg::Digest(entries) => {
                buf.push(SUB_DIGEST);
                buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
                for (id, inc) in entries {
                    buf.extend_from_slice(&id.to_le_bytes());
                    buf.extend_from_slice(&inc.to_le_bytes());
                }
            }
        }
        buf
    }

    /// Decode a MEMBERSHIP frame body. `None` on a malformed body (the link
    /// counts it as a frame_in and the caller drops it).
    pub fn decode(body: &[u8]) -> Option<MembershipMsg> {
        let sub = *body.first()?;
        let mut off = 1;
        match sub {
            SUB_SYNC => {
                let reply = *body.get(off)? != 0;
                off += 1;
                let updates = get_updates(body, &mut off)?;
                Some(MembershipMsg::Sync { reply, updates })
            }
            SUB_GOSSIP => Some(MembershipMsg::Gossip(get_updates(body, &mut off)?)),
            SUB_DIGEST => {
                let count = u16::from_le_bytes(body.get(off..off + 2)?.try_into().ok()?) as usize;
                off += 2;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = u16::from_le_bytes(body.get(off..off + 2)?.try_into().ok()?);
                    off += 2;
                    let inc = u32::from_le_bytes(body.get(off..off + 4)?.try_into().ok()?);
                    off += 4;
                    entries.push((id, inc));
                }
                Some(MembershipMsg::Digest(entries))
            }
            _ => None,
        }
    }
}

/// Apply an inbound message to the table, returning an optional response to send
/// back over the same link. Pure over `membership` — the node just moves bytes.
pub fn apply(
    msg: MembershipMsg,
    membership: &mut Membership,
    retransmit: u32,
) -> Option<MembershipMsg> {
    match msg {
        MembershipMsg::Sync { reply, updates } => {
            // Push half FIRST (may trigger our own refutation), so the pull we
            // answer with already carries the bumped self (spike fix #2).
            for u in &updates {
                membership.merge(u, retransmit);
            }
            if reply {
                Some(MembershipMsg::Sync {
                    reply: false,
                    updates: membership.full_state(),
                })
            } else {
                None
            }
        }
        MembershipMsg::Gossip(updates) => {
            for u in &updates {
                membership.merge(u, retransmit);
            }
            None
        }
        MembershipMsg::Digest(entries) => {
            // Answer with everything we hold that the digest shows is missing or
            // stale (peer's inc < ours, or peer doesn't list it at all).
            use std::collections::HashMap;
            let peer: HashMap<u16, u32> = entries.into_iter().collect();
            let newer: Vec<Update> = membership
                .full_state()
                .into_iter()
                .filter(|u| peer.get(&u.id).map(|&pi| u.inc > pi).unwrap_or(true))
                .collect();
            if newer.is_empty() {
                None
            } else {
                Some(MembershipMsg::Gossip(newer))
            }
        }
    }
}

/// Build the digest of what we currently know (for the anti-entropy timer).
pub fn build_digest(membership: &Membership) -> MembershipMsg {
    let entries = membership
        .full_state()
        .into_iter()
        .map(|u| (u.id, u.inc))
        .collect();
    MembershipMsg::Digest(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn up(id: u16, state: MState, inc: u32) -> Update {
        Update {
            id,
            addr: addr(9000 + id),
            state,
            inc,
        }
    }

    #[test]
    fn round_trip_every_message() {
        let msgs = [
            MembershipMsg::Sync {
                reply: true,
                updates: vec![up(1, MState::Alive, 3), up(2, MState::Suspect, 5)],
            },
            MembershipMsg::Sync {
                reply: false,
                updates: vec![],
            },
            MembershipMsg::Gossip(vec![up(9, MState::Dead, 2)]),
            MembershipMsg::Digest(vec![(1, 3), (2, 5), (9, 2)]),
        ];
        for m in msgs {
            assert_eq!(MembershipMsg::decode(&m.encode()), Some(m));
        }
    }

    #[test]
    fn sync_request_merges_and_replies_with_full_state() {
        let mut m = Membership::new(1, addr(1));
        let resp = apply(
            MembershipMsg::Sync {
                reply: true,
                updates: vec![up(2, MState::Alive, 1)],
            },
            &mut m,
            8,
        );
        // merged the pushed peer
        assert!(m.table().iter().any(|e| e.id == 2));
        // and replied with our full state (which now includes 2 and self)
        match resp {
            Some(MembershipMsg::Sync {
                reply: false,
                updates,
            }) => {
                assert!(updates.iter().any(|u| u.id == 1));
                assert!(updates.iter().any(|u| u.id == 2));
            }
            other => panic!("expected a Sync reply, got {other:?}"),
        }
    }

    #[test]
    fn push_pull_triggers_self_refutation() {
        // The exact heal: a peer pushes "you (node 2) are dead @5"; node 2 must
        // refute in the reply it sends back (spike fix #2, at the sync layer).
        let mut m = Membership::new(2, addr(2));
        let base = m.self_incarnation();
        let resp = apply(
            MembershipMsg::Sync {
                reply: true,
                updates: vec![Update {
                    id: 2,
                    addr: addr(2),
                    state: MState::Dead,
                    inc: base,
                }],
            },
            &mut m,
            8,
        );
        assert!(m.self_incarnation() > base, "must refute");
        // The reply carries our refuted (Alive, higher inc) self.
        if let Some(MembershipMsg::Sync { updates, .. }) = resp {
            let me = updates.iter().find(|u| u.id == 2).unwrap();
            assert_eq!(me.state, MState::Alive);
            assert!(me.inc > base);
        } else {
            panic!("expected reply");
        }
    }

    #[test]
    fn digest_answers_with_newer_updates_only() {
        let mut m = Membership::new(1, addr(1));
        m.merge(&up(2, MState::Alive, 5), 8);
        m.merge(&up(3, MState::Alive, 2), 8);
        // Peer's digest: it has 2@5 (same) and does not know 3.
        let resp = apply(MembershipMsg::Digest(vec![(2, 5)]), &mut m, 8);
        match resp {
            Some(MembershipMsg::Gossip(ups)) => {
                assert!(ups.iter().any(|u| u.id == 3), "3 is missing on the peer");
                assert!(
                    !ups.iter().any(|u| u.id == 2),
                    "2 is up to date on the peer"
                );
            }
            other => panic!("expected gossip of the missing entry, got {other:?}"),
        }
    }
}
