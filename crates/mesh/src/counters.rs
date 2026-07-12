//! Per-link counters (Rule 5: every drop counted, every link observable).
//!
//! These are the link-layer half of the `cluster.peers[]` stats shape §4.6
//! specifies (`{ nodeId, state, pressure, relayDrops, bytesIn/Out,
//! msgsIn/Out }`). The queue owns `pressure` and `relayDrops` (they are
//! properties of the bounded queue — [`crate::queue::PeerQueue`]); everything
//! else is here. 3D stitches both into the engine's `stats()`; 3A just makes
//! them exist and correct, because an invisible drop is a Rule 5 violation.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free per-link counters. One instance per link, read by diagnostics,
/// written by the link's reader/writer/handshake — never on any JS path (there
/// is no JS in this crate).
#[derive(Debug, Default)]
pub struct LinkCounters {
    /// Wire bytes received (including frame headers).
    pub bytes_in: AtomicU64,
    /// Wire bytes written.
    pub bytes_out: AtomicU64,
    /// Frames received and dispatched (known kinds).
    pub frames_in: AtomicU64,
    /// Frames handed to the writer.
    pub frames_out: AtomicU64,
    /// **`unknownFrames`** (§4.4): frames whose kind is outside the catalog,
    /// counted and skipped. Under sender suppression this reads **zero**; a
    /// nonzero value is a bug detector (a misbehaving or mis-negotiated peer),
    /// not a compatibility mechanism.
    pub unknown_frames: AtomicU64,
    /// **`authFailures`** (§4.7): handshakes refused because a peer's AUTH MAC
    /// did not verify (wrong secret, tampered transcript, or a reflected AUTH).
    pub auth_failures: AtomicU64,
    /// Handshakes closed because AUTH was not reached within `auth_timeout`.
    pub auth_timeouts: AtomicU64,
    /// **Sender-suppression trips (§4.4):** the mesh's own code attempted to
    /// emit a kind/feature the peer did not advertise. This is a *defense*
    /// counter — the emit is refused (never a wire write) and, in a debug
    /// build, also a `debug_assert`. In correct operation this is zero.
    pub suppressed_emits: AtomicU64,
    /// Idle-liveness PINGs sent.
    pub pings_sent: AtomicU64,
    /// PONGs received.
    pub pongs_recv: AtomicU64,
    /// Links closed because a length prefix exceeded the negotiated max frame
    /// (§4.4). The close is the whole response — there is no resync, so this is
    /// counted once, at the close, per link.
    pub oversize_closes: AtomicU64,
}

impl LinkCounters {
    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// A plain-data snapshot for tests and (eventually) `stats()`.
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            bytes_in: Self::get(&self.bytes_in),
            bytes_out: Self::get(&self.bytes_out),
            frames_in: Self::get(&self.frames_in),
            frames_out: Self::get(&self.frames_out),
            unknown_frames: Self::get(&self.unknown_frames),
            auth_failures: Self::get(&self.auth_failures),
            auth_timeouts: Self::get(&self.auth_timeouts),
            suppressed_emits: Self::get(&self.suppressed_emits),
            pings_sent: Self::get(&self.pings_sent),
            pongs_recv: Self::get(&self.pongs_recv),
            oversize_closes: Self::get(&self.oversize_closes),
        }
    }
}

/// A point-in-time copy of [`LinkCounters`]. Cheap to clone, `PartialEq` for
/// test assertions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CounterSnapshot {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub frames_in: u64,
    pub frames_out: u64,
    pub unknown_frames: u64,
    pub auth_failures: u64,
    pub auth_timeouts: u64,
    pub suppressed_emits: u64,
    pub pings_sent: u64,
    pub pongs_recv: u64,
    pub oversize_closes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_snapshot_reflects_writes() {
        let c = LinkCounters::default();
        LinkCounters::add(&c.frames_in, 3);
        LinkCounters::add(&c.unknown_frames, 1);
        let s = c.snapshot();
        assert_eq!(s.frames_in, 3);
        assert_eq!(s.unknown_frames, 1);
        assert_eq!(s.auth_failures, 0);
    }
}
