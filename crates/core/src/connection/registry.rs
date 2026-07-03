//! Sharded slab registry — Phase 1A.
//!
//! ConnectionId encodes shard index + generation + slab key: lookup is an
//! array index within a shard, IDs recycle, no hashing on the hot path, and
//! no global lock (Rule 2 — 16 shards, each its own mutex; the JS thread and
//! connection tasks contend only within a shard).
//!
//! The per-slot generation counter makes recycled IDs safe: a stale ID's
//! generation no longer matches the slot's, so `get`/`remove` miss instead of
//! addressing the wrong connection (see ids.rs).
//!
//! Memory cost per connection (Rule 4): one slab `Entry` = generation (4 B) +
//! `ConnHandle` (Mailbox Arc 8 B + control Sender 8 B + CloseSignal Arc 8 B)
//! ≈ 28 B in-slab, plus one u32 in the generation side-table and the pointed-to
//! per-connection structures themselves (mailbox, channels — counted in the
//! connection-task budget, see PR notes).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use slab::Slab;

use crate::connection::ConnHandle;
use crate::ids::{ConnectionId, GENERATION_MASK};

/// Power of two; shard = round-robin insert counter & (SHARDS-1).
pub const SHARDS: usize = 16;

struct Shard {
    slab: Slab<Entry>,
    /// Per-slot generation, persists across slab remove/insert so a recycled
    /// slot gets a fresh generation. Indexed by slab key.
    gens: Vec<u32>,
}

struct Entry {
    generation: u32,
    handle: ConnHandle,
}

pub struct Registry {
    shards: Box<[Mutex<Shard>]>,
    next: AtomicUsize,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        let shards = (0..SHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    slab: Slab::new(),
                    gens: Vec::new(),
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            next: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, handle: ConnHandle) -> ConnectionId {
        let shard_idx = self.next.fetch_add(1, Ordering::Relaxed) & (SHARDS - 1);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        let entry = shard.slab.vacant_entry();
        let key = entry.key();
        entry.insert(Entry {
            generation: 0, // fixed up below once we know the slot's generation
            handle,
        });
        let generation = if key < shard.gens.len() {
            // Recycled slot: bump so stale IDs miss.
            shard.gens[key] = (shard.gens[key] + 1) & GENERATION_MASK;
            shard.gens[key]
        } else {
            debug_assert_eq!(key, shard.gens.len());
            shard.gens.push(0);
            0
        };
        shard.slab[key].generation = generation;
        ConnectionId::new(shard_idx as u8, generation, key as u32)
    }

    pub fn get(&self, id: ConnectionId) -> Option<ConnHandle> {
        let shard = self.shards.get(id.shard() as usize)?.lock().unwrap();
        let entry = shard.slab.get(id.key() as usize)?;
        (entry.generation == id.generation()).then(|| entry.handle.clone())
    }

    pub fn remove(&self, id: ConnectionId) -> Option<ConnHandle> {
        let mut shard = self.shards.get(id.shard() as usize)?.lock().unwrap();
        let entry = shard.slab.get(id.key() as usize)?;
        if entry.generation != id.generation() {
            return None;
        }
        Some(shard.slab.remove(id.key() as usize).handle)
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().slab.len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all live handles (shutdown sweep). Collected under the
    /// shard locks, ACTED on outside them.
    pub fn handles(&self) -> Vec<(ConnectionId, ConnHandle)> {
        let mut out = Vec::new();
        for (si, shard) in self.shards.iter().enumerate() {
            let shard = shard.lock().unwrap();
            for (key, entry) in shard.slab.iter() {
                out.push((
                    ConnectionId::new(si as u8, entry.generation, key as u32),
                    entry.handle.clone(),
                ));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackpressurePolicy;
    use crate::connection::backpressure::Mailbox;
    use crate::connection::{CloseSignal, Control};
    use crate::metrics::Metrics;
    use std::sync::Arc;

    fn handle() -> ConnHandle {
        let metrics = Arc::new(Metrics::default());
        let (control, _rx) = tokio::sync::mpsc::channel::<Control>(4);
        let (close, _close_rx) = CloseSignal::new();
        ConnHandle {
            mailbox: Mailbox::new(1024, BackpressurePolicy::DropNewest, metrics),
            control,
            close,
        }
    }

    #[test]
    fn insert_get_remove() {
        let r = Registry::new();
        let id = r.insert(handle());
        assert!(r.get(id).is_some());
        assert_eq!(r.len(), 1);
        assert!(r.remove(id).is_some());
        assert!(r.get(id).is_none());
        assert!(r.remove(id).is_none(), "double remove must miss");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn recycled_slot_bumps_generation_and_stale_id_misses() {
        let r = Registry::new();
        // Fill one full round-robin cycle so the next insert reuses shard 0.
        let first = r.insert(handle());
        for _ in 0..SHARDS - 1 {
            let id = r.insert(handle());
            r.remove(id);
        }
        r.remove(first);
        let recycled = r.insert(handle()); // same shard 0, same slab key 0
        assert_eq!(recycled.shard(), first.shard());
        assert_eq!(recycled.key(), first.key());
        assert_ne!(recycled.generation(), first.generation());
        assert!(r.get(first).is_none(), "stale ID must miss");
        assert!(r.get(recycled).is_some());
        assert!(r.remove(first).is_none(), "stale remove must miss");
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn concurrent_insert_remove_recycle() {
        let r = Arc::new(Registry::new());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let r = r.clone();
                std::thread::spawn(move || {
                    let mut ids = Vec::new();
                    for i in 0..500 {
                        let id = r.insert(handle());
                        assert!(r.get(id).is_some());
                        ids.push(id);
                        if i % 3 == 0 {
                            let id = ids.swap_remove(ids.len() / 2);
                            assert!(r.remove(id).is_some());
                            assert!(r.get(id).is_none());
                        }
                    }
                    ids
                })
            })
            .collect();
        let mut live = 0;
        for t in threads {
            live += t.join().unwrap().len();
        }
        assert_eq!(r.len(), live);
    }
}
