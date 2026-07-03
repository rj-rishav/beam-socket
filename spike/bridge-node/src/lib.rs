//! The bridge under test. Designs A/B/C selectable at start.
//! Spec: RFC 0001 §3. Build order: B first (predicted winner), then A, then C.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Design {
    /// One TSFN call per event. Baseline — expected to lose.
    NaiveTsfn,
    /// TSFN per flush; events as a JS array of objects. Flush at N or timer.
    BatchedObjects { batch: usize, timer_ms: u64 },
    /// TSFN per flush; events encoded into one contiguous Buffer,
    /// decoded by a JS cursor reader.
    BatchedFlat { batch: usize, timer_ms: u64 },
}

// TODO(Phase 0, step 2 — ENGINEERING.md §4):
// #[napi] start(design, generator_config, callback) →
//   - spawn generator (bridge-core)
//   - drain its bounded queue per the selected design
//   - expose a `pressure()` counter #[napi] getter (drops + queue depth)
// Batch parameter sweep comes from RFC 0001 §4: N ∈ {64, 256, 1024},
// timer ∈ {0.25, 1, 4} ms.
