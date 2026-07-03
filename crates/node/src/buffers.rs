//! Buffer strategy at the FFI boundary.
//!
//! Inbound: external-backed Buffers (zero-copy) ABOVE the crossover
//! threshold; copy below it. The threshold is a constant here, cited from
//! the RFC 0001 spike (hypothesis was 1–4 KB — use the measured number).
//!
//! Outbound: one copy JS→Bytes at the boundary (unavoidable; Rust cannot
//! safely hold GC-managed memory across await points).

/// Copy-vs-external crossover, measured in the RFC 0001 spike.
///
/// Benchmark: `docs/rfcs/0001-results.md` §"Copy vs external buffers". For the
/// winning design's per-flush buffer, an external (zero-copy) buffer amortizes
/// its single GC finalizer over the whole batch. Measured (no-op, at ceiling):
/// external and copy tie at a ~20 KB flush buffer, and external pulls clearly
/// ahead by ~135 KB (512 B × 256). Below the threshold the per-buffer finalizer
/// cost outweighs the saved copy, so copying into V8 is cheaper.
///
/// The hypothesis in RFC §2 Q3 was 1–4 KB; the *measured* number is 16 KB.
/// Design C's flush buffers are `batch × payload` bytes and thus almost always
/// exceed this, so C uses external buffers in practice; individual messages are
/// then handed to app handlers as zero-copy subarray views (no per-message
/// allocation at all — the reason C has the lowest GC pressure of any design).
pub const EXTERNAL_BUFFER_THRESHOLD: usize = 16 * 1024;

/// Whether a buffer of `len` bytes handed to V8 should be external-backed
/// (zero-copy, one finalizer) rather than copied into V8-managed memory.
#[inline]
pub const fn should_externalize(len: usize) -> bool {
    len >= EXTERNAL_BUFFER_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crossover_threshold_matches_spike() {
        // Below threshold → copy into V8 (cheaper than a finalizer per buffer).
        assert!(!should_externalize(64));
        assert!(!should_externalize(512));
        assert!(!should_externalize(4 * 1024));
        assert!(!should_externalize(EXTERNAL_BUFFER_THRESHOLD - 1));
        // At/above → external (zero-copy) wins, per 0001-results.md.
        assert!(should_externalize(EXTERNAL_BUFFER_THRESHOLD));
        assert!(should_externalize(64 * 1024));
        // A typical design-C flush buffer (256 × 512 B) is well above threshold.
        assert!(should_externalize(256 * 512));
    }
}
