//! Buffer strategy at the FFI boundary.
//!
//! Inbound: external-backed Buffers (zero-copy) ABOVE the crossover
//! threshold; copy below it. The threshold is a constant here, cited from
//! the RFC 0001 spike (hypothesis was 1–4 KB — use the measured number).
//!
//! Outbound: one copy JS→Bytes at the boundary (unavoidable; Rust cannot
//! safely hold GC-managed memory across await points).
