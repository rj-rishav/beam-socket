//! beamsocket-node — the NAPI-RS binding.
//!
//! This crate is TRANSLATION ONLY. If you are writing logic here, it belongs
//! in beamsocket-core. The #[napi] surface stays flat: functions take
//! primitive IDs, not object graphs (ARCHITECTURE.md §2.2).
//!
//! Populated at Phase 0 graduation — the winning bridge design from
//! spike/bridge-node moves into bridge.rs with its measured constants.

pub mod bridge;
pub mod buffers;

/// The #[napi] surface. Only exists under `--features napi` (the addon
/// build); without it this crate is a plain rlib so `cargo test --workspace`
/// stays link-clean (CI never resolves napi_* symbols outside Node).
#[cfg(feature = "napi")]
mod binding;
