//! Rust → JS event bridge. THE component RFC 0001 exists to validate.
//!
//! Graduation rules (do not violate):
//! - The design and its constants (batch size, flush timer) come from
//!   docs/rfcs/0001-results.md. Cite the result in a comment next to each
//!   constant. Constants without citations get the PR rejected.
//! - The engine↔bridge queue is bounded; overflow increments bridge_pressure
//!   and applies the documented policy. Never silent.
