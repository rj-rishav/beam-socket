//! User → connections index — Phase 1C.
//!
//! Sharded DashMap<UserId, HashSet<ConnectionId>> (Rule 2: toUser is a hot
//! path, no global lock). Bind at authorize-accept, unbind at disconnect.
//!
//! Memory cost: one index entry per connection (~24–40 B) — measured and
//! published, per Rule 4.
//!
//! Required leak test: 10k connect/disconnect churn → index empty, RSS flat.
