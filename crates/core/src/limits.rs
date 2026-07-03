//! Admission control — Phase 1C. Enforced BEFORE any JS runs.
//!
//! - max_connections_per_ip / max_payload_bytes / max_rooms_per_connection
//! - trust_proxy: Never | Always | Cidrs — with Cidrs, honor X-Forwarded-For
//!   only when the socket peer is inside the list. Security boundary:
//!   spoofed XFF must be a tested case (ENGINEERING.md §7).
//!
//! Rule 3: every limit here gets tested in direct AND simulated-proxy
//! topologies.
