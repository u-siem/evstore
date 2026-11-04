//! Tamper evidence.
//!
//! Design: `docs/design/08-components.md` (tamper evidence).
//!
//! - Hash the content, not the file: canonical event encoding with a
//!   per-tenant sequence number.
//! - Merkle root per flush/seal and a hash chain per tenant; retention
//!   removals are recorded in the chain.
//! - Ed25519 signed checkpoints through a key-provider trait, and a
//!   checkpoint sink trait for external anchors.
