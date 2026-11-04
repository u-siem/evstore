//! Storage tiers.
//!
//! Design: `docs/design/08-components.md` (tiering).
//!
//! - Hot/warm on local directories; cold/frozen on object storage through a
//!   storage-backend trait.
//! - A sealed tenant moves as one object; a local skeleton lets queries
//!   prune without remote I/O; frozen strips postings and FSTs.
//! - Moves are copy → verify → commit → deferred delete.
