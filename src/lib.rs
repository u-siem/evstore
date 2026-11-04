//! # usiem-evstore
//!
//! Embedded, schema-aware, multi-tenant event store **library** for uSIEM.
//!
//! **Status: MVP + M3.** A single-tenant store per directory is
//! implemented: a write-ahead log, sealed columnar segments (per-field
//! dictionaries, postings), a generation-numbered manifest, retention,
//! compaction, a scan query with pruning and aggregations (see [`Store`]).
//! The rest of the design (catalog, tiering, integrity, replication, the
//! multi-tenant directory) lives in `docs/`
//! (start at `docs/README.md`); the modules that implement nothing yet keep
//! only their design notes. Every MVP cut is recorded in
//! `docs/decisions.md`.
//!
//! # Architecture
//!
//! - **A library, not a database server** ("distributed SQLite"): one
//!   instance owns its tenants and files. No cluster membership, consensus,
//!   leader election or cross-instance queries. Fleet concerns belong to the
//!   application; the library offers a pluggable replication channel
//!   ([`replication`]).
//! - **Tenants are physically separate**: no file, WAL or buffer ever holds
//!   bytes of two tenants, and every query targets exactly one tenant.
//! - **Schema knowledge drives performance**: typed encodings, templates,
//!   enums, pre-trained compression dictionaries and schema-chosen indexes
//!   come from a shared, content-addressed [`catalog`].
//!
//! Module map:
//!
//! | Module | Responsibility |
//! |---|---|
//! | [`catalog`] | Shared schema catalog and per-tenant private catalog |
//! | [`directory`] | Local tenant directory, states and lifecycle (MVP: the manifest) |
//! | [`wal`] | Per-tenant durability, WAL pool, coordinated group sync |
//! | [`segment`] | Segment files: adaptive schema groups, sealed files |
//! | [`index`] | Term, range and text indexes; statistics |
//! | [`query`] | Single-tenant query execution and aggregations |
//! | [`retention`] | Retention, quotas and disk-pressure handling |
//! | [`tiering`] | Hot/warm/cold/frozen storage tiers |
//! | [`integrity`] | Tamper evidence: Merkle roots, hash chains, checkpoints |
//! | [`replication`] | Replication records, sinks and replica apply |
//! | [`registry`] | Dynamic field registry (typed identities, cap, overflow) |
//! | [`aggregate`] | Grouped counts, sums, min/max over one store |

pub mod aggregate;
pub mod catalog;
mod codec;
pub mod directory;
mod error;
pub mod index;
pub mod integrity;
mod model;
pub mod query;
pub mod registry;
pub mod replication;
pub mod retention;
pub mod segment;
mod store;
pub mod tiering;
pub mod wal;

#[cfg(test)]
pub(crate) mod test_util;

pub use aggregate::{AggQuery, AggRow, GroupBy, Metric, MetricValue};
pub use error::{Result, StoreError};
pub use model::{Record, Value, ValueType};
pub use query::{ANY_FIELD, FieldOp, Filter, Order, Query, RecordFilter, RecordIter, TimeRange};
pub use registry::{FieldId, FieldInfo, FieldRegistry, IndexMode, OVERFLOW_FIELD};
pub use retention::RetentionPolicy;
pub use store::{Ack, MaintainReport, Options, Recovery, Stats, Store, now_ns};
pub use wal::SyncPolicy;

/// Crash-injection hook for the crash tests (feature `failpoints`).
///
/// When the environment variable `EVSTORE_CRASH_AT` equals `name`, runs
/// `before` (to leave a partial write behind) and aborts the process.
#[cfg(feature = "failpoints")]
pub(crate) fn failpoint(name: &str, before: impl FnOnce()) {
    if std::env::var("EVSTORE_CRASH_AT").is_ok_and(|v| v == name) {
        before();
        std::process::abort();
    }
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
pub(crate) fn failpoint(_name: &str, _before: impl FnOnce()) {}
