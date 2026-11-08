//! # usiem-evstore
//!
//! An embedded, schema-aware event store **library** for uSIEM: it is
//! linked into the application, like SQLite, and owns the files of one
//! directory. It starts no threads; the application calls
//! [`Store::maintain`] periodically.
//!
//! - **Durable ingest.** [`Store::append`] writes a batch to a
//!   checksummed write-ahead log as one atomic frame ([`SyncPolicy`] decides
//!   when it is fsynced). Records are searchable at once.
//! - **Compact columnar segments.** Flushes turn the WAL into immutable,
//!   memory-mapped segment files: time-sorted chunks, zstd-compressed
//!   columns, per-field string dictionaries, zone maps and, for large
//!   segments, Roaring postings.
//! - **Search and aggregations.** [`Store::search`] runs a [`Query`] (time
//!   range, [`Filter`], [`Order`], limit) with segment, chunk and index
//!   pruning; [`Store::aggregate`] groups and counts, sums, or finds minimums
//!   and maximums ([`AggQuery`]).
//! - **Crash safety.** A generation-numbered manifest is the commit point;
//!   open recovers from torn WAL tails, torn manifests and interrupted
//!   flushes or compactions ([`Recovery`]).
//! - **Retention and compaction** by ingest day ([`RetentionPolicy`]).
//!
//! **Status: MVP + M3.** One store directory holds one tenant. The
//! multi-tenant design (catalog, tiering, integrity, replication, the
//! tenant directory) lives in
//! [`docs/`](https://github.com/u-siem/evstore/tree/main/docs); the modules
//! that implement nothing yet keep only their design notes.
//!
//! ## Features
//!
//! | Feature | Adds |
//! |---|---|
//! | `failpoints` | Test-only crash injection (`EVSTORE_CRASH_AT=<name>` aborts the process at that point). Never enable it in production builds. |
//!
//! ## Example
//!
//! ```
//! use usiem_evstore::{FieldOp, Filter, Options, Order, Query, Record, Store, Value};
//!
//! # let dir = std::env::temp_dir().join(format!("evstore-doc-{}", std::process::id()));
//! let mut store = Store::open(&dir, Options::default())?;
//! store.append(vec![
//!     Record::new(1_700_000_000_000_000_000).with("host", Value::Str("nas".into())),
//!     Record::new(1_700_000_001_000_000_000).with("host", Value::Str("web01".into())),
//! ])?;
//! store.flush()?;
//!
//! // Strings compare case-insensitively.
//! let newest = Query {
//!     filter: Filter::field("host", FieldOp::Eq(Value::Str("NAS".into()))),
//!     order: Order::Desc,
//!     limit: Some(100),
//!     ..Query::default()
//! };
//! let hits: Vec<Record> = store.search(&newest)?.collect::<Result<_, _>>()?;
//! assert_eq!(hits.len(), 1);
//!
//! store.maintain(usiem_evstore::now_ns())?; // call every second or so
//! # drop(store);
//! # std::fs::remove_dir_all(&dir)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Architecture
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

#![cfg_attr(docsrs, feature(doc_cfg))]
// Numbers of different types compare, and sums accumulate, as `f64` by
// design; the precision loss past 2^53 is documented behaviour.
#![allow(clippy::cast_precision_loss)]
// Test data generators narrow random `u64`s on purpose.
#![cfg_attr(
    test,
    allow(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss
    )
)]

pub mod aggregate;
pub mod catalog;
mod codec;
pub mod directory;
mod error;
#[doc(hidden)]
pub mod fuzz_api;
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
