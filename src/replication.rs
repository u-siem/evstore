//! Replication channel.
//!
//! Design: `docs/design/07-replication-channel.md`.
//!
//! - Every committed change of a tenant becomes a replication record,
//!   delivered in commit order per tenant; a record never mixes tenants.
//! - `ReplicationSink` is a trait implemented by the application (peer
//!   instance, Kafka/NATS, object-store archiver, ...); no implementation is
//!   bundled in this crate.
//! - At-least-once delivery, idempotent `apply_replica` by `(tenant, seq)`,
//!   persisted cursors per sink and tenant.
//! - Lag policy: retain un-acked WAL up to a budget, then detach and resync
//!   from a snapshot. Asynchronous by default; `SinkAck` makes writes wait.
//! - Writer generation per tenant for fencing on application-driven
//!   promotion. The library never elects a writer.
