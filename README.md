# usiem-evstore

An embedded, schema-aware, multi-tenant **event store library** for [uSIEM](https://github.com/u-siem/usiem).

> **Status: MVP + M3.** A single-tenant store per directory works: write-ahead log, columnar segments with per-field dictionaries and postings, manifest, retention, compaction, filtered scans and aggregations (`Store`). It serves queries through usiem-query. The multi-tenant architecture in [`docs/`](docs/README.md) is still the target; every MVP cut is listed in [docs/decisions.md](docs/decisions.md). Measured cost: [spike 05](docs/spikes/05-r6-segment-build.md), [spike 06](docs/spikes/06-m3-dictionaries-postings-aggregations.md).

## What it is

- **A library, like SQLite.** It is linked into a uSIEM process and owns its tenants and files. It has no cluster, no consensus and no leader election. Fleet concerns (placement, failover decisions, fleet-wide search) belong to the application.
- **Schema-aware.** uSIEM knows the schema of every event. The store turns that into compact typed encodings, templates, enums, pre-trained compression dictionaries and schema-chosen indexes, aiming to beat generic stores (Elasticsearch/OpenSearch, Quickwit, ClickHouse) on density, ingest cost and query speed.
- **Multi-tenant with physical isolation.** Thousands of streaming tenants (SIEM customers) and millions of short-lived sealed tenants (for example one malware sandbox run each). No file, WAL or buffer ever mixes tenants, and every query targets exactly one tenant.
- **Replication through a trait.** Every committed write can be streamed to an application-provided `ReplicationSink` (another instance, Kafka/NATS, an object-store archive). Another instance can hold read-only replicas.
- **Multiplatform**: Windows, Linux and macOS.

## Documentation

Start with [docs/README.md](docs/README.md). The decisions taken so far are listed in [docs/decisions.md](docs/decisions.md).

## Building

```
cargo build
cargo test
cargo test --features failpoints --test crash   # crash recovery, real process aborts
cargo fmt
cargo clippy --all-features --all-targets
```

## Using it

```rust
use usiem_evstore::{FieldOp, Filter, Options, Order, Query, Record, Store, Value};

let mut store = Store::open("data/events", Options::default())?;
store.append(vec![Record::new(ts).with("host", Value::Str("nas".into()))])?;
let newest = Query {
    filter: Filter::field("host", FieldOp::Eq(Value::Str("NAS".into()))),
    order: Order::Desc,
    limit: Some(100),
    ..Query::default()
};
for record in store.search(&newest)? { /* ... */ }
store.maintain(usiem_evstore::now_ns())?; // call every second or so
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
