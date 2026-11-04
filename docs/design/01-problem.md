# Problem definition


## 1.1 What we are building
An **embedded event store library for SIEMs**: a Rust library linked into usiem, running on Windows, Linux and macOS. It stores security events and serves SIEM queries over them.

**Library, not database server — think "distributed SQLite"**:
- One library instance owns its tenants and its files. There is **no cluster membership, no consensus, no leader election, no cross-instance query engine** inside the library; those are too expensive for a library.
- **Fleet use is the application's job** (usiem decides which instance owns which tenant, when to fail over, how to search across instances). The library provides the primitives that make this easy and safe:
  - a **pluggable replication channel** (a trait) that receives every committed write, per tenant, in order ([§3.5](07-replication-channel.md));
  - an **apply API** so another instance can hold a read-only replica of a tenant;
  - a **writer generation** per tenant for fencing on application-driven promotion;
  - **mergeable partial results** so an external layer can federate searches and aggregations.

## 1.2 Thesis
General-purpose stores (Elasticsearch/OpenSearch, Quickwit/tantivy, ClickHouse) treat events as JSON documents or opaque lines. They rediscover structure at ingest and pay for it in CPU, disk and query time. A SIEM **already knows the schema of every event** (typed parser output, EVTX templates, vendor message catalogs, enums). The engine turns that knowledge into measurably better **storage density, ingest cost and query speed**.

## 1.3 Scale and isolation requirements
- **Thousands of schemas**, evolving across usiem releases; mostly shared, with some custom schemas per tenant.
- **Thousands to millions of tenants**, power-law sized. A tenant is a **general isolation unit**:

  | Shape | Example | Lifetime | Writes | Count |
  |---|---|---|---|---|
  | Streaming | SIEM/MSSP customer | years | continuous | thousands |
  | Burst-then-sealed | **malware sandbox run (UUID)** | minutes of writes, then read-only | bounded, then sealed | thousands/day → millions retained |

  Both shapes may run in the same deployment.
- **Physical separation of everything**: no file, WAL or in-memory buffer ever holds bytes of two tenants. Network connections may be shared, but frames and buffers are per tenant.
- **No cross-tenant queries, ever.** Every query targets exactly one tenant. Cross-run analytics (e.g. IOC lookups across sandbox runs) happen outside the engine, e.g. in a dedicated tenant written by usiem.
- **The tenant is the placement unit**: a tenant lives in exactly one writable library instance (plus any read-only replicas). Placement is decided by the application. The rare tenant too large for one instance is split by the application into several tenants (e.g. per source or time window) and federated outside.
- **Near-zero idle cost** per tenant: no RAM, handles, timers or background work for idle or sealed tenants.

## 1.4 Workload
Write-heavy and append-only; time-bounded queries, mostly over recent data. **Performance drivers**: selective filters (equality, ranges, CIDR), text search (messages, command lines, paths, URLs) and aggregations (count, group-by, top-N). A full ordered dump of a tenant is needed but is not a driver.

## 1.5 Success criteria
Compared against **Elasticsearch/OpenSearch, Quickwit/tantivy and ClickHouse** on the same datasets:
- bytes per event;
- EPS per core;
- query latency;
- idle-tenant cost and cost of adding a schema;
- isolation that holds even if query code has a bug.

## 1.6 Key tensions
| # | Tension | Resolved in |
|---|---|---|
| T1 | Per-tenant WAL and buffers × many active tenants (fsync storm, RAM) | [§3.2](04-durability-and-buffers.md) |
| T2 | Schema advantage × thousands of schemas × small tenants | [§3.1](03-tenant-storage-unit.md) |
| T3 | Millions of retained tenants × near-zero idle cost | [§3.4](06-tenant-directory.md) |
| T4 | Shared, evolving catalog × physically separate, long-lived data | [§3.3](05-schema-catalog.md) |
| T5 | Sealed and streaming tenants in one storage model | [§3.1](03-tenant-storage-unit.md) |
| T6 | Fleet use (replicas, failover, fleet search) × staying a lightweight library | [§3.5](07-replication-channel.md) |
| T7 | The cost of isolation must stay below the schema-driven savings | [§6](11-cost-risks-benchmarks.md) |
