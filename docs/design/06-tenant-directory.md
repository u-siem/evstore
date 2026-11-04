# Tenant directory

- **Directory record** in the instance's local directory (~100–200 B, **nothing derived from event content**): id, state, role (`Writable` or `ReadOnlyReplica`), writer generation, location (path/tier), size, created/sealed time, durability class, group/policy, key id.
- **Private manifest** inside tenant storage: segments, statistics, private catalog, field registry, catalog references.
- **Layout**: `tenants/<2-hex fan-out>/<uuid>/` while active. A **sealed tenant = 1 self-contained file**.
- **Store**: an embedded ordered KV store (`redb`-style B-tree: pure Rust, ACID, all three OSes) behind a trait. Secondary indexes `(state, id)` and `(policy, sealed_time, id)`. ~2 GB per 10 M tenants on disk, with only hot pages in RAM.
- **Idle cost**: 1 file + 1 record, 0 RAM, 0 handles, 0 timers. Queries open tenants through a **bounded LRU**.
- **State machine** with the directory as the commit point: `Creating → Active → Sealing → Sealed → (Moving) → Deleting → removed`, plus `Incomplete`. The pattern is always "write and fsync files → commit state → clean up".
- **Startup recovery** visits only in-flight tenants, using the state index.
- **Orphan sweep**: a slow background pass.
- **Retention** deletes expired tenants in throttled batches (one unlink + one record each).
- **Tenant groups** (customer/project) carry default retention, quotas and durability class. Groups never enable cross-tenant queries.
- **Crypto-shredding**: optional per group, off by default.
