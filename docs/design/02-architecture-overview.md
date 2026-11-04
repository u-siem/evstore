# Architecture overview


```
 ┌──────────────────────── library instance (inside a usiem process) ─────────────────────┐
 │                                                                                        │
 │  Shared (no tenant data)                 Per tenant (physically separate)              │
 │  ─────────────────────────               ─────────────────────────────────────         │
 │  Schema catalog (content-addressed,      Tenant T:                                     │
 │   signed bundles, dictionaries)            WAL file (pooled, crypto-erased)            │
 │  Tenant directory (local KV)               hot memtable (only if hot)                  │
 │  Sync coordinator (group fsync rounds)     segments: schema groups + indexes           │
 │  Memory budget, handle/tenant LRUs         manifest (stats, private catalog,           │
 │  Query engine (always 1 tenant scope)       field registry, sink cursors)              │
 │  Replication dispatcher ──┐              Sealed tenant = ONE self-contained file       │
 └───────────────────────────┼────────────────────────────────────────────────────────────┘
                             ▼  per-tenant, ordered replication records
          ┌──────────────── ReplicationSink trait (pluggable, app-provided) ───────────┐
          │  peer usiem instance (apply_replica) │ Kafka/NATS │ object store │ mirror │
          └────────────────────────────────────────────────────────────────────────────┘
   Fleet concerns (placement, failover decisions, fleet-wide search) live in usiem, outside the library.
```

Write path: `usiem event (typed) → tenant WAL (or SealOnly buffer) → [hot: memtable] → segment (schema groups) → merges / seal → tiering → retention`.
Read path: `tenant lookup → open tenant (LRU) → per-segment plan → schema-group pruning → indexes → chunk pruning → residual scan → late materialization`.
