# Decisions log

| Decision | Choice |
|---|---|
| Tenant isolation | Physical separation of everything; no cross-tenant queries |
| Tenant layout | Adaptive schema groups; sealed tenant = one self-contained file |
| Compression | Templates and schema-grouped columns are the main levers. **Pre-trained dictionaries are secondary and come after v1** (measured −7–13%, [spike 02](spikes/02-compression-by-tenant-size.md)); the format keeps dictionary references from day one |
| Durability | Class per tenant; `SealOnly` default for sandbox, `Wal(interval)` for streaming; coordinated group sync |
| Buffers | WAL-as-buffer for small/cold tenants; memtables only for hot tenants under a global budget |
| WAL file reuse | Preallocated pool with crypto-erase |
| Catalog | Content-addressed, signed bundles, never deleted; private catalog + field registry per tenant |
| Schema evolution | Stable ids and codes, template revisions, new identity for incompatible changes; no data rewrite |
| Directory | Routing record (no content) + private manifest; embedded B-tree KV; state machine as commit point |
| Groups | Tenant groups for policies and quotas only |
| Crypto-shredding | Optional per group, off by default |
| Deployment model | **Library ("distributed SQLite")**: no cluster, controller, consensus or leader election; fleet placement, failover decisions and fleet search belong to usiem |
| Replication | Pluggable `ReplicationSink` trait: per-tenant ordered records (WAL bytes), at-least-once + idempotent `apply_replica`, per-sink cursors, lag policy `Block/Retain/Detach`, snapshot resync, sealed-file fast path; async by default, `SinkAck` opt-in |
| Fencing | Writer generation per tenant; app-driven `promote`; older generations rejected |
| Sink lag | Default `Retain` (un-acked WAL kept up to a budget), then `Detach` + snapshot resync; local writes never blocked by default |
| Replica segments | Streaming-tenant replicas rebuild segments from records; sealed tenants ship as one file |
| Windows sync (R3) | **Measured 2026-10-24 ([spike 01](spikes/01-fsync-rounds-windows.md))**: per-file `FlushFileBuffers` takes 217–295 ms for 1k tenants, so the rule triggered. Windows rounds use **buffered write-through writes issued by the coordinator (8–32 threads) + one `FlushFileBuffers` per round** (8–16 ms for 1k tenants). Unbuffered I/O is optional only; write-through without the final flush is never trusted |
| Tiny tenants (R1) | Adopted: one release-bundle hash per segment, compact footer; domain dictionaries after v1. **Acceptance: a tenant of N events must be ≥ 3× smaller than a generic doc store at the same N** (measured 3–4× at 100–1k events, [spike 02](spikes/02-compression-by-tenant-size.md)) |
| Unique text (R2) | Adopted: structured string types (`Path`, `CommandLine`, `Url`, …) split into dictionary-coded parts + type tokenizers |
| Aggregation cache (R4) | Adopted: per-tenant cache of partial aggregates keyed by immutable segment id |
| Schemaless (R5) | Deferred until the schemaless share is measured; format keeps room for tenant-private inferred schemas; no template mining |
| Network | Up to the sink implementation; the library only guarantees records never mix tenants (optional per-tenant payload encryption) |
| Tamper evidence | Merkle + chain per tenant, Ed25519 checkpoints, checkpoint sink; FSS reserved |
| Full disk | Backpressure; never circular overwrite |
| Packing threshold | Cost-based per file using catalog compression profiles; **fallback ~100 events per schema** (measured best, [spike 02](spikes/02-compression-by-tenant-size.md)); hysteresis |
| Text indexes | Mode declared by the schema; none on small segments; opt-in trigram for `substring` fields |
| Segment limits | 256 MB–2 GB target, ~4 GB cap; huge tenants split by the application |
| Failure behaviour | Local recovery + replication semantics; [§8.4](design/09-design-details.md) is the contract |
| Aggregations | Tiered (metadata → code histograms → codes → vectorized); own kernels, Arrow-compatible blocks |
| WAL encryption | AES-256-GCM via **`ring`**, behind an internal trait (swappable, e.g. `aws-lc-rs` for FIPS); fresh key per WAL life, counter nonce. Measured ~10 GB/s, 1–4% of the ingest budget ([spike 03](spikes/03-wal-encryption.md)) |
| WAL compression | **WAL appends stay uncompressed** (schema-encoded); compression only at segment build. zstd costs 2–3 µs per small append ([spike 03](spikes/03-wal-encryption.md)) |
| WAL → segment | Convert at **~8 MiB of WAL per tenant** (configurable), at seal, or when a **global WAL budget** (default ~25% of RAM) is exceeded, largest WALs first. WAL scans measured ~0.4 ms per 1k events ([spike 04](spikes/04-wal-vs-segment.md)) |
| Segment build cost (R6) | New risk: prototype measured 10–16 µs/event; acceptance ≤ ~2 µs/event amortized; dedicated spike once the typed row format exists ([spike 04](spikes/04-wal-vs-segment.md)) |

## MVP (2026-11-04)

The first implementation is a single-tenant store for a homelab-scale SIEM. Each row is a deliberate cut or a choice the design left open; the design rows above still describe v1.

| Decision | Choice |
|---|---|
| MVP scope | One `Store` per directory = one tenant. No tenant directory or embedded KV store; the manifest is the commit point |
| MVP cuts | Catalog, templates and schema groups, structured string types, WAL encryption and file pool, durability classes, memtables, integrity, replication and fencing, tiering, indexes, aggregations: all deferred |
| WAL format | `wal-<gen>.log`: 16-byte header (magic, version, flags with bit 0 reserved for encryption, generation), then one `[u32 len][u32 crc32][payload]` frame per `append` call (batches are atomic). Replay truncates at the first bad or short frame |
| Sync | `SyncPolicy::Interval(1 s)` by default (`Always` optional); one store fsyncs its own file. Coordinated group sync is unneeded at 2–3 stores per process |
| Segment format | One columnar group per file; 4,096-row chunks; delta-coded timestamps; per-row value counts (multi-valued fields); one string dictionary per segment, sorted case-insensitively; one zstd frame per column per chunk; per-chunk zone maps; footer with CRC32 per section; read through `memmap2` |
| Manifest | `manifest-<generation>` files written new and fsynced, never renamed over; the newest valid one wins at open; the two newest are kept |
| Field registry | First type keeps the name; a conflicting type becomes `name#<type>`; cap of 2,000 identities, then values go to the reserved `@overflow` field (scan only) |
| Threads | None inside the store: the application calls `maintain(now)` (WAL flush when due, retention, compaction, deferred deletes) |
| Partitions and retention | Ingest-day partitions (not volume-adaptive); retention drops whole segments once `max(max_age, min_age)` has passed; size-tiered compaction inside a day (fanout 8, levels 0–3), zstd 3 at flush, 9 at merge |
| Windows file deletion | Segments are mmapped; files that cannot be deleted yet are retried at each `maintain`; unreferenced files are removed at open |
| Filter semantics | A leaf matches if any value matches; missing fields never match; strings compare case-insensitively; numbers compare across integer and float types (IEEE, so `-0.0 == 0.0`) |
| R6 (MVP) | Accepted for the homelab profile: 6.5 µs/event ingest + flush, about 16 µs/event background compaction ([spike 05](spikes/05-r6-segment-build.md)). The ~2 µs v1 target stays open: per-field dictionaries, no dictionary for text-scan fields |

## M3 (2026-11-04)

| Decision | Choice |
|---|---|
| Segment format v2 | One dictionary per string field per segment (replaces the shared dictionary); a field whose distinct values exceed half its values (and 16) is stored **plain** (length-prefixed, no dictionary, no zone map). Format v1 stays readable; compaction rewrites it ([spike 06](spikes/06-m3-dictionaries-postings-aggregations.md)) |
| Postings | Roaring row bitmaps per dictionary code, for dictionary fields of segments with ≥ 10k rows; used only to skip chunks (rows are still evaluated exactly). `roaring` 0.11 without default features (no SIMD proc-macro, keeps the dependency tree shallow). No FST: per-field sorted dictionaries already serve as the term dictionary |
| Aggregations | `Store::aggregate`: group by exact field values (single-valued only) and/or time buckets; count, count(field), sum, min, max. Tiers: chunk metadata for a plain count; dictionary codes as group keys within a segment; otherwise a per-row fold over the pruned chunks. Min/max order across incomparable types by type tag |
| Registry at append | Fields are registered when appended (not at flush), so the registry describes buffered rows too; re-registered on WAL replay |
| Pushdown from usiem-query | `EventSource::aggregate` (default: decline). `EvstoreSource` pushes a leading `stats` only when the answer is provably the executor's: exact filter translation, plain-name scalar group fields, integer-only sums and averages, single-typed non-float min/max, no `dc`/`values`, registry not full. Otherwise the executor streams |
| R6 (M3) | 5.2 µs/event ingest + flush, 12 µs/event background compaction; ~2 µs v1 target still open |
