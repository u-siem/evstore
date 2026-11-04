# Risk mitigations (R1–R6)


> Decided: R1, R2 and R4 adopted; R3 by measurement rule; R5 deferred (see [§7](../decisions.md)).

## R1. Tiny tenants (< ~1k events)
**Cost today**: 1k Sysmon events ≈ 1.2 MB of JSON ≈ ~120 KB stored. Fixed per-file overhead (footer, manifest, catalog references) of ~4–16 KB is 3–13%; at 100 events it can exceed 30%.

**Mitigations**:
- **Reference a release bundle, not individual objects**: a segment stores **one 16 B hash of the catalog release bundle** plus small local indexes into it, with individual hashes only for objects outside that bundle (custom or dynamic schemas). This shrinks the reference table from ~1–3 KB to ~20–100 B. Bundles are content-addressed and never deleted ([§3.3](05-schema-catalog.md)), so this is safe.
- **Compact footer**: varints, no per-column metadata in the packed group, statistics only where they pay off (none below the index threshold of [§8.2](09-design-details.md)).
- **No indexes on small segments** ([§8.2](09-design-details.md)).
- **Domain dictionaries for the packed group**: besides per-schema dictionaries, usiem ships a few **domain dictionaries** (Windows events, network, Linux/syslog) trained on mixed data, used for the packed group's streams.
- **The file-system floor is accepted**: physical separation means ≥ 1 allocation per tenant (4 KB on most file systems; NTFS stores tiny files inside the MFT record). 1 M tiny tenants ≈ 4 GB of slack, which is acceptable.

**Acceptance threshold** (revised after [spike 02](../spikes/02-compression-by-tenant-size.md)): a tenant of N events must be **≥ 3× smaller than a generic doc store at the same N**. Measured: 3.0× at 100 events, 4.1× at 1,000.

*Original threshold, replaced*: ≤ 2× (1k events) and ≤ 4× (100 events) the bytes/event of a large tenant. Measured 2.1–2.5× and 3.1–3.6×; the relative bar mostly measured how well large tenants compress.

*Domain dictionaries* move to after v1 together with per-schema dictionaries.

## R2. High-cardinality text (command lines, paths, URLs)
Whole values are mostly unique, but they are **structured and their parts repeat**. Schema knowledge can exploit that.

**Mitigations**:
- **Structured string types in the schema**: `Path`, `CommandLine`, `Url`, `Domain`, `RegistryKey`, `Email`, `Hash`. At ingest they are **decomposed into sub-columns**, for example:
  - `Path` → directory + file name + extension;
  - `Url` → scheme + host + path + query;
  - `CommandLine` → image + arguments.

  Each part is dictionary-coded, and its cardinality is far lower than the whole value's, so dictionaries become effective and searches on parts ("file name = x.exe", "host = evil.com") become **equality lookups on codes**. Values are reassembled on read.
- **Type-specific tokenizers** for the inverted index (path segments, argument tokens and flags such as `-enc`, URL segments). The token vocabulary stays small even when values are unique, so the index stays compact.
- **Opt-in trigram index** for true substring search ([§8.2](09-design-details.md)), on segments above the threshold.

**Acceptance threshold**: text-query latency within **1.5×** of ES/Quickwit, with **≤ 1/3** of their index size, on queries like `cmdline contains "-enc"`, `path endswith "\temp\x.exe"`, `url.host = ...`.

## R3. Many per-tenant WALs on Windows
**The unknown**: whether parallel `FlushFileBuffers` on many files coalesces device flushes on NVMe. This must be measured first ([§6](11-cost-risks-benchmarks.md) spike).

**Mitigations, by order of preference after the spike**:
1. **Group-sync rounds with parallel `FlushFileBuffers`** (the baseline design) if the spike shows acceptable round times.
2. **Write-through path on Windows**: open WAL files with `FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING` and issue each round's pending bytes per tenant as **sector-aligned overlapped writes (IOCP)**. Each write is durable when it completes (FUA), and no flush calls are needed. Costs: alignment padding for small batches and a per-tenant aligned buffer.
3. **`SinkAck` durability** (replica or archive acknowledges), which allows relaxing local sync to periodic.
4. `SealOnly` remains the default for sandbox runs, which removes most WALs entirely.

**Decision rule**: if a round with 1,000 dirty tenants takes > ~10 ms with option 1 on NVMe, use option 2 on Windows.

**Result (2026-10-24, [spike 01](../spikes/01-fsync-rounds-windows.md))**: option 1 took 295 ms (NVMe) and 217 ms (SATA SSD) for 1,000 tenants, so the rule triggered. Option 2 was refined:
- **buffered** write-through, keeping the page cache for WAL-as-buffer reads;
- **plus one `FlushFileBuffers` per round**, so durability does not depend on FUA support.

This measured 8 ms (NVMe) and 16 ms (SATA SSD) for 1,000 tenants. Unbuffered I/O gave at most ~20% more, so it stays optional.

## R4. Aggregations on large tenants
Beyond the tiers of [§8.5](09-design-details.md):
- **Result cache per immutable segment**: partial aggregates are cached by `(segment id, query fingerprint)`. Segments never change, so the cache can never be stale. Dashboards that repeat every minute recompute only new segments and the WAL tail. **This is likely the biggest practical win for SIEM dashboards.** The cache is per tenant and lives in the tenant's memory budget (physical separation holds).
- **Code histograms** ([§8.5](09-design-details.md) tier 2) already act as built-in rollups for low-cardinality fields.

**Acceptance threshold**: within **2×** of ClickHouse on a SIEM aggregation suite (top-N, date histograms, group-by on 1–3 keys, distinct counts) on a large tenant, cold cache. Faster than ClickHouse with a warm segment cache.

## R5. Share of schemaless events
**Mitigations**:
- **Measure first**: the share of events without a declared schema in real usiem deployments (sandbox and SIEM).
- **Tenant-private inferred schemas**: at merge or seal, dynamic events with a **stable shape** (same field set and types) get an **inferred schema stored in the tenant's private catalog** ([§3.3](05-schema-catalog.md)). They then use columnar groups, typed encodings and statistics like declared schemas. Nothing is shared between tenants.
- **Per-tenant trained dictionaries** for large tenants' inferred schemas: trained at seal or merge on the tenant's own data and stored inside the tenant's files.
- **Optional template mining** (Drain-style) for free-text messages inside a tenant, producing **private templates**. Off by default; declared templates in usiem schemas remain the main path.
- **Schema authoring tooling** in usiem (outside the engine): generate a schema draft from sample events, so moving data from schemaless to declared is cheap.

**Acceptance threshold**: schemaless data at least on par with Quickwit; with inferred schemas, within **1.5×** of declared schemas on density and query latency.

## R6. Segment build cost
**Found by** [spike 04](../spikes/04-wal-vs-segment.md): building a segment from a tenant's events took **10–16 µs per event** in a deliberately naive prototype (JSON values, one heap allocation per value, a `HashMap` per column, small zstd frames). The per-core ingest budget for the whole write path is 2.5–6.7 µs per event, and every event of a streaming tenant goes through a build at least once (plus merges).

**Mitigations**:
- build from the **typed, schema-encoded WAL rows**, not from JSON;
- **allocation-free column builders**: arena-backed values, dictionaries keyed by hashes of borrowed slices;
- **sort-based dictionary and postings construction** per column (cache-friendly), as planned for postings in the components design;
- larger compression frames at seal and merge;
- build on background threads, off the write path.

**Acceptance threshold**: segment build ≤ **~2 µs per event** amortized on one core, measured in a dedicated spike once the typed row format exists.
