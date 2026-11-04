# Cost check, risks and benchmarks


> Order-of-magnitude estimates. Baseline figures are typical ranges for well-configured deployments, not our measurements.

Reference event: Sysmon/Windows Security ≈ 1.2 KB as JSON.

| Metric | ES/OpenSearch | Quickwit | ClickHouse | **This design (estimate)** |
|---|---|---|---|---|
| Disk per event (× raw JSON) | ~0.3–1.0× | ~0.3–0.6× | ~0.05–0.15× (no text index) | **~0.07–0.18×** with search indexes |
| Ingest per core | ~2–5k EPS | ~5–15k EPS | ~50–200k rows/s | **~150–400k EPS** (typed input, embedded, schema-selected indexes) |
| Selective filter | fast (shared indexes) | fast | sort-key dependent | **fast** (only that tenant's files) |
| Text search | strong | strong | weak | **strong on declared text fields** |
| Large-tenant aggregations | moderate | moderate | best | **~within 2× of ClickHouse** |
| Idle tenant | heavy (index per tenant) | moderate | heavy (table per tenant) | **1 file + ~200 B, 0 RAM** |
| Physical isolation at millions of tenants | no | no | no | **yes** |

**Structural wins**:
1. Physical isolation at millions of tenants.
2. Ingest efficiency (likely 10×+ against ES and Quickwit).
3. ClickHouse-level density while also having search indexes.

| Risk | Mitigation | Evidence needed |
|---|---|---|
| R1 Tiny tenants (< ~1k events) | Templates + schema grouping (main levers), pre-trained dictionaries, compact footer | **measured** ([spike 02](../spikes/02-compression-by-tenant-size.md)): 3–4× smaller than a generic doc store at 100–1k events; relative R1 threshold missed narrowly at 1k (2.1–2.5×) |
| R2 High-cardinality text | Schema tokenizers + inverted index on declared fields | text latency vs ES/Quickwit |
| R3 Many WALs on Windows | Write-through + one device flush per round, `SealOnly` | **done on Windows**: [spike 01](../spikes/01-fsync-rounds-windows.md); Linux/macOS pending |
| R4 Large aggregations | Columnar + statistics + codes; vectorized kernels | aggregation benchmark |
| R5 Schemaless share | Easy schema authoring; measure the share | % of schemaless events in real deployments |
| R6 Segment build cost | Typed row format, allocation-free builders, batch-friendly dictionaries | prototype measured 10–16 µs/event ([spike 04](../spikes/04-wal-vs-segment.md)); target ≤ ~2 µs/event; spike once the typed row format exists |

**Spikes before committing**:
- fsync throughput with 10 / 100 / 1k / 10k dirty files per round (NTFS, ext4/XFS, APFS; NVMe and SATA);
- WAL scan vs segment for selective queries (WAL-to-segment threshold);
- per-WAL encryption overhead;
- compression with and without pre-trained dictionaries across tenant sizes.

**Benchmark plan**:
- **Datasets**: OTRF Security-Datasets, EVTX-ATTACK-SAMPLES, usiem sandbox runs, Loghub.
- **Baselines at best practice**: ES logsdb + best_compression; ClickHouse with typed columns, `LowCardinality` and ZSTD; Quickwit with a tuned mapping.
- **Fairness**: same hardware and durability settings.
- **Metrics**: those in [§1.5](01-problem.md), plus tenant create/seal/delete rates at 10k and 1M tenants.
