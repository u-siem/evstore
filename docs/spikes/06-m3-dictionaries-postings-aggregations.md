# Spike 06: per-field dictionaries, postings and aggregations (M3)

- **Date**: 2026-11-04
- **Code**: [`spikes/r6-segment-build`](../../spikes/r6-segment-build/) (same harness as [spike 05](05-r6-segment-build.md), plus an aggregation section)
- **Machine**: AMD Ryzen 7 7800X3D, Windows 11, Samsung 980 PRO (NVMe), release build, one thread
- **Data**: 1,000,000 synthetic syslog-like records, 11 fields, batches of 500, a flush every 8 MiB (69 segments), then compaction to 6 segments
- **Question**: what do the M3 changes buy, and what do they cost?
  - segment format v2: per-field dictionaries, and plain encoding for mostly-distinct fields;
  - Roaring postings for dictionary fields of segments with at least 10k rows;
  - store-side aggregations.

## Results

| | Spike 05 (MVP) | M3 |
|---|---|---|
| append + flush, µs/event | 6.50 | **5.23** |
| compaction, µs/event (background) | 16.3 | 12.1 |
| bytes/event after compaction | **34.7** | 39.8 |
| newest 100 | 2.0 ms | 2.0 ms |
| `host.name = nas`, newest 100 | 11.6 ms | **0.68 ms** |
| `process.name = sudo`, 100k rows materialized | **310 ms** | 402 ms |
| `message contains "job-7.sh"` | **213 ms** | 328 ms |
| `source.ip in /24` | **170 ms** | 283 ms |
| `process.pid = 4242` | **63 ms** | 86 ms |
| materialize all 1M events | 1,753 ms | **1,352 ms** |

Aggregations, computed in the store without materializing events:

| Aggregation | Groups | ms |
|---|---|---|
| count everything | 1 | 0.01 (chunk metadata) |
| count by `host.name` | 12 | 95 |
| count by 1h bucket | 1 | 65 |
| `sum(source.port)` by `process.name` | 10 | 123 |
| `process.name = sudo`, count by `user.name` | 6 | 57 |
| count by `host.name` and 5m bucket, plus `max(process.pid)` | 24 | 147 |

Streaming the same events through usiem-query would cost at least the 1,352 ms needed to materialize them, so these aggregations are 10–20× faster, and plain `count` is immediate.

## Findings

1. **Per-field dictionaries fix selective string queries.**
   - `host.name = nas` is now evaluated against the 12 host names, not about 330k strings.
   - Combined with postings, which let the newest-first scan open only the chunks that contain `nas`, the query is 17× faster.
2. **Plain encoding is a trade-off.**
   - Benefits: raw lines and messages no longer build a segment-wide sorted dictionary at flush (ingest −20%), and no large dictionary is cached in memory per segment.
   - Costs: materializing even one row from a chunk now decompresses the chunk's whole plain columns (~370 KB). Queries that match a few rows in many chunks are 1.3–1.7× slower. Storage is 15% larger, because sorted dictionaries compressed better than per-chunk text.
   - At homelab scale (≤ 0.4 s per million events scanned) that is acceptable, so the trade-off is kept.
3. **Postings help only when they skip chunks.**
   - They are decisive for selective terms (the 17× above).
   - They are neutral for frequent terms (`process.name = sudo` matches 10% of rows, spread over every chunk).
   - The keep/drop rule from the plan (keep them if they at least halve the selective query) is met.
4. **Ingest is at 5.2 µs/event.** Better, but the ~2 µs v1 target is still open. What remains: zstd on plain columns, per-value work in column building, and the fsync per flush.

## Possible next steps (not planned)

- Per-chunk materialization caches, or smaller frames for plain columns, to recover the selective full-scan numbers.
- A trigram or token index for `message contains`.
- Reducing plain-column size: a higher zstd level at flush, or zstd dictionaries trained per field.
