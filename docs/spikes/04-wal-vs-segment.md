# Spike 04: query latency on a tenant's WAL vs a segment

- **Date**: 2026-10-24
- **Code**: [`spikes/wal-vs-segment`](../../spikes/wal-vs-segment/README.md)
- **Raw results**: [results/wal-vs-segment-2026-10-24-ryzen-7800x3d.md](results/wal-vs-segment-2026-10-24-ryzen-7800x3d.md)
- **Questions** ([durability and buffers](../design/04-durability-and-buffers.md)):
  - how large can a tenant's WAL grow while queries scan it directly ("the WAL is the buffer for small tenants")?
  - what does converting it to a segment cost?

## Setup

- **Hardware**: AMD Ryzen 7 7800X3D, single thread, all data in memory (warm page cache).
- **Data**: OTRF Windows events (from [spike 02](02-compression-by-tenant-size.md)), `Message` dropped (templates), taken in order across tenants.
- **WAL**: schema-encoded uncompressed rows, AES-256-GCM encrypted in 64 KiB chunks (`ring`, [spike 03](03-wal-encryption.md)).
- **Segment**: schema groups, per-field zstd dictionary + code chunks, no postings lists (conservative).
- **Queries**: count all matches and materialize the first 100 events; both paths returned identical counts.

## Results (median ms per query)

| Events | WAL size | Segment size | `Image = <selective value>` WAL / segment | `CommandLine contains "whoami"` WAL / segment | `EventID = 1` WAL / segment |
|---|---|---|---|---|---|
| 1,000 | 0.9 MiB | 0.07 MiB | 0.16 / 0.05 | 0.16 / 0.003 | 0.16 / 0.04 |
| 10,000 | 9.2 MiB | 0.26 MiB | 3.2 / 0.56 | 3.2 / 0.01 | 3.2 / 0.09 |
| 50,000 | 63 MiB | 0.69 MiB | 22.9 / 0.71 | 23.1 / 0.19 | 22.9 / 0.10 |
| 200,000 | 235 MiB | 2.4 MiB | 87.3 / 2.3 | 88.0 / 0.55 | 86.8 / 0.12 |

- **WAL scan speed** is ~2.7 GB/s including decryption, i.e. **~0.35–0.45 ms per 1,000 events (~0.37 ms per MiB)**, the same for every predicate because nothing can be skipped.
- **Segment queries** are 3–740× faster: schema groups skip whole schemas (`EventID = 1`), and dictionaries evaluate predicates once per distinct value (`CommandLine contains`).
- **Segment build**, as prototyped here: **10–16 µs per event** (13 ms for 1k events, 3.1 s for 200k).

## Findings

1. **The WAL-as-buffer design holds for small tenants.** Typical sandbox runs (median 2.4k events, ~2.2 MiB of WAL) scan in under 1 ms, so they never need a segment before seal.
2. **A WAL-to-segment threshold around 8–16 MiB (≈ 8–16k events) keeps WAL scans at ~3–6 ms.** That bound applies to every query, including those a segment would answer in microseconds. Beyond ~25k events (~28 MiB) a WAL scan exceeds 10 ms per query.
3. **Memory sets a second bound.** The WAL lives in the page cache, so 1,000 active tenants × 16 MiB is 16 GiB. The threshold must also come from a global WAL budget: convert the largest WALs first when the budget is exceeded.
4. **New risk: segment build cost.** The prototype spends 10–16 µs per event building a segment. The [cost estimate](../design/11-cost-risks-benchmarks.md) assumes 2.5–6.7 µs per event for the whole ingest path. The prototype is deliberately naive (JSON values, a heap allocation per value, a `HashMap` per column, small zstd frames), so a real typed implementation should be much faster. But build cost has not been shown to fit the budget, and it is incurred for every event of streaming tenants.

## Design consequences (decided 2026-10-24, see [decisions](../decisions.md))

- **WAL-to-segment threshold**: convert at **~8 MiB of WAL per tenant** (default, configurable), at seal, or when a **global WAL budget** (default e.g. 25% of RAM) is exceeded, largest WALs first.
- **Add risk R6, segment build cost**, with a dedicated spike once the typed row format exists. Acceptance: build ≤ ~2 µs per event amortized, so flush plus ingest stay within the per-core budget.

## Caveats

- Single-threaded and in-memory. Real queries may run in parallel across segments, and a cold page cache would make WAL scans disk-bound.
- The segment has no postings lists or statistics, so real segments would be faster still. Real WAL rows may be more compact than this simple encoding (~930 B–1.3 KB per event here).
- The build cost is an upper bound from a naive prototype, not an estimate of the final engine.
