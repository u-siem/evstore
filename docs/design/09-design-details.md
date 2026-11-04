# Design details


> All open points (8.1–8.5) are resolved and adopted. They are kept here as the record of those decisions.

## 8.1 Packing threshold (adopted)
- **Cost-based, decided per file when the segment is written.** A schema gets its own columnar group when `estimated columnar size + group overhead < estimated packed size`.
  - **Group overhead** is known: header + ~32–64 B per column + reference entries.
  - **Size estimates** come from **per-schema compression profiles shipped in the catalog** (bytes/event columnar vs packed, measured by usiem when it trains the dictionaries).
- **Fallback** when no profile exists (custom or dynamic schemas): own group at **≥ ~100 events**. [Spike 02](../spikes/02-compression-by-tenant-size.md) measured 100 as best; 2,000 (the original value) was 16% worse for 1k-event tenants, and 20 gained nothing.
- **Hysteresis across merges** (e.g. promote at ≥ 1×, demote below 0.5×) stops a schema flip-flopping between layouts.
- **Sealed tenants** decide once at seal with the full distribution known, so their choice is optimal.

## 8.2 Text index policy (adopted)
- **The schema declares each text field's mode**: `TextTokenized(tokenizer)` for searchable fields (command lines, paths, URLs, message parameters), `TextScan` for other text, `None` for opaque values (hashes are `Term`, not text).
- **No indexes on small segments**: below a size threshold (e.g. < 10k events or < 1 MB of text in the group), a dictionary-aware scan is as fast as an index lookup, so the index is skipped. Indexes are built at **seal and merge**, when segments are big enough to benefit. This also keeps flushes of small tenants cheap (R1).
- **Substring search** (`*mimikatz*`):
  - by default: tokens + prefix via the FST, and the per-file dictionary scan (each distinct value checked once);
  - **opt-in trigram index** for fields the schema marks `substring` (typically command lines and paths), only on segments above the threshold.

## 8.3 Segment size limits (adopted; tenant shards removed by the library simplification)
- **Target merged segment size**: 256 MB–2 GB; **hard cap ~4 GB** (keeps mmap, upload, copy and verification manageable).
- **A sealed tenant above the cap** (rare: roughly tens of millions of events) becomes **several sealed files**, with the manifest in the last one. It is still self-contained and still one directory record.
- **A tenant too large for one instance** is split by the application into several tenants and federated outside ([§3.5](07-replication-channel.md)).

## 8.4 Failure behaviour of the library (replaces the cluster walk-through)

| Failure | What the library does | Acked data lost |
|---|---|---|
| **Process crash** | Startup recovery of in-flight tenants: WAL replay; `SealOnly` runs → `Incomplete` + event; replication resumes from persisted cursors (at-least-once redelivery, deduplicated by receivers) | none beyond the durability class |
| **Power loss** | Same as a crash; sealed files were fsynced before their ack | ≤ sync interval for `Wal(interval)`; none for `WalSync`/`SinkAck` |
| **Sink down or slow** | Lag policy: `Block` (backpressure), `Retain` (keep WAL up to a budget) or `Detach` (mark `NeedsResync`); health event. Local writes unaffected unless `Block` or `SinkAck` | none locally |
| **Sink lag beyond retention budget** | Tenant marked `NeedsResync`; snapshot shipped when the sink recovers, then records resume | none |
| **Duplicate or out-of-order delivery** at a receiver | Duplicates ignored by `(tenant, seq)`; a gap returns `NeedsResync(from_seq)` | none |
| **Writer instance lost for good** | Nothing automatic. The application calls `promote` on a replica (generation + 1) and routes writes there | async sinks: changes not yet acked by the sink; `SinkAck`: none |
| **Zombie old writer comes back** | Its records carry an older generation → rejected by replicas; it learns of the newer generation and turns read-only for that tenant | none |
| **Full disk** | Backpressure per tenant; tiering and retention free space ([§4.5](08-components.md)) | none |
| **Corrupt file** | Quarantine + health event; the application can restore it from a replica or archive through the snapshot/apply API | none if a replica or archive exists |

**Testing requirement**: deterministic simulation covers crashes, torn writes, ENOSPC and **sink faults** (loss, duplication, reordering, delay, disconnect), plus promotion with zombie writers ([§5](10-production-readiness.md)).

## 8.5 Aggregation execution model (adopted)

**Goal**: SIEM aggregations (dashboards, statistics, detection thresholds) fast on every tenant size; within ~2× of ClickHouse on large tenants (R4). Most tenants are small, so **avoiding work** matters more than raw kernel speed.

**Execution tiers, cheapest first** (the planner picks the cheapest that answers the query exactly):
1. **Metadata only**: counts per schema, time bucket or chunk from statistics. `count(*)` with a time filter and date histograms come from chunk time ranges and counts.
2. **Code histograms**: for low-cardinality fields (enums, dictionary-coded strings with ≤ ~256 distinct values per chunk), each chunk stores **per-code counts**. A group-by or top-N on those fields with only time/schema filters needs **no decoding at all**.
3. **Code-level execution**: group-by on integer codes (array or hash aggregation), filters as Roaring bitmaps turned into selection vectors. Labels are decoded only for the final result rows.
4. **Vectorized kernels**: batch-at-a-time (~1–4k values) over decoded column blocks, with selection vectors, for value-based aggregations (sum, min/max, percentiles on numeric fields).

**Functions in v1** (the SIEM set): `count`, `count_distinct` (HyperLogLog by default, exact up to a limit), `sum`, `min`, `max`, `avg`, `percentiles` (DDSketch), `top-N / terms`, `date_histogram`, group-by on up to K keys. All are **mergeable partials**, which serve across segments and, through the external federation layer, across instances ([§3.5](07-replication-channel.md)). Approximate answers are always flagged.

**Engine choice**
| Option | Pros | Cons |
|---|---|---|
| **Own minimal kernels** for the SIEM set, on code/column blocks | Small, tuned to codes and statistics, no large dependency, full control over memory limits | Limited to the defined function set |
| Embed Apache DataFusion (Arrow) | Vectorized engine, SQL, many functions for free | Big dependency and compile time; conversion to Arrow per block; harder to keep within the engine's memory governance |

**Recommendation**: **own minimal kernels in v1**, with column blocks laid out **Arrow-compatible** so DataFusion can be added later as an optional "advanced analytics" path without changing the format.

**Limits** ([§5](10-production-readiness.md)): group-by cardinality cap per query (error or approximate mode beyond it), memory budget per query, cancellation between batches.
