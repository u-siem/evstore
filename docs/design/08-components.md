# Components


## 4.1 Segment internals
- **Immutable single-file segments**, read with mmap. Sections: header, schema groups (columnar or packed PAX chunks), indexes, statistics, footer with offsets and per-section checksums. **Metadata is contiguous** so remote range reads work ([§4.6](08-components.md)).
- **Sorted by timestamp** inside each schema group: a time range becomes a binary search, and delta encoding compresses better.
- **Snapshots**: readers hold a manifest generation; flushes and merges publish new ones without blocking.
- **Merges** per tenant: time-tiered, never across distant partitions. A fast codec at flush and stronger zstd on merge.
- **Append-only**: no updates or per-event deletes. A tombstone slot is reserved in the format.

## 4.2 Indexes
- **FST term dictionaries over order-preserving keys** (sign-flipped integers, IPv4-mapped 128-bit IPs, total-order floats) + **Roaring postings**: one structure serves equality, IN, prefix, range and CIDR for every scalar type.
- **Index mode per field from the schema**: `Term`, `ZoneMapOnly` (high-cardinality numerics), `TextTokenized(tokenizer)`, `TextScan` (bloom-pruned scan), `None`.
- **Templates**: template text is tokenized once in the catalog. Text matching a template becomes `template_id IN {...}`.
- **Postings are built at flush/seal** with sort-based inversion; nothing is indexed per event at ingest.
- **Chunk pruning**: zone maps for every column, token blooms for scanned text fields.

## 4.3 Dynamic (schemaless) fields
Per-tenant field registry; dotted paths for nested objects; **type conflicts split into typed sub-fields**; index mode from configurable rules plus a cardinality fallback; a field-count cap with an overflow bag that is searchable only by scan.

## 4.4 Query execution (always one tenant)
Per segment:
1. segment pruning (time, field presence, FST misses);
2. **schema-group pruning**;
3. index phase with Roaring bitmaps;
4. chunk pruning;
5. residual scan of only the needed streams;
6. late materialization.

Also: limit pushdown (newest first), time-ordered merge across segments, and aggregations on codes and statistics. **Consistency per query**: `Eventual` or `ReadYourWrites` (read token = the tenant sequence number from the ack).

**Completeness watermark** per tenant: every event with ingest time ≤ W is durable and visible. Scheduled detection rules query up to W, so in-flight events are never missed.

## 4.5 Retention and disk pressure
- **Streaming tenants**: ingest-time partitions; retention drops whole segments (`max_age`, quota per tenant or group).
- **Sealed tenants**: deleted whole through the directory.
- **`min_age` protection floor**: data younger than this is never deleted under pressure, which defends against log flooding.
- **Disk watermarks**: low → warn; high → evict eligible data, lowest-priority groups first; full → **backpressure** on writers, plus a health event. **Never overwrite in a circular way.**
- **Legal hold** per tenant or group.

## 4.6 Tiering
- **Tiers**: hot and warm on local directories; cold and frozen on object storage (S3/Azure/GCS/MinIO) through a storage-backend trait (`object_store` crate).
- **Units that move**:
  - a **sealed tenant moves as one object**;
  - streaming segments move one by one.
- **Cold**: the file is unchanged. A **local skeleton** (footer, directories, statistics, FSTs; ~1–3%) lets pruning run without remote I/O, followed by range reads and a block cache.
- **Frozen**: postings and FSTs are stripped; data stays searchable by pruned scan, and the skeleton is fetched on demand.
- **Moves**: copy → verify → commit in the directory/manifest → deferred delete. Space pressure moves data down a tier before anything is deleted.

## 4.7 Tamper evidence
- **Hash the content, not the file**: canonical event encoding with a per-tenant sequence number. A **Merkle root per flush/seal** and a **hash chain per tenant**.
- **A sealed sandbox run is one Merkle root**, ideal as evidence.
- **Retention removals are recorded** in the chain, so expired data can be told apart from missing data.
- **v1 scope**:
  - the ledger travels in the replication stream, so replicas verify chain continuity;
  - **Ed25519 signed checkpoints** (key-provider trait: file, TPM, HSM, KMS) aggregating tenant heads (hashes only);
  - **checkpoint sink trait** for external anchors (WORM bucket, another usiem, remote syslog).
  - Forward-secure sealing: format flag reserved.
- **Verification**: `verify --chain`, plus **evidence export** (event + inclusion proof + signed checkpoint).

## 4.8 Cross-platform constraints
- **Deferred deletion**: Windows cannot delete open or mmapped files.
- **Retries with backoff**: Windows Defender briefly locks new files.
- **Short paths**: `\\?\` prefixed paths on Windows.
- **Directory fsync**: Unix only.
- **Encoding and I/O**: explicit little-endian; no `O_DIRECT`; mmap disabled on network file systems.
- **Clock jumps**: ingest time = `max(last_seen, wall_clock)`, so sequence numbers and partitions never go backwards.
- **64-bit only.**
