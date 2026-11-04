# Production readiness


| Area | Priority | Requirements |
|---|---|---|
| Failure model and integrity | P0 | Unrecoverable background errors (incl. **fsync errors, never retried**) ⇒ read-only + health event; ENOSPC-safe everywhere; corrupt segments **quarantined** (and re-fetched from a replica or the object store); background scrubbing; **fuzzed decoders** (data from disk and object storage is untrusted) |
| Resource governance | P0 | Global memory budget; per-tenant and per-group quotas and admission control; query limits (concurrency, memory, deadline, cancellation); I/O priority ingest > queries > flush > merge > tiering > scrub; input limits (event size, fields, depth); ingest slows when merges fall behind |
| Observability and operations | P0 | Metrics through the `metrics` facade, `tracing` spans, slow-query log, **explain with pruning statistics**, admin API (flush, merge, verify, tier, quarantine), CLI inspector |
| Backup and restore | P0 | Online snapshot = manifest + **hard links** to immutable files (incremental for free); tested restore |
| Export and import | P1 | Export to JSONL/Parquet, standalone tenant export ([§3.3](05-schema-catalog.md)); bulk import / backfill building sorted segments directly |
| Security | P1 | Encryption at rest (per tenant, after compression, envelope keys via a key-provider trait); crypto-shredding per group; restrictive file permissions; no event content in logs or errors |
| Compatibility | P0 | Separate versions for API, segment, WAL, manifest, directory and catalog formats; required/optional feature flags; **golden-file corpus** of every released format in CI; "N reads N−1" |
| Query contract | P0 | Stable pagination (cursor + snapshot lease with TTL); exact or estimated hit counts; documented visibility (read-your-writes); idempotent batch ids |
| Testing | P0 | **Deterministic simulation testing** (clock, file system, object store, network, scheduling as traits; crashes, torn writes, ENOSPC, replication sink faults, promotion with zombie writers); oracle tests against a naive filter; soak tests; OS matrix (NTFS + Defender, ext4/XFS, APFS); performance regression gates |
| Documentation | P0 | Guarantees contract (durability per class, visibility, retention, full disk, corruption); capacity planning; tuning; runbooks |
