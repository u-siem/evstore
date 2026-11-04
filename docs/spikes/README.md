# Spikes

Experiments that validate (or overturn) design assumptions before implementation. Each report states the hardware, the method, the results and what changed in the design. The spike code lives in [`../../spikes/`](../../spikes/).

| # | Spike | Risk | Status | Outcome |
|---|---|---|---|---|
| 01 | [Sync rounds over many per-tenant WALs on Windows](01-fsync-rounds-windows.md) | R3 | Done on Windows (NVMe + SATA SSD); Linux/macOS pending | Per-file flush is 20–40× too slow at 1,000 tenants; adopt write-through + one device flush per round on Windows |
| 02 | [Bytes per event by tenant size](02-compression-by-tenant-size.md) | R1 | Done on OTRF lab data (97 tenants, strict environment split) | 3–6× smaller than a generic doc store at every size; templates and schema grouping carry the win, dictionaries add 7–13%; packing fallback should be ~100, not 2,000 |

| 03 | [Cost of encrypting WAL writes](03-wal-encryption.md) | — (crypto-erase) | Done on Zen 4 | `ring` AES-256-GCM ~10 GB/s = 1–4% of the ingest budget; RustCrypto ~2 GB/s = 4–21%; don't zstd small WAL appends |
| 04 | [WAL scan vs segment](04-wal-vs-segment.md) | — (WAL-as-buffer) | Done on OTRF data, 1k–200k events | WAL scans ~0.4 ms per 1k events; convert at ~8 MiB; segment queries 3–740× faster; **segment build cost is a new risk (R6)** |
| 06 | [Per-field dictionaries, postings, aggregations (M3)](06-m3-dictionaries-postings-aggregations.md) | R6 | Done on 1M syslog-like events | Ingest 5.2 µs/event; selective newest-100 17× faster; aggregations 10–20× faster than streaming; plain columns cost 1.3–1.7× on sparse full scans and +15% size |
| 05 | [MVP ingest and segment-build cost](05-r6-segment-build.md) | R6 | Done on the MVP, 1M syslog-like events | 6.5 µs/event ingest + flush (homelab bar ≤ 20 met); ~2 µs v1 target still open; per-segment dictionary slows selective string queries |

All spikes listed in [the cost and risk plan](../design/11-cost-risks-benchmarks.md) have a first result. Still pending: spike 01 on Linux/macOS, and the ~2 µs segment build target (R6) once per-field dictionaries exist.
