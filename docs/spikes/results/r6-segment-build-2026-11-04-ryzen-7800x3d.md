# R6 segment build: raw results, 2026-11-04

- **Machine**: AMD Ryzen 7 7800X3D, Windows 11, store on a Samsung 980 PRO (NVMe), release build.
- **Command**: `cargo run --release -- --events 1000000` in `spikes/r6-segment-build` (batches of 500, flush every 8 MiB of buffered rows, `SyncPolicy::Interval(1 s)`).
- **Data**: 1,000,000 synthetic syslog-like records at 2,000 events/s of event time, 11 fields each (host, program, pid, priority, message, raw line, source IP and port, user, arrival time, origin).

## First run (MVP as first written)

| Step | Total | µs/event |
|---|---|---|
| append (WAL write + buffer) | 0.61 s | 0.61 |
| flush (69 segments) | 9.89 s | 9.89 |
| **append + flush** | 10.51 s | **10.51** |
| compaction (9 merges, 69 → 6 segments) | 16.02 s | 16.02 |

| Query | Rows | ms |
|---|---|---|
| newest 100 (`*`, desc) | 100 | 21.05 |
| `host.name = nas`, newest 100 | 100 | 15.33 |

## After two fixes

The fixes: flush writes segments from borrowed rows (no copy of the buffer), the dictionary is built from distinct strings only, and the k-way merge opens a segment only once the merge reaches its time range.

| Step | Total | µs/event |
|---|---|---|
| append (WAL write + buffer) | 0.61 s | 0.61 |
| flush (69 segments) | 5.90 s | 5.90 |
| **append + flush** | 6.50 s | **6.50** |
| compaction (9 merges, 69 → 6 segments) | 16.30 s | 16.30 |

| | Bytes | Bytes/event |
|---|---|---|
| raw syslog lines | 89,517,507 | 89.5 |
| store after flushes (zstd 3) | 39,360,086 | 39.4 |
| store after compaction (zstd 9) | 34,690,087 | 34.7 |

| Query (best of 5) | Rows | ms |
|---|---|---|
| newest 100 (`*`, desc) | 100 | 2.03 |
| `host.name = nas`, newest 100 | 100 | 11.64 |
| `process.name = sudo` (all) | 100,060 | 310.18 |
| `message contains "job-7.sh"` (all) | 2,480 | 213.42 |
| `source.ip in 192.168.7.0/24` (all) | 3,904 | 170.15 |
| `process.pid = 4242` (all) | 52 | 63.25 |
| last minute, all fields | 120,000 | 189.27 |
| count everything | 1,000,000 | 1,753.24 |
