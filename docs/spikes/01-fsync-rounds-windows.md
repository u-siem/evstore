# Spike 01: sync rounds over many per-tenant WALs on Windows (risk R3)

- **Date**: 2026-10-24
- **Code**: [`spikes/fsync-rounds`](../../spikes/fsync-rounds/README.md)
- **Raw results**: [NVMe](results/fsync-rounds-2026-10-24-windows-nvme-980pro.md), [SATA SSD](results/fsync-rounds-2026-10-24-windows-sata-a400.md)
- **Decision under test**: "measure first; switch Windows to write-through aligned I/O if a round with 1,000 dirty tenants exceeds ~10 ms" ([decisions](../decisions.md)).

## Setup

| | |
|---|---|
| OS | Windows 11 Pro (10.0.26300), NTFS |
| CPU | AMD Ryzen 7 7800X3D, 16 logical cores |
| NVMe | Samsung 980 PRO 1 TB (C:), consumer drive without power-loss protection |
| SATA SSD | Kingston A400 480 GB (D:), consumer DRAM-less drive |
| Workload | N open, preallocated 64 KiB WAL files; each round every tenant appends one 4 KiB record; round ends when all are durable; 10 rounds after a warm-up (3 minimum when a configuration exceeded its 20 s budget) |

Methods are described in the [spike README](../../spikes/fsync-rounds/README.md). Only methods marked "durable" are durable regardless of whether the drive honours FUA.

## Results (median round latency, ms, 32 threads)

| Dirty tenants | NVMe `par_flush` (durable) | NVMe `write_through+flush1` (durable) | NVMe `…_unbuffered+flush1` (durable) | SATA `par_flush` | SATA `write_through+flush1` | SATA `…_unbuffered+flush1` |
|---|---|---|---|---|---|---|
| 10 | 5.2 | 3.8 | 3.5 | 5.4 | 2.2 | 2.2 |
| 100 | 30.5 | 4.5 | 4.4 | 24.5 | 5.5 | 5.5 |
| **1,000** | **295** | **8.0** (p90 10.3) | **6.6** (p90 7.5) | **217** | **16.4** | **16.6** |
| 10,000 | 3,061 | 78.5 | 89.5 | 2,214 | 120.8 | 120.2 |

With 8 threads the write-through variants are within ~10–30% of the 32-thread figures; with 1 thread they are 4–7× slower.

Reference points:
- A single per-file flush costs **~3 ms on the NVMe** and **~1 ms on the SATA SSD** (`seq_flush` divided by N). Both drives lack power-loss protection, so every flush really empties a volatile cache.
- Plain write-through without the final flush (`write_through`, not proven durable) is only ~1–4 ms faster than `+flush1` at 1,000 tenants. The single device flush is cheap; **the per-file flushes are what is expensive**.

## Findings

1. **The decision rule is triggered.** With per-file `FlushFileBuffers`, a 1,000-tenant round takes 217–295 ms even with 32 threads, 20–30× over the ~10 ms threshold. Cost grows linearly with dirty tenants (~0.2–0.3 ms per tenant at best).
2. **Write-through + one device flush is the fast, safe round.** Writes with `FILE_FLAG_WRITE_THROUGH` from 8–32 threads, then a single `FlushFileBuffers` on any one file, which empties the whole device cache. It is durable whether or not the drive honours FUA:
   - NVMe: 8 ms for 1,000 tenants (meets the threshold), ~80 ms for 10,000;
   - SATA SSD: 16 ms for 1,000, ~120 ms for 10,000.

   That is **20–40× faster** than per-file flushing.
3. **Plain write-through alone must not be trusted.** On these consumer drives it cannot be shown to be durable, and it only saves the cost of one device flush.
4. **Unbuffered (`FILE_FLAG_NO_BUFFERING`) is not worth it as the default.** It is at most ~20% faster on the NVMe at 1,000 tenants and equal or slower elsewhere. Its costs:
   - every round pads each tenant to a 4 KiB sector multiple;
   - it bypasses the page cache, which the design uses as the buffer for small tenants (WAL-as-buffer), so WAL scans would hit the disk.
5. **10,000 dirty tenants per round is feasible** at ~80–120 ms, inside the default 200 ms durability interval, but `WalSync` latency then grows to ~100 ms. Very large numbers of dirty tenants need the interval to stretch, or more parallelism (overlapped I/O).

## Consequences for the design

- **Windows round mechanism**: the sync coordinator **issues the WAL writes itself** with `FILE_FLAG_WRITE_THROUGH` (buffered, so the page cache keeps serving WAL-as-buffer reads), in parallel from a small pool (8–32 threads, or overlapped I/O), and finishes each round with **one `FlushFileBuffers`**.
  - Consequence: on Windows, writers hand records to a per-tenant in-memory pending buffer instead of calling `write()` themselves. Each buffer holds at most one interval's worth of records, and its memory counts against the global budget.
- **Unbuffered I/O** stays an optional mode for tenants that write large batches.
- **Never rely on write-through without the final flush**, unless a device is known to be power-protected (not detectable without elevation on this test machine).

## Not covered yet

- **Linux** (`syncfs`, `io_uring` `fdatasync`) and **macOS** (`F_FULLFSYNC`): need `libc`; run the same spike there.
- **Proof of durability**: `+flush1` relies on the disk-level flush command emptying the whole device cache, which is how Windows' storage stack handles `FlushFileBuffers`. A power-loss test on target hardware is still required before shipping.
- Server-class drives with power-loss protection, where flushes are nearly free and the gap between methods should shrink.
- Overlapped I/O (IOCP) instead of a thread pool, and Defender's impact during real WAL rotation.
