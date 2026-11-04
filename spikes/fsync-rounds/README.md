# Spike: fsync rounds over many per-tenant WAL files (risk R3)

Decision under test (`docs/decisions.md`, "Windows sync (R3)"): *measure first; switch Windows to write-through aligned I/O if a round with 1,000 dirty tenants exceeds ~10 ms.*

Design context: `docs/design/04-durability-and-buffers.md`. Every tenant has its own WAL file, and a sync coordinator makes all dirty tenants durable in **rounds**. A round's latency is what an acknowledging writer waits for.

## What it does

For each level of N dirty tenants:
1. Creates N preallocated 64 KiB files (fully written once and synced, like reused pool files) and keeps them open.
2. Runs up to `--rounds` rounds (after one warm-up). In each round every tenant appends one 4 KiB record, and the round ends when all records are durable.
3. Prints one Markdown table row per method and thread count, then deletes the files.

| Method | Round = | Durable without FUA? |
|---|---|---|
| `seq_flush` | `FlushFileBuffers`/`fdatasync` on each file, one thread (writes happened before the round) | yes |
| `par_flush` | same, on T threads | yes |
| `write_through` | parallel writes with `FILE_FLAG_WRITE_THROUGH` | **only if the device honours FUA** |
| `write_through_unbuffered` | same + `FILE_FLAG_NO_BUFFERING`, sector-aligned buffers and offsets | **only if the device honours FUA** |
| `write_through+flush1` | write-through writes, then **one** `FlushFileBuffers` on any one file | yes: the flush command empties the whole device cache |
| `write_through_unbuffered+flush1` | unbuffered write-through writes + one flush | yes |

Write-through methods are Windows only; on Linux and macOS only `seq_flush`/`par_flush` run (`fdatasync`). `syncfs` (Linux) and `F_FULLFSYNC` (macOS) need `libc` and are not measured yet.

## Run

```
cargo run --release -- <dir on the disk under test> --levels 10,100,1000,10000 --threads 1,8,32 --rounds 10 --budget-secs 20
```

`--budget-secs` stops a configuration early (after at least 3 rounds) when slow methods would take too long. `--methods` picks a subset, e.g. `--methods par_flush,write_through+flush1`.

## Caveats when reading results

- **FUA**: whether a write-through write is durable on return depends on the drive and driver. Consumer SATA drives often lack FUA, and then write-through data can sit in the drive's volatile cache. Timing cannot prove durability; only `*_flush` and `*+flush1` methods are durable regardless.
- `+flush1` relies on the flush command reaching the whole device cache (disk-level `SYNCHRONIZE CACHE` / NVMe Flush), which is how Windows' storage stack handles `FlushFileBuffers`. A power-loss test on target hardware is still the only real proof.
- The Windows policy "Turn off Windows write-cache buffer flushing on the device" makes every method fast and **none** of them durable.
- Results depend on the drive, its fill level, background activity and antivirus (Defender scans new files during preparation, not during rounds).
