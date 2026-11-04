# Spike 05: ingest and segment-build cost of the MVP (R6)

- **Date**: 2026-11-04
- **Code**: [`spikes/r6-segment-build`](../../spikes/r6-segment-build/)
- **Raw results**: [results/r6-segment-build-2026-11-04-ryzen-7800x3d.md](results/r6-segment-build-2026-11-04-ryzen-7800x3d.md)
- **Question** ([spike 04](04-wal-vs-segment.md), risk R6): now that the typed row format exists, what does it cost per event to append to the WAL and build segments? The first acceptance bar is the homelab one: **at most 20 µs per event at 2,000 events/s**. The design target stays at about 2 µs.

## Setup

- AMD Ryzen 7 7800X3D, Windows 11, NVMe (Samsung 980 PRO), release build, one thread.
- 1,000,000 synthetic syslog-like records with 11 fields, shaped like a syslog receiver's events: host, program, pid, priority, message, raw line, source IP and port, user, arrival time, origin.
- Appends in batches of 500, `SyncPolicy::Interval(1 s)`. A segment is flushed every 8 MiB of buffered rows (69 segments), then `maintain` compacts.

## Results

| | µs/event |
|---|---|
| append (WAL frame + in-memory buffer) | 0.61 |
| flush (segment build: sort, dictionary, columns, zstd 3, fsync) | 5.90 |
| **append + flush** | **6.50** |
| compaction, in the background (69 → 6 segments, zstd 9) | 16.3 |

- **Size**: 34.7 bytes/event after compaction (39.4 after flush), against 89.5 bytes of raw syslog text. The store keeps the raw line plus every parsed field.
- **Queries** over the 1M events, which sit in 6 segments:
  - newest 100: 2 ms;
  - `host.name = nas`, newest 100: 12 ms;
  - selective full scans (`pid = 4242`, CIDR, `message contains`): 60–215 ms;
  - materializing every event: ~1.75 µs/event.
- The first version cost 10.5 µs/event (flush 9.9). Two fixes brought it to 6.5:
  - flush writes segments from borrowed rows, and builds the dictionary from distinct strings only;
  - the k-way merge opens a segment only when the merge reaches its time range, which took newest-100 from 21 ms to 2 ms.

## Findings

1. **The homelab bar is met.** At 6.5 µs/event on the ingest path plus about 16 µs/event of background compaction, 2,000 events/s cost roughly 4.5% of one core.
2. **The design target (~2 µs) is not met yet.** Flush time is spent on:
   - one dictionary per segment that holds every distinct string, mostly unique raw lines and messages;
   - per-value `String` work;
   - zstd.
   Per-field dictionaries and skipping the dictionary for text-scan fields (`message`, `@raw`) are the next levers.
3. **One dictionary per segment hurts selective string queries.** `host.name = nas` evaluates its predicate against every distinct string in the segment, which here means about 330k raw lines and messages, not the 12 host names. Per-field dictionaries fix this too. They belong with the M3 index work.
4. **Compaction at zstd level 9 costs more than the flush itself.** It runs off the ingest path, and each event is rewritten about twice before it reaches the top level. Level 6 is worth measuring if CPU becomes tight.

## Design consequences

- R6 is **accepted for the MVP / homelab profile** (≤ 20 µs/event).
- Kept open for v1: per-field dictionaries, no dictionary for text-scan fields, and re-measuring the ~2 µs target. Tracked in [decisions](../decisions.md).
