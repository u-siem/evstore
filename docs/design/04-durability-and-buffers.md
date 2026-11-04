# Durability and buffers per tenant

**Durability class per tenant**, chosen at creation (default from the tenant group):

| Class | Mechanism | On crash |
|---|---|---|
| **`SealOnly`** (default for sandbox) | No WAL; tenant-private buffer spilling to a tenant-private temp file; **one fsync at seal** | Tenant marked `Incomplete`, never silently partial; the run is retried or replayed |
| **`Wal(interval)`** (default for streaming) | Per-tenant WAL + coordinated group sync | Loses at most the interval |
| `WalSync` | Ack after the sync round that covers the write | Nothing acked is lost |
| `SinkAck(sink)` | Ack after the named replication sink confirms the records ([§3.5](07-replication-channel.md)); combinable with any WAL class | Survives loss of this instance, as far as the sink's own guarantees go |
| `SourceCheckpoint` | No WAL; the replayable source offset (Kafka, tailed file) is stored in the manifest at each flush | Re-read from the source |

**Coordinated group sync**: writers only `write()` and mark the tenant dirty. A coordinator flushes all dirty tenant WALs in **rounds**:
- **Linux**: `syncfs` when many tenants are dirty, `io_uring` `fdatasync` when few;
- **macOS**: per-file `fsync`, then one `F_FULLFSYNC`;
- **Windows** (measured, [spike 01](../spikes/01-fsync-rounds-windows.md)): the coordinator itself issues each dirty tenant's pending bytes as **`FILE_FLAG_WRITE_THROUGH` writes** (buffered, so the page cache still serves WAL-as-buffer reads) from 8–32 threads, then makes **one `FlushFileBuffers`** on any one file, which empties the whole device cache. Writers therefore append to a per-tenant in-memory pending buffer (at most one interval of records, counted in the global memory budget) instead of calling `write()`. Per-file `FlushFileBuffers` was measured 20–40× slower at 1,000 tenants.

Device flushes grow with rounds, not tenants. The interval adapts to load within the configured bound.

**Memory**:
- **The WAL is the buffer for small and cold tenants**: rows are already schema-encoded, queries scan the WAL, and its memory is evictable page cache.
- **Memtables exist only for hot tenants**, under a **global memory budget**. Blocks come from a shared free pool, but each block belongs to one tenant at a time and is zeroed on return.
- Conversion to segments happens when a tenant's WAL reaches **~8 MiB** (configurable), at seal, or when the **global WAL budget** (default ~25% of RAM, the page cache that holds WALs) is exceeded, largest WALs first. [Spike 04](../spikes/04-wal-vs-segment.md) measured WAL scans at ~0.4 ms per 1,000 events, so an 8 MiB WAL scans in ~3 ms. Typical sandbox runs (median 2.4k events) never convert before seal.
- **WAL appends are not compressed**: zstd costs 2–3 µs per small append ([spike 03](../spikes/03-wal-encryption.md)); compression happens only at segment build.

**Files**:
- **Preallocated WAL pool**, reused after **crypto-erase**: one key per WAL lifetime, discarded on release. Encryption is AES-256-GCM via `ring` behind an internal trait, with a counter nonce (the key is never reused). Measured cost: 1–4% of the ingest budget ([spike 03](../spikes/03-wal-encryption.md)).
- An **LRU of open handles**; idle tenants keep nothing open.
