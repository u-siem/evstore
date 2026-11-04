# Spike: query latency on a tenant's WAL vs a columnar segment

Question: up to what size can a tenant's recent data stay in its WAL ("the WAL is the buffer for small tenants", `docs/design/04-durability-and-buffers.md`) before converting it to a segment, and what does conversion cost?

## Models

- **WAL**: row-major, schema-encoded, uncompressed records (`[u32 len][u16 n]{[u16 field][u32 len][value]}*`), encrypted in 64 KiB chunks with AES-256-GCM (`ring`). A query decrypts and parses everything.
- **Segment**: events grouped by schema; per group and field a zstd dictionary of distinct values plus u32 codes in 4,096-row zstd chunks.
  - Equality and substring predicates are evaluated once per distinct value, then the codes are scanned.
  - There are no postings lists, so the segment numbers are conservative.
- **Queries**: selective `Image` equality, `CommandLine` substring, `EventID = 1`. Every query counts all matches and materializes the first 100 events. The two paths must return identical counts (asserted).
- `Message` is dropped (templates).
- Everything is in memory (warm page cache); nothing decoded is cached between queries.

## Run

```
cargo run --release -- <jsonl dir> --sizes 1000,10000,50000,200000
```

The input is the same OTRF JSONL directory as the `compression-dicts` spike; events are taken in file-name order across tenants.
