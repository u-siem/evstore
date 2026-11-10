# Changelog

All notable changes to this crate are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Release tags are the bare version,
with no prefix (`0.1.0`).

## [Unreleased]

### Changed

- Faster string matching in scans: ASCII values are lowercased into a
  reused buffer instead of a new allocation per value, globs match on UTF-8
  bytes without allocating, and the common `*lit*`, `lit*`, `*lit` and
  wildcard-free shapes skip backtracking.
- `Exists` and `HasType` leaves are answered from value presence without
  materializing values.
- Aggregations keep integer, time, boolean and IP group keys unencoded and
  look each group up once per row.
- Searches share the matching records of the in-memory buffer instead of
  copying them all; a record is cloned only when it is emitted (a
  newest-first query with a limit no longer copies the whole buffer).
- `benches/query.rs`: query and aggregation benchmarks (`cargo bench`),
  with a first-run column (segments planned cold).
- Searches plan segments lazily, when the iteration first reaches them: a
  newest-first query with a limit never reads the dictionaries or postings
  of older segments. Planning errors now arrive through the iterator, so
  `Store::search` currently always returns `Ok`.
- Postings narrow scans to candidate rows, not just chunks. When they
  decide a filter exactly (dictionary fields with postings, combined with
  `And`/`Or`), rows are not re-evaluated and filter columns are not
  decoded; a `Count` with such a filter is answered from the bitmaps.
- Materialized records are allocated at their final size.
- **Breaking:** `Query` has a new public field, `fields`, so struct literals
  need it (or `..Query::default()`). `Some(names)` returns only the fields
  with those exact names, including overflowed ones, and skips decoding
  every other column the filter doesn't need. Materializing all 420,000
  benchmark records with one field takes 59 ms instead of 273 ms.

### Documentation

- `docs/querying.md`: a guide to querying a store directly (`Query`,
  `Filter`/`FieldOp`, matching rules, aggregations, inspection, and which
  queries are pruned or scanned).

### Fixed

- `FieldOp::Glob`: a `*` in the pattern facing a literal `*` in the text
  is now always a wildcard (`*a*` matches `*ba`); it used to be matched as
  a literal first, without backtracking.


## [0.1.0]

First public release.

### Added

- `Store`: an embedded, single-tenant event store per directory, with a
  write-ahead log, memory-mapped columnar segments (per-field dictionaries,
  zone maps, Roaring postings), a generation-numbered manifest, crash
  recovery, ingest-day retention and tiered compaction.
- `Store::search` (`Query`, `Filter`, `Order`) and `Store::aggregate`
  (`AggQuery`: counts, sums, minimums and maximums by fields and time
  buckets).
- Feature `failpoints`: test-only crash injection.

[Unreleased]: https://github.com/u-siem/evstore/compare/0.1.0...HEAD
[0.1.0]: https://github.com/u-siem/evstore/releases/tag/0.1.0
