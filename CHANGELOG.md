# Changelog

All notable changes to this crate are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Release tags are the bare version,
with no prefix (`0.1.0`).

## [Unreleased]

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
