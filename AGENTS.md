# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## Project overview

**usiem-evstore** is the embedded event store **library** for uSIEM
(`../usiem`). It stores security events for many physically isolated tenants
and serves single-tenant queries over them. It is a library crate: no
binary, nothing to "run".

**Status: MVP.** `Store` (`src/store.rs`) is a working single-tenant store:
`wal`, `segment`, `directory` (manifest), `query`, `retention` and `registry`
are implemented in their MVP form, plus `index` (postings) and
`aggregate` from M3 (see each module's "MVP implementation"
docs and the MVP section of `docs/decisions.md`). `catalog`,
`tiering`, `integrity` and `replication` are still documentation only. Do not
extend a module without reading its design file in `docs/design/` and
`docs/decisions.md`.

Non-negotiable design rules (see `docs/decisions.md`):

- **Library, not a database server.** No cluster membership, consensus,
  leader election or cross-instance queries. Fleet concerns belong to the
  application; the library only offers the replication channel
  (`ReplicationSink` trait), `apply_replica` and writer generations.
- **Physical tenant separation of everything.** No file, WAL or in-memory
  buffer may ever hold bytes of two tenants. Replication records never mix
  tenants. Every query targets exactly one tenant.
- **Near-zero idle cost per tenant.** No per-tenant threads, timers,
  open handles or RAM for idle or sealed tenants.
- **Append-only.** No updates or per-event deletes.
- **Multiplatform.** Windows, Linux and macOS behave the same; see the
  cross-platform constraints in `docs/design/08-components.md`.

## Tech stack

- Rust, **edition 2024** (Rust ≥ 1.85), single crate, no workspace.
- **Dependencies**: `zstd`, `memmap2`, `crc32fast`, `roaring` (0.11, no
  default features). When adding one, follow
  uSIEM's rule: named crates with shallow dependency trees only. Candidates already considered in the
  design: `fst`, `roaring`, `zstd`, `memmap2`, `ring` (WAL encryption,
  behind a trait; chosen in docs/spikes/03-wal-encryption.md), an embedded
  B-tree KV store (`redb`-style), `object_store` (tiering only, behind a trait).

## Commands

```
cargo build
cargo test
cargo test --features failpoints --test crash
cargo fmt
cargo clippy --all-features --all-targets
```

CI (`.github/workflows/ci.yml`) builds and tests on Ubuntu, Windows and
macOS, plus `cargo fmt --check` and `cargo clippy`. Keep the repo fmt-clean
and clippy-clean.

## Repo layout

```
src/
├── lib.rs          # crate root and module map
├── store.rs        # MVP Store: open/append/search/flush/maintain
├── model.rs        # Record, Value, ValueType, comparisons
├── codec.rs        # little-endian helpers and the record codec
├── registry.rs     # field registry (typed identities, cap, overflow)
├── aggregate.rs    # Store::aggregate: grouped count/sum/min/max
├── error.rs        # StoreError
├── catalog.rs      # docs/design/05-schema-catalog.md
├── directory.rs    # docs/design/06-tenant-directory.md
├── wal.rs          # docs/design/04-durability-and-buffers.md
├── segment.rs      # docs/design/03-tenant-storage-unit.md (+ 08, 09)
├── index.rs        # docs/design/08-components.md (+ 09)
├── query.rs        # docs/design/08-components.md (+ 09)
├── retention.rs    # docs/design/08-components.md
├── tiering.rs      # docs/design/08-components.md
├── integrity.rs    # docs/design/08-components.md
└── replication.rs  # docs/design/07-replication-channel.md
tests/crash.rs      # crash recovery (feature failpoints)
spikes/             # standalone experiment crates (not built by CI)
docs/
├── README.md       # reading order
├── decisions.md    # every decision taken
└── design/         # one file per design topic
```

## Code conventions (inherited from uSIEM)

- Rustdoc (`///`, `//!`) on public items.
- **No `thiserror`/`anyhow`**: each fallible domain gets its own hand-written
  error enum implementing `Display` + `Error`.
- **No logging/tracing crate** inside the store; follow uSIEM's observability
  facade approach.
- Stateful external integrations are **traits implemented by the consumer,
  with no bundled implementation** (`ReplicationSink`, the catalog provider,
  storage backends, key providers). Sink implementations belong in separate
  optional crates.

## Testing conventions

- Plain `#[test]`, hand-written test doubles, no mocking framework.
- `query::tests` holds an oracle test: segment execution against
  `Filter::matches` on random records and filters (seeded `test_util::Rng`).
- Crash tests (`tests/crash.rs`, feature `failpoints`) abort a child process
  at named failpoints (`crate::failpoint`) and check recovery.
- Planned (see `docs/design/10-production-readiness.md`): deterministic
  simulation testing with clock, file system, object store, network and
  scheduling behind traits; oracle tests against a naive filter; golden-file
  corpus for every released on-disk format.

## Git conventions

Conventional-Commits-style prefixes (`feat:`, `fix:`, `docs:`) with short,
lower-case, imperative summaries, as in uSIEM.

## Changing the design

Design changes go into the relevant `docs/design/` file **and** a row in
`docs/decisions.md`. Code that contradicts a recorded decision needs the
decision updated first.
