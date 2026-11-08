# Contributing

Thanks for helping. Issues and pull requests are welcome.

## Before you open a pull request

CI runs all of these; running them first saves a round trip.

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked                 # no features
cargo test --locked --all-features  # adds the crash tests (feature `failpoints`)
cargo deny check                    # https://github.com/EmbarkStudios/cargo-deny
```

The minimum supported Rust version is 1.89. CI tests it, along with Linux,
Windows and macOS: file locking, fsync, memory mapping and deletion rules
differ between them, and the store must behave the same on all three.

## Design and on-disk formats

Read the relevant file in `docs/design/` and `docs/decisions.md` before
extending a module. A change to the design goes into its design file and a
row in `docs/decisions.md`.

The WAL, segment and manifest formats are documented in the module docs of
`src/wal.rs`, `src/segment.rs` and `src/directory.rs`. A change that alters
bytes on disk must:

- keep existing files readable, or bump the format version and say how old
  files are handled (segments of format v1 are still read today);
- update the module docs in the same pull request;
- add an entry under `Unreleased` in `CHANGELOG.md`.

## Fuzzing

The store reads files that may be damaged by crashes, disks or people, so
its decoders are fuzzed with [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz)
(nightly):

```sh
cargo +nightly fuzz run wal_replay
cargo +nightly fuzz run manifest
cargo +nightly fuzz run segment_footer
cargo +nightly fuzz run segment_block
cargo +nightly fuzz run segment_file
```

CI runs each target daily and on pull requests that touch the decoding code.
A crash found by fuzzing belongs in a regular test once fixed.

## Releases

Maintainers release by bumping `version` in `Cargo.toml`, moving the
`Unreleased` changelog entries under the new version, and pushing a tag that
is exactly the version, with no prefix: `git tag 0.2.0 && git push origin 0.2.0`.
The release workflow checks the tag, runs CI and creates the GitHub release.
The crate is not published to crates.io yet.
