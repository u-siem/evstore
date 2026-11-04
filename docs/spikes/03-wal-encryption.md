# Spike 03: cost of encrypting per-tenant WAL writes

- **Date**: 2026-10-24
- **Code**: [`spikes/wal-encryption`](../../spikes/wal-encryption/README.md)
- **Raw results**: [results/wal-encryption-2026-10-24-ryzen-7800x3d.md](results/wal-encryption-2026-10-24-ryzen-7800x3d.md)
- **Assumption under test** ([durability and buffers](../design/04-durability-and-buffers.md)): encrypting every WAL write for crypto-erase costs "≪ 5% of ingest CPU".

## Setup

AMD Ryzen 7 7800X3D (Zen 4, AES-NI, VAES, CLMUL), Windows 11, single thread, release build with default target features. Buffers are taken from a real OTRF tenant file (Windows event JSON).

## Results (single core)

| Buffer | AES-256-GCM RustCrypto | AES-256-GCM `ring` | ChaCha20-Poly1305 RustCrypto | zstd level 1 (reference) |
|---|---|---|---|---|
| 128 B | 127 ns (1.0 GB/s) | 68 ns (1.9 GB/s) | 1,277 ns (0.1 GB/s) | 2,324 ns |
| 512 B | 317 ns (1.6 GB/s) | 98 ns (5.2 GB/s) | 1,355 ns (0.4 GB/s) | 3,257 ns |
| 4 KiB | 2.1 µs (1.9 GB/s) | 0.42 µs (9.8 GB/s) | 3.0 µs (1.4 GB/s) | 8.7 µs |
| 64 KiB | 34 µs (1.9 GB/s) | 6.4 µs (10.2 GB/s) | 32 µs (2.0 GB/s) | 19.5 µs |
| 1 MiB | 541 µs (1.9 GB/s) | 101 µs (10.4 GB/s) | 496 µs (2.1 GB/s) | 265 µs |

- **Per WAL life** (random key + cipher init): ~160 ns with either AES implementation, 63 ns for ChaCha20-Poly1305. Negligible.
- With `-C target-cpu=native`, RustCrypto AES-GCM reaches ~2.15 GB/s (+10%) and ChaCha20-Poly1305 ~2.3 GB/s.

## What it means per event

Ingest budget from the [cost estimate](../design/11-cost-risks-benchmarks.md): 150–400k events/s per core, i.e. **2.5–6.7 µs per event**. WAL bytes per event depend on the not-yet-designed WAL row format; schema-encoded rows without the template text are plausibly 0.5–1 KB for Windows events.

| WAL bytes per event | RustCrypto AES-GCM | `ring` AES-GCM |
|---|---|---|
| 500 B | ~0.26 µs (4–10% of budget) | **~0.05 µs (0.8–2%)** |
| 1 KB | ~0.52 µs (8–21% of budget) | **~0.10 µs (1.5–4%)** |

(At the ≥ 4 KiB buffers a busy tenant flushes per round; tiny buffers cost more per byte but belong to tenants that write little.)

## Findings

1. **The "≪ 5%" assumption holds only with an optimised AES implementation.** `ring` costs 1–4% of the ingest budget. RustCrypto `aes-gcm` costs 4–21%, which is too much for something that exists only to make file reuse safe.
2. **AES-256-GCM is the right cipher here.** ChaCha20-Poly1305 is 10–20× slower on small buffers on this CPU, and no faster at large ones. It would only matter on CPUs without AES hardware, which are not a target.
3. **Setup per WAL life is negligible**, so a fresh key per WAL life (crypto-erase) costs nothing extra.
4. **Side finding: do not zstd-compress small WAL appends.** zstd level 1 has a fixed cost of ~2–3 µs per call, so compressing a 128 B–4 KiB round buffer costs more than encrypting it. When a small tenant's round carries one or two events, that is a third to all of the per-event ingest budget. Compression belongs at segment build (flush/seal); the WAL stays schema-encoded but uncompressed.
5. Encryption runs on the sync coordinator's thread pool (spike 01), off the writers' path, so it costs CPU, not write latency.

## Design consequences (decided 2026-10-24, see [decisions](../decisions.md))

- **Use `ring` for WAL encryption** (AES-256-GCM, per-WAL-life key, counter nonce). It is a shallow dependency (runtime: `cfg-if`, `getrandom`, `untrusted`; build: `cc`, because it contains C and assembly like `zstd`) and is widely used in the Rust ecosystem. It would sit behind a small internal trait so the implementation can be swapped (e.g. `aws-lc-rs` for FIPS deployments).
- **WAL appends stay uncompressed**; compression happens only when segments are built.

## Not covered

- Other CPUs (older Intel/AMD without VAES, ARM/Apple Silicon); `ring` uses optimised paths there too, but the numbers above are Zen 4 only.
- Decryption cost on WAL-as-buffer reads (expected symmetric with encryption).
- Multi-core scaling: AEAD is embarrassingly parallel across tenants, so no contention is expected.
