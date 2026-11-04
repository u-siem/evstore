# Spike: cost of encrypting per-tenant WAL writes

Design context: `docs/design/04-durability-and-buffers.md`. Pooled WAL files are reused after **crypto-erase**: every WAL life gets a fresh random key that is discarded on release. The key is never reused, so the nonce can be a per-WAL record counter.

Measured on one core:
- AES-256-GCM (RustCrypto `aes-gcm` and `ring`) and ChaCha20-Poly1305 (RustCrypto) throughput for buffer sizes from 128 B to 1 MiB, the range one tenant flushes per sync round;
- per-WAL-life setup (random key + cipher initialisation);
- zstd level 1 on the same buffers, as a reference.

## Run

```
cargo run --release -- [sample file]
```

Pass a file of real event data (e.g. a JSONL tenant file) to measure zstd on realistic content; AEAD cost does not depend on content. `RUSTFLAGS="-C target-cpu=native"` speeds up RustCrypto by ~10%.
