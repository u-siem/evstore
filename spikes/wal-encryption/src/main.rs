//! Spike: what does encrypting every per-tenant WAL write cost?
//!
//! Design (docs/design/04-durability-and-buffers.md): each WAL file life has
//! its own random key, discarded on release (crypto-erase), so a pooled file
//! can be reused without zeroing. Because a key is never reused, the nonce can
//! be a simple per-WAL record counter.
//!
//! The spike measures, on one core:
//! - AEAD throughput (AES-256-GCM, ChaCha20-Poly1305) for the buffer sizes a
//!   tenant flushes per sync round;
//! - the per-WAL-life setup cost (random key + cipher initialisation);
//! - zstd level 1 on the same buffers, as a reference for "cheap" work
//!   already on the write path.
//!
//! Usage: wal-encryption [sample file]   (defaults to synthetic text)

use std::hint::black_box;
use std::time::{Duration, Instant};

use aes_gcm::aead::{AeadInPlace, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use chacha20poly1305::ChaCha20Poly1305;

const SIZES: [usize; 7] = [128, 512, 2048, 4096, 16 * 1024, 64 * 1024, 1024 * 1024];
const MIN_TIME: Duration = Duration::from_millis(400);

fn sample_data(path: Option<&str>) -> Vec<u8> {
    let mut data = match path {
        Some(p) => std::fs::read(p).expect("read sample file"),
        None => Vec::new(),
    };
    if data.len() < 2 * 1024 * 1024 {
        // Synthetic, WAL-like text if no (or too small a) sample was given.
        let mut i = 0u64;
        while data.len() < 2 * 1024 * 1024 {
            data.extend_from_slice(
                format!(
                    "{{\"EventID\":10,\"SourceImage\":\"C:\\\\Windows\\\\system32\\\\svchost.exe\",\"TargetImage\":\"C:\\\\Windows\\\\System32\\\\lsass.exe\",\"GrantedAccess\":\"0x1010\",\"ProcessId\":{}}}\n",
                    i % 9000
                )
                .as_bytes(),
            );
            i += 1;
        }
    }
    data
}

fn nonce(counter: u64) -> Nonce<aes_gcm::aead::consts::U12> {
    let mut n = [0u8; 12];
    n[..8].copy_from_slice(&counter.to_le_bytes());
    Nonce::clone_from_slice(&n)
}

/// Runs `op` repeatedly for at least MIN_TIME; returns ns per call.
fn bench(mut op: impl FnMut(u64)) -> f64 {
    let mut iters = 1u64;
    loop {
        let start = Instant::now();
        for i in 0..iters {
            op(i);
        }
        let elapsed = start.elapsed();
        if elapsed >= MIN_TIME {
            return elapsed.as_nanos() as f64 / iters as f64;
        }
        iters *= 2;
    }
}

fn main() {
    let arg = std::env::args().nth(1);
    let data = sample_data(arg.as_deref());
    println!(
        "input: {}\n",
        arg.as_deref().unwrap_or("synthetic WAL-like text")
    );

    let aes = Aes256Gcm::new(&Aes256Gcm::generate_key(OsRng));
    let chacha = ChaCha20Poly1305::new(&ChaCha20Poly1305::generate_key(OsRng));
    let ring_key = ring_aes_key();

    println!(
        "| buffer | AES-256-GCM RustCrypto ns | GB/s | AES-256-GCM ring ns | GB/s | ChaCha20-Poly1305 RustCrypto ns | GB/s | zstd-1 ns | GB/s |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for size in SIZES {
        let src = &data[..size];
        let mut buf = src.to_vec();
        let aes_ns = bench(|i| {
            buf.copy_from_slice(src);
            let tag = aes
                .encrypt_in_place_detached(&nonce(i), b"", &mut buf)
                .expect("encrypt");
            black_box(tag);
        });
        let ring_ns = bench(|i| {
            buf.copy_from_slice(src);
            let n = ring::aead::Nonce::assume_unique_for_key(nonce(i).into());
            let tag = ring_key
                .seal_in_place_separate_tag(n, ring::aead::Aad::empty(), &mut buf)
                .expect("encrypt");
            black_box(tag.as_ref().len());
        });
        let chacha_ns = bench(|i| {
            buf.copy_from_slice(src);
            let n = chacha20poly1305::Nonce::clone_from_slice(nonce(i).as_slice());
            let tag = chacha
                .encrypt_in_place_detached(&n, b"", &mut buf)
                .expect("encrypt");
            black_box(tag);
        });
        let mut out = vec![0u8; zstd::zstd_safe::compress_bound(size)];
        let mut cctx = zstd::bulk::Compressor::new(1).expect("zstd");
        let zstd_ns = bench(|_| {
            let n = cctx.compress_to_buffer(src, &mut out).expect("compress");
            black_box(n);
        });
        let gbps = |ns: f64| size as f64 / ns;
        println!(
            "| {} | {:.0} | {:.2} | {:.0} | {:.2} | {:.0} | {:.2} | {:.0} | {:.2} |",
            human(size),
            aes_ns,
            gbps(aes_ns),
            ring_ns,
            gbps(ring_ns),
            chacha_ns,
            gbps(chacha_ns),
            zstd_ns,
            gbps(zstd_ns)
        );
    }

    // Per WAL life: fresh random key + cipher initialisation.
    let setup_aes = bench(|_| {
        black_box(Aes256Gcm::new(&Aes256Gcm::generate_key(OsRng)));
    });
    let setup_chacha = bench(|_| {
        black_box(ChaCha20Poly1305::new(&ChaCha20Poly1305::generate_key(
            OsRng,
        )));
    });
    let setup_ring = bench(|_| {
        black_box(ring_aes_key());
    });
    println!(
        "\nper WAL life (random key + cipher init): AES-256-GCM RustCrypto {setup_aes:.0} ns, AES-256-GCM ring {setup_ring:.0} ns, ChaCha20-Poly1305 {setup_chacha:.0} ns"
    );
}

/// A fresh random AES-256-GCM key for `ring`.
fn ring_aes_key() -> ring::aead::LessSafeKey {
    use ring::rand::SecureRandom;
    let mut key = [0u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut key)
        .expect("random key");
    let unbound = ring::aead::UnboundKey::new(&ring::aead::AES_256_GCM, &key).expect("key");
    ring::aead::LessSafeKey::new(unbound)
}

fn human(n: usize) -> String {
    if n >= 1024 * 1024 {
        format!("{} MiB", n / (1024 * 1024))
    } else if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}
