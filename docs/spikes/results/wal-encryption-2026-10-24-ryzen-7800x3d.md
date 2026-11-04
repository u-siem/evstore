input: otrf/json/execution__cmd_sharpview_pcre_net.jsonl

| buffer | AES-256-GCM RustCrypto ns | GB/s | AES-256-GCM ring ns | GB/s | ChaCha20-Poly1305 RustCrypto ns | GB/s | zstd-1 ns | GB/s |
|---|---|---|---|---|---|---|---|---|
| 128 B | 127 | 1.01 | 68 | 1.88 | 1277 | 0.10 | 2324 | 0.06 |
| 512 B | 317 | 1.62 | 98 | 5.22 | 1355 | 0.38 | 3257 | 0.16 |
| 2 KiB | 1086 | 1.89 | 237 | 8.64 | 2061 | 0.99 | 5889 | 0.35 |
| 4 KiB | 2118 | 1.93 | 418 | 9.81 | 3025 | 1.35 | 8695 | 0.47 |
| 16 KiB | 8433 | 1.94 | 1539 | 10.64 | 8715 | 1.88 | 12862 | 1.27 |
| 64 KiB | 33867 | 1.94 | 6435 | 10.18 | 32056 | 2.04 | 19495 | 3.36 |
| 1 MiB | 541173 | 1.94 | 101350 | 10.35 | 495701 | 2.12 | 265465 | 3.95 |

per WAL life (random key + cipher init): AES-256-GCM RustCrypto 169 ns, AES-256-GCM ring 160 ns, ChaCha20-Poly1305 63 ns
