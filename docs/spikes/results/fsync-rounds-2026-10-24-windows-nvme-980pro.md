| method | threads | dirty tenants | rounds | round median ms | round p90 ms | round max ms | µs per tenant (median) | write() median ms |
|---|---|---|---|---|---|---|---|---|
| seq_flush | 1 | 10 | 10 | 31.06 | 37.63 | 41.10 | 3105.6 | 0.06 |
| par_flush | 1 | 10 | 10 | 31.19 | 32.46 | 33.55 | 3118.9 | 0.05 |
| par_flush | 8 | 10 | 10 | 13.60 | 13.75 | 18.24 | 1360.0 | 0.05 |
| par_flush | 32 | 10 | 10 | 5.18 | 5.62 | 8.18 | 518.3 | 0.06 |
| write_through | 1 | 10 | 10 | 0.45 | 0.93 | 1.27 | 44.9 | (in round) |
| write_through | 8 | 10 | 10 | 0.33 | 0.62 | 0.65 | 33.2 | (in round) |
| write_through | 32 | 10 | 10 | 0.46 | 0.58 | 0.62 | 45.7 | (in round) |
| write_through_unbuffered | 1 | 10 | 10 | 0.40 | 0.42 | 0.42 | 39.8 | (in round) |
| write_through_unbuffered | 8 | 10 | 10 | 0.29 | 0.31 | 0.31 | 28.7 | (in round) |
| write_through_unbuffered | 32 | 10 | 10 | 0.45 | 0.52 | 1.74 | 45.1 | (in round) |
| write_through+flush1 | 1 | 10 | 10 | 3.58 | 3.77 | 3.82 | 358.1 | (in round) |
| write_through+flush1 | 8 | 10 | 10 | 3.41 | 3.50 | 3.61 | 341.0 | (in round) |
| write_through+flush1 | 32 | 10 | 10 | 3.77 | 4.84 | 7.73 | 377.2 | (in round) |
| write_through_unbuffered+flush1 | 1 | 10 | 10 | 3.44 | 3.70 | 5.23 | 344.0 | (in round) |
| write_through_unbuffered+flush1 | 8 | 10 | 10 | 3.30 | 3.63 | 3.84 | 330.0 | (in round) |
| write_through_unbuffered+flush1 | 32 | 10 | 10 | 3.50 | 3.58 | 5.64 | 349.8 | (in round) |
| seq_flush | 1 | 100 | 10 | 316.58 | 345.86 | 349.50 | 3165.8 | 0.68 |
| par_flush | 1 | 100 | 10 | 318.33 | 324.82 | 328.89 | 3183.3 | 0.68 |
| par_flush | 8 | 100 | 10 | 110.12 | 115.54 | 121.61 | 1101.2 | 0.62 |
| par_flush | 32 | 100 | 10 | 30.54 | 33.11 | 33.33 | 305.4 | 0.46 |
| write_through | 1 | 100 | 10 | 4.74 | 5.66 | 6.19 | 47.4 | (in round) |
| write_through | 8 | 100 | 10 | 0.89 | 0.94 | 1.13 | 8.9 | (in round) |
| write_through | 32 | 100 | 10 | 1.28 | 1.42 | 1.46 | 12.8 | (in round) |
| write_through_unbuffered | 1 | 100 | 10 | 3.00 | 3.43 | 3.54 | 30.0 | (in round) |
| write_through_unbuffered | 8 | 100 | 10 | 0.82 | 0.97 | 0.97 | 8.2 | (in round) |
| write_through_unbuffered | 32 | 100 | 10 | 1.49 | 4.21 | 4.96 | 14.9 | (in round) |
| write_through+flush1 | 1 | 100 | 10 | 7.48 | 8.26 | 8.39 | 74.8 | (in round) |
| write_through+flush1 | 8 | 100 | 10 | 4.15 | 4.32 | 4.52 | 41.5 | (in round) |
| write_through+flush1 | 32 | 100 | 10 | 4.52 | 4.65 | 9.54 | 45.2 | (in round) |
| write_through_unbuffered+flush1 | 1 | 100 | 10 | 6.26 | 6.75 | 11.35 | 62.6 | (in round) |
| write_through_unbuffered+flush1 | 8 | 100 | 10 | 3.98 | 4.09 | 4.32 | 39.8 | (in round) |
| write_through_unbuffered+flush1 | 32 | 100 | 10 | 4.41 | 6.48 | 8.24 | 44.1 | (in round) |
| seq_flush | 1 | 1000 | 9 | 1913.94 | 2229.66 | 2566.19 | 1913.9 | 6.55 |
| par_flush | 1 | 1000 | 6 | 3294.02 | 3483.51 | 3483.51 | 3294.0 | 7.83 |
| par_flush | 8 | 1000 | 10 | 1108.99 | 1142.17 | 1179.68 | 1109.0 | 6.59 |
| par_flush | 32 | 1000 | 10 | 295.29 | 298.51 | 305.13 | 295.3 | 6.11 |
| write_through | 1 | 1000 | 10 | 49.00 | 52.77 | 52.85 | 49.0 | (in round) |
| write_through | 8 | 1000 | 10 | 7.15 | 8.27 | 8.55 | 7.2 | (in round) |
| write_through | 32 | 1000 | 10 | 4.28 | 4.49 | 4.88 | 4.3 | (in round) |
| write_through_unbuffered | 1 | 1000 | 10 | 33.23 | 35.41 | 45.15 | 33.2 | (in round) |
| write_through_unbuffered | 8 | 1000 | 10 | 4.64 | 4.77 | 5.29 | 4.6 | (in round) |
| write_through_unbuffered | 32 | 1000 | 10 | 2.86 | 2.98 | 3.49 | 2.9 | (in round) |
| write_through+flush1 | 1 | 1000 | 10 | 56.23 | 63.16 | 75.97 | 56.2 | (in round) |
| write_through+flush1 | 8 | 1000 | 10 | 11.30 | 12.52 | 15.46 | 11.3 | (in round) |
| write_through+flush1 | 32 | 1000 | 10 | 8.04 | 10.32 | 39.81 | 8.0 | (in round) |
| write_through_unbuffered+flush1 | 1 | 1000 | 10 | 34.94 | 36.56 | 37.09 | 34.9 | (in round) |
| write_through_unbuffered+flush1 | 8 | 1000 | 10 | 7.84 | 8.13 | 8.17 | 7.8 | (in round) |
| write_through_unbuffered+flush1 | 32 | 1000 | 10 | 6.58 | 7.51 | 10.54 | 6.6 | (in round) |
| seq_flush | 1 | 10000 | 3 | 33948.16 | 34184.07 | 34184.07 | 3394.8 | 67.97 |
| par_flush | 1 | 10000 | 3 | 34638.57 | 35647.83 | 35647.83 | 3463.9 | 63.07 |
| par_flush | 8 | 10000 | 3 | 11810.25 | 11837.90 | 11837.90 | 1181.0 | 67.13 |
| par_flush | 32 | 10000 | 6 | 3061.29 | 3121.91 | 3121.91 | 306.1 | 67.41 |
| write_through | 1 | 10000 | 10 | 396.65 | 471.66 | 480.40 | 39.7 | (in round) |
| write_through | 8 | 10000 | 10 | 66.00 | 67.98 | 68.92 | 6.6 | (in round) |
| write_through | 32 | 10000 | 10 | 59.49 | 64.19 | 65.71 | 5.9 | (in round) |
| write_through_unbuffered | 1 | 10000 | 10 | 276.94 | 286.89 | 287.74 | 27.7 | (in round) |
| write_through_unbuffered | 8 | 10000 | 10 | 73.08 | 88.10 | 125.63 | 7.3 | (in round) |
| write_through_unbuffered | 32 | 10000 | 10 | 77.09 | 98.03 | 144.46 | 7.7 | (in round) |
| write_through+flush1 | 1 | 10000 | 10 | 490.01 | 507.27 | 563.63 | 49.0 | (in round) |
| write_through+flush1 | 8 | 10000 | 10 | 75.48 | 81.60 | 103.39 | 7.5 | (in round) |
| write_through+flush1 | 32 | 10000 | 10 | 78.51 | 98.76 | 132.74 | 7.9 | (in round) |
| write_through_unbuffered+flush1 | 1 | 10000 | 10 | 297.89 | 329.89 | 348.56 | 29.8 | (in round) |
| write_through_unbuffered+flush1 | 8 | 10000 | 10 | 85.78 | 97.93 | 105.70 | 8.6 | (in round) |
| write_through_unbuffered+flush1 | 32 | 10000 | 10 | 89.48 | 93.65 | 134.36 | 8.9 | (in round) |
