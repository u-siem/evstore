| method | threads | dirty tenants | rounds | round median ms | round p90 ms | round max ms | µs per tenant (median) | write() median ms |
|---|---|---|---|---|---|---|---|---|
| seq_flush | 1 | 10 | 10 | 10.44 | 10.69 | 11.39 | 1043.9 | 0.05 |
| par_flush | 1 | 10 | 10 | 10.92 | 13.95 | 13.97 | 1092.1 | 0.04 |
| par_flush | 8 | 10 | 10 | 7.61 | 8.48 | 9.65 | 760.6 | 0.05 |
| par_flush | 32 | 10 | 10 | 5.39 | 5.55 | 7.07 | 538.9 | 0.04 |
| write_through | 1 | 10 | 10 | 0.66 | 0.72 | 0.77 | 65.9 | (in round) |
| write_through | 8 | 10 | 10 | 0.45 | 0.48 | 0.56 | 45.5 | (in round) |
| write_through | 32 | 10 | 10 | 0.53 | 0.74 | 54.73 | 52.5 | (in round) |
| write_through_unbuffered | 1 | 10 | 10 | 0.58 | 0.62 | 0.66 | 58.5 | (in round) |
| write_through_unbuffered | 8 | 10 | 10 | 0.33 | 0.35 | 0.35 | 33.5 | (in round) |
| write_through_unbuffered | 32 | 10 | 10 | 0.41 | 0.45 | 1.14 | 40.8 | (in round) |
| write_through+flush1 | 1 | 10 | 10 | 2.50 | 2.85 | 2.99 | 249.5 | (in round) |
| write_through+flush1 | 8 | 10 | 10 | 2.11 | 2.16 | 2.48 | 211.4 | (in round) |
| write_through+flush1 | 32 | 10 | 10 | 2.20 | 2.26 | 2.26 | 219.7 | (in round) |
| write_through_unbuffered+flush1 | 1 | 10 | 10 | 2.30 | 2.40 | 2.49 | 229.9 | (in round) |
| write_through_unbuffered+flush1 | 8 | 10 | 10 | 2.08 | 2.23 | 2.30 | 208.1 | (in round) |
| write_through_unbuffered+flush1 | 32 | 10 | 10 | 2.17 | 2.19 | 2.27 | 217.2 | (in round) |
| seq_flush | 1 | 100 | 10 | 108.92 | 121.41 | 122.08 | 1089.2 | 0.56 |
| par_flush | 1 | 100 | 10 | 107.24 | 116.12 | 117.70 | 1072.4 | 0.44 |
| par_flush | 8 | 100 | 10 | 58.18 | 65.04 | 65.06 | 581.8 | 0.41 |
| par_flush | 32 | 100 | 10 | 24.51 | 25.76 | 28.39 | 245.1 | 0.40 |
| write_through | 1 | 100 | 10 | 6.39 | 7.62 | 7.85 | 63.9 | (in round) |
| write_through | 8 | 100 | 10 | 2.03 | 2.18 | 2.47 | 20.3 | (in round) |
| write_through | 32 | 100 | 10 | 1.53 | 1.60 | 1.68 | 15.3 | (in round) |
| write_through_unbuffered | 1 | 100 | 10 | 5.60 | 7.77 | 9.17 | 56.0 | (in round) |
| write_through_unbuffered | 8 | 100 | 10 | 1.88 | 2.02 | 2.92 | 18.8 | (in round) |
| write_through_unbuffered | 32 | 100 | 10 | 1.56 | 2.13 | 2.83 | 15.6 | (in round) |
| write_through+flush1 | 1 | 100 | 10 | 8.34 | 8.78 | 8.96 | 83.4 | (in round) |
| write_through+flush1 | 8 | 100 | 10 | 5.63 | 5.88 | 6.27 | 56.3 | (in round) |
| write_through+flush1 | 32 | 100 | 10 | 5.52 | 6.03 | 6.17 | 55.2 | (in round) |
| write_through_unbuffered+flush1 | 1 | 100 | 10 | 7.52 | 7.93 | 8.02 | 75.2 | (in round) |
| write_through_unbuffered+flush1 | 8 | 100 | 10 | 5.49 | 5.71 | 6.03 | 54.9 | (in round) |
| write_through_unbuffered+flush1 | 32 | 100 | 10 | 5.45 | 5.62 | 5.65 | 54.5 | (in round) |
| seq_flush | 1 | 1000 | 10 | 1104.47 | 1112.15 | 1155.62 | 1104.5 | 5.39 |
| par_flush | 1 | 1000 | 10 | 1094.27 | 1102.51 | 1102.54 | 1094.3 | 5.48 |
| par_flush | 8 | 1000 | 10 | 568.56 | 573.13 | 576.13 | 568.6 | 6.39 |
| par_flush | 32 | 1000 | 10 | 217.22 | 219.16 | 234.36 | 217.2 | 5.19 |
| write_through | 1 | 1000 | 10 | 65.29 | 67.94 | 71.96 | 65.3 | (in round) |
| write_through | 8 | 1000 | 10 | 15.41 | 15.47 | 19.26 | 15.4 | (in round) |
| write_through | 32 | 1000 | 10 | 12.00 | 16.44 | 16.76 | 12.0 | (in round) |
| write_through_unbuffered | 1 | 1000 | 10 | 54.76 | 56.18 | 59.43 | 54.8 | (in round) |
| write_through_unbuffered | 8 | 1000 | 10 | 14.27 | 14.73 | 14.79 | 14.3 | (in round) |
| write_through_unbuffered | 32 | 1000 | 10 | 11.83 | 11.97 | 12.29 | 11.8 | (in round) |
| write_through+flush1 | 1 | 1000 | 10 | 65.94 | 70.17 | 71.76 | 65.9 | (in round) |
| write_through+flush1 | 8 | 1000 | 10 | 19.70 | 19.88 | 56.31 | 19.7 | (in round) |
| write_through+flush1 | 32 | 1000 | 10 | 16.40 | 16.57 | 17.48 | 16.4 | (in round) |
| write_through_unbuffered+flush1 | 1 | 1000 | 10 | 55.27 | 57.04 | 57.21 | 55.3 | (in round) |
| write_through_unbuffered+flush1 | 8 | 1000 | 10 | 18.67 | 22.87 | 22.94 | 18.7 | (in round) |
| write_through_unbuffered+flush1 | 32 | 1000 | 10 | 16.56 | 16.69 | 16.89 | 16.6 | (in round) |
| seq_flush | 1 | 10000 | 3 | 11006.22 | 11077.88 | 11077.88 | 1100.6 | 51.70 |
| par_flush | 1 | 10000 | 3 | 10963.87 | 11150.01 | 11150.01 | 1096.4 | 59.22 |
| par_flush | 8 | 10000 | 3 | 5620.03 | 5697.80 | 5697.80 | 562.0 | 57.77 |
| par_flush | 32 | 10000 | 8 | 2213.74 | 2222.98 | 2256.60 | 221.4 | 53.43 |
| write_through | 1 | 10000 | 10 | 680.88 | 699.64 | 753.94 | 68.1 | (in round) |
| write_through | 8 | 10000 | 10 | 153.70 | 160.16 | 207.58 | 15.4 | (in round) |
| write_through | 32 | 10000 | 10 | 119.54 | 126.74 | 169.59 | 12.0 | (in round) |
| write_through_unbuffered | 1 | 10000 | 10 | 546.93 | 554.07 | 564.21 | 54.7 | (in round) |
| write_through_unbuffered | 8 | 10000 | 10 | 144.74 | 149.55 | 191.46 | 14.5 | (in round) |
| write_through_unbuffered | 32 | 10000 | 10 | 115.63 | 120.95 | 162.07 | 11.6 | (in round) |
| write_through+flush1 | 1 | 10000 | 10 | 633.31 | 646.62 | 665.37 | 63.3 | (in round) |
| write_through+flush1 | 8 | 10000 | 10 | 157.04 | 163.03 | 200.53 | 15.7 | (in round) |
| write_through+flush1 | 32 | 10000 | 10 | 120.82 | 133.99 | 167.35 | 12.1 | (in round) |
| write_through_unbuffered+flush1 | 1 | 10000 | 10 | 531.54 | 539.68 | 543.16 | 53.2 | (in round) |
| write_through_unbuffered+flush1 | 8 | 10000 | 10 | 148.33 | 153.56 | 194.73 | 14.8 | (in round) |
| write_through_unbuffered+flush1 | 32 | 10000 | 10 | 120.16 | 125.02 | 131.19 | 12.0 | (in round) |
