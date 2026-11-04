selective value: Image = System

| events | WAL MiB | segment MiB | segment build ms (µs/event) | query | matches | WAL ms | segment ms | WAL / segment |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0.89 | 0.07 | 12.8 (12.8) | Image = <0.5% value> | 4 | 0.16 | 0.05 | 3.1× |
|  |  | |  | CommandLine contains "whoami" | 0 | 0.16 | 0.00 | 51.9× |
|  |  | |  | EventID = 1 | 4 | 0.16 | 0.04 | 3.6× |
| 10000 | 9.19 | 0.26 | 93.8 (9.4) | Image = <0.5% value> | 12 | 3.23 | 0.56 | 5.8× |
|  |  | |  | CommandLine contains "whoami" | 0 | 3.18 | 0.01 | 361.0× |
|  |  | |  | EventID = 1 | 14 | 3.19 | 0.09 | 34.2× |
| 50000 | 62.91 | 0.69 | 511.0 (10.2) | Image = <0.5% value> | 304 | 22.89 | 0.71 | 32.1× |
|  |  | |  | CommandLine contains "whoami" | 4 | 23.10 | 0.19 | 124.8× |
|  |  | |  | EventID = 1 | 166 | 22.87 | 0.10 | 222.7× |
| 200000 | 234.73 | 2.43 | 3136.3 (15.7) | Image = <0.5% value> | 1022 | 87.28 | 2.28 | 38.3× |
|  |  | |  | CommandLine contains "whoami" | 8 | 88.00 | 0.55 | 160.1× |
|  |  | |  | EventID = 1 | 412 | 86.81 | 0.12 | 742.6× |
