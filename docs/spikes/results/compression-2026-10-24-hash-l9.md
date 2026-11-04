split: hash of tenant name (train and test share lab environments)

zstd level 9; bytes per event (ratio to raw JSON in parentheses)

| tenant size | tenants | events | raw_json | json_block64k | columnar | columnar_tpl | columnar_tpl_dict |
|---|---|---|---|---|---|---|---|
| 100 events | 67 | 6700 | 2095 (1.000) | 152 (0.073) | 80 (0.038) | 51 (0.024) | 39 (0.019) |
| 1000 events | 49 | 49000 | 2418 (1.000) | 146 (0.060) | 44 (0.018) | 31 (0.013) | 30 (0.012) |
| 10000 events | 10 | 100000 | 2268 (1.000) | 104 (0.046) | 22 (0.010) | 14 (0.006) | 15 (0.007) |
| full tenants | 68 | 342707 | 2405 (1.000) | 113 (0.047) | 24 (0.010) | 16 (0.007) | 16 (0.007) |
