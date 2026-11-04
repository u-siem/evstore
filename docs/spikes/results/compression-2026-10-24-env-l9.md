split: train on environment `theshire.local`, test on all other environments

zstd level 9; bytes per event (ratio to raw JSON in parentheses)

| tenant size | tenants | events | raw_json | json_block64k | columnar | columnar_tpl | columnar_tpl_dict |
|---|---|---|---|---|---|---|---|
| 100 events | 53 | 5300 | 1563 (1.000) | 108 (0.069) | 73 (0.047) | 41 (0.026) | 34 (0.022) |
| 1000 events | 36 | 36000 | 2005 (1.000) | 100 (0.050) | 41 (0.021) | 29 (0.014) | 29 (0.014) |
| 10000 events | 12 | 120000 | 2030 (1.000) | 67 (0.033) | 17 (0.009) | 11 (0.005) | 11 (0.005) |
| full tenants | 54 | 281306 | 2096 (1.000) | 72 (0.034) | 20 (0.009) | 13 (0.006) | 13 (0.006) |
