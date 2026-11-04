split: train on environment `theshire.local`, test on all other environments

zstd level 3; pack threshold 100 events per schema; bytes per event (ratio to raw JSON in parentheses)

| tenant size | tenants | events | raw_json | json_block64k | columnar | columnar_tpl | tpl_dict_row | tpl_dict_col |
|---|---|---|---|---|---|---|---|---|
| 100 events | 53 | 5300 | 1563 (1.000) | 122 (0.078) | 83 (0.053) | 46 (0.029) | 40 (0.026) | 40 (0.026) |
| 1000 events | 36 | 36000 | 2005 (1.000) | 111 (0.055) | 47 (0.023) | 31 (0.015) | 27 (0.014) | 27 (0.013) |
| 10000 events | 12 | 120000 | 2030 (1.000) | 74 (0.037) | 20 (0.010) | 12 (0.006) | 11 (0.006) | 11 (0.006) |
| full tenants | 54 | 281306 | 2096 (1.000) | 80 (0.038) | 23 (0.011) | 14 (0.007) | 13 (0.006) | 13 (0.006) |
