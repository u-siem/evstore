split: train on environment `theshire.local`, test on all other environments

zstd level 3; pack threshold 500 events per schema; bytes per event (ratio to raw JSON in parentheses)

| tenant size | tenants | events | raw_json | json_block64k | columnar | columnar_tpl | tpl_dict_row | tpl_dict_col |
|---|---|---|---|---|---|---|---|---|
| 100 events | 53 | 5300 | 1563 (1.000) | 122 (0.078) | 83 (0.053) | 46 (0.029) | 40 (0.026) | 40 (0.026) |
| 1000 events | 36 | 36000 | 2005 (1.000) | 111 (0.055) | 50 (0.025) | 33 (0.016) | 32 (0.016) | 32 (0.016) |
| 10000 events | 12 | 120000 | 2030 (1.000) | 74 (0.037) | 21 (0.010) | 12 (0.006) | 12 (0.006) | 12 (0.006) |
| full tenants | 54 | 281306 | 2096 (1.000) | 80 (0.038) | 23 (0.011) | 14 (0.007) | 14 (0.007) | 14 (0.007) |
