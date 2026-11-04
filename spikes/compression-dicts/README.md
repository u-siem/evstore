# Spike: bytes per event by tenant size (risk R1)

Questions:
- how much do schema grouping, templates and pre-trained dictionaries reduce storage?
- how much more does a small tenant cost per event than a large one?
- which packing threshold (open point 8.1) works best?

Design context:
- `docs/design/03-tenant-storage-unit.md`;
- `docs/design/09-design-details.md` (packing threshold);
- `docs/design/12-risk-mitigations.md` (R1).

## Input

A directory of JSONL files, **one file per tenant**. The report used the OTRF Security-Datasets Windows host datasets: each one is a recorded attack simulation, so it behaves like a sandbox run. Fetch them yourself (the archives contain attack-tool command lines, so antivirus may block some of them; skip those, don't work around the AV).

## Encodings (zstd, same level for all)

| Encoding | Models |
|---|---|
| `raw_json` | uncompressed JSON lines |
| `json_block64k` | JSON in 64 KiB blocks, one zstd frame each: a generic document store |
| `columnar` | events grouped by schema (Channel + EventID) and sorted by timestamp. Schemas with ≥ `--pack-threshold` events are column-major groups; the rest share a packed row-major group. Values are text, with no typed encodings yet |
| `columnar_tpl` | `columnar` without `Message`, which a template + parameters reconstruct (an upper bound for the template lever) |
| `tpl_dict_row` / `tpl_dict_col` | `columnar_tpl` with pre-trained per-schema dictionaries (trained on row-shaped or column-shaped samples) and a domain dictionary for the packed group |

The columnar encodings include modelled metadata: 128 B per file, 24 B per group, 6 B per column.

## Train/test split

Dictionaries only see training tenants.
- **Default**: by hash of the tenant name. Training and test tenants then share lab environments, which is **optimistic** for dictionaries.
- **`--train-env <suffix>`**: train only on tenants whose hosts match the suffix and test on every other environment. This is the honest setting.

## Run

```
cargo run --release -- <jsonl dir> --level 3 --train-env theshire.local --pack-threshold 100 --sizes 100,1000,10000
```

Each test tenant is measured as prefixes of every size it reaches, and once at full size.
