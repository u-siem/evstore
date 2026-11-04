# Spike 02: bytes per event by tenant size (risk R1)

- **Date**: 2026-10-24
- **Code**: [`spikes/compression-dicts`](../../spikes/compression-dicts/README.md)
- **Raw results**: [`results/compression-2026-10-24-*.md`](results/)
- **Questions**:
  - how much do schema grouping, templates and pre-trained dictionaries save?
  - does a tiny tenant meet the R1 thresholds ([risk mitigations](../design/12-risk-mitigations.md)): bytes/event at 1k events ≤ 2× a large tenant's, and at 100 events ≤ 4×?
  - which packing threshold works best ([open point 8.1](../design/09-design-details.md))?

## Data

[OTRF Security-Datasets](https://github.com/OTRF/Security-Datasets), Windows host datasets: Sysmon, Security, PowerShell and others. Each dataset is one recorded attack simulation and is treated as **one tenant**.
- 113 datasets downloaded; **16 were blocked by Windows Defender** (they contain attack-tool command lines) and were excluded.
- **97 tenants**, 572,641 events, ~2.25 KB of JSON per event. Tenant size: 68–40,569 events, median 2,395.
- 330 schemas (Channel + EventID), power-law distributed; Sysmon 10/12/7/13 dominate.
- `Message` (the rendered event text, reconstructable from a template) is **36% of all bytes**.

**Split used for the headline results**: dictionaries trained only on the `theshire.local` lab (43 tenants), tested on the 54 tenants from the other ~11 lab environments (different hosts, domains, users and SIDs). This mimics uSIEM shipping dictionaries trained on its own reference corpus to a customer whose environment it has never seen.

## Results: zstd level 3, strict split, pack threshold 100 events per schema

Bytes per event (ratio to raw JSON):

| Tenant size | Tenants | Generic doc store (`json_block64k`) | Schema-grouped columns | + templates (no `Message`) | + pre-trained dictionaries |
|---|---|---|---|---|---|
| 100 events | 53 | 122 (0.078) | 83 (0.053) | 46 (0.029) | **40** (0.026) |
| 1,000 events | 36 | 111 (0.055) | 47 (0.023) | 31 (0.015) | **27** (0.014) |
| 10,000 events | 12 | 74 (0.037) | 20 (0.010) | 12 (0.006) | **11** (0.006) |
| full tenants | 54 | 80 (0.038) | 23 (0.011) | 14 (0.007) | **13** (0.006) |

Other runs (raw files):
- **zstd level 9** (the "recompress on merge/seal" level) saves another ~10–15% at every size. With threshold 2,000 and dictionaries: 100 events 40 → 34, 1,000 events 32 → 29, full tenants 15 → 13.
- **Hash split** (training and test share lab environments) gives dictionaries a slightly larger benefit, as expected.
- **Column-shaped vs row-shaped dictionary training** made no measurable difference.

### Pack threshold sweep (same setup, with dictionaries)

| Threshold | 100 events | 1,000 events | 10,000 events | full |
|---|---|---|---|---|
| 2,000 (current fallback in 8.1) | 40 | 32 | 13 | 15 |
| 500 | 40 | 32 | 12 | 14 |
| **100** | **40** | **27** | **11** | **13** |
| 20 | 40 | 27 | 12 | 13 |

## Findings

1. **The two big levers are schema grouping and templates, not dictionaries.**
   - Schema-grouped columns: −30% (100 events) to −72% (full tenants) against a generic doc store.
   - Dropping the template-derivable `Message` saves another ~40% at every size.
   - Pre-trained dictionaries add **−13% for tiny and 1k tenants and −7–8% for large ones**. The design called them "the biggest help for small tenants"; on this data they help, but modestly.
2. **The design beats a generic doc store at every tenant size**: 3× at 100 events, 4× at 1,000, 6× at full size (40 vs 122, 27 vs 111, 13 vs 80 bytes/event).
3. **R1 relative thresholds**, measured against the 10k bucket (11 B/event) or full tenants (13 B/event):
   - 100 events: **3.1–3.6×**, within the ≤ 4× threshold;
   - 1,000 events: **2.1–2.5×**, narrowly **above** the ≤ 2× threshold.

   The large-tenant baseline compresses so well (~160× vs raw JSON) that a relative threshold is a hard bar. In absolute terms small tenants are still 3–4× better than the generic store at the same size.
4. **The packing fallback of 2,000 events per schema is too high.** Around 100 events per schema is best: −16% at 1k events, −13% for full tenants. Going lower (20) gains nothing and makes tiny tenants slightly worse without dictionaries.

## Caveats

- **Lab data is repetitive.** The same tooling, few hosts and Sysmon-heavy content mean absolute ratios (13 B/event, ~160×) are optimistic for production. Relative comparisons between encodings are more trustworthy than absolute numbers.
- **The template lever is an upper bound.** It assumes `Message` adds nothing beyond the other fields. That holds for most Windows events, but not for every schema (some PowerShell events).
- **Conservative elsewhere.**
  - Values are stored as text: there are no typed integer or IP encodings, no structured string decomposition (R2) and no FSST.
  - Metadata overhead is modelled, not measured from a real format.
- **The training corpus is small**: one lab environment and 49 schema dictionaries. A real uSIEM corpus would be broader, which could change the dictionary benefit in either direction.

## Design consequences (decided 2026-10-24, see [decisions](../decisions.md))

- **Keep templates as a must-have** and push schema-grouped columns at all tenant sizes: these carry the density thesis.
- **Lower the packing fallback in 8.1 from 2,000 to ~100 events per schema.**
- **Pre-trained dictionaries become a secondary optimisation.** The format keeps dictionary references from day one; the training pipeline and shipping can come after v1.
- **Replace R1's relative thresholds with an absolute comparison**, e.g. "a tenant of N events must be ≥ 3× smaller than a generic doc store at the same N". The relative threshold mostly measures how well large tenants compress.
