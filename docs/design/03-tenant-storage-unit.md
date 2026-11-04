# Tenant storage unit

**Layout: adaptive schema groups.** Inside a segment:
- a schema with enough events (threshold chosen per file, e.g. ≥ ~1–5k) gets its **own columnar group**;
- rare schemas share one **packed "mixed" group** (PAX: per-field streams inside chunks, union of fields).

This works because schema volume is power-law per tenant too, so the columnar layout pays off where the volume is and the long tail adds no per-structure overhead.

**One format, two lifecycles**:
- **Sealed tenants**: buffer/WAL during the run. At **seal**, write **one final, globally sorted, fully indexed file** with the manifest in its footer. Result: 1 file per sandbox run.
- **Streaming tenants**: segments per ingest-time partition, merged over time (LSM). **Partition span adapts to volume** (weeks for tiny tenants, hours for large ones) to keep file counts low.

**Schema knowledge levers** (all come from the shared catalog; no tenant data is shared):
1. **Typed encodings**: template + parameters, enum codes, IPs as integers, delta timestamps. No field names, no JSON.
2. **Pre-trained compression per schema** (**after v1**): zstd dictionaries and FSST symbol tables trained by usiem and shipped in the catalog. [Spike 02](../spikes/02-compression-by-tenant-size.md) measured −13% for small tenants and −7–8% for large ones, much less than templates (~−40%) and schema grouping (−30–72%). The format keeps dictionary references from day one.
3. **Per-file string dictionaries**: text and equality predicates are evaluated once per distinct value, then mapped to rows by code.
4. **Schema-chosen indexes and tokenizers**: path, command-line, URL/domain, IP/CIDR; no index for high-cardinality numerics.
5. **Schema-group pruning**: a per-file map `field → groups` plus per-group statistics skips groups that cannot match.
6. **Statistics for aggregations**: per-group and per-chunk counts, min/max, distinct sketches, plus codes. Many aggregations are answered from metadata or codes.
