# Documentation

Architecture of usiem-evstore at a medium level of detail: the problem, the chosen design, and the reasons for it. It is not an implementation plan.

## Reading order

| # | Document | Contents |
|---|---|---|
| 1 | [Problem definition](design/01-problem.md) | What we build, why, scale and isolation requirements, workload, success criteria, key tensions T1–T7 |
| 2 | [Architecture overview](design/02-architecture-overview.md) | Shared vs per-tenant parts of an instance, write and read paths |
| 3 | [Tenant storage unit](design/03-tenant-storage-unit.md) | Adaptive schema groups, sealed vs streaming tenants, schema-knowledge levers (T2, T5) |
| 4 | [Durability and buffers](design/04-durability-and-buffers.md) | Durability classes, coordinated group sync, WAL-as-buffer, WAL pool (T1) |
| 5 | [Schema catalog](design/05-schema-catalog.md) | Content-addressed catalog, release bundles, private catalogs, evolution rules (T4) |
| 6 | [Tenant directory](design/06-tenant-directory.md) | Directory records, state machine, recovery, retention, groups (T3) |
| 7 | [Replication channel](design/07-replication-channel.md) | Replication records, `ReplicationSink` trait, replicas and fencing, fleet search outside the library (T6) |
| 8 | [Components](design/08-components.md) | Segment internals, indexes, schemaless fields, query execution, retention, tiering, tamper evidence, cross-platform constraints |
| 9 | [Design details](design/09-design-details.md) | Packing threshold, text index policy, segment limits, failure behaviour, aggregation model |
| 10 | [Production readiness](design/10-production-readiness.md) | Requirements by area and priority |
| 11 | [Cost, risks and benchmarks](design/11-cost-risks-benchmarks.md) | Estimates vs baselines, risks R1–R6, experiments to run first, benchmark plan (T7) |
| 12 | [Risk mitigations](design/12-risk-mitigations.md) | Mitigation design and acceptance thresholds for R1–R6 |
| — | [Decisions log](decisions.md) | Every decision taken, in one table |
| — | [Spikes](spikes/README.md) | Experiments that validate design assumptions, with hardware, results and consequences |

## Conventions for these documents

- `§N` links refer to the section numbering of the original single design document; each one points to the file that now holds that section.
- Every change of direction is recorded in [decisions.md](decisions.md) together with the document it affects.
- Estimates are labelled as estimates until measured.
