# Fleet use: the replication channel


**Principle**: like SQLite with Litestream or libSQL replication, the library **emits** every committed change and can **apply** changes from elsewhere, but never decides who is primary or where tenants live. No membership, election, consensus or cross-instance query engine.

## Replication records
Every committed change of a tenant becomes a **replication record**, delivered **in commit order per tenant**:
| Record | Content |
|---|---|
| `TenantCreated` | id, group, policy, durability class, writer generation |
| `Batch` | sequence range, the **same schema-encoded bytes as the WAL** (no re-encoding), catalog references, tamper-chain hash |
| `Sealed` | sealed file hash + Merkle root (file shipped separately, see below) |
| `PolicyChanged`, `RetentionRemoved`, `Deleted` | lifecycle changes, including what retention removed |
| `Promoted` | new writer generation (see fencing) |

- **One record = one tenant**: records never mix tenants, preserving physical separation in transit. Payloads can optionally be **encrypted with the tenant's key**, so a sink (Kafka, S3) never sees plaintext.
- **Ordering**: total order per tenant by sequence number; **no ordering across tenants**.
- **Format**: stable and versioned (it is the WAL record format plus a small envelope).

## The trait (sketch, signatures indicative)
```rust
pub trait ReplicationSink: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// Deliver records; per-tenant order is preserved across calls.
    fn deliver(&self, records: &[ReplicationRecord]) -> Result<(), SinkError>;
    /// Optional fast path: ship a sealed tenant as one immutable file.
    fn deliver_sealed_file(&self, tenant: TenantId, file: &SealedFile) -> Result<(), SinkError> {
        Err(SinkError::Unsupported)
    }
}
// Progress is confirmed separately, so sinks can be asynchronous:
db.replication().ack(sink_name, tenant, up_to_seq);

// Receiving side (another library instance):
db.apply_replica(record)?;          // idempotent by (tenant, seq); tenant is ReadOnlyReplica here
```
- **Registration**: sinks are registered on the database, optionally filtered by tenant group (e.g. replicate only MSSP tenants, archive only sandbox runs).
- **Delivery semantics**: **at-least-once**, with **idempotent apply** by `(tenant, seq)`, so receivers see each change effectively once. A gap in sequence numbers makes `apply_replica` return `NeedsResync(from_seq)`.
- **Cursors**: the library persists a cursor per `(sink, tenant)` in the tenant's manifest and resumes from it after a restart.

## Coupling with local writes
- **Default: asynchronous.** Replication never slows local writes. Each sink has a bounded queue and a **lag policy**:
  - `Block`: writers to affected tenants get backpressure;
  - `Retain`: keep un-acked WAL ranges on disk up to a budget (bytes/age), the Litestream model;
  - `Detach`: stop replicating that tenant and mark it `NeedsResync`.
- **Opt-in synchronous**: durability class `SinkAck(sink)` ([§3.2](04-durability-and-buffers.md)) makes a write return only after that sink acks it.
- **Resync / snapshot**: when a sink is too far behind (beyond retention), the library ships a **snapshot**: the sealed file, or the current segments plus the WAL tail, then continues with records.
- **Sealed tenants**: at seal, `deliver_sealed_file` ships one immutable file. That is the cheapest possible replica for sandbox runs, and the natural archive unit for object storage.

## Replicas and fencing without election
- On a receiving instance, a replicated tenant has role **`ReadOnlyReplica`**: it serves queries, rejects local writes and keeps applying records.
- **Writer generation**: each tenant carries a generation number. `db.promote(tenant)` (called by the application) makes a replica `Writable` and increments the generation; subsequent records carry it.
- Any instance **rejects records from an older generation** than it knows. A "zombie" old writer that comes back is refused by replicas, and learns it was demoted when it sees a newer generation.
- **The library does not prevent two writers**: deciding which instance is the writer is the application's job (its own config, orchestration or coordination service). The library guarantees that **mixing generations is detected and refused**, never silently merged.

## Catalog and integrity on the receiving side
- Records reference catalog objects by hash. A receiver missing an object fetches it through a **`CatalogProvider` trait** (from the sender, an object store or a usiem release). Bundles are signed, so trust is preserved.
- Batch records carry the tenant's **tamper-chain hashes**, so a replica verifies continuity and detects loss or tampering in transit ([§4.7](08-components.md)).

## Sink implementations (separate optional crates, not core)
- **Peer sink**: TCP/QUIC + TLS to another usiem instance calling `apply_replica` (one connection may carry many tenants, but frames and buffers are per tenant).
- **Kafka/NATS sink**: one topic or subject per group, keyed by tenant.
- **Object-store archiver**: Litestream-style WAL chunks + sealed files to S3/Azure/GCS/MinIO; restore = `apply_replica` from the archive.
- **Directory mirror**: for tests and local backups.

## Fleet search outside the library
- The query API works on one tenant per call and can return **serializable, mergeable partial results** (time-ordered hit streams with cursors, partial aggregates: counts, sums, sketches). An external layer in usiem fans out over instances and merges.
- **Huge tenants**: split by the application into several tenants and federated the same way.

**Object storage** (optional, [§4.6](08-components.md)) remains a tiering target of the local instance, independent of replication.
