//! Tenant directory.
//!
//! Design: `docs/design/06-tenant-directory.md`.
//!
//! - One small record per tenant (no data derived from event content): state,
//!   role (`Writable` / `ReadOnlyReplica`), writer generation, location,
//!   size, policy, key id.
//! - Embedded ordered key-value store behind a trait, with indexes by state
//!   and by retention policy.
//! - State machine with the directory as the commit point:
//!   `Creating → Active → Sealing → Sealed → (Moving) → Deleting`, plus
//!   `Incomplete`. Startup recovery visits only in-flight tenants.
//! - Tenant groups carry default policies; they never enable cross-tenant
//!   queries.
//!
//! # MVP implementation
//!
//! One store directory is one tenant, so there is no tenant directory yet;
//! the commit point is the store's **manifest**:
//!
//! - Files `manifest-<generation:016x>` hold the live segment list (id,
//!   ingest day, row count, time range, size, compaction level), the field
//!   registry, the WAL generation in use, the next segment id and an opaque
//!   application blob, followed by a CRC32 of everything before it.
//! - A commit writes a brand-new file with the next generation, fsyncs it
//!   (and, on Unix, the directory), then deletes all but the two newest
//!   manifests. Nothing is ever renamed over an existing file, which avoids
//!   Windows' rename-over-open-file pitfalls.
//! - Open picks the newest manifest that validates; a torn newer one is
//!   skipped. Files the manifest does not reference (segments from an
//!   interrupted flush or compaction, stale WALs) are deleted at open.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::codec::{DResult, DecodeError, Reader, crc32, put_len, put_lp};
use crate::error::{Result, StoreError};
use crate::registry::FieldRegistry;

const MAGIC: &[u8; 4] = b"EVMF";
const VERSION: u16 = 1;
const KEEP_MANIFESTS: usize = 2;

pub(crate) fn manifest_file_name(generation: u64) -> String {
    format!("manifest-{generation:016x}")
}

pub(crate) fn parse_manifest_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("manifest-")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// One live segment as recorded in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentEntry {
    pub id: u64,
    /// Ingest day (days since the Unix epoch, UTC): the retention partition.
    pub day: i64,
    pub rows: u64,
    pub ts_min: i64,
    pub ts_max: i64,
    pub bytes: u64,
    /// 0 for flushed segments, `max(inputs) + 1` after compaction.
    pub level: u8,
}

#[derive(Debug, Clone)]
pub(crate) struct Manifest {
    pub generation: u64,
    pub wal_generation: u64,
    pub next_segment: u64,
    pub registry: FieldRegistry,
    pub segments: Vec<SegmentEntry>,
    pub app_meta: Vec<u8>,
}

/// Result of loading the newest valid manifest.
pub(crate) struct Loaded {
    pub manifest: Manifest,
    /// Highest generation seen on disk, valid or not (the next commit must
    /// go above it).
    pub max_generation: u64,
    /// Newer manifests that failed validation.
    pub skipped: usize,
}

impl Manifest {
    pub(crate) fn new(max_fields: usize) -> Manifest {
        Manifest {
            generation: 0,
            wal_generation: 1,
            next_segment: 1,
            registry: FieldRegistry::new(max_fields),
            segments: Vec::new(),
            app_meta: Vec::new(),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.wal_generation.to_le_bytes());
        out.extend_from_slice(&self.next_segment.to_le_bytes());
        self.registry.encode(&mut out);
        put_len(&mut out, self.segments.len());
        for s in &self.segments {
            out.extend_from_slice(&s.id.to_le_bytes());
            out.extend_from_slice(&s.day.to_le_bytes());
            out.extend_from_slice(&s.rows.to_le_bytes());
            out.extend_from_slice(&s.ts_min.to_le_bytes());
            out.extend_from_slice(&s.ts_max.to_le_bytes());
            out.extend_from_slice(&s.bytes.to_le_bytes());
            out.push(s.level);
        }
        put_lp(&mut out, &self.app_meta);
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub(crate) fn decode(data: &[u8], max_fields: usize) -> DResult<Manifest> {
        if data.len() < 4 {
            return Err(DecodeError("manifest too short"));
        }
        let (body, crc) = data.split_at(data.len() - 4);
        if crc32(body) != u32::from_le_bytes(crc.try_into().expect("4 bytes")) {
            return Err(DecodeError("manifest checksum mismatch"));
        }
        let mut r = Reader::new(body);
        if r.bytes(4)? != MAGIC {
            return Err(DecodeError("bad manifest magic"));
        }
        if r.u16()? != VERSION {
            return Err(DecodeError("unsupported manifest version"));
        }
        if r.u16()? != 0 {
            return Err(DecodeError("unsupported manifest flags"));
        }
        let generation = r.u64()?;
        let wal_generation = r.u64()?;
        let next_segment = r.u64()?;
        let registry = FieldRegistry::decode(&mut r, max_fields)?;
        let n = r.len(49)?;
        let mut segments = Vec::with_capacity(n);
        for _ in 0..n {
            segments.push(SegmentEntry {
                id: r.u64()?,
                day: r.i64()?,
                rows: r.u64()?,
                ts_min: r.i64()?,
                ts_max: r.i64()?,
                bytes: r.u64()?,
                level: r.u8()?,
            });
        }
        let app_meta = r.lp_bytes()?.to_vec();
        if !r.is_empty() {
            return Err(DecodeError("trailing bytes in manifest"));
        }
        Ok(Manifest {
            generation,
            wal_generation,
            next_segment,
            registry,
            segments,
            app_meta,
        })
    }

    /// Loads the newest valid manifest in `dir`, if any exists.
    pub(crate) fn load(dir: &Path, max_fields: usize) -> Result<Option<Loaded>> {
        let mut gens = list_generations(dir)?;
        gens.sort_unstable_by(|a, b| b.cmp(a));
        let max_generation = gens.first().copied().unwrap_or(0);
        let mut last_err = None;
        for (skipped, g) in gens.iter().enumerate() {
            let path = dir.join(manifest_file_name(*g));
            let data = fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
            match Manifest::decode(&data, max_fields) {
                Ok(m) if m.generation == *g => {
                    return Ok(Some(Loaded {
                        manifest: m,
                        max_generation,
                        skipped,
                    }));
                }
                Ok(_) => {
                    last_err = Some(StoreError::corrupt(
                        &path,
                        8,
                        "generation does not match file name",
                    ));
                }
                Err(e) => last_err = Some(StoreError::corrupt(&path, 0, e.0)),
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// Writes this manifest as generation `generation` and makes it durable,
    /// then prunes old manifests.
    pub(crate) fn commit(&mut self, dir: &Path, generation: u64) -> Result<()> {
        self.generation = generation;
        let path = dir.join(manifest_file_name(generation));
        let data = self.encode();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        crate::failpoint("manifest-write-mid", || {
            let _ = file.write_all(&data[..data.len() / 2]);
            let _ = file.sync_all();
        });
        file.write_all(&data)
            .map_err(|e| StoreError::io(&path, e))?;
        file.sync_all().map_err(|e| StoreError::io(&path, e))?;
        drop(file);
        sync_dir(dir)?;
        let mut gens = list_generations(dir)?;
        gens.sort_unstable_by(|a, b| b.cmp(a));
        for g in gens
            .into_iter()
            .filter(|g| *g < generation)
            .skip(KEEP_MANIFESTS - 1)
        {
            // Best effort: a leftover old manifest is harmless.
            let _ = fs::remove_file(dir.join(manifest_file_name(g)));
        }
        Ok(())
    }
}

fn list_generations(dir: &Path) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| StoreError::io(dir, e))? {
        let entry = entry.map_err(|e| StoreError::io(dir, e))?;
        if let Some(g) = entry.file_name().to_str().and_then(parse_manifest_name) {
            out.push(g);
        }
    }
    Ok(out)
}

/// Makes directory entries durable (Unix only; Windows has no equivalent
/// and NTFS journals metadata).
#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| StoreError::io(dir, e))?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ValueType;
    use crate::test_util::TempDir;

    fn sample() -> Manifest {
        let mut m = Manifest::new(100);
        m.registry.resolve("host", ValueType::Str);
        m.segments.push(SegmentEntry {
            id: 7,
            day: 20_000,
            rows: 10,
            ts_min: -5,
            ts_max: 99,
            bytes: 1234,
            level: 1,
        });
        m.app_meta = b"hello".to_vec();
        m.wal_generation = 3;
        m.next_segment = 8;
        m
    }

    #[test]
    fn commit_and_load() {
        let dir = TempDir::new("manifest");
        assert!(Manifest::load(dir.path(), 100).unwrap().is_none());
        let mut m = sample();
        m.commit(dir.path(), 1).unwrap();
        m.commit(dir.path(), 2).unwrap();
        m.next_segment = 9;
        m.commit(dir.path(), 3).unwrap();
        let loaded = Manifest::load(dir.path(), 100).unwrap().unwrap();
        assert_eq!(loaded.manifest.generation, 3);
        assert_eq!(loaded.manifest.next_segment, 9);
        assert_eq!(loaded.manifest.segments, m.segments);
        assert_eq!(loaded.manifest.app_meta, b"hello");
        assert_eq!(loaded.manifest.registry.ids("host").len(), 1);
        // Only the two newest manifests remain.
        assert_eq!(list_generations(dir.path()).unwrap().len(), 2);
    }

    #[test]
    fn torn_newest_falls_back() {
        let dir = TempDir::new("manifest-torn");
        let mut m = sample();
        m.commit(dir.path(), 1).unwrap();
        m.next_segment = 50;
        m.commit(dir.path(), 2).unwrap();
        let path = dir.path().join(manifest_file_name(2));
        let data = fs::read(&path).unwrap();
        fs::write(&path, &data[..data.len() - 3]).unwrap();
        let loaded = Manifest::load(dir.path(), 100).unwrap().unwrap();
        assert_eq!(loaded.manifest.generation, 1);
        assert_eq!(loaded.manifest.next_segment, 8);
        assert_eq!(loaded.max_generation, 2);
        assert_eq!(loaded.skipped, 1);
        // Every manifest corrupt: an error, not an empty store.
        let p1 = dir.path().join(manifest_file_name(1));
        fs::write(&p1, b"garbage").unwrap();
        assert!(Manifest::load(dir.path(), 100).is_err());
    }
}
