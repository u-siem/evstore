//! The single-tenant [`Store`] (MVP).

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::codec::put_field;
use crate::directory::{Manifest, SegmentEntry, parse_manifest_name};
use crate::error::{Result, StoreError};
use crate::model::{Record, Value, ValueType};
use crate::query::{self, Filter, Merge, Query, RecordIter, SegmentScan, Source, TimeRange};
use crate::registry::{FieldRegistry, OVERFLOW_FIELD};
use crate::retention::{self, RetentionPolicy, day_of};
use crate::segment::{
    DEFAULT_CHUNK_ROWS, Segment, SegmentWriter, StrEncoding, choose_plain, parse_segment_name,
    segment_file_name, write_segment,
};
use crate::wal::{SyncPolicy, Wal, parse_wal_name};

/// Store configuration.
#[derive(Debug, Clone)]
pub struct Options {
    /// When appends are fsynced.
    pub sync: SyncPolicy,
    /// Flush the WAL into a segment once buffered rows reach this many bytes
    /// (approximate in-memory size).
    pub flush_bytes: usize,
    /// Flush a non-empty WAL at the next `maintain` once it is this old.
    pub flush_age: Duration,
    /// Field identity cap; later fields go to the overflow field.
    pub max_fields: usize,
    /// Rows per segment chunk.
    pub chunk_rows: usize,
    /// zstd level for flushed segments.
    pub zstd_level: i32,
    /// zstd level for compacted segments.
    pub compact_zstd_level: i32,
    /// Segments of one day and level merged at once.
    pub compaction_fanout: usize,
    /// What `maintain` drops.
    pub retention: RetentionPolicy,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync: SyncPolicy::default(),
            flush_bytes: 8 << 20,
            flush_age: Duration::from_secs(300),
            max_fields: 2000,
            chunk_rows: DEFAULT_CHUNK_ROWS,
            zstd_level: 3,
            compact_zstd_level: 9,
            compaction_fanout: 8,
            retention: RetentionPolicy::default(),
        }
    }
}

/// Acknowledgement of an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    /// Records appended by this call.
    pub appended: usize,
    /// Records in the store after the call.
    pub total: u64,
    /// Whether the records are already fsynced (see [`SyncPolicy`]).
    pub synced: bool,
}

/// What happened while opening the store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Records replayed from the WAL.
    pub wal_records: usize,
    /// Bytes cut from a torn WAL tail.
    pub wal_truncated_bytes: u64,
    /// Newer manifests skipped because they failed validation.
    pub manifests_skipped: usize,
    /// Unreferenced files deleted (interrupted flushes or compactions).
    pub orphans_removed: usize,
}

/// What a [`Store::maintain`] call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintainReport {
    /// The WAL was flushed into a segment.
    pub flushed: bool,
    /// Segments dropped by retention.
    pub segments_dropped: usize,
    /// Compactions performed.
    pub compactions: usize,
    /// Files still waiting for deletion (in use by readers).
    pub pending_deletes: usize,
}

/// Summary statistics of a store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Records in segments.
    pub segment_rows: u64,
    /// Records in the WAL buffer.
    pub buffered_rows: u64,
    /// Live segments.
    pub segments: usize,
    /// Bytes in live segments.
    pub segment_bytes: u64,
    /// Bytes in the current WAL.
    pub wal_bytes: u64,
    /// Oldest and newest record time, if any.
    pub time_range: Option<(i64, i64)>,
}

/// An embedded, single-tenant event store rooted at one directory.
///
/// Writes go to a WAL and an in-memory buffer; [`flush`](Store::flush) (or
/// [`maintain`](Store::maintain), which the application calls periodically)
/// turns them into an immutable segment. The store starts no threads.
///
/// `search` takes `&self` and returns an owning iterator, so readers can run
/// under a shared lock while writers take an exclusive one.
pub struct Store {
    dir: PathBuf,
    opts: Options,
    _lock: File,
    manifest: Manifest,
    max_generation: u64,
    segments: Vec<Arc<Segment>>,
    wal: Wal,
    buffer: Vec<Record>,
    buffer_bytes: usize,
    pending_delete: Vec<PathBuf>,
    app_meta_dirty: bool,
    recovery: Recovery,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("dir", &self.dir)
            .field("segments", &self.segments.len())
            .field("buffered", &self.buffer.len())
            .finish()
    }
}

/// Current wall-clock time in nanoseconds since the Unix epoch.
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

impl Store {
    /// Opens (or creates) the store in `dir`, recovering from any crash.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Store> {
        if opts.chunk_rows == 0 || opts.flush_bytes == 0 {
            return Err(StoreError::InvalidArgument(
                "chunk_rows and flush_bytes must be positive".into(),
            ));
        }
        let dir = dir.as_ref();
        fs::create_dir_all(dir).map_err(|e| StoreError::io(dir, e))?;
        // Canonical paths are `\\?\`-prefixed on Windows, lifting MAX_PATH.
        let dir = fs::canonicalize(dir).map_err(|e| StoreError::io(dir, e))?;
        let lock_path = dir.join("LOCK");
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| StoreError::io(&lock_path, e))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Err(StoreError::Locked(dir)),
            Err(fs::TryLockError::Error(e)) => return Err(StoreError::io(&lock_path, e)),
        }

        let mut recovery = Recovery::default();
        let (mut manifest, mut max_generation) = match Manifest::load(&dir, opts.max_fields)? {
            Some(loaded) => {
                recovery.manifests_skipped = loaded.skipped;
                (loaded.manifest, loaded.max_generation)
            }
            None => {
                // Fresh store: anything here is debris from an interrupted init.
                remove_matching(&dir, |n| {
                    parse_wal_name(n).is_some() || parse_segment_name(n).is_some()
                })?;
                let mut m = Manifest::new(opts.max_fields);
                Wal::create(&dir, m.wal_generation)?;
                m.commit(&dir, 1)?;
                (m, 1)
            }
        };
        max_generation = max_generation.max(manifest.generation);

        let mut segments = Vec::with_capacity(manifest.segments.len());
        for entry in &manifest.segments {
            segments.push(Arc::new(Segment::open(
                &dir.join(segment_file_name(entry.id)),
            )?));
        }

        let wal_path = dir.join(crate::wal::wal_file_name(manifest.wal_generation));
        let (wal, replay) = if wal_path.exists() {
            Wal::open(&dir, manifest.wal_generation)?
        } else {
            (
                Wal::create(&dir, manifest.wal_generation)?,
                Default::default(),
            )
        };
        recovery.wal_records = replay.records.len();
        recovery.wal_truncated_bytes = replay.truncated_bytes;

        let live_segments: Vec<u64> = manifest.segments.iter().map(|s| s.id).collect();
        let live_wal = manifest.wal_generation;
        recovery.orphans_removed = remove_matching(&dir, |n| {
            parse_segment_name(n).is_some_and(|id| !live_segments.contains(&id))
                || parse_wal_name(n).is_some_and(|g| g != live_wal)
        })?;
        // Ids above anything on disk, even if a manifest recorded less.
        if let Some(max) = live_segments.iter().max() {
            manifest.next_segment = manifest.next_segment.max(max + 1);
        }

        register_fields(&mut manifest.registry, &replay.records);
        let buffer_bytes = replay.records.iter().map(Record::approx_size).sum();
        Ok(Store {
            dir,
            opts,
            _lock: lock,
            manifest,
            max_generation,
            segments,
            wal,
            buffer: replay.records,
            buffer_bytes,
            pending_delete: Vec::new(),
            app_meta_dirty: false,
            recovery,
        })
    }

    /// The store directory (canonical form).
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// What happened at open.
    pub fn recovery(&self) -> Recovery {
        self.recovery
    }

    /// The options the store was opened with.
    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// Appends records. They are searchable immediately and durable once
    /// synced (see [`Ack::synced`]). A batch is atomic across crashes.
    ///
    /// Flushes into a segment when the buffer exceeds `flush_bytes`.
    pub fn append(&mut self, records: Vec<Record>) -> Result<Ack> {
        if records
            .iter()
            .any(|r| r.fields.iter().any(|(n, _)| n == OVERFLOW_FIELD))
        {
            return Err(StoreError::InvalidArgument(format!(
                "field name {OVERFLOW_FIELD} is reserved"
            )));
        }
        let synced = self.wal.append(&records, self.opts.sync)?;
        register_fields(&mut self.manifest.registry, &records);
        let appended = records.len();
        self.buffer_bytes += records.iter().map(Record::approx_size).sum::<usize>();
        self.buffer.extend(records);
        let mut synced = synced;
        if self.buffer_bytes >= self.opts.flush_bytes {
            self.flush_at(now_ns())?;
            synced = true;
        }
        Ok(Ack {
            appended,
            total: self.len(),
            synced,
        })
    }

    /// fsyncs pending WAL appends.
    pub fn sync(&mut self) -> Result<()> {
        self.wal.sync()
    }

    /// Runs a query.
    pub fn search(&self, query: &Query) -> Result<RecordIter> {
        query::execute(&self.segments, &self.buffer, query)
    }

    /// Runs an aggregation (see [`AggQuery`](crate::AggQuery)). Row order
    /// is unspecified.
    pub fn aggregate(&self, query: &crate::AggQuery) -> Result<Vec<crate::AggRow>> {
        crate::aggregate::aggregate(&self.segments, &self.buffer, query)
    }

    /// Number of records.
    pub fn len(&self) -> u64 {
        self.manifest.segments.iter().map(|s| s.rows).sum::<u64>() + self.buffer.len() as u64
    }

    /// True when the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The field registry.
    pub fn fields(&self) -> &FieldRegistry {
        &self.manifest.registry
    }

    /// Summary statistics.
    pub fn stats(&self) -> Stats {
        let segment_rows = self.manifest.segments.iter().map(|s| s.rows).sum();
        let mut range: Option<(i64, i64)> = None;
        let mut widen = |lo: i64, hi: i64| {
            range = Some(match range {
                None => (lo, hi),
                Some((a, b)) => (a.min(lo), b.max(hi)),
            });
        };
        for s in &self.manifest.segments {
            widen(s.ts_min, s.ts_max);
        }
        for r in &self.buffer {
            widen(r.ts, r.ts);
        }
        Stats {
            segment_rows,
            buffered_rows: self.buffer.len() as u64,
            segments: self.manifest.segments.len(),
            segment_bytes: self.manifest.segments.iter().map(|s| s.bytes).sum(),
            wal_bytes: self.wal.data_len(),
            time_range: range,
        }
    }

    /// Application metadata stored with the manifest.
    pub fn app_meta(&self) -> &[u8] {
        &self.manifest.app_meta
    }

    /// Replaces the application metadata; it is persisted by the next
    /// manifest commit (the next flush, compaction or retention drop, or an
    /// explicit [`flush`](Store::flush)).
    pub fn set_app_meta(&mut self, meta: Vec<u8>) {
        self.manifest.app_meta = meta;
        self.app_meta_dirty = true;
    }

    /// Flushes buffered records into a new segment (and persists pending
    /// application metadata).
    pub fn flush(&mut self) -> Result<()> {
        self.flush_at(now_ns())
    }

    /// Periodic housekeeping: sync the WAL, flush it when due, apply
    /// retention, compact, and retry deferred deletions. `now_ns` is the
    /// wall-clock time used for ingest days and retention.
    pub fn maintain(&mut self, now_ns: i64) -> Result<MaintainReport> {
        let mut report = MaintainReport::default();
        self.wal.sync()?;
        if !self.buffer.is_empty()
            && (self.buffer_bytes >= self.opts.flush_bytes || self.wal.age() >= self.opts.flush_age)
        {
            self.flush_at(now_ns)?;
            report.flushed = true;
        }
        report.segments_dropped = self.apply_retention(now_ns)?;
        while let Some(ids) =
            retention::compaction_pick(&self.manifest.segments, self.opts.compaction_fanout)
        {
            self.compact(&ids)?;
            report.compactions += 1;
        }
        self.retry_deletes();
        report.pending_deletes = self.pending_delete.len();
        Ok(report)
    }

    fn next_generation(&mut self) -> u64 {
        self.max_generation += 1;
        self.max_generation
    }

    /// Moves fields the registry cannot hold into the overflow field.
    fn pack_overflow(registry: &mut FieldRegistry, rec: &mut Record) {
        let mut packed = Vec::new();
        rec.fields.retain(|(name, value)| {
            if registry.resolve(name, value.value_type()).is_some() {
                true
            } else {
                let mut bytes = Vec::new();
                put_field(&mut bytes, name, value);
                packed.push(bytes);
                false
            }
        });
        for p in packed {
            rec.fields
                .push((OVERFLOW_FIELD.to_owned(), Value::Bytes(p)));
        }
    }

    pub(crate) fn flush_at(&mut self, now_ns: i64) -> Result<()> {
        if self.buffer.is_empty() {
            self.wal.sync()?;
            if self.app_meta_dirty {
                let generation = self.next_generation();
                self.manifest.commit(&self.dir, generation)?;
                self.app_meta_dirty = false;
            }
            return Ok(());
        }
        // Register fields; only records with fields past the cap are copied
        // (to pack those fields into the overflow field).
        let mut registry = self.manifest.registry.clone();
        let mut packed: Vec<(usize, Record)> = Vec::new();
        for (i, r) in self.buffer.iter().enumerate() {
            let fits = r
                .fields
                .iter()
                .all(|(n, v)| registry.resolve(n, v.value_type()).is_some());
            if !fits {
                let mut copy = r.clone();
                Self::pack_overflow(&mut registry, &mut copy);
                packed.push((i, copy));
            }
        }
        let mut rows: Vec<&Record> = self.buffer.iter().collect();
        for (i, r) in &packed {
            rows[*i] = r;
        }
        let id = self.manifest.next_segment;
        let seg_path = self.dir.join(segment_file_name(id));
        let summary = write_segment(&seg_path, &rows, self.opts.zstd_level, self.opts.chunk_rows)?;
        drop(rows);
        drop(packed);
        crate::failpoint("flush-before-commit", || {});

        let new_wal = Wal::create(&self.dir, self.wal.generation() + 1)?;
        let mut next = self.manifest.clone();
        next.registry = registry;
        next.next_segment = id + 1;
        next.wal_generation = new_wal.generation();
        next.segments.push(SegmentEntry {
            id,
            day: day_of(now_ns),
            rows: summary.rows,
            ts_min: summary.ts_min,
            ts_max: summary.ts_max,
            bytes: summary.bytes,
            level: 0,
        });
        let generation = self.next_generation();
        if let Err(e) = next.commit(&self.dir, generation) {
            // The old manifest and WAL stay authoritative.
            let _ = fs::remove_file(new_wal.path());
            let _ = fs::remove_file(&seg_path);
            return Err(e);
        }
        crate::failpoint("flush-after-commit", || {});
        self.manifest = next;
        self.app_meta_dirty = false;
        let segment = Segment::open(&seg_path)?;
        self.segments.push(Arc::new(segment));
        let old_wal = std::mem::replace(&mut self.wal, new_wal);
        let old_path = old_wal.path().to_owned();
        drop(old_wal);
        self.delete_or_defer(old_path);
        self.buffer.clear();
        self.buffer_bytes = 0;
        Ok(())
    }

    /// Drops expired segments; returns how many.
    fn apply_retention(&mut self, now_ns: i64) -> Result<usize> {
        let expired = retention::expired(&self.manifest.segments, &self.opts.retention, now_ns);
        if expired.is_empty() {
            return Ok(0);
        }
        let mut next = self.manifest.clone();
        next.segments.retain(|s| !expired.contains(&s.id));
        let generation = self.next_generation();
        next.commit(&self.dir, generation)?;
        self.manifest = next;
        self.app_meta_dirty = false;
        self.detach_segments(&expired);
        Ok(expired.len())
    }

    /// Merges segments `ids` (one day, one level) into a new segment.
    fn compact(&mut self, ids: &[u64]) -> Result<()> {
        let inputs: Vec<(SegmentEntry, Arc<Segment>)> = self
            .manifest
            .segments
            .iter()
            .zip(&self.segments)
            .filter(|(e, _)| ids.contains(&e.id))
            .map(|(e, s)| (*e, Arc::clone(s)))
            .collect();
        let encodings = merged_encodings(&inputs)?;
        let id = self.manifest.next_segment;
        let path = self.dir.join(segment_file_name(id));
        let result = (|| -> Result<_> {
            let mut writer = SegmentWriter::create(
                &path,
                encodings,
                self.opts.compact_zstd_level,
                self.opts.chunk_rows,
                true,
            )?;
            let mut sources: Vec<(i64, Source)> = Vec::new();
            for (_, seg) in &inputs {
                if let Some(scan) =
                    SegmentScan::new(Arc::clone(seg), &Filter::True, TimeRange::ALL, false)?
                    && let Some(bound) = scan.bound()
                {
                    sources.push((bound, Box::new(scan)));
                }
            }
            let mut registry = self.manifest.registry.clone();
            for rec in Merge::new(sources, false) {
                let mut rec = rec?;
                Self::pack_overflow(&mut registry, &mut rec);
                writer.push(rec)?;
            }
            writer.finish()
        })();
        let summary = match result {
            Ok(s) => s,
            Err(e) => {
                let _ = fs::remove_file(&path);
                return Err(e);
            }
        };
        let mut next = self.manifest.clone();
        let day = inputs[0].0.day;
        let level = inputs.iter().map(|(e, _)| e.level).max().unwrap_or(0) + 1;
        next.segments.retain(|s| !ids.contains(&s.id));
        next.next_segment = id + 1;
        // Keep the list ordered by id (oldest first).
        next.segments.push(SegmentEntry {
            id,
            day,
            rows: summary.rows,
            ts_min: summary.ts_min,
            ts_max: summary.ts_max,
            bytes: summary.bytes,
            level,
        });
        let generation = self.next_generation();
        if let Err(e) = next.commit(&self.dir, generation) {
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        self.manifest = next;
        self.app_meta_dirty = false;
        let seg = Arc::new(Segment::open(&path)?);
        self.detach_segments(ids);
        self.segments.push(seg);
        Ok(())
    }

    /// Removes segments `ids` from the open set and deletes their files
    /// (deferred while readers hold them).
    fn detach_segments(&mut self, ids: &[u64]) {
        let mut kept = Vec::with_capacity(self.segments.len());
        for seg in std::mem::take(&mut self.segments) {
            let id = seg
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(parse_segment_name);
            if id.is_some_and(|id| ids.contains(&id)) {
                let path = seg.path().to_owned();
                drop(seg);
                self.delete_or_defer(path);
            } else {
                kept.push(seg);
            }
        }
        self.segments = kept;
    }

    fn delete_or_defer(&mut self, path: PathBuf) {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => self.pending_delete.push(path),
        }
    }

    fn retry_deletes(&mut self) {
        let pending = std::mem::take(&mut self.pending_delete);
        for p in pending {
            self.delete_or_defer(p);
        }
    }

    /// Files whose deletion is deferred (still mapped by a reader).
    pub fn pending_deletes(&self) -> usize {
        self.pending_delete.len()
    }

    /// Ids of the live segments, oldest first.
    #[cfg(test)]
    pub(crate) fn segment_entries(&self) -> &[SegmentEntry] {
        &self.manifest.segments
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.wal.sync();
    }
}

/// String encodings for a compaction output: a field stays dictionary-coded
/// only if every input has it that way and the union is not mostly distinct.
fn merged_encodings(
    inputs: &[(SegmentEntry, Arc<Segment>)],
) -> Result<HashMap<String, StrEncoding>> {
    let mut merged: HashMap<String, Option<HashSet<String>>> = HashMap::new();
    for (_, seg) in inputs {
        for (name, ty) in &seg.fields {
            if *ty != ValueType::Str || name == OVERFLOW_FIELD {
                continue;
            }
            let slot = merged
                .entry(name.clone())
                .or_insert_with(|| Some(HashSet::new()));
            match seg.string_encoding(name)? {
                Some(StrEncoding::Dict(strings)) => {
                    if let Some(set) = slot {
                        set.extend(strings);
                    }
                }
                Some(StrEncoding::Plain) => *slot = None,
                None => {}
            }
        }
    }
    let rows: u64 = inputs.iter().map(|(e, _)| e.rows).sum();
    Ok(merged
        .into_iter()
        .map(|(name, set)| {
            let enc = match set {
                Some(set) if !choose_plain(set.len(), rows as usize) => {
                    StrEncoding::Dict(set.into_iter().collect())
                }
                _ => StrEncoding::Plain,
            };
            (name, enc)
        })
        .collect())
}

/// Registers the fields of `records` (the registry then also describes
/// buffered data; it is persisted with the next manifest commit).
fn register_fields(registry: &mut FieldRegistry, records: &[Record]) {
    for r in records {
        for (name, value) in &r.fields {
            registry.resolve(name, value.value_type());
        }
    }
}

/// Deletes files in `dir` whose names match; returns how many.
fn remove_matching(dir: &Path, pred: impl Fn(&str) -> bool) -> Result<usize> {
    let mut n = 0;
    for entry in fs::read_dir(dir).map_err(|e| StoreError::io(dir, e))? {
        let entry = entry.map_err(|e| StoreError::io(dir, e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if parse_manifest_name(name).is_none()
            && pred(name)
            && fs::remove_file(entry.path()).is_ok()
        {
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{ANY_FIELD, FieldOp, Order};
    use crate::retention::DAY_NS;
    use crate::test_util::TempDir;

    fn rec(i: i64) -> Record {
        Record::new(i)
            .with("i", Value::I64(i))
            .with("host", Value::Str(format!("host{}", i % 4)))
    }

    fn all(store: &Store, order: Order) -> Vec<i64> {
        let q = Query {
            order,
            ..Query::default()
        };
        store.search(&q).unwrap().map(|r| r.unwrap().ts).collect()
    }

    fn small() -> Options {
        Options {
            sync: SyncPolicy::Always,
            chunk_rows: 16,
            ..Options::default()
        }
    }

    #[test]
    fn append_flush_reopen() {
        let dir = TempDir::new("store-reopen");
        {
            let mut s = Store::open(dir.path(), small()).unwrap();
            assert!(s.is_empty());
            let ack = s.append((0..50).map(rec).collect()).unwrap();
            assert_eq!(
                ack,
                Ack {
                    appended: 50,
                    total: 50,
                    synced: true
                }
            );
            s.flush().unwrap();
            s.append((50..60).rev().map(rec).collect()).unwrap();
            assert_eq!(all(&s, Order::Asc), (0..60).collect::<Vec<_>>());
            assert_eq!(s.stats().segments, 1);
            assert_eq!(s.stats().buffered_rows, 10);
            assert_eq!(s.stats().time_range, Some((0, 59)));
        }
        let s = Store::open(dir.path(), small()).unwrap();
        assert_eq!(s.recovery().wal_records, 10);
        assert_eq!(s.len(), 60);
        assert_eq!(all(&s, Order::Desc), (0..60).rev().collect::<Vec<_>>());
        let q = Query {
            filter: Filter::field("host", FieldOp::Eq(Value::Str("HOST1".into()))),
            limit: Some(3),
            order: Order::Desc,
            ..Query::default()
        };
        let got: Vec<i64> = s.search(&q).unwrap().map(|r| r.unwrap().ts).collect();
        assert_eq!(got, vec![57, 53, 49]);
        assert_eq!(s.fields().ids("host").len(), 1);
    }

    #[test]
    fn registry_covers_buffered_rows() {
        let dir = TempDir::new("store-registry");
        let mut s = Store::open(dir.path(), small()).unwrap();
        s.append(vec![rec(1).with("port", Value::Str("80".into()))])
            .unwrap();
        assert_eq!(s.fields().ids("port").len(), 1);
        drop(s);
        // Replayed from the WAL, never flushed.
        let s = Store::open(dir.path(), small()).unwrap();
        assert_eq!(s.fields().ids("port").len(), 1);
        assert_eq!(s.fields().ids("i").len(), 1);
    }

    #[test]
    fn lock_is_exclusive() {
        let dir = TempDir::new("store-lock");
        let s = Store::open(dir.path(), small()).unwrap();
        assert!(matches!(
            Store::open(dir.path(), small()),
            Err(StoreError::Locked(_))
        ));
        drop(s);
        Store::open(dir.path(), small()).unwrap();
    }

    #[test]
    fn auto_flush_on_size() {
        let dir = TempDir::new("store-autoflush");
        let opts = Options {
            flush_bytes: 2000,
            ..small()
        };
        let mut s = Store::open(dir.path(), opts).unwrap();
        for i in 0..100 {
            s.append(vec![rec(i)]).unwrap();
        }
        assert!(s.stats().segments >= 3);
        assert_eq!(all(&s, Order::Asc), (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn overflow_round_trips_and_is_searchable() {
        let dir = TempDir::new("store-overflow");
        let opts = Options {
            max_fields: 2,
            ..small()
        };
        let mut s = Store::open(dir.path(), opts.clone()).unwrap();
        let r = Record::new(1)
            .with("a", Value::Str("x".into()))
            .with("b", Value::I64(2))
            .with("c", Value::Str("Overflowed".into()))
            .with("d", Value::Ip(7));
        s.append(vec![r.clone()]).unwrap();
        s.flush().unwrap();
        assert!(s.fields().is_full());
        assert!(s.fields().ids("c").is_empty());
        drop(s);
        let s = Store::open(dir.path(), opts).unwrap();
        let mut got: Vec<Record> = s
            .search(&Query::default())
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(got.len(), 1);
        got[0].fields.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got[0], r);
        let find = |f: Filter| {
            s.search(&Query {
                filter: f,
                ..Query::default()
            })
            .unwrap()
            .count()
        };
        assert_eq!(
            find(Filter::field(
                "c",
                FieldOp::Eq(Value::Str("overflowed".into()))
            )),
            1
        );
        assert_eq!(find(Filter::field("d", FieldOp::Cidr { lo: 0, hi: 10 })), 1);
        assert_eq!(
            find(Filter::field(ANY_FIELD, FieldOp::Contains("flow".into()))),
            1
        );
        assert_eq!(find(Filter::field(OVERFLOW_FIELD, FieldOp::Exists)), 0);
        let other = TempDir::new("store-reserved");
        let mut s2 = Store::open(other.path(), small()).unwrap();
        let bad = Record::new(1).with(OVERFLOW_FIELD, Value::Bool(true));
        assert!(matches!(
            s2.append(vec![bad]),
            Err(StoreError::InvalidArgument(_))
        ));
    }

    #[test]
    fn compaction_and_retention() {
        let dir = TempDir::new("store-compact");
        let opts = Options {
            compaction_fanout: 3,
            retention: RetentionPolicy {
                max_age: Some(Duration::from_secs(2 * 86_400)),
                min_age: Duration::ZERO,
            },
            ..small()
        };
        let mut s = Store::open(dir.path(), opts.clone()).unwrap();
        let day0 = 20_000 * DAY_NS;
        let mut expected = Vec::new();
        // Day 0: five flushes; day 1: one flush.
        for k in 0..5i64 {
            let rows: Vec<Record> = (0..40).map(|i| rec(i * 10 + k)).collect();
            expected.extend(rows.iter().map(|r| r.ts));
            s.append(rows).unwrap();
            s.flush_at(day0 + k).unwrap();
        }
        s.append(vec![rec(10_000)]).unwrap();
        s.flush_at(day0 + DAY_NS).unwrap();
        expected.push(10_000);
        expected.sort_unstable();

        let report = s.maintain(day0 + DAY_NS + 1).unwrap();
        assert_eq!(report.compactions, 1);
        assert_eq!(report.segments_dropped, 0);
        let levels: Vec<(i64, u8)> = s
            .segment_entries()
            .iter()
            .map(|e| (e.day - 20_000, e.level))
            .collect();
        assert_eq!(levels, vec![(0, 0), (0, 0), (1, 0), (0, 1)]);
        assert_eq!(all(&s, Order::Asc), expected);
        assert_eq!(s.len(), 201);
        drop(s);

        let mut s = Store::open(dir.path(), opts.clone()).unwrap();
        assert_eq!(s.recovery().orphans_removed, 0);
        assert_eq!(all(&s, Order::Asc), expected);
        // Day 0 expires at the start of day 3; day 1 a day later.
        let report = s.maintain(day0 + 3 * DAY_NS).unwrap();
        assert_eq!(report.segments_dropped, 3);
        assert_eq!(all(&s, Order::Asc), vec![10_000]);
        let report = s.maintain(day0 + 4 * DAY_NS).unwrap();
        assert_eq!(report.segments_dropped, 1);
        assert!(s.is_empty());
        let segment_files = fs::read_dir(s.path())
            .unwrap()
            .filter(|e| {
                let n = e.as_ref().unwrap().file_name();
                parse_segment_name(n.to_str().unwrap()).is_some()
            })
            .count();
        assert_eq!(segment_files, 0);
    }

    #[test]
    fn reader_keeps_dropped_segment_alive() {
        let dir = TempDir::new("store-defer");
        let opts = Options {
            retention: RetentionPolicy {
                max_age: Some(Duration::from_secs(86_400)),
                min_age: Duration::ZERO,
            },
            ..small()
        };
        let mut s = Store::open(dir.path(), opts).unwrap();
        s.append((0..100).map(rec).collect()).unwrap();
        s.flush_at(0).unwrap();
        let mut iter = s.search(&Query::default()).unwrap();
        assert_eq!(iter.next().unwrap().unwrap().ts, 0);
        let report = s.maintain(5 * DAY_NS).unwrap();
        assert_eq!(report.segments_dropped, 1);
        // The open iterator still reads the dropped segment.
        assert_eq!(iter.by_ref().count(), 99);
        drop(iter);
        let report = s.maintain(5 * DAY_NS).unwrap();
        assert_eq!(report.pending_deletes, 0);
        assert_eq!(s.pending_deletes(), 0);
    }

    #[test]
    fn orphans_are_removed_at_open() {
        let dir = TempDir::new("store-orphans");
        {
            let mut s = Store::open(dir.path(), small()).unwrap();
            s.append((0..10).map(rec).collect()).unwrap();
            s.flush().unwrap();
        }
        fs::write(dir.path().join(segment_file_name(99)), b"junk").unwrap();
        fs::write(dir.path().join(crate::wal::wal_file_name(77)), b"junk").unwrap();
        let s = Store::open(dir.path(), small()).unwrap();
        assert_eq!(s.recovery().orphans_removed, 2);
        assert_eq!(s.len(), 10);
    }

    #[test]
    fn app_meta_persists() {
        let dir = TempDir::new("store-meta");
        {
            let mut s = Store::open(dir.path(), small()).unwrap();
            s.set_app_meta(b"catalog-v1".to_vec());
            s.flush().unwrap();
        }
        let mut s = Store::open(dir.path(), small()).unwrap();
        assert_eq!(s.app_meta(), b"catalog-v1");
        s.set_app_meta(b"v2".to_vec());
        s.append(vec![rec(1)]).unwrap();
        s.flush().unwrap();
        drop(s);
        assert_eq!(Store::open(dir.path(), small()).unwrap().app_meta(), b"v2");
    }

    #[test]
    fn torn_manifest_falls_back_to_previous() {
        let dir = TempDir::new("store-manifest");
        {
            let mut s = Store::open(dir.path(), small()).unwrap();
            s.append((0..10).map(rec).collect()).unwrap();
            s.flush().unwrap();
            s.set_app_meta(b"x".to_vec());
            s.flush().unwrap();
        }
        let mut gens: Vec<u64> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| parse_manifest_name(e.unwrap().file_name().to_str().unwrap()))
            .collect();
        gens.sort_unstable();
        let newest = dir
            .path()
            .join(crate::directory::manifest_file_name(*gens.last().unwrap()));
        let data = fs::read(&newest).unwrap();
        fs::write(&newest, &data[..data.len() / 2]).unwrap();
        let mut s = Store::open(dir.path(), small()).unwrap();
        assert_eq!(s.recovery().manifests_skipped, 1);
        assert_eq!(s.len(), 10);
        assert_eq!(s.app_meta(), b"");
        // Later commits go above the torn generation.
        s.append(vec![rec(11)]).unwrap();
        s.flush().unwrap();
        drop(s);
        assert_eq!(Store::open(dir.path(), small()).unwrap().len(), 11);
    }
}
