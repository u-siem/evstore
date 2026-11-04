//! Segment files.
//!
//! Design: `docs/design/03-tenant-storage-unit.md`,
//! `docs/design/08-components.md` (segment internals) and
//! `docs/design/09-design-details.md` (packing threshold, size limits).
//!
//! - Immutable, single-file segments with contiguous metadata and
//!   per-section checksums.
//! - Adaptive schema groups: columnar groups for frequent schemas, one packed
//!   group for the long tail; cost-based threshold with hysteresis.
//! - Sealed tenants are one self-contained file with the manifest in the
//!   footer. Streaming tenants use ingest-time partitions merged over time.
//! - Typed encodings, structured string types split into dictionary-coded
//!   parts. Pre-trained per-schema and domain dictionaries come after v1
//!   (the format keeps dictionary references from day one).
//!
//! # MVP implementation (format v2)
//!
//! `seg-<id:016x>.evs`, little-endian, one columnar group (no schema
//! groups, no packed group):
//!
//! ```text
//! header   "EVSG" u16 version u16 flags
//! blocks   zstd frames: per chunk a timestamp block and one block per
//!          field present in the chunk; then per string field its
//!          dictionary and, for large segments, its postings
//! footer   rows, ts range, field table (name, type, string encoding,
//!          dictionary and postings refs), chunk directory (rows, ts
//!          range, block refs, zone maps)
//! tail     u64 footer offset, u32 footer len, u32 footer crc32, "EVSG"
//! ```
//!
//! - Rows are sorted by timestamp and cut into chunks (default 4,096 rows).
//! - Every block carries its compressed length, raw length and CRC32.
//! - Timestamps are delta-coded. A field column is a per-row value count
//!   (0 = absent; more than 1 = multi-valued) followed by the values.
//! - Each string field has its own encoding. **Dictionary**: codes into the
//!   field's dictionary, sorted by lowercase then original form, so
//!   case-insensitive predicates are evaluated once per distinct value.
//!   **Plain**: length-prefixed strings, for fields where most values are
//!   distinct (raw lines, messages); they are matched value by value.
//! - Postings ([`crate::index`]): segments of at least
//!   [`POSTINGS_MIN_ROWS`] rows store, per dictionary field, a row bitmap
//!   per code, used to skip chunks.
//! - Zone maps per chunk and field: min/max dictionary code for dictionary
//!   strings, min/max value for numbers, times, IPs and booleans.
//! - Files are read through `memmap2`. Format v1 (one dictionary shared by
//!   all string fields) is still readable; compaction rewrites it as v2.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use memmap2::Mmap;
use roaring::RoaringBitmap;

use crate::codec::{
    DResult, DecodeError, Reader, crc32, get_tagged, put_ivarint, put_len, put_lp, put_tagged,
    put_uvarint,
};
use crate::error::{Result, StoreError};
use crate::model::{Record, Value, ValueType, cmp_ci};

const MAGIC: &[u8; 4] = b"EVSG";
const VERSION: u16 = 2;
const HEADER_LEN: u64 = 8;
const TAIL_LEN: usize = 20;

/// Default rows per chunk.
pub const DEFAULT_CHUNK_ROWS: usize = 4096;

/// Segments with at least this many rows get postings.
pub const POSTINGS_MIN_ROWS: u64 = 10_000;

pub(crate) fn segment_file_name(id: u64) -> String {
    format!("seg-{id:016x}.evs")
}

/// Parses `seg-<id>.evs`.
pub(crate) fn parse_segment_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("seg-")?.strip_suffix(".evs")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Location of one compressed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockRef {
    pub off: u64,
    pub len: u32,
    pub raw_len: u32,
    pub crc: u32,
}

impl BlockRef {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.off.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.raw_len.to_le_bytes());
        out.extend_from_slice(&self.crc.to_le_bytes());
    }

    fn decode(r: &mut Reader<'_>) -> DResult<BlockRef> {
        Ok(BlockRef {
            off: r.u64()?,
            len: r.u32()?,
            raw_len: r.u32()?,
            crc: r.u32()?,
        })
    }
}

/// Min/max summary of one column in one chunk.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ZoneMap {
    /// No summary (bytes and plain-string columns).
    None,
    /// Min and max dictionary code.
    Codes(u32, u32),
    /// Min and max value (same type as the column).
    Values(Value, Value),
}

impl ZoneMap {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            ZoneMap::None => out.push(0),
            ZoneMap::Codes(lo, hi) => {
                out.push(1);
                out.extend_from_slice(&lo.to_le_bytes());
                out.extend_from_slice(&hi.to_le_bytes());
            }
            ZoneMap::Values(lo, hi) => {
                out.push(2);
                put_tagged(out, lo);
                put_tagged(out, hi);
            }
        }
    }

    fn decode(r: &mut Reader<'_>) -> DResult<ZoneMap> {
        Ok(match r.u8()? {
            0 => ZoneMap::None,
            1 => ZoneMap::Codes(r.u32()?, r.u32()?),
            2 => ZoneMap::Values(get_tagged(r)?, get_tagged(r)?),
            _ => return Err(DecodeError("unknown zone map kind")),
        })
    }
}

/// One field column of one chunk.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnMeta {
    /// Index into the segment's field table.
    pub field: u32,
    pub block: BlockRef,
    /// Rows with at least one value.
    pub present: u32,
    pub zone: ZoneMap,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChunkMeta {
    pub rows: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    pub ts_block: BlockRef,
    pub columns: Vec<ColumnMeta>,
    /// Index of the chunk's first row in the segment.
    pub first_row: u32,
}

/// Where a dictionary-coded string field keeps its dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DictRef {
    pub block: BlockRef,
    pub len: u32,
    /// Cache slot (v1 segments share one slot between all fields).
    slot: usize,
}

/// Per-field storage details beyond name and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct FieldStorage {
    /// Set for dictionary-coded string fields.
    pub dict: Option<DictRef>,
    /// Postings block, for dictionary fields of large segments.
    pub postings: Option<BlockRef>,
}

/// Decoded values of a column, by type.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColData {
    /// Dictionary codes.
    Str(Vec<u32>),
    /// Plain strings.
    Plain(PlainStrings),
    Int(Vec<i64>),
    U64(Vec<u64>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Bytes(Vec<Vec<u8>>),
    Ip(Vec<u128>),
}

impl ColData {
    fn new(ty: ValueType, plain: bool) -> ColData {
        match ty {
            ValueType::Str if plain => ColData::Plain(PlainStrings::default()),
            ValueType::Str => ColData::Str(Vec::new()),
            ValueType::I64 | ValueType::Time => ColData::Int(Vec::new()),
            ValueType::U64 => ColData::U64(Vec::new()),
            ValueType::F64 => ColData::F64(Vec::new()),
            ValueType::Bool => ColData::Bool(Vec::new()),
            ValueType::Bytes => ColData::Bytes(Vec::new()),
            ValueType::Ip => ColData::Ip(Vec::new()),
        }
    }

    fn len(&self) -> usize {
        match self {
            ColData::Str(v) => v.len(),
            ColData::Plain(v) => v.len(),
            ColData::Int(v) => v.len(),
            ColData::U64(v) => v.len(),
            ColData::F64(v) => v.len(),
            ColData::Bool(v) => v.len(),
            ColData::Bytes(v) => v.len(),
            ColData::Ip(v) => v.len(),
        }
    }
}

/// Strings of a plain column, packed into one buffer.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct PlainStrings {
    data: String,
    ends: Vec<u32>,
}

impl PlainStrings {
    pub fn push(&mut self, s: &str) {
        self.data.push_str(s);
        self.ends.push(self.data.len() as u32);
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn get(&self, i: usize) -> &str {
        let start = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.data[start..self.ends[i] as usize]
    }
}

/// A decoded column: values of row `r` are `offsets[r]..offsets[r + 1]`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Column {
    pub ty: ValueType,
    pub offsets: Vec<u32>,
    pub data: ColData,
}

impl Column {
    pub fn range(&self, row: usize) -> std::ops::Range<usize> {
        self.offsets[row] as usize..self.offsets[row + 1] as usize
    }

    /// Materializes value `i`; dictionary codes are looked up in `dict`,
    /// which must be the column's field dictionary.
    pub fn value(&self, i: usize, dict: Option<&Dict>) -> Value {
        match &self.data {
            ColData::Str(v) => Value::Str(
                dict.expect("dictionary column needs its dictionary")
                    .get(v[i])
                    .to_owned(),
            ),
            ColData::Plain(v) => Value::Str(v.get(i).to_owned()),
            ColData::Int(v) if self.ty == ValueType::Time => Value::Time(v[i]),
            ColData::Int(v) => Value::I64(v[i]),
            ColData::U64(v) => Value::U64(v[i]),
            ColData::F64(v) => Value::F64(v[i]),
            ColData::Bool(v) => Value::Bool(v[i]),
            ColData::Bytes(v) => Value::Bytes(v[i].clone()),
            ColData::Ip(v) => Value::Ip(v[i]),
        }
    }
}

/// A string dictionary: originals plus lowercase forms.
#[derive(Debug, Default)]
pub(crate) struct Dict {
    strings: Vec<String>,
    lower: Vec<String>,
}

impl Dict {
    /// Sorts and dedups dictionary input (lowercase first, then original).
    pub fn sort(strings: &mut Vec<String>) {
        strings.sort_by(|a, b| cmp_ci(a, b).then_with(|| a.cmp(b)));
        strings.dedup();
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn get(&self, code: u32) -> &str {
        &self.strings[code as usize]
    }

    /// Lowercase form of `code`.
    pub fn lower(&self, code: u32) -> &str {
        &self.lower[code as usize]
    }

    pub fn strings(&self) -> &[String] {
        &self.strings
    }
}

/// How a string field is encoded in a segment being written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StrEncoding {
    /// Dictionary of every value the field will hold (any order, may repeat).
    Dict(Vec<String>),
    /// No dictionary.
    Plain,
}

/// Picks dictionary or plain for a field with `distinct` distinct values out
/// of `values`: plain when more than half the values are distinct.
pub(crate) fn choose_plain(distinct: usize, values: usize) -> bool {
    distinct > 16 && distinct * 2 > values
}

/// Metadata of a written segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentSummary {
    pub rows: u64,
    pub ts_min: i64,
    pub ts_max: i64,
    pub bytes: u64,
}

struct ColBuilder {
    ty: ValueType,
    counts: Vec<u32>,
    data: ColData,
}

/// Writer-side state of one string field.
struct StrField {
    /// `None` for plain fields.
    codes: Option<HashMap<String, u32>>,
    /// Row bitmap per code (only when postings are built).
    postings: Vec<RoaringBitmap>,
}

/// Streams time-sorted records into a new segment file.
pub(crate) struct SegmentWriter {
    out: BufWriter<File>,
    path: PathBuf,
    pos: u64,
    level: i32,
    chunk_rows: usize,
    postings: bool,
    encodings: HashMap<String, StrEncoding>,
    fields: Vec<(String, ValueType)>,
    field_ids: HashMap<String, Vec<(ValueType, u32)>>,
    strings: HashMap<u32, StrField>,
    pending: Vec<Record>,
    chunks: Vec<ChunkMeta>,
    rows: u64,
    ts_min: i64,
    ts_max: i64,
    last_ts: i64,
}

impl SegmentWriter {
    /// Creates `path` (must not exist). `encodings` gives, per string field
    /// name, its dictionary or plain encoding; fields not listed are plain.
    /// `postings` asks for postings (written only if the segment ends up
    /// with at least [`POSTINGS_MIN_ROWS`] rows).
    pub fn create(
        path: &Path,
        encodings: HashMap<String, StrEncoding>,
        level: i32,
        chunk_rows: usize,
        postings: bool,
    ) -> Result<SegmentWriter> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| StoreError::io(path, e))?;
        let mut out = BufWriter::with_capacity(1 << 20, file);
        let mut header = Vec::with_capacity(HEADER_LEN as usize);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        out.write_all(&header)
            .map_err(|e| StoreError::io(path, e))?;
        Ok(SegmentWriter {
            out,
            path: path.to_owned(),
            pos: HEADER_LEN,
            level,
            chunk_rows: chunk_rows.max(1),
            postings,
            encodings,
            fields: Vec::new(),
            field_ids: HashMap::new(),
            strings: HashMap::new(),
            pending: Vec::new(),
            chunks: Vec::new(),
            rows: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            last_ts: i64::MIN,
        })
    }

    /// Adds a record; records must arrive in non-decreasing `ts` order.
    pub fn push(&mut self, rec: Record) -> Result<()> {
        if rec.ts < self.last_ts {
            return Err(StoreError::InvalidArgument(
                "segment rows must be pushed in time order".into(),
            ));
        }
        self.last_ts = rec.ts;
        self.pending.push(rec);
        if self.pending.len() >= self.chunk_rows {
            self.write_chunk()?;
        }
        Ok(())
    }

    /// Writes time-sorted rows directly, without copying them. Must not be
    /// mixed with buffered rows from [`push`](Self::push).
    pub fn push_sorted(&mut self, rows: &[&Record]) -> Result<()> {
        if !self.pending.is_empty() {
            return Err(StoreError::InvalidArgument("push_sorted after push".into()));
        }
        for r in rows {
            if r.ts < self.last_ts {
                return Err(StoreError::InvalidArgument(
                    "segment rows must be pushed in time order".into(),
                ));
            }
            self.last_ts = r.ts;
        }
        for chunk in rows.chunks(self.chunk_rows) {
            self.write_rows(chunk)?;
        }
        Ok(())
    }

    fn local_field(&mut self, name: &str, ty: ValueType) -> u32 {
        if let Some(ids) = self.field_ids.get(name)
            && let Some((_, id)) = ids.iter().find(|(t, _)| *t == ty)
        {
            return *id;
        }
        let id = self.fields.len() as u32;
        self.fields.push((name.to_owned(), ty));
        self.field_ids
            .entry(name.to_owned())
            .or_default()
            .push((ty, id));
        if ty == ValueType::Str {
            let codes = match self.encodings.remove(name) {
                Some(StrEncoding::Dict(mut dict)) => {
                    Dict::sort(&mut dict);
                    Some(
                        dict.into_iter()
                            .enumerate()
                            .map(|(i, s)| (s, i as u32))
                            .collect::<HashMap<_, _>>(),
                    )
                }
                _ => None,
            };
            let postings = match (&codes, self.postings) {
                (Some(c), true) => vec![RoaringBitmap::new(); c.len()],
                _ => Vec::new(),
            };
            self.strings.insert(id, StrField { codes, postings });
        }
        id
    }

    fn write_block(&mut self, raw: &[u8]) -> Result<BlockRef> {
        let compressed =
            zstd::bulk::compress(raw, self.level).map_err(|e| StoreError::io(&self.path, e))?;
        let block = BlockRef {
            off: self.pos,
            len: u32::try_from(compressed.len())
                .map_err(|_| StoreError::InvalidArgument("block too large".into()))?,
            raw_len: u32::try_from(raw.len())
                .map_err(|_| StoreError::InvalidArgument("block too large".into()))?,
            crc: crc32(&compressed),
        };
        self.out
            .write_all(&compressed)
            .map_err(|e| StoreError::io(&self.path, e))?;
        self.pos += compressed.len() as u64;
        Ok(block)
    }

    fn write_chunk(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        let rows: Vec<&Record> = pending.iter().collect();
        self.write_rows(&rows)
    }

    fn write_rows(&mut self, rows: &[&Record]) -> Result<()> {
        let n = rows.len();
        let ts_min = rows[0].ts;
        let ts_max = rows[n - 1].ts;
        let first_row = u32::try_from(self.rows)
            .map_err(|_| StoreError::InvalidArgument("segment exceeds u32 rows".into()))?;

        let mut ts_raw = Vec::with_capacity(n * 2 + 8);
        ts_raw.extend_from_slice(&ts_min.to_le_bytes());
        let mut prev = ts_min;
        for r in rows {
            put_uvarint(&mut ts_raw, r.ts.wrapping_sub(prev) as u64);
            prev = r.ts;
        }
        let ts_block = self.write_block(&ts_raw)?;

        let mut builders: Vec<Option<ColBuilder>> = Vec::new();
        for (row, rec) in rows.iter().enumerate() {
            for (name, value) in &rec.fields {
                let ty = value.value_type();
                let id = self.local_field(name, ty);
                let idx = id as usize;
                if builders.len() <= idx {
                    builders.resize_with(idx + 1, || None);
                }
                let plain = self.strings.get(&id).is_some_and(|s| s.codes.is_none());
                let b = builders[idx].get_or_insert_with(|| ColBuilder {
                    ty,
                    counts: vec![0; n],
                    data: ColData::new(ty, plain),
                });
                b.counts[row] += 1;
                match (&mut b.data, value) {
                    (ColData::Str(v), Value::Str(s)) => {
                        let field = self.strings.get_mut(&id).expect("string field state");
                        let codes = field.codes.as_ref().expect("dictionary field");
                        let code = *codes.get(s.as_str()).ok_or_else(|| {
                            StoreError::InvalidArgument(format!(
                                "string {s:?} missing from the dictionary of {name}"
                            ))
                        })?;
                        if let Some(bitmap) = field.postings.get_mut(code as usize) {
                            bitmap.insert(first_row + row as u32);
                        }
                        v.push(code);
                    }
                    (ColData::Plain(v), Value::Str(s)) => v.push(s),
                    (ColData::Int(v), Value::I64(x) | Value::Time(x)) => v.push(*x),
                    (ColData::U64(v), Value::U64(x)) => v.push(*x),
                    (ColData::F64(v), Value::F64(x)) => v.push(*x),
                    (ColData::Bool(v), Value::Bool(x)) => v.push(*x),
                    (ColData::Bytes(v), Value::Bytes(x)) => v.push(x.clone()),
                    (ColData::Ip(v), Value::Ip(x)) => v.push(*x),
                    _ => unreachable!("column type follows value type"),
                }
            }
        }

        let mut columns = Vec::new();
        for (id, b) in builders.into_iter().enumerate() {
            let Some(b) = b else { continue };
            let mut raw = Vec::with_capacity(n + b.data.len() * 4);
            for c in &b.counts {
                put_uvarint(&mut raw, u64::from(*c));
            }
            encode_values(&mut raw, &b.data);
            let block = self.write_block(&raw)?;
            columns.push(ColumnMeta {
                field: id as u32,
                block,
                present: b.counts.iter().filter(|c| **c > 0).count() as u32,
                zone: zone_of(b.ty, &b.data),
            });
        }

        self.rows += n as u64;
        self.ts_min = self.ts_min.min(ts_min);
        self.ts_max = self.ts_max.max(ts_max);
        self.chunks.push(ChunkMeta {
            rows: n as u32,
            ts_min,
            ts_max,
            ts_block,
            columns,
            first_row,
        });
        crate::failpoint("segment-write-mid", || {
            let _ = self.out.flush();
            let _ = self.out.get_ref().sync_all();
        });
        Ok(())
    }

    /// Writes the remaining rows, dictionaries, postings and footer, then
    /// fsyncs.
    pub fn finish(mut self) -> Result<SegmentSummary> {
        self.write_chunk()?;
        let with_postings = self.postings && self.rows >= POSTINGS_MIN_ROWS;
        let mut storage = vec![FieldStorage::default(); self.fields.len()];
        let mut string_ids: Vec<u32> = self.strings.keys().copied().collect();
        string_ids.sort_unstable();
        for id in string_ids {
            let field = self.strings.remove(&id).expect("string field");
            let Some(codes) = field.codes else { continue };
            let mut dict: Vec<(String, u32)> = codes.into_iter().collect();
            dict.sort_unstable_by_key(|(_, c)| *c);
            let mut raw = Vec::new();
            put_len(&mut raw, dict.len());
            for (s, _) in &dict {
                put_lp(&mut raw, s.as_bytes());
            }
            let block = self.write_block(&raw)?;
            let postings = if with_postings {
                Some(self.write_block(&crate::index::encode_postings(&field.postings))?)
            } else {
                None
            };
            storage[id as usize] = FieldStorage {
                dict: Some(DictRef {
                    block,
                    len: dict.len() as u32,
                    slot: id as usize,
                }),
                postings,
            };
        }

        let mut footer = Vec::new();
        footer.extend_from_slice(&self.rows.to_le_bytes());
        footer.extend_from_slice(&self.ts_min.to_le_bytes());
        footer.extend_from_slice(&self.ts_max.to_le_bytes());
        put_len(&mut footer, self.fields.len());
        for ((name, ty), st) in self.fields.iter().zip(&storage) {
            put_lp(&mut footer, name.as_bytes());
            footer.push(*ty as u8);
            match &st.dict {
                None => footer.push(0),
                Some(d) => {
                    footer.push(1);
                    d.block.encode(&mut footer);
                    footer.extend_from_slice(&d.len.to_le_bytes());
                    match &st.postings {
                        None => footer.push(0),
                        Some(p) => {
                            footer.push(1);
                            p.encode(&mut footer);
                        }
                    }
                }
            }
        }
        put_len(&mut footer, self.chunks.len());
        for c in &self.chunks {
            footer.extend_from_slice(&c.rows.to_le_bytes());
            footer.extend_from_slice(&c.ts_min.to_le_bytes());
            footer.extend_from_slice(&c.ts_max.to_le_bytes());
            c.ts_block.encode(&mut footer);
            put_len(&mut footer, c.columns.len());
            for col in &c.columns {
                put_len(&mut footer, col.field as usize);
                col.block.encode(&mut footer);
                footer.extend_from_slice(&col.present.to_le_bytes());
                col.zone.encode(&mut footer);
            }
        }
        let footer_off = self.pos;
        let mut tail = Vec::with_capacity(TAIL_LEN);
        tail.extend_from_slice(&footer_off.to_le_bytes());
        tail.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        tail.extend_from_slice(&crc32(&footer).to_le_bytes());
        tail.extend_from_slice(MAGIC);
        self.out
            .write_all(&footer)
            .map_err(|e| StoreError::io(&self.path, e))?;
        self.out
            .write_all(&tail)
            .map_err(|e| StoreError::io(&self.path, e))?;
        let bytes = footer_off + footer.len() as u64 + TAIL_LEN as u64;
        let file = self
            .out
            .into_inner()
            .map_err(|e| StoreError::io(&self.path, e.into_error()))?;
        file.sync_all().map_err(|e| StoreError::io(&self.path, e))?;
        Ok(SegmentSummary {
            rows: self.rows,
            ts_min: self.ts_min,
            ts_max: self.ts_max,
            bytes,
        })
    }
}

fn encode_values(out: &mut Vec<u8>, data: &ColData) {
    match data {
        ColData::Str(v) => v.iter().for_each(|c| put_uvarint(out, u64::from(*c))),
        ColData::Plain(v) => (0..v.len()).for_each(|i| put_lp(out, v.get(i).as_bytes())),
        ColData::Int(v) => v.iter().for_each(|x| put_ivarint(out, *x)),
        ColData::U64(v) => v.iter().for_each(|x| put_uvarint(out, *x)),
        ColData::F64(v) => v
            .iter()
            .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
        ColData::Bool(v) => v.iter().for_each(|x| out.push(u8::from(*x))),
        ColData::Bytes(v) => v.iter().for_each(|x| put_lp(out, x)),
        ColData::Ip(v) => v
            .iter()
            .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
    }
}

/// Decodes `n` values of type `ty`; `dict_len` is `Some` for dictionary
/// strings.
fn decode_values(
    r: &mut Reader<'_>,
    ty: ValueType,
    n: usize,
    dict_len: Option<usize>,
) -> DResult<ColData> {
    Ok(match ty {
        ValueType::Str => match dict_len {
            Some(len) => {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    let c = r.uvarint()?;
                    if c >= len as u64 {
                        return Err(DecodeError("dictionary code out of range"));
                    }
                    v.push(c as u32);
                }
                ColData::Str(v)
            }
            None => {
                let mut v = PlainStrings::default();
                for _ in 0..n {
                    v.push(r.str()?);
                }
                ColData::Plain(v)
            }
        },
        ValueType::I64 | ValueType::Time => {
            ColData::Int((0..n).map(|_| r.ivarint()).collect::<DResult<_>>()?)
        }
        ValueType::U64 => ColData::U64((0..n).map(|_| r.uvarint()).collect::<DResult<_>>()?),
        ValueType::F64 => ColData::F64((0..n).map(|_| r.f64()).collect::<DResult<_>>()?),
        ValueType::Bool => ColData::Bool(
            (0..n)
                .map(|_| match r.u8()? {
                    0 => Ok(false),
                    1 => Ok(true),
                    _ => Err(DecodeError("invalid bool")),
                })
                .collect::<DResult<_>>()?,
        ),
        ValueType::Bytes => ColData::Bytes(
            (0..n)
                .map(|_| r.lp_bytes().map(<[u8]>::to_vec))
                .collect::<DResult<_>>()?,
        ),
        ValueType::Ip => ColData::Ip((0..n).map(|_| r.u128()).collect::<DResult<_>>()?),
    })
}

fn zone_of(ty: ValueType, data: &ColData) -> ZoneMap {
    fn mm<T: Copy>(v: &[T], cmp: impl Fn(&T, &T) -> std::cmp::Ordering) -> Option<(T, T)> {
        let lo = *v.iter().min_by(|a, b| cmp(a, b))?;
        let hi = *v.iter().max_by(|a, b| cmp(a, b))?;
        Some((lo, hi))
    }
    let zone = match data {
        ColData::Str(v) => mm(v, Ord::cmp).map(|(a, b)| ZoneMap::Codes(a, b)),
        ColData::Int(v) => mm(v, Ord::cmp).map(|(a, b)| {
            if ty == ValueType::Time {
                ZoneMap::Values(Value::Time(a), Value::Time(b))
            } else {
                ZoneMap::Values(Value::I64(a), Value::I64(b))
            }
        }),
        ColData::U64(v) => {
            mm(v, Ord::cmp).map(|(a, b)| ZoneMap::Values(Value::U64(a), Value::U64(b)))
        }
        ColData::F64(v) => {
            // NaN never matches a comparison, so it stays out of the zone.
            let finite: Vec<f64> = v.iter().copied().filter(|x| !x.is_nan()).collect();
            mm(&finite, f64::total_cmp).map(|(a, b)| ZoneMap::Values(Value::F64(a), Value::F64(b)))
        }
        ColData::Bool(v) => {
            mm(v, Ord::cmp).map(|(a, b)| ZoneMap::Values(Value::Bool(a), Value::Bool(b)))
        }
        ColData::Ip(v) => mm(v, Ord::cmp).map(|(a, b)| ZoneMap::Values(Value::Ip(a), Value::Ip(b))),
        ColData::Plain(_) | ColData::Bytes(_) => None,
    };
    zone.unwrap_or(ZoneMap::None)
}

/// An open, memory-mapped segment.
pub(crate) struct Segment {
    path: PathBuf,
    map: Mmap,
    pub rows: u64,
    pub ts_min: i64,
    pub ts_max: i64,
    pub fields: Vec<(String, ValueType)>,
    pub storage: Vec<FieldStorage>,
    pub chunks: Vec<ChunkMeta>,
    dicts: Vec<OnceLock<Dict>>,
    postings: Vec<OnceLock<Vec<RoaringBitmap>>>,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("path", &self.path)
            .field("rows", &self.rows)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

struct Footer {
    rows: u64,
    ts_min: i64,
    ts_max: i64,
    fields: Vec<(String, ValueType)>,
    storage: Vec<FieldStorage>,
    chunks: Vec<ChunkMeta>,
    slots: usize,
}

impl Segment {
    /// Maps `path` and validates its header, tail and footer.
    pub fn open(path: &Path) -> Result<Segment> {
        let file = File::open(path).map_err(|e| StoreError::io(path, e))?;
        // SAFETY: segment files are immutable once written and the store never
        // truncates or rewrites a published segment; deletion happens only
        // after the last reader drops its `Arc<Segment>`.
        let map = unsafe { Mmap::map(&file) }.map_err(|e| StoreError::io(path, e))?;
        let corrupt = |off: u64, why: &str| StoreError::corrupt(path, off, why);
        let len = map.len();
        if len < HEADER_LEN as usize + TAIL_LEN || &map[..4] != MAGIC {
            return Err(corrupt(0, "bad segment header"));
        }
        let version = u16::from_le_bytes([map[4], map[5]]);
        if version != 1 && version != VERSION {
            return Err(corrupt(
                4,
                &format!("unsupported segment version {version}"),
            ));
        }
        let tail = &map[len - TAIL_LEN..];
        if &tail[16..] != MAGIC {
            return Err(corrupt((len - 4) as u64, "bad segment tail magic"));
        }
        let footer_off = u64::from_le_bytes(tail[..8].try_into().expect("8 bytes"));
        let footer_len = u32::from_le_bytes(tail[8..12].try_into().expect("4 bytes")) as u64;
        let footer_crc = u32::from_le_bytes(tail[12..16].try_into().expect("4 bytes"));
        if footer_off.checked_add(footer_len) != Some((len - TAIL_LEN) as u64) {
            return Err(corrupt(
                (len - TAIL_LEN) as u64,
                "footer bounds do not match file size",
            ));
        }
        let footer = &map[footer_off as usize..len - TAIL_LEN];
        if crc32(footer) != footer_crc {
            return Err(corrupt(footer_off, "footer checksum mismatch"));
        }
        let parsed = parse_footer(footer, version).map_err(|e| corrupt(footer_off, e.0))?;
        if parsed.chunks.iter().map(|c| u64::from(c.rows)).sum::<u64>() != parsed.rows {
            return Err(corrupt(footer_off, "chunk rows do not add up"));
        }
        let nfields = parsed.fields.len();
        Ok(Segment {
            path: path.to_owned(),
            map,
            rows: parsed.rows,
            ts_min: parsed.ts_min,
            ts_max: parsed.ts_max,
            fields: parsed.fields,
            storage: parsed.storage,
            chunks: parsed.chunks,
            dicts: (0..parsed.slots).map(|_| OnceLock::new()).collect(),
            postings: (0..nfields).map(|_| OnceLock::new()).collect(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn block(&self, b: &BlockRef) -> Result<Vec<u8>> {
        let start = b.off as usize;
        let bytes = start
            .checked_add(b.len as usize)
            .and_then(|end| self.map.get(start..end))
            .ok_or_else(|| StoreError::corrupt(&self.path, b.off, "block out of bounds"))?;
        if crc32(bytes) != b.crc {
            return Err(StoreError::corrupt(
                &self.path,
                b.off,
                "block checksum mismatch",
            ));
        }
        let raw = zstd::bulk::decompress(bytes, b.raw_len as usize)
            .map_err(|e| StoreError::corrupt(&self.path, b.off, format!("zstd: {e}")))?;
        if raw.len() != b.raw_len as usize {
            return Err(StoreError::corrupt(
                &self.path,
                b.off,
                "block length mismatch",
            ));
        }
        Ok(raw)
    }

    /// The dictionary of string field `field`, decoded on first use; `None`
    /// for plain and non-string fields.
    pub fn field_dict(&self, field: u32) -> Result<Option<&Dict>> {
        let Some(dict) = self.storage[field as usize].dict else {
            return Ok(None);
        };
        if let Some(d) = self.dicts[dict.slot].get() {
            return Ok(Some(d));
        }
        let raw = self.block(&dict.block)?;
        let mut r = Reader::new(&raw);
        let parse = |r: &mut Reader<'_>| -> DResult<Vec<String>> {
            let n = r.len(1)?;
            (0..n).map(|_| r.str().map(str::to_owned)).collect()
        };
        let strings =
            parse(&mut r).map_err(|e| StoreError::corrupt(&self.path, dict.block.off, e.0))?;
        if strings.len() != dict.len as usize {
            return Err(StoreError::corrupt(
                &self.path,
                dict.block.off,
                "dictionary size mismatch",
            ));
        }
        let lower = strings.iter().map(|s| s.to_lowercase()).collect();
        Ok(Some(
            self.dicts[dict.slot].get_or_init(|| Dict { strings, lower }),
        ))
    }

    /// Row bitmaps per dictionary code of `field`, if the segment has
    /// postings for it.
    pub fn field_postings(&self, field: u32) -> Result<Option<&[RoaringBitmap]>> {
        let st = &self.storage[field as usize];
        let (Some(block), Some(dict)) = (st.postings, st.dict) else {
            return Ok(None);
        };
        if let Some(p) = self.postings[field as usize].get() {
            return Ok(Some(p));
        }
        let raw = self.block(&block)?;
        let bitmaps = crate::index::decode_postings(&raw, dict.len as usize)
            .map_err(|e| StoreError::corrupt(&self.path, block.off, e.0))?;
        Ok(Some(self.postings[field as usize].get_or_init(|| bitmaps)))
    }

    /// Decodes the timestamps of chunk `c`.
    pub fn timestamps(&self, c: usize) -> Result<Vec<i64>> {
        let chunk = &self.chunks[c];
        let raw = self.block(&chunk.ts_block)?;
        let mut r = Reader::new(&raw);
        let parse = |r: &mut Reader<'_>| -> DResult<Vec<i64>> {
            let mut ts = r.i64()?;
            let mut out = Vec::with_capacity(chunk.rows as usize);
            for _ in 0..chunk.rows {
                ts = ts.wrapping_add(r.uvarint()? as i64);
                out.push(ts);
            }
            Ok(out)
        };
        parse(&mut r).map_err(|e| StoreError::corrupt(&self.path, chunk.ts_block.off, e.0))
    }

    /// Decodes column `col` (index into the chunk's column list) of chunk `c`.
    pub fn column(&self, c: usize, col: usize) -> Result<Column> {
        let chunk = &self.chunks[c];
        let meta = &chunk.columns[col];
        let ty = self.fields[meta.field as usize].1;
        let dict_len = self.storage[meta.field as usize]
            .dict
            .map(|d| d.len as usize);
        let raw = self.block(&meta.block)?;
        let mut r = Reader::new(&raw);
        let mut parse = || -> DResult<Column> {
            let mut offsets = Vec::with_capacity(chunk.rows as usize + 1);
            let mut total = 0u32;
            offsets.push(0);
            for _ in 0..chunk.rows {
                let n = u32::try_from(r.uvarint()?).map_err(|_| DecodeError("count overflow"))?;
                total = total.checked_add(n).ok_or(DecodeError("count overflow"))?;
                offsets.push(total);
            }
            let data = decode_values(&mut r, ty, total as usize, dict_len)?;
            if !r.is_empty() {
                return Err(DecodeError("trailing bytes in column"));
            }
            Ok(Column { ty, offsets, data })
        };
        parse().map_err(|e| StoreError::corrupt(&self.path, meta.block.off, e.0))
    }

    /// Indexes into `fields` whose name is `name`.
    pub fn field_indexes(&self, name: &str) -> Vec<u32> {
        self.fields
            .iter()
            .enumerate()
            .filter(|(_, (n, _))| n == name)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// How string field `name` is stored here: its dictionary strings, or
    /// `None` when it is plain or absent (for compaction).
    pub fn string_encoding(&self, name: &str) -> Result<Option<StrEncoding>> {
        let Some(field) = self
            .field_indexes(name)
            .into_iter()
            .find(|f| self.fields[*f as usize].1 == ValueType::Str)
        else {
            return Ok(None);
        };
        Ok(Some(match self.field_dict(field)? {
            Some(d) => StrEncoding::Dict(d.strings().to_vec()),
            None => StrEncoding::Plain,
        }))
    }
}

fn parse_footer(footer: &[u8], version: u16) -> DResult<Footer> {
    let mut r = Reader::new(footer);
    let rows = r.u64()?;
    let ts_min = r.i64()?;
    let ts_max = r.i64()?;
    let shared_dict = if version == 1 {
        Some((BlockRef::decode(&mut r)?, r.u32()?))
    } else {
        None
    };
    let nfields = r.len(2)?;
    let mut fields = Vec::with_capacity(nfields);
    let mut storage = Vec::with_capacity(nfields);
    for i in 0..nfields {
        let name = r.str()?.to_owned();
        let ty = ValueType::from_tag(r.u8()?).ok_or(DecodeError("unknown field type"))?;
        let st = match shared_dict {
            Some((block, len)) => FieldStorage {
                dict: (ty == ValueType::Str).then_some(DictRef {
                    block,
                    len,
                    slot: 0,
                }),
                postings: None,
            },
            None => match r.u8()? {
                0 => FieldStorage::default(),
                1 => {
                    if ty != ValueType::Str {
                        return Err(DecodeError("dictionary on a non-string field"));
                    }
                    let block = BlockRef::decode(&mut r)?;
                    let len = r.u32()?;
                    let postings = match r.u8()? {
                        0 => None,
                        1 => Some(BlockRef::decode(&mut r)?),
                        _ => return Err(DecodeError("bad postings flag")),
                    };
                    FieldStorage {
                        dict: Some(DictRef {
                            block,
                            len,
                            slot: i,
                        }),
                        postings,
                    }
                }
                _ => return Err(DecodeError("unknown string encoding")),
            },
        };
        fields.push((name, ty));
        storage.push(st);
    }
    let slots = if shared_dict.is_some() { 1 } else { nfields };
    let nchunks = r.len(36)?;
    let mut chunks = Vec::with_capacity(nchunks);
    let mut first_row = 0u64;
    for _ in 0..nchunks {
        let rows = r.u32()?;
        let ts_min = r.i64()?;
        let ts_max = r.i64()?;
        let ts_block = BlockRef::decode(&mut r)?;
        let ncols = r.len(25)?;
        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let field =
                u32::try_from(r.uvarint()?).map_err(|_| DecodeError("field index overflow"))?;
            if field as usize >= fields.len() {
                return Err(DecodeError("column field index out of range"));
            }
            let block = BlockRef::decode(&mut r)?;
            let present = r.u32()?;
            let zone = ZoneMap::decode(&mut r)?;
            columns.push(ColumnMeta {
                field,
                block,
                present,
                zone,
            });
        }
        chunks.push(ChunkMeta {
            rows,
            ts_min,
            ts_max,
            ts_block,
            columns,
            first_row: u32::try_from(first_row).map_err(|_| DecodeError("too many rows"))?,
        });
        first_row += u64::from(rows);
    }
    if !r.is_empty() {
        return Err(DecodeError("trailing bytes in footer"));
    }
    Ok(Footer {
        rows,
        ts_min,
        ts_max,
        fields,
        storage,
        chunks,
        slots,
    })
}

/// Chooses each string field's encoding from the rows that will be written.
pub(crate) fn encodings_for(rows: &[&Record]) -> HashMap<String, StrEncoding> {
    let mut seen: HashMap<&str, (HashSet<&str>, usize)> = HashMap::new();
    for r in rows {
        for (name, v) in &r.fields {
            if let Value::Str(s) = v {
                let e = seen.entry(name).or_default();
                e.0.insert(s);
                e.1 += 1;
            }
        }
    }
    seen.into_iter()
        .map(|(name, (distinct, values))| {
            let enc = if choose_plain(distinct.len(), values) {
                StrEncoding::Plain
            } else {
                StrEncoding::Dict(distinct.into_iter().map(str::to_owned).collect())
            };
            (name.to_owned(), enc)
        })
        .collect()
}

/// Writes `rows` (any order; sorted here by time, stably) as a new segment.
pub(crate) fn write_segment(
    path: &Path,
    rows: &[&Record],
    level: i32,
    chunk_rows: usize,
) -> Result<SegmentSummary> {
    let mut order: Vec<&Record> = rows.to_vec();
    order.sort_by_key(|r| r.ts);
    let encodings = encodings_for(&order);
    let mut w = SegmentWriter::create(path, encodings, level, chunk_rows, true)?;
    w.push_sorted(&order)?;
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TempDir;

    pub(crate) fn read_all(seg: &Segment) -> Vec<Record> {
        let mut out = Vec::new();
        for c in 0..seg.chunks.len() {
            let ts = seg.timestamps(c).unwrap();
            let cols: Vec<Column> = (0..seg.chunks[c].columns.len())
                .map(|i| seg.column(c, i).unwrap())
                .collect();
            for (row, t) in ts.iter().enumerate() {
                let mut rec = Record::new(*t);
                for (i, col) in cols.iter().enumerate() {
                    let field = seg.chunks[c].columns[i].field;
                    let name = &seg.fields[field as usize].0;
                    let dict = seg.field_dict(field).unwrap();
                    for v in col.range(row) {
                        rec.push(name.clone(), col.value(v, dict));
                    }
                }
                out.push(rec);
            }
        }
        out
    }

    fn sorted_fields(mut r: Record) -> Record {
        r.fields.sort_by(|a, b| a.0.cmp(&b.0));
        r
    }

    fn sample_rows() -> Vec<Record> {
        (0..12_000i64)
            .map(|i| {
                let mut r = Record::new(1_000 + (i * 7919) % 5000)
                    .with("host", Value::Str(format!("Host{}", i % 13)))
                    .with("msg", Value::Str(format!("unique message {i}")))
                    .with("n", Value::I64(i - 5000));
                if i % 3 == 0 {
                    r.push("ip", Value::Ip(i as u128));
                    r.push("tag", Value::Str("a".into()));
                    r.push("tag", Value::Str("B".into()));
                }
                if i % 5 == 0 {
                    r.push("n", Value::Str("mixed".into()));
                    r.push("f", Value::F64(i as f64 / 3.0));
                    r.push("t", Value::Time(i));
                    r.push("b", Value::Bool(i % 2 == 0));
                    r.push("u", Value::U64(u64::MAX - i as u64));
                    r.push("raw", Value::Bytes(vec![i as u8; 3]));
                }
                r
            })
            .collect()
    }

    #[test]
    fn write_and_read_back() {
        let dir = TempDir::new("seg-rt");
        let rows = sample_rows();
        let path = dir.path().join(segment_file_name(1));
        let summary = write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 1000).unwrap();
        assert_eq!(summary.rows, 12_000);
        assert_eq!(summary.ts_min, 1_000);
        let seg = Segment::open(&path).unwrap();
        assert_eq!(seg.chunks.len(), 12);
        assert_eq!(seg.chunks[3].first_row, 3000);
        assert_eq!(seg.field_indexes("n").len(), 2);
        let mut expected = rows;
        expected.sort_by_key(|r| r.ts);
        let got = read_all(&seg);
        assert_eq!(got.len(), expected.len());
        for (g, e) in got.into_iter().zip(expected) {
            assert_eq!(sorted_fields(g), sorted_fields(e));
        }
        // Per-field encodings: hosts are a 13-entry dictionary, messages plain.
        let host = seg.field_indexes("host")[0];
        assert_eq!(seg.field_dict(host).unwrap().unwrap().len(), 13);
        let msg = seg.field_indexes("msg")[0];
        assert!(seg.field_dict(msg).unwrap().is_none());
        let col = seg.chunks[0]
            .columns
            .iter()
            .find(|c| c.field == host)
            .unwrap();
        assert!(matches!(col.zone, ZoneMap::Codes(_, _)));
        assert_eq!(col.present, 1000);
        // Postings: every host row is in exactly one code's bitmap.
        let postings = seg.field_postings(host).unwrap().unwrap();
        assert_eq!(postings.iter().map(|b| b.len()).sum::<u64>(), 12_000);
        assert!(seg.field_postings(msg).unwrap().is_none());
    }

    #[test]
    fn small_segments_have_no_postings() {
        let dir = TempDir::new("seg-small");
        let rows: Vec<Record> = (0..100)
            .map(|i| Record::new(i).with("h", Value::Str(format!("h{}", i % 3))))
            .collect();
        let path = dir.path().join(segment_file_name(1));
        write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 10).unwrap();
        let seg = Segment::open(&path).unwrap();
        assert!(seg.field_postings(0).unwrap().is_none());
        assert_eq!(seg.field_dict(0).unwrap().unwrap().len(), 3);
    }

    #[test]
    fn reads_format_v1() {
        // A v1 segment written by the M2 code: two string fields sharing one
        // dictionary ("a", "B", "x").
        let dir = TempDir::new("seg-v1");
        let path = dir.path().join(segment_file_name(1));
        std::fs::write(&path, v1_fixture()).unwrap();
        let seg = Segment::open(&path).unwrap();
        let got: Vec<Record> = read_all(&seg).into_iter().map(sorted_fields).collect();
        assert_eq!(
            got,
            vec![
                Record::new(1)
                    .with("k", Value::Str("x".into()))
                    .with("s", Value::Str("a".into())),
                Record::new(2)
                    .with("k", Value::Str("x".into()))
                    .with("s", Value::Str("B".into())),
            ]
        );
        assert_eq!(
            seg.string_encoding("s").unwrap(),
            Some(StrEncoding::Dict(vec!["a".into(), "B".into(), "x".into()]))
        );
    }

    /// Builds a v1 file by hand, following the M2 layout.
    fn v1_fixture() -> Vec<u8> {
        let mut file = Vec::new();
        file.extend_from_slice(MAGIC);
        file.extend_from_slice(&1u16.to_le_bytes());
        file.extend_from_slice(&0u16.to_le_bytes());
        let block = |file: &mut Vec<u8>, raw: &[u8]| {
            let c = zstd::bulk::compress(raw, 3).unwrap();
            let b = BlockRef {
                off: file.len() as u64,
                len: c.len() as u32,
                raw_len: raw.len() as u32,
                crc: crc32(&c),
            };
            file.extend_from_slice(&c);
            b
        };
        let mut ts = Vec::new();
        ts.extend_from_slice(&1i64.to_le_bytes());
        put_uvarint(&mut ts, 0);
        put_uvarint(&mut ts, 1);
        let ts_block = block(&mut file, &ts);
        // Dictionary codes: a=0, B=1, x=2.
        let s_block = block(&mut file, &[1, 1, 0, 1]);
        let k_block = block(&mut file, &[1, 1, 2, 2]);
        let mut dict = Vec::new();
        put_len(&mut dict, 3);
        for s in ["a", "B", "x"] {
            put_lp(&mut dict, s.as_bytes());
        }
        let dict_block = block(&mut file, &dict);
        let mut footer = Vec::new();
        footer.extend_from_slice(&2u64.to_le_bytes());
        footer.extend_from_slice(&1i64.to_le_bytes());
        footer.extend_from_slice(&2i64.to_le_bytes());
        dict_block.encode(&mut footer);
        footer.extend_from_slice(&3u32.to_le_bytes());
        put_len(&mut footer, 2);
        for name in ["s", "k"] {
            put_lp(&mut footer, name.as_bytes());
            footer.push(ValueType::Str as u8);
        }
        put_len(&mut footer, 1);
        footer.extend_from_slice(&2u32.to_le_bytes());
        footer.extend_from_slice(&1i64.to_le_bytes());
        footer.extend_from_slice(&2i64.to_le_bytes());
        ts_block.encode(&mut footer);
        put_len(&mut footer, 2);
        for (field, b) in [(0usize, s_block), (1, k_block)] {
            put_len(&mut footer, field);
            b.encode(&mut footer);
            footer.extend_from_slice(&2u32.to_le_bytes());
            ZoneMap::None.encode(&mut footer);
        }
        let off = file.len() as u64;
        file.extend_from_slice(&footer);
        file.extend_from_slice(&off.to_le_bytes());
        file.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        file.extend_from_slice(&crc32(&footer).to_le_bytes());
        file.extend_from_slice(MAGIC);
        file
    }

    #[test]
    fn rejects_unsorted_and_corruption() {
        let dir = TempDir::new("seg-bad");
        let path = dir.path().join(segment_file_name(2));
        let mut w = SegmentWriter::create(&path, HashMap::new(), 3, 10, false).unwrap();
        w.push(Record::new(5)).unwrap();
        assert!(w.push(Record::new(4)).is_err());
        drop(w);

        let path = dir.path().join(segment_file_name(3));
        let rows: Vec<Record> = (0..100)
            .map(|i| Record::new(i).with("s", Value::Str(format!("s{i}"))))
            .collect();
        write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 10).unwrap();
        let good = std::fs::read(&path).unwrap();
        // Flip a byte in the first data block: open succeeds, the read fails.
        let mut bad = good.clone();
        bad[HEADER_LEN as usize + 2] ^= 1;
        std::fs::write(&path, &bad).unwrap();
        let seg = Segment::open(&path).unwrap();
        assert!(matches!(seg.timestamps(0), Err(StoreError::Corrupt { .. })));
        drop(seg);
        // Flip a footer byte: open fails.
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - TAIL_LEN - 3] ^= 1;
        std::fs::write(&path, &bad).unwrap();
        assert!(matches!(
            Segment::open(&path),
            Err(StoreError::Corrupt { .. })
        ));
        // Truncated file: open fails.
        std::fs::write(&path, &good[..good.len() - 1]).unwrap();
        assert!(Segment::open(&path).is_err());
    }

    #[test]
    fn dictionary_order_is_case_insensitive() {
        let mut d = vec![
            "b".to_owned(),
            "A".into(),
            "a".into(),
            "B".into(),
            "a".into(),
        ];
        Dict::sort(&mut d);
        assert_eq!(d, ["A", "a", "B", "b"]);
    }

    #[test]
    fn plain_choice() {
        assert!(!choose_plain(10, 10), "tiny fields keep a dictionary");
        assert!(choose_plain(600, 1000));
        assert!(!choose_plain(400, 1000));
    }
}
