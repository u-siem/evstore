//! Query execution (always one tenant per query).
//!
//! Design: `docs/design/08-components.md` (query execution) and
//! `docs/design/09-design-details.md` (aggregation model).
//!
//! - Pipeline: segment pruning → schema-group pruning → index phase →
//!   chunk pruning → residual scan → late materialization.
//! - Aggregations in tiers: metadata → code histograms → code-level →
//!   vectorized kernels, all producing mergeable partial results.
//! - Per-tenant cache of partial aggregates keyed by immutable segment id.
//! - Serializable partial results so an external layer can federate queries
//!   across instances.
//!
//! # MVP implementation
//!
//! A [`Query`] is a time range, a [`Filter`], an [`Order`] and an optional
//! limit. Execution, per segment:
//!
//! 1. prune by time range, then by field presence (a leaf on a field the
//!    segment does not have is `false`);
//! 2. prune chunks by time and zone maps, then by postings (the index
//!    phase, see [`crate::index`]);
//! 3. evaluate string predicates once per distinct value of each field's
//!    dictionary (plain string fields are matched value by value);
//! 4. scan only the filter's columns, then materialize matching rows;
//! 5. merge segments and the in-memory buffer by time (k-way), stopping at
//!    the limit.
//!
//! Semantics: a leaf matches a record when **any** value of the field
//! matches (multi-valued fields); a missing field never matches, so
//! `Not(leaf)` matches records without the field. Strings compare
//! case-insensitively. Values of different numeric types compare
//! numerically; other type mismatches never match. Aggregations live in
//! [`crate::aggregate`].

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::codec::{Reader, get_field};
use crate::error::{Result, StoreError};
use crate::model::{Record, Value, ValueType, compare};
use crate::registry::OVERFLOW_FIELD;
use crate::segment::{ColData, Column, Segment, ZoneMap};

/// Pseudo-field matching every string value of a record.
pub const ANY_FIELD: &str = "@any";

/// A boolean filter over record fields.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Filter {
    /// Matches everything.
    #[default]
    True,
    /// All children match (`And(vec![])` is true).
    And(Vec<Filter>),
    /// Any child matches (`Or(vec![])` is false).
    Or(Vec<Filter>),
    /// The child does not match.
    Not(Box<Filter>),
    /// A predicate on one field (or [`ANY_FIELD`]).
    Field {
        /// Field name.
        field: String,
        /// Predicate on the field's values.
        op: FieldOp,
    },
}

impl Filter {
    /// Shorthand for a field predicate.
    pub fn field(field: impl Into<String>, op: FieldOp) -> Filter {
        Filter::Field {
            field: field.into(),
            op,
        }
    }

    /// Evaluates the filter on one record (the reference semantics).
    ///
    /// To test many records, compile once with [`RecordFilter::new`].
    pub fn matches(&self, rec: &Record) -> bool {
        RecordFilter::new(self).matches(rec)
    }
}

/// A [`Filter`] compiled for repeated evaluation on records.
#[derive(Debug)]
pub struct RecordFilter(RecPlan);

#[derive(Debug)]
enum RecPlan {
    True,
    And(Vec<RecPlan>),
    Or(Vec<RecPlan>),
    Not(Box<RecPlan>),
    Leaf { field: String, op: CompiledOp },
}

impl RecordFilter {
    /// Compiles `filter`.
    pub fn new(filter: &Filter) -> RecordFilter {
        fn build(f: &Filter) -> RecPlan {
            match f {
                Filter::True => RecPlan::True,
                Filter::And(v) => RecPlan::And(v.iter().map(build).collect()),
                Filter::Or(v) => RecPlan::Or(v.iter().map(build).collect()),
                Filter::Not(f) => RecPlan::Not(Box::new(build(f))),
                Filter::Field { field, op } => RecPlan::Leaf {
                    field: field.clone(),
                    op: CompiledOp::new(op),
                },
            }
        }
        RecordFilter(build(filter))
    }

    /// Evaluates the filter on `rec`.
    pub fn matches(&self, rec: &Record) -> bool {
        fn eval(p: &RecPlan, rec: &Record) -> bool {
            match p {
                RecPlan::True => true,
                RecPlan::And(v) => v.iter().all(|p| eval(p, rec)),
                RecPlan::Or(v) => v.iter().any(|p| eval(p, rec)),
                RecPlan::Not(p) => !eval(p, rec),
                RecPlan::Leaf { field, op } if field == ANY_FIELD => rec
                    .fields
                    .iter()
                    .any(|(_, v)| matches!(v, Value::Str(_)) && op.matches_value(v)),
                RecPlan::Leaf { field, op } => rec.values(field).any(|v| op.matches_value(v)),
            }
        }
        eval(&self.0, rec)
    }
}

/// Predicate on the values of one field.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldOp {
    /// The field has at least one value.
    Exists,
    /// A value equals the literal.
    Eq(Value),
    /// A value equals any literal.
    In(Vec<Value>),
    /// A string value starts with the prefix.
    Prefix(String),
    /// A value lies within the bounds (`bool` = inclusive).
    Range {
        /// Lower bound.
        lower: Option<(Value, bool)>,
        /// Upper bound.
        upper: Option<(Value, bool)>,
    },
    /// An IP value lies in `lo..=hi` (IPv4 is IPv4-mapped).
    Cidr {
        /// First address of the network.
        lo: u128,
        /// Last address of the network.
        hi: u128,
    },
    /// A string value contains the substring.
    Contains(String),
    /// A string value matches the glob (`*` any run, `?` one character).
    Glob(String),
    /// The field has a value of this type.
    HasType(ValueType),
}

/// Inclusive-exclusive time window `[start, end)` on `Record::ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeRange {
    /// Inclusive start (`None` = unbounded).
    pub start: Option<i64>,
    /// Exclusive end (`None` = unbounded).
    pub end: Option<i64>,
}

impl TimeRange {
    /// The unbounded range.
    pub const ALL: TimeRange = TimeRange {
        start: None,
        end: None,
    };

    /// True when `ts` is inside the range.
    pub fn contains(&self, ts: i64) -> bool {
        self.start.is_none_or(|s| ts >= s) && self.end.is_none_or(|e| ts < e)
    }

    /// True when `[min, max]` intersects the range.
    pub fn overlaps(&self, min: i64, max: i64) -> bool {
        self.start.is_none_or(|s| max >= s) && self.end.is_none_or(|e| min < e)
    }
}

/// Result order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    /// Oldest first; ties keep ingest order.
    #[default]
    Asc,
    /// Newest first.
    Desc,
    /// Whatever order is cheapest (segment by segment, then the buffer).
    Storage,
}

/// A single-tenant query.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Query {
    /// Time window.
    pub time: TimeRange,
    /// Filter on fields.
    pub filter: Filter,
    /// Result order.
    pub order: Order,
    /// Maximum number of records.
    pub limit: Option<usize>,
}

/// Iterator over query results.
pub type RecordIter = Box<dyn Iterator<Item = Result<Record>> + Send>;

// ---------------------------------------------------------------------------
// Compiled predicates

#[derive(Debug)]
struct CompiledOp {
    op: FieldOp,
    /// Lowercase string literals, for string comparisons.
    lower: Vec<String>,
    /// Shape of a `Glob` pattern (`None` for other operators).
    glob: Option<GlobShape>,
}

/// A glob pattern classified once, so common shapes skip backtracking.
#[derive(Debug)]
enum GlobShape {
    /// Only `*`s: matches everything.
    Any,
    /// No wildcard: whole-string equality.
    Exact(String),
    /// `lit*`.
    Prefix(String),
    /// `*lit`.
    Suffix(String),
    /// `*lit*`.
    Contains(String),
    /// Anything else.
    General,
}

impl GlobShape {
    /// Classifies a lowercase pattern.
    fn of(pattern: &str) -> GlobShape {
        let core = pattern.trim_matches('*');
        if core.contains(['*', '?']) {
            return GlobShape::General;
        }
        if core.is_empty() {
            return if pattern.is_empty() {
                GlobShape::Exact(String::new())
            } else {
                GlobShape::Any
            };
        }
        let lit = core.to_string();
        match (pattern.starts_with('*'), pattern.ends_with('*')) {
            (true, true) => GlobShape::Contains(lit),
            (true, false) => GlobShape::Suffix(lit),
            (false, true) => GlobShape::Prefix(lit),
            (false, false) => GlobShape::Exact(lit),
        }
    }

    fn matches(&self, pattern: &str, s: &str) -> bool {
        match self {
            GlobShape::Any => true,
            GlobShape::Exact(l) => s == l,
            GlobShape::Prefix(l) => s.starts_with(l.as_str()),
            GlobShape::Suffix(l) => s.ends_with(l.as_str()),
            GlobShape::Contains(l) => s.contains(l.as_str()),
            GlobShape::General => glob_match(pattern, s),
        }
    }
}

impl CompiledOp {
    fn new(op: &FieldOp) -> CompiledOp {
        let lower = match op {
            FieldOp::Eq(Value::Str(s))
            | FieldOp::Prefix(s)
            | FieldOp::Contains(s)
            | FieldOp::Glob(s) => {
                vec![s.to_lowercase()]
            }
            FieldOp::In(vals) => vals
                .iter()
                .filter_map(|v| match v {
                    Value::Str(s) => Some(s.to_lowercase()),
                    _ => None,
                })
                .collect(),
            FieldOp::Range { lower, upper } => [lower, upper]
                .into_iter()
                .map(|b| match b {
                    Some((Value::Str(s), _)) => s.to_lowercase(),
                    _ => String::new(),
                })
                .collect(),
            _ => Vec::new(),
        };
        let glob = match op {
            FieldOp::Glob(_) => Some(GlobShape::of(&lower[0])),
            _ => None,
        };
        CompiledOp {
            op: op.clone(),
            lower,
            glob,
        }
    }

    /// Answers the predicate for a field that has at least one value
    /// without looking at the values, when the operator allows it.
    fn on_presence(&self, ty: ValueType) -> Option<bool> {
        match &self.op {
            FieldOp::Exists => Some(true),
            FieldOp::HasType(t) => Some(*t == ty),
            _ => None,
        }
    }

    /// Matches a lowercase string value.
    fn matches_lower(&self, s: &str) -> bool {
        match &self.op {
            FieldOp::Exists => true,
            FieldOp::Eq(Value::Str(_)) => s == self.lower[0],
            FieldOp::In(_) => self.lower.iter().any(|l| l == s),
            FieldOp::Prefix(_) => s.starts_with(self.lower[0].as_str()),
            FieldOp::Contains(_) => s.contains(self.lower[0].as_str()),
            FieldOp::Glob(_) => self
                .glob
                .as_ref()
                .is_some_and(|g| g.matches(&self.lower[0], s)),
            FieldOp::Range { lower, upper } => {
                let ok_lo = match lower {
                    None => true,
                    Some((Value::Str(_), inc)) => {
                        bound_ok(s.cmp(self.lower[0].as_str()), true, *inc)
                    }
                    Some(_) => false,
                };
                let ok_hi = match upper {
                    None => true,
                    Some((Value::Str(_), inc)) => {
                        bound_ok(s.cmp(self.lower[1].as_str()), false, *inc)
                    }
                    Some(_) => false,
                };
                ok_lo && ok_hi
            }
            FieldOp::HasType(t) => *t == ValueType::Str,
            FieldOp::Eq(_) | FieldOp::Cidr { .. } => false,
        }
    }

    fn matches_value(&self, v: &Value) -> bool {
        if let Value::Str(s) = v {
            return with_lower(s, |l| self.matches_lower(l));
        }
        match &self.op {
            FieldOp::Exists => true,
            FieldOp::Eq(lit) => compare(v, lit) == Some(Ordering::Equal),
            FieldOp::In(lits) => lits.iter().any(|l| compare(v, l) == Some(Ordering::Equal)),
            FieldOp::Range { lower, upper } => range_ok(v, lower.as_ref(), upper.as_ref()),
            FieldOp::Cidr { lo, hi } => matches!(v, Value::Ip(ip) if ip >= lo && ip <= hi),
            FieldOp::HasType(t) => v.value_type() == *t,
            FieldOp::Prefix(_) | FieldOp::Contains(_) | FieldOp::Glob(_) => false,
        }
    }

    /// Conservative check: can any value in `[lo, hi]` match?
    fn may_match_range(&self, lo: &Value, hi: &Value) -> bool {
        match &self.op {
            FieldOp::Exists => true,
            FieldOp::Eq(lit) => within(lit, lo, hi),
            FieldOp::In(lits) => lits.iter().any(|l| within(l, lo, hi)),
            FieldOp::Range { lower, upper } => {
                let lo_ok = match lower {
                    None => true,
                    Some((b, inc)) => compare(hi, b).is_some_and(|o| bound_ok(o, true, *inc)),
                };
                let hi_ok = match upper {
                    None => true,
                    Some((b, inc)) => compare(lo, b).is_some_and(|o| bound_ok(o, false, *inc)),
                };
                lo_ok && hi_ok
            }
            FieldOp::Cidr { lo: a, hi: b } => match (lo, hi) {
                (Value::Ip(x), Value::Ip(y)) => x <= b && y >= a,
                _ => false,
            },
            FieldOp::HasType(t) => lo.value_type() == *t,
            FieldOp::Prefix(_) | FieldOp::Contains(_) | FieldOp::Glob(_) => false,
        }
    }
}

std::thread_local! {
    /// Reused buffer for lowercasing ASCII values in scans.
    static LOWER: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// Calls `f` with the lowercase form of `s`. ASCII text is lowercased into
/// a reused per-thread buffer; only non-ASCII text allocates (it needs the
/// full Unicode rules of `str::to_lowercase`).
fn with_lower<T>(s: &str, f: impl FnOnce(&str) -> T) -> T {
    let mut upper = false;
    for b in s.bytes() {
        if !b.is_ascii() {
            return f(&s.to_lowercase());
        }
        upper |= b.is_ascii_uppercase();
    }
    if !upper {
        return f(s);
    }
    LOWER.with(|cell| match cell.try_borrow_mut() {
        Ok(mut buf) => {
            buf.clear();
            buf.push_str(s);
            buf.make_ascii_lowercase();
            f(&buf)
        }
        Err(_) => f(&s.to_ascii_lowercase()),
    })
}

fn within(v: &Value, lo: &Value, hi: &Value) -> bool {
    compare(v, lo).is_some_and(|o| o != Ordering::Less)
        && compare(v, hi).is_some_and(|o| o != Ordering::Greater)
}

/// `ord` is `value.cmp(bound)`; `lower` says which side the bound is.
fn bound_ok(ord: Ordering, lower: bool, inclusive: bool) -> bool {
    match ord {
        Ordering::Equal => inclusive,
        Ordering::Greater => lower,
        Ordering::Less => !lower,
    }
}

fn range_ok(v: &Value, lower: Option<&(Value, bool)>, upper: Option<&(Value, bool)>) -> bool {
    let lo = match lower {
        None => true,
        Some((b, inc)) => compare(v, b).is_some_and(|o| bound_ok(o, true, *inc)),
    };
    let hi = match upper {
        None => true,
        Some((b, inc)) => compare(v, b).is_some_and(|o| bound_ok(o, false, *inc)),
    };
    lo && hi
}

/// Glob match: `*` matches any run of characters, `?` one character. Every
/// `*` and `?` in the pattern is a wildcard (there is no escaping).
///
/// Works on UTF-8 bytes without allocating: literals compare byte by byte,
/// and `?` or a backtracking `*` advance by a whole character, so positions
/// in `text` always stay on character boundaries.
pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    fn char_len(lead: u8) -> usize {
        match lead {
            0x00..=0x7f => 1,
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            _ => 4,
        }
    }
    let (p, t) = (pattern.as_bytes(), text.as_bytes());
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == b'?' {
            pi += 1;
            ti += char_len(t[ti]);
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((sp, st)) = star {
            let next = st + char_len(t[st]);
            pi = sp + 1;
            ti = next;
            star = Some((sp, next));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == b'*')
}

// ---------------------------------------------------------------------------
// Per-segment plan

pub(crate) struct Leaf {
    op: CompiledOp,
    /// Segment field indexes holding values of this field.
    fields: Vec<u32>,
    /// Name to look for in overflow entries (`None` = `@any`).
    name: Option<String>,
    /// Overflow field index, when the segment has one.
    overflow: Option<u32>,
    /// Per dictionary-coded string field: which codes match.
    codes: HashMap<u32, CodeMatch>,
}

/// Predicate results for one field dictionary.
struct CodeMatch {
    /// Does code `i` match?
    hits: Vec<bool>,
    /// Prefix sums of `hits`, for zone-map checks.
    sums: Vec<u32>,
}

pub(crate) enum Plan {
    Const(bool),
    And(Vec<Plan>),
    Or(Vec<Plan>),
    Not(Box<Plan>),
    Leaf(Box<Leaf>),
}

pub(crate) fn compile(filter: &Filter, seg: &Segment) -> Result<Plan> {
    Ok(match filter {
        Filter::True => Plan::Const(true),
        Filter::And(children) => {
            let mut out = Vec::new();
            for c in children {
                match compile(c, seg)? {
                    Plan::Const(true) => {}
                    Plan::Const(false) => return Ok(Plan::Const(false)),
                    p => out.push(p),
                }
            }
            if out.is_empty() {
                Plan::Const(true)
            } else {
                Plan::And(out)
            }
        }
        Filter::Or(children) => {
            let mut out = Vec::new();
            for c in children {
                match compile(c, seg)? {
                    Plan::Const(false) => {}
                    Plan::Const(true) => return Ok(Plan::Const(true)),
                    p => out.push(p),
                }
            }
            if out.is_empty() {
                Plan::Const(false)
            } else {
                Plan::Or(out)
            }
        }
        Filter::Not(child) => match compile(child, seg)? {
            Plan::Const(b) => Plan::Const(!b),
            p => Plan::Not(Box::new(p)),
        },
        Filter::Field { field, op } => {
            if field == OVERFLOW_FIELD {
                return Ok(Plan::Const(false));
            }
            let any = field == ANY_FIELD;
            let fields: Vec<u32> = if any {
                seg.fields
                    .iter()
                    .enumerate()
                    .filter(|(_, (n, t))| *t == ValueType::Str && n != OVERFLOW_FIELD)
                    .map(|(i, _)| u32::try_from(i).expect("parse_footer caps fields at u32"))
                    .collect()
            } else {
                seg.field_indexes(field)
            };
            let overflow = seg.field_indexes(OVERFLOW_FIELD).first().copied();
            if fields.is_empty() && overflow.is_none() {
                return Ok(Plan::Const(false));
            }
            let op = CompiledOp::new(op);
            let mut codes = HashMap::new();
            for f in &fields {
                let Some(dict) = seg.field_dict(*f)? else {
                    continue;
                };
                let hits: Vec<bool> = (0..u32::try_from(dict.len())
                    .expect("dictionary sizes are u32"))
                    .map(|c| op.matches_lower(dict.lower(c)))
                    .collect();
                let mut sums = Vec::with_capacity(hits.len() + 1);
                let mut acc = 0u32;
                sums.push(0);
                for m in &hits {
                    acc += u32::from(*m);
                    sums.push(acc);
                }
                codes.insert(*f, CodeMatch { hits, sums });
            }
            Plan::Leaf(Box::new(Leaf {
                op,
                fields,
                name: (!any).then(|| field.clone()),
                overflow,
                codes,
            }))
        }
    })
}

/// Conservative chunk check: false means no row in the chunk can match.
pub(crate) fn may_match(plan: &Plan, seg: &Segment, chunk: usize) -> bool {
    match plan {
        Plan::Const(b) => *b,
        Plan::And(v) => v.iter().all(|p| may_match(p, seg, chunk)),
        Plan::Or(v) => v.iter().any(|p| may_match(p, seg, chunk)),
        Plan::Not(_) => true,
        Plan::Leaf(leaf) => seg.chunks[chunk].columns.iter().any(|col| {
            if Some(col.field) == leaf.overflow {
                return true;
            }
            if !leaf.fields.contains(&col.field) {
                return false;
            }
            match &col.zone {
                ZoneMap::None => true,
                ZoneMap::Codes(lo, hi) => leaf
                    .codes
                    .get(&col.field)
                    .is_none_or(|m| m.sums[*hi as usize + 1] > m.sums[*lo as usize]),
                ZoneMap::Values(lo, hi) => leaf.op.may_match_range(lo, hi),
            }
        }),
    }
}

/// Index phase: rows that may match according to postings, or `None` when
/// the postings cannot narrow the plan (every row is a candidate).
pub(crate) fn candidates(plan: &Plan, seg: &Segment) -> Result<Option<RoaringBitmap>> {
    Ok(match plan {
        Plan::Const(true) | Plan::Not(_) => None,
        Plan::Const(false) => Some(RoaringBitmap::new()),
        Plan::And(v) => {
            let mut acc: Option<RoaringBitmap> = None;
            for p in v {
                if let Some(c) = candidates(p, seg)? {
                    acc = Some(match acc {
                        None => c,
                        Some(a) => a & c,
                    });
                }
            }
            acc
        }
        Plan::Or(v) => {
            let mut acc = RoaringBitmap::new();
            for p in v {
                match candidates(p, seg)? {
                    None => return Ok(None),
                    Some(c) => acc |= c,
                }
            }
            Some(acc)
        }
        Plan::Leaf(leaf) => {
            if leaf.overflow.is_some() {
                return Ok(None);
            }
            let mut acc = RoaringBitmap::new();
            for f in &leaf.fields {
                let (Some(m), Some(postings)) = (leaf.codes.get(f), seg.field_postings(*f)?) else {
                    return Ok(None);
                };
                for (code, hit) in m.hits.iter().enumerate() {
                    if *hit {
                        acc |= &postings[code];
                    }
                }
            }
            Some(acc)
        }
    })
}

/// Whether `rows` holds any row of chunk `chunk`.
pub(crate) fn chunk_has_candidates(rows: &RoaringBitmap, seg: &Segment, chunk: usize) -> bool {
    let c = &seg.chunks[chunk];
    let (first, last) = (c.first_row, c.first_row + c.rows - 1);
    let below = if first == 0 { 0 } else { rows.rank(first - 1) };
    rows.rank(last) > below
}

pub(crate) fn collect_fields(plan: &Plan, out: &mut Vec<u32>) {
    match plan {
        Plan::Const(_) => {}
        Plan::And(v) | Plan::Or(v) => v.iter().for_each(|p| collect_fields(p, out)),
        Plan::Not(p) => collect_fields(p, out),
        Plan::Leaf(l) => {
            out.extend(&l.fields);
            out.extend(l.overflow);
        }
    }
}

/// Decoded columns of one chunk, keyed by segment field index.
pub(crate) struct ChunkData {
    pub columns: HashMap<u32, Column>,
    pub overflow: Option<Vec<Vec<(String, Value)>>>,
}

impl ChunkData {
    pub(crate) fn new() -> ChunkData {
        ChunkData {
            columns: HashMap::new(),
            overflow: None,
        }
    }

    /// Decodes the chunk's columns in `wanted` (all when `None`) that are
    /// not loaded yet.
    pub(crate) fn load(
        &mut self,
        seg: &Segment,
        chunk: usize,
        wanted: Option<&[u32]>,
    ) -> Result<()> {
        for (i, col) in seg.chunks[chunk].columns.iter().enumerate() {
            if wanted.is_some_and(|w| !w.contains(&col.field)) {
                continue;
            }
            if seg.fields[col.field as usize].0 == OVERFLOW_FIELD {
                if self.overflow.is_none() {
                    self.overflow = Some(decode_overflow(seg, &seg.column(chunk, i)?)?);
                }
            } else if let std::collections::hash_map::Entry::Vacant(slot) =
                self.columns.entry(col.field)
            {
                slot.insert(seg.column(chunk, i)?);
            }
        }
        Ok(())
    }
}

fn decode_overflow(seg: &Segment, col: &Column) -> Result<Vec<Vec<(String, Value)>>> {
    let ColData::Bytes(items) = &col.data else {
        return Err(StoreError::corrupt(
            seg.path(),
            0,
            "overflow column is not bytes",
        ));
    };
    let rows = col.offsets.len() - 1;
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut fields = Vec::new();
        for i in col.range(row) {
            let mut r = Reader::new(&items[i]);
            let f = get_field(&mut r).map_err(|e| StoreError::corrupt(seg.path(), 0, e.0))?;
            fields.push(f);
        }
        out.push(fields);
    }
    Ok(out)
}

pub(crate) fn eval(plan: &Plan, data: &ChunkData, row: usize) -> bool {
    match plan {
        Plan::Const(b) => *b,
        Plan::And(v) => v.iter().all(|p| eval(p, data, row)),
        Plan::Or(v) => v.iter().any(|p| eval(p, data, row)),
        Plan::Not(p) => !eval(p, data, row),
        Plan::Leaf(leaf) => {
            for f in &leaf.fields {
                let Some(col) = data.columns.get(f) else {
                    continue;
                };
                let range = col.range(row);
                if range.is_empty() {
                    continue;
                }
                if let Some(hit) = leaf.op.on_presence(col.ty) {
                    if hit {
                        return true;
                    }
                    continue;
                }
                let hit = match &col.data {
                    ColData::Str(codes) => leaf
                        .codes
                        .get(f)
                        .is_some_and(|m| codes[range].iter().any(|c| m.hits[*c as usize])),
                    ColData::Plain(v) => range
                        .into_iter()
                        .any(|i| with_lower(v.get(i), |l| leaf.op.matches_lower(l))),
                    ColData::Int(v) => {
                        let time = col.ty == ValueType::Time;
                        v[range].iter().any(|x| {
                            leaf.op.matches_value(&if time {
                                Value::Time(*x)
                            } else {
                                Value::I64(*x)
                            })
                        })
                    }
                    ColData::U64(v) => v[range]
                        .iter()
                        .any(|x| leaf.op.matches_value(&Value::U64(*x))),
                    ColData::F64(v) => v[range]
                        .iter()
                        .any(|x| leaf.op.matches_value(&Value::F64(*x))),
                    ColData::Bool(v) => v[range]
                        .iter()
                        .any(|x| leaf.op.matches_value(&Value::Bool(*x))),
                    ColData::Ip(v) => v[range]
                        .iter()
                        .any(|x| leaf.op.matches_value(&Value::Ip(*x))),
                    ColData::Bytes(v) => v[range]
                        .iter()
                        .any(|x| leaf.op.matches_value(&Value::Bytes(x.clone()))),
                };
                if hit {
                    return true;
                }
            }
            if let Some(rows) = &data.overflow {
                return rows[row].iter().any(|(n, v)| match &leaf.name {
                    Some(name) => n == name && leaf.op.matches_value(v),
                    None => matches!(v, Value::Str(_)) && leaf.op.matches_value(v),
                });
            }
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Segment scan

/// Streams matching records of one segment in time order.
pub(crate) struct SegmentScan {
    seg: Arc<Segment>,
    plan: Plan,
    filter_fields: Vec<u32>,
    time: TimeRange,
    desc: bool,
    /// Remaining chunk indexes, in visiting order (reversed for popping).
    chunks: Vec<usize>,
    ready: std::vec::IntoIter<Record>,
    failed: bool,
}

impl SegmentScan {
    /// Returns `None` when the segment cannot match at all.
    pub(crate) fn new(
        seg: Arc<Segment>,
        filter: &Filter,
        time: TimeRange,
        desc: bool,
    ) -> Result<Option<SegmentScan>> {
        if !time.overlaps(seg.ts_min, seg.ts_max) {
            return Ok(None);
        }
        let plan = compile(filter, &seg)?;
        if matches!(plan, Plan::Const(false)) {
            return Ok(None);
        }
        let mut filter_fields = Vec::new();
        collect_fields(&plan, &mut filter_fields);
        filter_fields.sort_unstable();
        filter_fields.dedup();
        let rows = candidates(&plan, &seg)?;
        let mut chunks: Vec<usize> = (0..seg.chunks.len())
            .filter(|c| {
                let m = &seg.chunks[*c];
                time.overlaps(m.ts_min, m.ts_max)
                    && may_match(&plan, &seg, *c)
                    && rows
                        .as_ref()
                        .is_none_or(|r| chunk_has_candidates(r, &seg, *c))
            })
            .collect();
        if !desc {
            chunks.reverse();
        }
        Ok(Some(SegmentScan {
            seg,
            plan,
            filter_fields,
            time,
            desc,
            chunks,
            ready: Vec::new().into_iter(),
            failed: false,
        }))
    }

    /// First timestamp this scan can produce (its next chunk's minimum
    /// ascending, maximum descending); `None` when no chunk survived pruning.
    pub(crate) fn bound(&self) -> Option<i64> {
        let c = &self.seg.chunks[*self.chunks.last()?];
        Some(if self.desc { c.ts_max } else { c.ts_min })
    }

    fn load_chunk(&mut self, c: usize) -> Result<Vec<Record>> {
        let seg = &self.seg;
        let ts = seg.timestamps(c)?;
        let mut data = ChunkData::new();
        data.load(seg, c, Some(&self.filter_fields))?;
        let rows: Vec<usize> = (0..ts.len())
            .filter(|r| self.time.contains(ts[*r]) && eval(&self.plan, &data, *r))
            .collect();
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        // Late materialization: decode the remaining columns only now.
        data.load(seg, c, None)?;
        let all = data;
        let chunk = &seg.chunks[c];
        let mut columns = Vec::with_capacity(chunk.columns.len());
        for meta in &chunk.columns {
            if let Some(col) = all.columns.get(&meta.field) {
                let name = &seg.fields[meta.field as usize].0;
                columns.push((name, col, seg.field_dict(meta.field)?));
            }
        }
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let mut rec = Record::new(ts[r]);
            for (name, col, dict) in &columns {
                for i in col.range(r) {
                    rec.fields.push(((*name).clone(), col.value(i, *dict)));
                }
            }
            if let Some(over) = &all.overflow {
                rec.fields.extend(over[r].iter().cloned());
            }
            out.push(rec);
        }
        if self.desc {
            out.reverse();
        }
        Ok(out)
    }
}

impl Iterator for SegmentScan {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(r) = self.ready.next() {
                return Some(Ok(r));
            }
            let c = self.chunks.pop()?;
            match self.load_chunk(c) {
                Ok(rows) => self.ready = rows.into_iter(),
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Merge

pub(crate) type Source = Box<dyn Iterator<Item = Result<Record>> + Send>;

struct Head {
    ts: i64,
    source: usize,
    desc: bool,
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    /// Max-heap order: the next record to emit is the greatest.
    fn cmp(&self, other: &Self) -> Ordering {
        if self.desc {
            self.ts.cmp(&other.ts).then(self.source.cmp(&other.source))
        } else {
            other.ts.cmp(&self.ts).then(other.source.cmp(&self.source))
        }
    }
}

/// K-way merge of time-sorted sources. Ties go to the lower source index
/// ascending and the higher one descending, which keeps ingest order.
///
/// Each source comes with a bound: the first timestamp it can produce
/// (its minimum ascending, its maximum descending). A source is only opened
/// once the merge reaches its bound, so a newest-first query with a limit
/// touches the newest segments only.
pub(crate) struct Merge {
    sources: Vec<Source>,
    bounds: Vec<i64>,
    heads: Vec<Option<Record>>,
    heap: BinaryHeap<Head>,
    /// Unopened sources, the next one to open last.
    waiting: Vec<usize>,
    desc: bool,
    pending_error: Option<StoreError>,
}

impl Merge {
    pub(crate) fn new(sources: Vec<(i64, Source)>, desc: bool) -> Merge {
        let (bounds, sources): (Vec<i64>, Vec<Source>) = sources.into_iter().unzip();
        let mut waiting: Vec<usize> = (0..sources.len()).collect();
        // Pop order: smallest bound first ascending, largest first descending;
        // on equal bounds, the source that wins ties opens first.
        waiting.sort_by(|a, b| {
            let (x, y) = (bounds[*a], bounds[*b]);
            if desc {
                x.cmp(&y).then(a.cmp(b))
            } else {
                y.cmp(&x).then(b.cmp(a))
            }
        });
        Merge {
            heads: (0..sources.len()).map(|_| None).collect(),
            sources,
            bounds,
            heap: BinaryHeap::new(),
            waiting,
            desc,
            pending_error: None,
        }
    }

    fn pull(&mut self, source: usize) {
        match self.sources[source].next() {
            Some(Ok(r)) => {
                self.heap.push(Head {
                    ts: r.ts,
                    source,
                    desc: self.desc,
                });
                self.heads[source] = Some(r);
            }
            Some(Err(e)) => {
                self.pending_error.get_or_insert(e);
            }
            None => {}
        }
    }

    /// Opens every waiting source that could emit before (or tie with) the
    /// current head.
    fn open_due(&mut self) {
        while let Some(&next) = self.waiting.last() {
            let due = match self.heap.peek() {
                None => true,
                Some(top) if self.desc => self.bounds[next] >= top.ts,
                Some(top) => self.bounds[next] <= top.ts,
            };
            if !due {
                break;
            }
            self.waiting.pop();
            self.pull(next);
        }
    }
}

impl Iterator for Merge {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        self.open_due();
        if let Some(e) = self.pending_error.take() {
            self.heap.clear();
            self.waiting.clear();
            return Some(Err(e));
        }
        let head = self.heap.pop()?;
        let rec = self.heads[head.source].take().expect("head present");
        self.pull(head.source);
        Some(Ok(rec))
    }
}

/// Builds the result iterator over segments (oldest first) and the
/// in-memory buffer (ingest order).
pub(crate) fn execute(
    segments: &[Arc<Segment>],
    buffer: &[Arc<Record>],
    query: &Query,
) -> Result<RecordIter> {
    let desc = query.order == Order::Desc;
    let mut sources: Vec<(i64, Source)> = Vec::new();
    for seg in segments {
        if let Some(scan) = SegmentScan::new(Arc::clone(seg), &query.filter, query.time, desc)?
            && let Some(bound) = scan.bound()
        {
            sources.push((bound, Box::new(scan)));
        }
    }
    // Buffered matches are shared, not copied: each record is cloned only
    // when the merge emits it.
    let rf = RecordFilter::new(&query.filter);
    let mut mem: Vec<Arc<Record>> = buffer
        .iter()
        .filter(|r| query.time.contains(r.ts) && rf.matches(r))
        .cloned()
        .collect();
    if query.order != Order::Storage {
        mem.sort_by_key(|r| r.ts);
        if desc {
            mem.reverse();
        }
    }
    if let Some(first) = mem.first() {
        sources.push((
            first.ts,
            Box::new(mem.into_iter().map(|r| Ok(Record::clone(&r)))),
        ));
    }
    let iter: RecordIter = if query.order == Order::Storage {
        Box::new(sources.into_iter().flat_map(|(_, s)| s))
    } else {
        Box::new(Merge::new(sources, desc))
    };
    Ok(match query.limit {
        Some(n) => Box::new(iter.take(n)),
        None => iter,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::segment::{Segment, segment_file_name, write_segment};
    use crate::test_util::{Rng, TempDir};

    #[test]
    fn glob() {
        assert!(glob_match("a*c", "abbbc"));
        assert!(glob_match("*", ""));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*.exe", "c:\\x\\cmd.exe"));
        assert!(!glob_match("*.exe", "cmd.exe.txt"));
        assert!(glob_match("*a*b*", "xxaxxbxx"));
        // `?` takes one character, however many bytes it has.
        assert!(glob_match("a?c", "aäc"));
        assert!(glob_match("?", "€"));
        assert!(!glob_match("??", "€"));
        assert!(glob_match("*ä*", "xxäxx"));
        assert!(glob_match("ä*€", "äxx€"));
        // A `*` in the pattern is a wildcard even facing a `*` in the text.
        assert!(glob_match("*a*", "*ba"));
        assert!(glob_match("*a", "*ba"));
    }

    #[test]
    fn glob_shapes_agree_with_general_matching() {
        let mut rng = Rng::new(0x61ab);
        let alphabet = ['a', 'b', 'ä', '*', '?', '€'];
        let gen_str = |rng: &mut Rng, max: u64| -> String {
            (0..rng.below(max))
                .map(|_| *rng.pick(&alphabet))
                .collect::<String>()
        };
        for _ in 0..20_000 {
            let pattern = gen_str(&mut rng, 6);
            let text = gen_str(&mut rng, 8).replace('?', "a");
            let shape = GlobShape::of(&pattern);
            assert_eq!(
                shape.matches(&pattern, &text),
                glob_match(&pattern, &text),
                "{pattern:?} vs {text:?} ({shape:?})"
            );
        }
    }

    #[test]
    fn lowercasing_reuses_a_buffer_and_keeps_unicode_rules() {
        assert_eq!(with_lower("ABC def", str::to_owned), "abc def");
        assert_eq!(with_lower("plain", str::to_owned), "plain");
        // Final sigma needs `str::to_lowercase`, not per-character rules.
        assert_eq!(with_lower("ΟΔΟΣ", str::to_owned), "οδος");
        // Nested use falls back instead of panicking on the shared buffer.
        assert_eq!(
            with_lower("AB", |a| with_lower("CD", |b| format!("{a}{b}"))),
            "abcd"
        );
    }

    #[test]
    fn record_semantics() {
        let r = Record::new(1)
            .with("user", Value::Str("Admin".into()))
            .with("port", Value::I64(443))
            .with("tag", Value::Str("x".into()))
            .with("tag", Value::Str("y".into()));
        let f = |field: &str, op| Filter::field(field, op);
        assert!(f("user", FieldOp::Eq(Value::Str("ADMIN".into()))).matches(&r));
        assert!(f("port", FieldOp::Eq(Value::U64(443))).matches(&r));
        assert!(f("port", FieldOp::Eq(Value::F64(443.0))).matches(&r));
        assert!(!f("port", FieldOp::Eq(Value::Str("443".into()))).matches(&r));
        assert!(f("tag", FieldOp::Eq(Value::Str("y".into()))).matches(&r));
        assert!(!f("missing", FieldOp::Exists).matches(&r));
        assert!(Filter::Not(Box::new(f("missing", FieldOp::Exists))).matches(&r));
        assert!(f(ANY_FIELD, FieldOp::Contains("dmi".into())).matches(&r));
        assert!(!Filter::Or(vec![]).matches(&r));
        assert!(Filter::And(vec![]).matches(&r));
        let range = FieldOp::Range {
            lower: Some((Value::I64(443), false)),
            upper: None,
        };
        assert!(!f("port", range).matches(&r));
    }

    // ---- oracle test: segment execution vs. `Filter::matches` ----

    const HOSTS: [&str; 6] = ["web01", "WEB02", "db01", "nas", "Ärger", "fw"];
    const USERS: [&str; 5] = ["root", "admin", "Alice", "bob", "svc-backup"];

    pub(crate) fn random_record(rng: &mut Rng, ts: i64) -> Record {
        let mut r = Record::new(ts);
        if rng.chance(90) {
            r.push("host", Value::Str((*rng.pick(&HOSTS)).into()));
        }
        if rng.chance(60) {
            r.push("user", Value::Str((*rng.pick(&USERS)).into()));
        }
        if rng.chance(15) {
            r.push("user", Value::Str((*rng.pick(&USERS)).into()));
        }
        if rng.chance(70) {
            r.push("port", Value::I64(rng.below(70_000) as i64));
        } else if rng.chance(30) {
            r.push("port", Value::Str(format!("{}", rng.below(100))));
        }
        if rng.chance(50) {
            r.push("bytes", Value::U64(rng.below(1 << 40)));
        }
        if rng.chance(30) {
            r.push("score", Value::F64(rng.below(1000) as f64 / 10.0));
        }
        if rng.chance(40) {
            r.push("ok", Value::Bool(rng.chance(50)));
        }
        if rng.chance(60) {
            let ip = 0xffff_0a00_0000u128 | u128::from(rng.below(1 << 16) as u16);
            r.push("src", Value::Ip(ip));
        }
        if rng.chance(20) {
            r.push("seen", Value::Time(ts - rng.below(1000) as i64));
        }
        if rng.chance(80) {
            let msg = format!(
                "{} login from {} id={}",
                rng.pick(&USERS),
                rng.pick(&HOSTS),
                rng.below(500)
            );
            r.push("message", Value::Str(msg));
        }
        if rng.chance(5) {
            r.push("blob", Value::Bytes(vec![rng.below(4) as u8]));
        }
        r
    }

    pub(crate) fn random_filter(rng: &mut Rng, depth: u32) -> Filter {
        let leaf = |rng: &mut Rng| -> Filter {
            let pick = rng.below(13);
            match pick {
                0 => Filter::field(
                    *rng.pick(&["host", "user", "missing", "blob"]),
                    FieldOp::Exists,
                ),
                1 => Filter::field(
                    "host",
                    FieldOp::Eq(Value::Str(rng.pick(&HOSTS).to_uppercase())),
                ),
                2 => Filter::field(
                    "user",
                    FieldOp::In(vec![
                        Value::Str((*rng.pick(&USERS)).into()),
                        Value::Str((*rng.pick(&USERS)).into()),
                    ]),
                ),
                3 => Filter::field(
                    "host",
                    FieldOp::Prefix((*rng.pick(&["web", "D", "ä", "x"])).into()),
                ),
                4 => {
                    let a = rng.below(70_000) as i64;
                    Filter::field(
                        "port",
                        FieldOp::Range {
                            lower: rng.chance(80).then(|| (Value::I64(a), rng.chance(50))),
                            upper: rng.chance(80).then(|| {
                                (Value::U64(a as u64 + rng.below(20_000)), rng.chance(50))
                            }),
                        },
                    )
                }
                5 => Filter::field(
                    "port",
                    FieldOp::Eq(Value::Str(format!("{}", rng.below(100)))),
                ),
                6 => {
                    let base = 0xffff_0a00_0000u128 | (u128::from(rng.below(256) as u8) << 8);
                    Filter::field(
                        "src",
                        FieldOp::Cidr {
                            lo: base,
                            hi: base | 0xff,
                        },
                    )
                }
                7 => Filter::field(
                    "message",
                    FieldOp::Contains((*rng.pick(&["LOGIN", "alice", "id=4", "nope"])).into()),
                ),
                8 => Filter::field(
                    ANY_FIELD,
                    FieldOp::Contains((*rng.pick(&["web", "root", "ärg"])).into()),
                ),
                9 => Filter::field(
                    "user",
                    FieldOp::Glob((*rng.pick(&["a*", "*o*", "?ob", "svc-*"])).into()),
                ),
                10 => Filter::field(
                    "score",
                    FieldOp::Range {
                        lower: Some((Value::F64(rng.below(100) as f64), true)),
                        upper: None,
                    },
                ),
                11 => Filter::field("ok", FieldOp::Eq(Value::Bool(rng.chance(50)))),
                _ => Filter::field(
                    "bytes",
                    FieldOp::Range {
                        lower: None,
                        upper: Some((Value::I64(rng.below(1 << 40) as i64), true)),
                    },
                ),
            }
        };
        if depth == 0 || rng.chance(40) {
            return leaf(rng);
        }
        match rng.below(3) {
            0 => Filter::And(
                (0..=rng.below(3))
                    .map(|_| random_filter(rng, depth - 1))
                    .collect(),
            ),
            1 => Filter::Or(
                (0..=rng.below(3))
                    .map(|_| random_filter(rng, depth - 1))
                    .collect(),
            ),
            _ => Filter::Not(Box::new(random_filter(rng, depth - 1))),
        }
    }

    fn normalize(mut r: Record) -> Record {
        r.fields.sort_by_cached_key(|(n, v)| format!("{n}={v:?}"));
        r
    }

    fn rid(r: &Record) -> u64 {
        match r.get("rid") {
            Some(Value::U64(x)) => *x,
            other => panic!("record without rid: {other:?}"),
        }
    }

    #[test]
    fn oracle_segments_match_naive_filter() {
        let dir = TempDir::new("query-oracle");
        let mut rng = Rng::new(0x5eed);
        let mut next_id = 0u64;
        let mut gen_rows = |rng: &mut Rng, n: usize| -> Vec<Record> {
            (0..n)
                .map(|_| {
                    let ts = rng.below(20_000) as i64;
                    next_id += 1;
                    random_record(rng, ts).with("rid", Value::U64(next_id))
                })
                .collect()
        };
        let mut all = Vec::new();
        let mut segments = Vec::new();
        // The middle segment is large enough to get postings.
        for s in 0..3u64 {
            let rows = gen_rows(&mut rng, if s == 1 { 10_500 } else { 2000 });
            let path = dir.path().join(segment_file_name(s));
            write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 512).unwrap();
            segments.push(Arc::new(Segment::open(&path).unwrap()));
            all.extend(rows);
        }
        let buffer: Vec<Arc<Record>> = gen_rows(&mut rng, 500).into_iter().map(Arc::new).collect();
        all.extend(buffer.iter().map(|r| Record::clone(r)));
        let by_id: HashMap<u64, Record> =
            all.iter().map(|r| (rid(r), normalize(r.clone()))).collect();

        for i in 0..300 {
            let filter = random_filter(&mut rng, 3);
            let time = if rng.chance(50) {
                TimeRange::ALL
            } else {
                let a = rng.below(20_000) as i64;
                TimeRange {
                    start: Some(a),
                    end: Some(a + rng.below(8000) as i64),
                }
            };
            let order = *rng.pick(&[Order::Asc, Order::Desc, Order::Storage]);
            let query = Query {
                time,
                filter: filter.clone(),
                order,
                limit: None,
            };
            let got: Vec<Record> = execute(&segments, &buffer, &query)
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let rf = RecordFilter::new(&filter);
            let mut want: Vec<u64> = all
                .iter()
                .filter(|r| time.contains(r.ts) && rf.matches(r))
                .map(rid)
                .collect();
            match order {
                Order::Asc => assert!(
                    got.windows(2).all(|w| w[0].ts <= w[1].ts),
                    "query {i} not ascending"
                ),
                Order::Desc => assert!(
                    got.windows(2).all(|w| w[0].ts >= w[1].ts),
                    "query {i} not descending"
                ),
                Order::Storage => {}
            }
            // Materialized records are complete.
            for r in got.iter().take(20) {
                assert_eq!(normalize(r.clone()), by_id[&rid(r)], "query {i}");
            }
            let mut ids: Vec<u64> = got.iter().map(rid).collect();
            ids.sort_unstable();
            want.sort_unstable();
            assert_eq!(ids, want, "query {i}: {filter:?} {time:?}");

            // Limit pushdown returns a prefix of the full ordered result.
            if order != Order::Storage {
                let limited: Vec<i64> = execute(
                    &segments,
                    &buffer,
                    &Query {
                        limit: Some(7),
                        ..query.clone()
                    },
                )
                .unwrap()
                .map(|r| r.unwrap().ts)
                .collect();
                assert_eq!(
                    limited,
                    got.iter().map(|r| r.ts).take(7).collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn postings_skip_chunks_zone_maps_cannot() {
        let dir = TempDir::new("query-postings");
        // "a" and "z" in every chunk defeat the zone maps; "m" only in chunk 2.
        let rows: Vec<Record> = (0..12_288i64)
            .map(|i| {
                let v = match (i / 4096, i % 7) {
                    (2, 3) => "m",
                    (_, 0) => "a",
                    _ => "z",
                };
                Record::new(i).with("v", Value::Str(v.into()))
            })
            .collect();
        let path = dir.path().join(segment_file_name(1));
        write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 4096).unwrap();
        let seg = Arc::new(Segment::open(&path).unwrap());
        let filter = Filter::field("v", FieldOp::Eq(Value::Str("M".into())));
        let scan = SegmentScan::new(Arc::clone(&seg), &filter, TimeRange::ALL, false)
            .unwrap()
            .unwrap();
        assert_eq!(scan.chunks, vec![2]);
        let expected = (8192..12_288).filter(|i| i % 7 == 3).count();
        assert_eq!(scan.count(), expected);
        // OR with an unindexed leaf keeps every chunk.
        let either = Filter::Or(vec![filter, Filter::field("w", FieldOp::Exists)]);
        let scan = SegmentScan::new(seg, &either, TimeRange::ALL, false)
            .unwrap()
            .unwrap();
        assert_eq!(scan.chunks.len(), 1, "absent field folds to false");
    }

    #[test]
    fn pruning_skips_segments_and_chunks() {
        let dir = TempDir::new("query-prune");
        let rows: Vec<Record> = (0..4096i64)
            .map(|i| {
                Record::new(i)
                    .with("n", Value::I64(i))
                    .with("h", Value::Str(format!("h{}", i / 1024)))
            })
            .collect();
        let path = dir.path().join(segment_file_name(1));
        write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 1024).unwrap();
        let seg = Arc::new(Segment::open(&path).unwrap());
        let scan =
            |f: Filter| SegmentScan::new(Arc::clone(&seg), &f, TimeRange::ALL, false).unwrap();
        assert!(scan(Filter::field("absent", FieldOp::Exists)).is_none());
        let s = scan(Filter::field("n", FieldOp::Eq(Value::I64(2000)))).unwrap();
        assert_eq!(s.chunks.len(), 1);
        let s = scan(Filter::field("h", FieldOp::Eq(Value::Str("H3".into())))).unwrap();
        assert_eq!(s.chunks, vec![3]);
        let s = SegmentScan::new(
            Arc::clone(&seg),
            &Filter::True,
            TimeRange {
                start: Some(5000),
                end: None,
            },
            false,
        )
        .unwrap();
        assert!(s.is_none());
    }
}
