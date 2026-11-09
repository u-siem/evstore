//! Aggregations: grouped counts, sums, minimums and maximums over one store.
//!
//! Design: `docs/design/09-design-details.md` (aggregation model).
//!
//! # MVP implementation
//!
//! An [`AggQuery`] filters like a [`Query`](crate::Query), groups matching
//! records by fields and/or a time bucket, and computes [`Metric`]s per
//! group. Results are exact and do not depend on segment layout.
//!
//! Semantics, per matching record:
//! - [`GroupBy::Field`]: the record joins a group only when the field has
//!   **exactly one** value; that value (exact type, case-sensitive strings,
//!   floats by bit pattern) is the key part. Records with the field missing
//!   or multi-valued are skipped.
//! - [`GroupBy::TimeBucket`]: `ts` floored to the span (`div_euclid`).
//! - [`Metric::Count`]: records. [`Metric::CountField`]: records where the
//!   field has at least one value.
//! - [`Metric::Sum`]: sum (as `f64`) and count of the field's numeric values
//!   (`I64`, `U64`, `F64`).
//! - [`Metric::Min`] / [`Metric::Max`]: over the field's values, compared
//!   like filters do; values of incomparable types order by type tag
//!   (`Str` < `I64` < `U64` < `F64` < `Bool` < `Bytes` < `Time` < `Ip`), so
//!   the result never depends on visiting order.
//!
//! Execution reuses the query pipeline (time, field-presence, zone-map and
//! postings pruning). Dictionary-coded group keys are accumulated as codes
//! and turned into values once per segment. A count with no filter and no
//! groups is answered from chunk metadata.

use std::collections::HashMap;
use std::sync::Arc;

use crate::codec::{Reader, get_tagged, put_tagged};
use crate::error::{Result, StoreError};
use crate::model::{Record, Value, ValueType, compare};
use crate::query::{
    ChunkData, Filter, Plan, RecordFilter, TimeRange, candidates, chunk_has_candidates,
    collect_fields, compile, eval, may_match,
};
use crate::registry::OVERFLOW_FIELD;
use crate::segment::{ColData, Segment};

/// How records are grouped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupBy {
    /// By the value of a field.
    Field(String),
    /// By `ts` floored to a span in nanoseconds (must be positive).
    TimeBucket {
        /// Bucket width in nanoseconds.
        span_ns: i64,
    },
}

/// What is computed per group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Metric {
    /// Number of records.
    Count,
    /// Number of records where the field has a value.
    CountField(String),
    /// Sum and count of the field's numeric values.
    Sum(String),
    /// Smallest value of the field.
    Min(String),
    /// Largest value of the field.
    Max(String),
}

/// A metric's result.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// For `Count` and `CountField`.
    Count(u64),
    /// For `Sum`: the sum and how many numeric values contributed.
    Sum {
        /// Sum of the values.
        sum: f64,
        /// Numeric values seen.
        values: u64,
    },
    /// For `Min` and `Max`; `None` when the field never had a value.
    Value(Option<Value>),
}

/// An aggregation query.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AggQuery {
    /// Time window.
    pub time: TimeRange,
    /// Which records take part.
    pub filter: Filter,
    /// Grouping (empty: one group with every matching record).
    pub group_by: Vec<GroupBy>,
    /// Metrics per group.
    pub metrics: Vec<Metric>,
}

/// One group of the result.
#[derive(Debug, Clone, PartialEq)]
pub struct AggRow {
    /// One value per [`GroupBy`] entry.
    pub key: Vec<Value>,
    /// One result per [`Metric`].
    pub metrics: Vec<MetricValue>,
}

#[derive(Debug, Clone)]
enum Acc {
    Count(u64),
    Sum(f64, u64),
    Min(Option<Value>),
    Max(Option<Value>),
}

impl Acc {
    fn new(m: &Metric) -> Acc {
        match m {
            Metric::Count | Metric::CountField(_) => Acc::Count(0),
            Metric::Sum(_) => Acc::Sum(0.0, 0),
            Metric::Min(_) => Acc::Min(None),
            Metric::Max(_) => Acc::Max(None),
        }
    }

    fn add_value(&mut self, v: &Value) {
        match self {
            Acc::Count(_) => {}
            Acc::Sum(sum, n) => {
                let x = match v {
                    Value::I64(x) => *x as f64,
                    Value::U64(x) => *x as f64,
                    Value::F64(x) => *x,
                    _ => return,
                };
                *sum += x;
                *n += 1;
            }
            Acc::Min(cur) => keep_extreme(cur, v, std::cmp::Ordering::Less),
            Acc::Max(cur) => keep_extreme(cur, v, std::cmp::Ordering::Greater),
        }
    }

    fn merge(&mut self, other: Acc) {
        match (self, other) {
            (Acc::Count(a), Acc::Count(b)) => *a += b,
            (Acc::Sum(a, n), Acc::Sum(b, m)) => {
                *a += b;
                *n += m;
            }
            (me @ (Acc::Min(_) | Acc::Max(_)), Acc::Min(Some(v)) | Acc::Max(Some(v))) => {
                me.add_value(&v);
            }
            _ => {}
        }
    }

    fn finish(self) -> MetricValue {
        match self {
            Acc::Count(n) => MetricValue::Count(n),
            Acc::Sum(sum, values) => MetricValue::Sum { sum, values },
            Acc::Min(v) | Acc::Max(v) => MetricValue::Value(v),
        }
    }
}

/// Replaces `cur` with `v` when `v` compares `want` against it.
fn keep_extreme(cur: &mut Option<Value>, v: &Value, want: std::cmp::Ordering) {
    match cur {
        None => *cur = Some(v.clone()),
        Some(c) => {
            let ord = compare(v, c)
                .unwrap_or_else(|| (v.value_type() as u8).cmp(&(c.value_type() as u8)));
            if ord == want {
                *cur = Some(v.clone());
            }
        }
    }
}

fn value_key(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    put_tagged(&mut out, v);
    out
}

/// Group key part inside one segment: dictionary codes stay codes, and
/// fixed-size values stay unencoded (no allocation per row).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Part {
    Code(u32, u32),
    Time(i64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Ip(u128),
    Bytes(Vec<u8>),
}

impl Part {
    /// Encoded key part, as [`value_key`] writes it.
    fn encode(self, seg: &Segment) -> Result<Vec<u8>> {
        Ok(match self {
            Part::Bytes(b) => b,
            Part::Time(t) => value_key(&Value::Time(t)),
            Part::I64(x) => value_key(&Value::I64(x)),
            Part::U64(x) => value_key(&Value::U64(x)),
            Part::Bool(x) => value_key(&Value::Bool(x)),
            Part::Ip(x) => value_key(&Value::Ip(x)),
            Part::Code(field, code) => {
                let dict = seg
                    .field_dict(field)?
                    .expect("coded field has a dictionary");
                value_key(&Value::Str(dict.get(code).to_owned()))
            }
        })
    }
}

type Groups = HashMap<Vec<Vec<u8>>, Vec<Acc>>;

fn merge_into(groups: &mut Groups, key: Vec<Vec<u8>>, accs: Vec<Acc>) {
    match groups.get_mut(&key) {
        Some(existing) => {
            for (a, b) in existing.iter_mut().zip(accs) {
                a.merge(b);
            }
        }
        None => {
            groups.insert(key, accs);
        }
    }
}

/// Runs `q` over the segments and the WAL buffer.
pub(crate) fn aggregate(
    segments: &[Arc<Segment>],
    buffer: &[Arc<Record>],
    q: &AggQuery,
) -> Result<Vec<AggRow>> {
    for g in &q.group_by {
        if let GroupBy::TimeBucket { span_ns } = g
            && *span_ns <= 0
        {
            return Err(StoreError::InvalidArgument(
                "time bucket span must be positive".into(),
            ));
        }
    }
    let mut groups: Groups = HashMap::new();
    for seg in segments {
        aggregate_segment(seg, q, &mut groups)?;
    }
    aggregate_records(buffer.iter().map(|r| &**r), q, &mut groups);
    if q.group_by.is_empty() && groups.is_empty() {
        groups.insert(Vec::new(), q.metrics.iter().map(Acc::new).collect());
    }
    let mut rows = Vec::with_capacity(groups.len());
    for (key, accs) in groups {
        let key = key
            .iter()
            .map(|k| get_tagged(&mut Reader::new(k)).expect("keys are encoded here"))
            .collect();
        rows.push(AggRow {
            key,
            metrics: accs.into_iter().map(Acc::finish).collect(),
        });
    }
    Ok(rows)
}

/// `ts` floored to a multiple of `span`; saturates at `i64::MIN` when the
/// floor is not representable (`div_euclid(span) * span` would overflow).
fn bucket(ts: i64, span: i64) -> i64 {
    ts.saturating_sub(ts.rem_euclid(span))
}

/// Reference path, also used for the in-memory buffer.
fn aggregate_records<'a>(
    records: impl IntoIterator<Item = &'a Record>,
    q: &AggQuery,
    groups: &mut Groups,
) {
    let filter = RecordFilter::new(&q.filter);
    'rows: for r in records {
        if !q.time.contains(r.ts) || !filter.matches(r) {
            continue;
        }
        let mut key = Vec::with_capacity(q.group_by.len());
        for g in &q.group_by {
            match g {
                GroupBy::TimeBucket { span_ns } => {
                    key.push(value_key(&Value::Time(bucket(r.ts, *span_ns))));
                }
                GroupBy::Field(f) => {
                    let mut vals = r.values(f);
                    match (vals.next(), vals.next()) {
                        (Some(v), None) => key.push(value_key(v)),
                        _ => continue 'rows,
                    }
                }
            }
        }
        let mut accs: Vec<Acc> = q.metrics.iter().map(Acc::new).collect();
        for (m, acc) in q.metrics.iter().zip(&mut accs) {
            match m {
                Metric::Count => *acc = Acc::Count(1),
                Metric::CountField(f) => *acc = Acc::Count(u64::from(r.values(f).next().is_some())),
                Metric::Sum(f) | Metric::Min(f) | Metric::Max(f) => {
                    r.values(f).for_each(|v| acc.add_value(v));
                }
            }
        }
        merge_into(groups, key, accs);
    }
}

fn metric_field(m: &Metric) -> Option<&str> {
    match m {
        Metric::Count => None,
        Metric::CountField(f) | Metric::Sum(f) | Metric::Min(f) | Metric::Max(f) => Some(f),
    }
}

fn aggregate_segment(seg: &Arc<Segment>, q: &AggQuery, groups: &mut Groups) -> Result<()> {
    if !q.time.overlaps(seg.ts_min, seg.ts_max) {
        return Ok(());
    }
    let plan = compile(&q.filter, seg)?;
    if matches!(plan, Plan::Const(false)) {
        return Ok(());
    }
    let rows = candidates(&plan, seg)?;
    let chunks: Vec<usize> = (0..seg.chunks.len())
        .filter(|c| {
            let m = &seg.chunks[*c];
            q.time.overlaps(m.ts_min, m.ts_max)
                && may_match(&plan, seg, *c)
                && rows
                    .as_ref()
                    .is_none_or(|r| chunk_has_candidates(r, seg, *c))
        })
        .collect();

    // Counting everything needs no column at all.
    let count_only = matches!(plan, Plan::Const(true))
        && q.group_by.is_empty()
        && q.metrics.iter().all(|m| *m == Metric::Count);

    let overflow = seg.field_indexes(OVERFLOW_FIELD).first().copied();
    let group_fields: Vec<Option<Vec<u32>>> = q
        .group_by
        .iter()
        .map(|g| match g {
            GroupBy::Field(f) => Some(seg.field_indexes(f)),
            GroupBy::TimeBucket { .. } => None,
        })
        .collect();
    let metric_fields: Vec<Vec<u32>> = q
        .metrics
        .iter()
        .map(|m| {
            metric_field(m)
                .map(|f| seg.field_indexes(f))
                .unwrap_or_default()
        })
        .collect();
    // A group field the segment lacks (and no overflow): nothing groups.
    if overflow.is_none() && group_fields.iter().flatten().any(std::vec::Vec::is_empty) {
        return Ok(());
    }
    let mut wanted = Vec::new();
    collect_fields(&plan, &mut wanted);
    group_fields.iter().flatten().for_each(|f| wanted.extend(f));
    for f in &metric_fields {
        wanted.extend(f);
    }
    wanted.extend(overflow);
    wanted.sort_unstable();
    wanted.dedup();

    let mut local: HashMap<Vec<Part>, Vec<Acc>> = HashMap::new();
    for c in chunks {
        let meta = &seg.chunks[c];
        if count_only && q.time.contains(meta.ts_min) && q.time.contains(meta.ts_max) {
            let accs = q
                .metrics
                .iter()
                .map(|_| Acc::Count(u64::from(meta.rows)))
                .collect();
            merge_local(&mut local, Vec::new(), accs);
            continue;
        }
        let ts = seg.timestamps(c)?;
        let mut data = ChunkData::new();
        data.load(seg, c, Some(&wanted))?;
        let mut key = Vec::with_capacity(q.group_by.len());
        'rows: for (row, &t) in ts.iter().enumerate() {
            if !q.time.contains(t) || !eval(&plan, &data, row) {
                continue;
            }
            key.clear();
            for (g, fields) in q.group_by.iter().zip(&group_fields) {
                match (g, fields) {
                    (GroupBy::TimeBucket { span_ns }, _) => {
                        key.push(Part::Time(bucket(t, *span_ns)));
                    }
                    (GroupBy::Field(name), Some(fields)) => {
                        match single_part(seg, &data, fields, name, row)? {
                            Some(p) => key.push(p),
                            None => continue 'rows,
                        }
                    }
                    (GroupBy::Field(_), None) => unreachable!("field groups have fields"),
                }
            }
            // Update the group's accumulators in place: one lookup and no
            // allocation per row once the group exists.
            let update = |accs: &mut [Acc]| -> Result<()> {
                for ((m, acc), fields) in q.metrics.iter().zip(accs).zip(&metric_fields) {
                    match m {
                        Metric::Count => {
                            if let Acc::Count(n) = acc {
                                *n += 1;
                            }
                        }
                        Metric::CountField(f) => {
                            let mut any = false;
                            for_each_value(seg, &data, fields, f, row, &mut |_| any = true)?;
                            if let Acc::Count(n) = acc {
                                *n += u64::from(any);
                            }
                        }
                        Metric::Sum(f) | Metric::Min(f) | Metric::Max(f) => {
                            for_each_value(seg, &data, fields, f, row, &mut |v| {
                                acc.add_value(v);
                            })?;
                        }
                    }
                }
                Ok(())
            };
            if let Some(accs) = local.get_mut(&key) {
                update(accs)?;
            } else {
                let mut accs: Vec<Acc> = q.metrics.iter().map(Acc::new).collect();
                update(&mut accs)?;
                local.insert(key.clone(), accs);
            }
        }
    }

    for (key, accs) in local {
        let mut out = Vec::with_capacity(key.len());
        for part in key {
            out.push(part.encode(seg)?);
        }
        merge_into(groups, out, accs);
    }
    Ok(())
}

fn merge_local(local: &mut HashMap<Vec<Part>, Vec<Acc>>, key: Vec<Part>, accs: Vec<Acc>) {
    match local.get_mut(&key) {
        Some(existing) => {
            for (a, b) in existing.iter_mut().zip(accs) {
                a.merge(b);
            }
        }
        None => {
            local.insert(key, accs);
        }
    }
}

/// The key part of a field with exactly one value in `row`, or `None`.
fn single_part(
    seg: &Segment,
    data: &ChunkData,
    fields: &[u32],
    name: &str,
    row: usize,
) -> Result<Option<Part>> {
    let mut found: Option<Part> = None;
    let mut n = 0usize;
    for f in fields {
        let Some(col) = data.columns.get(f) else {
            continue;
        };
        let range = col.range(row);
        n += range.len();
        if n > 1 {
            return Ok(None);
        }
        if let Some(i) = range.clone().next() {
            found = Some(match &col.data {
                ColData::Str(codes) => Part::Code(*f, codes[i]),
                ColData::Int(v) if col.ty == ValueType::Time => Part::Time(v[i]),
                ColData::Int(v) => Part::I64(v[i]),
                ColData::U64(v) => Part::U64(v[i]),
                ColData::Bool(v) => Part::Bool(v[i]),
                ColData::Ip(v) => Part::Ip(v[i]),
                _ => Part::Bytes(value_key(&col.value(i, seg.field_dict(*f)?))),
            });
        }
    }
    if let Some(over) = &data.overflow {
        for (n2, v) in &over[row] {
            if n2 == name {
                n += 1;
                if n > 1 {
                    return Ok(None);
                }
                found = Some(Part::Bytes(value_key(v)));
            }
        }
    }
    Ok(if n == 1 { found } else { None })
}

/// Calls `f` with every value of field `name` in `row`.
fn for_each_value(
    seg: &Segment,
    data: &ChunkData,
    fields: &[u32],
    name: &str,
    row: usize,
    f: &mut dyn FnMut(&Value),
) -> Result<()> {
    for field in fields {
        let Some(col) = data.columns.get(field) else {
            continue;
        };
        let dict = seg.field_dict(*field)?;
        for i in col.range(row) {
            f(&col.value(i, dict));
        }
    }
    if let Some(over) = &data.overflow {
        for (n, v) in &over[row] {
            if n == name {
                f(v);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::FieldOp;
    use crate::query::tests::{random_filter, random_record};
    use crate::segment::{segment_file_name, write_segment};
    use crate::test_util::{Rng, TempDir};

    fn sorted(mut rows: Vec<AggRow>) -> Vec<AggRow> {
        rows.sort_by_key(|r| format!("{:?}", r.key));
        rows
    }

    fn approx_eq(a: &[AggRow], b: &[AggRow]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| {
                x.key == y.key
                    && x.metrics.iter().zip(&y.metrics).all(|(m, n)| match (m, n) {
                        (
                            MetricValue::Sum {
                                sum: s1,
                                values: v1,
                            },
                            MetricValue::Sum {
                                sum: s2,
                                values: v2,
                            },
                        ) => v1 == v2 && (s1 - s2).abs() <= 1e-6 * s1.abs().max(1.0),
                        _ => m == n,
                    })
            })
    }

    #[test]
    fn oracle_aggregations_match_naive_fold() {
        let dir = TempDir::new("agg-oracle");
        let mut rng = Rng::new(0xa66);
        let mut all = Vec::new();
        let mut segments = Vec::new();
        for s in 0..3u64 {
            let n = if s == 1 { 10_500 } else { 1500 };
            let rows: Vec<Record> = (0..n)
                .map(|_| {
                    let ts = rng.below(20_000) as i64;
                    random_record(&mut rng, ts)
                })
                .collect();
            let path = dir.path().join(segment_file_name(s));
            write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 512).unwrap();
            segments.push(Arc::new(Segment::open(&path).unwrap()));
            all.extend(rows);
        }
        let buffer: Vec<Arc<Record>> = (0..300)
            .map(|_| {
                let ts = rng.below(20_000) as i64;
                Arc::new(random_record(&mut rng, ts))
            })
            .collect();
        all.extend(buffer.iter().map(|r| Record::clone(r)));

        let groups = [
            GroupBy::Field("host".into()),
            GroupBy::Field("user".into()),
            GroupBy::Field("port".into()),
            GroupBy::Field("ok".into()),
            GroupBy::Field("src".into()),
            GroupBy::Field("message".into()),
            GroupBy::TimeBucket { span_ns: 1000 },
        ];
        let fields = ["port", "bytes", "score", "seen", "host", "src", "missing"];
        for i in 0..150 {
            let n_groups = rng.below(3) as usize;
            let group_by: Vec<GroupBy> = (0..n_groups).map(|_| rng.pick(&groups).clone()).collect();
            let metrics: Vec<Metric> = (0..=rng.below(4))
                .map(|_| {
                    let f = (*rng.pick(&fields)).to_string();
                    match rng.below(5) {
                        0 => Metric::Count,
                        1 => Metric::CountField(f),
                        2 => Metric::Sum(f),
                        3 => Metric::Min(f),
                        _ => Metric::Max(f),
                    }
                })
                .collect();
            let filter = if rng.chance(40) {
                Filter::True
            } else {
                random_filter(&mut rng, 2)
            };
            let time = if rng.chance(50) {
                TimeRange::ALL
            } else {
                let a = rng.below(20_000) as i64;
                TimeRange {
                    start: Some(a),
                    end: Some(a + rng.below(8000) as i64),
                }
            };
            let q = AggQuery {
                time,
                filter,
                group_by,
                metrics,
            };
            let got = sorted(aggregate(&segments, &buffer, &q).unwrap());
            let mut naive: Groups = HashMap::new();
            aggregate_records(&all, &q, &mut naive);
            if q.group_by.is_empty() && naive.is_empty() {
                naive.insert(Vec::new(), q.metrics.iter().map(Acc::new).collect());
            }
            let want = sorted(
                naive
                    .into_iter()
                    .map(|(k, accs)| AggRow {
                        key: k
                            .iter()
                            .map(|b| get_tagged(&mut Reader::new(b)).unwrap())
                            .collect(),
                        metrics: accs.into_iter().map(Acc::finish).collect(),
                    })
                    .collect(),
            );
            assert!(
                approx_eq(&got, &want),
                "query {i}: {q:?}\n got {got:?}\nwant {want:?}"
            );
        }
    }

    #[test]
    fn bucket_floor_saturates() {
        // `div_euclid(3) * 3` overflowed here.
        assert_eq!(bucket(i64::MIN, 3), i64::MIN);
        assert_eq!(bucket(i64::MIN + 1, 3), i64::MIN);
        assert_eq!(bucket(-1, 10), -10);
        assert_eq!(bucket(25, 10), 20);
        assert_eq!(bucket(i64::MAX, 7), i64::MAX - i64::MAX.rem_euclid(7));
    }

    #[test]
    fn counts_and_buckets() {
        let dir = TempDir::new("agg-basic");
        let rows: Vec<Record> = (0..5000i64)
            .map(|i| {
                Record::new(i)
                    .with("h", Value::Str(format!("h{}", i % 3)))
                    .with("n", Value::I64(i))
            })
            .collect();
        let path = dir.path().join(segment_file_name(1));
        write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 1024).unwrap();
        let segs = vec![Arc::new(Segment::open(&path).unwrap())];
        let q = |group_by, metrics, filter| AggQuery {
            time: TimeRange::ALL,
            filter,
            group_by,
            metrics,
        };
        let all = aggregate(&segs, &[], &q(vec![], vec![Metric::Count], Filter::True)).unwrap();
        assert_eq!(all[0].metrics, vec![MetricValue::Count(5000)]);
        let by_host = sorted(
            aggregate(
                &segs,
                &[],
                &q(
                    vec![GroupBy::Field("h".into())],
                    vec![Metric::Count, Metric::Max("n".into())],
                    Filter::True,
                ),
            )
            .unwrap(),
        );
        assert_eq!(by_host.len(), 3);
        assert_eq!(by_host[0].key, vec![Value::Str("h0".into())]);
        assert_eq!(
            by_host[0].metrics,
            vec![
                MetricValue::Count(1667),
                MetricValue::Value(Some(Value::I64(4998)))
            ]
        );
        let buckets = aggregate(
            &segs,
            &[],
            &q(
                vec![GroupBy::TimeBucket { span_ns: 1000 }],
                vec![Metric::Sum("n".into())],
                Filter::field("h", FieldOp::Eq(Value::Str("H1".into()))),
            ),
        )
        .unwrap();
        assert_eq!(buckets.len(), 5);
        // No match and no groups: one empty row.
        let none = aggregate(
            &segs,
            &[],
            &q(
                vec![],
                vec![Metric::Count, Metric::Min("n".into())],
                Filter::field("h", FieldOp::Eq(Value::Str("zz".into()))),
            ),
        )
        .unwrap();
        assert_eq!(
            none,
            vec![AggRow {
                key: vec![],
                metrics: vec![MetricValue::Count(0), MetricValue::Value(None)]
            }]
        );
        assert!(
            aggregate(
                &segs,
                &[],
                &q(
                    vec![GroupBy::TimeBucket { span_ns: 0 }],
                    vec![],
                    Filter::True
                )
            )
            .is_err()
        );
    }
}
