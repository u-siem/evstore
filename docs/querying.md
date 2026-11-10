# Querying a store

This guide covers how to read data from a `Store` through the Rust API:
searches, filters, aggregations, matching rules, and which queries the
indexes speed up. It describes the current MVP implementation. The target
design is in [design/08-components.md](design/08-components.md) and
[design/09-design-details.md](design/09-design-details.md).

```rust
use usiem_evstore::{
    AggQuery, FieldOp, Filter, GroupBy, Metric, Options, Order, Query, Store, TimeRange, Value,
};
```

## Contents

1. [Opening a store](#1-opening-a-store)
2. [Searching: `Query`](#2-searching-query)
3. [Filters: `Filter` and `FieldOp`](#3-filters-filter-and-fieldop)
4. [Values and matching rules](#4-values-and-matching-rules)
5. [Consuming results](#5-consuming-results)
6. [Aggregations: `AggQuery`](#6-aggregations-aggquery)
7. [Inspecting a store](#7-inspecting-a-store)
8. [Performance: what is pruned, what is scanned](#8-performance-what-is-pruned-what-is-scanned)
9. [Stores written through usiem-query](#9-stores-written-through-usiem-query)
10. [A complete example](#10-a-complete-example)

## 1. Opening a store

```rust
let store = Store::open("data/events", Options::default())?;
```

- **Exclusive lock.** `Store::open` takes an exclusive OS lock on
  `<dir>/LOCK`. While another process has the store open, `open` fails
  with `StoreError::Locked`. There is **no read-only mode**, so to query a
  store that a running application owns, go through that application.
- **`open` writes.** It creates the directory if it is missing, runs crash
  recovery (reported by `store.recovery()`), and deletes orphan segment and
  WAL files. Double-check the path before opening: a typo creates an empty
  store.
- **`Options`** mostly tune ingest. The only one that affects queries is
  `chunk_rows` (default 4096), which sets how fine-grained chunk pruning is.
- **Concurrency in one process.** `search` and `aggregate` take `&self`;
  `append`, `flush` and `maintain` take `&mut self`. Wrap the store in a
  `RwLock` to run searches in parallel with ingest.
- **Snapshots.** Appended records are searchable immediately, before any
  flush. A search iterator holds a snapshot: it keeps working after later
  appends, flushes, compactions and retention. Files it still uses are
  deleted only once it is dropped.

## 2. Searching: `Query`

```rust
let q = Query {
    time: TimeRange { start: Some(from_ns), end: Some(to_ns) },
    filter: Filter::field("host", FieldOp::Eq(Value::Str("web07".into()))),
    order: Order::Desc,
    limit: Some(100),
    fields: None,
};
for record in store.search(&q)? {
    let record = record?;
    println!("{} {:?}", record.ts, record.get("host"));
}
```

| Field | Default | Meaning |
|---|---|---|
| `time` | `TimeRange::ALL` | half-open `[start, end)` in nanoseconds on `Record::ts`; `None` is unbounded |
| `filter` | `Filter::True` | which records match |
| `order` | `Order::Asc` | result order |
| `limit` | `None` | maximum number of records |
| `fields` | `None` | names of the fields to return; `None` returns every field (see [projection](#5-consuming-results)) |

Every field of `Query` is public, so a struct literal must name them all.
Write `..Query::default()` to stay compatible with fields added later.

**Orders:**

- `Order::Asc`: oldest first. Records with equal timestamps keep their
  ingest order.
- `Order::Desc`: newest first.
- `Order::Storage`: the cheapest order. Each segment comes out in time
  order, segment by segment, then the in-memory buffer in ingest order.
  The overall result is **not** sorted. Use it for exports and full scans.

**Limit.** Results are merged lazily by time, and a segment is opened only
when the merge reaches its time range. Planning (reading the dictionaries
and postings a filter needs) also waits until then. So `Order::Desc` with
a small `limit` reads only the newest segments, which makes "latest N
events" cheap even on a cold store.

## 3. Filters: `Filter` and `FieldOp`

```rust
pub enum Filter {
    True,                                   // matches everything (default)
    And(Vec<Filter>),                       // And(vec![]) is true
    Or(Vec<Filter>),                        // Or(vec![]) is false
    Not(Box<Filter>),
    Field { field: String, op: FieldOp },
}
```

`Filter::field(name, op)` is the only constructor helper. Build `And`, `Or`
and `Not` with the variants directly.

| `FieldOp` | Matches when a value of the field … |
|---|---|
| `Exists` | exists, of any type |
| `Eq(v)` | equals `v` |
| `In(vec![a, b])` | equals any of them |
| `Prefix(s)` | is a string starting with `s` |
| `Contains(s)` | is a string containing `s` |
| `Glob(p)` | is a string matching `p` as a whole: `*` is any run, `?` one character, no escaping |
| `Range { lower, upper }` | lies within the bounds; each is `Option<(Value, inclusive)>` |
| `Cidr { lo, hi }` | is an IP in `lo..=hi` (both inclusive, as `u128`) |
| `HasType(t)` | has a value of type `t` |

```rust
// host = web07 AND port in [1000, 1100) AND NOT user = root
let filter = Filter::And(vec![
    Filter::field("host", FieldOp::Eq(Value::Str("web07".into()))),
    Filter::field("port", FieldOp::Range {
        lower: Some((Value::I64(1000), true)),
        upper: Some((Value::I64(1100), false)),
    }),
    Filter::Not(Box::new(Filter::field("user", FieldOp::Eq(Value::Str("root".into()))))),
]);
```

**Pseudo-fields:**

- `ANY_FIELD` (`"@any"`) tests every **string** value of the record:
  `Filter::field(ANY_FIELD, FieldOp::Contains("admin".into()))` is a
  free-text search. It never matches numbers, IPs or times.
- `OVERFLOW_FIELD` (`"@overflow"`) is reserved. `append` rejects it, and a
  filter on it matches nothing.

**Evaluating outside the store.** To test records in memory,
`Filter::matches(&record)` gives the reference semantics, and
`RecordFilter::new(&filter)` compiles the filter once for checking many
records.

## 4. Values and matching rules

`Value` has eight variants: `Str`, `I64`, `U64`, `F64`, `Bool`, `Bytes`,
`Time` (nanoseconds) and `Ip` (`u128`). A `Record` is a timestamp `ts`
plus a list of `(name, Value)` pairs. A name may repeat, which makes the
field **multi-valued**.

### General rules

- **Any value.** A leaf matches when **any** value of the field matches.
- **Missing fields.** A missing field never matches, so
  `Not(field op)` matches records that lack the field. To require the field,
  add `Filter::field(name, FieldOp::Exists)` with `And`.
- **Type mismatches.** A literal of the wrong type never matches.
  `Str("443")` does not match `I64(443)`, and `I64` does not match a `Time`
  field.

### Strings

- Strings always compare **case-insensitively**, with full Unicode
  lowercasing. This covers `Eq`, `In`, `Prefix`, `Contains`, `Glob` and
  string `Range`.
- A string `Range` compares the lowercased forms byte by byte.
- `Prefix`, `Contains` and `Glob` only ever match string values.
- `Bytes` compare exactly.

### Numbers

`I64`, `U64` and `F64` compare **numerically across types**. A `port`
stored as `I64(443)` matches `Eq(U64(443))` and `Eq(F64(443.0))`.
`-0.0 == 0.0`, and NaN matches nothing.

### Times

A field holding `Value::Time` only matches `Value::Time` literals.
`Record::ts` is not a field: filter it with `Query::time`.

### IP addresses

- **IPv4 is stored IPv4-mapped** (`::ffff:a.b.c.d`). Build literals with
  `Value::ipv4(addr)` or `Value::ip(addr)`, never by hand from a `u32`.
- `Cidr` takes the first and last address as `u128`, also IPv4-mapped.
  For `10.0.7.0/24`:

  ```rust
  use std::net::Ipv4Addr;
  let net = u32::from(Ipv4Addr::new(10, 0, 7, 0));
  let mask = !0u32 << (32 - 24);
  let lo = u128::from(Ipv4Addr::from(net & mask).to_ipv6_mapped());
  let hi = u128::from(Ipv4Addr::from(net | !mask).to_ipv6_mapped());
  // lo = 0xffff_0a00_0700, hi = 0xffff_0a00_07ff
  let filter = Filter::field("src", FieldOp::Cidr { lo, hi });
  ```

  For IPv6, use `u128::from(Ipv6Addr)` with a 128-bit mask.
- `Eq` and `Range` on IPs compare the `u128` values.

### Fields stored with more than one type

If a field name has been written with several types (for example `port` as
`I64` in some records and as `Str` in others), the store keeps one column
per type. A filter on `port` searches all of them, and records come back
with the original name.

## 5. Consuming results

`search` returns a `RecordIter`, a
`Box<dyn Iterator<Item = Result<Record>> + Send>`. It owns its data, so it
can move to another thread and outlive later writes.

- **Errors.** `search` currently always returns `Ok`: segments are planned
  when the iteration reaches them, so every failure, including reading a
  dictionary or postings, arrives as an `Err` item. With `Order::Asc` and
  `Order::Desc` the iterator ends after it. With `Order::Storage` only the
  failing segment ends and the next one follows, so stop at the first
  `Err` if you need all or nothing.
- **Projection.** By default every matching record comes back with all its
  fields. `fields: Some(names)` returns only the fields with those exact
  names (overflowed fields included), and the store decodes no other
  column except the ones the filter itself reads. Records keep their `ts`
  and come back even when none of the named fields is present. For wide
  records this is several times faster: reading 420,000 benchmark records
  with one field takes about a fifth of the time of reading them whole.
- **Stopping early.** There is no pagination cursor and no cancellation:
  stop iterating, or set a `limit`.
- **Counting.** Use an aggregation with `Metric::Count` rather than
  `search(..)?.count()`. With no filter, or with a filter that postings
  decide exactly (see [section 8](#8-performance-what-is-pruned-what-is-scanned)),
  and no `group_by`, whole chunks are counted from metadata and bitmaps
  without decoding a record.

Records come back with their original field names and `ts`.
`record.values(name)` returns every value of a field, and `record.get(name)`
the first one.

## 6. Aggregations: `AggQuery`

```rust
let q = AggQuery {
    time: TimeRange::ALL,
    filter: Filter::field("action", FieldOp::Eq(Value::Str("deny".into()))),
    group_by: vec![
        GroupBy::Field("src".into()),
        GroupBy::TimeBucket { span_ns: 3_600_000_000_000 }, // 1 h
    ],
    metrics: vec![Metric::Count, Metric::Sum("bytes".into()), Metric::Max("port".into())],
};
for row in store.aggregate(&q)? {
    // row.key: one Value per GroupBy; row.metrics: one MetricValue per Metric
}
```

**Grouping (`GroupBy`):**

- `Field(name)`: a record joins a group only when the field has **exactly
  one** value. Records where it is missing or multi-valued are skipped.
- `TimeBucket { span_ns }`: `ts` floored to the span. The key is a
  `Value::Time`. A span of 0 or less is `StoreError::InvalidArgument`.
- **Keys are exact values.** Unlike filters, string keys are
  **case-sensitive** (`"Admin"` and `"admin"` are two groups), floats group
  by bit pattern, and each type is distinct.
- With an empty `group_by`, all matching records form one group.

**Metrics:**

| `Metric` | `MetricValue` |
|---|---|
| `Count` | `Count(n)`: records in the group |
| `CountField(f)` | `Count(n)`: records where `f` has a value |
| `Sum(f)` | `Sum { sum: f64, values: u64 }` over the `I64`/`U64`/`F64` values; average = `sum / values` |
| `Min(f)`, `Max(f)` | `Value(Option<Value>)`, compared like filters; values of different types order by type: `Str < I64 < U64 < F64 < Bool < Bytes < Time < Ip` |

**Results:**

- Results are exact and do not depend on segment layout.
- **Row order is unspecified.** Sort the `Vec<AggRow>` yourself.
- With no `group_by` and no matches, you get exactly one row with zeroed
  metrics. With groups and no matches, you get an empty `Vec`.

**Not available (yet):**

- distinct count
- average as its own metric
- percentiles
- top-N
- a group-cardinality cap or memory budget: every group is held in memory

## 7. Inspecting a store

| Method | Returns |
|---|---|
| `store.len()`, `is_empty()` | records, including the unflushed buffer |
| `store.stats()` | `Stats { segment_rows, buffered_rows, segments, segment_bytes, wal_bytes, time_range }` |
| `store.fields()` | the `FieldRegistry`: `iter()` yields `(FieldId, &FieldInfo { name, ty, mode, storage_name })` for every field and type seen; `ids(name)` lists one name's identities |
| `store.app_meta()` | opaque bytes the application stored with the store |

The registry has no per-field counts or value lists. After `max_fields`
(default 2000) field identities, new fields go to the scan-only overflow
column and are not listed.

## 8. Performance: what is pruned, what is scanned

Pruning only skips work, so results never depend on which indexes exist.
A surviving row is evaluated against the filter unless the postings
already decided it exactly.

**Pruning, from coarsest to finest:**

1. **Segment pruning.**
   - Segments outside `time` are skipped.
   - A leaf on a field the segment does not contain is `false` there (and
     `Not` of it is `true`), so such segments often fold away completely.
2. **Chunk pruning** (chunks of `chunk_rows` rows).
   - Chunks are pruned by their time range, then by zone maps.
   - Number, time, IP and bool columns keep min/max *values*, which help
     `Eq`, `In`, `Range` and `Cidr`.
   - Dictionary string columns keep min/max dictionary *codes*, which help
     every string op.
   - Nothing is pruned under `Not`.
3. **Postings.**
   - Segments of at least **10 000 rows** keep a Roaring bitmap per
     dictionary value of each dictionary-coded string field.
   - A string leaf then reads only the chunks that contain matching values,
     and only the matching rows of those chunks.
   - A filter built only from such leaves with `And` and `Or` is decided
     **exactly** by the postings. Its rows are not re-evaluated and its
     columns are not decoded, and a `Count` with no groups is read off the
     bitmaps.
   - An `And` that mixes indexed and other leaves still narrows to the
     indexed candidates, then evaluates those rows.
   - Postings are not used under `Not`, in an `Or` where some branch has no
     postings, or for a leaf that could also match in the overflow column.
4. **Dictionary evaluation.** String predicates on dictionary fields run
   once per *distinct value*, not once per row. A glob or `Contains` on a
   low-cardinality field such as a host name is therefore cheap.

**When a field is "plain".** A string field gets no dictionary, zone map or
postings when it has more than 16 distinct values and those make up more
than half of its values. Messages, command lines and raw lines usually end
up plain, and every value is then matched one by one.

**What forces a scan of every surviving chunk:**

- `Contains`, `Glob` or `Prefix` on a plain string field;
- `ANY_FIELD`;
- any filter under `Not`;
- an `Or` that mixes indexed and unindexed leaves;
- fields in the overflow column;
- numeric, IP or time ranges whose values are spread across every chunk
  (zone maps only help when data is clustered).

Numbers, times and IPs have zone maps only, no inverted index.

**Late materialization.** Only the columns the filter reads are decoded
first. The rest of the row is decoded only for matches, and with
`Query::fields` only the named columns are. The in-memory buffer is always
filtered record by record.

Measured numbers for each query shape are in
[spikes/results/](spikes/results/), and you can run them with `cargo bench`
(`benches/query.rs`).

## 9. Stores written through usiem-query

[usiem-query](https://github.com/u-siem/query)'s `EvstoreSource` stores
uSIEM events in an evstore `Store`. It encodes event structure into field
names:

- meta fields `@arrival`, `@origin`, `@raw` or `@raw_json`;
- nested objects flattened with `\x1f` separators;
- arrays as multi-valued fields with a `\x1e` suffix;
- user fields that start with `@` get an extra `@`.

`EvstoreSource::store()` gives access to the underlying `Store`. Raw
filters and `Query::fields` against such a store must use the encoded
names. The mapping is
documented in usiem-query's `store::evstore` module. For most purposes the
query language there is the better interface.

## 10. A complete example

```rust
use std::net::Ipv4Addr;
use usiem_evstore::{
    AggQuery, FieldOp, Filter, GroupBy, Metric, MetricValue, Options, Order, Query, Record,
    Store, TimeRange, Value, now_ns,
};

fn main() -> usiem_evstore::Result<()> {
    let mut store = Store::open("data/events", Options::default())?;

    let t0 = 1_700_000_000_000_000_000_i64;
    store.append(
        (0..1000)
            .map(|i| {
                Record::new(t0 + i * 1_000_000_000)
                    .with("host", Value::Str(format!("web{:02}", i % 4)))
                    .with("src", Value::ipv4(Ipv4Addr::new(10, 0, 7, (i % 250) as u8)))
                    .with("port", Value::I64(1000 + i % 200))
                    .with("bytes", Value::U64(100 + i as u64))
            })
            .collect(),
    )?;

    // The 10 newest records from web01 with a port of at least 1100.
    let q = Query {
        time: TimeRange::ALL,
        filter: Filter::And(vec![
            Filter::field("host", FieldOp::Eq(Value::Str("WEB01".into()))),
            Filter::field("port", FieldOp::Range { lower: Some((Value::I64(1100), true)), upper: None }),
        ]),
        order: Order::Desc,
        limit: Some(10),
        ..Query::default()
    };
    for record in store.search(&q)? {
        let record = record?;
        println!("{} {:?}", record.ts, record.get("port"));
    }

    // Bytes per host per minute.
    let agg = AggQuery {
        group_by: vec![
            GroupBy::Field("host".into()),
            GroupBy::TimeBucket { span_ns: 60_000_000_000 },
        ],
        metrics: vec![Metric::Count, Metric::Sum("bytes".into())],
        ..AggQuery::default()
    };
    let mut rows = store.aggregate(&agg)?;
    rows.sort_by(|a, b| format!("{:?}", a.key).cmp(&format!("{:?}", b.key)));
    for row in &rows {
        if let [MetricValue::Count(n), MetricValue::Sum { sum, .. }] = row.metrics.as_slice() {
            println!("{:?}: {n} records, {sum} bytes", row.key);
        }
    }

    store.maintain(now_ns())?; // in a real application, about once a second
    Ok(())
}
```
