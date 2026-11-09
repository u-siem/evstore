//! Query benchmarks (std only, `harness = false`).
//!
//! `cargo bench --bench query` builds a store of synthetic security events
//! (several segments with postings plus an unflushed buffer) and prints the
//! median time of each query shape as a Markdown table.
//!
//! Environment: `EVSTORE_BENCH_SEGMENTS` (default 20), `EVSTORE_BENCH_ROWS`
//! rows per segment (default 20,000), `EVSTORE_BENCH_RUNS` (default 7).

#![allow(
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::print_stdout
)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use usiem_evstore::{
    ANY_FIELD, AggQuery, FieldOp, Filter, GroupBy, Metric, Options, Order, Query, Record, Store,
    SyncPolicy, TimeRange, Value,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

const EVENT_IDS: [i64; 8] = [4624, 4625, 4634, 4648, 4672, 4688, 4720, 5156];

fn record(rng: &mut Rng, ts: i64) -> Record {
    let host = format!("WEB{:02}", rng.below(50));
    let user = format!("user{}", rng.below(200));
    let user = if rng.below(50) == 0 {
        "Admin".to_string()
    } else {
        user
    };
    let ip = 0xffff_0a00_0000u128 | u128::from(rng.below(1 << 16) as u16);
    let port = rng.below(65_536) as i64;
    let event_id = EVENT_IDS[rng.below(EVENT_IDS.len() as u64) as usize];
    let message = format!(
        "Failed password for {user} from 10.0.{}.{} port {port} ssh2 session={:016X}",
        (ip >> 8) & 0xff,
        ip & 0xff,
        rng.next()
    );
    let mut r = Record::new(ts)
        .with("host", Value::Str(host))
        .with("user", Value::Str(user))
        .with("src", Value::Ip(ip))
        .with("port", Value::I64(port))
        .with("event_id", Value::I64(event_id))
        .with("bytes", Value::U64(rng.below(1 << 20)))
        .with("message", Value::Str(message));
    if rng.below(10) == 0 {
        r.push("blob", Value::Bytes(vec![0u8; 32]));
    }
    r
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

/// (first run, median of the next `runs`, result). The first run plans
/// segments cold (dictionaries and postings not yet decoded).
fn time<T>(runs: usize, mut f: impl FnMut() -> T) -> (Duration, Duration, T) {
    let t = Instant::now();
    let mut out = f();
    let first = t.elapsed();
    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t = Instant::now();
        out = black_box(f());
        samples.push(t.elapsed());
    }
    (first, median(samples), out)
}

fn str_eq(field: &str, v: &str) -> Filter {
    Filter::field(field, FieldOp::Eq(Value::Str(v.into())))
}

fn main() {
    let segments = env("EVSTORE_BENCH_SEGMENTS", 20);
    let rows = env("EVSTORE_BENCH_ROWS", 20_000);
    let runs = env("EVSTORE_BENCH_RUNS", 7);

    let dir = std::env::temp_dir().join(format!("evstore-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let opts = Options {
        sync: SyncPolicy::Interval(Duration::from_secs(3600)),
        flush_bytes: usize::MAX,
        ..Options::default()
    };
    let mut store = Store::open(&dir, opts.clone()).expect("open store");
    let mut rng = Rng(0x00be_ac4e);
    let mut ts = 1_700_000_000_000_000_000i64;
    let build = Instant::now();
    for s in 0..=segments {
        let batch: Vec<Record> = (0..rows)
            .map(|_| {
                ts += 1_000_000 + rng.below(1_000_000) as i64;
                record(&mut rng, ts)
            })
            .collect();
        store.append(batch).expect("append");
        // The last batch stays in the buffer.
        if s < segments {
            store.flush().expect("flush");
        }
    }
    let total = store.len();
    eprintln!(
        "built {segments} segments x {rows} rows + {rows} buffered ({total} records) in {:.1?}",
        build.elapsed()
    );

    let search = |filter: Filter, order: Order, limit: Option<usize>| Query {
        time: TimeRange::ALL,
        filter,
        order,
        limit,
    };
    let searches: Vec<(&str, Query)> = vec![
        (
            "eq dict (host), asc, all",
            search(str_eq("host", "web07"), Order::Asc, None),
        ),
        (
            "eq dict rare (user=admin), asc, all",
            search(str_eq("user", "ADMIN"), Order::Asc, None),
        ),
        (
            "eq dict, desc, limit 100",
            search(str_eq("host", "web07"), Order::Desc, Some(100)),
        ),
        (
            "true, desc, limit 100",
            search(Filter::True, Order::Desc, Some(100)),
        ),
        (
            "contains plain (message)",
            search(
                Filter::field("message", FieldOp::Contains("password for admin".into())),
                Order::Storage,
                None,
            ),
        ),
        (
            "glob plain *for admin*",
            search(
                Filter::field("message", FieldOp::Glob("*for admin*".into())),
                Order::Storage,
                None,
            ),
        ),
        (
            "@any contains admin",
            search(
                Filter::field(ANY_FIELD, FieldOp::Contains("admin".into())),
                Order::Storage,
                None,
            ),
        ),
        (
            "range int (port 1000..1100)",
            search(
                Filter::field(
                    "port",
                    FieldOp::Range {
                        lower: Some((Value::I64(1000), true)),
                        upper: Some((Value::I64(1100), false)),
                    },
                ),
                Order::Storage,
                None,
            ),
        ),
        (
            "or(eq dict, bytes exists)",
            search(
                Filter::Or(vec![
                    str_eq("user", "admin"),
                    Filter::field("blob", FieldOp::Exists),
                ]),
                Order::Storage,
                None,
            ),
        ),
        (
            "true, storage, all (materialize everything)",
            search(Filter::True, Order::Storage, None),
        ),
        (
            "cidr /24 (src)",
            search(
                Filter::field(
                    "src",
                    FieldOp::Cidr {
                        lo: 0xffff_0a00_0700,
                        hi: 0xffff_0a00_07ff,
                    },
                ),
                Order::Storage,
                None,
            ),
        ),
    ];

    let agg = |filter: Filter, group_by: Vec<GroupBy>, metrics: Vec<Metric>| AggQuery {
        time: TimeRange::ALL,
        filter,
        group_by,
        metrics,
    };
    let aggs: Vec<(&str, AggQuery)> = vec![
        (
            "count, no filter",
            agg(Filter::True, vec![], vec![Metric::Count]),
        ),
        (
            "count where host=web07",
            agg(str_eq("host", "web07"), vec![], vec![Metric::Count]),
        ),
        (
            "count by event_id (int)",
            agg(
                Filter::True,
                vec![GroupBy::Field("event_id".into())],
                vec![Metric::Count],
            ),
        ),
        (
            "count by host (dict)",
            agg(
                Filter::True,
                vec![GroupBy::Field("host".into())],
                vec![Metric::Count],
            ),
        ),
        (
            "sum bytes by src (ip)",
            agg(
                Filter::True,
                vec![GroupBy::Field("src".into())],
                vec![Metric::Sum("bytes".into())],
            ),
        ),
    ];

    // Each query starts on a freshly opened store, so "first" includes
    // decoding the dictionaries and postings it needs.
    println!("| query | first | median | rows |");
    println!("|---|---:|---:|---:|");
    for (name, q) in &searches {
        drop(store);
        store = Store::open(&dir, opts.clone()).expect("reopen store");
        let (first, d, n) = time(runs, || {
            let mut n = 0usize;
            for r in store.search(q).expect("search") {
                black_box(r.expect("record"));
                n += 1;
            }
            n
        });
        println!("| {name} | {first:.2?} | {d:.2?} | {n} |");
    }
    for (name, q) in &aggs {
        drop(store);
        store = Store::open(&dir, opts.clone()).expect("reopen store");
        let (first, d, n) = time(runs, || store.aggregate(q).expect("aggregate").len());
        println!("| agg: {name} | {first:.2?} | {d:.2?} | {n} groups |");
    }

    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}
