//! R6 spike: what does ingest cost per event in the evstore MVP?
//!
//! Generates syslog-like records shaped like syslog events (host, program, pid,
//! message, raw line, addresses, arrival time, origin), appends them in
//! batches, and flushes a segment every `--flush-mib` of buffered rows. It
//! reports the append (WAL) cost and the flush (segment build) cost per
//! event separately, then compaction, size on disk and a few queries.
//!
//! ```text
//! cargo run --release -- [--events 1000000] [--batch 500] [--flush-mib 8] [--dir <path>]
//! ```

use std::time::{Duration, Instant};

use usiem_evstore::{
    AggQuery, FieldOp, Filter, GroupBy, Metric, Options, Order, Query, Record, RetentionPolicy,
    Store, SyncPolicy, TimeRange, Value,
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

const HOSTS: [&str; 12] = [
    "nas", "pi", "router", "fw", "web01", "web02", "db01", "proxmox", "ha", "printer", "nvr", "ap1",
];
const PROGRAMS: [&str; 10] = [
    "sshd", "cron", "kernel", "systemd", "dnsmasq", "nginx", "filterlog", "sudo", "smbd", "dhcpd",
];
const USERS: [&str; 6] = ["root", "admin", "pi", "backup", "www-data", "alice"];

fn record(rng: &mut Rng, i: u64, t0: i64) -> Record {
    let ts = t0 + i as i64 * 500_000; // 2,000 events/s
    let host = HOSTS[rng.below(HOSTS.len() as u64) as usize];
    let program = PROGRAMS[rng.below(PROGRAMS.len() as u64) as usize];
    let pid = 100 + rng.below(30_000);
    let user = USERS[rng.below(USERS.len() as u64) as usize];
    let src = 0xffff_c0a8_0000u128 | u128::from(rng.below(1 << 16) as u16);
    let port = rng.below(65_536);
    let message = match program {
        "sshd" => format!("Failed password for {user} from 192.168.{}.{} port {port} ssh2", rng.below(256), rng.below(256)),
        "cron" => format!("({user}) CMD (/usr/local/bin/job-{}.sh)", rng.below(40)),
        "filterlog" => format!("{},,,1000000103,igb0,match,block,in,4,0x0,,64,{},0,DF,6,tcp,60", rng.below(200), rng.below(65_535)),
        "sudo" => format!("{user} : TTY=pts/{} ; PWD=/home/{user} ; USER=root ; COMMAND=/usr/bin/apt update", rng.below(4)),
        _ => format!("{program}[{pid}]: event {} state={} took {}ms", rng.below(10_000), rng.below(5), rng.below(2_000)),
    };
    let raw = format!("<38>Nov  4 09:05:01 {host} {program}[{pid}]: {message}");
    Record::new(ts)
        .with("@arrival", Value::Time(ts + rng.below(50_000_000) as i64))
        .with("@origin", Value::Str(format!("{{\"type\":\"ip\",\"value\":\"192.168.1.{}\"}}", 10 + HOSTS.iter().position(|h| *h == host).unwrap())))
        .with("@raw", Value::Str(raw))
        .with("host.name", Value::Str(host.into()))
        .with("process.name", Value::Str(program.into()))
        .with("process.pid", Value::I64(pid as i64))
        .with("log.syslog.priority", Value::I64(38))
        .with("message", Value::Str(message))
        .with("source.ip", Value::Ip(src))
        .with("source.port", Value::I64(port as i64))
        .with("user.name", Value::Str(user.into()))
}

fn arg(args: &[String], name: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

fn dir_size(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn time_query(store: &Store, label: &str, q: &Query) {
    let mut best = Duration::MAX;
    let mut count = 0;
    for _ in 0..5 {
        let t = Instant::now();
        count = store.search(q).unwrap().map(|r| r.unwrap()).count();
        best = best.min(t.elapsed());
    }
    println!("| {label} | {count} | {:.2} |", best.as_secs_f64() * 1e3);
}

fn time_agg(store: &Store, label: &str, q: &AggQuery) {
    let mut best = Duration::MAX;
    let mut groups = 0;
    for _ in 0..5 {
        let t = Instant::now();
        groups = store.aggregate(q).unwrap().len();
        best = best.min(t.elapsed());
    }
    println!("| {label} | {groups} | {:.2} |", best.as_secs_f64() * 1e3);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let events: u64 = arg(&args, "--events", "1000000").parse().unwrap();
    let batch: u64 = arg(&args, "--batch", "500").parse().unwrap();
    let flush_mib: usize = arg(&args, "--flush-mib", "8").parse().unwrap();
    let dir = std::path::PathBuf::from(arg(
        &args,
        "--dir",
        &std::env::temp_dir().join("evstore-r6").to_string_lossy(),
    ));
    let _ = std::fs::remove_dir_all(&dir);

    let opts = Options {
        sync: SyncPolicy::Interval(Duration::from_secs(1)),
        flush_bytes: usize::MAX, // flushes are driven (and timed) below
        retention: RetentionPolicy::default(),
        ..Options::default()
    };
    let mut store = Store::open(&dir, opts).unwrap();
    let mut rng = Rng(0x5eed_1234);
    let t0 = 1_762_246_800_000_000_000i64; // 2025-11-04 09:00 UTC

    let gen_start = Instant::now();
    let all: Vec<Record> = (0..events).map(|i| record(&mut rng, i, t0)).collect();
    let gen_time = gen_start.elapsed();
    let raw_bytes: usize = all.iter().map(|r| match r.get("@raw") {
        Some(Value::Str(s)) => s.len(),
        _ => 0,
    }).sum();

    let flush_at = flush_mib << 20;
    let (mut append_time, mut flush_time) = (Duration::ZERO, Duration::ZERO);
    let (mut flushes, mut buffered_bytes) = (0, 0usize);
    let mut iter = all.into_iter();
    loop {
        let chunk: Vec<Record> = iter.by_ref().take(batch as usize).collect();
        if chunk.is_empty() {
            break;
        }
        buffered_bytes += chunk.iter().map(|r| 64 + r.fields.iter().map(|(n, v)| n.len() + match v {
            Value::Str(s) => s.len() + 24,
            _ => 16,
        }).sum::<usize>()).sum::<usize>();
        let t = Instant::now();
        store.append(chunk).unwrap();
        append_time += t.elapsed();
        if buffered_bytes >= flush_at {
            let t = Instant::now();
            store.flush().unwrap();
            flush_time += t.elapsed();
            flushes += 1;
            buffered_bytes = 0;
        }
    }
    let t = Instant::now();
    store.flush().unwrap();
    flush_time += t.elapsed();
    flushes += 1;
    let segments_before = store.stats().segments;
    let size_before = dir_size(store.path());

    let t = Instant::now();
    let report = store.maintain(t0).unwrap();
    let compact_time = t.elapsed();
    let stats = store.stats();
    let size_after = dir_size(store.path());

    let per = |d: Duration| d.as_secs_f64() * 1e6 / events as f64;
    println!("## Ingest ({events} events, batches of {batch}, flush every {flush_mib} MiB)\n");
    println!("| Step | Total | µs/event |");
    println!("|---|---|---|");
    println!("| generate records (not counted) | {:.2} s | {:.2} |", gen_time.as_secs_f64(), per(gen_time));
    println!("| append (WAL write + buffer) | {:.2} s | {:.2} |", append_time.as_secs_f64(), per(append_time));
    println!("| flush ({flushes} segments) | {:.2} s | {:.2} |", flush_time.as_secs_f64(), per(flush_time));
    println!("| **append + flush** | {:.2} s | **{:.2}** |", (append_time + flush_time).as_secs_f64(), per(append_time + flush_time));
    println!("| compaction ({} merges, {segments_before} → {} segments) | {:.2} s | {:.2} |", report.compactions, stats.segments, compact_time.as_secs_f64(), per(compact_time));
    println!();
    println!("## Size\n");
    println!("| | Bytes | Bytes/event |");
    println!("|---|---|---|");
    println!("| raw syslog lines | {raw_bytes} | {:.1} |", raw_bytes as f64 / events as f64);
    println!("| store after flushes (level 3) | {size_before} | {:.1} |", size_before as f64 / events as f64);
    println!("| store after compaction (level 9) | {size_after} | {:.1} |", size_after as f64 / events as f64);
    println!();

    println!("## Queries (best of 5)\n");
    println!("| Query | Rows | ms |");
    println!("|---|---|---|");
    let q = |filter: Filter, order: Order, limit: Option<usize>, time: TimeRange| Query { time, filter, order, limit };
    let all_time = TimeRange::ALL;
    time_query(&store, "newest 100 (`*`, desc)", &q(Filter::True, Order::Desc, Some(100), all_time));
    time_query(&store, "`host.name = nas`, newest 100", &q(Filter::field("host.name", FieldOp::Eq(Value::Str("NAS".into()))), Order::Desc, Some(100), all_time));
    time_query(&store, "`process.name = sudo` (all)", &q(Filter::field("process.name", FieldOp::Eq(Value::Str("sudo".into()))), Order::Storage, None, all_time));
    time_query(&store, "`message contains \"job-7.sh\"` (all)", &q(Filter::field("message", FieldOp::Contains("job-7.sh".into())), Order::Storage, None, all_time));
    time_query(&store, "`source.ip in 192.168.7.0/24` (all)", &q(Filter::field("source.ip", FieldOp::Cidr { lo: 0xffff_c0a8_0700, hi: 0xffff_c0a8_07ff }), Order::Storage, None, all_time));
    time_query(&store, "`process.pid = 4242` (all)", &q(Filter::field("process.pid", FieldOp::Eq(Value::I64(4242))), Order::Storage, None, all_time));
    let last_minute = TimeRange { start: Some(t0 + events as i64 * 500_000 - 60_000_000_000), end: None };
    time_query(&store, "last minute, all fields", &q(Filter::True, Order::Asc, None, last_minute));
    time_query(&store, "count everything", &q(Filter::True, Order::Storage, None, all_time));

    println!();
    println!("## Aggregations (best of 5)
");
    println!("| Aggregation | Groups | ms |");
    println!("|---|---|---|");
    let agg = |filter: Filter, group_by: Vec<GroupBy>, metrics: Vec<Metric>| AggQuery {
        time: TimeRange::ALL,
        filter,
        group_by,
        metrics,
    };
    time_agg(&store, "count everything", &agg(Filter::True, vec![], vec![Metric::Count]));
    time_agg(&store, "count by `host.name`", &agg(Filter::True, vec![GroupBy::Field("host.name".into())], vec![Metric::Count]));
    time_agg(&store, "count by 1h bucket", &agg(Filter::True, vec![GroupBy::TimeBucket { span_ns: 3_600_000_000_000 }], vec![Metric::Count]));
    time_agg(&store, "sum(`source.port`) by `process.name`", &agg(Filter::True, vec![GroupBy::Field("process.name".into())], vec![Metric::Sum("source.port".into())]));
    time_agg(&store, "`process.name = sudo`, count by `user.name`", &agg(Filter::field("process.name", FieldOp::Eq(Value::Str("sudo".into()))), vec![GroupBy::Field("user.name".into())], vec![Metric::Count]));
    time_agg(&store, "count by `host.name`, 5m bucket", &agg(Filter::True, vec![GroupBy::Field("host.name".into()), GroupBy::TimeBucket { span_ns: 300_000_000_000 }], vec![Metric::Count, Metric::Max("process.pid".into())]));

    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}
