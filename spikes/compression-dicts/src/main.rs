//! Spike for risk R1: how many bytes per event does a tenant cost, by tenant
//! size, and how much do schema grouping, templates and pre-trained
//! dictionaries help?
//!
//! Input: a directory of JSONL files, one per tenant (e.g. OTRF
//! Security-Datasets, where each file is one recorded attack run).
//!
//! Tenants are split by a hash of their file name into a TRAIN set (stands
//! in for the reference corpus uSIEM trains dictionaries on) and a TEST set.
//! Dictionaries never see TEST data.
//!
//! Encodings (all zstd at the same level):
//! - `json_block64k`: the tenant's JSON lines in 64 KiB blocks, one zstd
//!   frame per block: a generic document store (Elasticsearch/Quickwit-like).
//! - `columnar`: events grouped by schema (Channel, EventID), sorted by
//!   timestamp. Schemas with >= --pack-threshold events (default 2000) get a column-major
//!   group (one frame per group); the rest share one packed row-major group.
//!   Values are kept as text: no typed encodings yet, so this is conservative.
//! - `columnar_tpl`: as `columnar`, without the `Message` field, which a
//!   template + parameters reconstruct (upper bound of the template lever).
//! - `columnar_tpl_dict`: as `columnar_tpl`, with a pre-trained dictionary
//!   per schema for columnar groups and one domain dictionary for the packed
//!   group.
//!
//! Every columnar encoding also pays modelled metadata: FILE_OVERHEAD per
//! tenant file, GROUP_OVERHEAD per group and COLUMN_OVERHEAD per column.
//!
//! Usage: compression-dicts <jsonl dir> [--level 3] [--sizes 100,1000,10000] [--train-env theshire.local] [--pack-threshold 2000]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// Default; overridable with --pack-threshold.
const DEFAULT_PACK_THRESHOLD: usize = 2000;
/// Train events per column-shaped dictionary sample.
const COLUMN_SAMPLE_EVENTS: usize = 64;
const FILE_OVERHEAD: usize = 128;
const GROUP_OVERHEAD: usize = 24;
const COLUMN_OVERHEAD: usize = 6;
const BLOCK: usize = 64 * 1024;
const SCHEMA_DICT_SIZE: usize = 32 * 1024;
const DOMAIN_DICT_SIZE: usize = 64 * 1024;
const MAX_TRAIN_SAMPLES: usize = 4000;
/// Value separator and "field absent" marker inside column streams.
const SEP: u8 = 0x1e;
const ABSENT: u8 = 0x1f;

type Event = serde_json::Map<String, Value>;
type SchemaKey = String;
/// A measured tenant sample: raw JSON lines and their parsed events.
type Sample<'a> = (Vec<&'a [u8]>, Vec<&'a Event>);

struct Tenant {
    name: String,
    /// Raw JSON line of each event, in file order.
    lines: Vec<Vec<u8>>,
    events: Vec<Event>,
}

fn schema_of(e: &Event) -> SchemaKey {
    let channel = e
        .get("Channel")
        .or_else(|| e.get("SourceName"))
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_ascii_lowercase();
    let id = e.get("EventID").map(|v| v.to_string()).unwrap_or_default();
    format!("{channel}#{id}")
}

fn value_text(v: &Value) -> Vec<u8> {
    match v {
        Value::String(s) => s.as_bytes().to_vec(),
        other => other.to_string().into_bytes(),
    }
}

/// True when any event of the tenant comes from a host in `env`.
fn in_env(t: &Tenant, env: &str) -> bool {
    t.events.iter().any(|e| {
        e.get("Hostname")
            .and_then(Value::as_str)
            .is_some_and(|h| h.to_ascii_lowercase().contains(env))
    })
}

fn is_train(name: &str) -> bool {
    // FNV-1a: stable across runs and platforms.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h.is_multiple_of(3)
}

fn load(dir: &Path) -> Vec<Tenant> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read input dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    paths.sort();
    let mut tenants = Vec::new();
    let mut skipped = 0;
    for p in paths {
        // Files blocked by the antivirus fail to read; skip them.
        let Ok(data) = fs::read(&p) else {
            skipped += 1;
            continue;
        };
        let mut lines = Vec::new();
        let mut events = Vec::new();
        for line in data.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            if let Ok(Value::Object(obj)) = serde_json::from_slice::<Value>(line) {
                lines.push(line.to_vec());
                events.push(obj);
            }
        }
        if !events.is_empty() {
            let name = p.file_stem().unwrap().to_string_lossy().into_owned();
            tenants.push(Tenant {
                name,
                lines,
                events,
            });
        }
    }
    eprintln!(
        "loaded {} tenants ({} unreadable, skipped)",
        tenants.len(),
        skipped
    );
    tenants
}

fn timestamp(e: &Event) -> &str {
    e.get("@timestamp").and_then(Value::as_str).unwrap_or("")
}

/// One event's values in a fixed field order: a training sample shaped like
/// the column data it will help compress.
fn row_record(e: &Event, fields: &[String], drop_message: bool) -> Vec<u8> {
    let mut out = Vec::new();
    for f in fields {
        if drop_message && f == "Message" {
            continue;
        }
        match e.get(f) {
            Some(v) => out.extend_from_slice(&value_text(v)),
            None => out.push(ABSENT),
        }
        out.push(SEP);
    }
    out
}

/// Packed (row-major) record: field name ids are modelled as 2 bytes each.
fn packed_record(e: &Event, drop_message: bool) -> Vec<u8> {
    let mut out = Vec::new();
    for (k, v) in e {
        if drop_message && k == "Message" {
            continue;
        }
        out.extend_from_slice(&(k.len() as u16).to_le_bytes()[..2]);
        out.extend_from_slice(&value_text(v));
        out.push(SEP);
    }
    out
}

struct Dicts {
    /// Per-schema dictionaries trained on row-shaped samples.
    schema_row: HashMap<SchemaKey, Vec<u8>>,
    /// Per-schema dictionaries trained on column-shaped samples.
    schema_col: HashMap<SchemaKey, Vec<u8>>,
    domain: Vec<u8>,
}

#[derive(Clone, Copy)]
enum DictKind {
    Row,
    Col,
}

fn field_order(events: &[&Event]) -> Vec<String> {
    let set: BTreeSet<&String> = events.iter().flat_map(|e| e.keys()).collect();
    set.into_iter().cloned().collect()
}

fn train(tenants: &[&Tenant]) -> Dicts {
    let mut by_schema: BTreeMap<SchemaKey, Vec<&Event>> = BTreeMap::new();
    for t in tenants {
        for e in &t.events {
            by_schema.entry(schema_of(e)).or_default().push(e);
        }
    }
    let mut schema_row = HashMap::new();
    let mut schema_col = HashMap::new();
    let mut domain_samples = Vec::new();
    for (key, events) in &by_schema {
        let fields = field_order(events);
        let step = (events.len() / MAX_TRAIN_SAMPLES).max(1);
        let samples: Vec<Vec<u8>> = events
            .iter()
            .step_by(step)
            .map(|e| row_record(e, &fields, true))
            .collect();
        for e in events.iter().step_by((events.len() / 200).max(1)) {
            domain_samples.push(packed_record(e, true));
        }
        if samples.len() >= 50
            && let Ok(d) = zstd::dict::from_samples(&samples, SCHEMA_DICT_SIZE)
        {
            schema_row.insert(key.clone(), d);
        }
        let tpl_fields: Vec<String> = fields.iter().filter(|f| *f != "Message").cloned().collect();
        let col_samples: Vec<Vec<u8>> = events
            .chunks(COLUMN_SAMPLE_EVENTS)
            .take(MAX_TRAIN_SAMPLES)
            .map(|chunk| column_stream(chunk, &tpl_fields))
            .collect();
        if col_samples.len() >= 10
            && let Ok(d) = zstd::dict::from_samples(&col_samples, SCHEMA_DICT_SIZE)
        {
            schema_col.insert(key.clone(), d);
        }
    }
    let domain = zstd::dict::from_samples(&domain_samples, DOMAIN_DICT_SIZE).unwrap_or_default();
    eprintln!(
        "trained {} row-shaped / {} column-shaped schema dictionaries ({} schemas seen), domain dictionary {} bytes",
        schema_row.len(),
        schema_col.len(),
        by_schema.len(),
        domain.len()
    );
    Dicts {
        schema_row,
        schema_col,
        domain,
    }
}

fn compress(data: &[u8], level: i32, dict: Option<&[u8]>) -> usize {
    if data.is_empty() {
        return 0;
    }
    let out = match dict {
        Some(d) if !d.is_empty() => {
            zstd::bulk::Compressor::with_dictionary(level, d).and_then(|mut c| c.compress(data))
        }
        _ => zstd::bulk::compress(data, level),
    };
    out.expect("zstd").len()
}

fn json_block64k(lines: &[&[u8]], level: i32) -> usize {
    let mut total = 0;
    let mut block = Vec::with_capacity(BLOCK + 4096);
    for l in lines {
        block.extend_from_slice(l);
        block.push(b'\n');
        if block.len() >= BLOCK {
            total += compress(&block, level, None);
            block.clear();
        }
    }
    total + compress(&block, level, None)
}

fn column_stream(evs: &[&Event], fields: &[String]) -> Vec<u8> {
    let mut stream = Vec::new();
    for f in fields {
        for e in evs {
            match e.get(f) {
                Some(v) => stream.extend_from_slice(&value_text(v)),
                None => stream.push(ABSENT),
            }
            stream.push(SEP);
        }
    }
    stream
}

fn columnar(
    events: &[&Event],
    level: i32,
    drop_message: bool,
    dicts: Option<(&Dicts, DictKind)>,
    pack_threshold: usize,
) -> usize {
    let mut groups: BTreeMap<SchemaKey, Vec<&Event>> = BTreeMap::new();
    for e in events {
        groups.entry(schema_of(e)).or_default().push(e);
    }
    let mut total = FILE_OVERHEAD;
    let mut packed: Vec<&Event> = Vec::new();
    for (key, mut evs) in groups {
        if evs.len() < pack_threshold {
            packed.extend(evs);
            continue;
        }
        evs.sort_by(|a, b| timestamp(a).cmp(timestamp(b)));
        let fields: Vec<String> = field_order(&evs)
            .into_iter()
            .filter(|f| !(drop_message && f == "Message"))
            .collect();
        let stream = column_stream(&evs, &fields);
        let dict = dicts
            .and_then(|(d, kind)| match kind {
                DictKind::Row => d.schema_row.get(&key),
                DictKind::Col => d.schema_col.get(&key),
            })
            .map(Vec::as_slice);
        total += GROUP_OVERHEAD + COLUMN_OVERHEAD * fields.len() + compress(&stream, level, dict);
    }
    if !packed.is_empty() {
        packed.sort_by(|a, b| timestamp(a).cmp(timestamp(b)));
        let mut stream = Vec::new();
        for e in &packed {
            stream.extend_from_slice(&packed_record(e, drop_message));
        }
        let dict = dicts.map(|(d, _)| d.domain.as_slice());
        total += GROUP_OVERHEAD + compress(&stream, level, dict);
    }
    total
}

const ENCODINGS: [&str; 6] = [
    "raw_json",
    "json_block64k",
    "columnar",
    "columnar_tpl",
    "tpl_dict_row",
    "tpl_dict_col",
];

fn measure(lines: &[&[u8]], events: &[&Event], level: i32, dicts: &Dicts, pt: usize) -> [usize; 6] {
    [
        lines.iter().map(|l| l.len() + 1).sum(),
        json_block64k(lines, level),
        columnar(events, level, false, None, pt),
        columnar(events, level, true, None, pt),
        columnar(events, level, true, Some((dicts, DictKind::Row)), pt),
        columnar(events, level, true, Some((dicts, DictKind::Col)), pt),
    ]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .expect("usage: compression-dicts <jsonl dir> [--level N] [--sizes a,b]"),
    );
    let mut level = 3;
    let mut sizes = vec![100, 1000, 10000];
    let mut train_env: Option<String> = None;
    let mut pack_threshold = DEFAULT_PACK_THRESHOLD;
    while let Some(flag) = args.next() {
        let v = args.next().expect("flag value");
        match flag.as_str() {
            "--level" => level = v.parse().expect("level"),
            "--sizes" => sizes = v.split(',').map(|x| x.parse().expect("size")).collect(),
            // Strict split: train only on tenants whose hosts belong to this
            // environment (e.g. a domain suffix); test on all the others.
            "--train-env" => train_env = Some(v.to_ascii_lowercase()),
            "--pack-threshold" => pack_threshold = v.parse().expect("pack threshold"),
            other => panic!("unknown flag {other}"),
        }
    }

    let tenants = load(&dir);
    let (train_set, test_set): (Vec<&Tenant>, Vec<&Tenant>) = match &train_env {
        None => tenants.iter().partition(|t| is_train(&t.name)),
        Some(env) => tenants.iter().partition(|t| in_env(t, env)),
    };
    match &train_env {
        None => println!("split: hash of tenant name (train and test share lab environments)\n"),
        Some(env) => {
            println!("split: train on environment `{env}`, test on all other environments\n")
        }
    }
    eprintln!(
        "train tenants: {} ({} events), test tenants: {} ({} events)",
        train_set.len(),
        train_set.iter().map(|t| t.events.len()).sum::<usize>(),
        test_set.len(),
        test_set.iter().map(|t| t.events.len()).sum::<usize>()
    );
    let dicts = train(&train_set);

    // Buckets: each fixed size (prefix of every test tenant big enough), plus
    // "full" (every test tenant at its natural size).
    let mut buckets: Vec<(String, Vec<Sample>)> = Vec::new();
    for &n in &sizes {
        let samples = test_set
            .iter()
            .filter(|t| t.events.len() >= n)
            .map(|t| {
                (
                    t.lines[..n].iter().map(Vec::as_slice).collect(),
                    t.events[..n].iter().collect(),
                )
            })
            .collect();
        buckets.push((format!("{n} events"), samples));
    }
    buckets.push((
        "full tenants".to_string(),
        test_set
            .iter()
            .map(|t| {
                (
                    t.lines.iter().map(Vec::as_slice).collect(),
                    t.events.iter().collect(),
                )
            })
            .collect(),
    ));

    println!(
        "zstd level {level}; pack threshold {pack_threshold} events per schema; bytes per event (ratio to raw JSON in parentheses)\n"
    );
    print!("| tenant size | tenants | events |");
    for e in ENCODINGS {
        print!(" {e} |");
    }
    println!();
    println!("|---|---|---|{}", "---|".repeat(ENCODINGS.len()));
    for (label, samples) in &buckets {
        if samples.is_empty() {
            continue;
        }
        let mut totals = [0usize; 6];
        let mut events = 0;
        for (lines, evs) in samples {
            let m = measure(lines, evs, level, &dicts, pack_threshold);
            for i in 0..6 {
                totals[i] += m[i];
            }
            events += evs.len();
        }
        print!("| {label} | {} | {events} |", samples.len());
        for t in totals {
            let per = t as f64 / events as f64;
            let ratio = t as f64 / totals[0] as f64;
            print!(" {per:.0} ({ratio:.3}) |");
        }
        println!();
    }
}
