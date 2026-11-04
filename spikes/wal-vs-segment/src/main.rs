//! Spike: when does scanning a tenant's WAL become too slow compared with a
//! columnar segment? This sets the WAL-to-segment conversion threshold of
//! the "WAL is the buffer for small tenants" design
//! (docs/design/04-durability-and-buffers.md).
//!
//! WAL model: row-major, schema-encoded but uncompressed records
//!   `[u32 len][u16 n_fields]{[u16 field id][u32 value len][value]}*`,
//! encrypted in 64 KiB chunks with AES-256-GCM (`ring`, spike 03). A query
//! decrypts and parses every record.
//!
//! Segment model: events grouped by schema; per group and field a zstd
//! dictionary of distinct values plus u32 codes in 4096-row zstd chunks.
//! Equality: look the value up in the dictionary, then scan the codes.
//! Substring: test every distinct value once, then scan the codes. No
//! postings lists, so the segment numbers are conservative.
//!
//! Every query counts all matches and materializes the first 100 (all
//! fields). Data is in memory (warm page cache); nothing decoded is cached
//! between queries.
//!
//! Usage: wal-vs-segment <jsonl dir> [--sizes 1000,10000,50000,200000]

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use serde_json::Value;

const WAL_CHUNK: usize = 64 * 1024;
const ROW_CHUNK: usize = 4096;
const ABSENT: u32 = u32::MAX;
const MATERIALIZE: usize = 100;
const ZSTD_LEVEL: i32 = 3;

type Event = serde_json::Map<String, Value>;
/// One materialized event: (field id, value) pairs.
type Hit = Vec<(u16, Vec<u8>)>;

fn value_text(v: &Value) -> Vec<u8> {
    match v {
        Value::String(s) => s.as_bytes().to_vec(),
        other => other.to_string().into_bytes(),
    }
}

fn schema_of(e: &Event) -> String {
    let channel = e
        .get("Channel")
        .or_else(|| e.get("SourceName"))
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_ascii_lowercase();
    let id = e.get("EventID").map(|v| v.to_string()).unwrap_or_default();
    format!("{channel}#{id}")
}

/// Loads up to `max` events from the JSONL files, in file-name order.
/// `Message` is dropped: the design stores template ids, not rendered text.
fn load(dir: &Path, max: usize) -> Vec<Event> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    paths.sort();
    let mut events = Vec::with_capacity(max);
    for p in paths {
        let Ok(data) = std::fs::read(&p) else {
            continue; // blocked by the antivirus
        };
        for line in data.split(|&b| b == b'\n') {
            if let Ok(Value::Object(mut e)) = serde_json::from_slice::<Value>(line) {
                e.remove("Message");
                events.push(e);
                if events.len() == max {
                    return events;
                }
            }
        }
    }
    events
}

#[derive(Default)]
struct Registry {
    ids: HashMap<String, u16>,
    names: Vec<String>,
}

impl Registry {
    fn id(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.ids.get(name) {
            return id;
        }
        let id = self.names.len() as u16;
        self.names.push(name.to_string());
        self.ids.insert(name.to_string(), id);
        id
    }
}

// ---------------------------------------------------------------- WAL

struct Wal {
    key: LessSafeKey,
    /// Encrypted chunks: ciphertext followed by the 16-byte tag.
    chunks: Vec<Vec<u8>>,
    plain_len: usize,
}

fn nonce(i: usize) -> Nonce {
    let mut n = [0u8; 12];
    n[..8].copy_from_slice(&(i as u64).to_le_bytes());
    Nonce::assume_unique_for_key(n)
}

fn build_wal(events: &[Event], reg: &mut Registry) -> Wal {
    let mut plain = Vec::new();
    for e in events {
        let start = plain.len();
        plain.extend_from_slice(&[0u8; 4]);
        plain.extend_from_slice(&(e.len() as u16).to_le_bytes());
        for (k, v) in e {
            let bytes = value_text(v);
            plain.extend_from_slice(&reg.id(k).to_le_bytes());
            plain.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            plain.extend_from_slice(&bytes);
        }
        let len = (plain.len() - start) as u32;
        plain[start..start + 4].copy_from_slice(&len.to_le_bytes());
    }
    let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[7u8; 32]).expect("key"));
    let chunks = plain
        .chunks(WAL_CHUNK)
        .enumerate()
        .map(|(i, c)| {
            let mut buf = c.to_vec();
            key.seal_in_place_append_tag(nonce(i), Aad::empty(), &mut buf)
                .expect("seal");
            buf
        })
        .collect();
    Wal {
        key,
        chunks,
        plain_len: plain.len(),
    }
}

enum Pred<'a> {
    Eq(&'a [u8]),
    Contains(&'a [u8]),
}

impl Pred<'_> {
    fn matches(&self, v: &[u8]) -> bool {
        match self {
            Pred::Eq(x) => v == *x,
            Pred::Contains(x) => v.windows(x.len()).any(|w| w == *x),
        }
    }
}

/// Decrypts and scans the whole WAL. Returns (matches, materialized events).
fn query_wal(wal: &Wal, field: u16, pred: &Pred) -> (usize, Vec<Hit>) {
    let mut plain = Vec::with_capacity(wal.plain_len);
    for (i, c) in wal.chunks.iter().enumerate() {
        let mut buf = c.clone();
        let out = wal
            .key
            .open_in_place(nonce(i), Aad::empty(), &mut buf)
            .expect("open");
        plain.extend_from_slice(out);
    }
    let mut matches = 0;
    let mut hits = Vec::new();
    let mut pos = 0;
    while pos < plain.len() {
        let len = u32::from_le_bytes(plain[pos..pos + 4].try_into().unwrap()) as usize;
        let rec = &plain[pos..pos + len];
        let n = u16::from_le_bytes(rec[4..6].try_into().unwrap());
        let mut p = 6;
        let mut matched = false;
        for _ in 0..n {
            let id = u16::from_le_bytes(rec[p..p + 2].try_into().unwrap());
            let vlen = u32::from_le_bytes(rec[p + 2..p + 6].try_into().unwrap()) as usize;
            let v = &rec[p + 6..p + 6 + vlen];
            if id == field && pred.matches(v) {
                matched = true;
            }
            p += 6 + vlen;
        }
        if matched {
            matches += 1;
            if hits.len() < MATERIALIZE {
                let mut fields = Vec::new();
                let mut p = 6;
                for _ in 0..n {
                    let id = u16::from_le_bytes(rec[p..p + 2].try_into().unwrap());
                    let vlen = u32::from_le_bytes(rec[p + 2..p + 6].try_into().unwrap()) as usize;
                    fields.push((id, rec[p + 6..p + 6 + vlen].to_vec()));
                    p += 6 + vlen;
                }
                hits.push(fields);
            }
        }
        pos += len;
    }
    (matches, hits)
}

// ------------------------------------------------------------ segment

struct Column {
    field: u16,
    /// zstd of `{[u32 len][value]}*`.
    dict: Vec<u8>,
    dict_raw_len: usize,
    /// zstd of little-endian u32 codes, ROW_CHUNK rows each.
    chunks: Vec<Vec<u8>>,
}

struct Group {
    rows: usize,
    columns: Vec<Column>,
}

struct Segment {
    groups: Vec<Group>,
    bytes: usize,
}

fn build_segment(events: &[Event], reg: &mut Registry) -> Segment {
    let mut by_schema: BTreeMap<String, Vec<&Event>> = BTreeMap::new();
    for e in events {
        by_schema.entry(schema_of(e)).or_default().push(e);
    }
    let mut groups = Vec::new();
    let mut bytes = 0;
    for evs in by_schema.values() {
        let mut fields: Vec<&String> = evs.iter().flat_map(|e| e.keys()).collect();
        fields.sort();
        fields.dedup();
        let mut columns = Vec::new();
        for f in fields {
            let mut dict_index: HashMap<Vec<u8>, u32> = HashMap::new();
            let mut dict_raw = Vec::new();
            let mut codes = Vec::with_capacity(evs.len());
            for e in evs {
                let code = match e.get(f.as_str()) {
                    None => ABSENT,
                    Some(v) => {
                        let bytes = value_text(v);
                        let next = dict_index.len() as u32;
                        *dict_index.entry(bytes.clone()).or_insert_with(|| {
                            dict_raw.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                            dict_raw.extend_from_slice(&bytes);
                            next
                        })
                    }
                };
                codes.push(code);
            }
            let dict = zstd::bulk::compress(&dict_raw, ZSTD_LEVEL).expect("zstd");
            let chunks: Vec<Vec<u8>> = codes
                .chunks(ROW_CHUNK)
                .map(|c| {
                    let raw: Vec<u8> = c.iter().flat_map(|x| x.to_le_bytes()).collect();
                    zstd::bulk::compress(&raw, ZSTD_LEVEL).expect("zstd")
                })
                .collect();
            bytes += dict.len() + chunks.iter().map(Vec::len).sum::<usize>();
            columns.push(Column {
                field: reg.id(f),
                dict,
                dict_raw_len: dict_raw.len(),
                chunks,
            });
        }
        groups.push(Group {
            rows: evs.len(),
            columns,
        });
    }
    Segment { groups, bytes }
}

fn decode_dict(c: &Column) -> Vec<Vec<u8>> {
    let raw = zstd::bulk::decompress(&c.dict, c.dict_raw_len).expect("dict");
    let mut out = Vec::new();
    let mut p = 0;
    while p < raw.len() {
        let len = u32::from_le_bytes(raw[p..p + 4].try_into().unwrap()) as usize;
        out.push(raw[p + 4..p + 4 + len].to_vec());
        p += 4 + len;
    }
    out
}

fn decode_codes(c: &Column, chunk: usize, rows: usize) -> Vec<u32> {
    let n = (rows - chunk * ROW_CHUNK).min(ROW_CHUNK);
    let raw = zstd::bulk::decompress(&c.chunks[chunk], n * 4).expect("codes");
    raw.as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect()
}

fn query_segment(seg: &Segment, field: u16, pred: &Pred) -> (usize, Vec<Hit>) {
    let mut matches = 0;
    let mut hits = Vec::new();
    for g in &seg.groups {
        let Some(col) = g.columns.iter().find(|c| c.field == field) else {
            continue; // schema-group pruning
        };
        let dict = decode_dict(col);
        let mut wanted = vec![false; dict.len()];
        let mut any = false;
        for (code, v) in dict.iter().enumerate() {
            if pred.matches(v) {
                wanted[code] = true;
                any = true;
            }
        }
        if !any {
            continue; // dictionary pruning
        }
        let mut hit_rows = Vec::new();
        for chunk in 0..col.chunks.len() {
            for (i, code) in decode_codes(col, chunk, g.rows).into_iter().enumerate() {
                if code != ABSENT && wanted[code as usize] {
                    matches += 1;
                    if hits.len() + hit_rows.len() < MATERIALIZE {
                        hit_rows.push(chunk * ROW_CHUNK + i);
                    }
                }
            }
        }
        if hit_rows.is_empty() {
            continue;
        }
        // Late materialization: decode each column's dictionary and the code
        // chunks that contain hits, once per query.
        let mut events: Vec<Hit> = vec![Vec::new(); hit_rows.len()];
        for c in &g.columns {
            let dict = decode_dict(c);
            let mut cached: Option<(usize, Vec<u32>)> = None;
            for (h, &row) in hit_rows.iter().enumerate() {
                let chunk = row / ROW_CHUNK;
                if cached.as_ref().is_none_or(|(k, _)| *k != chunk) {
                    cached = Some((chunk, decode_codes(c, chunk, g.rows)));
                }
                let code = cached.as_ref().unwrap().1[row % ROW_CHUNK];
                if code != ABSENT {
                    events[h].push((c.field, dict[code as usize].clone()));
                }
            }
        }
        hits.extend(events);
    }
    (matches, hits)
}

// -------------------------------------------------------------- bench

fn median_ms(mut f: impl FnMut() -> usize) -> (f64, usize) {
    let mut times = Vec::new();
    let mut result = 0;
    let start = Instant::now();
    while times.len() < 5 || (times.len() < 30 && start.elapsed().as_millis() < 2000) {
        let t = Instant::now();
        result = black_box(f());
        times.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(f64::total_cmp);
    (times[times.len() / 2], result)
}

/// Picks the `Image` value whose frequency is closest to 0.5% of events.
fn pick_selective_image(events: &[Event]) -> Vec<u8> {
    let mut freq: HashMap<Vec<u8>, usize> = HashMap::new();
    for e in events {
        if let Some(v) = e.get("Image") {
            *freq.entry(value_text(v)).or_default() += 1;
        }
    }
    let target = events.len() / 200;
    freq.into_iter()
        .min_by_key(|(_, n)| n.abs_diff(target))
        .map(|(v, _)| v)
        .unwrap_or_default()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .expect("usage: wal-vs-segment <jsonl dir> [--sizes a,b]"),
    );
    let mut sizes = vec![1000, 10000, 50000, 200000];
    while let Some(flag) = args.next() {
        let v = args.next().expect("value");
        match flag.as_str() {
            "--sizes" => sizes = v.split(',').map(|x| x.parse().expect("size")).collect(),
            other => panic!("unknown flag {other}"),
        }
    }
    let max = *sizes.iter().max().unwrap();
    let all = load(&dir, max);
    eprintln!("loaded {} events (Message dropped)", all.len());
    let image = pick_selective_image(&all);
    println!(
        "selective value: Image = {}\n",
        String::from_utf8_lossy(&image)
    );

    println!(
        "| events | WAL MiB | segment MiB | segment build ms (µs/event) | query | matches | WAL ms | segment ms | WAL / segment |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for &n in &sizes {
        if n > all.len() {
            continue;
        }
        let events = &all[..n];
        let mut reg = Registry::default();
        let wal = build_wal(events, &mut reg);
        let t = Instant::now();
        let seg = build_segment(events, &mut reg);
        let build_ms = t.elapsed().as_secs_f64() * 1000.0;
        let queries: [(&str, u16, Pred); 3] = [
            ("Image = <0.5% value>", reg.id("Image"), Pred::Eq(&image)),
            (
                "CommandLine contains \"whoami\"",
                reg.id("CommandLine"),
                Pred::Contains(b"whoami"),
            ),
            ("EventID = 1", reg.id("EventID"), Pred::Eq(b"1")),
        ];
        for (i, (label, field, pred)) in queries.iter().enumerate() {
            let (wal_ms, wal_n) = median_ms(|| {
                let (m, h) = query_wal(&wal, *field, pred);
                m + h.len()
            });
            let (seg_ms, seg_n) = median_ms(|| {
                let (m, h) = query_segment(&seg, *field, pred);
                m + h.len()
            });
            assert_eq!(wal_n, seg_n, "WAL and segment disagree on {label}");
            let (matches, _) = query_segment(&seg, *field, pred);
            let size_cols = if i == 0 {
                format!(
                    "{:.2} | {:.2} | {:.1} ({:.1})",
                    wal.plain_len as f64 / 1048576.0,
                    seg.bytes as f64 / 1048576.0,
                    build_ms,
                    build_ms * 1000.0 / n as f64
                )
            } else {
                " | | ".to_string()
            };
            let n_col = if i == 0 { n.to_string() } else { String::new() };
            println!(
                "| {n_col} | {size_cols} | {label} | {matches} | {wal_ms:.2} | {seg_ms:.2} | {:.1}× |",
                wal_ms / seg_ms
            );
        }
    }
}
