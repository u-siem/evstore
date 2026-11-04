//! Spike for risk R3: how long does one coordinated sync round take when N
//! per-tenant WAL files are dirty?
//!
//! Model (docs/design/04-durability-and-buffers.md):
//! - every tenant has its own preallocated WAL file, kept open;
//! - in each round every dirty tenant has appended one record;
//! - the round ends when every appended record is durable.
//!
//! "Round latency" is what an acknowledging writer waits for:
//! - buffered methods: the flush phase only (writers already did `write()`
//!   into the page cache before the round started);
//! - write-through methods: the writes themselves (they are durable when they
//!   return, no flush call exists).
//!
//! Usage:
//!   fsync-rounds <dir> [--levels 10,100,1000] [--threads 1,8,32]
//!                [--rounds 10] [--budget-secs 30] [--methods all|a,b]

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Size of one appended record. A multiple of the sector size, so the
/// unbuffered method can use the same size without padding.
const RECORD: usize = 4096;
/// Preallocated size of each WAL file (the WAL pool preallocates).
const FILE_SIZE: u64 = 64 * 1024;
/// Upper bound on rounds per configuration: FILE_SIZE / RECORD.
const MAX_ROUNDS: usize = (FILE_SIZE as usize) / RECORD;

#[derive(Clone, Copy, PartialEq)]
enum Method {
    /// write() each file, then one thread flushes them one after another.
    SeqFlush,
    /// write() each file, then T threads flush in parallel.
    ParFlush,
    /// Files opened with FILE_FLAG_WRITE_THROUGH; T threads write in
    /// parallel. Durable on return **only if the device honours FUA**;
    /// otherwise the data may sit in the drive's volatile cache. Windows only.
    WriteThrough,
    /// FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING with sector-aligned
    /// buffers and offsets; T threads. Same FUA caveat. Windows only.
    WriteThroughUnbuffered,
    /// WriteThrough, then one FlushFileBuffers on a single file. The flush
    /// command reaches the whole device cache, so every write-through write
    /// is durable even without FUA: one device flush per round. Windows only.
    WriteThroughFlushOne,
    /// WriteThroughUnbuffered + one FlushFileBuffers. Windows only.
    WriteThroughUnbufferedFlushOne,
}

const ALL_METHODS: [Method; 6] = [
    Method::SeqFlush,
    Method::ParFlush,
    Method::WriteThrough,
    Method::WriteThroughUnbuffered,
    Method::WriteThroughFlushOne,
    Method::WriteThroughUnbufferedFlushOne,
];

impl Method {
    fn name(self) -> &'static str {
        match self {
            Method::SeqFlush => "seq_flush",
            Method::ParFlush => "par_flush",
            Method::WriteThrough => "write_through",
            Method::WriteThroughUnbuffered => "write_through_unbuffered",
            Method::WriteThroughFlushOne => "write_through+flush1",
            Method::WriteThroughUnbufferedFlushOne => "write_through_unbuffered+flush1",
        }
    }

    fn parse(s: &str) -> Option<Method> {
        ALL_METHODS.into_iter().find(|m| m.name() == s)
    }

    fn available(self) -> bool {
        match self {
            Method::SeqFlush | Method::ParFlush => true,
            _ => cfg!(windows),
        }
    }

    fn flush_one(self) -> bool {
        matches!(
            self,
            Method::WriteThroughFlushOne | Method::WriteThroughUnbufferedFlushOne
        )
    }

    fn uses_threads(self) -> bool {
        self != Method::SeqFlush
    }

    fn buffered(self) -> bool {
        matches!(self, Method::SeqFlush | Method::ParFlush)
    }
}

struct Config {
    dir: PathBuf,
    levels: Vec<usize>,
    threads: Vec<usize>,
    rounds: usize,
    budget: Duration,
    methods: Vec<Method>,
}

fn parse_list(s: &str) -> Vec<usize> {
    s.split(',').filter_map(|x| x.trim().parse().ok()).collect()
}

fn parse_args() -> Result<Config, String> {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(args.next().ok_or("missing <dir>")?);
    let mut cfg = Config {
        dir,
        levels: vec![10, 100, 1000],
        threads: vec![1, 8, 32],
        rounds: 10,
        budget: Duration::from_secs(30),
        methods: ALL_METHODS.to_vec(),
    };
    while let Some(flag) = args.next() {
        let value = args.next().ok_or(format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--levels" => cfg.levels = parse_list(&value),
            "--threads" => cfg.threads = parse_list(&value),
            "--rounds" => cfg.rounds = value.parse().map_err(|_| "bad --rounds")?,
            "--budget-secs" => {
                cfg.budget = Duration::from_secs(value.parse().map_err(|_| "bad --budget-secs")?)
            }
            "--methods" if value != "all" => {
                cfg.methods = value
                    .split(',')
                    .map(|m| Method::parse(m.trim()).ok_or(format!("unknown method {m}")))
                    .collect::<Result<_, _>>()?
            }
            "--methods" => {}
            other => return Err(format!("unknown flag {other}")),
        }
    }
    cfg.rounds = cfg.rounds.clamp(1, MAX_ROUNDS - 1);
    cfg.methods.retain(|m| m.available());
    Ok(cfg)
}

#[cfg(windows)]
fn open_wal(path: &Path, method: Method) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    let flags = match method {
        Method::WriteThrough | Method::WriteThroughFlushOne => FILE_FLAG_WRITE_THROUGH,
        Method::WriteThroughUnbuffered | Method::WriteThroughUnbufferedFlushOne => {
            FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING
        }
        _ => 0,
    };
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(flags)
        .open(path)
}

#[cfg(not(windows))]
fn open_wal(path: &Path, _method: Method) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

/// Creates N preallocated files, made durable before measuring so that file
/// creation and size changes are not part of any round.
fn prepare_files(dir: &Path, n: usize) -> std::io::Result<Vec<PathBuf>> {
    fs::create_dir_all(dir)?;
    let mut paths = Vec::with_capacity(n);
    for i in 0..n {
        let path = dir.join(format!("tenant-{i:06}.wal"));
        let f = File::create(&path)?;
        f.set_len(FILE_SIZE)?;
        // Write the whole file once so the valid data length covers it and
        // later writes are plain overwrites (as with a reused pool file).
        let mut f = f;
        f.write_all(&vec![0u8; FILE_SIZE as usize])?;
        f.sync_all()?;
        paths.push(path);
    }
    Ok(paths)
}

/// A 4 KiB-aligned record buffer without unsafe code: over-allocate and slice.
struct AlignedRecord {
    storage: Vec<u8>,
    offset: usize,
}

impl AlignedRecord {
    fn new(fill: u8) -> Self {
        let mut storage = vec![fill; RECORD * 2];
        let offset = storage.as_ptr().align_offset(RECORD);
        storage[offset..offset + RECORD].fill(fill);
        AlignedRecord { storage, offset }
    }

    fn bytes(&self) -> &[u8] {
        &self.storage[self.offset..self.offset + RECORD]
    }
}

fn append(file: &mut File, round: usize, record: &[u8]) -> std::io::Result<()> {
    file.seek(SeekFrom::Start((round * RECORD) as u64))?;
    file.write_all(record)
}

/// Runs `work` over `files`, split into `threads` contiguous chunks.
fn in_parallel<F>(files: &mut [File], threads: usize, work: F) -> std::io::Result<()>
where
    F: Fn(&mut File) -> std::io::Result<()> + Sync,
{
    let chunk = files.len().div_ceil(threads.max(1)).max(1);
    std::thread::scope(|s| {
        let handles: Vec<_> = files
            .chunks_mut(chunk)
            .map(|part| {
                let work = &work;
                s.spawn(move || part.iter_mut().try_for_each(work))
            })
            .collect();
        handles
            .into_iter()
            .try_for_each(|h| h.join().expect("worker panicked"))
    })
}

struct Measurement {
    round_ms: Vec<f64>,
    write_ms: Vec<f64>,
}

fn run_config(
    paths: &[PathBuf],
    method: Method,
    threads: usize,
    cfg: &Config,
) -> std::io::Result<Measurement> {
    let mut files: Vec<File> = paths
        .iter()
        .map(|p| open_wal(p, method))
        .collect::<Result<_, _>>()?;
    let record = AlignedRecord::new(0xA5);
    let record = record.bytes();
    let mut m = Measurement {
        round_ms: Vec::new(),
        write_ms: Vec::new(),
    };
    let started = Instant::now();
    // Round 0 is a warm-up and is not recorded.
    for round in 0..=cfg.rounds {
        let (write_ms, round_ms) = match method {
            Method::SeqFlush | Method::ParFlush => {
                let t0 = Instant::now();
                for f in files.iter_mut() {
                    append(f, round, record)?;
                }
                let write_ms = ms(t0.elapsed());
                let t1 = Instant::now();
                if method == Method::SeqFlush {
                    files.iter_mut().try_for_each(|f| f.sync_data())?;
                } else {
                    in_parallel(&mut files, threads, |f| f.sync_data())?;
                }
                (write_ms, ms(t1.elapsed()))
            }
            _ => {
                let t0 = Instant::now();
                in_parallel(&mut files, threads, |f| append(f, round, record))?;
                if method.flush_one() {
                    files[0].sync_data()?;
                }
                let elapsed = ms(t0.elapsed());
                (elapsed, elapsed)
            }
        };
        if round > 0 {
            m.write_ms.push(write_ms);
            m.round_ms.push(round_ms);
            if started.elapsed() > cfg.budget && m.round_ms.len() >= 3 {
                break;
            }
        }
    }
    Ok(m)
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn main() {
    let cfg = match parse_args() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!(
                "usage: fsync-rounds <dir> [--levels 10,100,1000] [--threads 1,8,32] \
                 [--rounds 10] [--budget-secs 30] [--methods all|seq_flush,par_flush,...]"
            );
            std::process::exit(2);
        }
    };
    println!(
        "| method | threads | dirty tenants | rounds | round median ms | round p90 ms | round max ms | µs per tenant (median) | write() median ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for &n in &cfg.levels {
        let dir = cfg.dir.join(format!("level-{n}"));
        let paths = match prepare_files(&dir, n) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("error preparing {n} files in {}: {e}", dir.display());
                std::process::exit(1);
            }
        };
        for &method in &cfg.methods {
            let thread_counts: Vec<usize> = if method.uses_threads() {
                cfg.threads.clone()
            } else {
                vec![1]
            };
            for threads in thread_counts {
                match run_config(&paths, method, threads, &cfg) {
                    Ok(m) => {
                        let mut r = m.round_ms.clone();
                        r.sort_by(f64::total_cmp);
                        let mut w = m.write_ms.clone();
                        w.sort_by(f64::total_cmp);
                        let median = percentile(&r, 0.5);
                        let write_col = if method.buffered() {
                            format!("{:.2}", percentile(&w, 0.5))
                        } else {
                            "(in round)".to_string()
                        };
                        println!(
                            "| {} | {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.1} | {} |",
                            method.name(),
                            threads,
                            n,
                            r.len(),
                            median,
                            percentile(&r, 0.9),
                            r[r.len() - 1],
                            median * 1000.0 / n as f64,
                            write_col
                        );
                    }
                    Err(e) => println!(
                        "| {} | {} | {} | error: {} | | | | | |",
                        method.name(),
                        threads,
                        n,
                        e
                    ),
                }
            }
        }
        if let Err(e) = fs::remove_dir_all(&dir) {
            eprintln!("warning: could not remove {}: {e}", dir.display());
        }
    }
}
