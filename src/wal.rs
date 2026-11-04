//! Per-tenant durability and write buffers.
//!
//! Design: `docs/design/04-durability-and-buffers.md`.
//!
//! - Durability class per tenant: `SealOnly`, `Wal(interval)`, `WalSync`,
//!   `SinkAck`, `SourceCheckpoint`.
//! - A coordinated group sync flushes all dirty tenant WALs in rounds, with
//!   the cheapest mechanism per platform. Windows (measured in
//!   `docs/spikes/01-fsync-rounds-windows.md`): the coordinator issues
//!   buffered write-through writes in parallel, then one `FlushFileBuffers`
//!   per round.
//! - The WAL is the buffer for small and cold tenants; memtables exist only
//!   for hot tenants, under a global memory budget.
//! - Preallocated WAL file pool, crypto-erased before reuse: AES-256-GCM via
//!   `ring` behind an internal trait, fresh key per WAL life.
//! - WAL appends are schema-encoded but uncompressed; a tenant's WAL becomes
//!   a segment at ~8 MiB, at seal, or when the global WAL budget is exceeded.
//!
//! # MVP implementation
//!
//! One WAL file per store, `wal-<gen:016x>.log`:
//!
//! - 16-byte header: magic `EVWL`, format version (`u16`), flags (`u16`; bit
//!   0 is reserved for encryption and must be 0 in the MVP), generation
//!   (`u64`).
//! - Then frames, one per `append` call so a batch is atomic:
//!   `[u32 len][u32 crc32(payload)][payload]`, where the payload is a count
//!   followed by self-describing records (field names inline).
//! - Replay stops at the first short, oversized or bad-checksum frame and
//!   truncates the file there (a torn tail). Frames after a corrupt one are
//!   lost; the store reports how many bytes were dropped.
//! - [`SyncPolicy`] decides when appends are fsynced. The coordinated group
//!   sync, durability classes, the file pool and encryption are deferred.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::codec::{Reader, crc32, get_record, put_len, put_record};
use crate::error::{Result, StoreError};
use crate::model::Record;

const MAGIC: &[u8; 4] = b"EVWL";
const VERSION: u16 = 1;
const HEADER_LEN: u64 = 16;
/// Upper bound for one frame; anything larger is treated as corruption.
const MAX_FRAME: u32 = 256 << 20;

/// When appended records are made durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// fsync after every append.
    Always,
    /// fsync at most once per interval; `Store::sync`, `flush` and
    /// `maintain` always sync.
    Interval(Duration),
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Interval(Duration::from_secs(1))
    }
}

pub(crate) fn wal_file_name(generation: u64) -> String {
    format!("wal-{generation:016x}.log")
}

/// Parses `wal-<gen>.log`.
pub(crate) fn parse_wal_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Outcome of replaying a WAL at open.
#[derive(Debug, Default)]
pub(crate) struct Replay {
    pub records: Vec<Record>,
    /// Bytes cut from a torn or corrupt tail.
    pub truncated_bytes: u64,
}

pub(crate) struct Wal {
    file: File,
    path: PathBuf,
    generation: u64,
    len: u64,
    last_sync: Instant,
    unsynced: bool,
    created: Instant,
}

impl Wal {
    /// Creates a new, empty WAL (fails if it exists) and syncs its header.
    pub fn create(dir: &Path, generation: u64) -> Result<Wal> {
        let path = dir.join(wal_file_name(generation));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        let mut header = Vec::with_capacity(HEADER_LEN as usize);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&generation.to_le_bytes());
        file.write_all(&header)
            .map_err(|e| StoreError::io(&path, e))?;
        file.sync_all().map_err(|e| StoreError::io(&path, e))?;
        Ok(Wal {
            file,
            path,
            generation,
            len: HEADER_LEN,
            last_sync: Instant::now(),
            unsynced: false,
            created: Instant::now(),
        })
    }

    /// Opens an existing WAL, replays its frames and truncates a torn tail.
    pub fn open(dir: &Path, generation: u64) -> Result<(Wal, Replay)> {
        let path = dir.join(wal_file_name(generation));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|e| StoreError::io(&path, e))?;
        if data.len() < HEADER_LEN as usize || &data[..4] != MAGIC {
            return Err(StoreError::corrupt(&path, 0, "bad WAL header"));
        }
        let version = u16::from_le_bytes([data[4], data[5]]);
        let flags = u16::from_le_bytes([data[6], data[7]]);
        let file_gen = u64::from_le_bytes(data[8..16].try_into().expect("8 bytes"));
        if version != VERSION {
            return Err(StoreError::corrupt(
                &path,
                4,
                format!("unsupported WAL version {version}"),
            ));
        }
        if flags != 0 {
            return Err(StoreError::corrupt(
                &path,
                6,
                format!("unsupported WAL flags {flags:#x}"),
            ));
        }
        if file_gen != generation {
            return Err(StoreError::corrupt(
                &path,
                8,
                "WAL generation does not match its name",
            ));
        }

        let mut replay = Replay::default();
        let mut pos = HEADER_LEN as usize;
        while let Some(frame) = next_frame(&data, pos) {
            match decode_batch(&mut Reader::new(frame)) {
                Some(batch) => replay.records.extend(batch),
                None => break,
            }
            pos += 8 + frame.len();
        }
        let valid = pos as u64;
        replay.truncated_bytes = data.len() as u64 - valid;
        if replay.truncated_bytes > 0 {
            file.set_len(valid).map_err(|e| StoreError::io(&path, e))?;
            file.sync_all().map_err(|e| StoreError::io(&path, e))?;
        }
        file.seek(SeekFrom::Start(valid))
            .map_err(|e| StoreError::io(&path, e))?;
        Ok((
            Wal {
                file,
                path,
                generation,
                len: valid,
                last_sync: Instant::now(),
                unsynced: false,
                created: Instant::now(),
            },
            replay,
        ))
    }

    /// Appends one batch as a single frame. Returns whether it is synced.
    pub fn append(&mut self, records: &[Record], policy: SyncPolicy) -> Result<bool> {
        if records.is_empty() {
            return Ok(!self.unsynced);
        }
        let mut payload = Vec::with_capacity(64 * records.len());
        put_len(&mut payload, records.len());
        for rec in records {
            put_record(&mut payload, rec);
        }
        let len = u32::try_from(payload.len())
            .ok()
            .filter(|l| *l <= MAX_FRAME)
            .ok_or_else(|| StoreError::InvalidArgument("append batch exceeds 256 MiB".into()))?;
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&crc32(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        crate::failpoint("wal-append-mid", || {
            let _ = self.file.write_all(&frame[..frame.len() / 2]);
            let _ = self.file.sync_all();
        });
        self.file
            .write_all(&frame)
            .map_err(|e| StoreError::io(&self.path, e))?;
        self.len += frame.len() as u64;
        self.unsynced = true;
        let due = match policy {
            SyncPolicy::Always => true,
            SyncPolicy::Interval(every) => self.last_sync.elapsed() >= every,
        };
        if due {
            self.sync()?;
        }
        Ok(!self.unsynced)
    }

    /// fsyncs pending appends (no-op when nothing is pending).
    pub fn sync(&mut self) -> Result<()> {
        if self.unsynced {
            self.file
                .sync_data()
                .map_err(|e| StoreError::io(&self.path, e))?;
            self.unsynced = false;
        }
        self.last_sync = Instant::now();
        Ok(())
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes of frames (excluding the header).
    pub fn data_len(&self) -> u64 {
        self.len - HEADER_LEN
    }

    /// Time since this WAL was created or opened.
    pub fn age(&self) -> Duration {
        self.created.elapsed()
    }

    #[cfg(test)]
    pub fn has_unsynced(&self) -> bool {
        self.unsynced
    }
}

/// Returns the payload of the frame at `pos`, if complete and valid.
fn next_frame(data: &[u8], pos: usize) -> Option<&[u8]> {
    let head = data.get(pos..pos.checked_add(8)?)?;
    let len = u32::from_le_bytes(head[..4].try_into().ok()?);
    let crc = u32::from_le_bytes(head[4..].try_into().ok()?);
    if len > MAX_FRAME {
        return None;
    }
    let payload = data.get(pos + 8..pos + 8 + len as usize)?;
    (crc32(payload) == crc).then_some(payload)
}

fn decode_batch(r: &mut Reader<'_>) -> Option<Vec<Record>> {
    let n = r.len(2).ok()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(get_record(r).ok()?);
    }
    r.is_empty().then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Value;
    use crate::test_util::TempDir;

    fn rec(i: i64) -> Record {
        Record::new(i)
            .with("i", Value::I64(i))
            .with("s", Value::Str(format!("v{i}")))
    }

    #[test]
    fn names() {
        assert_eq!(wal_file_name(255), "wal-00000000000000ff.log");
        assert_eq!(parse_wal_name("wal-00000000000000ff.log"), Some(255));
        assert_eq!(parse_wal_name("wal-ff.log"), None);
    }

    #[test]
    fn append_and_replay() {
        let dir = TempDir::new("wal-replay");
        let mut wal = Wal::create(dir.path(), 3).unwrap();
        assert!(wal.append(&[rec(1), rec(2)], SyncPolicy::Always).unwrap());
        assert!(
            !wal.append(&[rec(3)], SyncPolicy::Interval(Duration::from_secs(3600)))
                .unwrap()
        );
        assert!(wal.has_unsynced());
        wal.sync().unwrap();
        drop(wal);
        let (wal, replay) = Wal::open(dir.path(), 3).unwrap();
        assert_eq!(replay.records, vec![rec(1), rec(2), rec(3)]);
        assert_eq!(replay.truncated_bytes, 0);
        assert!(wal.data_len() > 0);
        assert!(Wal::open(dir.path(), 4).is_err());
    }

    #[test]
    fn torn_tail_is_truncated_at_every_cut() {
        let dir = TempDir::new("wal-torn");
        let mut wal = Wal::create(dir.path(), 1).unwrap();
        wal.append(&[rec(1)], SyncPolicy::Always).unwrap();
        let first_end = HEADER_LEN + wal.data_len();
        wal.append(&[rec(2), rec(3)], SyncPolicy::Always).unwrap();
        let full_len = HEADER_LEN + wal.data_len();
        drop(wal);
        let path = dir.path().join(wal_file_name(1));
        let full = std::fs::read(&path).unwrap();
        for cut in first_end..full_len {
            std::fs::write(&path, &full[..cut as usize]).unwrap();
            let (wal, replay) = Wal::open(dir.path(), 1).unwrap();
            assert_eq!(replay.records, vec![rec(1)], "cut {cut}");
            assert_eq!(replay.truncated_bytes, cut - first_end);
            assert_eq!(wal.data_len(), first_end - HEADER_LEN);
            drop(wal);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), first_end);
        }
    }

    #[test]
    fn bit_flip_drops_frame_and_rest() {
        let dir = TempDir::new("wal-flip");
        let mut wal = Wal::create(dir.path(), 1).unwrap();
        wal.append(&[rec(1)], SyncPolicy::Always).unwrap();
        let first_end = (HEADER_LEN + wal.data_len()) as usize;
        wal.append(&[rec(2)], SyncPolicy::Always).unwrap();
        wal.append(&[rec(3)], SyncPolicy::Always).unwrap();
        drop(wal);
        let path = dir.path().join(wal_file_name(1));
        let mut data = std::fs::read(&path).unwrap();
        data[first_end + 12] ^= 0x40;
        std::fs::write(&path, &data).unwrap();
        let (mut wal, replay) = Wal::open(dir.path(), 1).unwrap();
        assert_eq!(replay.records, vec![rec(1)]);
        // Appends continue after the truncation point.
        wal.append(&[rec(4)], SyncPolicy::Always).unwrap();
        drop(wal);
        let (_, replay) = Wal::open(dir.path(), 1).unwrap();
        assert_eq!(replay.records, vec![rec(1), rec(4)]);
    }

    #[test]
    fn bad_header_is_corrupt() {
        let dir = TempDir::new("wal-header");
        std::fs::write(dir.path().join(wal_file_name(1)), b"nope").unwrap();
        assert!(matches!(
            Wal::open(dir.path(), 1),
            Err(StoreError::Corrupt { .. })
        ));
    }
}
