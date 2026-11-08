//! Crash tests: a child process aborts at a failpoint, the parent reopens
//! the store and checks that every synced record survived and nothing is
//! corrupt.
//!
//! Run with `cargo test --features failpoints --test crash`.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::process::Command;

use usiem_evstore::{Options, Query, Record, Store, SyncPolicy, Value};

const POINTS: [&str; 5] = [
    "wal-append-mid",
    "segment-write-mid",
    "flush-before-commit",
    "manifest-write-mid",
    "flush-after-commit",
];

fn opts() -> Options {
    Options {
        sync: SyncPolicy::Always,
        chunk_rows: 4,
        ..Options::default()
    }
}

fn rec(i: i64) -> Record {
    Record::new(i)
        .with("i", Value::I64(i))
        .with("msg", Value::Str(format!("event {i}")))
}

/// Child side: does nothing unless spawned by `crash_at_every_failpoint`.
#[test]
fn crash_child() {
    let (Ok(dir), Ok(point)) = (std::env::var("CRASH_DIR"), std::env::var("CRASH_POINT")) else {
        return;
    };
    let mut store = Store::open(&dir, opts()).unwrap();
    store.append((0..5).map(rec).collect()).unwrap();
    store.flush().unwrap();
    let ack = store.append((5..10).map(rec).collect()).unwrap();
    assert!(ack.synced);
    // Arm the failpoint only now, so setup cannot trip it.
    // SAFETY: this test process is single-purpose; no other thread reads
    // the environment concurrently.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("EVSTORE_CRASH_AT", &point);
    }
    if point == "wal-append-mid" {
        let _ = store.append((10..20).map(rec).collect());
    } else {
        let _ = store.flush();
    }
    panic!("failpoint {point} did not fire");
}

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("evstore-crash-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).expect("create temp dir");
    p
}

fn run_child(dir: &Path, point: &str) {
    let status = Command::new(std::env::current_exe().expect("test binary path"))
        .args(["crash_child", "--exact", "--nocapture", "--test-threads=1"])
        .env("CRASH_DIR", dir)
        .env("CRASH_POINT", point)
        .env_remove("EVSTORE_CRASH_AT")
        .status()
        .expect("spawn crash child");
    assert!(!status.success(), "child for {point} exited cleanly");
}

#[test]
fn crash_at_every_failpoint() {
    for point in POINTS {
        let dir = temp_dir(point);
        run_child(&dir, point);

        let mut store =
            Store::open(&dir, opts()).unwrap_or_else(|e| panic!("{point}: reopen failed: {e}"));
        let ts: Vec<i64> = store
            .search(&Query::default())
            .unwrap()
            .map(|r| r.unwrap().ts)
            .collect();
        assert_eq!(
            ts,
            (0..10).collect::<Vec<_>>(),
            "{point}: synced records lost or duplicated"
        );
        let recovery = store.recovery();
        match point {
            "wal-append-mid" => assert!(recovery.wal_truncated_bytes > 0, "{point}: {recovery:?}"),
            "manifest-write-mid" => assert_eq!(recovery.manifests_skipped, 1, "{point}"),
            "segment-write-mid" | "flush-before-commit" | "flush-after-commit" => {
                assert!(recovery.orphans_removed >= 1, "{point}: {recovery:?}");
            }
            _ => unreachable!(),
        }

        // The store keeps working after recovery.
        store.append((10..12).map(rec).collect()).unwrap();
        store.flush().unwrap();
        drop(store);
        let store = Store::open(&dir, opts()).unwrap();
        assert_eq!(store.len(), 12, "{point}");
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
