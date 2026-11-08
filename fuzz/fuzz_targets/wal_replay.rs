//! Arbitrary bytes as a WAL file and as one frame payload: replay never
//! panics, and the valid prefix never extends past the input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use usiem_evstore::fuzz_api::{wal_file, wal_payload};

fuzz_target!(|data: &[u8]| {
    let (_, valid) = wal_file(data);
    assert!(valid <= data.len());
    let _ = wal_payload(data);
});
