//! Arbitrary bytes as a manifest body (the checksum is added by the entry
//! point): decoding, field registry included, never panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = usiem_evstore::fuzz_api::manifest(data);
});
