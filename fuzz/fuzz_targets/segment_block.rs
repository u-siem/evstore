//! Arbitrary bytes as a decompressed segment block (dictionary, timestamps,
//! column or postings, picked by the first byte): decoding never panics or
//! over-allocates.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = usiem_evstore::fuzz_api::segment_block(data);
});
