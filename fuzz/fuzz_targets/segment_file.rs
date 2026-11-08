//! Arbitrary bytes as a whole segment file: opening it, decoding every block
//! and running queries and aggregations over it never panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    usiem_evstore::fuzz_api::segment_file(data.to_vec());
});
