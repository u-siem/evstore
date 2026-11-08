//! Arbitrary bytes as a segment footer (first byte: format version): parsing
//! never panics or over-allocates.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let [version, footer @ ..] = data {
        let _ = usiem_evstore::fuzz_api::segment_footer(footer, *version);
    }
});
