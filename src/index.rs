//! Indexes and statistics.
//!
//! Design: `docs/design/08-components.md` (indexes) and
//! `docs/design/09-design-details.md` (text index policy).
//!
//! - FST term dictionaries over order-preserving keys + Roaring postings:
//!   equality, IN, prefix, range and CIDR for every scalar type.
//! - Index mode per field declared by the schema; no indexes on small
//!   segments; opt-in trigram index for `substring` fields.
//! - Zone maps, token blooms, per-chunk code histograms for aggregations.
//!
//! # MVP implementation
//!
//! - **Postings** for dictionary-coded string fields of segments with at
//!   least [`POSTINGS_MIN_ROWS`](crate::segment::POSTINGS_MIN_ROWS) rows:
//!   one Roaring bitmap of segment row numbers per dictionary code, stored
//!   as one block per field (a count, then length-prefixed serialized
//!   bitmaps in code order).
//! - The term dictionary is the field's sorted segment dictionary, so no
//!   FST is needed yet. Numbers, times and IPs rely on zone maps; trigrams
//!   are deferred.
//! - Postings only prune: the query turns them into a candidate row set,
//!   skips chunks without candidates, and still evaluates every remaining
//!   row exactly.

use roaring::RoaringBitmap;

use crate::codec::{DResult, DecodeError, Reader, put_len, put_lp};

/// Serializes one bitmap per dictionary code.
pub(crate) fn encode_postings(bitmaps: &[RoaringBitmap]) -> Vec<u8> {
    let mut out = Vec::new();
    put_len(&mut out, bitmaps.len());
    let mut buf = Vec::new();
    for b in bitmaps {
        buf.clear();
        b.serialize_into(&mut buf).expect("writing to a Vec");
        put_lp(&mut out, &buf);
    }
    out
}

/// Parses what [`encode_postings`] wrote; `codes` is the dictionary size.
pub(crate) fn decode_postings(raw: &[u8], codes: usize) -> DResult<Vec<RoaringBitmap>> {
    let mut r = Reader::new(raw);
    let n = r.len(1)?;
    if n != codes {
        return Err(DecodeError("postings count does not match dictionary"));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let bytes = r.lp_bytes()?;
        out.push(
            RoaringBitmap::deserialize_from(bytes)
                .map_err(|_| DecodeError("bad postings bitmap"))?,
        );
    }
    if !r.is_empty() {
        return Err(DecodeError("trailing bytes in postings"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postings_round_trip() {
        let bitmaps: Vec<RoaringBitmap> = vec![
            [1u32, 5, 70_000].into_iter().collect(),
            RoaringBitmap::new(),
            (0..10_000).collect(),
        ];
        let raw = encode_postings(&bitmaps);
        assert_eq!(decode_postings(&raw, 3).unwrap(), bitmaps);
        assert!(decode_postings(&raw, 2).is_err());
        assert!(decode_postings(&raw[..raw.len() - 1], 3).is_err());
    }
}
