//! Entry points for the fuzz targets in `fuzz/`. Not a stable API: hidden
//! from the docs and free to change in any release.
//!
//! Each function decodes untrusted bytes the way the store decodes its own
//! files, and must never panic, abort or allocate beyond what the input can
//! justify, whatever the bytes.

use std::path::Path;
use std::sync::Arc;

use crate::codec::{Reader, crc32};
use crate::directory::Manifest;
use crate::model::ValueType;
use crate::query::{ANY_FIELD, FieldOp, Filter, Query};
use crate::segment::{
    Segment, SegmentBytes, decode_column, decode_dict, decode_timestamps, parse_footer,
};
use crate::{AggQuery, GroupBy, Metric, Value};

/// Replays a whole WAL file (header and frames); returns the records
/// recovered and the length of the valid prefix.
pub fn wal_file(data: &[u8]) -> (usize, usize) {
    let (records, valid) = crate::wal::replay_frames(data);
    (records.len(), valid)
}

/// Decodes one WAL frame payload (a record batch), skipping the frame CRC.
pub fn wal_payload(data: &[u8]) -> Option<usize> {
    crate::wal::decode_batch(&mut Reader::new(data)).map(|b| b.len())
}

/// Decodes a manifest body; its CRC is appended here so that the fuzzer
/// reaches the structure behind it.
pub fn manifest(body: &[u8]) -> bool {
    let mut data = body.to_vec();
    data.extend_from_slice(&crc32(body).to_le_bytes());
    Manifest::decode(&data, 2000).is_ok()
}

/// Parses a segment footer (format version 1 or 2, by the low bit of
/// `version`).
pub fn segment_footer(footer: &[u8], version: u8) -> bool {
    parse_footer(footer, if version & 1 == 0 { 2 } else { 1 }).is_ok()
}

/// Decodes one decompressed segment block. The first byte picks the
/// decoder (dictionary, timestamps, column of a type, postings), the next
/// two give a row count; the rest is the block.
pub fn segment_block(data: &[u8]) -> bool {
    let [kind, a, b, raw @ ..] = data else {
        return false;
    };
    let rows = u32::from(u16::from_le_bytes([*a, *b]));
    match kind % 4 {
        0 => decode_dict(raw).is_ok(),
        1 => decode_timestamps(raw, rows).is_ok(),
        2 => {
            let ty = ValueType::from_tag(kind / 4 % 8 + 1).unwrap_or(ValueType::Str);
            let dict = (kind & 0x80 != 0).then_some(usize::from(*a));
            decode_column(raw, rows, ty, dict).is_ok()
        }
        _ => crate::index::decode_postings(raw, usize::from(*a)).is_ok(),
    }
}

/// Opens a whole segment file from memory, decodes every block and runs a
/// few queries and aggregations over it.
pub fn segment_file(data: Vec<u8>) {
    let Ok(seg) = Segment::from_bytes(Path::new("fuzz.evs"), SegmentBytes::Owned(data)) else {
        return;
    };
    for c in 0..seg.chunks.len() {
        let _ = seg.timestamps(c);
        for col in 0..seg.chunks[c].columns.len() {
            let _ = seg.column(c, col);
        }
    }
    for f in 0..seg.fields.len() {
        let f = u32::try_from(f).expect("footer caps fields at u32");
        let _ = seg.field_dict(f);
        let _ = seg.field_postings(f);
    }
    let segs = [Arc::new(seg)];
    let mut filters = vec![
        Filter::True,
        Filter::field(ANY_FIELD, FieldOp::Contains("a".into())),
    ];
    for (name, _) in &segs[0].fields {
        filters.push(Filter::field(name.clone(), FieldOp::Exists));
        filters.push(Filter::field(
            name.clone(),
            FieldOp::Eq(Value::Str("a".into())),
        ));
        filters.push(Filter::field(name.clone(), FieldOp::Eq(Value::I64(1))));
    }
    for filter in filters {
        let q = Query {
            filter: filter.clone(),
            ..Query::default()
        };
        crate::query::execute(&segs, &[], &q)
            .take(10_000)
            .for_each(drop);
        let group_by = segs[0]
            .fields
            .first()
            .map(|(n, _)| vec![GroupBy::Field(n.clone())])
            .unwrap_or_default();
        let agg = AggQuery {
            filter,
            group_by,
            metrics: vec![Metric::Count],
            ..AggQuery::default()
        };
        let _ = crate::aggregate::aggregate(&segs, &[], &agg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use crate::segment::{segment_file_name, write_segment};
    use crate::test_util::{Rng, TempDir};

    #[test]
    fn entry_points_survive_random_bytes() {
        let mut rng = Rng::new(0xf022);
        for _ in 0..20_000 {
            let len = rng.below(64) as usize;
            let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            wal_file(&data);
            wal_payload(&data);
            manifest(&data);
            segment_footer(&data, data.first().copied().unwrap_or(0));
            segment_block(&data);
        }
    }

    #[test]
    fn segment_file_reads_a_real_segment_and_its_truncations() {
        let dir = TempDir::new("fuzz-api");
        let path = dir.path().join(segment_file_name(1));
        let rows: Vec<Record> = (0..300)
            .map(|i| {
                Record::new(i)
                    .with("host", Value::Str(format!("h{}", i % 7)))
                    .with("n", Value::I64(i))
            })
            .collect();
        write_segment(&path, &rows.iter().collect::<Vec<_>>(), 3, 64).unwrap();
        let data = std::fs::read(&path).unwrap();
        segment_file(data.clone());
        for cut in (0..data.len()).step_by(7) {
            segment_file(data[..cut].to_vec());
        }
    }
}
