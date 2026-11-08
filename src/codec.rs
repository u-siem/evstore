//! Little-endian binary helpers and the record codec shared by the WAL,
//! segments and the manifest.

use crate::model::{Record, Value, ValueType};

/// Decoding failure; callers turn it into [`StoreError::Corrupt`](crate::StoreError).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecodeError(pub &'static str);

pub(crate) type DResult<T> = Result<T, DecodeError>;

/// Bounds-checked cursor over a byte slice.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    /// Bytes left to read.
    pub(crate) fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub(crate) fn bytes(&mut self, n: usize) -> DResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(DecodeError("length overflow"))?;
        let out = self
            .buf
            .get(self.pos..end)
            .ok_or(DecodeError("unexpected end of data"))?;
        self.pos = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> DResult<[u8; N]> {
        let b = self.bytes(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(b);
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> DResult<u8> {
        Ok(self.array::<1>()?[0])
    }
    pub(crate) fn u16(&mut self) -> DResult<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    pub(crate) fn u32(&mut self) -> DResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub(crate) fn u64(&mut self) -> DResult<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub(crate) fn i64(&mut self) -> DResult<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }
    pub(crate) fn u128(&mut self) -> DResult<u128> {
        Ok(u128::from_le_bytes(self.array()?))
    }
    pub(crate) fn f64(&mut self) -> DResult<f64> {
        Ok(f64::from_le_bytes(self.array()?))
    }

    pub(crate) fn uvarint(&mut self) -> DResult<u64> {
        let mut out = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                if shift == 63 && b > 1 {
                    return Err(DecodeError("varint overflow"));
                }
                return Ok(out);
            }
        }
        Err(DecodeError("varint too long"))
    }

    pub(crate) fn ivarint(&mut self) -> DResult<i64> {
        let z = self.uvarint()?;
        Ok((z >> 1).cast_signed() ^ -(z & 1).cast_signed())
    }

    /// A `uvarint` used as a length or count, bounded by the remaining input
    /// (each counted item takes at least `min_item` bytes).
    pub(crate) fn len(&mut self, min_item: usize) -> DResult<usize> {
        let n = usize::try_from(self.uvarint()?).map_err(|_| DecodeError("length overflow"))?;
        let remaining = self.buf.len().saturating_sub(self.pos);
        if n.saturating_mul(min_item.max(1)) > remaining && min_item > 0 {
            return Err(DecodeError("length exceeds data"));
        }
        Ok(n)
    }

    pub(crate) fn lp_bytes(&mut self) -> DResult<&'a [u8]> {
        let n = self.len(1)?;
        self.bytes(n)
    }

    pub(crate) fn str(&mut self) -> DResult<&'a str> {
        std::str::from_utf8(self.lp_bytes()?).map_err(|_| DecodeError("invalid UTF-8"))
    }
}

pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v.to_le_bytes()[0] | 0x80);
        v >>= 7;
    }
    out.push(v.to_le_bytes()[0]);
}

pub(crate) fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)).cast_unsigned());
}

pub(crate) fn put_len(out: &mut Vec<u8>, n: usize) {
    put_uvarint(out, n as u64);
}

pub(crate) fn put_lp(out: &mut Vec<u8>, bytes: &[u8]) {
    put_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

/// Encodes a value body (no tag).
pub(crate) fn put_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Str(s) => put_lp(out, s.as_bytes()),
        Value::Bytes(b) => put_lp(out, b),
        Value::I64(x) | Value::Time(x) => put_ivarint(out, *x),
        Value::U64(x) => put_uvarint(out, *x),
        Value::F64(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Bool(x) => out.push(u8::from(*x)),
        Value::Ip(x) => out.extend_from_slice(&x.to_le_bytes()),
    }
}

/// Decodes a value body of type `ty`.
pub(crate) fn get_value(r: &mut Reader<'_>, ty: ValueType) -> DResult<Value> {
    Ok(match ty {
        ValueType::Str => Value::Str(r.str()?.to_owned()),
        ValueType::Bytes => Value::Bytes(r.lp_bytes()?.to_vec()),
        ValueType::I64 => Value::I64(r.ivarint()?),
        ValueType::Time => Value::Time(r.ivarint()?),
        ValueType::U64 => Value::U64(r.uvarint()?),
        ValueType::F64 => Value::F64(r.f64()?),
        ValueType::Bool => match r.u8()? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(DecodeError("invalid bool")),
        },
        ValueType::Ip => Value::Ip(r.u128()?),
    })
}

/// Encodes a tagged value.
pub(crate) fn put_tagged(out: &mut Vec<u8>, v: &Value) {
    out.push(v.value_type() as u8);
    put_value(out, v);
}

/// Decodes a tagged value.
pub(crate) fn get_tagged(r: &mut Reader<'_>) -> DResult<Value> {
    let ty = ValueType::from_tag(r.u8()?).ok_or(DecodeError("unknown value tag"))?;
    get_value(r, ty)
}

/// Encodes a named field (`name`, tagged value).
pub(crate) fn put_field(out: &mut Vec<u8>, name: &str, v: &Value) {
    put_lp(out, name.as_bytes());
    put_tagged(out, v);
}

/// Decodes a named field.
pub(crate) fn get_field(r: &mut Reader<'_>) -> DResult<(String, Value)> {
    let name = r.str()?.to_owned();
    Ok((name, get_tagged(r)?))
}

/// Encodes a whole record (self-describing: field names inline).
pub(crate) fn put_record(out: &mut Vec<u8>, rec: &Record) {
    put_ivarint(out, rec.ts);
    put_len(out, rec.fields.len());
    for (name, v) in &rec.fields {
        put_field(out, name, v);
    }
}

/// Decodes a record written by [`put_record`].
pub(crate) fn get_record(r: &mut Reader<'_>) -> DResult<Record> {
    let ts = r.ivarint()?;
    let n = r.len(3)?;
    let mut fields = Vec::with_capacity(n);
    for _ in 0..n {
        fields.push(get_field(r)?);
    }
    Ok(Record { ts, fields })
}

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample() -> Record {
        Record::new(-1_700_000_000_123)
            .with("message", Value::Str("héllo wörld".into()))
            .with("n", Value::I64(i64::MIN))
            .with("u", Value::U64(u64::MAX))
            .with("f", Value::F64(-0.5))
            .with("b", Value::Bool(true))
            .with("raw", Value::Bytes(vec![0, 1, 255]))
            .with("t", Value::Time(i64::MAX))
            .with("ip", Value::Ip(u128::MAX))
            .with("n", Value::I64(7))
    }

    #[test]
    fn varints() {
        for v in [0u64, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let mut out = Vec::new();
            put_uvarint(&mut out, v);
            assert_eq!(Reader::new(&out).uvarint().unwrap(), v);
        }
        for v in [0i64, -1, 1, i64::MIN, i64::MAX, -300] {
            let mut out = Vec::new();
            put_ivarint(&mut out, v);
            assert_eq!(Reader::new(&out).ivarint().unwrap(), v);
        }
        assert!(Reader::new(&[0xff; 11]).uvarint().is_err());
    }

    #[test]
    fn record_round_trip() {
        let rec = sample();
        let mut out = Vec::new();
        put_record(&mut out, &rec);
        let mut r = Reader::new(&out);
        assert_eq!(get_record(&mut r).unwrap(), rec);
        assert!(r.is_empty());
        // Every truncation fails cleanly.
        for cut in 0..out.len() {
            assert!(
                get_record(&mut Reader::new(&out[..cut])).is_err(),
                "cut {cut}"
            );
        }
    }
}
