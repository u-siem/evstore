//! Record model: what the store accepts and returns.

use std::cmp::Ordering;

/// Type of a [`Value`]. The discriminant is the on-disk tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum ValueType {
    /// UTF-8 string.
    Str = 1,
    /// Signed 64-bit integer.
    I64 = 2,
    /// Unsigned 64-bit integer.
    U64 = 3,
    /// 64-bit float.
    F64 = 4,
    /// Boolean.
    Bool = 5,
    /// Opaque bytes.
    Bytes = 6,
    /// Timestamp, nanoseconds since the Unix epoch (UTC).
    Time = 7,
    /// IP address; IPv4 is stored IPv4-mapped (`::ffff:a.b.c.d`).
    Ip = 8,
}

impl ValueType {
    /// All types, in tag order.
    pub const ALL: [ValueType; 8] = [
        ValueType::Str,
        ValueType::I64,
        ValueType::U64,
        ValueType::F64,
        ValueType::Bool,
        ValueType::Bytes,
        ValueType::Time,
        ValueType::Ip,
    ];

    /// Decodes an on-disk tag.
    pub fn from_tag(tag: u8) -> Option<ValueType> {
        ValueType::ALL
            .get(usize::from(tag).wrapping_sub(1))
            .copied()
    }

    /// Short suffix used to name a typed identity of a field (`name#i64`).
    pub fn suffix(self) -> &'static str {
        match self {
            ValueType::Str => "str",
            ValueType::I64 => "i64",
            ValueType::U64 => "u64",
            ValueType::F64 => "f64",
            ValueType::Bool => "bool",
            ValueType::Bytes => "bytes",
            ValueType::Time => "time",
            ValueType::Ip => "ip",
        }
    }

    pub(crate) fn is_numeric(self) -> bool {
        matches!(self, ValueType::I64 | ValueType::U64 | ValueType::F64)
    }
}

/// A field value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// UTF-8 string.
    Str(String),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// 64-bit float.
    F64(f64),
    /// Boolean.
    Bool(bool),
    /// Opaque bytes.
    Bytes(Vec<u8>),
    /// Timestamp, nanoseconds since the Unix epoch (UTC).
    Time(i64),
    /// IP address as a 128-bit number; IPv4 is IPv4-mapped.
    Ip(u128),
}

impl Value {
    /// Type of this value.
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Str(_) => ValueType::Str,
            Value::I64(_) => ValueType::I64,
            Value::U64(_) => ValueType::U64,
            Value::F64(_) => ValueType::F64,
            Value::Bool(_) => ValueType::Bool,
            Value::Bytes(_) => ValueType::Bytes,
            Value::Time(_) => ValueType::Time,
            Value::Ip(_) => ValueType::Ip,
        }
    }

    /// Builds an IP value from an IPv4 address (IPv4-mapped).
    pub fn ipv4(addr: std::net::Ipv4Addr) -> Value {
        Value::Ip(u128::from(addr.to_ipv6_mapped()))
    }

    /// Builds an IP value from any IP address.
    pub fn ip(addr: std::net::IpAddr) -> Value {
        match addr {
            std::net::IpAddr::V4(v4) => Value::ipv4(v4),
            std::net::IpAddr::V6(v6) => Value::Ip(u128::from(v6)),
        }
    }

    /// Approximate in-memory size in bytes, used for buffer accounting.
    pub(crate) fn approx_size(&self) -> usize {
        match self {
            Value::Str(s) => 24 + s.len(),
            Value::Bytes(b) => 24 + b.len(),
            Value::Ip(_) => 16,
            _ => 8,
        }
    }
}

/// Compares two values the way filters do.
///
/// Values of the same type compare naturally (strings case-insensitively,
/// floats by IEEE rules, so `-0.0 == 0.0` and NaN is incomparable). Numbers
/// of different types (`I64`, `U64`, `F64`) compare numerically. Anything
/// else is incomparable (`None`).
pub(crate) fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    use Value::*;
    Some(match (a, b) {
        (Str(x), Str(y)) => cmp_ci(x, y),
        (I64(x), I64(y)) | (Time(x), Time(y)) => x.cmp(y),
        (U64(x), U64(y)) => x.cmp(y),
        (F64(x), F64(y)) => return x.partial_cmp(y),
        (Bool(x), Bool(y)) => x.cmp(y),
        (Bytes(x), Bytes(y)) => x.cmp(y),
        (Ip(x), Ip(y)) => x.cmp(y),
        (I64(x), U64(y)) => i128::from(*x).cmp(&i128::from(*y)),
        (U64(x), I64(y)) => i128::from(*x).cmp(&i128::from(*y)),
        (I64(x), F64(y)) => return (*x as f64).partial_cmp(y),
        (F64(x), I64(y)) => return x.partial_cmp(&(*y as f64)),
        (U64(x), F64(y)) => return (*x as f64).partial_cmp(y),
        (F64(x), U64(y)) => return x.partial_cmp(&(*y as f64)),
        _ => return None,
    })
}

/// Case-insensitive string comparison (Unicode lowercase).
pub(crate) fn cmp_ci(a: &str, b: &str) -> Ordering {
    if a.is_ascii() && b.is_ascii() {
        let (x, y) = (a.as_bytes(), b.as_bytes());
        for (p, q) in x.iter().zip(y) {
            match p.to_ascii_lowercase().cmp(&q.to_ascii_lowercase()) {
                Ordering::Equal => {}
                other => return other,
            }
        }
        x.len().cmp(&y.len())
    } else {
        a.to_lowercase().cmp(&b.to_lowercase())
    }
}

/// One stored event: a timestamp plus named fields.
///
/// A name may repeat; the field is then multi-valued. Names starting with
/// `@` are free for the application (the store reserves only `@overflow`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Record {
    /// Event time, nanoseconds since the Unix epoch (UTC). Queries order and
    /// filter by it.
    pub ts: i64,
    /// Field name/value pairs.
    pub fields: Vec<(String, Value)>,
}

impl Record {
    /// Creates an empty record at `ts`.
    pub fn new(ts: i64) -> Record {
        Record {
            ts,
            fields: Vec::new(),
        }
    }

    /// Adds a field value (builder style).
    pub fn with(mut self, name: impl Into<String>, value: Value) -> Record {
        self.fields.push((name.into(), value));
        self
    }

    /// Adds a field value.
    pub fn push(&mut self, name: impl Into<String>, value: Value) {
        self.fields.push((name.into(), value));
    }

    /// Iterates the values of field `name`.
    pub fn values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Value> + 'a {
        self.fields
            .iter()
            .filter(move |(n, _)| n == name)
            .map(|(_, v)| v)
    }

    /// First value of field `name`.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub(crate) fn approx_size(&self) -> usize {
        16 + self
            .fields
            .iter()
            .map(|(n, v)| 24 + n.len() + v.approx_size())
            .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_round_trip() {
        for t in ValueType::ALL {
            assert_eq!(ValueType::from_tag(t as u8), Some(t));
        }
        assert_eq!(ValueType::from_tag(0), None);
        assert_eq!(ValueType::from_tag(9), None);
    }

    #[test]
    fn float_compare_is_ieee() {
        assert_eq!(
            compare(&Value::F64(-0.0), &Value::I64(0)),
            Some(Ordering::Equal)
        );
        assert_eq!(compare(&Value::F64(f64::NAN), &Value::F64(1.0)), None);
    }

    #[test]
    fn compare_rules() {
        assert_eq!(
            compare(&Value::Str("ABC".into()), &Value::Str("abc".into())),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare(&Value::I64(-1), &Value::U64(1)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&Value::F64(2.5), &Value::I64(2)),
            Some(Ordering::Greater)
        );
        assert_eq!(compare(&Value::Str("1".into()), &Value::I64(1)), None);
        assert_eq!(cmp_ci("Ärger", "ärger"), Ordering::Equal);
    }
}
