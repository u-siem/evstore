//! Dynamic field registry (design §4.3, MVP form).
//!
//! Every distinct `(name, type)` pair seen at ingest gets a stable
//! [`FieldId`]. The first type seen for a name keeps the bare name as its
//! storage name; a later, conflicting type becomes a new identity named
//! `name#<type>` (for example `port#str`). Queries by name fan out to every
//! identity of the name, and records are always returned with the original
//! name, so the suffix never leaks to callers.
//!
//! The registry is capped (`Options::max_fields`, default 2,000 identities).
//! Values of fields first seen past the cap go to the reserved
//! [`OVERFLOW_FIELD`], which only a scan can search.

use std::collections::HashMap;

use crate::codec::{DResult, DecodeError, Reader, put_len, put_lp};
use crate::model::ValueType;

/// Stable id of a typed field identity.
pub type FieldId = u32;

/// Reserved field holding values of fields past the registry cap.
pub const OVERFLOW_FIELD: &str = "@overflow";

/// How a field is indexed. In the MVP every field gets zone maps and
/// strings are dictionary-coded; the mode records the intent for the index
/// work that follows (term dictionaries, text scans).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexMode {
    /// Exact terms: dictionary codes, later a term index.
    Term = 1,
    /// Free text searched by scanning (`message`, `@raw`).
    TextScan = 2,
    /// High-cardinality numbers and times: zone maps only.
    ZoneMapOnly = 3,
}

impl IndexMode {
    fn from_tag(tag: u8) -> Option<IndexMode> {
        match tag {
            1 => Some(IndexMode::Term),
            2 => Some(IndexMode::TextScan),
            3 => Some(IndexMode::ZoneMapOnly),
            _ => None,
        }
    }

    /// Default rule: text fields are scanned, numbers and times only get
    /// zone maps, everything else is a term field.
    pub fn for_field(name: &str, ty: ValueType) -> IndexMode {
        const TEXT_FIELDS: [&str; 3] = ["message", "@raw", "msg"];
        if ty == ValueType::Str && TEXT_FIELDS.contains(&name) {
            IndexMode::TextScan
        } else if ty.is_numeric() || ty == ValueType::Time {
            IndexMode::ZoneMapOnly
        } else {
            IndexMode::Term
        }
    }
}

/// One typed identity of a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldInfo {
    /// Field name as callers use it.
    pub name: String,
    /// Value type of this identity.
    pub ty: ValueType,
    /// Index mode chosen when the identity was created.
    pub mode: IndexMode,
    /// Storage name: `name` for the first identity, `name#<type>` after.
    pub storage_name: String,
}

/// Per-store registry of field identities.
#[derive(Debug, Clone)]
pub struct FieldRegistry {
    fields: Vec<FieldInfo>,
    by_name: HashMap<String, Vec<FieldId>>,
    max_fields: usize,
}

impl FieldRegistry {
    /// Empty registry holding at most `max_fields` identities (the overflow
    /// field does not count).
    pub fn new(max_fields: usize) -> FieldRegistry {
        let mut reg = FieldRegistry {
            fields: Vec::new(),
            by_name: HashMap::new(),
            max_fields,
        };
        reg.insert(OVERFLOW_FIELD.to_owned(), ValueType::Bytes, IndexMode::Term);
        reg
    }

    fn insert(&mut self, name: String, ty: ValueType, mode: IndexMode) -> FieldId {
        let id = self.fields.len() as FieldId;
        let ids = self.by_name.entry(name.clone()).or_default();
        let storage_name = if ids.is_empty() {
            name.clone()
        } else {
            format!("{name}#{}", ty.suffix())
        };
        ids.push(id);
        self.fields.push(FieldInfo {
            name,
            ty,
            mode,
            storage_name,
        });
        id
    }

    /// Returns the id of `(name, ty)`, creating it if needed. `None` means
    /// the registry is full and the value belongs in [`OVERFLOW_FIELD`].
    pub fn resolve(&mut self, name: &str, ty: ValueType) -> Option<FieldId> {
        if name == OVERFLOW_FIELD {
            return None;
        }
        if let Some(id) = self.get_id(name, ty) {
            return Some(id);
        }
        if self.fields.len() > self.max_fields {
            return None;
        }
        Some(self.insert(name.to_owned(), ty, IndexMode::for_field(name, ty)))
    }

    /// Looks up an existing identity without creating it.
    pub fn get_id(&self, name: &str, ty: ValueType) -> Option<FieldId> {
        self.ids(name)
            .iter()
            .copied()
            .find(|id| self.fields[*id as usize].ty == ty)
    }

    /// All identities of `name` (empty if unknown).
    pub fn ids(&self, name: &str) -> &[FieldId] {
        self.by_name.get(name).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Details of identity `id`.
    pub fn info(&self, id: FieldId) -> &FieldInfo {
        &self.fields[id as usize]
    }

    /// All identities except the overflow field, in creation order.
    pub fn iter(&self) -> impl Iterator<Item = (FieldId, &FieldInfo)> {
        self.fields
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, f)| (i as FieldId, f))
    }

    /// Number of identities (excluding the overflow field).
    pub fn len(&self) -> usize {
        self.fields.len() - 1
    }

    /// True when no field has been registered yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True once new names go to the overflow field.
    pub fn is_full(&self) -> bool {
        self.fields.len() > self.max_fields
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        put_len(out, self.fields.len() - 1);
        for (_, f) in self.iter() {
            put_lp(out, f.name.as_bytes());
            out.push(f.ty as u8);
            out.push(f.mode as u8);
        }
    }

    pub(crate) fn decode(r: &mut Reader<'_>, max_fields: usize) -> DResult<FieldRegistry> {
        let mut reg = FieldRegistry::new(max_fields);
        let n = r.len(3)?;
        for _ in 0..n {
            let name = r.str()?.to_owned();
            let ty = ValueType::from_tag(r.u8()?).ok_or(DecodeError("unknown field type"))?;
            let mode = IndexMode::from_tag(r.u8()?).ok_or(DecodeError("unknown index mode"))?;
            if name == OVERFLOW_FIELD || reg.get_id(&name, ty).is_some() {
                return Err(DecodeError("duplicate field identity"));
            }
            // Ids are positional; a lowered cap never drops known fields.
            reg.insert(name, ty, mode);
        }
        Ok(reg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_conflict_splits_identity() {
        let mut reg = FieldRegistry::new(10);
        let a = reg.resolve("port", ValueType::I64).unwrap();
        let b = reg.resolve("port", ValueType::Str).unwrap();
        assert_ne!(a, b);
        assert_eq!(reg.resolve("port", ValueType::I64), Some(a));
        assert_eq!(reg.info(a).storage_name, "port");
        assert_eq!(reg.info(b).storage_name, "port#str");
        assert_eq!(reg.ids("port"), &[a, b]);
        assert_eq!(reg.info(a).mode, IndexMode::ZoneMapOnly);
        assert_eq!(reg.info(b).mode, IndexMode::Term);
        assert_eq!(reg.ids("missing"), &[] as &[FieldId]);
    }

    #[test]
    fn cap_sends_new_fields_to_overflow() {
        let mut reg = FieldRegistry::new(2);
        assert!(reg.resolve("a", ValueType::Str).is_some());
        assert!(reg.resolve("b", ValueType::Str).is_some());
        assert!(reg.is_full());
        assert_eq!(reg.resolve("c", ValueType::Str), None);
        // Known identities still resolve.
        assert!(reg.resolve("a", ValueType::Str).is_some());
        assert_eq!(reg.resolve(OVERFLOW_FIELD, ValueType::Bytes), None);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn encode_round_trip() {
        let mut reg = FieldRegistry::new(100);
        reg.resolve("message", ValueType::Str);
        reg.resolve("x", ValueType::U64);
        reg.resolve("x", ValueType::Ip);
        let mut out = Vec::new();
        reg.encode(&mut out);
        let back = FieldRegistry::decode(&mut Reader::new(&out), 100).unwrap();
        assert_eq!(back.fields, reg.fields);
        assert_eq!(back.info(1).mode, IndexMode::TextScan);
    }
}
