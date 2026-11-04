//! Schema catalog.
//!
//! Design: `docs/design/05-schema-catalog.md`.
//!
//! - Content-addressed, immutable objects (schema versions, templates,
//!   enums, tokenizer configs, compression dictionaries) identified by a
//!   hash of their canonical form; never deleted.
//! - Signed release bundles shipped by uSIEM, imported idempotently.
//! - Segments reference the release bundle plus local ids into it.
//! - Per-tenant private catalog and dynamic field registry, stored inside the
//!   tenant's own storage (nothing tenant-specific enters the shared catalog).
//! - Evolution rules: stable field ids and enum codes, template revisions,
//!   new field identity on incompatible changes; data is never rewritten.
