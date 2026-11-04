# Schema catalog

- **Content-addressed, immutable objects** for schema versions, templates, enums, tokenizer configs and dictionaries, identified by BLAKE3-128 of their canonical form. Mutable pointers `name → current version` are used by writers only.
- **Signed release bundles**: verified, then imported idempotently at startup. A rollback only moves pointers back.
- **Segments reference only what they use**: a small `local id → hash` table (~1–3 KB).
- **Never delete catalog objects.** Estimated total over the product's life: hundreds of MB to a few GB. Reference-tracked GC is a possible later addition.
- **Lazy loading** into an LRU of compiled decoders and tokenizers: thousands of schemas cost disk, not RAM.
- **Standalone export**: a tenant can be exported with every referenced object embedded (archives, legal handover).
- **Isolation**: **tenant custom schemas** live in a **private catalog inside the tenant's storage**, and the **dynamic field registry is per tenant** (field names can leak information). Resolution order: private first, then shared.
- **Evolution rules**:
  - stable field ids (renames are aliases);
  - append-only enum codes, never reused;
  - template revisions (old events render with their original text);
  - an incompatible type change ⇒ **a new field identity**, and queries by name fan out to both;
  - dictionaries version independently, and merges re-encode with the current one;
  - **data is never rewritten because a schema changed.**
