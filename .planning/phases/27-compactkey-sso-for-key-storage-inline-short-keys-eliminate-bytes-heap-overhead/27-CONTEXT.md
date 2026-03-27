# Phase 27: CompactKey SSO for Key Storage — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace `Bytes` key representation in DashTable with a `CompactKey` struct that inlines short keys (≤23 bytes) to eliminate heap allocations and improve cache locality. Keys >23 bytes use heap with 12-byte inline prefix for fast comparison.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key constraints from deep dive analysis:
- Current key: `Bytes` = 24 bytes struct (ptr:8 + len:8 + Arc header:8) + heap data
- CompactEntry is already 24 bytes (SSO + tagged pointers achieved in Phase 9)
- Gap is entirely in key representation — Bytes adds 24 bytes struct + heap alloc per key
- CompactKey must be same size as Bytes (24 bytes) for zero-regression DashTable layout
- >90% of real Redis keys are ≤23 bytes — inline path dominates
- Requires unsafe Rust for heap path (Box::into_raw, from_raw_parts)
- Must implement: Hash, Eq, PartialEq, Clone, Drop, Debug, AsRef<[u8]>
- DashTable signature changes: `DashTable<Bytes, Entry>` → `DashTable<CompactKey, Entry>`
- All key comparison paths must be updated
- Real win is cache performance — inline keys mean DashTable segment scan reads key data from contiguous memory

**Key files:**
- `src/storage/dashtable/mod.rs` — DashTable generic over key type
- `src/storage/dashtable/segment.rs` — Segment<K,V> stores keys
- `src/storage/db.rs` — Database uses DashTable<Bytes, Entry>
- `src/storage/compact_value.rs` — CompactValue SSO pattern to follow
- All command handlers that create/access keys

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/storage/compact_value.rs` — CompactValue SSO pattern (16 bytes, inline ≤12 bytes)
- `src/storage/dashtable/` — Already generic over K: Hash + Eq
- `src/storage/entry.rs` — CompactEntry (24 bytes)

### Established Patterns
- CompactValue uses `len_and_tag: u32` + `payload: [u8; 12]` for SSO
- Tagged pointers with low 3 bits for type encoding
- `as_bytes()` returns `&[u8]` from either inline or heap path
- Phase 9 established the SSO pattern — follow same unsafe discipline

### Integration Points
- `src/storage/db.rs` — Database::set/get/del all use Bytes keys
- `src/shard/coordinator.rs` — Cross-shard dispatch uses Bytes keys
- `src/persistence/wal.rs` — WAL serialization with key data
- `src/persistence/snapshot.rs` — Snapshot serialization
- `src/persistence/rdb.rs` — RDB serialization

</code_context>

<specifics>
## Specific Ideas

CompactKey layout (24 bytes):
- Inline: `[len:1 | data:23]` (len ≤ 23, high bit clear)
- Heap: `[marker:1 | len_hi:1 | len_lo:2 | pad:4 | ptr:8 | prefix:8]` (high bit set)
- Same total size as Bytes (24 bytes) — DashTable segment layout unchanged

</specifics>

<deferred>
## Deferred Ideas

- Key interning for frequently accessed keys
- Arena-allocated keys for batch operations
- Compact key with embedded hash for skip-hash lookups

</deferred>
