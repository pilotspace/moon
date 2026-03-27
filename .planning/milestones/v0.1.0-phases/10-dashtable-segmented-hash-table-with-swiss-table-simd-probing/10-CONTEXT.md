# Phase 10: DashTable Segmented Hash Table with Swiss Table SIMD Probing - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace `std::collections::HashMap<Bytes, Entry>` in Database with a custom segmented hash table combining Dragonfly's DashTable macro-architecture (directory → segments → buckets with per-segment rehashing and zero memory spike) and hashbrown's Swiss Table SIMD micro-optimization (16-byte control groups with `_mm_cmpeq_epi8` parallel fingerprint comparison). This is a prerequisite for forkless persistence (Phase 14) and thread-per-core sharding (Phase 11).

</domain>

<decisions>
## Implementation Decisions

### Segment architecture
- [auto] Directory (array of pointers) → Segments → Buckets hierarchy
- Each segment: 56 regular buckets + 4 stash buckets (overflow), matching Dragonfly's Dash paper layout
- Routing: `hash(key)` → segment ID (high bits) → 2 home buckets within segment (mid bits)
- Overflow to stash buckets when home buckets full; stash checked on every lookup (only 4 buckets, cheap)

### Per-segment rehashing
- [auto] When a segment fills beyond load threshold, only that segment splits into two — NOT the entire table
- Directory doubles when needed (array of pointers, cheap to copy)
- Eliminates Redis's dual-table memory spike entirely — incremental growth with bounded memory overhead
- Split operation: iterate segment entries, rehash into two new segments based on one additional hash bit

### Swiss Table SIMD probing within segments
- [auto] Each bucket group has 16 1-byte control bytes: 7-bit H2 hash fingerprint + 1-bit full/empty flag
- SIMD comparison: `_mm_cmpeq_epi8` on x86-64 compares 16 control bytes simultaneously against lookup key's H2
- Bitmask extraction + `trailing_zeros()` finds first match position — ~6ns per probed group
- Fallback for non-SIMD platforms: scalar 8-byte comparison (still faster than chaining)

### Hash function
- [auto] Use `xxhash64` (via `xxhash-rust` crate) for key hashing — faster than SipHash for non-adversarial workloads
- Split hash into: H1 (segment routing + bucket selection) and H2 (7-bit fingerprint for control bytes)
- For Redis Cluster compatibility: CRC16 hash for slot assignment, xxhash64 for internal table routing

### Entry storage
- [auto] Store entries inline within buckets (no pointer chaining) — entries from Phase 9's CompactEntry (24 bytes)
- Each bucket holds one key-value entry + 1 control byte — 56 buckets × 25 bytes = ~1.4KB per segment
- Key stored as pointer to Bytes (8 bytes) + 24-byte CompactEntry = 32 bytes per slot

### API compatibility
- [auto] DashTable exposes HashMap-like API: `get(&key)`, `get_mut(&key)`, `insert(key, value)`, `remove(&key)`
- Implement `Iterator` for SCAN support — iterate segments sequentially for cursor stability
- Database struct swap is a drop-in replacement — command handlers don't change

### Claude's Discretion
- Exact segment size (56+4 is Dash paper default, may tune based on cache line fit)
- Whether to use `core::arch::x86_64` intrinsics directly or abstract SIMD via a helper trait
- Load factor threshold for segment split (87.5% per Swiss Table, may differ for segments)
- Whether stash buckets use the same SIMD control byte scheme or simpler linear scan

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Hash table design: merging DashTable with Swiss Table — Full DashTable + Swiss Table hybrid design spec
- `.planning/architect-blue-print.md` §Persistence without fork — DashTable segments enable compartmentalized snapshots (Phase 14 dependency)

### Research papers (referenced in blueprint)
- "Dash: Scalable Hashing on Persistent Memory" — segment architecture, stash buckets, per-segment rehashing
- hashbrown (Rust Swiss Table) — SIMD control byte probing implementation reference

### Current implementation
- `src/storage/db.rs` — Database struct using HashMap<Bytes, Entry> (773 lines) — primary replacement target
- `src/storage/entry.rs` — Entry struct (417 lines) — will use Phase 9's CompactEntry
- `src/command/mod.rs` — Command dispatch (421 lines) — uses Database API, must not change
- `src/storage/eviction.rs` — Eviction (437 lines) — iterates storage, must work with new table

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `hashbrown` is already in Rust's std HashMap — its SIMD probing logic is the reference implementation
- `xxhash-rust` crate available for fast hashing
- `ordered-float` already a dependency — sorted set BTreeMap keys work with any hash table

### Established Patterns
- `Database` wraps HashMap and tracks `used_memory` — same pattern with DashTable
- `db.data.get()` / `db.data.get_mut()` / `db.data.insert()` — DashTable must match this API
- SCAN iteration sorts keys — DashTable iterator must yield entries in a deterministic order for cursor stability

### Integration Points
- `Database::get()` and `Database::get_mut()` with lazy expiration — wrap DashTable lookups
- `Database::set()` with version increment and memory tracking — wrap DashTable inserts
- `try_evict_if_needed()` in eviction.rs — random sampling from DashTable (need random bucket access)
- `rdb.rs` serialization — iterate all entries for snapshot

</code_context>

<specifics>
## Specific Ideas

- Blueprint claims 6-16 bytes overhead per item vs Redis's 16-32 bytes — target 40% memory reduction from table structure alone
- DashTable segments are the foundation for Phase 14 forkless persistence — each segment can be independently snapshot-serialized
- Segment-level operations enable Phase 11 thread-per-core: each shard owns segments, no cross-shard table access

</specifics>

<deferred>
## Deferred Ideas

- Concurrent DashTable (lock-free per-segment access) — Phase 11 handles sharding differently (per-shard tables)
- Persistent memory support (Dash paper's primary use case) — not applicable for in-memory store

</deferred>

---

*Phase: 10-dashtable-segmented-hash-table-with-swiss-table-simd-probing*
*Context gathered: 2026-03-24*
