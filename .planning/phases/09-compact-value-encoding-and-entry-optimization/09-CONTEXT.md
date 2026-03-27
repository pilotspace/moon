# Phase 9: Compact Value Encoding and Entry Optimization - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Redesign the `Entry` struct and `RedisValue` enum to achieve 16-24 bytes per-key overhead through small-string optimization (SSO for values <= 12 bytes stored inline without heap allocation), tagged pointers encoding value type in low bits, and embedded TTL as a 4-byte delta from a per-database base timestamp. This replaces the current ~56-byte overhead Entry struct.

</domain>

<decisions>
## Implementation Decisions

### Small-string optimization (SSO)
- [auto] Values <= 12 bytes stored inline in a 16-byte struct: 4-byte length + 12 bytes data
- Values > 12 bytes: 4-byte length + 4-byte inline prefix (for fast comparison) + 8-byte heap pointer
- SSO avoids heap allocation for the majority of real-world Redis values (counters, flags, short strings)
- Use `union` or enum with `#[repr(C)]` for the inline/heap discriminant

### Tagged pointers
- [auto] Encode value type in low 3 bits of the heap pointer (pointer alignment guarantees low bits are zero)
- Type tags: 0=string, 1=hash, 2=list, 3=set, 4=zset, 5=stream (reserved), 6=integer (special fast path), 7=reserved
- For inline SSO values, use the high nibble of the length field as the type tag
- Integer type tag enables fast-path INCR/DECR without parsing string bytes

### Embedded TTL
- [auto] Replace `expires_at_ms: u64` (8 bytes) with `ttl_delta: u32` (4 bytes)
- TTL delta is seconds from a per-database base timestamp (updated periodically)
- ~136-year range (u32 max seconds) — more than sufficient
- 0 = no expiry (sentinel value); base timestamp stored once per Database struct
- Millisecond precision lost — acceptable trade-off for 50% TTL field size reduction (Redis PEXPIRE rounds to seconds in many deployments)

### Entry struct layout
- [auto] Target 24 bytes total per entry: 16-byte compact value + 4-byte TTL delta + 4-byte packed metadata
- Packed metadata (existing from Phase 7): last_access:16 | version:8 | access_counter:8 (fit in u32)
- Consider `#[repr(C)]` for predictable layout and no padding waste

### Migration strategy
- [auto] Incremental migration: new CompactEntry wraps old Entry initially, then refactor Database to use CompactEntry directly
- All command handlers access values through accessor methods (not direct field access) to insulate from layout changes
- Benchmark with 1M keys to validate >= 40% memory reduction

### Claude's Discretion
- Exact bit layout of the packed metadata u32 (may differ from Phase 7's u64 packing)
- Whether to use Rust `union` vs enum for SSO discriminant
- Cache-line alignment of the entry struct
- Whether integer fast-path (type tag 6) is worth implementing now or deferred

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Hash table design — 16-byte compact value encoding spec, SSO details, tagged pointer scheme

### Current implementation
- `src/storage/entry.rs` — Current Entry struct (417 lines) with RedisValue enum, packed metadata, LFU counter
- `src/storage/db.rs` — Database struct (773 lines) using HashMap<Bytes, Entry>
- `src/storage/eviction.rs` — Eviction policies (437 lines) that read entry metadata
- `src/command/string.rs` — String commands (1420 lines) that access RedisValue::String directly

### Prior phase context
- `.planning/phases/07-compare-architecture-resolve-bottlenecks-and-optimize-performance/07-01-PLAN.md` — Phase 7 entry compaction (removed created_at, packed metadata into u64)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `entry.rs` metadata packing helpers (`last_access()`, `version()`, `access_counter()`) — pattern to extend for new layout
- `current_secs()` / `current_time_ms()` — time helpers already exist, TTL delta can build on current_secs()

### Established Patterns
- `RedisValue` enum with 5 variants — must remain backward compatible for all command handlers
- `Entry::estimate_memory()` — must be updated for new layout to keep eviction accurate
- `Database::set()` auto-increments version — must work with new packed metadata

### Integration Points
- Every command handler in `src/command/*.rs` accesses `entry.value` — SSO must be transparent via accessor methods
- `src/persistence/rdb.rs` serializes Entry — must handle new encoding
- `src/persistence/aof.rs` — no changes needed (serializes commands, not entries)
- Eviction engine reads `last_access` and `access_counter` — must work with new u32 packing

</code_context>

<specifics>
## Specific Ideas

- Blueprint targets 57-71% memory reduction per key — from ~56 bytes to 16-24 bytes
- Redis itself stores ~56 bytes per key (24-byte dictEntry + 8-byte bucket pointer + 16-byte robj + SDS header)
- SSO covers majority of real workloads: counters (1-10 bytes), session tokens (< 12 bytes), boolean flags (1 byte)

</specifics>

<deferred>
## Deferred Ideas

- Compact collection encodings (listpack, intset) — Phase 15
- DashTable inline entry storage — Phase 10

</deferred>

---

*Phase: 09-compact-value-encoding-and-entry-optimization*
*Context gathered: 2026-03-24*
