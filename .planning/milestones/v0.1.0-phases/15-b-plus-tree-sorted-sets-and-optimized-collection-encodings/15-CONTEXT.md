# Phase 15: B+ Tree Sorted Sets and Optimized Collection Encodings - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace BTreeMap-based sorted sets with a cache-friendly B+ tree achieving 2-3 bytes per entry overhead (vs ~37 bytes current). Implement listpack encoding for small hashes, lists, sets, and sorted sets (compact byte-array with sequential access). Implement intset encoding for integer-only sets. Automatic encoding upgrade when collection exceeds size threshold.

</domain>

<decisions>
## Implementation Decisions

### B+ tree for sorted sets
- [auto] Custom B+ tree with configurable fanout (order ~128 for cache-line-friendly nodes)
- Leaf nodes store (score, member) pairs packed sequentially — cache-friendly iteration
- Internal nodes store separator scores + child pointers only
- Per-entry overhead target: 2-3 bytes amortized (vs BTreeMap's ~37 bytes with pointer overhead)
- Dual-index maintained: B+ tree for score-ordered access + HashMap for O(1) member→score lookup (same as current)

### Listpack encoding for small collections
- [auto] Listpack = compact byte-array storing entries sequentially (Redis 7+ replacement for ziplist)
- Entry format: [encoding-byte][data][backlen] — backlen enables reverse traversal without cascade updates
- Used for: small hashes (< 128 entries), small lists (< 128 elements), small sets (< 128 members), small sorted sets (< 128 members)
- Configurable thresholds via CONFIG SET (matching Redis's hash-max-listpack-entries, etc.)

### Intset encoding for integer sets
- [auto] Sorted array of integers with configurable encoding width (int16, int32, int64)
- Automatic width upgrade when a wider integer is added
- Binary search for membership check — O(log N) but cache-friendly sequential memory
- Convert to HashSet when a non-integer member is added or set exceeds size threshold

### Encoding upgrade triggers
- [auto] Listpack → full data structure when:
  - Element count exceeds threshold (default 128)
  - Single element size exceeds threshold (default 64 bytes)
- Intset → HashSet when:
  - Non-integer element added
  - Element count exceeds threshold (default 512)
- Upgrade is one-way (no downgrade) — simpler and matches Redis behavior

### RedisValue enum changes
- [auto] Extend RedisValue enum with compact variants:
  - `ListPack(Vec<u8>)` — used by small hashes, lists, sets, and sorted sets (tagged by the parent Entry type)
  - `IntSet(Vec<u8>)` — sorted integer array with width prefix
  - OR: keep existing variants but make them generic over storage backend (trait-based)
- Recommended: separate variants `HashSmall(ListPack)` / `Hash(HashMap)` etc. for clarity

### Claude's Discretion
- B+ tree node size (target: fit in 2-3 cache lines = 128-192 bytes)
- Whether listpack is implemented as a separate crate or module within storage/
- OBJECT ENCODING command support (report "listpack", "intset", "skiplist" etc.)
- Whether to implement listpack reverse iteration (needed for LRANGE with negative indices)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Hash table design — B+ tree for sorted sets: 2-3 bytes per entry

### Redis reference
- Redis listpack.c — entry format (encoding-byte + data + backlen), no cascade updates
- Redis intset.c — sorted integer array with width upgrade
- Redis object.c — encoding selection logic and upgrade thresholds

### Current implementation
- `src/storage/entry.rs` — RedisValue enum with SortedSet {members, scores} dual-index (line 28-36)
- `src/command/sorted_set.rs` — Sorted set commands (2466 lines) — must work with both B+ tree and BTreeMap
- `src/command/hash.rs` — Hash commands (1259 lines) — must work with both listpack and HashMap
- `src/command/list.rs` — List commands (1325 lines) — must work with both listpack and VecDeque
- `src/command/set.rs` — Set commands (1542 lines) — must work with both intset/listpack and HashSet

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ordered-float` crate — needed for B+ tree score ordering (already a dependency)
- All command handlers — must be updated to dispatch on encoding type

### Established Patterns
- `zadd_member()`/`zrem_member()` helpers enforce dual-index consistency — extend for B+ tree
- Collection commands access `entry.value` directly via match — add new match arms for compact variants

### Integration Points
- RDB/AOF persistence — must serialize/deserialize compact encodings
- OBJECT ENCODING command — report correct encoding type
- CONFIG SET for threshold tuning — need new config parameters
- Memory estimation (`estimate_memory()`) — must account for compact encoding sizes

</code_context>

<specifics>
## Specific Ideas

- Blueprint: BTreeMap ~37 bytes per entry vs B+ tree 2-3 bytes — 12-18x memory reduction for sorted sets
- Listpack avoids cascade updates (ziplist's fatal flaw) via backlen format
- Redis uses same thresholds for years: 128 entries, 64 bytes max element — proven defaults

</specifics>

<deferred>
## Deferred Ideas

- Quicklist (linked list of listpacks for large lists) — evaluate if VecDeque is insufficient
- Radix tree for Streams — Phase 18

</deferred>

---

*Phase: 15-b-plus-tree-sorted-sets-and-optimized-collection-encodings*
*Context gathered: 2026-03-24*
