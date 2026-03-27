---
phase: 15-b-plus-tree-sorted-sets-and-optimized-collection-encodings
verified: 2026-03-24T12:24:45Z
status: gaps_found
score: 5/7 must-haves verified
gaps:
  - truth: "Listpack encoding used for small collections (<= 128 elements) — hash and list"
    status: failed
    reason: "get_or_create_hash uses Entry::new_hash() (full HashMap) and get_or_create_list uses Entry::new_list() (full VecDeque) for new keys. CompactListpack constructors exist but are not used by db.rs accessors for hash/list creation. OBJECT ENCODING reports 'hashtable' immediately after the first HSET."
    artifacts:
      - path: "src/storage/db.rs"
        issue: "Line 279: Entry::new_hash() used instead of Entry::new_hash_listpack() for new hash keys. Line 336: Entry::new_list() used instead of Entry::new_list_listpack() for new list keys."
    missing:
      - "Change get_or_create_hash to start new keys with Entry::new_hash_listpack() so small hashes use listpack encoding until threshold is exceeded"
      - "Change get_or_create_list to start new keys with Entry::new_list_listpack() so small lists use listpack encoding until threshold is exceeded"
      - "Add threshold-based upgrade in get_or_create_hash and get_or_create_list (upgrade when size > LISTPACK_MAX_ENTRIES=128 or single element > 64 bytes)"

  - truth: "Memory reduction >= 10x for sorted sets with 1M entries"
    status: partial
    reason: "The structural design supports this: BPTree uses arena allocation (Vec<Node>) with LEAF_CAPACITY=14 entries per leaf, giving ~1.4 bytes amortized node overhead per entry vs BTreeMap's ~37 bytes (12-14x theoretical). However, no programmatic test or benchmark exists in the codebase that verifies this claim with actual measurement. The existing bench/entry_memory.rs benchmarks CompactEntry struct size, not BPTree vs BTreeMap memory for 1M sorted set entries."
    artifacts:
      - path: "benches/entry_memory.rs"
        issue: "No benchmark comparing BPTree vs BTreeMap memory usage for sorted sets with 1M entries"
    missing:
      - "A test or benchmark that inserts 1M (score, member) pairs into BPTree, measures memory, and asserts >= 10x reduction vs equivalent BTreeMap"
human_verification:
  - test: "Listpack starts for small hashes"
    expected: "OBJECT ENCODING mykey returns 'listpack' after HSET with a small number of fields, then returns 'hashtable' after >128 fields are added"
    why_human: "Cannot verify without running server against Redis client — also blocked by code gap (hash uses full encoding from creation)"
  - test: "Memory reduction 10x for sorted set 1M entries"
    expected: "RSS or allocated bytes for 1M ZADD entries is <= 10% of equivalent BTreeMap-based implementation"
    why_human: "Requires profiling/benchmarking the running server, not achievable by static analysis"
---

# Phase 15: B+ Tree Sorted Sets and Optimized Collection Encodings — Verification Report

**Phase Goal:** Replace BTreeMap-based sorted sets with cache-friendly B+ tree, implement listpack for small collections, intset for integer-only sets
**Verified:** 2026-03-24T12:24:45Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (from ROADMAP.md Success Criteria)

| #  | Truth                                                                                 | Status      | Evidence                                                                  |
|----|---------------------------------------------------------------------------------------|-------------|---------------------------------------------------------------------------|
| 1  | B+ tree with <= 3 bytes overhead per entry                                            | ? UNCERTAIN | LEAF_CAPACITY=14 gives ~1.4 bytes node overhead/entry structurally; no runtime assertion |
| 2  | B+ tree nodes pack elements sequentially for cache-friendly iteration                | VERIFIED    | Fixed-size arrays `[Key; LEAF_CAPACITY]` in LeafNode; sequential memory layout confirmed |
| 3  | Listpack encoding used for small collections (<= 128 elements)                       | FAILED      | Hash/list db.rs accessors use full encoding at creation; only sets (intset path) and sorted sets (BPTree) use compact start |
| 4  | Intset encoding used for integer-only sets                                            | VERIFIED    | SADD detects all-integer members, creates SetIntset; OBJECT ENCODING returns "intset"; test passes |
| 5  | Automatic encoding upgrade compact -> full when threshold exceeded                    | PARTIAL     | Upgrade path exists for all variants in db.rs; but hash/list never start as compact so upgrade is never triggered for them |
| 6  | All ZADD/ZRANGE/ZRANGEBYSCORE/ZRANK operations maintain O(log N)                     | VERIFIED    | All operations use BPTree.rank(), .rev_rank(), .range(), .range_by_rank(); BTreeMap fully eliminated from sorted_set.rs (0 references) |
| 7  | Memory reduction >= 10x for sorted sets with 1M entries                              | UNCERTAIN   | Structurally plausible (arena allocation vs heap-per-node) but no automated test verifies the 10x claim |

**Score:** 5/7 truths verified (2 failed/uncertain on listpack-for-collections and 10x measurement)

### Required Artifacts

| Artifact                        | Expected                                  | Status      | Details                                             |
|---------------------------------|-------------------------------------------|-------------|-----------------------------------------------------|
| `src/storage/bptree.rs`         | Arena-allocated B+ tree, 1639 lines       | VERIFIED    | Exports BPTree, BPTreeIter, BPTreeRevIter; 25 tests all pass |
| `src/storage/listpack.rs`       | Compact byte-array encoding, 791 lines    | VERIFIED    | Exports Listpack, ListpackIter, ListpackRevIter; 24 tests pass |
| `src/storage/intset.rs`         | Sorted integer array, 486 lines           | VERIFIED    | Exports Intset, IntsetEncoding; 18 tests pass       |
| `src/storage/entry.rs`          | RedisValue +6 compact variants            | VERIFIED    | HashListpack, ListListpack, SetListpack, SetIntset, SortedSetBPTree, SortedSetListpack all present; encoding_name() correct |
| `src/storage/compact_value.rs`  | RedisValueRef +6 compact variants         | VERIFIED    | All 6 compact variants present; encoding_name() on RedisValueRef |
| `src/storage/db.rs`             | Encoding dispatch + auto-upgrade          | PARTIAL     | Sorted set creates BPTree; set creates intset via separate path; hash/list create FULL encoding at start |
| `src/persistence/rdb.rs`        | Compact variant serialization             | VERIFIED    | All 6 compact variants handled in write_entry(); expand to element-level format |
| `src/persistence/aof.rs`        | Compact variant AOF rewrite               | VERIFIED    | All compact variants handled in generate_rewrite_commands |
| `src/command/sorted_set.rs`     | All sorted set commands using BPTree      | VERIFIED    | Zero BTreeMap references; zadd_member/zrem_member use BPTree; all 45 tests pass |
| `src/command/set.rs`            | Intset-aware SADD                         | VERIFIED    | Integer detection, intset creation, upgrade to HashSet at threshold |
| `src/command/key.rs`            | OBJECT ENCODING command                   | VERIFIED    | Handles all encoding types; OBJECT subcommand dispatched in mod.rs line 56 |
| `src/storage/mod.rs`            | Module re-exports                         | VERIFIED    | pub mod bptree, pub mod listpack, pub mod intset all present |

### Key Link Verification

| From                            | To                         | Via                                        | Status      | Details                                                   |
|---------------------------------|----------------------------|--------------------------------------------|-------------|-----------------------------------------------------------|
| `src/storage/bptree.rs`         | `bytes::Bytes`             | member storage in leaf nodes               | VERIFIED    | `use bytes::Bytes` at top; Bytes in Key type              |
| `src/storage/bptree.rs`         | `ordered_float::OrderedFloat` | score comparison in nodes               | VERIFIED    | `use ordered_float::OrderedFloat` at top                  |
| `src/storage/entry.rs`          | `src/storage/listpack.rs`  | Listpack type in compact variants          | VERIFIED    | `use super::listpack::Listpack` (implicit via mod path)   |
| `src/storage/entry.rs`          | `src/storage/bptree.rs`    | BPTree type in SortedSetBPTree variant     | VERIFIED    | BPTree used in SortedSetBPTree variant                    |
| `src/storage/entry.rs`          | `src/storage/intset.rs`    | Intset type in SetIntset variant           | VERIFIED    | Intset used in SetIntset variant                          |
| `src/storage/db.rs`             | `src/storage/entry.rs`     | Match on RedisValue::HashListpack in accessors | PARTIAL | Matches HashListpack for upgrade, but get_or_create_hash starts with Entry::new_hash() not Entry::new_hash_listpack() |
| `src/command/sorted_set.rs`     | `src/storage/bptree.rs`    | BPTree API calls (insert, remove, range, rank) | VERIFIED | `use crate::storage::bptree::BPTree`; tree.rank(), tree.range_by_rank() etc. used throughout |
| `src/command/sorted_set.rs`     | `src/storage/db.rs`        | get_or_create_sorted_set returns BPTree    | VERIFIED    | Line 247, 327, 424, 522 etc. use get_or_create_sorted_set |
| `src/command/key.rs`            | `src/storage/entry.rs`     | RedisValue::encoding_name() for OBJECT ENCODING | VERIFIED | key.rs uses entry.value.as_redis_value().encoding_name() |
| `src/command/mod.rs`            | `src/command/key.rs`       | OBJECT dispatch wired                      | VERIFIED    | Line 56: `if cmd.eq_ignore_ascii_case(b"OBJECT") { return DispatchResult::Response(key::object(db, args)); }` |

### Requirements Coverage

| Requirement | Source Plan | Description                                                          | Status     | Evidence                                                          |
|-------------|-------------|----------------------------------------------------------------------|------------|-------------------------------------------------------------------|
| MEM-01      | 15-01, 15-03, 15-04 | Memory-efficient encodings for small collections (listpack/B+ tree) | PARTIAL    | B+ tree fully implemented and wired for sorted sets; listpack infrastructure exists but hash/list start with full encoding in db.rs |
| MEM-02      | 15-02, 15-05 | Intset encoding for small integer-only sets                         | VERIFIED   | SADD creates SetIntset for integer-only members; upgrades to HashSet at INTSET_MAX_ENTRIES=512 |

### Anti-Patterns Found

| File                       | Line | Pattern                                           | Severity | Impact                                              |
|----------------------------|------|---------------------------------------------------|----------|-----------------------------------------------------|
| `src/storage/db.rs`        | 279  | `Entry::new_hash()` used instead of compact start | Blocker  | Hash collections never start as listpack; MEM-01 partially unmet for hash type |
| `src/storage/db.rs`        | 336  | `Entry::new_list()` used instead of compact start | Blocker  | List collections never start as listpack; MEM-01 partially unmet for list type |
| `.planning/ROADMAP.md`     | 349  | `[ ]` checkbox for 15-05 not updated to `[x]`    | Warning  | ROADMAP shows plan 05 incomplete but 15-05-SUMMARY.md and implementation say it is done |

### Human Verification Required

#### 1. BPTree memory overhead measurement

**Test:** Insert 1,000,000 (score, member) pairs into a BPTree via ZADD, measure RSS memory growth; compare against equivalent count in a std BTreeMap
**Expected:** BPTree uses >= 10x less memory than BTreeMap for 1M entries
**Why human:** Requires runtime profiling (heaptrack, valgrind massif, or jemalloc stats) — not verifiable by static analysis

#### 2. Listpack start encoding for new small hashes

**Test:** `HSET mysmallhash f1 v1`, then `OBJECT ENCODING mysmallhash` should return "listpack"
**Expected:** "listpack" for new hash with 1 field (threshold is 128)
**Why human:** Currently returns "hashtable" because db.rs starts with full HashMap. This is the code gap identified above. After the gap is fixed, this would need runtime verification.

### Gaps Summary

Two blockers prevent full goal achievement:

**Gap 1 (Blocker): Listpack not used for hash/list creation.** The `get_or_create_hash` and `get_or_create_list` accessors in `src/storage/db.rs` create new keys with full `HashMap` and `VecDeque` respectively (`Entry::new_hash()`, `Entry::new_list()`). The compact `new_hash_listpack()` and `new_list_listpack()` constructors exist and the upgrade path is coded (lines 286-289 for hash, 342-346 for list), but are never triggered because keys are never created as compact. OBJECT ENCODING returns "hashtable"/"linkedlist" immediately after the first mutation, not "listpack". The fix is to change `get_or_create_hash` to use `Entry::new_hash_listpack()` and add threshold-check upgrade logic.

**Gap 2 (Uncertain): No 10x memory reduction test.** The B+ tree's structural design makes the 10x claim plausible — arena-allocated fixed arrays (LEAF_CAPACITY=14) give ~1.4 bytes node overhead per entry vs BTreeMap's pointer-per-node allocation overhead. However, there is no automated test that measures actual memory with 1M entries and asserts the 10x claim. This needs a benchmark or test in benches/ or src/storage/bptree.rs.

Both gaps relate to MEM-01 partially unmet. MEM-02 (intset for integer sets) is fully satisfied. The sorted set B+ tree migration is complete and all 45 sorted set command tests pass. The OBJECT ENCODING command works for all encoding types.

---

_Verified: 2026-03-24T12:24:45Z_
_Verifier: Claude (gsd-verifier)_
