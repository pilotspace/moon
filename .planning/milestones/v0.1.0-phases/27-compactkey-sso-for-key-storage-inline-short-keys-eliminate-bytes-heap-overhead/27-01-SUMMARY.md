---
phase: 27-compactkey-sso-for-key-storage-inline-short-keys-eliminate-bytes-heap-overhead
plan: 01
subsystem: storage
tags: [sso, compact-key, dashtable, memory-optimization, unsafe-rust]

requires:
  - phase: 09-compact-value-encoding
    provides: CompactValue SSO pattern and tagged heap pointer discipline
  - phase: 10-dashtable-segmented-hash-table
    provides: DashTable with generic K,V segments and Swiss Table SIMD probing

provides:
  - CompactKey 24-byte struct with SSO for keys <= 23 bytes
  - DashTable<CompactKey, V> replacing DashTable<Bytes, V>
  - Zero heap allocation for >90% of real Redis keys

affects: [28-performance-tuning, persistence, eviction]

tech-stack:
  added: []
  patterns: [compact-key-sso, heap-path-prefix-caching]

key-files:
  created:
    - src/storage/compact_key.rs
  modified:
    - src/storage/dashtable/mod.rs
    - src/storage/db.rs
    - src/storage/eviction.rs
    - src/persistence/aof.rs
    - src/persistence/rdb.rs
    - src/persistence/snapshot.rs
    - src/command/key.rs
    - src/command/persistence.rs
    - src/cluster/migration.rs
    - src/server/expiration.rs
    - src/storage/mod.rs

key-decisions:
  - "CompactKey layout: data[0] is length/tag byte, data[1..24] is key data or heap metadata"
  - "Heap path caches 12-byte inline prefix for fast mismatch rejection"
  - "Database::set() still accepts Bytes, converts internally to CompactKey::from(key)"
  - "write_entry changed to &[u8] instead of &Bytes for persistence portability"

patterns-established:
  - "CompactKey SSO: 24 bytes inline stores keys <= 23 bytes; heap path for longer keys"
  - "to_bytes() method on CompactKey for Bytes conversion when needed (Frame::BulkString)"

requirements-completed: [COMPACTKEY-01, COMPACTKEY-02]

duration: 14min
completed: 2026-03-25
---

# Phase 27 Plan 01: CompactKey SSO for Key Storage Summary

**24-byte CompactKey with SSO inlining keys <= 23 bytes, eliminating Bytes heap allocation for >90% of real Redis keys in DashTable**

## Performance

- **Duration:** 14 min
- **Started:** 2026-03-25T09:08:18Z
- **Completed:** 2026-03-25T09:22:23Z
- **Tasks:** 2
- **Files modified:** 12

## Accomplishments
- Created CompactKey struct (24 bytes) with inline/heap discriminant and 12-byte heap prefix caching
- Full trait suite: Hash, Eq, Ord, Clone, Drop, AsRef<[u8]>, Borrow<[u8]>, Debug, Send, Sync
- Swapped DashTable from Bytes to CompactKey across entire codebase (db, persistence, eviction, commands)
- All 1020 tests pass with zero functional regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Create CompactKey struct with SSO** - `f09730b` (feat)
2. **Task 2: Swap DashTable from Bytes to CompactKey** - `3981d77` (feat)

## Files Created/Modified
- `src/storage/compact_key.rs` - CompactKey struct with SSO, all trait impls, 26 unit tests
- `src/storage/mod.rs` - Added compact_key module declaration
- `src/storage/dashtable/mod.rs` - DashTable<CompactKey, V> impl block, updated tests
- `src/storage/db.rs` - Database uses CompactKey internally, Bytes->CompactKey conversion in set()
- `src/storage/eviction.rs` - Eviction uses CompactKey for sampled keys
- `src/persistence/aof.rs` - AOF rewrite uses key.to_bytes() for Frame::BulkString
- `src/persistence/rdb.rs` - write_entry takes &[u8], save_from_snapshot uses CompactKey
- `src/persistence/snapshot.rs` - Snapshot overflow uses &[u8] for key comparison
- `src/command/key.rs` - KEYS/SCAN use CompactKey for sorted key collections
- `src/command/persistence.rs` - BGSAVE snapshot collection uses CompactKey
- `src/cluster/migration.rs` - GetKeysInSlot uses key.as_bytes() for slot calculation
- `src/server/expiration.rs` - Expire cycle uses key.as_bytes() for removal

## Decisions Made
- CompactKey layout: data[0] is length/tag byte (high bit = heap flag), data[1..24] is key data or heap metadata
- Heap path stores raw pointer as usize in data[4..12] with 12-byte prefix in data[12..24] for fast rejection
- Database::set() signature kept as Bytes to minimize cascading changes across 100+ call sites
- Added Ord/PartialOrd and From<String> to CompactKey for SCAN sort and format!() key creation
- write_entry in rdb.rs changed from &Bytes to &[u8] for clean portability with CompactKey

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Cascading type changes across persistence, eviction, and command modules**
- **Found during:** Task 2 (DashTable swap)
- **Issue:** Changing DashTable<Bytes, V> to DashTable<CompactKey, V> caused compilation failures in 10 additional files beyond the planned dashtable/mod.rs
- **Fix:** Updated all consumers: persistence (aof, rdb, snapshot), storage (db, eviction), commands (key, persistence), cluster (migration), server (expiration)
- **Files modified:** 10 files beyond plan scope
- **Verification:** All 1020 tests pass
- **Committed in:** 3981d77 (Task 2 commit)

**2. [Rule 3 - Blocking] Added Ord/PartialOrd, From<String>, to_bytes() to CompactKey**
- **Found during:** Task 2 (DashTable swap)
- **Issue:** SCAN sorts keys (needs Ord), tests use format!() (needs From<String>), Frame::BulkString needs Bytes (needs to_bytes())
- **Fix:** Added three additional trait impls/methods to CompactKey
- **Files modified:** src/storage/compact_key.rs
- **Verification:** Compilation passes, all tests pass
- **Committed in:** 3981d77 (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both fixes were necessary for compilation. The cascading change was inherent to swapping the key type. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- CompactKey is fully integrated -- ready for Phase 28 performance tuning
- to_bytes() creates a copy; future optimization could avoid this for hot paths
- DashTable segment.rs and iter.rs required zero changes (already fully generic)

---
*Phase: 27-compactkey-sso-for-key-storage-inline-short-keys-eliminate-bytes-heap-overhead*
*Completed: 2026-03-25*
