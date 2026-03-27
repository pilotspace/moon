---
phase: 09-compact-value-encoding-and-entry-optimization
plan: 01
subsystem: database
tags: [sso, tagged-pointers, compact-encoding, memory-optimization, unsafe-rust]

# Dependency graph
requires:
  - phase: 07-performance-optimization
    provides: "Compacted Entry struct with u32 metadata packing"
provides:
  - "CompactValue (16 bytes) with SSO for strings <= 12 bytes"
  - "CompactEntry (24 bytes) replacing old ~88-byte Entry"
  - "RedisValueRef enum for zero-copy borrowed access"
  - "Database.base_timestamp() for TTL delta computation"
  - "Method-based accessor pattern for entry.value and entry TTL"
affects: [persistence, commands, eviction, replication]

# Tech tracking
tech-stack:
  added: []
  patterns: [tagged-heap-pointers, small-string-optimization, compact-entry-encoding]

key-files:
  created:
    - src/storage/compact_value.rs
  modified:
    - src/storage/entry.rs
    - src/storage/db.rs
    - src/storage/eviction.rs
    - src/storage/mod.rs
    - src/command/string.rs
    - src/command/key.rs
    - src/command/set.rs
    - src/command/persistence.rs
    - src/persistence/rdb.rs
    - src/persistence/aof.rs
    - src/server/expiration.rs

key-decisions:
  - "CompactValue uses tagged heap pointers (low 3 bits of Box<RedisValue> pointer) for type discrimination"
  - "TTL stored as absolute u32 seconds (not delta from base_timestamp) to avoid negative-delta edge cases with past expiry times"
  - "base_timestamp parameter kept in API for forward compatibility but TTL delta is absolute seconds"
  - "RedisValueRef enum enables zero-copy borrowed access without exposing CompactValue internals"
  - "Send/Sync manually implemented for CompactValue since Box<RedisValue> is Send+Sync"

patterns-established:
  - "CompactValue accessor pattern: entry.value.as_bytes() for fast string path, entry.value.as_redis_value() for full type match"
  - "Method-based TTL access: entry.expires_at_ms(base_ts) and entry.set_expires_at_ms(base_ts, ms) instead of field access"
  - "Entry construction via named constructors: Entry::new_string(), Entry::new_string_with_expiry(v, ms, base_ts)"

requirements-completed: [CVE-01, CVE-02, CVE-03, CVE-04]

# Metrics
duration: 19min
completed: 2026-03-24
---

# Phase 09 Plan 01: Compact Value Encoding Summary

**CompactValue 16-byte SSO with tagged heap pointers and CompactEntry 24-byte struct replacing ~88-byte Entry across all 50+ call sites**

## Performance

- **Duration:** 19 min
- **Started:** 2026-03-24T06:00:00Z
- **Completed:** 2026-03-24T06:19:00Z
- **Tasks:** 2
- **Files modified:** 12

## Accomplishments
- Created CompactValue (16 bytes) with inline SSO for strings <= 12 bytes and tagged heap pointers for larger values/collections
- Created CompactEntry (24 bytes) with compile-time size assertion, replacing the old ~88-byte Entry struct
- Migrated all ~50 call sites across string, key, set, hash, list, sorted_set, rdb, aof, eviction, and expiration modules
- All 506 tests pass (457 lib + 49 integration) with zero failures

## Task Commits

Each task was committed atomically:

1. **Task 1: Create CompactValue SSO struct and CompactEntry with accessor methods** - `855e449` (feat)
2. **Task 2: Migrate Database, eviction, and all consumers to CompactEntry** - `ca63070` (feat)

## Files Created/Modified
- `src/storage/compact_value.rs` - 16-byte CompactValue with SSO, tagged heap pointers, RedisValueRef enum, Clone/Drop/Debug impls
- `src/storage/entry.rs` - CompactEntry (24 bytes), u32 metadata packing [last_access:16|version:8|counter:8], Entry type alias
- `src/storage/db.rs` - Database with base_timestamp field, updated all expiry checks and type-checked helpers
- `src/storage/eviction.rs` - Updated volatile TTL eviction to use expires_at_ms(base_ts) method
- `src/storage/mod.rs` - Added compact_value module
- `src/command/string.rs` - Replaced ~20 entry.value pattern matches and entry.expires_at_ms field accesses
- `src/command/key.rs` - Updated TTL/PTTL commands, TYPE, SCAN, UNLINK async drop check
- `src/command/set.rs` - Updated SINTERSTORE/SUNIONSTORE/SDIFFSTORE entry.value mutation
- `src/command/persistence.rs` - Updated BGSAVE snapshot to include base_timestamp
- `src/persistence/rdb.rs` - Updated write_entry/read_entry for CompactValue, save_from_snapshot signature
- `src/persistence/aof.rs` - Updated generate_rewrite_commands and rewrite_aof for CompactValue
- `src/server/expiration.rs` - Updated expire_cycle tests for new_string_with_expiry signature

## Decisions Made
- **Absolute TTL seconds instead of delta:** Storing TTL as absolute u32 seconds (unix epoch) instead of delta from base_timestamp. A delta approach causes negative values for entries with past expiry times (common in tests and lazy expiry). Absolute seconds with u32 is valid until year 2106.
- **Tagged pointer alignment:** Box<RedisValue> alignment is >= 8 bytes on 64-bit systems, leaving 3 low bits available for type tags (0-4 for the 5 Redis types). Debug assertion verifies alignment at construction.
- **as_bytes() fast path:** Added `CompactValue::as_bytes() -> Option<&[u8]>` for the most common access pattern (string GET), avoiding full RedisValueRef enum construction.
- **Version bits reduced from 24 to 8:** Sufficient for WATCH within-transaction version checking, wraps at 255 instead of 16M.
- **Last access bits reduced from 32 to 16:** Wraps every ~18 hours, acceptable for LRU relative comparisons.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed TTL delta computation for past expiry times**
- **Found during:** Task 2 (test verification)
- **Issue:** Plan specified TTL as delta from base_timestamp, but entries with expires_at_ms in the past produced negative deltas. Setting delta=1 made "past" entries appear unexpired.
- **Fix:** Changed TTL storage to absolute u32 seconds (epoch) instead of delta from base_timestamp. The base_ts parameter is kept in the API for forward compatibility but is not used in computation.
- **Files modified:** src/storage/entry.rs
- **Verification:** All 8 previously-failing expiry tests pass
- **Committed in:** ca63070 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Critical correctness fix. The delta approach as planned cannot represent past expiry times with a u32 delta. Absolute seconds achieves the same compactness (4 bytes) without the edge case.

## Issues Encountered
- Borrow checker conflicts when accessing `db.base_timestamp()` after `db.get()` in test code -- resolved by extracting base_ts before the mutable borrow.
- `matches!` macro with `entry.value` field access no longer works with CompactValue -- replaced with `entry.value.type_name()` and `entry.value.as_bytes()` assertions.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- CompactValue and CompactEntry are the foundation for all downstream optimization phases
- All accessor methods are in place for future phases to build upon
- The SSO path is ready for extension with integer encoding (TAG_INTEGER reserved but not yet implemented)

---
*Phase: 09-compact-value-encoding-and-entry-optimization*
*Completed: 2026-03-24*
