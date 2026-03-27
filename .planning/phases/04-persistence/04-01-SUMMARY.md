---
phase: 04-persistence
plan: 01
subsystem: persistence
tags: [rdb, binary-format, crc32, bgsave, snapshot, serialization]

# Dependency graph
requires:
  - phase: 03-collection-data-types
    provides: "All 5 RedisValue variants (String, Hash, List, Set, SortedSet), Database, Entry types"
provides:
  - "RDB binary snapshot format with CRC32 checksum"
  - "save/load functions for all 5 data types with TTL"
  - "BGSAVE command with clone-on-snapshot pattern"
  - "Startup RDB loading in listener"
  - "Persistence config fields (dir, dbfilename, appendonly, appendfsync, save, appendfilename)"
  - "save_from_snapshot for pre-cloned data (used by BGSAVE)"
affects: [04-02-aof, 04-03-integration-tests]

# Tech tracking
tech-stack:
  added: [crc32fast, tempfile]
  patterns: [atomic-file-write, clone-on-snapshot, spawn_blocking-for-io]

key-files:
  created:
    - src/persistence/mod.rs
    - src/persistence/rdb.rs
    - src/command/persistence.rs
  modified:
    - Cargo.toml
    - src/config.rs
    - src/lib.rs
    - src/command/mod.rs
    - src/server/connection.rs
    - src/server/listener.rs

key-decisions:
  - "Custom binary format with RUSTREDIS magic header and CRC32 footer for integrity"
  - "Atomic file write via .tmp rename for crash safety"
  - "BGSAVE handled in connection loop (like AUTH) to access Arc<Mutex<Vec<Database>>> and Arc<ServerConfig>"
  - "Clone-on-snapshot: clone all data under lock, serialize in spawn_blocking to avoid blocking event loop"
  - "TTL stored as Unix millis in RDB, converted to/from Instant on save/load"
  - "AtomicBool SAVE_IN_PROGRESS guard prevents concurrent BGSAVE operations"

patterns-established:
  - "Persistence binary format: magic header + version + DB_SELECTOR sections + EOF_MARKER + CRC32"
  - "Background save pattern: AtomicBool guard, clone under lock, spawn_blocking"
  - "Connection-level command interception for commands needing Arc<ServerConfig>"

requirements-completed: [PERS-01, PERS-03, PERS-04]

# Metrics
duration: 3min
completed: 2026-03-23
---

# Phase 04 Plan 01: RDB Persistence Summary

**Custom RDB binary format with CRC32 checksums, BGSAVE clone-on-snapshot in spawn_blocking, and startup restore**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-23T08:10:58Z
- **Completed:** 2026-03-23T08:14:42Z
- **Tasks:** 2
- **Files modified:** 9

## Accomplishments
- RDB binary format serializes/deserializes all 5 RedisValue types (String, Hash, List, Set, SortedSet) with TTL preservation
- BGSAVE command returns immediately, clones data under lock, writes RDB in background via spawn_blocking
- Server loads RDB snapshot on startup before accepting connections, with graceful fallback on error
- CRC32 checksum validation detects file corruption
- Atomic .tmp+rename write pattern prevents partial/corrupt RDB files on crash

## Task Commits

Each task was committed atomically:

1. **Task 1: Persistence config, module scaffold, and RDB serialize/deserialize** - `e5f7369` (feat)
2. **Task 2: BGSAVE command and startup RDB loading** - `c4da7a1` (feat)

## Files Created/Modified
- `src/persistence/mod.rs` - Persistence module with rdb submodule
- `src/persistence/rdb.rs` - RDB binary format: save/load/save_from_snapshot with CRC32 checksum (12 tests)
- `src/command/persistence.rs` - BGSAVE command handler with AtomicBool concurrency guard
- `Cargo.toml` - Added crc32fast and tempfile dependencies
- `src/config.rs` - Added persistence CLI flags (dir, dbfilename, appendonly, appendfsync, save, appendfilename)
- `src/lib.rs` - Added persistence module
- `src/command/mod.rs` - Added persistence command module
- `src/server/connection.rs` - BGSAVE interception before dispatch, Arc<ServerConfig> parameter
- `src/server/listener.rs` - Startup RDB loading, Arc<ServerConfig> for connection sharing

## Decisions Made
- Custom binary format with RUSTREDIS magic header rather than Redis-compatible RDB (simpler, sufficient for our use case)
- Atomic file write via .tmp rename for crash safety
- BGSAVE handled in connection loop (like AUTH) because it needs Arc<Mutex<Vec<Database>>> and Arc<ServerConfig> which dispatch() does not have
- Clone-on-snapshot: clone all data under lock, then serialize in spawn_blocking to avoid blocking the event loop
- TTL stored as Unix millisecond timestamps in RDB, converted to/from Instant on save/load
- AtomicBool SAVE_IN_PROGRESS guard prevents concurrent BGSAVE operations with clear error message

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- RDB persistence foundation complete
- save_from_snapshot function ready for AOF rewrite (Plan 02)
- ServerConfig wrapped in Arc for sharing across connections
- Ready for AOF append-only file (Plan 02) and integration tests (Plan 03)

## Self-Check: PASSED

- All 3 created files exist on disk
- Both task commits (e5f7369, c4da7a1) verified in git log
- 377 tests pass (cargo test --lib)

---
*Phase: 04-persistence*
*Completed: 2026-03-23*
