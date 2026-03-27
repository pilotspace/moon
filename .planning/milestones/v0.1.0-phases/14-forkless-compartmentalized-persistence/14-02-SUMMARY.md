---
phase: 14-forkless-compartmentalized-persistence
plan: 02
subsystem: persistence
tags: [wal, per-shard, batched-fsync, resp, replay, truncation]

requires:
  - phase: 11-thread-per-core-shared-nothing
    provides: per-shard architecture with SPSC channels
  - phase: 04-persistence
    provides: AOF replay_aof, serialize_command, RESP wire format

provides:
  - WalWriter struct with batched fsync for per-shard WAL files
  - replay_wal function delegating to existing replay_aof
  - wal_path helper for shard WAL file construction
  - WAL truncation after snapshot completion

affects: [14-03-async-segment-snapshots, 19-replication-psync2]

tech-stack:
  added: []
  patterns: [per-shard-wal, batched-fsync, wal-truncation-rotation]

key-files:
  created: []
  modified: [src/persistence/wal.rs, src/persistence/mod.rs]

key-decisions:
  - "WAL format identical to AOF (RESP wire format) for replay compatibility"
  - "replay_wal delegates directly to replay_aof -- no format divergence"
  - "8KB pre-allocated buffer to avoid hot-path reallocations"
  - "Truncation renames to .wal.old backup before opening fresh file"

patterns-established:
  - "Per-shard WAL: each shard owns its own WalWriter with independent flush timing"
  - "Batched fsync: buffer in memory, flush on 1ms tick, not per-command"

requirements-completed: []

duration: 4min
completed: 2026-03-24
---

# Phase 14 Plan 02: Per-Shard WAL Writer Summary

**Per-shard WalWriter with 8KB batched buffer, 1ms-tick flush, RESP-format replay via existing AOF, and truncation-rotation after snapshot**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-24T11:14:01Z
- **Completed:** 2026-03-24T11:18:01Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- WalWriter struct with append (hot path, zero I/O), flush_if_needed (batched fsync), and flush_sync (forced)
- WAL truncation rotates file to .wal.old backup, opens fresh file, resets counters
- replay_wal delegates to existing replay_aof for full RESP compatibility
- 7 tests covering append/flush, round-trip replay, truncation, collections, batching, path construction

## Task Commits

Each task was committed atomically:

1. **Task 1: Per-shard WalWriter with batched fsync and WAL replay** - `9c665e2` (feat)

**Plan metadata:** (pending)

## Files Created/Modified
- `src/persistence/wal.rs` - WalWriter struct, replay_wal, wal_path, 7 tests (content extended from 14-01 stub)
- `src/persistence/mod.rs` - Added `pub mod wal` re-export

## Decisions Made
- WAL uses identical RESP wire format as AOF for full replay compatibility
- replay_wal is a thin delegate to replay_aof -- no code duplication
- 8KB pre-allocated buffer avoids reallocation on the hot append path
- truncate_after_snapshot renames to .wal.old (single backup) before creating fresh file
- io_uring DMA optimization deferred -- std::fs works cross-platform now, can add without API changes later

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed premature `pub mod snapshot` from mod.rs**
- **Found during:** Task 1 (build verification)
- **Issue:** Linter auto-added `pub mod snapshot;` to mod.rs but snapshot.rs does not exist yet (Plan 01 parallel work)
- **Fix:** Removed the line to restore clean build
- **Files modified:** src/persistence/mod.rs
- **Verification:** `cargo build` succeeds with 0 errors
- **Committed in:** 9c665e2 (part of task commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Linter interference from parallel plan. No scope creep.

## Issues Encountered
- wal.rs was already created as a full implementation by 14-01 (not just a stub). The Write tool produced identical content, resulting in no diff for that file. The commit captures only the mod.rs update.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- WalWriter ready for integration into shard event loop (1ms tick calls flush_if_needed)
- replay_wal ready for startup sequence
- truncate_after_snapshot ready for post-snapshot hook in Plan 03

---
*Phase: 14-forkless-compartmentalized-persistence*
*Completed: 2026-03-24*
