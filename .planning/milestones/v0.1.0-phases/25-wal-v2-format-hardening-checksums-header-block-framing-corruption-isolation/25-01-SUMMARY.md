---
phase: 25-wal-v2-format-hardening
plan: 01
subsystem: persistence
tags: [wal, crc32, binary-format, checksums, block-framing]

requires:
  - phase: 14-forkless-compartmentalized-persistence
    provides: Per-shard WAL writer with RESP wire format
provides:
  - WAL v2 writer with 32-byte RRDWAL header and CRC32-checksummed block framing
  - cmd_count tracking per flush cycle
affects: [25-02-wal-v2-replay, persistence, snapshot]

tech-stack:
  added: []
  patterns: [CRC32 block framing, versioned binary header, block-level corruption isolation]

key-files:
  created: []
  modified: [src/persistence/wal.rs]

key-decisions:
  - "Header is 32 bytes: RRDWAL(6B) + version(1B) + shard_id(2B LE) + epoch(8B LE) + reserved(15B)"
  - "CRC32 covers cmd_count(2B) + db_idx(1B) + payload -- not the block_len prefix"
  - "Three write_all calls for block frame (7-byte header, payload, 4-byte CRC) -- OS coalesces in page cache"
  - "Replay tests marked #[ignore] pending Plan 02 v2-aware replay implementation"

patterns-established:
  - "WAL v2 block layout: [block_len:4 LE][cmd_count:2 LE][db_idx:1][payload:var][crc32:4 LE]"
  - "Header written eagerly in new() and truncate_after_snapshot(), lazy check in do_write()"

requirements-completed: [WAL-V2-WRITE]

duration: 2min
completed: 2026-03-25
---

# Phase 25 Plan 01: WAL v2 Writer Format Summary

**WAL v2 binary writer with 32-byte RRDWAL header and CRC32-checksummed block framing using crc32fast**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-25T08:29:54Z
- **Completed:** 2026-03-25T08:32:21Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- WAL files now start with 32-byte versioned header (magic "RRDWAL", version 2, shard_id, epoch)
- Every flush produces a CRC32-checksummed block frame enabling per-block corruption detection
- cmd_count tracked per flush cycle via saturating_add (minimal hot-path overhead)
- Header written on fresh files and after truncate_after_snapshot
- 7 tests pass, 2 replay tests marked #[ignore] for Plan 02

## Task Commits

Each task was committed atomically:

1. **Task 1: Add WAL v2 constants, header writer, and block-framing flush** - `999b3b1` (feat)

## Files Created/Modified
- `src/persistence/wal.rs` - WAL v2 writer with header, block framing, CRC32 checksums, cmd_count tracking, updated tests

## Decisions Made
- Header layout: 6B magic + 1B version + 2B shard_id + 8B epoch + 15B reserved = 32 bytes
- CRC32 scope: covers cmd_count + db_idx + payload (not block_len prefix) for block-level integrity
- Three sequential write_all calls for block frame instead of building a Vec (avoids allocation, OS coalesces)
- Replay tests #[ignore] with TODO comment -- Plan 02 implements v2-aware replay

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- WAL v2 writer complete, ready for Plan 02 (v2-aware replay reader)
- Replay tests will be un-ignored and updated in Plan 02

---
*Phase: 25-wal-v2-format-hardening*
*Completed: 2026-03-25*
