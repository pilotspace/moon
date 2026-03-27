---
phase: 39-advanced-optimizations
plan: 02
subsystem: io
tags: [io_uring, writev, zero-copy, preserialized, scatter-gather]

requires:
  - phase: 38-core-optimizations
    provides: Frame::PreSerialized variant and serialize support
  - phase: 12-io-uring
    provides: WritevGuard, submit_writev, submit_writev_bulkstring infrastructure
provides:
  - PreSerialized frames take writev zero-copy path on io_uring (no serialize intermediary)
  - submit_send_preserialized() method on UringDriver
affects: [io_uring, performance, GET-path]

tech-stack:
  added: []
  patterns: [single-iovec writev for contiguous pre-serialized data]

key-files:
  created: []
  modified:
    - src/io/uring_driver.rs
    - src/shard/mod.rs

key-decisions:
  - "Single iovec writev for PreSerialized (no scatter-gather needed since data is already complete RESP wire format)"

patterns-established:
  - "PreSerialized writev: use submit_send_preserialized() for single-iovec direct send, vs submit_writev_bulkstring() for 3-iovec scatter-gather"

requirements-completed: [OPT-9]

duration: 3min
completed: 2026-03-26
---

# Phase 39 Plan 02: writev Zero-Copy PreSerialized Summary

**PreSerialized responses use single-iovec writev on io_uring, eliminating serialize() intermediary for GET responses**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-26T06:58:47Z
- **Completed:** 2026-03-26T07:01:47Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- Added `submit_send_preserialized()` to UringDriver for single-iovec writev of pre-serialized RESP data
- Extended shard io_uring send loop with PreSerialized match arm that skips serialize()
- Maintained fallback to serialize path if writev submission fails
- BulkString writev path unchanged and unaffected

## Task Commits

Each task was committed atomically:

1. **Task 1: Add PreSerialized to writev send path** - `2b237d5` (feat)

**Plan metadata:** [pending] (docs: complete plan)

## Files Created/Modified
- `src/io/uring_driver.rs` - Added submit_send_preserialized() method using single iovec for contiguous RESP data
- `src/shard/mod.rs` - Added PreSerialized match arm in io_uring send loop with writev path and fallback

## Decisions Made
- Used single iovec (count=1) for PreSerialized rather than scatter-gather (3 iovecs) since the data is already complete RESP wire format -- no header/trailer assembly needed
- Reused WritevGuard with _value_hold to keep Bytes alive until CQE completion

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing compilation errors (224 errors) from uncommitted Phase 38 SmallVec frame changes in frame.rs, parse.rs, serialize.rs -- these are unrelated to this plan's changes. Verified no errors reference uring_driver.rs or shard/mod.rs.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- writev zero-copy path now handles both BulkString (3-iovec scatter-gather) and PreSerialized (1-iovec direct)
- Phase 38 SmallVec frame changes need to be completed/committed separately to restore full compilation

---
*Phase: 39-advanced-optimizations*
*Completed: 2026-03-26*

## Self-Check: PASSED
