---
phase: 28-performance-tuning
plan: 02
subsystem: performance
tags: [io_uring, pipeline-batching, buffer-management, refcell, memory-bloat]

# Dependency graph
requires:
  - phase: 12-io-uring
    provides: "io_uring event loop with Recv/Send handlers"
  - phase: 11-sharded
    provides: "Tokio sharded connection handler with pipeline batching"
provides:
  - "Batched command dispatch in io_uring Recv handler (parse-all/dispatch-all/send-all)"
  - "Buffer shrink guards preventing per-connection memory bloat"
affects: [performance-tuning, benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [three-phase-batch-dispatch, buffer-capacity-shrink-guard]

key-files:
  created: []
  modified:
    - src/shard/mod.rs
    - src/server/connection.rs

key-decisions:
  - "Three-phase batch dispatch in io_uring: parse-all frames, dispatch-all under single borrow_mut, send-all responses"
  - "64KB threshold for buffer shrink guards balances memory reclaim vs reallocation overhead"
  - "read_buf shrink preserves unparsed trailing data via split+extend_from_slice"

patterns-established:
  - "Buffer shrink guard: reallocate to base capacity when exceeding threshold after I/O completion"
  - "Batch dispatch: collect parsed frames before acquiring mutable borrow for cache locality and lock reduction"

requirements-completed: [PERF-28-03]

# Metrics
duration: 3min
completed: 2026-03-25
---

# Phase 28 Plan 02: Adaptive Pipeline Batching Summary

**io_uring batch dispatch reducing RefCell borrow overhead from N-per-pipeline to 1, plus 64KB buffer shrink guards preventing per-connection memory bloat**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-25T09:43:39Z
- **Completed:** 2026-03-25T09:46:39Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Refactored io_uring Recv handler to three-phase batch dispatch: parse all frames first, dispatch all under single databases.borrow_mut(), then serialize and send all responses
- Added capacity shrink guards on both write_buf and read_buf in Tokio sharded connection handler to prevent permanent memory bloat from occasional large pipelines
- All 1134+ tests pass with zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Batch command dispatch in io_uring event handler** - `988cf42` (perf)
2. **Task 2: Optimize Tokio handler write_buf reuse** - `550d018` (perf)

## Files Created/Modified
- `src/shard/mod.rs` - Refactored handle_uring_event IoEvent::Recv to three-phase batch dispatch
- `src/server/connection.rs` - Added write_buf and read_buf capacity shrink guards after write_all

## Decisions Made
- Three-phase batch dispatch: parse-all into Vec<Frame>, dispatch-all under single borrow_mut, send-all via existing writev/pool/heap paths
- 64KB threshold chosen for shrink guards: balances memory reclaim (large pipeline responses can be 100KB+) against avoiding excessive reallocations for normal traffic
- read_buf shrink preserves unparsed trailing data via split+extend_from_slice to avoid data loss at batch boundaries

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- io_uring and Tokio paths now both use batched dispatch with buffer lifecycle management
- Ready for Phase 29 or further performance benchmarking

---
*Phase: 28-performance-tuning*
*Completed: 2026-03-25*
