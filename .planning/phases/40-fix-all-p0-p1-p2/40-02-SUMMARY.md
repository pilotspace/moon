---
phase: 40-fix-all-p0-p1-p2
plan: 02
subsystem: server
tags: [monoio, allocation, buffer-reuse, channel-capacity, container-pooling]

# Dependency graph
requires:
  - phase: 40-fix-all-p0-p1-p2
    provides: Batched PipelineBatch dispatch in Monoio handler (40-01)
provides:
  - Per-connection buffer reuse eliminating per-read heap allocation
  - CONN_CHANNEL_CAPACITY increased to 4096 for 5K+ client stability
  - Pre-allocated batch containers (responses, remote_groups, reply_futures) reused via clear/drain
affects: [performance, high-client-stability, memory-allocation]

# Tech tracking
tech-stack:
  added: []
  patterns: [buffer-reuse-ownership-io, pre-allocated-container-reuse, drain-for-hashmap-reuse]

key-files:
  created: []
  modified:
    - src/server/connection.rs
    - src/shard/mesh.rs

key-decisions:
  - "Use Vec::resize for monoio buffer reuse instead of re-allocation -- ownership I/O returns buffer after read"
  - "Increase CONN_CHANNEL_CAPACITY to 4096 matching CHANNEL_BUFFER_SIZE for symmetric sizing"
  - "Use HashMap::drain() to reuse remote_groups allocation across loop iterations"

patterns-established:
  - "Monoio handler pre-allocates all hot-path containers before main loop, reuses via clear/drain"

requirements-completed: [FIX-P1, FIX-P2]

# Metrics
duration: 4min
completed: 2026-03-26
---

# Phase 40 Plan 02: Buffer Reuse and Container Pooling Summary

**Eliminated per-read heap allocation via buffer reuse in Monoio handler, increased CONN_CHANNEL_CAPACITY to 4096, and pre-allocated batch dispatch containers outside main loop for 5K+ client stability**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-26T12:32:08Z
- **Completed:** 2026-03-26T12:35:46Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Moved `vec![0u8; 8192]` read buffer outside Monoio handler main loop, reusing via `resize` + ownership reassignment
- Increased `CONN_CHANNEL_CAPACITY` from 256 to 4096 in mesh.rs to support 5K+ concurrent clients
- Pre-allocated `responses`, `remote_groups`, and `reply_futures` containers before main loop
- Replaced per-batch `HashMap::new()` and `Vec::new()` with `.clear()` and `.drain()` for zero-allocation reuse

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix per-read buffer allocation and increase channel capacity** - `f78912c` (feat)
2. **Task 2: Pool oneshot reply channels per-connection** - `b5c7d78` (feat)

## Files Created/Modified
- `src/server/connection.rs` - Buffer reuse (tmp_buf outside loop), pre-allocated responses/remote_groups/reply_futures with clear/drain reuse pattern
- `src/shard/mesh.rs` - CONN_CHANNEL_CAPACITY increased from 256 to 4096

## Decisions Made
- Used `Vec::resize(8192, 0)` for buffer reset instead of zeroing -- monoio overwrites on read anyway
- Matched CONN_CHANNEL_CAPACITY to CHANNEL_BUFFER_SIZE (both 4096) for symmetric channel sizing
- Used `HashMap::drain()` instead of consuming `for..in` to preserve HashMap allocation across iterations

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing test compilation errors (tokio crate unresolved under monoio feature, type inference in integration tests) -- confirmed identical on parent commit per 40-01-SUMMARY, not introduced by this change.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- All P0, P1, P2 fixes from Phase 40 are now complete
- Monoio handler has full parity with Tokio handler for batch dispatch plus allocation optimization
- Ready for high-client-count benchmarking to validate stability improvements

---
*Phase: 40-fix-all-p0-p1-p2*
*Completed: 2026-03-26*

## Self-Check: PASSED
