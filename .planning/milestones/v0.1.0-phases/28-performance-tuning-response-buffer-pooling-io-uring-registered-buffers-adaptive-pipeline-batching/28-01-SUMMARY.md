---
phase: 28-performance-tuning
plan: 01
subsystem: io
tags: [io_uring, registered-buffers, write-fixed, zero-copy, buffer-pool]

# Dependency graph
requires:
  - phase: 12-io-uring-networking
    provides: UringDriver, IoEvent, InFlightSend, WritevGuard
provides:
  - SendBufPool with pre-registered 8KB send buffers (IORING_REGISTER_BUFFERS)
  - submit_send_fixed using IORING_OP_WRITE_FIXED (no per-I/O get_user_pages)
  - InFlightSend::Fixed variant with pool reclaim on SendComplete
affects: [performance-tuning, io-uring]

# Tech tracking
tech-stack:
  added: []
  patterns: [registered-buffer-pool, write-fixed-send, pool-first-heap-fallback]

key-files:
  created: []
  modified:
    - src/io/uring_driver.rs
    - src/shard/mod.rs

key-decisions:
  - "SendBufPool 256x8KB (2MB) per shard with LIFO free_list for cache-hot reuse"
  - "WriteFixed opcode for socket sends with buf_idx encoded in user_data aux field"
  - "Pool-first with graceful heap BytesMut fallback when exhausted or response > 8KB"
  - "All cleanup paths (Disconnect, SendError, ParseError) reclaim Fixed buffers"

patterns-established:
  - "send_serialized helper: pool alloc -> copy -> submit_send_fixed, fallback to heap submit_send"
  - "InFlightSend::Fixed(u16) variant for registered buffer lifecycle tracking"

requirements-completed: [PERF-28-01, PERF-28-02]

# Metrics
duration: 4min
completed: 2026-03-25
---

# Phase 28 Plan 01: Registered Send Buffer Pool Summary

**io_uring WRITE_FIXED with 256x8KB pre-registered send buffers eliminating per-I/O get_user_pages() kernel overhead**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-25T09:36:50Z
- **Completed:** 2026-03-25T09:40:58Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- SendBufPool with 256 pre-registered 8KB buffers (2MB per shard) using IORING_REGISTER_BUFFERS
- submit_send_fixed uses IORING_OP_WRITE_FIXED, skipping per-I/O page pinning
- Shard event loop serializes responses into pooled buffers with graceful heap fallback
- SendComplete CQE reclaims Fixed buffers back to pool; all cleanup paths handle reclaim
- All 1134 tests pass with no regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Add registered send buffer pool to UringDriver** - `f738a92` (feat)
2. **Task 2: Wire pooled send buffers into shard io_uring event loop** - `56ce1d0` (feat)

## Files Created/Modified
- `src/io/uring_driver.rs` - SendBufPool struct, register_buffers in init(), submit_send_fixed, alloc/reclaim methods
- `src/shard/mod.rs` - InFlightSend::Fixed variant, send_serialized helper, pool reclaim in all cleanup paths

## Decisions Made
- SendBufPool uses Vec<Vec<u8>> with LIFO free_list (stack pop/push) for cache-hot buffer reuse
- WriteFixed opcode with buf_idx encoded in user_data aux field for CQE-based reclaim
- Response > 8KB falls back to heap BytesMut (large responses are rare, pool covers typical RESP)
- send_serialized() centralizes pool-first-heap-fallback logic, called from both BulkString fallback and standard paths

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Registered send buffer pool ready for production use on Linux
- Non-Linux platforms unaffected (all io_uring code is cfg-gated)
- Adaptive pipeline batching can build on this foundation

---
*Phase: 28-performance-tuning*
*Completed: 2026-03-25*
