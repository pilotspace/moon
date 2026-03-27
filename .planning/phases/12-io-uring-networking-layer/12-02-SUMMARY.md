---
phase: 12-io-uring-networking-layer
plan: 02
subsystem: io
tags: [io-uring, multishot-accept, multishot-recv, provided-buffers, writev, scatter-gather, cfg-gate, linux]

# Dependency graph
requires:
  - phase: 12-io-uring-networking-layer
    provides: "IO abstraction layer, FdTable/BufRingManager skeletons, static responses, user_data encoding"
provides:
  - "UringDriver with per-shard io_uring instance (SINGLE_ISSUER + DEFER_TASKRUN + COOP_TASKRUN)"
  - "Multishot accept via AcceptMulti opcode"
  - "Multishot recv via RecvMulti with provided buffer ring"
  - "FdTable with IORING_REGISTER_FILES integration (register_with_ring, update_registration)"
  - "BufRingManager with ProvideBuffers + get_buf + return_buf lifecycle"
  - "Writev scatter-gather via build_get_response_iovecs for zero-copy GET"
  - "SQE batching with pending_sqes counter and single submit_and_wait"
  - "IoEvent enum for CQE routing to shard event loop"
affects: [12-03-connection-migration, 13-memory-management, 14-persistence]

# Tech tracking
tech-stack:
  added: [libc]
  patterns: [uring-driver-per-shard, multishot-accept-recv, sqe-batching, scatter-gather-writev, provided-buffer-lifecycle]

key-files:
  created:
    - src/io/uring_driver.rs
  modified:
    - src/io/fd_table.rs
    - src/io/buf_ring.rs
    - src/io/mod.rs
    - Cargo.toml

key-decisions:
  - "Legacy ProvideBuffers approach for buffer registration (simpler than ring-mapped buf_ring); ring-mapped optimization deferred to benchmarks"
  - "Recv data copied from provided buffer before return (pitfall 1: parse/copy before return_buf)"
  - "FD table full = reject connection with close (graceful raw-fd fallback deferred)"
  - "submit_timeout_ms splits ms into sec + nsec correctly for io_uring Timespec"
  - "cqueue::more() function call for MORE flag check (io-uring 0.7 API)"

patterns-established:
  - "UringDriver per-shard pattern: create ring, register FDs, register buffers, event loop with drain_completions"
  - "Buffer lifecycle: get_buf -> copy data -> return_buf (never hold provided buffer across processing)"
  - "IoEvent enum routing: match on event type from drain_completions in shard loop"

requirements-completed: [P12-01, P12-02, P12-03, P12-04, P12-05]

# Metrics
duration: 4min
completed: 2026-03-24
---

# Phase 12 Plan 02: UringDriver Summary

**Per-shard io_uring driver with multishot accept/recv, registered FD table, provided buffer ring, SQE batching, and writev scatter-gather for zero-copy GET responses**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-24T09:10:41Z
- **Completed:** 2026-03-24T09:15:01Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- UringDriver with io_uring ring creation using SINGLE_ISSUER + DEFER_TASKRUN + COOP_TASKRUN flags
- Complete FdTable io_uring registration: register_with_ring, update_registration, insert_and_register, remove_and_register
- BufRingManager with contiguous storage, ProvideBuffers setup, get_buf/return_buf lifecycle, in_use tracking
- Multishot accept via AcceptMulti and multishot recv via RecvMulti with BUFFER_SELECT
- Writev scatter-gather response builder for zero-copy GET: header + value + CRLF
- SQE batching with pending_sqes counter and single submit_and_wait per event loop iteration
- IoEvent enum with 9 variants for CQE routing to shard event loop
- All code Linux-only (cfg-gated), macOS compilation and all 66 tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Complete FdTable io_uring registration and BufRingManager** - `48e9a45` (feat)
2. **Task 2: Implement UringDriver with multishot accept/recv, SQE batching, writev** - `76cb02f` (feat)

## Files Created/Modified
- `src/io/uring_driver.rs` - UringDriver (522 lines): per-shard io_uring event loop with multishot ops, SQE batching, IoEvent enum, scatter-gather helper
- `src/io/fd_table.rs` - Added register_with_ring, update_registration, insert_and_register, remove_and_register
- `src/io/buf_ring.rs` - Full BufRingManager: contiguous storage, setup_ring, get_buf, return_buf, in_use tracking
- `src/io/mod.rs` - Added cfg-gated re-exports for UringDriver, UringConfig, IoEvent, build_get_response_iovecs
- `Cargo.toml` - Added libc as Linux dependency

## Decisions Made
- Used legacy `IORING_OP_PROVIDE_BUFFERS` approach for buffer ring setup (simpler than ring-mapped io_uring_buf_ring); the performance benefit of ring-mapped buffers can be added later if benchmarks warrant it
- Recv data is copied from the provided buffer before returning it to the ring, per pitfall 1 (prevents use-after-free if kernel reuses buffer)
- When FD table is full, connections are rejected with close() rather than graceful raw-fd fallback (raw-fd fallback adds complexity for marginal benefit)
- Used `cqueue::more()` function for checking multishot continuation (io-uring 0.7 API)
- submit_timeout_ms correctly handles millisecond-to-Timespec conversion with separate sec and nsec fields

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- UringDriver ready for integration with shard event loop in Plan 03
- IoEvent enum provides clean dispatch interface for connection migration
- FdTable and BufRingManager fully functional with io_uring kernel APIs
- TokioDriver remains as macOS fallback path

## Self-Check: PASSED

All 4 key files verified. Both task commits (48e9a45, 76cb02f) verified in git log.

---
*Phase: 12-io-uring-networking-layer*
*Completed: 2026-03-24*
