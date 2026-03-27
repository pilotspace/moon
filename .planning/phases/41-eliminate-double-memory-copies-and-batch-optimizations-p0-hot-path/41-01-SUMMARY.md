---
phase: 41-eliminate-double-memory-copies-and-batch-optimizations-p0-hot-path
plan: 01
subsystem: server
tags: [zero-copy, bytes, monoio, memcpy, hot-path, io-buf]

# Dependency graph
requires:
  - phase: 40-fix-all-p0-p1-p2
    provides: "Batch dispatch and buffer reuse foundation"
provides:
  - "Zero-copy write path via BytesMut::freeze() -> Bytes for monoio write_all"
  - "Eliminated read-path zero-fill overhead via unsafe set_len"
  - "Pre-allocated frames Vec outside main loop with drain/clear reuse"
affects: [performance-benchmarks, monoio-handler, pipeline-throughput]

# Tech tracking
tech-stack:
  added: [monoio/bytes feature]
  patterns: [freeze-zero-copy-write, unsafe-set-len-read-optimization, vec-drain-reuse]

key-files:
  created: []
  modified:
    - Cargo.toml
    - src/server/connection.rs

key-decisions:
  - "Used .freeze() for zero-copy BytesMut-to-Bytes conversion instead of .to_vec() memcpy on all 16 write paths"
  - "Used unsafe set_len instead of resize(8192, 0) to eliminate per-read 8KB zero-fill memset"
  - "Used frames.drain(..) to move ownership out of pre-allocated Vec while retaining allocation"

patterns-established:
  - "Zero-copy write pattern: encode into BytesMut, .freeze() to Bytes, pass to monoio write_all"
  - "Unsafe read buffer pattern: set_len on pre-allocated Vec to skip zero-fill before ownership I/O read"

requirements-completed: [MEMCPY-01, MEMCPY-02, MEMCPY-03]

# Metrics
duration: 27min
completed: 2026-03-26
---

# Phase 41 Plan 01: Eliminate Double Memory Copies Summary

**Zero-copy write path via BytesMut::freeze() replacing 16 .to_vec() memcpy calls, plus read-path zero-fill elimination and frames Vec pre-allocation**

## Performance

- **Duration:** 27 min
- **Started:** 2026-03-26T13:33:56Z
- **Completed:** 2026-03-26T14:00:56Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Eliminated all 16 .to_vec() memcpy calls in monoio write paths with zero-copy .freeze()
- Enabled monoio "bytes" feature for Bytes IoBuf support
- Removed per-read-cycle 8KB zero-fill memset via unsafe set_len
- Pre-allocated frames Vec outside main loop, reused via drain/clear pattern

## Task Commits

Each task was committed atomically:

1. **Task 1: Enable monoio bytes feature and eliminate write-path .to_vec() copy** - `f4fc71c` (feat)
2. **Task 2: Eliminate read-path double copy and pre-allocate frames Vec** - `d25548f` (feat)

## Files Created/Modified
- `Cargo.toml` - Added "bytes" feature to monoio dependency
- `src/server/connection.rs` - Zero-copy write path (.freeze()), read-path optimization (unsafe set_len), frames Vec pre-allocation

## Decisions Made
- Used .freeze() which works on any BytesMut (both split() results and standalone buffers)
- Used frames.drain(..) instead of borrowing iteration to allow ownership transfer while retaining allocation
- Added explicit bytes::Bytes type annotations on all write_all return types for clarity

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed frames ownership move in for loop**
- **Found during:** Task 2 (pre-allocate frames Vec)
- **Issue:** Moving frames Vec declaration outside the loop caused `for frame in frames` to move the Vec, preventing reuse
- **Fix:** Changed `for frame in frames` to `for frame in frames.drain(..)` to drain elements while keeping allocation
- **Files modified:** src/server/connection.rs
- **Verification:** cargo check --features runtime-monoio passes clean
- **Committed in:** d25548f (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Essential fix for Vec reuse pattern. No scope creep.

## Issues Encountered
- Tests require --no-default-features --features runtime-tokio (pre-existing: both runtime features cannot be enabled simultaneously due to duplicate definitions). Verified compilation and test binary compilation pass.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Zero-copy write path and optimized read path ready for benchmarking
- Pipeline throughput improvement expected from eliminating ~27-45% memcpy overhead

---
*Phase: 41-eliminate-double-memory-copies-and-batch-optimizations-p0-hot-path*
*Completed: 2026-03-26*

## Self-Check: PASSED
