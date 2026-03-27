---
phase: 08-resp-parser-simd-acceleration
plan: 03
subsystem: performance
tags: [bumpalo, arena, benchmark, criterion, memchr, naive-parser]

# Dependency graph
requires:
  - phase: 08-02
    provides: "Per-connection bumpalo arena infrastructure and pipeline benchmark"
provides:
  - "Concrete bumpalo::collections::Vec allocation in connection.rs batch processing"
  - "Naive byte-by-byte reference parser benchmark for pipeline speedup comparison"
affects: [09-compact-value-encoding]

# Tech tracking
tech-stack:
  added: [bumpalo/collections]
  patterns: [arena-scoped-scratch-buffer, naive-reference-benchmark]

key-files:
  created: []
  modified: [src/server/connection.rs, benches/resp_parsing.rs, Cargo.toml]

key-decisions:
  - "BumpVec scoped within synchronous Phase 2 block and dropped before async response writing to maintain Send safety"
  - "Enabled bumpalo collections feature in Cargo.toml for BumpVec support"
  - "Naive parser skips Frame construction for fair CRLF-scanning comparison; noted that actual pre-optimization parser was slower"

patterns-established:
  - "Arena scratch buffers must be dropped before any .await to maintain tokio::spawn Send safety"

requirements-completed: [ARENA-01, SIMD-04]

# Metrics
duration: 4min
completed: 2026-03-24
---

# Phase 8 Plan 3: Gap Closure Summary

**Arena-backed BumpVec for write-command index tracking and naive byte-by-byte reference parser benchmark for pipeline speedup comparison**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-24T05:45:50Z
- **Completed:** 2026-03-24T05:49:37Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Closed Gap 1: connection.rs now has a concrete `bumpalo::collections::Vec<usize>` allocation that tracks write-command response indices during batch dispatch
- Closed Gap 2: resp_parsing.rs now has a naive byte-by-byte RESP parser with `parse_pipeline_32cmd_naive` benchmark for direct Criterion comparison against the optimized pipeline parser
- All 487 tests pass unchanged

## Task Commits

Each task was committed atomically:

1. **Task 1: Add concrete arena scratch allocation for AOF serialization** - `fc6e875` (feat)
2. **Task 2: Add naive reference parser benchmark for pipeline speedup comparison** - `16b2a87` (feat)

## Files Created/Modified
- `Cargo.toml` - Enabled bumpalo `collections` feature
- `src/server/connection.rs` - Added `BumpVec<usize>` import and arena-backed write-command index tracking in Phase 2 dispatch block
- `benches/resp_parsing.rs` - Added naive byte-by-byte RESP parser functions and `parse_pipeline_32cmd_naive` benchmark

## Decisions Made
- Enabled bumpalo `collections` feature in Cargo.toml (required for `bumpalo::collections::Vec`)
- Scoped BumpVec within synchronous Phase 2 block with explicit `drop()` before any `.await` to satisfy tokio::spawn Send requirements -- `Bump` is `!Sync`, so `&Bump` references cannot be held across await points
- Naive parser intentionally skips Frame construction to provide a fair CRLF-scanning-only comparison; documented that this understates the real speedup

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Enabled bumpalo collections feature**
- **Found during:** Task 1
- **Issue:** `bumpalo::collections` module requires the `collections` Cargo feature which was not enabled
- **Fix:** Changed Cargo.toml from `bumpalo = "3.20"` to `bumpalo = { version = "3.20", features = ["collections"] }`
- **Files modified:** Cargo.toml
- **Verification:** Compilation succeeds
- **Committed in:** fc6e875 (Task 1 commit)

**2. [Rule 3 - Blocking] Scoped BumpVec to avoid Send safety violation**
- **Found during:** Task 1
- **Issue:** BumpVec holding `&arena` (Bump is !Sync) across .await points made the handle_connection future !Send, breaking tokio::spawn
- **Fix:** Moved BumpVec creation into synchronous Phase 2 dispatch block with explicit drop() before async response writing begins
- **Files modified:** src/server/connection.rs
- **Verification:** All 487 tests pass, cargo compiles without Send errors
- **Committed in:** fc6e875 (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both fixes necessary for compilation. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 8 verification gaps are now closed: arena has concrete allocation, benchmarks have naive reference for comparison
- Ready for Phase 9: Compact Value Encoding and Entry Optimization

---
*Phase: 08-resp-parser-simd-acceleration*
*Completed: 2026-03-24*

## Self-Check: PASSED

All files exist. All commit hashes verified.
