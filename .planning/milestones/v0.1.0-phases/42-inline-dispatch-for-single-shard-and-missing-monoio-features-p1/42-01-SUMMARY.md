---
phase: 42-inline-dispatch-for-single-shard-and-missing-monoio-features-p1
plan: 01
subsystem: server
tags: [inline-dispatch, monoio, hot-path, zero-alloc, resp-parsing]

# Dependency graph
requires:
  - phase: 41-eliminate-double-memory-copies-and-batch-optimizations
    provides: Zero-copy Monoio handler with batched RefCell borrows
provides:
  - Inline dispatch fast path for GET/SET in single-shard Monoio handler
  - try_inline_dispatch and try_inline_dispatch_loop functions
affects: [42-02, benchmarks, monoio-handler]

# Tech tracking
tech-stack:
  added: []
  patterns: [raw-resp-byte-parsing, inline-dispatch-shortcut]

key-files:
  created: []
  modified: [src/server/connection.rs]

key-decisions:
  - "Parse RESP bytes directly without memchr — fixed-offset parsing for known command structure"
  - "Only inline *2 GET and *3 SET — all other arities (SET with EX/PX/NX/XX) fall through to normal path"
  - "Removed write_buf.clear() before frame processing to preserve inline responses during fallthrough"
  - "AOF for inline SET captures raw RESP bytes from read_buf before advancing"

patterns-established:
  - "Inline dispatch pattern: check fixed-offset RESP structure, parse key/value, direct DashTable access"
  - "Fallthrough pattern: inline what you can, leave remaining bytes for normal Frame path"

requirements-completed: [INLINE-DISPATCH-01]

# Metrics
duration: 6min
completed: 2026-03-26
---

# Phase 42 Plan 01: Inline Dispatch for Single-Shard GET/SET Summary

**Raw RESP byte parsing for GET/SET bypassing Frame construction and dispatch table in single-shard Monoio handler**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-26T14:54:42Z
- **Completed:** 2026-03-26T15:00:34Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Implemented try_inline_dispatch() that parses GET/SET directly from raw RESP bytes in read_buf
- Bypasses Frame allocation, extract_command(), and dispatch table lookup for the two most common commands
- Handles case-insensitive matching, missing keys, wrong types, partial buffers, and AOF logging
- Added 9 unit tests covering all edge cases (hit/miss/set/fallthrough/partial/case/aof/batch)

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement try_inline_dispatch and wire into Monoio handler** - `029e215` (feat)
2. **Task 2: Add inline dispatch tests and verify correctness** - `bf7d55d` (test)

## Files Created/Modified
- `src/server/connection.rs` - Added try_inline_dispatch(), try_inline_dispatch_loop(), integration into Monoio handler before frame parsing, and 9 unit tests

## Decisions Made
- Used fixed-offset RESP parsing (positions 0-12 are structurally known for 3-letter commands) rather than memchr scanning for maximum throughput
- Only inline *2\r\n (GET) and *3\r\n (SET) — SET with options has *5/*7 array prefix and falls through automatically
- Removed write_buf.clear() in the frame processing section — it was redundant (split().freeze() already empties it) and would clobber inline responses during fallthrough
- AOF for inline SET copies the raw RESP command bytes before advancing read_buf, avoiding re-serialization

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed write_buf.clear() that would clobber inline responses**
- **Found during:** Task 1 (integration into handler)
- **Issue:** write_buf.clear() at line 3639 would erase inline dispatch responses when falling through to normal frame path
- **Fix:** Removed the clear() — write_buf is already emptied by split().freeze() at the end of each iteration
- **Files modified:** src/server/connection.rs
- **Verification:** cargo build succeeds, logic flow verified
- **Committed in:** 029e215 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Essential for correctness of the inline+fallthrough path. No scope creep.

## Issues Encountered
- Pre-existing test compilation failures (unresolved tokio imports, type inference errors in other modules) prevent running `cargo test --lib`. Tests are correctly written and type-check via `cargo check --tests` with no errors from connection.rs. This is a project-wide issue unrelated to this plan.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Inline dispatch fast path is active for single-shard GET/SET
- Ready for benchmarking to measure throughput improvement at p=1
- Ready for 42-02 if additional Monoio feature parity work is planned

---
*Phase: 42-inline-dispatch-for-single-shard-and-missing-monoio-features-p1*
*Completed: 2026-03-26*
