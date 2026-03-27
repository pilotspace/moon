---
phase: 36-cpu-and-memory-profiling
plan: 01
subsystem: profiling
tags: [criterion, benchmarks, memory-profiling, cpu-profiling, rss, fragmentation]

requires:
  - phase: 18-compact-entry-and-key
    provides: CompactKey SSO implementation (24-byte inline/heap key struct)
provides:
  - CompactKey inline vs heap Criterion micro-benchmarks
  - Automated profiling script measuring per-key memory, fragmentation, CPU efficiency
  - PROFILING-REPORT.md generation with comparison tables
affects: [36-02-execution, performance-tuning]

tech-stack:
  added: []
  patterns: [callback-based server iteration in bash, background CPU sampling]

key-files:
  created:
    - benches/compact_key.rs
    - profile.sh
  modified:
    - Cargo.toml

key-decisions:
  - "Bytes is 32 bytes in bytes 1.x (not 24 as plan assumed); CompactKey saves 8 bytes per key vs Bytes"
  - "Used callback pattern for per-server measurement loops instead of bash multi-variable for (invalid syntax)"

patterns-established:
  - "Profile script pattern: measure_per_server callback with server_label/port/pid args"
  - "CPU sampling: background process with 1s interval writing to temp file"

requirements-completed: [PROF-01, PROF-02, PROF-03]

duration: 6min
completed: 2026-03-26
---

# Phase 36 Plan 01: CPU & Memory Profiling Infrastructure Summary

**CompactKey Criterion benchmarks (11 tests: inline 2.4x faster than heap) and profile.sh script automating per-key memory, fragmentation, and CPU efficiency measurements across Redis/Tokio/Monoio**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-26T03:28:18Z
- **Completed:** 2026-03-26T03:34:44Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- 11 CompactKey benchmarks covering create/as_bytes/hash/eq/clone for inline and heap paths
- Inline key creation is 2.4x faster than heap (37ns vs 88ns), clone is sub-nanosecond (556ps)
- 711-line profile.sh script with 4 measurement sections, argument parsing, server lifecycle management
- Script generates structured PROFILING-REPORT.md with per-key overhead, fragmentation, CPU, and Criterion tables

## Task Commits

Each task was committed atomically:

1. **Task 1: CompactKey Criterion benchmark** - `21194bc` (feat)
2. **Task 2: Create profile.sh automated profiling script** - `5989804` (feat)

## Files Created/Modified
- `benches/compact_key.rs` - 11 Criterion benchmarks for CompactKey inline vs heap paths
- `profile.sh` - Automated profiling: per-key memory, fragmentation, CPU efficiency, Criterion
- `Cargo.toml` - Added [[bench]] compact_key target

## Decisions Made
- Bytes is 32 bytes in bytes 1.x, not 24 as plan assumed; adjusted assertion to verify CompactKey saves space vs Bytes
- Used callback-based measure_per_server pattern to avoid invalid bash multi-variable for loops

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed incorrect Bytes size assertion**
- **Found during:** Task 1 (CompactKey benchmark)
- **Issue:** Plan said "assert CompactKey is 24 bytes, same as Bytes" but Bytes is 32 bytes in bytes 1.x
- **Fix:** Changed assertion to verify Bytes >= 24 bytes, documenting the 8-byte savings
- **Files modified:** benches/compact_key.rs
- **Verification:** cargo bench --bench compact_key runs all 11 benchmarks successfully
- **Committed in:** 21194bc (Task 1 commit)

**2. [Rule 3 - Blocking] Fixed invalid bash multi-variable for syntax**
- **Found during:** Task 2 (profile.sh)
- **Issue:** Initial script used `for a b c in ...` which is invalid bash syntax
- **Fix:** Refactored to callback pattern with measure_per_server helper function
- **Files modified:** profile.sh
- **Verification:** bash -n profile.sh passes syntax check
- **Committed in:** 5989804 (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking)
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Issues Encountered
None beyond auto-fixed deviations.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- profile.sh ready for execution in Plan 02 to produce PROFILING-REPORT.md
- All Criterion benchmarks operational and producing timing data

---
*Phase: 36-cpu-and-memory-profiling*
*Completed: 2026-03-26*
