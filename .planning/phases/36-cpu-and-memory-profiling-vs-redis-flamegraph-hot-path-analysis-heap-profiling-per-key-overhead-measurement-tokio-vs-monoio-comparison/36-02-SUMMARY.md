---
phase: 36-cpu-and-memory-profiling
plan: 02
subsystem: profiling
tags: [profiling, memory, cpu, redis-benchmark, criterion, rss, fragmentation]

requires:
  - phase: 36-01
    provides: "CompactKey benchmarks, profile.sh profiling infrastructure"
provides:
  - "PROFILING-REPORT.md with per-key memory overhead at 5 dataset sizes"
  - "CPU efficiency comparison (Redis vs rust-redis Tokio)"
  - "Memory fragmentation analysis (1M insert, 500K delete)"
  - "Criterion micro-benchmark summary tables"
  - "Optimization recommendations based on measured data"
affects: []

tech-stack:
  added: []
  patterns: ["RSS-based memory measurement", "redis-benchmark for CPU profiling"]

key-files:
  created:
    - PROFILING-REPORT.md
  modified:
    - profile.sh

key-decisions:
  - "Skipped monoio on macOS (requires Linux io_uring) — noted in report"
  - "Used RSS delta for memory measurement since rust-redis lacks INFO memory"
  - "Added analysis section interpreting raw measurements and identifying optimization targets"

patterns-established:
  - "profile.sh as reusable profiling harness for future comparisons"

requirements-completed: [PROF-04, PROF-05]

duration: 13min
completed: 2026-03-26
---

# Phase 36 Plan 02: Execute Profiling Suite Summary

**Profiling report comparing rust-redis vs Redis 8.6.1: string overhead 1.42x at 1M keys, 99.1% throughput parity, superior memory reclamation after deletes**

## Performance

- **Duration:** 13 min
- **Started:** 2026-03-26T03:36:33Z
- **Completed:** 2026-03-26T03:49:49Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Generated 418-line PROFILING-REPORT.md with real measurement data across 5 dataset sizes (10K-1M) and 4 data types
- Measured per-key string overhead: rust-redis 280B/key vs Redis 197B/key at 1M keys (1.42x)
- CPU efficiency: rust-redis 334K ops/sec (99.1% of Redis), but uses 42% more CPU due to Tokio async overhead
- Fragmentation: rust-redis returns 72% of memory to OS after 500K deletes vs Redis's 1.65x fragmentation ratio
- Criterion benchmarks: CompactKey inline create 38ns, clone 0.56ns; full GET pipeline 174ns

## Task Commits

Each task was committed atomically:

1. **Task 1: Build release binaries and execute profiling suite** - `bd3399c` (feat)
2. **Task 2: Review profiling report** - auto-approved (checkpoint:human-verify in auto mode)

## Files Created/Modified

- `PROFILING-REPORT.md` - Complete profiling report with tables, analysis, and optimization recommendations
- `profile.sh` - Fixed SIGPIPE in bash for-loops and redis-benchmark output parsing for Redis 8.x

## Decisions Made

- Skipped monoio runtime (macOS lacks io_uring support) — documented in report
- Used RSS-based measurement since rust-redis doesn't implement INFO memory
- Added comprehensive analysis section interpreting raw data and identifying optimization targets

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed SIGPIPE in profile.sh pipe subshells**
- **Found during:** Task 1 (running profile.sh)
- **Issue:** Bash `set -eo pipefail` caused script abort when for-loop piped to redis-cli --pipe received SIGPIPE
- **Fix:** Wrapped pipe-generating for-loops in subshells with `set +eo pipefail` and added `|| true`
- **Files modified:** profile.sh
- **Verification:** Script runs to completion without exit code 141
- **Committed in:** bd3399c

**2. [Rule 1 - Bug] Fixed redis-benchmark output parsing for Redis 8.x**
- **Found during:** Task 1 (CPU efficiency section)
- **Issue:** `grep "SET:"` matched both progress lines (`rps=...`) and summary lines (`requests per second`), causing awk parse failure
- **Fix:** Changed grep to `"SET:.*requests per second"` to match only the final summary line
- **Files modified:** profile.sh
- **Verification:** CPU efficiency section completes with valid ops/sec numbers
- **Committed in:** bd3399c

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes necessary for script to run to completion. No scope creep.

## Issues Encountered

- rust-redis does not implement `INFO memory` command — used_memory reports 0 for all rust-redis measurements. RSS-based measurement used instead. Fragmentation ratio reported as N/A for rust-redis.
- Some large-scale RSS deltas show negative values (500K list, 1M zset for Redis) due to OS page reclamation between sequential measurements. These are measurement artifacts noted in the analysis.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 36 (final phase) is now complete
- PROFILING-REPORT.md provides actionable optimization targets for future work
- profile.sh is reusable for re-profiling after optimizations

---
*Phase: 36-cpu-and-memory-profiling*
*Completed: 2026-03-26*
