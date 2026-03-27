---
phase: 23-end-to-end-performance-validation-and-benchmark-suite
plan: 02
subsystem: benchmarking
tags: [memtier_benchmark, memory-overhead, snapshot-spike, open-loop, rate-limiting, coordinated-omission]

# Dependency graph
requires:
  - phase: 23-01
    provides: bench-v2.sh with server lifecycle, throughput sweep, stub functions
provides:
  - run_memory_suite() with INFO MEMORY per-key overhead measurement
  - run_snapshot_suite() with BGSAVE RSS spike polling
  - run_open_loop_suite() with rate-limited memtier at 5 rate points
affects: [23-03]

# Tech tracking
tech-stack:
  added: []
  patterns: [INFO MEMORY used_memory for per-key overhead, RSS polling for snapshot spike, --rate-limiting per-connection rate derivation]

key-files:
  created: []
  modified: [bench-v2.sh]

key-decisions:
  - "INFO MEMORY used_memory (not RSS) for per-key overhead -- RSS includes TCP buffers, FD tables, OS overhead"
  - "RSS (not used_memory) for snapshot spike -- captures full system-visible memory pressure during BGSAVE"
  - "Open-loop rates divided by total_conns (threads * 50 clients) for per-connection --rate-limiting"
  - "Conservative 100K ops/sec fallback when no closed-loop results available for rate derivation"

patterns-established:
  - "measure_memory_overhead: baseline -> load -> diff / num_keys -> FLUSHALL cleanup"
  - "measure_snapshot_spike: steady RSS -> BGSAVE -> 100ms poll loop -> spike % calculation"
  - "run_open_loop_suite: extract peak from closed-loop -> compute 5 rate points -> run each"

requirements-completed: []

# Metrics
duration: 2min
completed: 2026-03-25
---

# Phase 23 Plan 02: Memory Overhead, Snapshot Spike, and Open-Loop Rate-Limited Suites Summary

**Three measurement suites replacing stubs: INFO MEMORY per-key overhead with FLUSHALL cleanup, BGSAVE RSS spike polling at 100ms intervals, and coordinated-omission-aware open-loop tests at 10/25/50/75/90% of closed-loop peak**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-24T18:55:26Z
- **Completed:** 2026-03-24T18:57:06Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Replaced run_memory_suite() stub with measure_memory_overhead() using INFO MEMORY used_memory (not RSS) for accurate allocator-tracked per-key overhead
- Replaced run_snapshot_suite() stub with measure_snapshot_spike() using RSS polling at 100ms intervals during BGSAVE, with LASTSAVE-based completion detection
- Replaced run_open_loop_suite() stub with rate-limited memtier benchmarks at 5 rate points (10/25/50/75/90% of peak), with per-connection rate calculation
- FLUSHALL cleanup after memory measurement (Pitfall 7 compliance)
- Smoke test mode: memory/snapshot cap at 10K keys, open-loop runs only 50% rate point
- Conservative fallback (100K ops/sec) when no closed-loop results available for rate derivation

## Task Commits

Each task was committed atomically:

1. **Task 1+2: Replace all three stub functions with real implementations** - `342d198` (feat)

Note: Both tasks modified the same contiguous stub block in bench-v2.sh, so they were committed together as a single atomic change.

## Files Created/Modified
- `bench-v2.sh` - Memory overhead suite (measure_memory_overhead + run_memory_suite), snapshot spike suite (measure_snapshot_spike + run_snapshot_suite), open-loop suite (run_open_loop + run_open_loop_suite)

## Decisions Made
- INFO MEMORY used_memory chosen over RSS for per-key overhead (RSS includes TCP buffers, FD tables, OS overhead that are not per-key)
- RSS chosen for snapshot spike measurement (captures full system-visible memory pressure during fork/copy-on-write)
- Per-connection rate = (peak * percentage) / total_conns ensures --rate-limiting flag works correctly (it applies per connection, not globally)
- 100K ops/sec conservative fallback when no closed-loop results exist prevents division-by-zero and gives reasonable rate points for standalone open-loop testing

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- bench-v2.sh ready for Plan 03 to implement generate_report() producing BENCHMARK-v2.md
- Only the generate_report() stub remains (Plan 03)
- All measurement suites produce CSV files in RESULTS_DIR for report consumption

---
*Phase: 23-end-to-end-performance-validation-and-benchmark-suite*
*Completed: 2026-03-25*
