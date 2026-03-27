---
phase: 23-end-to-end-performance-validation-and-benchmark-suite
plan: 01
subsystem: benchmarking
tags: [memtier_benchmark, benchmark, zipfian, hdrhistogram, performance]

# Dependency graph
requires:
  - phase: 06-setup-and-benchmark-real-use-cases
    provides: bench.sh server lifecycle patterns (start/wait/benchmark/kill)
provides:
  - bench-v2.sh with server lifecycle management, P:P pre-population, Z:Z Zipfian throughput sweep
  - --smoke-test mode for fast CI/dev validation (1K keys, 10s)
  - Stub hooks for memory, snapshot, open-loop suites (Plan 02) and report generation (Plan 03)
affects: [23-02, 23-03]

# Tech tracking
tech-stack:
  added: [memtier_benchmark]
  patterns: [P:P sequential pre-population, Z:Z Zipfian benchmark, 5-iteration median methodology, HdrHistogram file output]

key-files:
  created: [bench-v2.sh]
  modified: []

key-decisions:
  - "FLUSHALL after rust-redis suite and before Redis 7.x suite to reset key state"
  - "RUN_THROUGHPUT flag added (not in plan) to support --memory-only/--snapshot-only/--open-loop-only without running primary throughput"
  - "redis-cli -h SERVER_HOST for DBSIZE verification to support remote server mode"

patterns-established:
  - "P:P pre-population before Z:Z benchmark to ensure full key coverage (Pitfall 2 guard)"
  - "Smoke test overrides: reduced KEY_MAX, TEST_TIME, WARMUP_TIME, RUN_COUNT for fast validation"

requirements-completed: []

# Metrics
duration: 2min
completed: 2026-03-25
---

# Phase 23 Plan 01: Benchmark Automation Script Summary

**bench-v2.sh with memtier_benchmark: P:P sequential pre-population, Z:Z Zipfian throughput sweep across 4 concurrency levels and 3 pipeline depths, --smoke-test mode for dev validation**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-24T18:51:39Z
- **Completed:** 2026-03-24T18:53:50Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Created bench-v2.sh with full server lifecycle management (start/wait/benchmark/kill) for both rust-redis and Redis 7.x
- Pre-population using memtier_benchmark with --key-pattern=P:P --ratio=1:0 ensuring all N keys exist before benchmarking
- Primary throughput sweep with Z:Z Zipfian distribution, 5 iterations, HdrHistogram output, across clients=(1,10,50,100) x pipeline=(1,16,64)
- --smoke-test mode reduces to 1K keys, 10s runs, 1 iteration for fast dev/CI validation
- Missing memtier_benchmark detection with install instructions for macOS, Ubuntu, and source build

## Task Commits

Each task was committed atomically:

1. **Task 1: Write bench-v2.sh core (server lifecycle + pre-population + primary throughput test)** - `b54e955` (feat)

## Files Created/Modified
- `bench-v2.sh` - Primary v2 benchmark runner with memtier_benchmark, server lifecycle, pre-population, throughput sweep, and stub hooks for Plan 02/03

## Decisions Made
- Added RUN_THROUGHPUT flag to cleanly support --memory-only/--snapshot-only/--open-loop-only modes that skip primary throughput (plan had `|| true` fallthrough which was fragile)
- Used `redis-cli -h "$SERVER_HOST"` for DBSIZE verification to properly support remote server mode
- Moved FLUSHALL before stop_rust_server (not after) to ensure server is still running when flush executes

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added RUN_THROUGHPUT flag for section-only modes**
- **Found during:** Task 1 (argument parsing)
- **Issue:** Plan used `|| true` pattern for memory/snapshot/open-loop guards which would run throughput even in --memory-only mode
- **Fix:** Added RUN_THROUGHPUT=true flag, set to false for --memory-only/--snapshot-only/--open-loop-only, wrapped throughput suite in conditional
- **Files modified:** bench-v2.sh
- **Verification:** `bash bench-v2.sh --help` shows all flags, `bash -n` passes
- **Committed in:** b54e955

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential for correctness of section-only modes. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- bench-v2.sh ready for Plan 02 to implement run_memory_suite(), run_snapshot_suite(), run_open_loop_suite()
- bench-v2.sh ready for Plan 03 to implement generate_report() producing BENCHMARK-v2.md
- Stub functions prevent undefined function errors when running current script

---
*Phase: 23-end-to-end-performance-validation-and-benchmark-suite*
*Completed: 2026-03-25*
