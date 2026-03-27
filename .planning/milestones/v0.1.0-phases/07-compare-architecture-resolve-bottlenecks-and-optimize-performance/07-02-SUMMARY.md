---
phase: 07-compare-architecture-resolve-bottlenecks-and-optimize-performance
plan: 02
subsystem: server
tags: [parking_lot, pipeline-batching, lock-optimization, concurrency, async]

# Dependency graph
requires:
  - phase: 07-01
    provides: "Compacted Entry struct for reduced memory per key"
provides:
  - "Pipeline batching in connection handler (N locks -> 1 per batch)"
  - "parking_lot::Mutex replacing std::sync::Mutex throughout codebase"
  - "now_or_never frame collection for non-blocking batch assembly"
affects: [07-03, benchmarks, performance]

# Tech tracking
tech-stack:
  added: [parking_lot 0.12]
  patterns: [pipeline-batching, batch-then-flush, lock-free-response-writing]

key-files:
  created: []
  modified:
    - src/server/connection.rs
    - src/server/listener.rs
    - src/server/expiration.rs
    - src/persistence/aof.rs
    - src/persistence/auto_save.rs
    - src/command/persistence.rs
    - Cargo.toml

key-decisions:
  - "parking_lot::Mutex replaces std::sync::Mutex in all 6 files (no poisoning, faster uncontended)"
  - "Pipeline batching via now_or_never() collects up to 1024 frames per batch cycle"
  - "Lock released BEFORE response writing and AOF channel sends"
  - "SUBSCRIBE mid-batch flushes accumulated responses then breaks batch loop"
  - "Transaction AOF entries collected in batch aof_entries vec, sent after batch completes"

patterns-established:
  - "Batch-then-flush: collect frames -> execute under single lock -> write responses outside lock"
  - "Connection-level commands (AUTH, CONFIG, BGSAVE, PUBLISH, SUBSCRIBE) handled without db lock in batch loop"

requirements-completed: [OPT-01, OPT-03]

# Metrics
duration: 4min
completed: 2026-03-23
---

# Phase 7 Plan 2: Pipeline Batching and parking_lot Migration Summary

**Pipeline batching via now_or_never() reduces lock acquisitions from N-per-pipeline to 1-per-batch; parking_lot::Mutex replaces std::sync::Mutex in all 6 files**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-23T12:26:01Z
- **Completed:** 2026-03-23T12:29:40Z
- **Tasks:** 1
- **Files modified:** 7

## Accomplishments
- Replaced std::sync::Mutex with parking_lot::Mutex across all 6 files (no poisoning, faster uncontended locking)
- Implemented pipeline batching: frames collected non-blocking via now_or_never(), executed under single lock, responses written outside lock
- All 15+ connection-level special cases (AUTH, SUBSCRIBE, MULTI/EXEC, CONFIG, BGSAVE, BGREWRITEAOF, PUBLISH, WATCH, eviction) correctly handled within batch loop
- Subscriber mode left unchanged (separate select! loop)
- All 49 tests pass with zero failures

## Task Commits

Each task was committed atomically:

1. **Task 1: Migrate to parking_lot::Mutex and implement pipeline batching** - `2ef6cee` (feat)

## Files Created/Modified
- `Cargo.toml` - Added parking_lot = "0.12" dependency
- `src/server/connection.rs` - Pipeline batching with now_or_never(), parking_lot::Mutex, batch-then-flush pattern
- `src/server/listener.rs` - parking_lot::Mutex for db and pubsub_registry creation
- `src/server/expiration.rs` - parking_lot::Mutex for active expiration background task
- `src/persistence/aof.rs` - parking_lot::Mutex for AOF rewrite snapshot
- `src/persistence/auto_save.rs` - parking_lot::Mutex for auto-save timer
- `src/command/persistence.rs` - parking_lot::Mutex for BGSAVE snapshot

## Decisions Made
- Used parking_lot::Mutex (no poisoning, faster uncontended) over std::sync::Mutex throughout
- Batch size capped at 1024 frames (prevents unbounded memory for pathological pipelines)
- SUBSCRIBE mid-batch flushes accumulated responses and AOF entries before entering subscriber mode, then drops remaining batch frames
- Transaction EXEC AOF entries collected into batch-level aof_entries vec, sent after entire batch completes (outside lock)
- WATCH acquires db lock briefly within the batch for-loop (short-lived, no await while held)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Pipeline batching and parking_lot migration complete
- Ready for Phase 7 Plan 3 (further optimizations or benchmarking)
- All existing tests green, no regressions

---
*Phase: 07-compare-architecture-resolve-bottlenecks-and-optimize-performance*
*Completed: 2026-03-23*
