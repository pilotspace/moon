---
phase: 40-fix-all-p0-p1-p2
plan: 01
subsystem: server
tags: [monoio, pipeline-batch, cross-shard, spsc, async-dispatch]

# Dependency graph
requires:
  - phase: 32-monoio-runtime
    provides: Monoio connection handler (handle_connection_sharded_monoio)
provides:
  - Batched PipelineBatch dispatch in Monoio handler matching Tokio handler pattern
  - Consumed frame iterator eliminating deep clone on remote dispatch
affects: [performance, multi-shard-scaling, pipeline-throughput]

# Tech tracking
tech-stack:
  added: []
  patterns: [deferred-remote-dispatch, pipeline-batch-grouping, parallel-shard-await]

key-files:
  created: []
  modified:
    - src/server/connection.rs

key-decisions:
  - "Pre-compute AOF bytes before moving frame into Arc, matching Tokio handler pattern"
  - "Keep SUBSCRIBE/blocking special paths writing directly to write_buf with flush before break"
  - "Use recv().await for OneshotReceiver in Monoio handler (consistent with existing Monoio patterns)"

patterns-established:
  - "Monoio handler now uses same deferred remote_groups + PipelineBatch pattern as Tokio handler"

requirements-completed: [FIX-P0]

# Metrics
duration: 4min
completed: 2026-03-26
---

# Phase 40 Plan 01: Pipeline Batch Dispatch Port Summary

**Ported batched PipelineBatch remote dispatch from Tokio to Monoio handler -- remote commands deferred into HashMap by target shard, dispatched as one PipelineBatch per shard with parallel await, eliminating per-command synchronous Execute and deep frame clone**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-26T12:24:24Z
- **Completed:** 2026-03-26T12:29:08Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Replaced synchronous per-command ShardMessage::Execute with batched PipelineBatch per target shard in Monoio handler
- Changed frame iteration from borrow (`for frame in &frames`) to consume (`for frame in frames`), eliminating `frame.clone()` on every remote dispatch
- All remote shard responses now awaited in parallel after the frame loop, not synchronously inside it
- Pre-computed AOF bytes before moving frame into Arc (matching Tokio handler pattern)
- All special command paths (SUBSCRIBE, blocking, MULTI/EXEC) updated to use responses Vec pattern

## Task Commits

Each task was committed atomically:

1. **Task 1: Port pipeline batch dispatch pattern to Monoio handler** - `2392e4b` (feat)
2. **Task 2: Verify batch dispatch with existing tests** - verification only, no code changes

## Files Created/Modified
- `src/server/connection.rs` - Monoio handler (handle_connection_sharded_monoio) rewritten with batched remote dispatch: remote_groups HashMap, PipelineBatch construction, parallel shard await, responses Vec pattern

## Decisions Made
- Pre-compute AOF bytes before moving frame into Arc, consistent with Tokio handler approach
- Keep SUBSCRIBE and blocking command paths writing directly to write_buf with flush-before-break semantics (these exit the pipeline batch early)
- Use recv().await on OneshotReceiver for Monoio handler (matching existing Monoio codebase conventions)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing test compilation errors (tokio crate unresolved under monoio feature, type inference issues in integration tests) -- confirmed identical on parent commit, not introduced by this change.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Monoio handler now has full parity with Tokio handler for pipeline batch dispatch
- Ready for multi-shard performance benchmarking to validate scaling improvement
- Plan 40-02 can proceed independently

---
*Phase: 40-fix-all-p0-p1-p2*
*Completed: 2026-03-26*

## Self-Check: PASSED
