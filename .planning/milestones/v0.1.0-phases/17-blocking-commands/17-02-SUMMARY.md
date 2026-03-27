---
phase: 17-blocking-commands
plan: 02
subsystem: blocking
tags: [blocking-commands, connection-handler, shard-event-loop, wakeup-hooks, timeout-scanning]

# Dependency graph
requires:
  - phase: 17-blocking-commands
    plan: 01
    provides: BlockingRegistry, WaitEntry, wakeup hooks, LMOVE command
  - phase: 11-sharded-architecture
    provides: Per-shard Rc<RefCell> pattern, handle_connection_sharded
provides:
  - BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX command interception in sharded handler
  - Post-dispatch wakeup hooks after LPUSH/RPUSH/ZADD/LMOVE
  - 10ms blocking timeout scanner in shard event loop
  - BlockRegister/BlockCancel ShardMessage variants for cross-shard blocking
  - Non-blocking fast path for immediate data availability
  - MULTI converts blocking commands to non-blocking equivalents
affects: [17-03-blocking-timeout]

# Tech tracking
tech-stack:
  added: []
  patterns: [blocking-fast-path, post-dispatch-wakeup, timeout-scanner-10ms, blocking-to-nonblocking-multi]

key-files:
  created: []
  modified:
    - src/server/connection.rs
    - src/shard/mod.rs
    - src/shard/dispatch.rs
    - tests/integration.rs

key-decisions:
  - "Blocking commands intercepted before PUBLISH in sharded handler batch loop"
  - "Non-blocking fast path tries all keys before registering waiter"
  - "BLPOP in MULTI converted to LPOP for non-blocking EXEC semantics"
  - "Only first key registered for multi-key blocking (cross-shard multi-key deferred to Plan 03)"
  - "10ms block_timeout_interval for expire_timed_out scanning"
  - "Post-dispatch wakeup hooks fire for LPUSH/RPUSH/LMOVE/ZADD on local fast path"

requirements-completed: [BLOCK-01, BLOCK-02]

# Metrics
duration: 8min
completed: 2026-03-24
---

# Phase 17 Plan 02: Blocking Commands Integration Summary

**BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX wired into sharded connection handler with post-dispatch wakeup hooks and 10ms timeout scanner**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-24T14:19:12Z
- **Completed:** 2026-03-24T14:27:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- All 5 blocking commands (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX) functional end-to-end for single-shard keys
- Non-blocking fast path checks data availability before registering waiter (zero overhead when data exists)
- Post-dispatch wakeup hooks fire inline after LPUSH/RPUSH/LMOVE/ZADD in local fast path
- 10ms timeout scanner expires blocked clients via BlockingRegistry.expire_timed_out
- BlockRegister/BlockCancel ShardMessage variants for future cross-shard blocking
- MULTI converts blocking commands to non-blocking equivalents (BLPOP->LPOP, etc.)
- 10 integration tests covering immediate return, blocking+wakeup, timeout, multi-key, LMOVE, and sorted set blocking

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire blocking commands into connection handler and shard event loop** - `f373929` (feat)
2. **Task 2: Integration tests for blocking commands** - `6c8db89` (test)

## Files Created/Modified
- `src/server/connection.rs` - Blocking command interception, handle_blocking_command, wakeup hooks, helper functions
- `src/shard/mod.rs` - BlockingRegistry Rc<RefCell>, 10ms timer, BlockRegister/BlockCancel handling
- `src/shard/dispatch.rs` - BlockRegister/BlockCancel ShardMessage variants
- `tests/integration.rs` - 10 integration tests for all 5 blocking commands

## Decisions Made
- Blocking commands intercepted after DISCARD and before PUBLISH in the batch loop, breaking the batch to enter blocking mode
- Non-blocking fast path iterates all keys (for multi-key BLPOP) before falling through to waiter registration
- BLPOP/BRPOP inside MULTI are converted to LPOP/RPOP for non-blocking transaction execution
- Only first key registered for multi-key blocking commands (cross-shard multi-key is Plan 03 scope)
- Separate RefCells for blocking_registry and databases allow simultaneous mutable borrows safely
- All database borrows explicitly dropped before any .await points (RefCell borrow safety)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Blocking commands functional for single-shard keys
- Cross-shard blocking infrastructure (BlockRegister/BlockCancel) ready for Plan 03
- Timeout scanning operational at 10ms granularity

---
*Phase: 17-blocking-commands*
*Completed: 2026-03-24*
