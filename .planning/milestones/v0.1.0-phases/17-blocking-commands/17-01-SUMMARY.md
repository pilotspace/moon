---
phase: 17-blocking-commands
plan: 01
subsystem: blocking
tags: [tokio, oneshot, blocking-commands, fifo, lmove, wakeup-hooks]

# Dependency graph
requires:
  - phase: 15-bptree-compact-encodings
    provides: BPTree sorted set storage with iter/iter_rev
  - phase: 11-sharded-architecture
    provides: Per-shard Database ownership, Rc<RefCell> pattern
provides:
  - BlockingRegistry with per-key FIFO wait queues (register, pop_front, remove_wait, expire_timed_out)
  - WaitEntry/BlockedCommand/Direction types for blocking command infrastructure
  - try_wake_list_waiter and try_wake_zset_waiter wakeup hook functions
  - Database helper methods (list_pop_front/back, list_push_front/back, zset_pop_min/max)
  - LMOVE command handler with dispatch and AOF registration
affects: [17-02-blocking-integration, 17-03-blocking-timeout]

# Tech tracking
tech-stack:
  added: [tokio::sync::oneshot]
  patterns: [blocking-registry-per-shard, wakeup-hook-post-mutation, wait-id-dedup]

key-files:
  created:
    - src/blocking/mod.rs
    - src/blocking/wakeup.rs
  modified:
    - src/lib.rs
    - src/command/mod.rs
    - src/command/list.rs
    - src/storage/db.rs
    - src/persistence/aof.rs

key-decisions:
  - "BlockingRegistry uses HashMap<(usize, Bytes), VecDeque<WaitEntry>> for per-key FIFO queues"
  - "wait_keys HashMap tracks cross-key registrations for O(1) cleanup on wakeup"
  - "Database thin wrapper methods (list_pop_front/back, zset_pop_min/max) avoid code duplication with command handlers"
  - "LMOVE validates both source and destination types before mutation for atomicity"

patterns-established:
  - "Wakeup hook pattern: post-mutation function takes &mut BlockingRegistry + &mut Database"
  - "wait_id deduplication: shared ID across multi-key waiters for single-wakeup semantics"

requirements-completed: [BLOCK-01, BLOCK-02]

# Metrics
duration: 5min
completed: 2026-03-24
---

# Phase 17 Plan 01: Blocking Commands Infrastructure Summary

**BlockingRegistry with FIFO per-key wait queues, LMOVE command, and post-mutation wakeup hooks for list/sorted set producers**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-24T14:10:33Z
- **Completed:** 2026-03-24T14:15:35Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- BlockingRegistry with register/pop_front/remove_wait/has_waiters/expire_timed_out and cross-key wait_id cleanup
- Wakeup hooks (try_wake_list_waiter, try_wake_zset_waiter) that execute pop operations and deliver results via oneshot channels
- LMOVE command with LEFT/RIGHT direction parsing, WRONGTYPE validation, same-key rotation support
- Database thin wrapper methods for direct list/sorted-set pop/push operations used by both wakeup hooks and LMOVE

## Task Commits

Each task was committed atomically:

1. **Task 1: Create BlockingRegistry, wakeup hooks, Direction enum, Database helpers, LMOVE** - `8e7a4a9` (feat)
2. **Task 2: Add LMOVE unit tests** - `207048f` (test)

## Files Created/Modified
- `src/blocking/mod.rs` - BlockingRegistry, WaitEntry, BlockedCommand, Direction enum with FIFO wait queues
- `src/blocking/wakeup.rs` - try_wake_list_waiter and try_wake_zset_waiter post-mutation hooks
- `src/lib.rs` - Added `pub mod blocking` module declaration
- `src/command/mod.rs` - LMOVE dispatch entry
- `src/command/list.rs` - LMOVE command handler, parse_direction helper, 8 unit tests
- `src/storage/db.rs` - list_pop_front/back, list_push_front/back, zset_pop_min/max helper methods
- `src/persistence/aof.rs` - LMOVE added to write commands list and test

## Decisions Made
- BlockingRegistry uses HashMap<(usize, Bytes), VecDeque<WaitEntry>> for per-key FIFO queues with wait_keys cross-reference map
- Database thin wrapper methods extract core pop/push logic to avoid duplication between command handlers and wakeup hooks
- LMOVE validates both source and destination key types before performing any mutation for atomicity
- expire_timed_out uses two-pass approach: collect timed-out entries first, then clean up wait_keys

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] LMOVE implementation merged into Task 1**
- **Found during:** Task 1 (BlockingRegistry + dispatch entry)
- **Issue:** Dispatch entry for LMOVE requires the function to exist for cargo check to pass
- **Fix:** Implemented LMOVE command handler in Task 1 alongside the dispatch entry
- **Files modified:** src/command/list.rs
- **Verification:** cargo check passes
- **Committed in:** 8e7a4a9 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Task ordering adjusted to satisfy compilation dependency. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- BlockingRegistry ready for Plan 02 to integrate at connection level with BLPOP/BRPOP/BLMOVE
- Wakeup hooks ready for Plan 02 to call inline after LPUSH/RPUSH/ZADD dispatch
- LMOVE foundation ready for BLMOVE in Plan 02

---
*Phase: 17-blocking-commands*
*Completed: 2026-03-24*
