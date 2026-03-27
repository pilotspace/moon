---
phase: 11-thread-per-core-shared-nothing-architecture
plan: 03
subsystem: infrastructure
tags: [thread-per-core, current-thread-runtime, shard-bootstrap, round-robin, shared-nothing]

# Dependency graph
requires:
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 01)
    provides: Shard struct with owned Vec<Database>, ShardMessage enum
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 02)
    provides: ChannelMesh with SPSC producers/consumers and connection channels
provides:
  - N shard threads each running tokio::runtime::Builder::new_current_thread()
  - Sharded listener with round-robin connection distribution
  - Shard::run() event loop with expiry and stub connection handling
  - --shards CLI flag with auto-detection from CPU count
  - expire_cycle_direct() public wrapper for per-shard expiry
affects: [11-04-connection-handling, 11-05-cross-shard-dispatch, 12-io-uring]

# Tech tracking
tech-stack:
  added: []
  patterns: [manual runtime construction (no #[tokio::main]), thread-per-core with current_thread runtimes, round-robin connection assignment]

key-files:
  created: []
  modified: [src/main.rs, src/config.rs, src/server/listener.rs, src/shard/mod.rs, src/server/expiration.rs]

key-decisions:
  - "Removed #[tokio::main] -- main.rs manually creates per-shard current_thread runtimes via std::thread::Builder"
  - "Listener runs on main thread with its own current_thread runtime, not on a shard thread"
  - "Round-robin connection distribution (simplest, no hot-shard risk with uniform clients)"
  - "Shard::run() stubs connection handling (drop stream) -- Plan 04 wires up full handler"
  - "expire_cycle_direct() wraps private expire_cycle for shared-nothing per-shard access"

patterns-established:
  - "Thread-per-core: std::thread::Builder per shard, tokio current_thread runtime per thread"
  - "Listener distributes connections via tokio mpsc conn_tx channels to shard threads"
  - "Existing run()/run_with_shutdown() preserved for integration test backward compatibility"

requirements-completed: [SHARD-04, SHARD-05]

# Metrics
duration: 3min
completed: 2026-03-24
---

# Phase 11 Plan 03: Shard Bootstrap and Sharded Listener Summary

**N shard threads with per-thread current_thread runtimes, round-robin connection distribution, and shard event loop with cooperative expiry**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-24T07:56:25Z
- **Completed:** 2026-03-24T07:59:25Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Replaced #[tokio::main] with manual N+1 current_thread runtime construction (N shards + 1 listener)
- Sharded listener distributes connections to shard threads via round-robin over tokio mpsc channels
- Shard::run() event loop handles connection receive, cooperative active expiry, and graceful shutdown
- --shards CLI flag with auto-detect (0 = std::thread::available_parallelism)
- All 510 lib tests pass with zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Add --shards config flag and shard bootstrap in main.rs** - `7aa0372` (feat)
2. **Task 2: Implement sharded listener and basic shard event loop** - `757dada` (feat)

## Files Created/Modified
- `src/main.rs` - Rewritten: manual runtime creation, N shard threads, listener on main thread
- `src/config.rs` - Added --shards flag with default 0 (auto-detect), plus unit tests
- `src/server/listener.rs` - Added run_sharded() for round-robin connection distribution
- `src/shard/mod.rs` - Added Shard::run() event loop with expiry and stub connection handling
- `src/server/expiration.rs` - Added expire_cycle_direct() public wrapper for per-shard access

## Decisions Made
- Removed #[tokio::main] macro entirely -- main() is now a plain fn that manually creates runtimes
- Listener runs on main thread (not a shard) with its own current_thread runtime
- Round-robin is the initial distribution strategy (simplest, Plan 04 can add hash-based routing)
- Connection handling in Shard::run() is intentionally stubbed (logs and drops) -- Plan 04 wires up the full handler
- Existing run()/run_with_shutdown() left intact for integration test backward compatibility

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added expire_cycle_direct and Shard::run in Task 1 instead of Task 2**
- **Found during:** Task 1 (main.rs bootstrap)
- **Issue:** main.rs calls Shard::run() and run_sharded() which must exist for cargo check to pass
- **Fix:** Implemented run_sharded(), Shard::run(), and expire_cycle_direct() in Task 1 alongside main.rs rewrite
- **Files modified:** src/server/listener.rs, src/shard/mod.rs, src/server/expiration.rs
- **Verification:** cargo check passes, all 510 tests pass
- **Committed in:** 7aa0372 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Task 2 work pulled into Task 1 for compilation. Task 2 added config tests. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Shard threads running with current_thread runtimes, ready for Plan 04 connection handler
- Connection channels operational, round-robin distribution verified
- expire_cycle_direct ready for per-shard cooperative expiry
- Existing integration tests still work via preserved run()/run_with_shutdown()

---
*Phase: 11-thread-per-core-shared-nothing-architecture*
*Completed: 2026-03-24*
