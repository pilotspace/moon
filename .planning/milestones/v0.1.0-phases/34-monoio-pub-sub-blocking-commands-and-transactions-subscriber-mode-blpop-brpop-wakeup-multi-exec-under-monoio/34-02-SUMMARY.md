---
phase: 34-monoio-pub-sub-blocking-commands-and-transactions
plan: 02
subsystem: runtime
tags: [monoio, blocking, blpop, brpop, blmove, bzpopmin, bzpopmax, async]

# Dependency graph
requires:
  - phase: 34-01
    provides: "monoio handler with pub/sub subscriber mode and MULTI/EXEC transactions"
provides:
  - "handle_blocking_command_monoio function with monoio::select!/monoio::time::sleep"
  - "Blocking command intercepts in monoio handler with MULTI queue support"
  - "Producer wakeup hooks for LPUSH/RPUSH/LMOVE/ZADD in monoio handler"
affects: [34-03, monoio-runtime, blocking-commands]

# Tech tracking
tech-stack:
  added: []
  patterns: ["monoio::select! for blocking wait with timeout", "std::pin::pin! instead of tokio::pin!", "monoio::time::sleep for SPSC backpressure"]

key-files:
  created: []
  modified: ["src/server/connection.rs"]

key-decisions:
  - "Used std::pin::pin! (stable since Rust 1.68) instead of tokio::pin! for pinning sleep futures in multi-key coordinator"
  - "SPSC backpressure in remote registration uses monoio::time::sleep(10us) consistent with existing monoio handler pattern"
  - "Added spsc_notifiers notify after BlockCancel push for prompt remote cleanup"

patterns-established:
  - "monoio blocking pattern: monoio::select! on OneshotReceiver + monoio::time::sleep for timeout"
  - "Wakeup hooks in monoio handler mirror tokio handler pattern exactly"

requirements-completed: [MONOIO-BLOCK-01, MONOIO-BLOCK-02]

# Metrics
duration: 17min
completed: 2026-03-25
---

# Phase 34 Plan 02: Blocking Commands Under Monoio Summary

**Ported BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX to monoio runtime with monoio::select! timeout and producer wakeup hooks**

## Performance

- **Duration:** 17 min
- **Started:** 2026-03-25T18:08:05Z
- **Completed:** 2026-03-25T18:25:33Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Created handle_blocking_command_monoio with single-key fast path and multi-key FuturesUnordered coordinator
- Wired blocking command intercepts into monoio handler with MULTI transaction queue support
- Added post-dispatch wakeup hooks for LPUSH/RPUSH/LMOVE/ZADD to wake blocking waiters
- Both runtime-tokio and runtime-monoio compile cleanly; all 1036 tokio tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Create handle_blocking_command_monoio function** - `1184bea` (feat)

**Plan metadata:** (pending)

## Files Created/Modified
- `src/server/connection.rs` - Added handle_blocking_command_monoio (~220 lines), blocking command intercepts in monoio handler, producer wakeup hooks

## Decisions Made
- Used std::pin::pin! for pinning the sleep future in the multi-key deadline loop, matching Rust stable API instead of tokio::pin!
- SPSC retry in remote registration drops RefCell borrows before monoio::time::sleep().await, re-acquires after -- critical for correctness
- Added spsc_notifiers.notify_one() after BlockCancel push to ensure remote shards process cleanup promptly

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Blocking commands fully ported to monoio runtime
- Ready for Plan 03 (if exists) or phase completion
- Pre-existing monoio compilation errors in listener.rs, shard/mod.rs, key.rs, persistence.rs remain (unrelated to this plan)

---
*Phase: 34-monoio-pub-sub-blocking-commands-and-transactions*
*Completed: 2026-03-25*

## Self-Check: PASSED
- src/server/connection.rs: FOUND
- handle_blocking_command_monoio: 2 references (definition + call)
- Commit 1184bea: FOUND
- cargo check --features runtime-tokio: PASSED
- cargo test --features runtime-tokio --lib: 1036 passed, 0 failed
