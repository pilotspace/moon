---
phase: 05-pub-sub-transactions-and-eviction
plan: 04
subsystem: testing
tags: [integration-tests, pubsub, transactions, eviction, config]

requires:
  - phase: 05-01
    provides: Eviction engine and CONFIG commands
  - phase: 05-02
    provides: Pub/Sub registry and subscriber mode
  - phase: 05-03
    provides: MULTI/EXEC/DISCARD/WATCH transaction state machine
provides:
  - Integration tests proving Phase 5 features work end-to-end over real TCP
  - Pub/Sub tests verifying two-client messaging via SUBSCRIBE/PUBLISH/PSUBSCRIBE
  - Transaction tests verifying MULTI/EXEC atomicity and WATCH abort
  - Eviction tests verifying OOM rejection and allkeys-lru eviction
  - CONFIG GET/SET tests verifying runtime parameter tuning
affects: []

tech-stack:
  added: []
  patterns: [start_server_with_maxmemory helper, pre-write eviction check timing awareness]

key-files:
  created: []
  modified: [tests/integration.rs]

key-decisions:
  - "Used redis crate async PubSub API (get_async_pubsub) for subscriber tests with on_message stream"
  - "Non-multiplexed connections (get_tokio_connection) for transaction tests to maintain per-connection state"
  - "Eviction tests use CONFIG SET to lower maxmemory after data insertion to trigger predictable eviction"
  - "Pre-write eviction check timing: OOM triggers on the write AFTER memory exceeds limit, not the write that causes it"

patterns-established:
  - "Eviction integration pattern: insert data first, then lower maxmemory via CONFIG SET to trigger eviction on next write"
  - "PubSub integration pattern: separate subscriber (PubSub connection) and publisher (multiplexed connection)"

requirements-completed: [PUB-01, PUB-02, PUB-03, PUB-04, TXN-01, TXN-02, EVIC-01, EVIC-02, EVIC-03]

duration: 6min
completed: 2026-03-23
---

# Phase 5 Plan 4: Integration Tests Summary

**End-to-end integration tests for Pub/Sub messaging, MULTI/EXEC/WATCH transactions, CONFIG GET/SET, and LRU eviction over real TCP connections**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-23T09:20:33Z
- **Completed:** 2026-03-23T09:26:18Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- 17 new integration tests covering all Phase 5 features end-to-end
- Pub/Sub: subscribe/publish, psubscribe pattern matching, unsubscribe, multiple channels, subscriber count
- Transactions: MULTI/EXEC basic atomicity, DISCARD rollback, EXEC/DISCARD without MULTI errors, nested MULTI error, WATCH success, WATCH abort on key modification
- CONFIG: GET single param, SET maxmemory, GET glob pattern, SET policy
- Eviction: noeviction OOM rejection, allkeys-lru eviction, CONFIG SET triggers eviction
- Full test suite green: 49 tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Pub/Sub and Transaction integration tests** - `91ce15a` (feat)
2. **Task 2: Eviction and CONFIG integration tests** - `da20b1c` (feat)

## Files Created/Modified
- `tests/integration.rs` - Added 17 integration tests and `start_server_with_maxmemory` helper

## Decisions Made
- Used redis crate `get_async_pubsub()` for subscriber connections with `on_message()` stream for receiving messages
- Used non-multiplexed `get_tokio_connection()` for transaction tests because multiplexed connections share state
- Eviction tests use CONFIG SET to lower maxmemory after data insertion rather than starting with small limits, avoiding pre-write check timing issues
- Pre-write eviction check timing: the server checks memory BEFORE each write, so OOM error occurs on the write AFTER memory already exceeds the limit

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed redis::Value assertion in EXEC results**
- **Found during:** Task 1 (test_multi_exec_basic, test_watch_success)
- **Issue:** redis::Value comparison failed because redis crate represents "OK" differently than expected `SimpleString("OK")`
- **Fix:** Changed EXEC result assertions to parse via `FromRedisValue` instead of direct Value enum comparison
- **Files modified:** tests/integration.rs
- **Verification:** Tests pass
- **Committed in:** 91ce15a (Task 1 commit)

**2. [Rule 1 - Bug] Fixed eviction test timing assumptions**
- **Found during:** Task 2 (test_eviction_noeviction_rejects_writes, test_eviction_allkeys_lru)
- **Issue:** Tests assumed OOM would trigger on the write that pushed memory over the limit, but eviction check runs BEFORE dispatch (pre-write), so memory must already exceed limit
- **Fix:** Adjusted noeviction test to use 3 writes (third triggers OOM after first two exceed limit). Changed allkeys-lru test to insert data first then lower maxmemory via CONFIG SET
- **Files modified:** tests/integration.rs
- **Verification:** Tests pass reliably
- **Committed in:** da20b1c (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes necessary for test correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 5 complete: all features implemented and verified end-to-end
- Full test suite (49 tests) passes: unit tests + integration tests
- Project v1.0 milestone complete

---
*Phase: 05-pub-sub-transactions-and-eviction*
*Completed: 2026-03-23*
