---
phase: 17-blocking-commands
plan: 03
subsystem: blocking
tags: [cross-shard, blpop, futures-unordered, spsc, blocking-commands]

# Dependency graph
requires:
  - phase: 17-02
    provides: "BlockingRegistry, WaitEntry, BlockRegister/BlockCancel ShardMessage variants, handle_blocking_command single-key path"
provides:
  - "Cross-shard multi-key BLPOP/BRPOP/BZPOPMIN/BZPOPMAX coordinator"
  - "Globally unique wait_ids via shard_id-encoded upper bits"
  - "Wakeup hooks in shard Execute handler for remote LPUSH/RPUSH/ZADD/LMOVE"
affects: [18-streams, 19-replication]

# Tech tracking
tech-stack:
  added: []
  patterns: [FuturesUnordered first-wakeup-wins, ChannelMesh target_index for SPSC routing, loop-based Err skipping in select]

key-files:
  created: []
  modified:
    - src/server/connection.rs
    - src/blocking/mod.rs
    - src/shard/mod.rs
    - tests/integration.rs

key-decisions:
  - "FuturesUnordered with loop-based Err skipping for first-wakeup-wins (remove_wait drops other senders causing Err race)"
  - "Single-key fast path preserved unchanged for zero overhead"
  - "BlockRegister race-check guarded by exists() to prevent waiter destruction on empty keys"
  - "Wakeup hooks added to shard Execute handler for cross-shard producer commands"

patterns-established:
  - "Cross-shard blocking: register via SPSC BlockRegister, cancel via BlockCancel, using ChannelMesh::target_index"
  - "Multi-key coordinator: FuturesUnordered with continue-on-Err loop inside tokio::select!"

requirements-completed: [BLOCK-01]

# Metrics
duration: 17min
completed: 2026-03-24
---

# Phase 17 Plan 03: Cross-Shard Multi-Key BLPOP Summary

**FuturesUnordered-based cross-shard multi-key blocking coordinator with BlockRegister/BlockCancel SPSC dispatch, first-wakeup-wins semantics, and shard-encoded globally unique wait_ids**

## Performance

- **Duration:** 17 min
- **Started:** 2026-03-24T14:38:51Z
- **Completed:** 2026-03-24T14:55:46Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Cross-shard multi-key BLPOP/BRPOP/BZPOPMIN/BZPOPMAX registers on ALL keys across local and remote shards
- First-wakeup-wins: any shard delivering data wakes the client; remaining registrations cancelled via BlockCancel
- Single-key fast path remains zero overhead (no FuturesUnordered, no HashMap)
- Globally unique wait_ids prevent cross-shard collision (shard_id in upper 16 bits)
- Wakeup hooks in shard Execute handler ensure remote LPUSH/RPUSH/ZADD/LMOVE trigger blocked client wakeup

## Task Commits

Each task was committed atomically:

1. **Task 1: Globally unique wait_ids and cross-shard coordinator** - `2bc94d9` (feat)
2. **Task 2: Integration tests + bug fixes** - `2ea81c6` (test)

## Files Created/Modified
- `src/blocking/mod.rs` - BlockingRegistry::new(shard_id) with shard-encoded wait_ids
- `src/server/connection.rs` - Multi-key FuturesUnordered coordinator, extract_bytes pub(crate)
- `src/shard/mod.rs` - Wakeup hooks in Execute handler, exists() guard in BlockRegister
- `tests/integration.rs` - 3 new integration tests (cross-shard wakeup, timeout, all-local regression)

## Decisions Made
- FuturesUnordered with loop-based Err-skipping instead of mpsc channel: lower allocation overhead, handles remove_wait race correctly by continuing past Err receivers
- BlockRegister race-check guarded by exists(): prevents try_wake_list_waiter from destroying waiter registration when key has no data (it sends None = timeout signal)
- Wakeup hooks in shard Execute handler: necessary because cross-shard LPUSH via Execute bypasses connection-level wakeup hooks

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] SPSC target index must use ChannelMesh::target_index**
- **Found during:** Task 2 (test debugging)
- **Issue:** Producer Vec is indexed by mesh target, not raw shard ID. Using raw shard ID caused out-of-bounds or wrong-shard dispatch.
- **Fix:** Use `ChannelMesh::target_index(shard_id, target)` for both BlockRegister send and BlockCancel cleanup.
- **Files modified:** src/server/connection.rs
- **Verification:** blpop_cross_shard_wakeup test passes
- **Committed in:** 2ea81c6

**2. [Rule 1 - Bug] FuturesUnordered Err race with remove_wait cleanup**
- **Found during:** Task 2 (test debugging)
- **Issue:** When wakeup fires, remove_wait drops other oneshot senders. FuturesUnordered could return Err(RecvError) before Ok(Some(frame)), causing premature Null return.
- **Fix:** Changed from single select to loop-based select that skips Err receivers with `continue`.
- **Files modified:** src/server/connection.rs
- **Verification:** blpop_multi_key_all_local_regression test passes
- **Committed in:** 2ea81c6

**3. [Rule 1 - Bug] BlockRegister race-check destroys waiter on empty key**
- **Found during:** Task 2 (test debugging)
- **Issue:** BlockRegister handler calls try_wake_list_waiter speculatively. When key is empty, it pops the waiter, sends None (timeout), and removes registration -- destroying the blocking wait.
- **Fix:** Guard try_wake calls with `dbs[db_index].exists(&key)` check.
- **Files modified:** src/shard/mod.rs
- **Verification:** blpop_cross_shard_timeout test passes (0.3s timeout honored)
- **Committed in:** 2ea81c6

**4. [Rule 2 - Missing Critical] Wakeup hooks in shard Execute handler**
- **Found during:** Task 2 (test debugging)
- **Issue:** When LPUSH arrives at a remote shard via ShardMessage::Execute, the shard dispatches the command but never fires blocking wakeup hooks. Cross-shard blocked clients would never wake.
- **Fix:** Added post-dispatch wakeup hooks for LPUSH/RPUSH/LMOVE/ZADD in handle_shard_message_shared Execute handler.
- **Files modified:** src/shard/mod.rs
- **Verification:** blpop_cross_shard_wakeup test passes
- **Committed in:** 2ea81c6

---

**Total deviations:** 4 auto-fixed (3 bugs, 1 missing critical)
**Impact on plan:** All fixes necessary for cross-shard blocking correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 17 blocking commands fully complete (all 7 success criteria verified)
- Cross-shard coordination infrastructure proven (BlockRegister/BlockCancel/wakeup hooks)
- Ready for Phase 18 (Streams) or any subsequent phase

---
*Phase: 17-blocking-commands*
*Completed: 2026-03-24*
