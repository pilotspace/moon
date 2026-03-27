---
phase: 11-thread-per-core-shared-nothing-architecture
plan: 04
subsystem: infrastructure
tags: [thread-per-core, shared-nothing, connection-handler, cross-shard-dispatch, VLL, SPSC, Rc-RefCell]

# Dependency graph
requires:
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 01)
    provides: Shard struct with owned Vec<Database>, ShardMessage enum, key_to_shard routing
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 02)
    provides: ChannelMesh with SPSC producers/consumers for inter-shard communication
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 03)
    provides: Shard::run() event loop, sharded listener, round-robin connection distribution
provides:
  - handle_connection_sharded() with zero-overhead local fast-path execution
  - Cross-shard single-key dispatch via SPSC ShardMessage::Execute with oneshot reply
  - VLL multi-key coordinator (MGET, MSET, DEL, UNLINK, EXISTS) with BTreeMap ascending-order dispatch
  - Shard event loop polling SPSC consumers for Execute/MultiExecute/PubSubFanOut
  - Rc<RefCell<Vec<Database>>> for safe single-threaded cooperative database sharing
affects: [11-05-benchmarks, 12-io-uring, 14-persistence]

# Tech tracking
tech-stack:
  added: []
  patterns: [Rc<RefCell> for single-threaded shared state, tokio::task::spawn_local for !Send types, BTreeMap for VLL ascending-order dispatch, SPSC spin-retry with yield_now]

key-files:
  created: [src/shard/coordinator.rs]
  modified: [src/server/connection.rs, src/shard/mod.rs, src/pubsub/mod.rs]

key-decisions:
  - "Rc<RefCell<Vec<Database>>> for database sharing within single-threaded shard runtime (zero Arc/Mutex overhead)"
  - "tokio::task::spawn_local for connection tasks (Rc is !Send, requires LocalSet-compatible runtime)"
  - "BTreeMap groups for VLL ascending shard-ID ordering (deadlock prevention for multi-key commands)"
  - "SPSC spin-retry with tokio::task::yield_now() on full ring buffer (cooperative back-pressure)"
  - "MultiExecute batches remote keys per shard (fewer SPSC messages vs individual Execute per key)"
  - "Existing handle_connection() preserved for backward compatibility with integration tests"

patterns-established:
  - "Connection handler pattern: extract key -> key_to_shard -> local fast-path or SPSC dispatch"
  - "VLL coordinator pattern: BTreeMap<shard_id, Vec<key>> -> iterate ascending -> dispatch in order"
  - "SPSC send pattern: try_push in scoped borrow, yield on full, retry"

requirements-completed: [SHARD-06, SHARD-07, SHARD-08]

# Metrics
duration: 11min
completed: 2026-03-24
---

# Phase 11 Plan 04: Connection Handler Wiring and Cross-Shard Dispatch Summary

**Sharded connection handler with zero-overhead local fast-path, SPSC cross-shard dispatch, and VLL multi-key coordination via BTreeMap ascending-order shard grouping**

## Performance

- **Duration:** 11 min
- **Started:** 2026-03-24T08:01:38Z
- **Completed:** 2026-03-24T08:12:38Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Connection handler runs within shard event loop with direct mutable database access via Rc<RefCell> (zero cross-shard overhead for local commands)
- Single-key remote commands dispatch via SPSC ShardMessage::Execute with oneshot reply channel
- VLL coordinator groups multi-key commands by shard in BTreeMap (ascending order for deadlock prevention), batches remote dispatch via MultiExecute
- Shard event loop polls SPSC consumers every 1ms for incoming Execute/MultiExecute/PubSubFanOut messages
- All 518 lib tests pass with zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Adapt connection handler for per-shard database access with cross-shard dispatch** - `d282603` (feat)
2. **Task 2: Implement VLL multi-key coordinator for MGET/MSET/DEL** - `1ea641c` (feat)

## Files Created/Modified
- `src/server/connection.rs` - Added handle_connection_sharded() with local fast-path and SPSC remote dispatch, extract_primary_key(), is_multi_key_command(), execute_transaction_sharded()
- `src/shard/mod.rs` - Rewired Shard::run() with Rc<RefCell> database wrapping, spawn_local connection tasks, drain_spsc_shared with Execute/MultiExecute handling
- `src/shard/coordinator.rs` - Created: VLL coordinator with BTreeMap grouping for MGET, MSET, DEL/UNLINK/EXISTS, fast local path, batched remote dispatch
- `src/pubsub/mod.rs` - Added derive(Default) for PubSubRegistry (linter)

## Decisions Made
- Used Rc<RefCell<Vec<Database>>> instead of Arc<Mutex> because the single-threaded current_thread runtime guarantees no concurrent access -- zero synchronization overhead on the fast path
- Used tokio::task::spawn_local for connection tasks because Rc is !Send (requires LocalSet-compatible runtime, which current_thread provides)
- BTreeMap for VLL ascending-order dispatch ensures deadlock-free multi-shard coordination
- SPSC spin-retry uses scoped borrow_mut (dropped before yield_now) to avoid holding RefCell borrow across await points
- MultiExecute batches multiple keys per shard in a single SPSC message, reducing ring buffer contention
- Existing handle_connection() preserved for backward compatibility with integration tests using run_with_shutdown()
- Transactions in sharded mode restricted to local-shard keys only (cross-shard distributed transactions deferred)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Created coordinator.rs alongside Task 1 for compilation**
- **Found during:** Task 1 (connection handler)
- **Issue:** connection.rs calls coordinate_multi_key() which lives in coordinator.rs; `pub mod coordinator;` requires the file to exist
- **Fix:** Created coordinator.rs as part of Task 1 commit, refined in Task 2
- **Files modified:** src/shard/coordinator.rs
- **Verification:** cargo check passes
- **Committed in:** d282603 (Task 1)

**2. [Rule 3 - Blocking] Wrapped PubSubRegistry in Rc<RefCell> for shard sharing**
- **Found during:** Task 1 (shard event loop wiring)
- **Issue:** Connection handler needs pubsub_registry access, but it's owned by Shard which is moved into Rc<RefCell>
- **Fix:** Added pubsub_registry to the Rc<RefCell> wrapping alongside databases
- **Files modified:** src/shard/mod.rs, src/server/connection.rs
- **Verification:** cargo check passes, pubsub tests pass

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both fixes necessary for compilation. Coordinator pulled into Task 1 for dependency resolution. No scope creep.

## Issues Encountered
- SPSC spin-retry initially used `let prod = &mut producers[target_idx]` which created a long-lived borrow preventing `drop(producers)` in the retry loop. Fixed by scoping the borrow to the try_push call.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Sharded connection handler fully operational for single-key and multi-key commands
- Cross-shard dispatch via SPSC verified in unit tests
- Ready for Plan 05 (benchmarks/validation) or Plan 06+ work
- Integration tests still use existing handle_connection() via preserved backward-compatible path

---
*Phase: 11-thread-per-core-shared-nothing-architecture*
*Completed: 2026-03-24*
