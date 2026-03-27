---
phase: 11-thread-per-core-shared-nothing-architecture
plan: 05
subsystem: pubsub
tags: [pubsub, shared-nothing, spsc, fan-out, per-shard]

# Dependency graph
requires:
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 01)
    provides: Shard struct with owned Vec<Database>, ShardMessage enum with PubSubFanOut variant
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 02)
    provides: ChannelMesh with SPSC producers/consumers
  - phase: 11-thread-per-core-shared-nothing-architecture (plan 03)
    provides: Shard::run() event loop, sharded listener
provides:
  - Per-shard PubSubRegistry (no global Mutex)
  - Cross-shard PUBLISH fan-out via SPSC PubSubFanOut messages
  - SPSC drain loop handles PubSubFanOut delivery to local subscribers
  - Rc<RefCell<PubSubRegistry>> sharing with connection tasks
affects: [11-06-persistence, 12-io-uring, 16-resp3-client-side-caching]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-shard pubsub registry, cross-shard fan-out via SPSC, Rc<RefCell> for single-threaded sharing]

key-files:
  created: []
  modified: [src/shard/mod.rs, src/pubsub/mod.rs, src/server/connection.rs, src/shard/coordinator.rs]

key-decisions:
  - "PubSubRegistry is per-shard, wrapped in Rc<RefCell> for spawn_local sharing -- no global Mutex"
  - "PUBLISH fans out to all other shards via SPSC PubSubFanOut (best-effort try_push)"
  - "Shard event loop drains SPSC every 1ms, delivering PubSubFanOut to local subscribers"
  - "SUBSCRIBE/UNSUBSCRIBE in sharded mode deferred (stub error) -- only PUBLISH fan-out implemented"

patterns-established:
  - "Per-shard pubsub: each shard owns its PubSubRegistry, PUBLISH fans out cross-shard"
  - "SPSC fan-out: publisher pushes PubSubFanOut to all other shards, receivers deliver locally"

requirements-completed: [SHARD-09]

# Metrics
duration: 10min
completed: 2026-03-24
---

# Phase 11 Plan 05: Per-Shard PubSub with Cross-Shard Fan-Out Summary

**Per-shard PubSubRegistry with SPSC-based cross-shard PUBLISH fan-out -- no global Mutex for Pub/Sub**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-24T08:01:46Z
- **Completed:** 2026-03-24T08:12:20Z
- **Tasks:** 1
- **Files modified:** 4

## Accomplishments
- PubSubRegistry added as per-shard field (no global Arc<Mutex>), wrapped in Rc<RefCell> for connection task sharing
- PUBLISH in sharded handler delivers locally then fans out PubSubFanOut to all other shards via SPSC
- Shard event loop drain_spsc_shared handles PubSubFanOut delivery to local subscribers
- PubSubRegistry derives Default for std::mem::take compatibility
- Fixed coordinator.rs API mismatches (CompactEntry.value(), set signature)
- All 518 lib tests pass with zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Make PubSubRegistry per-shard and implement cross-shard fan-out** - `d282603` (feat)

Note: Core pubsub changes to shard/mod.rs, connection.rs, pubsub/mod.rs were committed in concurrent 11-04 execution (`0dee98f`). Task 1 commit fixes coordinator.rs API mismatches.

## Files Created/Modified
- `src/shard/mod.rs` - Added pubsub_registry field, Rc<RefCell> wrapping, SPSC drain with PubSubFanOut handling
- `src/pubsub/mod.rs` - Added #[derive(Default)] for PubSubRegistry (std::mem::take support)
- `src/server/connection.rs` - Added PUBLISH fan-out + SUBSCRIBE stubs in handle_connection_sharded
- `src/shard/coordinator.rs` - Fixed API mismatches (CompactEntry, set signature) in VLL coordinator

## Decisions Made
- PubSubRegistry is per-shard, no global Mutex -- eliminates last shared mutable state for pubsub
- PUBLISH returns local subscriber count immediately; remote delivery is best-effort (try_push, matches Redis PUBLISH semantics)
- SUBSCRIBE/UNSUBSCRIBE in sharded mode returns stub error for now; full subscriber mode deferred
- pubsub_registry restored from Rc on shard shutdown (same pattern as databases)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed syntax error in handle_connection_sharded remote dispatch**
- **Found during:** Task 1
- **Issue:** Pre-existing brace mismatch in else-if block (over-indented closing brace)
- **Fix:** Corrected indentation and brace alignment
- **Files modified:** src/server/connection.rs
- **Committed in:** 0dee98f (concurrent 11-04 execution)

**2. [Rule 3 - Blocking] Created coordinator.rs module and fixed API mismatches**
- **Found during:** Task 1
- **Issue:** pub mod coordinator referenced non-existent file; original had CompactEntry.value() and set(key, value, None) API mismatches
- **Fix:** Created coordinator.rs using dispatch() function for all operations instead of direct DB access
- **Files modified:** src/shard/coordinator.rs
- **Committed in:** d282603

**3. [Rule 2 - Missing Critical] Added #[derive(Default)] to PubSubRegistry**
- **Found during:** Task 1
- **Issue:** std::mem::take requires Default trait; PubSubRegistry did not implement it
- **Fix:** Added #[derive(Default)] to PubSubRegistry struct
- **Files modified:** src/pubsub/mod.rs
- **Committed in:** 0dee98f (concurrent 11-04 execution)

---

**Total deviations:** 3 auto-fixed (1 bug, 1 blocking, 1 missing critical)
**Impact on plan:** All fixes necessary for compilation. No scope creep.

## Issues Encountered
- Concurrent Plan 04 execution committed changes to same files (shard/mod.rs, connection.rs, pubsub/mod.rs) during this plan's execution. Changes were compatible and merged cleanly.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Per-shard PubSubRegistry operational with cross-shard fan-out
- SUBSCRIBE/UNSUBSCRIBE in sharded mode needs full implementation (deferred)
- Ready for Plan 06 (persistence) and Phase 12 (io_uring)

---
*Phase: 11-thread-per-core-shared-nothing-architecture*
*Completed: 2026-03-24*
