---
phase: 20-cluster-mode-with-hash-slots-and-gossip-protocol
plan: 03
subsystem: cluster
tags: [cluster-commands, slot-routing, moved-ask, asking, crossslot, cluster-meet]

# Dependency graph
requires:
  - phase: 20-cluster-mode-with-hash-slots-and-gossip-protocol
    provides: ClusterState, ClusterNode, SlotRoute, slot_for_key, CLUSTER_ENABLED AtomicBool from Plan 01
  - phase: 20-cluster-mode-with-hash-slots-and-gossip-protocol
    provides: run_cluster_bus, run_gossip_ticker, cluster_enabled/cluster_node_timeout config from Plan 02
provides:
  - handle_cluster_command dispatcher for all CLUSTER subcommands
  - CLUSTER INFO/MYID/NODES/SLOTS/MEET/ADDSLOTS/DELSLOTS/SETSLOT/KEYSLOT/COUNTKEYSINSLOT/RESET/REPLICATE
  - Pre-dispatch slot routing in handle_connection_sharded (MOVED/ASK/CROSSSLOT)
  - ASKING command with per-connection flag cleared unconditionally before routing
  - ClusterState initialization and cluster bus/gossip startup in main.rs
affects: [20-04-slot-migration]

# Tech tracking
tech-stack:
  added: []
  patterns: [pre-dispatch slot routing with AtomicBool gate, ASKING flag clear-before-check]

key-files:
  created: [src/cluster/command.rs]
  modified: [src/cluster/mod.rs, src/server/connection.rs, src/shard/mod.rs, src/main.rs, tests/integration.rs, tests/replication_test.rs]

key-decisions:
  - "ASKING flag cleared unconditionally before routing check (not just on ASKING-eligible slots)"
  - "CROSSSLOT check runs after single-key routing for multi-key commands"
  - "CLUSTER MEET generates placeholder node ID via generate_repl_id (replaced by gossip handshake)"
  - "cluster_state passed as Option<Arc<RwLock<ClusterState>>> through Shard::run() to connection handler"

patterns-established:
  - "Pre-dispatch slot routing: AtomicBool gate -> RwLock read -> route_slot() -> MOVED/ASK/Local"
  - "CLUSTER command interception pattern: same level as AUTH/CONFIG in connection handler"

requirements-completed: [CLUSTER-06, CLUSTER-07, CLUSTER-08, CLUSTER-09, CLUSTER-13]

# Metrics
duration: 7min
completed: 2026-03-24
---

# Phase 20 Plan 03: CLUSTER Commands and Slot Routing Summary

**All CLUSTER subcommands (INFO/MYID/NODES/SLOTS/MEET/ADDSLOTS/DELSLOTS/SETSLOT/KEYSLOT/RESET/REPLICATE) with pre-dispatch MOVED/ASK/CROSSSLOT routing in sharded handler**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-24T16:50:20Z
- **Completed:** 2026-03-24T16:57:03Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Full CLUSTER command family with 12 subcommands dispatched from single handle_cluster_command entry point
- Pre-dispatch slot routing with AtomicBool gate for zero overhead on non-cluster path
- ASKING flag per-connection with unconditional clear before routing (per Redis spec)
- CROSSSLOT error for multi-key commands spanning different hash slots
- ClusterState initialized in main.rs with cluster bus and gossip ticker started on listener runtime
- 9 unit tests for CLUSTER subcommands, 26 total cluster tests passing, 863 lib tests no regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Create src/cluster/command.rs with all CLUSTER subcommand handlers** - `a3eabf4` (feat)
2. **Task 2: Wire slot routing into handle_connection_sharded and start cluster in main.rs** - `02bcd43` (feat)

## Files Created/Modified
- `src/cluster/command.rs` - All CLUSTER subcommand handlers: INFO, MYID, NODES, SLOTS, MEET, ADDSLOTS, DELSLOTS, SETSLOT, KEYSLOT, COUNTKEYSINSLOT, RESET, REPLICATE
- `src/cluster/mod.rs` - Added pub mod command declaration
- `src/server/connection.rs` - ASKING/CLUSTER interception, slot routing pre-dispatch with MOVED/ASK/CROSSSLOT, extract_primary_key expanded for keyless commands
- `src/shard/mod.rs` - Added cluster_state and config_port params to Shard::run() and handle_connection_sharded call
- `src/main.rs` - ClusterState initialization, CLUSTER_ENABLED AtomicBool set, cluster bus and gossip ticker spawn
- `tests/integration.rs` - Added cluster_enabled and cluster_node_timeout fields to ServerConfig structs
- `tests/replication_test.rs` - Added cluster_enabled and cluster_node_timeout fields to ServerConfig struct

## Decisions Made
- ASKING flag cleared unconditionally before routing check (not conditionally on slot state) per Redis spec
- CROSSSLOT check runs after single-key slot routing to avoid redundant work when slot is non-local
- CLUSTER MEET generates placeholder node ID that gossip will replace during handshake
- Fixed plan bug: KEYSLOT test expected 12356 but CRC16-XMODEM("foo") == 12182 (verified in slots.rs)
- Removed duplicate MYID match arm from plan's code

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed incorrect KEYSLOT test vector in plan**
- **Found during:** Task 1 (CLUSTER command handlers)
- **Issue:** Plan test asserted slot_for_key("foo") == 12356, but verified CRC16-XMODEM result is 12182
- **Fix:** Changed test assertion to 12182 matching existing slots.rs test
- **Files modified:** src/cluster/command.rs
- **Verification:** test_keyslot_foo passes
- **Committed in:** a3eabf4

**2. [Rule 1 - Bug] Removed duplicate MYID match arm**
- **Found during:** Task 1
- **Issue:** Plan code had MYID listed twice in match arms
- **Fix:** Removed duplicate
- **Files modified:** src/cluster/command.rs
- **Committed in:** a3eabf4

**3. [Rule 3 - Blocking] Fixed integration test ServerConfig structs missing cluster fields**
- **Found during:** Task 2 (compilation verification)
- **Issue:** Plan 20-02 added cluster_enabled/cluster_node_timeout to ServerConfig but integration tests were not updated
- **Fix:** Added cluster_enabled: false and cluster_node_timeout: 15000 to all 6 ServerConfig initializers in tests
- **Files modified:** tests/integration.rs, tests/replication_test.rs
- **Committed in:** 02bcd43

---

**Total deviations:** 3 auto-fixed (2 bugs, 1 blocking)
**Impact on plan:** All auto-fixes necessary for correctness. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All CLUSTER commands available for slot migration orchestration (Plan 04)
- Pre-dispatch routing active for MOVED/ASK redirections
- Cluster bus and gossip running when --cluster-enabled flag set
- SETSLOT MIGRATING/IMPORTING/NODE/STABLE ready for migration state machine

---
*Phase: 20-cluster-mode-with-hash-slots-and-gossip-protocol*
*Completed: 2026-03-24*
