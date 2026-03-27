---
phase: 19-replication-with-psync2-protocol
plan: 03
subsystem: replication
tags: [psync2, replicaof, replconf, readonly, info-replication, replica-task]

# Dependency graph
requires:
  - phase: 19-replication-with-psync2-protocol
    provides: ReplicationState, ReplicaHandshakeState, build_info_replication from Plan 01
provides:
  - REPLICAOF/SLAVEOF command handlers with ReplicaofAction enum
  - REPLCONF handshake responder (OK for all subcommands)
  - READONLY enforcement rejecting writes when role=Replica
  - INFO replication section appended via build_info_replication
  - Outbound replica task (run_replica_task) with PSYNC2 handshake and streaming
affects: [19-04-full-resync-and-wiring]

# Tech tracking
tech-stack:
  added: []
  patterns: [replicaof-action-enum, readonly-intercept-before-dispatch, info-section-append]

key-files:
  created:
    - src/replication/replica.rs
  modified:
    - src/command/connection.rs
    - src/server/connection.rs
    - src/replication/mod.rs
    - src/shard/mod.rs

key-decisions:
  - "REPLICAOF returns ReplicaofAction enum letting connection handler perform side effects (role change, task spawn)"
  - "READONLY check placed after HELLO/QUIT/REPLICAOF intercepts but before MULTI/dispatch for correct ordering"
  - "INFO intercepted in sharded handler (not dispatch) to append replication section from Arc<RwLock<ReplicationState>>"
  - "run_replica_task uses tokio::task::spawn_local since it runs on the shard's LocalSet"

patterns-established:
  - "ReplicaofAction pattern: command parser returns action enum, caller executes side effects"
  - "READONLY gate: is_write_command check before dispatch, returns READONLY error on replica"

requirements-completed: [REQ-19-01, REQ-19-05, REQ-19-06, REQ-19-07]

# Metrics
duration: 6min
completed: 2026-03-24
---

# Phase 19 Plan 03: Connection Commands Summary

**REPLICAOF/SLAVEOF command handlers, REPLCONF handshake, READONLY enforcement on replicas, INFO replication section, and outbound replica task with PSYNC2 handshake**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-24T16:05:47Z
- **Completed:** 2026-03-24T16:11:36Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- REPLICAOF/SLAVEOF handlers returning ReplicaofAction for role transitions and replica task spawning
- REPLCONF handshake responder accepting all subcommands with OK
- READONLY enforcement in handle_connection_sharded rejecting writes when role=Replica
- INFO replication section appended from build_info_replication when repl_state is present
- Outbound replica task (run_replica_task) with PSYNC2 handshake, full/partial resync, and streaming mode
- 8 unit tests for replicaof and replconf command handlers

## Task Commits

Each task was committed atomically:

1. **Task 1: REPLICAOF, REPLCONF, SLAVEOF command handlers and INFO replication** - `bd0b828` (feat)
2. **Task 2: Wire REPLICAOF/REPLCONF/READONLY into handle_connection_sharded** - `3c96931` (feat)

## Files Created/Modified
- `src/command/connection.rs` - Added replicaof(), replconf(), ReplicaofAction enum, and 8 tests
- `src/replication/replica.rs` - New outbound replica task with PSYNC2 handshake and streaming
- `src/replication/mod.rs` - Added pub mod replica
- `src/server/connection.rs` - REPLICAOF/REPLCONF/INFO interception, READONLY enforcement, repl_state parameter
- `src/shard/mod.rs` - Updated caller to pass repl_state=None, fixed Rust 2024 match ergonomics

## Decisions Made
- REPLICAOF returns ReplicaofAction enum so the connection handler performs side effects (spawn task, change role)
- READONLY check placed after command intercepts (HELLO/QUIT/REPLICAOF) but before MULTI/dispatch
- INFO intercepted in sharded handler to append replication section from Arc<RwLock<ReplicationState>>
- run_replica_task spawned via spawn_local (not spawn) since it runs on the shard's LocalSet

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed Rust 2024 match ergonomics in shard/mod.rs**
- **Found during:** Task 1
- **Issue:** Rust 2024 edition rejects `ref mut` and `ref` in implicitly-borrowing patterns (wal_append_and_fanout)
- **Fix:** Removed explicit `ref mut`/`ref` from match patterns
- **Files modified:** src/shard/mod.rs
- **Verification:** cargo check --lib compiles cleanly
- **Committed in:** bd0b828 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Fix necessary for compilation. No scope creep.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- All connection commands wired and compiling, ready for Plan 04 (full resync and wiring)
- repl_state parameter passed as None; Plan 04 will wire the actual Arc from shard initialization
- run_replica_task stream_commands is a stub (clears buffer); Plan 04 adds RESP parsing and dispatch

---
*Phase: 19-replication-with-psync2-protocol*
*Completed: 2026-03-24*
