---
phase: 19-replication-with-psync2-protocol
plan: 02
subsystem: replication
tags: [psync2, replication, wal-fanout, shard-streaming, spsc]

# Dependency graph
requires:
  - phase: 19-01
    provides: ReplicationBacklog, ReplicationState, increment_shard_offset
  - phase: 14-forkless-compartmentalized-persistence
    provides: Per-shard WAL writer
provides:
  - wal_append_and_fanout() replacing bare wal.append() in shard event loop
  - RegisterReplica/UnregisterReplica ShardMessage variants
  - Per-shard ReplicationBacklog and replica_txs fan-out list
affects: [19-03-connection-commands, 19-04-full-resync]

# Tech tracking
tech-stack:
  added: []
  patterns: [wal-fanout-atomic-append, try-send-nonblocking-fanout, per-shard-replica-channels]

key-files:
  created: []
  modified:
    - src/shard/dispatch.rs
    - src/shard/mod.rs

key-decisions:
  - "wal_append_and_fanout as static Shard method: WAL append + backlog + offset + fan-out in single call"
  - "try_send for fan-out: lagging replicas are skipped, never block the shard event loop"
  - "repl_state is Option<Arc<RwLock<ReplicationState>>> -- None until wired by Plan 03"
  - "ReplicationBacklog always created (1MB per shard) even without persistence_dir -- replication may be enabled later"

patterns-established:
  - "Atomic WAL+backlog+offset+fanout: every write flows through wal_append_and_fanout"
  - "Non-blocking replica fan-out: try_send with dropped errors for slow replicas"

requirements-completed: [REQ-19-03, REQ-19-04]

# Metrics
duration: 4min
completed: 2026-03-24
---

# Phase 19 Plan 02: WAL Fan-Out Integration Summary

**Replaced bare wal.append() with wal_append_and_fanout() that atomically appends to WAL, pushes to ReplicationBacklog, increments monotonic offset, and fans out to replica sender channels via non-blocking try_send**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-24T16:05:10Z
- **Completed:** 2026-03-24T16:09:40Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- RegisterReplica and UnregisterReplica ShardMessage variants for per-shard replica channel management
- wal_append_and_fanout() helper that atomically: (1) appends to WAL, (2) appends to ReplicationBacklog, (3) increments monotonic shard offset via ReplicationState, (4) fans out bytes to all connected replica sender channels
- All bare wal.append() calls in Execute and MultiExecute handlers replaced with wal_append_and_fanout
- Per-shard ReplicationBacklog (1MB), replica_txs Vec, and repl_state Option added to shard run() loop
- drain_spsc_shared and handle_shard_message_shared signatures extended with replication parameters

## Task Commits

Each task was committed atomically:

1. **Task 1: Add RegisterReplica/UnregisterReplica to ShardMessage** - `6fc9ca3` (feat)
2. **Task 2: WAL fan-out integration in shard event loop** - `bd0b828` (feat)

## Files Created/Modified
- `src/shard/dispatch.rs` - Added RegisterReplica and UnregisterReplica variants to ShardMessage enum
- `src/shard/mod.rs` - Added wal_append_and_fanout(), per-shard replication fields, replaced bare WAL appends, wired Register/Unregister handling

## Decisions Made
- wal_append_and_fanout as static Shard method keeping all replication logic in one place
- try_send for fan-out ensures lagging replicas never block the hot write path
- ReplicationBacklog always created even without persistence_dir (replication may be enabled later)
- repl_state is None in this plan; wired by Plan 03 when REPLICAOF is processed

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Every WAL write now also updates ReplicationBacklog and fans out to replica channels
- RegisterReplica/UnregisterReplica ready for Plan 03 connection command wiring
- repl_state placeholder ready for Arc<RwLock<ReplicationState>> injection in Plan 03

## Self-Check: PASSED

All 2 modified files verified present. Both commit hashes (6fc9ca3, bd0b828) verified in git log. 835/835 tests pass.

---
*Phase: 19-replication-with-psync2-protocol*
*Completed: 2026-03-24*
