---
phase: 31-full-monoio-migration
plan: 03
subsystem: runtime
tags: [monoio, tokio, channels, cancellation, flume, cfg-gate, runtime-agnostic]

requires:
  - phase: 31-01
    provides: "Runtime-agnostic channel/cancel primitives (flume mpsc/oneshot/watch, AtomicBool CancellationToken)"
provides:
  - "All 13 subsystem files migrated to runtime-agnostic channel/cancel types"
  - "ACL file I/O converted from async tokio::fs to sync std::fs"
  - "Cluster/replication TCP types cfg-gated behind runtime-tokio"
  - "All tokio::select! sites cfg-gated"
affects: [31-04]

tech-stack:
  added: []
  patterns: ["cfg-gate tokio-specific async I/O under runtime-tokio feature", "flume recv_async() for async channel receive", "std::time::Instant replacing tokio::time::Instant for deadlines"]

key-files:
  created: []
  modified:
    - src/persistence/aof.rs
    - src/persistence/auto_save.rs
    - src/replication/master.rs
    - src/replication/replica.rs
    - src/replication/state.rs
    - src/cluster/bus.rs
    - src/cluster/gossip.rs
    - src/cluster/failover.rs
    - src/blocking/mod.rs
    - src/pubsub/subscriber.rs
    - src/tracking/mod.rs
    - src/acl/io.rs
    - src/server/listener.rs
    - src/server/connection.rs
    - src/server/expiration.rs
    - src/shard/mod.rs
    - src/main.rs
    - src/command/persistence.rs
    - tests/integration.rs
    - tests/replication_test.rs

key-decisions:
  - "ACL file I/O converted to synchronous std::fs (rare file ops, not hot path, simplifies runtime abstraction)"
  - "Cluster/replication TCP functions cfg-gated entirely rather than abstracting TcpStream (deep tokio coupling with select!, spawn, OwnedWriteHalf)"
  - "tokio::time::Instant replaced with std::time::Instant in blocking module deadlines for runtime independence"
  - "Cascading call-site fixes applied to server/listener, server/connection, main.rs, shard/mod.rs to maintain type compatibility (Rule 3)"

patterns-established:
  - "cfg-gate pattern: TCP-heavy async functions get #[cfg(feature = \"runtime-tokio\")] at function level"
  - "Channel migration: tokio::sync::mpsc -> channel::mpsc_bounded, recv().await -> recv_async().await"
  - "Oneshot migration: tokio::sync::oneshot::channel() -> channel::oneshot(), Ok(Some(v)) -> Ok(v) pattern"

requirements-completed: [MONOIO-FULL-03]

duration: 46min
completed: 2026-03-25
---

# Phase 31 Plan 03: Subsystem Channel/Cancel Migration Summary

**Migrated 13 subsystem files from direct tokio channel/cancel/fs imports to runtime-agnostic flume channels, custom CancellationToken, and cfg-gated TCP types**

## Performance

- **Duration:** 46 min
- **Started:** 2026-03-25T12:34:29Z
- **Completed:** 2026-03-25T13:20:47Z
- **Tasks:** 2
- **Files modified:** 20

## Accomplishments
- All persistence, replication, cluster, blocking, pubsub, tracking, and ACL files free of unconditional tokio dependencies
- ACL file I/O converted from async tokio::fs to synchronous std::fs (simpler, runtime-agnostic)
- All tokio::select! sites properly cfg-gated behind runtime-tokio feature
- All TCP types (TcpStream, TcpListener, OwnedWriteHalf) cfg-gated or behind cfg-gated functions
- 1036 library tests pass, integration tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1+2: Migrate all 13 subsystems + cascading fixes** - `3093a44` (feat)

**Note:** Tasks 1 and 2 were committed together because cascading type changes across call sites made isolated commits impractical. Plan 02 (31-02, commit 5036857) had already migrated persistence/replication types as part of shard subsystem work.

## Files Created/Modified
- `src/persistence/aof.rs` - cfg-gated tokio::fs/io, flume MpscReceiver for AOF messages
- `src/persistence/auto_save.rs` - WatchSender, CancellationToken, cfg-gated select!/interval
- `src/replication/master.rs` - cfg-gated OwnedWriteHalf, channel::oneshot for snapshots
- `src/replication/replica.rs` - cfg-gated all TCP functions
- `src/replication/state.rs` - channel::MpscSender in ReplicaInfo
- `src/cluster/bus.rs` - cfg-gated TCP, CancellationToken, flume for SharedVoteTx
- `src/cluster/gossip.rs` - cfg-gated TCP, CancellationToken, mpsc_unbounded for votes
- `src/cluster/failover.rs` - cfg-gated TCP, MpscReceiver for votes, std::time for deadlines
- `src/blocking/mod.rs` - channel::OneshotSender, std::time::Instant for deadlines
- `src/pubsub/subscriber.rs` - channel::MpscSender for frame delivery
- `src/tracking/mod.rs` - channel::MpscSender throughout, updated tests
- `src/acl/io.rs` - converted async tokio::fs to sync std::fs, removed async from all functions
- `src/server/listener.rs` - channel types for AOF, CancellationToken (cascading)
- `src/server/connection.rs` - channel types, recv_async() calls (cascading)
- `src/main.rs` - channel::watch, channel::mpsc_bounded (cascading)
- `tests/integration.rs` - runtime::cancel::CancellationToken import
- `tests/replication_test.rs` - runtime::cancel::CancellationToken import

## Decisions Made
- ACL file I/O: async tokio::fs -> sync std::fs because ACL file ops are startup/command-time only, never hot path
- Cluster TCP functions: cfg-gated at function level rather than abstracting TcpStream, because these functions deeply intertwine TCP I/O with tokio::select!, tokio::spawn, and runtime-specific timers
- Used std::time::Instant instead of tokio::time::Instant for blocking deadlines since std::time is runtime-agnostic
- Cascading call-site fixes in server/, shard/, main.rs were necessary to maintain type compatibility after changing struct field types

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fix cascading type mismatches in call-site files**
- **Found during:** Task 1 (after changing type signatures in target files)
- **Issue:** Changing `tokio::sync::mpsc::Sender` -> `channel::MpscSender` in structs caused type mismatches at all call sites (server/listener.rs, server/connection.rs, main.rs, shard/mod.rs, command/persistence.rs, tests/)
- **Fix:** Updated all call sites to use runtime-agnostic channel creation and receive APIs
- **Files modified:** server/listener.rs, server/connection.rs, server/expiration.rs, shard/mod.rs, main.rs, command/persistence.rs, tests/integration.rs, tests/replication_test.rs
- **Verification:** cargo build succeeds, 1036 tests pass
- **Committed in:** 3093a44

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Cascading fixes were necessary and expected -- changing struct field types requires updating all callers. No scope creep.

## Issues Encountered
- Plan 02 (31-02) had already migrated some persistence/replication files as part of its shard subsystem work, so those changes were already committed. This plan built on top of those changes.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All subsystem files migrated to runtime-agnostic types
- Ready for Plan 04 (final integration and verification)
- tokio::spawn and tokio::select! remain cfg-gated (as designed)

---
*Phase: 31-full-monoio-migration*
*Completed: 2026-03-25*
