---
phase: 14-forkless-compartmentalized-persistence
plan: 03
subsystem: persistence
tags: [snapshot-integration, wal-integration, shard-event-loop, cow-intercept, bgsave-sharded, startup-restore]

requires:
  - phase: 14-01
    provides: SnapshotState cooperative serializer, shard_snapshot_save/load
  - phase: 14-02
    provides: WalWriter with batched fsync, replay_wal

provides:
  - SnapshotState wired into shard event loop (one segment per tick)
  - WalWriter per-shard with COW intercept and WAL append on write commands
  - SnapshotBegin replaces SnapshotRequest for cooperative snapshots
  - bgsave_start_sharded sends SnapshotBegin to all shards via SPSC
  - Auto-save triggers snapshots via watch channel
  - Startup loads per-shard snapshots + replays per-shard WAL

affects: [19-replication-psync2, shard-lifecycle, persistence]

tech-stack:
  added: [tokio::sync::watch]
  patterns: [cooperative-snapshot-tick, cow-intercept-before-dispatch, watch-channel-trigger]

key-files:
  created: []
  modified:
    - src/shard/mod.rs
    - src/shard/dispatch.rs
    - src/command/persistence.rs
    - src/persistence/auto_save.rs
    - src/server/connection.rs
    - src/main.rs
    - tests/integration.rs

key-decisions:
  - "SnapshotBegin deferred from drain_spsc_shared to main event loop via pending_snapshot option for borrow-safety"
  - "COW intercept runs before cmd_dispatch in handle_shard_message_shared, only clones if segment is pending"
  - "Auto-save uses tokio::sync::watch channel to signal snapshot epoch changes to all shards"
  - "Shard::restore_from_persistence loads snapshot then replays WAL on shard thread before event loop"
  - "BGSAVE in sharded mode sends SnapshotBegin to all shards via existing SPSC producers"

patterns-established:
  - "Watch channel broadcast pattern for cross-thread snapshot coordination"
  - "Pending message pattern: SPSC drain collects deferred messages, caller handles after drain"

requirements-completed: []

duration: 8min
completed: 2026-03-24
---

# Phase 14 Plan 03: Shard Integration -- Cooperative Snapshot + Per-Shard WAL Summary

**Wired forkless snapshot engine and per-shard WAL into shard event loop with COW intercept, SnapshotBegin dispatch, watch-channel auto-save triggers, and startup per-shard restore**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-24T11:23:15Z
- **Completed:** 2026-03-24T11:31:19Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- SnapshotState advances one segment per 1ms tick in the shard event loop (cooperative, non-blocking)
- COW intercept captures old values before write dispatch for pending snapshot segments
- WalWriter appends serialized RESP for every successful write command via SPSC dispatch
- WAL flushes on 1ms tick, truncates after successful snapshot finalization
- SnapshotBegin replaces old clone-based SnapshotRequest in ShardMessage enum
- bgsave_start_sharded sends SnapshotBegin to all shards via existing SPSC producers
- Auto-save uses tokio::sync::watch channel to broadcast snapshot epoch to all shards
- Shard::restore_from_persistence loads per-shard RRDSHARD snapshot then replays per-shard WAL
- Watch channel receiver in shard event loop detects epoch changes and initiates snapshot
- All 66 tests pass (unit + integration)

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire SnapshotState and WalWriter into shard event loop and update dispatch** - `75b313f` (feat)
2. **Task 2: Update BGSAVE, auto-save, and startup restore for per-shard persistence** - `27e83bd` (feat)

## Files Created/Modified
- `src/shard/mod.rs` - Snapshot state machine in spsc tick, COW intercept, WalWriter per shard, restore_from_persistence
- `src/shard/dispatch.rs` - SnapshotBegin replaces SnapshotRequest with epoch + snapshot_dir + reply_tx
- `src/command/persistence.rs` - bgsave_start_sharded, SNAPSHOT_EPOCH atomic counter
- `src/persistence/auto_save.rs` - run_auto_save_sharded using watch channel
- `src/server/connection.rs` - BGSAVE intercept in sharded connection handler
- `src/main.rs` - Watch channel setup, persistence_dir, auto-save wiring, shard restore
- `tests/integration.rs` - Updated Shard::run() call with new parameters

## Decisions Made
- SnapshotBegin deferred from drain_spsc_shared to main event loop via pending_snapshot option for borrow-safety with Rc<RefCell>
- COW intercept extracts primary key from command frame args[1], checks segment pending, clones only if needed
- Auto-save uses watch channel (broadcast) rather than mpsc (point-to-point) for multi-shard notification
- Shard restore happens on shard thread before event loop (no cross-thread database sharing needed)
- BGSAVE in sharded mode reuses existing SPSC producers from connection handler's dispatch_tx

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed Rust 2024 edition binding pattern errors**
- **Found during:** Task 1 (compilation)
- **Issue:** `ref mut` in pattern position within `&mut` context triggers error in Rust 2024 edition
- **Fix:** Changed `if let Some(ref mut wal) = wal_writer` to `if let Some(wal) = wal_writer.as_mut()` inside handle_shard_message_shared
- **Files modified:** src/shard/mod.rs
- **Committed in:** 75b313f

**2. [Rule 3 - Blocking] Fixed integration test for new Shard::run() signature**
- **Found during:** Task 2 (test compilation)
- **Issue:** Integration test called `shard.run()` with 6 args, now requires 8 (persistence_dir + snapshot_trigger_rx)
- **Fix:** Added `None` and a fresh watch receiver as the two new parameters
- **Files modified:** tests/integration.rs
- **Committed in:** 27e83bd

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Trivial signature/edition fixes. No scope creep.

## Issues Encountered
- Rust 2024 edition stricter binding rules required using `.as_mut()` instead of `ref mut` in pattern for `&mut Option<T>` parameters
- SnapshotBegin handling requires deferred processing due to Rc<RefCell> borrow conflicts in drain_spsc_shared

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Forkless per-shard persistence is fully integrated end-to-end
- Snapshot + WAL + auto-save + startup restore all working
- Ready for Phase 15+ (B+ Tree sorted sets, RESP3, etc.)

---
*Phase: 14-forkless-compartmentalized-persistence*
*Completed: 2026-03-24*
