---
phase: 31-full-monoio-migration
plan: 02
subsystem: runtime
tags: [flume, channels, cancellation, runtime-agnostic, monoio]

requires:
  - phase: 31-01
    provides: "Runtime-agnostic channel/cancel primitives (flume, AtomicBool+waker CancellationToken)"
provides:
  - "Core server infrastructure using runtime-agnostic channels and cancellation"
  - "Shard subsystem (dispatch, mesh, coordinator, mod.rs) fully migrated"
  - "Server layer (listener, connection, expiration) fully migrated"
  - "main.rs using runtime-agnostic channel/cancel types"
affects: [31-03, 31-04]

tech-stack:
  added: []
  patterns:
    - "channel::OneshotSender/OneshotReceiver replacing tokio::sync::oneshot in all ShardMessage variants"
    - "channel::MpscSender/MpscReceiver replacing tokio::sync::mpsc for connection distribution and AOF"
    - "channel::Notify replacing tokio::sync::Notify for SPSC wake"
    - "channel::WatchSender/WatchReceiver replacing tokio::sync::watch for snapshot triggers"
    - "send_async()/recv_async() for flume async channel operations"

key-files:
  created: []
  modified:
    - src/shard/dispatch.rs
    - src/shard/mesh.rs
    - src/shard/coordinator.rs
    - src/shard/mod.rs
    - src/server/listener.rs
    - src/server/connection.rs
    - src/server/expiration.rs
    - src/main.rs
    - src/blocking/mod.rs
    - src/pubsub/subscriber.rs
    - src/runtime/channel.rs
    - tests/integration.rs

key-decisions:
  - "WatchReceiver derives Clone for multi-shard snapshot trigger subscription"
  - "tokio::time::Instant replaced with std::time::Instant in blocking WaitEntry deadlines"
  - "sleep_until(Instant) replaced with sleep(Duration) for std::time::Instant compatibility"
  - "flume send_async()/recv_async() used instead of tokio mpsc send()/recv() for async channel ops"

patterns-established:
  - "channel::oneshot() for all cross-shard oneshot communication"
  - "channel::mpsc_bounded() for connection distribution and AOF channels"
  - "channel::watch() for snapshot trigger coordination"
  - "OneshotReceiver implements Future for FuturesUnordered compatibility"

requirements-completed: [MONOIO-FULL-02]

duration: 7min
completed: 2026-03-25
---

# Phase 31 Plan 02: Core Server Channel/Cancel Migration Summary

**Migrated 10+ core server files from tokio::sync to runtime-agnostic flume channels and custom CancellationToken, eliminating ~91 of 152 compile errors for dual-runtime support**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-25T12:34:22Z
- **Completed:** 2026-03-25T12:41:00Z
- **Tasks:** 2
- **Files modified:** 12

## Accomplishments
- Shard subsystem (dispatch.rs, mesh.rs, coordinator.rs, mod.rs) fully migrated to channel::OneshotSender/MpscSender/Notify/WatchReceiver
- Server layer (listener.rs, connection.rs, expiration.rs) migrated to runtime-agnostic CancellationToken and channels
- main.rs uses channel::watch for snapshot triggers, channel::mpsc_bounded for AOF, runtime cancel::CancellationToken
- All 1036 lib tests pass, integration tests compile cleanly

## Task Commits

Each task was committed atomically:

1. **Task 1: Migrate shard subsystem to runtime-agnostic types** - `5036857` (feat)
2. **Task 2: Migrate main.rs, listener.rs, connection.rs, expiration.rs** - `3093a44` (feat)

## Files Created/Modified
- `src/shard/dispatch.rs` - ShardMessage enum variants use channel::OneshotSender/MpscSender
- `src/shard/mesh.rs` - ChannelMesh uses channel::MpscSender/MpscReceiver/Notify for conn distribution and SPSC wake
- `src/shard/coordinator.rs` - Cross-shard coordination uses channel::oneshot()
- `src/shard/mod.rs` - Event loop uses runtime-agnostic channels, WatchReceiver, CancellationToken
- `src/server/listener.rs` - run_sharded takes channel::MpscSender, uses send_async()
- `src/server/connection.rs` - All mpsc/oneshot replaced with channel equivalents, recv_async() for flume
- `src/server/expiration.rs` - CancellationToken from runtime::cancel
- `src/main.rs` - AOF channel, watch channel, CancellationToken all runtime-agnostic
- `src/blocking/mod.rs` - WaitEntry uses channel::OneshotSender and std::time::Instant
- `src/pubsub/subscriber.rs` - Uses channel::MpscSender instead of tokio::sync::mpsc::Sender
- `src/runtime/channel.rs` - WatchReceiver derives Clone for multi-shard subscription
- `tests/integration.rs` - Updated to use channel types for sharded test harness

## Decisions Made
- WatchReceiver#Clone: Added derive(Clone) so multiple shards can subscribe to snapshot triggers (tokio::sync::watch::Receiver is Clone)
- std::time::Instant: Replaced tokio::time::Instant in WaitEntry deadlines since blocking module is now runtime-agnostic; sleep_until converted to sleep(duration) pattern
- flume async API: send_async()/recv_async() used consistently for all async channel operations (flume's send() is synchronous blocking)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Updated blocking/mod.rs WaitEntry to use channel::OneshotSender**
- **Found during:** Task 1 (shard subsystem migration)
- **Issue:** ShardMessage::BlockRegister reply_tx changed to channel::OneshotSender but WaitEntry still used tokio::sync::oneshot::Sender
- **Fix:** Updated WaitEntry.reply_tx type and deadline to use runtime-agnostic types
- **Files modified:** src/blocking/mod.rs
- **Verification:** Compiles, all tests pass
- **Committed in:** 5036857

**2. [Rule 3 - Blocking] Updated pubsub/subscriber.rs to use channel::MpscSender**
- **Found during:** Task 2 (connection.rs migration)
- **Issue:** Subscriber expected tokio::sync::mpsc::Sender but connection.rs now creates channel::MpscSender
- **Fix:** Updated Subscriber struct and constructor to use channel::MpscSender
- **Files modified:** src/pubsub/subscriber.rs
- **Verification:** Compiles, pubsub tests pass
- **Committed in:** 3093a44

**3. [Rule 3 - Blocking] Added Clone derive to WatchReceiver**
- **Found during:** Task 2 (main.rs migration)
- **Issue:** main.rs clones snapshot_trigger_rx for each shard but WatchReceiver didn't implement Clone
- **Fix:** Added #[derive(Clone)] to WatchReceiver (both inner fields are Clone)
- **Files modified:** src/runtime/channel.rs
- **Verification:** Compiles, all tests pass
- **Committed in:** 3093a44

---

**Total deviations:** 3 auto-fixed (3 blocking)
**Impact on plan:** All auto-fixes required for compilation. No scope creep -- cascading type changes from the planned migration.

## Issues Encountered
None - systematic find-replace with compilation-driven iteration.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Core server infrastructure fully migrated to runtime-agnostic primitives
- Ready for 31-03 (remaining subsystem migration) and 31-04 (select! macro migration)
- tokio::select! blocks remain in place (cfg-gated in 31-04)
- tokio::net::TcpStream/TcpListener remain in place (cfg-gated in 31-04)

---
*Phase: 31-full-monoio-migration*
*Completed: 2026-03-25*
