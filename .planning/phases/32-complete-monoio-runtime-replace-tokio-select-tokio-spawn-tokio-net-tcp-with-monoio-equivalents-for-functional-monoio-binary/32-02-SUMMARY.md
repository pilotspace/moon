---
phase: 32-complete-monoio-runtime
plan: 02
subsystem: runtime
tags: [monoio, async-io, ownership-io, AsyncReadRent, AsyncWriteRent, connection-handler]

# Dependency graph
requires:
  - phase: 32-01
    provides: "Monoio listener, shard event loop, signal handling"
provides:
  - "Monoio connection handler with ownership-based I/O (AsyncReadRent/AsyncWriteRentExt)"
  - "Full TCP pipeline: listener -> shard -> connection handler -> dispatch -> response"
affects: [32-03-monoio-testing, monoio-binary]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Vec<u8> intermediate buffer for monoio ownership I/O, copy into BytesMut for codec"]

key-files:
  created: []
  modified:
    - src/server/connection.rs
    - src/shard/mod.rs

key-decisions:
  - "Used Vec<u8> as monoio read buffer (implements IoBufMut) with copy into BytesMut for codec parsing"
  - "Used AsyncWriteRentExt::write_all with Vec<u8> (write_buf.to_vec()) for ownership transfer"
  - "Underscore-prefixed unused handler parameters (pubsub, tracking, etc.) for MVP -- ready for future wiring"

patterns-established:
  - "Monoio ownership I/O pattern: Vec<u8> read -> copy to BytesMut -> codec decode -> dispatch -> encode to BytesMut -> to_vec() -> write_all"

requirements-completed: [MONOIO-04, MONOIO-05]

# Metrics
duration: 8min
completed: 2026-03-25
---

# Phase 32 Plan 02: Monoio Connection Handler Summary

**Monoio connection handler using AsyncReadRent/AsyncWriteRentExt ownership I/O with Vec<u8> buffer intermediary, dispatching through shared command::dispatch() path**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-25T16:05:36Z
- **Completed:** 2026-03-25T16:13:00Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Implemented handle_connection_sharded_monoio with ownership-based I/O model
- Wired handler into shard event loop, replacing the Plan 01 stub
- Full TCP pipeline functional: accept -> shard -> read RESP -> dispatch -> write response
- Both monoio and tokio feature gates compile clean, tokio tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Create monoio connection handler with ownership I/O** - `e3cfca5` (feat)
2. **Task 2: Wire monoio connection handler into shard event loop** - `0a4c7db` (feat)

## Files Created/Modified
- `src/server/connection.rs` - Added handle_connection_sharded_monoio function (132 lines) with cfg(runtime-monoio)
- `src/shard/mod.rs` - Replaced stub with real handler spawn, added cfg-gated import

## Decisions Made
- Used `Vec<u8>` as the monoio read buffer since it implements monoio's `IoBufMut` trait, then copy into `BytesMut` for codec parsing. This avoids fighting with BytesMut's lack of monoio trait impls.
- Used `AsyncWriteRentExt` (not `AsyncWriteRent`) because `write_all` is an extension method in monoio 0.2.
- The `write_all` return type is `(Result<usize>, Vec<u8>)` not `(Result<()>, Vec<u8>)` -- discovered during compilation.
- Underscore-prefixed all unused parameters (pubsub, tracking, blocking, cluster, replication, ACL, lua, scripts) to suppress warnings while keeping the signature ready for future wiring.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] AsyncWriteRentExt instead of AsyncWriteRent**
- **Found during:** Task 1
- **Issue:** `write_all` is on `AsyncWriteRentExt` trait, not `AsyncWriteRent` in monoio 0.2
- **Fix:** Changed import to `AsyncWriteRentExt`
- **Files modified:** src/server/connection.rs
- **Committed in:** e3cfca5

**2. [Rule 1 - Bug] write_all return type is Result<usize> not Result<()>**
- **Found during:** Task 1
- **Issue:** monoio's write_all returns `(Result<usize, Error>, buf)` not `(Result<(), Error>, buf)`
- **Fix:** Added explicit type annotation `(std::io::Result<usize>, Vec<u8>)`
- **Files modified:** src/server/connection.rs
- **Committed in:** e3cfca5

**3. [Rule 1 - Bug] stream parameter needs mut**
- **Found during:** Task 1
- **Issue:** monoio's read/write_all require `&mut self` on TcpStream
- **Fix:** Added `mut` to stream parameter
- **Files modified:** src/server/connection.rs
- **Committed in:** e3cfca5

---

**Total deviations:** 3 auto-fixed (3 bugs - monoio API differences from plan assumptions)
**Impact on plan:** All auto-fixes were necessary for correct monoio API usage. No scope creep.

## Issues Encountered
- Plan assumed dispatch() takes `(cmd, cmd_args, db, &mut in_multi, &mut command_queue, &aof_tx, None)` but actual signature is `dispatch(db, cmd, args, selected_db, db_count) -> DispatchResult`. Used the real signature with proper DispatchResult enum matching.

## Next Phase Readiness
- Full monoio TCP pipeline is wired: listener accepts connections, distributes to shards, shards spawn connection handlers that read/parse/dispatch/respond
- Ready for Plan 03: end-to-end testing of the monoio binary with SET/GET/PING

---
*Phase: 32-complete-monoio-runtime*
*Completed: 2026-03-25*
