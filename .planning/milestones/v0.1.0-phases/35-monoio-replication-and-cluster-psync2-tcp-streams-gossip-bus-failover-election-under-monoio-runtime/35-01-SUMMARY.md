---
phase: 35-monoio-replication-and-cluster
plan: 01
subsystem: replication
tags: [monoio, psync2, ownership-io, cfg-gate, tcp-stream]

requires:
  - phase: 32-monoio-runtime
    provides: "monoio ownership I/O patterns in connection handler"
  - phase: 19-replication
    provides: "tokio PSYNC2 master/replica implementation"
provides:
  - "cfg(runtime-monoio) handle_psync_on_master with ownership I/O writes"
  - "cfg(runtime-monoio) register_replica_with_shards using Rc<RefCell<TcpStream>>"
  - "cfg(runtime-monoio) run_replica_task with monoio::net::TcpStream::connect"
  - "cfg(runtime-monoio) run_handshake_and_stream with ownership write macro"
  - "cfg(runtime-monoio) stream_commands, read_line, read_bulk_string with ownership read"
affects: [35-02-cluster-bus]

tech-stack:
  added: []
  patterns: ["monoio ownership I/O for replication TCP", "Rc<RefCell<TcpStream>> for single-threaded stream sharing", "1-byte ownership read loop for handshake read_line"]

key-files:
  created: []
  modified: [src/replication/master.rs, src/replication/replica.rs]

key-decisions:
  - "Used Rc<RefCell<TcpStream>> instead of take/put-back Option pattern since monoio write_all takes &mut self not ownership of stream"
  - "1-byte read loop for read_line acceptable since only called during PSYNC handshake (5 times), not hot path"

patterns-established:
  - "Monoio replication I/O: ownership read/write with cfg-gate mirroring tokio functions"
  - "RefCell borrow_mut across await safe in single-threaded cooperative monoio model"

requirements-completed: [MONOIO-REPL-01, MONOIO-REPL-02]

duration: 4min
completed: 2026-03-25
---

# Phase 35 Plan 01: Monoio Replication PSYNC2 Summary

**PSYNC2 master/replica replication ported to monoio ownership I/O with cfg-gated function pairs for all async operations**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-25T18:33:54Z
- **Completed:** 2026-03-25T18:37:53Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Master-side PSYNC handler (handle_psync_on_master) ported to monoio with full/partial resync support
- Replica registration (register_replica_with_shards) uses Rc<RefCell<TcpStream>> with monoio::spawn sender tasks
- Full replica-side PSYNC flow (connect, handshake, RDB load, streaming) ported to monoio
- All 5 replica async functions have monoio variants using ownership I/O
- Both runtime-tokio and runtime-monoio feature gates compile cleanly
- All 29 existing replication tests pass unchanged

## Task Commits

Each task was committed atomically:

1. **Task 1: Add monoio cfg-gated master-side PSYNC handler and replica registration** - `8a9a1ff` (feat)
2. **Task 2: Add monoio cfg-gated replica-side PSYNC connection, handshake, and streaming** - `50534ee` (feat)

## Files Created/Modified
- `src/replication/master.rs` - Added cfg(runtime-monoio) handle_psync_on_master and register_replica_with_shards
- `src/replication/replica.rs` - Added cfg(runtime-monoio) run_replica_task, run_handshake_and_stream, stream_commands, read_line, read_bulk_string

## Decisions Made
- Used `Rc<RefCell<TcpStream>>` for shared stream in monoio sender tasks instead of the take/put-back `Option` pattern, since monoio's `write_all` takes `&mut self` plus owned buffer (not ownership of stream). RefCell borrow across `.await` is safe because monoio is single-threaded and cooperative.
- 1-byte ownership read loop for `read_line` is acceptable because it's only called during PSYNC handshake (~5 times total), not in the streaming hot path.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed Rc<RefCell<Option<TcpStream>>> take/put-back pattern**
- **Found during:** Task 1 (register_replica_with_shards)
- **Issue:** Plan suggested take/put-back pattern with `Rc<RefCell<Option<TcpStream>>>`, but monoio's `write_all` returns `(Result, Vec<u8>)` not `(Result, TcpStream)` -- stream ownership stays with caller
- **Fix:** Used simpler `Rc<RefCell<TcpStream>>` with direct `borrow_mut()` across await
- **Files modified:** src/replication/master.rs
- **Verification:** Compiles clean under `--no-default-features --features runtime-monoio`
- **Committed in:** 8a9a1ff (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Auto-fix corrected incorrect monoio API assumption in plan. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Replication module fully ported to monoio
- Ready for Plan 35-02 cluster bus and gossip porting

## Self-Check: PASSED

- [x] src/replication/master.rs exists
- [x] src/replication/replica.rs exists
- [x] SUMMARY.md exists
- [x] Commit 8a9a1ff exists
- [x] Commit 50534ee exists

---
*Phase: 35-monoio-replication-and-cluster*
*Completed: 2026-03-25*
