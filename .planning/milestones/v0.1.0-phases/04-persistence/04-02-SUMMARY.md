---
phase: 04-persistence
plan: 02
subsystem: persistence
tags: [aof, append-only-file, fsync, crash-recovery, bgrewriteaof, auto-save]

# Dependency graph
requires:
  - phase: 04-persistence/01
    provides: RDB snapshot save/load, BGSAVE command, ServerConfig persistence fields
provides:
  - AOF writer task with mpsc channel and configurable fsync policies
  - AOF replay for startup crash recovery
  - BGREWRITEAOF compaction with synthetic commands for all 5 data types
  - Auto-save timer with configurable rules
  - Startup restore priority (AOF > RDB > empty)
  - Write command detection and AOF logging from connection handler
affects: [05-production-hardening]

# Tech tracking
tech-stack:
  added: [tokio-fs]
  patterns: [mpsc-channel-aof-logging, resp-based-aof-format, atomic-file-rewrite]

key-files:
  created:
    - src/persistence/aof.rs
    - src/persistence/auto_save.rs
  modified:
    - src/persistence/mod.rs
    - src/server/connection.rs
    - src/server/listener.rs
    - src/command/persistence.rs
    - Cargo.toml
    - tests/integration.rs

key-decisions:
  - "AOF uses RESP wire format (same as client protocol) for maximum compatibility and reuse"
  - "Write command detection via static WRITE_COMMANDS list with case-insensitive matching"
  - "AOF serialization happens before dispatch (which consumes the frame) to capture original command"
  - "AOF logging skipped for error responses (command failed, no state change)"
  - "BGREWRITEAOF uses try_send to avoid blocking the connection handler"
  - "Rewrite uses clone-under-lock + temp Database reconstruction for generate_rewrite_commands"
  - "Auto-save shares AtomicU64 change counter between connections and timer task"
  - "tokio fs feature added for async file I/O in AOF writer"

patterns-established:
  - "AOF channel pattern: mpsc::Sender<AofMessage> passed to connections, Receiver owned by writer task"
  - "Pre-dispatch serialization: capture frame bytes before dispatch consumes it"
  - "Atomic file rewrite: write to .tmp then rename for crash safety"

requirements-completed: [PERS-02, PERS-03, PERS-05]

# Metrics
duration: 5min
completed: 2026-03-23
---

# Phase 04 Plan 02: AOF Persistence Summary

**AOF append-only log with three fsync policies, RESP-format replay, BGREWRITEAOF compaction for all 5 data types, and auto-save timer**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-23T08:16:54Z
- **Completed:** 2026-03-23T08:22:23Z
- **Tasks:** 2
- **Files modified:** 9

## Accomplishments
- AOF writer task with mpsc channel accepting Append/Rewrite/Shutdown messages and three fsync policies (always/everysec/no)
- AOF replay on startup restores all data types via command dispatch, with graceful degradation on corrupt/truncated files
- BGREWRITEAOF generates synthetic SET/HSET/RPUSH/SADD/ZADD + PEXPIRE commands from current state
- Startup restore priority: AOF over RDB when both exist
- Auto-save timer triggers BGSAVE based on configurable rules (N changes in M seconds)
- 21 new unit tests (15 AOF + 6 auto-save) all passing

## Task Commits

Each task was committed atomically:

1. **Task 1+2: AOF writer, replay, rewrite, auto-save, startup priority** - `0f1d2e5` (feat)

**Plan metadata:** [pending] (docs: complete plan)

## Files Created/Modified
- `src/persistence/aof.rs` - AOF writer task, replay, rewrite, is_write_command, serialize_command
- `src/persistence/auto_save.rs` - parse_save_rules, run_auto_save timer task
- `src/persistence/mod.rs` - Added pub mod aof, pub mod auto_save
- `src/server/connection.rs` - AOF logging via aof_tx channel, BGREWRITEAOF handling, change counter
- `src/server/listener.rs` - AOF/RDB startup restore priority, AOF writer spawn, auto-save spawn
- `src/command/persistence.rs` - bgrewriteaof_start function
- `Cargo.toml` - Added tokio fs feature
- `Cargo.lock` - Updated dependencies
- `tests/integration.rs` - Updated ServerConfig struct literals with persistence fields

## Decisions Made
- Combined Task 1 and Task 2 into a single commit since they share tightly coupled code (aof.rs contains both writer and rewrite logic)
- AOF uses RESP wire format for maximum compatibility with the existing parser
- Pre-dispatch serialization: frame is serialized to RESP bytes before dispatch consumes it
- AOF logging is skipped when command returns an error response (no state change occurred)
- BGREWRITEAOF uses try_send to avoid blocking the connection handler on a full channel
- Added tokio "fs" feature for async file operations (Rule 3 - blocking dependency)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added tokio fs feature**
- **Found during:** Task 1
- **Issue:** tokio::fs module requires the "fs" feature which was not enabled
- **Fix:** Added "fs" to tokio features in Cargo.toml
- **Files modified:** Cargo.toml
- **Verification:** Build passes with async file operations
- **Committed in:** 0f1d2e5

**2. [Rule 3 - Blocking] Fixed private module import**
- **Found during:** Task 1
- **Issue:** protocol::frame module is private; Frame and ParseConfig must be imported via re-exports
- **Fix:** Changed import to use crate::protocol::{Frame, ParseConfig}
- **Files modified:** src/persistence/aof.rs
- **Verification:** Build passes
- **Committed in:** 0f1d2e5

**3. [Rule 3 - Blocking] Updated integration test ServerConfig struct literals**
- **Found during:** Task 1
- **Issue:** Integration tests use struct literals for ServerConfig without the persistence fields added in 04-01
- **Fix:** Added all persistence fields to both start_server and start_server_with_pass functions
- **Files modified:** tests/integration.rs
- **Verification:** All 24 integration tests pass
- **Committed in:** 0f1d2e5

---

**Total deviations:** 3 auto-fixed (3 blocking)
**Impact on plan:** All auto-fixes necessary to unblock compilation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Complete persistence layer: RDB snapshots (Plan 01) + AOF logging (Plan 02)
- Server now survives crashes with configurable durability guarantees
- Ready for Phase 05: production hardening

---
*Phase: 04-persistence*
*Completed: 2026-03-23*
