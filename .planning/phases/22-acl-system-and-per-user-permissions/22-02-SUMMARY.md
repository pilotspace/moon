---
phase: 22-acl-system-and-per-user-permissions
plan: 02
subsystem: auth
tags: [acl, permissions, noperm, auth-acl, connection-handler]

# Dependency graph
requires:
  - phase: 22-acl-system-and-per-user-permissions
    provides: AclTable, AclUser, AclLog, ACL file I/O from plan 01
provides:
  - ACL-aware auth (auth_acl) for 1-arg and 2-arg AUTH forms
  - NOPERM gate in both connection handlers (non-sharded and sharded)
  - 9 ACL subcommand handlers (SETUSER/GETUSER/DELUSER/LIST/WHOAMI/CAT/LOG/SAVE/LOAD)
  - ACL table wired into both handle_connection and handle_connection_sharded
  - Per-connection AclLog for denied command auditing
affects: [23-end-to-end-benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [connection-level ACL intercept, NOPERM error prefix, exempt command bypass]

key-files:
  created: [src/command/acl.rs]
  modified: [src/command/connection.rs, src/command/mod.rs, src/server/connection.rs, src/server/listener.rs, src/shard/mod.rs, src/main.rs, src/acl/table.rs, tests/integration.rs]

key-decisions:
  - "NOPERM error uses exact prefix 'NOPERM ...' (not 'ERR NOPERM') per Redis protocol"
  - "Exempt commands (AUTH, HELLO, QUIT, ACL) bypass NOPERM gate via early continue"
  - "hello_acl returns 4-tuple with optional authenticated username for current_user tracking"
  - "ACL table created from config at startup in both listener.rs (non-sharded) and main.rs (sharded)"
  - "Per-connection AclLog (128-entry) tracks denied commands and failed auth attempts"

patterns-established:
  - "ACL intercept pattern: ACL command handled at connection level like AUTH/BGSAVE, not dispatched"
  - "auth_acl returns (Frame, Option<String>) tuple for response+username, replacing bool comparison"
  - "Peer address captured before Framed::new wraps the TcpStream"

requirements-completed: [ACL-03, ACL-04, ACL-05, ACL-06]

# Metrics
duration: 11min
completed: 2026-03-24
---

# Phase 22 Plan 02: ACL Command Handlers and Connection Wiring Summary

**ACL-aware authentication (auth_acl) with 1-arg/2-arg AUTH, NOPERM pre-dispatch gate in both handlers, and 9 ACL subcommands (SETUSER/GETUSER/DELUSER/LIST/WHOAMI/CAT/LOG/SAVE/LOAD)**

## Performance

- **Duration:** 11 min
- **Started:** 2026-03-24T18:14:55Z
- **Completed:** 2026-03-24T18:26:29Z
- **Tasks:** 2
- **Files modified:** 9

## Accomplishments
- All 9 ACL subcommands implemented in src/command/acl.rs with 16 unit tests
- auth_acl() replaces legacy auth() for ACL-aware 1-arg and 2-arg AUTH
- hello_acl() replaces hello() for HELLO AUTH option using AclTable
- NOPERM gate added to both handle_connection and handle_connection_sharded
- ACL table created at startup and passed through to all connection handlers
- Per-connection current_user tracking and AclLog for denied command auditing
- 981 library tests pass (0 regressions)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create src/command/acl.rs -- ACL subcommand handlers** - `501e73d` (feat)
2. **Task 2: Wire ACL into connection handlers -- auth_acl, current_user state, NOPERM gate** - `8652e22` (feat)

## Files Created/Modified
- `src/command/acl.rs` - handle_acl() dispatches all 9 ACL subcommands with full test coverage
- `src/command/mod.rs` - Added pub mod acl with note about connection-level intercept
- `src/command/connection.rs` - Added auth_acl() and hello_acl() ACL-aware functions
- `src/acl/table.rs` - Added AclTable::new_empty() for ACL LOAD support
- `src/server/connection.rs` - Both handlers: acl_table param, current_user, acl_log, NOPERM gate, ACL intercept
- `src/server/listener.rs` - ACL table creation from config, passed to handle_connection
- `src/shard/mod.rs` - Shard::run() accepts acl_table and runtime_config, passes to handle_connection_sharded
- `src/main.rs` - ACL table and runtime_config creation, passed to shard threads
- `tests/integration.rs` - Updated shard.run() call sites with new acl_table and runtime_config params

## Decisions Made
- NOPERM error uses exact "NOPERM ..." prefix (not "ERR NOPERM") per Redis wire protocol
- Exempt commands (AUTH, HELLO, QUIT, ACL) are handled via early `continue` before the NOPERM gate
- hello_acl returns a 4-tuple `(Frame, u8, Option<Bytes>, Option<String>)` to propagate authenticated username
- Legacy auth()/hello() functions kept for backward compatibility with existing unit tests
- Per-connection AclLog at 128-entry capacity (same as Plan 01 default)
- Peer address captured via stream.peer_addr() BEFORE Framed::new wraps the stream

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ACL system fully live: every connection uses AclTable, every command checked
- All ACL subcommands available for runtime user management
- 4 pre-existing integration test failures (test_list_commands, test_hash_commands, test_hscan, test_aof_restore_on_startup) confirmed unrelated to ACL changes

---
*Phase: 22-acl-system-and-per-user-permissions*
*Completed: 2026-03-24*
