---
phase: 22-acl-system-and-per-user-permissions
plan: 03
subsystem: testing
tags: [acl, integration-tests, noperm, auth, key-patterns]

# Dependency graph
requires:
  - phase: 22-acl-system-and-per-user-permissions
    provides: ACL command handlers, auth_acl, NOPERM gate, AclTable from plans 01+02
provides:
  - 12 integration tests validating all 8 ACL requirement IDs (ACL-01 through ACL-08)
  - Key pattern enforcement in NOPERM gate (both connection handlers)
affects: [23-end-to-end-benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-connection ACL LOG testing, start_server_with_aclfile helper]

key-files:
  created: []
  modified: [tests/integration.rs, src/server/connection.rs]

key-decisions:
  - "ACL LOG is per-connection: test denial and read log on same connection"
  - "Key pattern check added after command permission check in NOPERM gate for both handlers"

patterns-established:
  - "ACL integration tests use connect_single (non-multiplexed) for AUTH flow control"
  - "start_server_with_aclfile helper for ACL SAVE/LOAD persistence tests"

requirements-completed: [ACL-01, ACL-02, ACL-03, ACL-04, ACL-05, ACL-06, ACL-07, ACL-08]

# Metrics
duration: 5min
completed: 2026-03-24
---

# Phase 22 Plan 03: ACL Integration Test Suite Summary

**12 integration tests covering SETUSER/GETUSER/DELUSER/LIST/WHOAMI/CAT/LOG/SAVE/LOAD, 2-arg AUTH, NOPERM enforcement, and key pattern restrictions across all 8 ACL requirement IDs**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-24T18:30:13Z
- **Completed:** 2026-03-24T18:35:22Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- All 12 test_acl_* integration tests pass against live server instances
- Every ACL requirement ID (ACL-01 through ACL-08) validated end-to-end
- Key pattern enforcement added to NOPERM gate (was missing from Plan 02 wiring)
- 981 library tests pass (0 regressions)

## Task Commits

Each task was committed atomically:

1. **Task 1: ACL integration test suite with key pattern fix** - `a106da3` (test)

## Files Created/Modified
- `tests/integration.rs` - 12 new ACL integration tests + start_server_with_aclfile helper
- `src/server/connection.rs` - Added check_key_permission call to NOPERM gate in both handlers

## Decisions Made
- ACL LOG is per-connection (not global), so denial + log read must happen on same connection
- Key pattern check uses is_write_command from persistence::aof for write detection
- Tests use connect_single (non-multiplexed) for precise AUTH flow control

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added key pattern enforcement to NOPERM gate**
- **Found during:** Task 1 (test_acl_key_pattern_single, test_acl_key_pattern_mset)
- **Issue:** NOPERM gate only called check_command_permission, not check_key_permission -- key pattern restrictions were not enforced at the connection level
- **Fix:** Added check_key_permission call after check_command_permission in both handle_connection and handle_connection_sharded
- **Files modified:** src/server/connection.rs
- **Verification:** test_acl_key_pattern_single and test_acl_key_pattern_mset now pass
- **Committed in:** a106da3 (part of task commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential for ACL-06 key pattern enforcement correctness. No scope creep.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ACL system fully tested end-to-end: all 8 requirement IDs validated
- Phase 22 complete -- ready for Phase 23 end-to-end benchmarks

---
*Phase: 22-acl-system-and-per-user-permissions*
*Completed: 2026-03-24*
