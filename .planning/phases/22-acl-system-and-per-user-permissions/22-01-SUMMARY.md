---
phase: 22-acl-system-and-per-user-permissions
plan: 01
subsystem: auth
tags: [acl, sha256, permissions, security, user-management]

# Dependency graph
requires:
  - phase: 03-collection-data-types
    provides: glob_match for key/channel pattern matching
provides:
  - AclTable with per-user command/key/channel permission checking
  - AclUser with SHA256 password hashing and rule parser
  - AclLog circular buffer for denied access auditing
  - ACL file I/O with atomic tmp+rename persistence
  - acl_table_from_config async constructor with aclfile fallback
affects: [22-02-acl-command-handlers, connection-handlers, shard-handlers]

# Tech tracking
tech-stack:
  added: [sha2, hex]
  patterns: [ACL rule token parser, circular buffer audit log, atomic file I/O]

key-files:
  created: [src/acl/mod.rs, src/acl/table.rs, src/acl/rules.rs, src/acl/log.rs, src/acl/io.rs]
  modified: [src/lib.rs, src/config.rs, Cargo.toml, src/storage/eviction.rs, tests/integration.rs, tests/replication_test.rs]

key-decisions:
  - "is_command_allowed: empty allowed + non-empty denied = all-except-denied semantics (not deny-all)"
  - "ACL file I/O uses sorted output for deterministic roundtrip"
  - "acl_table_from_config falls back to load_or_default on missing aclfile (non-fatal)"

patterns-established:
  - "ACL rule parser: match on token prefix for on/off/nopass/>/</#/!/~/%R~/%W~/&/+/-/reset* tokens"
  - "Permission check returns Option<String>: None=allowed, Some(reason)=denied"

requirements-completed: [ACL-01, ACL-02, ACL-07, ACL-08]

# Metrics
duration: 7min
completed: 2026-03-24
---

# Phase 22 Plan 01: ACL Data Layer Summary

**ACL data layer with SHA256 password hashing, 15-category rule parser, per-user command/key/channel permission checking, 128-entry circular audit log, and Redis-compatible file I/O**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-24T18:02:25Z
- **Completed:** 2026-03-24T18:09:25Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- AclTable, AclUser, CommandPermissions, KeyPattern structs with full CRUD operations
- Rule parser handling all Redis ACL token types (on/off, passwords, key patterns, channel patterns, command categories)
- SHA256 password hashing via sha2+hex with verify_password support
- AclLog circular buffer (128-entry max) with most-recent-first retrieval
- ACL file save/load with atomic tmp+rename and comment/blank line handling
- 956 tests pass (43 new ACL tests + all existing)

## Task Commits

Each task was committed atomically:

1. **Task 1: ACL module -- AclUser, AclTable, rule parser, ACL LOG** - `142b3b1` (feat)
2. **Task 2: Add aclfile to ServerConfig and wire AclTable startup loading** - `bb4bd0e` (feat)

## Files Created/Modified
- `src/acl/mod.rs` - Module re-exports for AclTable, AclUser, AclLog, IO functions
- `src/acl/table.rs` - AclTable HashMap-backed user store, AclUser struct, permission checking, key extraction
- `src/acl/rules.rs` - apply_rule() token parser, hash_password(), verify_password(), 15 command categories
- `src/acl/log.rs` - AclLog VecDeque circular buffer with push/entries/reset
- `src/acl/io.rs` - user_to_acl_line(), parse_acl_line(), acl_save(), acl_load(), acl_table_from_config()
- `src/lib.rs` - Added pub mod acl
- `src/config.rs` - Added aclfile to ServerConfig and RuntimeConfig
- `Cargo.toml` - Added sha2 0.10 and hex 0.4 dependencies

## Decisions Made
- Fixed is_command_allowed semantics: when AllAllowed transitions to Specific via deny, empty allowed set with non-empty denied set means "all except denied" (not deny-all)
- ACL file serialization sorts allowed/denied commands for deterministic output
- acl_table_from_config silently falls back to load_or_default when aclfile not found (non-fatal)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed is_command_allowed deny-after-all semantics**
- **Found during:** Task 1 (AclTable implementation)
- **Issue:** Plan's is_command_allowed returned false for all commands when transitioning from AllAllowed to Specific via -cmd, because empty allowed set was treated as deny-all
- **Fix:** Added check: if allowed is empty and denied is non-empty, return true (all except denied)
- **Files modified:** src/acl/table.rs
- **Verification:** test_check_command_permission_denied passes (GET allowed after +@all -set)
- **Committed in:** 142b3b1

**2. [Rule 3 - Blocking] Fixed pre-existing shard test compilation error**
- **Found during:** Task 1 (compilation)
- **Issue:** shard/mod.rs test called drain_spsc_shared with 11 args but function expects 12 (missing script_cache from Phase 21)
- **Fix:** Added script_cache Rc<RefCell<ScriptCache>> argument to both test call sites
- **Files modified:** src/shard/mod.rs
- **Verification:** cargo test --lib passes
- **Committed in:** 142b3b1

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking)
**Impact on plan:** Both fixes necessary for correctness and compilation. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ACL data layer complete: AclTable, AclUser, permission checking, file I/O all exported
- Plan 02 can wire ACL checks into connection handlers and implement ACL command family
- All contracts (check_command_permission, check_key_permission, check_channel_permission, authenticate) ready for integration

---
*Phase: 22-acl-system-and-per-user-permissions*
*Completed: 2026-03-24*
