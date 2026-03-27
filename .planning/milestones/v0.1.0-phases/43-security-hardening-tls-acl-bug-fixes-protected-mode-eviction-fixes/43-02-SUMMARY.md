---
phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes
plan: 02
subsystem: security
tags: [protected-mode, acl, config, redis-compat, connection-filtering]

# Dependency graph
requires:
  - phase: 22-acl-system-and-per-user-permissions
    provides: AclTable, AclLog, ACL file I/O, acl_table_from_config
provides:
  - Protected mode enforcement on accept (non-loopback rejection)
  - Configurable acllog-max-len via CONFIG SET/GET
  - Fixed aclfile loading in non-sharded mode via load_or_default delegation
  - CONFIG SET/GET support for protected-mode
affects: [43-03, server-security, config-management]

# Tech tracking
tech-stack:
  added: []
  patterns: [listener-level connection filtering, config-driven security defaults]

key-files:
  created: []
  modified:
    - src/config.rs
    - src/server/listener.rs
    - src/main.rs
    - src/acl/table.rs
    - src/server/connection.rs
    - src/command/config.rs

key-decisions:
  - "Protected mode check at listener level (before spawning handler) for early rejection"
  - "Monoio protected mode uses ownership-based write_all then drops stream"
  - "AclTable::load_or_default delegates to acl_table_from_config for DRY aclfile loading"

patterns-established:
  - "Listener-level security: reject connections before allocating handler resources"
  - "Config-driven defaults: protected_mode=yes prevents accidental exposure"

requirements-completed: [SEC-ACL-03, SEC-ACL-04, SEC-PROT-01]

# Metrics
duration: 5min
completed: 2026-03-26
---

# Phase 43 Plan 02: Protected Mode, ACL File Loading Fix, Configurable ACL Log Summary

**Redis-compatible protected mode rejecting non-loopback connections when no auth configured, fixed aclfile loading via load_or_default delegation, and CONFIG SET/GET for acllog-max-len and protected-mode**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-26T16:42:17Z
- **Completed:** 2026-03-26T16:47:35Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Protected mode rejects non-loopback connections with Redis-compatible DENIED error in all listener paths (non-sharded, Tokio sharded, Monoio sharded)
- Startup warning printed when protected mode is active and no password/aclfile configured
- AclTable::load_or_default now properly loads aclfile when configured instead of only using requirepass
- All 3 connection handlers read acllog_max_len from RuntimeConfig instead of hardcoding 128
- CONFIG SET/GET supports protected-mode (yes/no) and acllog-max-len (usize)

## Task Commits

Each task was committed atomically:

1. **Task 1: Protected mode + config additions** - `64bb574` (feat)
2. **Task 2: Fix aclfile loading + configurable acllog-max-len** - `a165e0b` (fix)

## Files Created/Modified
- `src/config.rs` - Added protected_mode and acllog_max_len to ServerConfig and RuntimeConfig
- `src/server/listener.rs` - Protected mode enforcement in all 3 listener paths (non-sharded, Tokio sharded, Monoio sharded)
- `src/main.rs` - Startup warning when protected mode is active
- `src/acl/table.rs` - Fixed load_or_default to delegate to acl_table_from_config for aclfile support
- `src/server/connection.rs` - Replaced hardcoded AclLog::new(128) with config-driven max_len in all 3 handlers
- `src/command/config.rs` - Added CONFIG GET/SET for protected-mode and acllog-max-len

## Decisions Made
- Protected mode check placed at listener level (before handler spawn) for minimal resource allocation on rejected connections
- Monoio path uses ownership-based AsyncWriteRentExt::write_all with Vec<u8> buffer, then drops stream to close
- AclTable::load_or_default delegates to existing acl_table_from_config (DRY) rather than duplicating aclfile loading logic

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing build errors (39-40 duplicate definition errors from cfg(feature) conflicts between runtime-tokio and runtime-monoio blocks) prevent full cargo build verification. These are NOT from this plan's changes. All our modified files compile cleanly with no errors attributed to them.

## Next Phase Readiness
- Protected mode and ACL config infrastructure ready for plan 43-03 (CONFIG SET requirepass wiring, eviction fixes)
- All security config fields in RuntimeConfig enable future CONFIG SET/GET extensions

---
*Phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes*
*Completed: 2026-03-26*
