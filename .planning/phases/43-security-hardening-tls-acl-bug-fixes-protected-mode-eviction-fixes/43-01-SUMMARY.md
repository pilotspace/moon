---
phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes
plan: 01
subsystem: security
tags: [auth-bypass, acl, eviction, lru, pubsub, requirepass, protected-mode]

# Dependency graph
requires:
  - phase: 42-inline-dispatch
    provides: "Sharded Tokio and Monoio handlers with inline dispatch"
provides:
  - "requirepass enforcement in both sharded handlers (P0 auth bypass fix)"
  - "ACL channel permission checks in all 5 SUBSCRIBE/PSUBSCRIBE paths"
  - "Eviction in sharded write paths + background eviction timer"
  - "Wraparound-safe LRU clock comparison via lru_is_older()"
  - "aggregate_used_memory helper for cross-database eviction"
  - "protected_mode, requirepass, acllog_max_len in RuntimeConfig"
affects: [43-02, 43-03, security-audit]

# Tech tracking
tech-stack:
  added: []
  patterns: ["i16 wrapping_sub for circular clock comparison", "ACL check before subscribe pattern"]

key-files:
  created: []
  modified:
    - src/shard/mod.rs
    - src/server/connection.rs
    - src/storage/eviction.rs
    - src/config.rs
    - src/server/listener.rs
    - src/main.rs

key-decisions:
  - "Used i16 wrapping_sub for LRU wraparound instead of modular arithmetic -- simpler and handles all edge cases"
  - "Added eviction check before write dispatch using separate borrow_mut to avoid holding lock during dispatch"
  - "Background eviction runs at same 100ms interval as expiry for consistency"

patterns-established:
  - "ACL channel check: acquire read lock, check_channel_permission, drop before I/O"
  - "Eviction guard: borrow_mut -> try_evict -> drop before main dispatch borrow_mut"

requirements-completed: [SEC-ACL-01, SEC-ACL-02, SEC-EVIC-01, SEC-EVIC-02, SEC-EVIC-03, SEC-EVIC-04]

# Metrics
duration: 9min
completed: 2026-03-26
---

# Phase 43 Plan 01: Auth Bypass Fix + Eviction Wiring + ACL Channel Enforcement Summary

**Fixed P0 auth bypass in both sharded handlers, enforced ACL channel permissions in all 5 SUBSCRIBE paths, wired eviction into sharded write dispatch with background timer, and fixed LRU u16 clock wraparound**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-26T16:41:33Z
- **Completed:** 2026-03-26T16:51:27Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Eliminated P0 auth bypass: both Tokio and Monoio sharded handlers now read requirepass from RuntimeConfig instead of passing hardcoded None
- ACL channel permission enforcement added to all 5 SUBSCRIBE/PSUBSCRIBE code paths (7 total check_channel_permission call sites)
- Eviction wired into sharded write dispatch paths for both Tokio and Monoio, plus background 100ms eviction timer in shard event loop
- LRU clock comparison fixed to handle u16 wraparound using signed-distance (i16 wrapping_sub)
- Added aggregate_used_memory helper for cross-database eviction decisions
- Protected mode enforcement added to all listener paths (linter-contributed)

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix auth bypass + wire eviction into both sharded handlers** - `df11d87` (feat)
2. **Task 2: ACL channel permission enforcement + LRU clock wraparound fix** - `23eb03b` (feat)

## Files Created/Modified
- `src/config.rs` - Added requirepass, protected_mode, acllog_max_len to RuntimeConfig
- `src/shard/mod.rs` - Wired requirepass from RuntimeConfig to both handler spawns, added background eviction timer
- `src/server/connection.rs` - Added try_evict_if_needed before write dispatch in both sharded handlers, added check_channel_permission in all 5 SUBSCRIBE paths
- `src/storage/eviction.rs` - Added lru_is_older() with wrapping_sub, aggregate_used_memory(), fixed test config
- `src/server/listener.rs` - Fixed monoio write_all mut borrow in protected mode handler
- `src/main.rs` - Protected mode startup warning

## Decisions Made
- Used i16 wrapping_sub for LRU wraparound: simpler than modular arithmetic, correct for all edge cases including clock at 0 vs 65535
- Separated eviction borrow_mut from dispatch borrow_mut: avoids holding database lock during eviction when dispatch also needs it
- Background eviction at 100ms matches existing expiry timer interval for consistency

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed monoio listener write_all mut borrow**
- **Found during:** Task 1 (build verification)
- **Issue:** Linter-added protected mode code in listener.rs passed `stream` instead of `&mut stream` to monoio write_all
- **Fix:** Changed `Ok((stream, addr))` to `Ok((mut stream, addr))` and `write_all(stream, ...)` to `write_all(&mut stream, ...)`
- **Files modified:** src/server/listener.rs
- **Committed in:** df11d87

**2. [Rule 3 - Blocking] Fixed monoio spawn borrow-after-move for requirepass**
- **Found during:** Task 2 (monoio build verification)
- **Issue:** runtime_config was moved into monoio::spawn closure for requirepass extraction, then cloned in next loop iteration
- **Fix:** Used already-cloned `rtcfg` instead of `runtime_config` inside the spawn closure
- **Files modified:** src/shard/mod.rs
- **Committed in:** 23eb03b

**3. [Rule 3 - Blocking] Fixed RuntimeConfig construction in eviction tests**
- **Found during:** Task 2 (test compilation)
- **Issue:** New fields (requirepass, protected_mode, acllog_max_len) missing in test make_config()
- **Fix:** Added the three new fields to the test helper
- **Files modified:** src/storage/eviction.rs
- **Committed in:** 23eb03b

---

**Total deviations:** 3 auto-fixed (3 blocking)
**Impact on plan:** All auto-fixes necessary for compilation. No scope creep.

## Issues Encountered
- 35 pre-existing compilation errors in unrelated modules (cluster, replication, runtime duplication) prevent full test suite execution. Eviction and ACL code compiles cleanly. Monoio feature builds with zero errors.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Auth bypass is closed; both sharded handlers enforce requirepass
- All SUBSCRIBE paths enforce ACL channel permissions
- Eviction is active in both sharded handlers and background timer
- Ready for Plan 02 (additional security hardening if planned)

---
*Phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes*
*Completed: 2026-03-26*
