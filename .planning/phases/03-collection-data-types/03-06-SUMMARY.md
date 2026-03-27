---
phase: 03-collection-data-types
plan: 06
subsystem: infra
tags: [expiration, auth, background-task, probabilistic-sampling, password]

# Dependency graph
requires:
  - phase: 03-01
    provides: "Database keys_with_expiry, is_key_expired, remove methods"
provides:
  - "Active expiration background task with probabilistic sampling"
  - "AUTH command and connection-level authentication gating"
  - "requirepass CLI option for server password configuration"
affects: [04-persistence, 05-production]

# Tech tracking
tech-stack:
  added: [tokio-time-feature]
  patterns: [probabilistic-expiration, connection-auth-gate]

key-files:
  created:
    - src/server/expiration.rs
  modified:
    - src/server/mod.rs
    - src/server/listener.rs
    - src/server/connection.rs
    - src/config.rs
    - src/command/connection.rs
    - Cargo.toml

key-decisions:
  - "AUTH handled directly in connection loop, not through dispatch (needs mutable authenticated state)"
  - "QUIT allowed even when unauthenticated (clean disconnect)"
  - "expire_cycle uses rand::seq::IndexedRandom::choose_multiple for sampling"
  - "Tokio time feature added for interval-based expiration scheduling"

patterns-established:
  - "Background task pattern: spawn with cloned Arc<Mutex<>> and child CancellationToken"
  - "Auth gate pattern: extract command name before dispatch, intercept AUTH/QUIT"

requirements-completed: [EXP-02, CONN-06, KEY-07]

# Metrics
duration: 3min
completed: 2026-03-23
---

# Phase 3 Plan 6: Active Expiration & AUTH Summary

**Probabilistic active expiration (100ms/20-key sampling with 1ms budget) and AUTH password authentication with connection-level gating**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-23T07:35:52Z
- **Completed:** 2026-03-23T07:38:58Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Active expiration background task runs every 100ms, samples 20 random keys with TTL, repeats if >25% expired, enforces 1ms budget
- AUTH command validates password against requirepass config, returns OK/WRONGPASS/ERR
- Connection auth gate rejects all commands except AUTH and QUIT when requirepass is set and client is unauthenticated
- requirepass CLI option added to ServerConfig

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement active expiration background task** - `68ae903` (feat)
2. **Task 2: Implement AUTH command and connection authentication gate** - `99d437e` (feat)

## Files Created/Modified
- `src/server/expiration.rs` - Active expiration background task with probabilistic expire_cycle
- `src/server/mod.rs` - Added expiration module declaration
- `src/server/listener.rs` - Spawns expiration task on startup, passes requirepass to connections
- `src/server/connection.rs` - Auth gate: intercepts AUTH before dispatch, rejects unauthenticated commands
- `src/config.rs` - Added requirepass CLI option to ServerConfig
- `src/command/connection.rs` - AUTH command handler with password validation
- `Cargo.toml` - Added tokio "time" feature

## Decisions Made
- AUTH handled directly in connection loop rather than through dispatch, since it needs mutable access to the `authenticated` flag on the connection
- QUIT is allowed even when unauthenticated, matching Redis behavior for clean disconnects
- expire_cycle runs entirely within one lock acquisition with a 1ms time budget to prevent holding the mutex too long
- Added tokio "time" feature for `tokio::time::interval` used by the expiration background task

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added tokio "time" feature to Cargo.toml**
- **Found during:** Task 1 (Active expiration)
- **Issue:** `tokio::time::interval` requires the "time" feature which was not enabled
- **Fix:** Added "time" to tokio features in Cargo.toml
- **Files modified:** Cargo.toml
- **Verification:** Build succeeds
- **Committed in:** 68ae903 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Essential for compilation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Active expiration prevents memory growth from unaccessed expired keys
- AUTH infrastructure ready for production deployment scenarios
- All 315 tests pass

---
*Phase: 03-collection-data-types*
*Completed: 2026-03-23*
