---
phase: 21-lua-scripting-engine
plan: 02
subsystem: scripting
tags: [lua, mlua, shard, spsc, integration-tests]

# Dependency graph
requires:
  - phase: 21-lua-scripting-engine
    provides: ScriptCache, setup_lua_vm(), handle_eval(), handle_evalsha(), handle_script_subcommand()
  - phase: 11-sharded-architecture
    provides: Shard::run(), handle_connection_sharded, SPSC ChannelMesh, Rc<RefCell> database sharing
provides:
  - Lua VM initialized per-shard in Shard::run() (Lua is !Send)
  - EVAL/EVALSHA/SCRIPT intercept in handle_connection_sharded
  - ScriptLoad fan-out via SPSC ChannelMesh for cross-shard EVALSHA
  - 13 integration tests covering all scripting requirements
affects: [22-acl-system, 23-benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [SPSC fan-out for ScriptLoad mirroring PubSubFanOut pattern, Rc<Lua> cloned into spawn_local tasks]

key-files:
  created: []
  modified:
    - src/shard/dispatch.rs
    - src/shard/mod.rs
    - src/server/connection.rs
    - tests/integration.rs

key-decisions:
  - "Lua VM and ScriptCache initialized after blocking_rc in Shard::run() (before snapshot_state)"
  - "EVAL/EVALSHA intercept placed after CLUSTER block, before cluster slot routing"
  - "ScriptLoad fan-out uses ChannelMesh::target_index matching PubSubFanOut pattern"
  - "lua and script_cache params added before config_port in handle_connection_sharded signature"

patterns-established:
  - "ScriptLoad SPSC fan-out: same try_push best-effort pattern as PubSubFanOut"
  - "SHA1 verification in ScriptLoad handler prevents cache poisoning"

requirements-completed: [EVAL-01, EVAL-02, EVAL-03, SCRIPT-01, SCRIPT-02, SCRIPT-03, SANDBOX-01, SANDBOX-02, REDIS-01, REDIS-02, TIMEOUT-01, CROSSSLOT-01]

# Metrics
duration: 5min
completed: 2026-03-24
---

# Phase 21 Plan 02: Lua Scripting Integration Summary

**Wired Lua VM into shard event loop with EVAL/EVALSHA/SCRIPT intercept in connection handler, ScriptLoad SPSC fan-out, and 13 integration tests**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-24T17:42:12Z
- **Completed:** 2026-03-24T17:47:30Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- ShardMessage::ScriptLoad variant for cross-shard script cache synchronization
- Lua VM (Rc<mlua::Lua>) and ScriptCache (Rc<RefCell>) initialized in Shard::run() on shard thread
- EVAL/EVALSHA intercepted before cluster slot routing with database borrow for script execution
- SCRIPT LOAD fan-out to all other shards via SPSC ChannelMesh (matching PubSubFanOut pattern)
- 13 integration tests all passing: scripting commands, sandbox, redis.call/pcall, timeout, return types

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire VM into shard and connection handler** - `4fc21d3` (feat)
2. **Task 2: Integration tests for all scripting requirements** - `b52cdc8` (test)

## Files Created/Modified
- `src/shard/dispatch.rs` - Added ScriptLoad { sha1, script } variant to ShardMessage enum
- `src/shard/mod.rs` - Lua VM init in Shard::run(), ScriptLoad handling in SPSC drain, clone into spawn_local
- `src/server/connection.rs` - Added lua/script_cache params, EVAL/EVALSHA/SCRIPT intercept blocks
- `tests/integration.rs` - 13 integration tests covering all scripting requirements

## Decisions Made
- Lua VM initialized after blocking_rc to keep Rc wrapping section contiguous
- EVAL/EVALSHA intercept placed after CLUSTER block but before cluster slot routing (scripts can contain their own key validation)
- ScriptLoad fan-out follows exact PubSubFanOut pattern: borrow_mut producers, iterate targets, try_push
- SHA1 verification in ScriptLoad SPSC handler prevents cache poisoning from malformed messages

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed NOSCRIPT error assertion in test_evalsha_noscript**
- **Found during:** Task 2 (integration test execution)
- **Issue:** redis crate formats error as "NoScriptError" not "NOSCRIPT"
- **Fix:** Added alternate check for "NoScript" in assertion
- **Files modified:** tests/integration.rs
- **Verification:** test_evalsha_noscript passes
- **Committed in:** b52cdc8 (part of Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Minor test assertion fix. No scope creep.

## Issues Encountered
None - all wiring followed the established patterns from PubSubFanOut and connection handler.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Full Lua scripting engine complete: module (Plan 01) + integration (Plan 02)
- Phase 21 complete, ready for Phase 22 (ACL System)
- All 12 scripting requirements verified via integration tests

---
*Phase: 21-lua-scripting-engine*
*Completed: 2026-03-24*
