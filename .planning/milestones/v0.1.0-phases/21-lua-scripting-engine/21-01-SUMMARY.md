---
phase: 21-lua-scripting-engine
plan: 01
subsystem: scripting
tags: [lua, mlua, sha1, sandbox, scripting]

# Dependency graph
requires:
  - phase: 11-sharded-architecture
    provides: thread-per-core shard model, Rc<RefCell> database sharing, SPSC channel mesh
  - phase: 02-working-server
    provides: dispatch() function, Frame enum, Database struct
provides:
  - ScriptCache with SHA1 hashing (load/get/exists/flush)
  - Sandboxed Lua 5.4 VM via mlua (no os/io/require/load/dofile/loadfile)
  - redis.call/pcall bridge via thread_local CURRENT_DB pointer
  - Full Frame to Lua type conversion (both directions)
  - Timeout hooks with 100k-instruction interval and 5-second wall-clock deadline
  - setup_lua_vm(), handle_eval(), handle_evalsha(), handle_script_subcommand()
  - parse_eval_args(), validate_keys_same_shard()
affects: [22-acl-system, 21-02-integration]

# Tech tracking
tech-stack:
  added: [mlua 0.11 (lua54 vendored), sha1_smol 1.0 (std)]
  patterns: [thread_local Cell pointer for bridge, closure-based cleanup for Pitfall 3]

key-files:
  created:
    - src/scripting/mod.rs
    - src/scripting/cache.rs
    - src/scripting/sandbox.rs
    - src/scripting/types.rs
    - src/scripting/bridge.rs
  modified:
    - Cargo.toml
    - src/lib.rs

key-decisions:
  - "sha1_smol requires std feature for hexdigest()"
  - "Closure-based cleanup pattern: run_script captures result, then unconditionally calls remove_timeout_hook+clear_script_db"
  - "handle_script_subcommand returns (Frame, Option<(String, Bytes)>) for SCRIPT LOAD fan-out signal"
  - "VmState::Continue required by mlua 0.11 hook callback (not Result<()>)"
  - "BorrowedBytes deref via &s.as_bytes() for Bytes::copy_from_slice compatibility"
  - "Frame::Double(f64) not OrderedFloat -- direct f64 cast to i64"

patterns-established:
  - "thread_local bridge: Set CURRENT_DB before Lua run, clear after, access via unsafe pointer dereference (safe in single-threaded shard)"
  - "Guaranteed cleanup: Capture script result in closure, always run remove_timeout_hook + clear_script_db unconditionally after"

requirements-completed: [EVAL-01, EVAL-02, EVAL-03, SCRIPT-01, SCRIPT-02, SCRIPT-03, SANDBOX-01, SANDBOX-02, REDIS-01, REDIS-02, TIMEOUT-01, CROSSSLOT-01]

# Metrics
duration: 9min
completed: 2026-03-24
---

# Phase 21 Plan 01: Lua Scripting Engine Core Summary

**Sandboxed Lua 5.4 VM via mlua with SHA1 script cache, redis.call/pcall bridge through thread_local DB pointer, full Frame-Lua type conversion, and 5-second timeout hooks**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-24T17:29:51Z
- **Completed:** 2026-03-24T17:39:00Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- ScriptCache with SHA1-keyed HashMap storing script source bytes, idempotent load
- Sandboxed Lua VM: os restricted to clock() only, package/load/dofile/loadfile/collectgarbage nil-ed out
- redis.call/pcall bridge: thread_local CURRENT_DB pointer, dispatch() invocation, write tracking for SCRIPT KILL
- Complete bidirectional type conversion: all LuaValue variants to Frame (including float truncation) and all Frame variants to LuaValue
- Timeout hook: every 100k instructions checks wall-clock, guaranteed removal in both success and error paths
- 41 unit tests passing across all submodules

## Task Commits

Each task was committed atomically:

1. **Task 1: Add dependencies and create ScriptCache plus module skeleton** - `155bc08` (feat)
2. **Task 2: Implement sandbox, type conversion, bridge, and complete mod.rs** - `fd2a8cd` (feat)

## Files Created/Modified
- `Cargo.toml` - Added mlua (lua54, vendored) and sha1_smol (std) dependencies
- `src/lib.rs` - Added pub mod scripting declaration
- `src/scripting/mod.rs` - Main orchestration: setup_lua_vm, handle_eval, handle_evalsha, handle_script_subcommand, run_script
- `src/scripting/cache.rs` - ScriptCache with SHA1 load/get/exists/flush/len
- `src/scripting/sandbox.rs` - Sandbox setup, timeout hooks, redis.* API registration
- `src/scripting/types.rs` - lua_value_to_frame and frame_to_lua_value type conversion
- `src/scripting/bridge.rs` - thread_local CURRENT_DB, make_redis_call_fn for redis.call/pcall

## Decisions Made
- sha1_smol requires `std` feature for hexdigest() method (alloc feature enables it via std)
- mlua 0.11 hook callback requires Result<VmState> return (not Result<()>)
- mlua 0.11 LuaValue has no lifetime parameter (unlike earlier versions)
- BorrowedBytes from mlua String::as_bytes() requires deref to &[u8] for Bytes::copy_from_slice
- Frame::Double wraps f64 directly (not OrderedFloat) -- cast to i64 for Lua Integer conversion

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed pre-existing test compile error in snapshot.rs**
- **Found during:** Task 2 (test verification)
- **Issue:** snapshot.rs:574 ambiguous as_ref() type inference (E0283)
- **Fix:** Changed to `s.as_ref() as &[u8]` for explicit type disambiguation
- **Files modified:** src/persistence/snapshot.rs
- **Verification:** cargo test --lib scripting passes (41 tests)
- **Committed in:** fd2a8cd (part of Task 2 commit)

**2. [Rule 1 - Bug] Adapted mlua 0.11 API differences from research patterns**
- **Found during:** Task 2 (initial build)
- **Issue:** Research patterns used LuaValue<'lua> (removed in mlua 0.11), Result<()> for hooks (now Result<VmState>), .into_inner() for f64 (Frame::Double wraps f64 not OrderedFloat)
- **Fix:** Removed lifetime parameters, used VmState::Continue, used direct f64 cast, used &s.as_bytes() deref
- **Files modified:** src/scripting/types.rs, src/scripting/sandbox.rs
- **Verification:** cargo build clean, 41 tests pass
- **Committed in:** fd2a8cd (part of Task 2 commit)

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** All fixes necessary for compilation and test execution. No scope creep.

## Issues Encountered
- mlua 0.11 API diverged from research document patterns in several ways (lifetime elision, VmState return, BorrowedBytes). All resolved through API investigation.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Full scripting module ready for Plan 02 integration into shard handler
- Plan 02 only needs to: call setup_lua_vm() in Shard::run(), pass Rc<Lua> and Rc<RefCell<ScriptCache>> to connection tasks, intercept EVAL/EVALSHA/SCRIPT commands

---
*Phase: 21-lua-scripting-engine*
*Completed: 2026-03-24*
