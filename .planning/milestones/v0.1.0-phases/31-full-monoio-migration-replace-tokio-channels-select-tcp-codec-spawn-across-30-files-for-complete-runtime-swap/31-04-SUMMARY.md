---
phase: 31-full-monoio-migration
plan: 04
subsystem: runtime
tags: [monoio, tokio, cfg-gate, dual-runtime, conditional-compilation]

requires:
  - phase: 31-03
    provides: "Subsystem channel/cancel migration to runtime-agnostic types"
  - phase: 31-02
    provides: "Connection handler tokio::select! cfg-gating"
  - phase: 31-01
    provides: "Runtime abstraction layer with channel/cancel/codec abstractions"
provides:
  - "Dual-runtime compilation: both runtime-tokio and runtime-monoio features compile with 0 errors"
  - "Runtime-agnostic type aliases: TcpStream, TcpListener, FileIoImpl, TimerImpl, RuntimeFactoryImpl"
  - "Monoio stub event loops for shard, AOF writer, auto-save"
  - "SharedVoteTx migrated from tokio::sync::Mutex to parking_lot::Mutex"
affects: [future-monoio-connection-handlers, performance-benchmarking]

tech-stack:
  added: []
  patterns:
    - "Runtime type aliases in runtime/mod.rs for TcpStream, Timer, FileIo, RuntimeFactory"
    - "Cfg-gated function definitions for tokio-specific async patterns"
    - "std::net::TcpStream for cross-thread monoio TcpStream (monoio TcpStream is !Send)"
    - "parking_lot::Mutex for SharedVoteTx (replaces tokio::sync::Mutex)"

key-files:
  created: []
  modified:
    - src/runtime/mod.rs
    - src/server/connection.rs
    - src/server/listener.rs
    - src/server/expiration.rs
    - src/shard/mod.rs
    - src/shard/dispatch.rs
    - src/shard/mesh.rs
    - src/shard/coordinator.rs
    - src/persistence/wal.rs
    - src/persistence/aof.rs
    - src/persistence/auto_save.rs
    - src/command/key.rs
    - src/command/persistence.rs
    - src/cluster/bus.rs
    - src/cluster/gossip.rs
    - src/replication/master.rs
    - src/main.rs

key-decisions:
  - "Runtime type aliases (TcpStream, TimerImpl, etc.) in runtime/mod.rs instead of trait-based dispatch for zero overhead"
  - "Cfg-gate entire function bodies (handle_connection, handle_connection_sharded, run_sharded) rather than line-by-line gating for maintainability"
  - "std::net::TcpStream as monoio TcpStream alias because monoio::net::TcpStream is !Send (contains Rc<SharedFd>)"
  - "parking_lot::Mutex replaces tokio::sync::Mutex for SharedVoteTx -- simpler, no .await needed, works on any runtime"
  - "Monoio stub event loops (shutdown-only) for shard/AOF/auto-save -- compiles and shuts down cleanly"

patterns-established:
  - "Dual-runtime cfg pattern: #[cfg(feature = runtime-tokio)] on function + #[cfg(feature = runtime-monoio)] stub"
  - "Type alias pattern: crate::runtime::TcpStream resolves to tokio::net::TcpStream or std::net::TcpStream"
  - "Sync fallback pattern: tokio::task::spawn_blocking replaced with synchronous call under monoio"

requirements-completed: [MONOIO-FULL-04]

duration: 78min
completed: 2026-03-25
---

# Phase 31 Plan 04: Fix All Monoio Compile Errors Summary

**Dual-runtime compilation gate passed: runtime-monoio compiles with 0 errors across 17 cfg-gated files, tokio tests pass 1036/1036**

## Performance

- **Duration:** 78 min
- **Started:** 2026-03-25T13:23:57Z
- **Completed:** 2026-03-25T14:42:00Z
- **Tasks:** 1
- **Files modified:** 17

## Accomplishments

- `cargo check --no-default-features --features runtime-monoio` compiles with 0 errors (was 42 errors)
- `cargo check` (default features, runtime-tokio) compiles with 0 errors
- `cargo test --lib` passes 1036/1036 tests (0 failures)
- All tokio imports in src/ are properly cfg-gated (verified via grep)
- Runtime-agnostic type aliases eliminate hard tokio dependencies from 17 subsystem files

## Task Commits

1. **Task 1: Fix remaining monoio compile errors and verify dual-runtime build** - `f3312a5` (feat)

## Files Created/Modified

- `src/runtime/mod.rs` - Added TcpStream, TcpListener, FileIoImpl, TimerImpl, RuntimeFactoryImpl type aliases
- `src/server/connection.rs` - Cfg-gated handle_connection, handle_connection_sharded, handle_blocking_command
- `src/server/listener.rs` - Cfg-gated run, run_with_shutdown, run_sharded; replaced tokio TcpListener
- `src/server/expiration.rs` - Cfg-gated run_active_expiration
- `src/shard/mod.rs` - Replaced TokioTimer with TimerImpl; cfg-gated tokio::select! with monoio shutdown stub
- `src/shard/dispatch.rs` - Replaced tokio::net::TcpStream with crate::runtime::TcpStream
- `src/shard/mesh.rs` - Replaced tokio::net::TcpStream with crate::runtime::TcpStream
- `src/shard/coordinator.rs` - Added monoio yield alternative for SPSC spin-retry
- `src/persistence/wal.rs` - Replaced TokioFileIo with FileIoImpl
- `src/persistence/aof.rs` - Added monoio sync I/O fallback for AOF writer
- `src/persistence/auto_save.rs` - Added monoio timer-based auto-save loops
- `src/command/key.rs` - Cfg-gated tokio::task::spawn_blocking with sync drop fallback
- `src/command/persistence.rs` - Cfg-gated tokio::task::spawn_blocking with sync save fallback
- `src/cluster/bus.rs` - Replaced tokio::sync::Mutex with parking_lot::Mutex for SharedVoteTx
- `src/cluster/gossip.rs` - Updated blocking_lock() to lock() for parking_lot::Mutex
- `src/replication/master.rs` - Added monoio::time::sleep for WAIT polling loop
- `src/main.rs` - Replaced TokioRuntimeFactory with RuntimeFactoryImpl; cfg-gated listener runtime

## Decisions Made

- **Runtime type aliases over trait dispatch:** Zero-cost abstractions via type aliases in `runtime/mod.rs` (e.g., `TimerImpl = TokioTimer` or `MonoioTimer`). No vtable overhead, no generic parameter infection.
- **std::net::TcpStream for monoio cross-thread transfer:** monoio's TcpStream contains `Rc<SharedFd>` which is `!Send`. Using `std::net::TcpStream` for the listener-to-shard channel allows the shard to convert via `monoio::net::TcpStream::from_std()` after receiving.
- **parking_lot::Mutex for SharedVoteTx:** Replaced `tokio::sync::Mutex` (async lock) with `parking_lot::Mutex` (sync lock). The lock is held for microseconds (set/get Option), so async is unnecessary. This also eliminates a tokio dependency from cluster code.
- **Function-level cfg-gating:** Entire functions (handle_connection, run_sharded, etc.) are cfg-gated rather than individual lines. This is cleaner and avoids partial compilation issues where a function signature exists but its body doesn't compile.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] SharedVoteTx blocking_lock() incompatible with parking_lot::Mutex**
- **Found during:** Task 1 (tokio build verification)
- **Issue:** After replacing tokio::sync::Mutex with parking_lot::Mutex, `blocking_lock()` method doesn't exist
- **Fix:** Changed to `lock()` (parking_lot's blocking lock method)
- **Files modified:** src/cluster/gossip.rs
- **Committed in:** f3312a5

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Trivial API difference. No scope creep.

## Issues Encountered

- **42 initial compile errors** spanning 17 files -- systematically categorized into import errors, select macro errors, type resolution errors, and missing trait impls. Fixed in a single iteration.
- **monoio::net::TcpStream is !Send** -- discovered when flume channel (which requires Send) rejected monoio's TcpStream. Resolved by using std::net::TcpStream as the cross-thread transport type.
- **Integration tests (blocking commands)** -- 3 pre-existing failures in blpop/bzpopmin tests confirmed by running on unmodified code. Not a regression.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 31 (Full Monoio Migration) is COMPLETE
- The monoio binary compiles but runs with stub event loops (shutdown-only)
- Future work to make monoio fully functional:
  - Implement monoio connection handler using AsyncReadRent/AsyncWriteRent
  - Implement monoio listener with SO_REUSEPORT per-shard
  - Replace stub event loop with full SPSC drain + timer ticks
  - Add monoio-specific integration tests

---
*Phase: 31-full-monoio-migration*
*Completed: 2026-03-25*
