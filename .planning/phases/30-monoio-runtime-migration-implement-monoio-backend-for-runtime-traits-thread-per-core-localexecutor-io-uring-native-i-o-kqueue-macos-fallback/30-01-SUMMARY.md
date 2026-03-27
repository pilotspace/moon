---
phase: 30-monoio-runtime-migration
plan: 01
subsystem: runtime
tags: [monoio, io_uring, kqueue, thread-per-core, async-runtime, feature-flags]

requires:
  - phase: 29-runtime-abstraction-layer
    provides: Runtime trait definitions (RuntimeTimer, RuntimeInterval, RuntimeSpawn, RuntimeFactory, FileIo, FileSync)
provides:
  - Monoio backend implementing all 6 runtime traits
  - runtime-monoio feature flag with compile-time mutex
  - FusionDriver support (io_uring on Linux, kqueue on macOS)
affects: [monoio-integration, runtime-migration, server-startup]

tech-stack:
  added: [monoio 0.2]
  patterns: [cfg-gated runtime backends, compile_error! mutual exclusion, FusionDriver cross-platform]

key-files:
  created: [src/runtime/monoio_impl.rs]
  modified: [Cargo.toml, src/runtime/mod.rs]

key-decisions:
  - "monoio 0.2 without timer feature (timer is built-in, no separate feature needed)"
  - "FileSync cfg(not(runtime-tokio)) guard to avoid duplicate impl for std::fs::File"
  - "FusionDriver for cross-platform support (io_uring Linux, kqueue macOS)"
  - "drop(monoio::spawn(future)) for fire-and-forget spawn semantics"

patterns-established:
  - "cfg-gated runtime backend: mod + re-export pattern with compile_error! mutex"
  - "FileSync dedup: only one backend defines impl FileSync for std::fs::File"

requirements-completed: [MONOIO-IMPL]

duration: 3min
completed: 2026-03-25
---

# Phase 30 Plan 01: Monoio Runtime Backend Summary

**Monoio backend for all 6 runtime traits with FusionDriver (io_uring/kqueue), feature-gated behind runtime-monoio with compile-time mutex**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-25T12:04:43Z
- **Completed:** 2026-03-25T12:07:35Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- All 6 runtime traits implemented for Monoio (MonoioTimer, MonoioInterval, MonoioSpawner, MonoioRuntimeFactory, MonoioFileIo, FileSync)
- Compile-time mutual exclusion prevents enabling both runtime-tokio and runtime-monoio
- Zero regression: all 1134 existing tests pass with default tokio feature
- FusionDriver provides io_uring on Linux and kqueue fallback on macOS

## Task Commits

Each task was committed atomically:

1. **Task 1: Add monoio dependency and implement all 6 runtime traits** - `c5b47d1` (feat)
2. **Task 2: Wire cfg-gated module exports and compile-time mutex** - `2c1c20e` (feat)

## Files Created/Modified
- `src/runtime/monoio_impl.rs` - All 6 trait implementations for Monoio runtime
- `Cargo.toml` - monoio optional dependency and runtime-monoio feature
- `src/runtime/mod.rs` - cfg-gated module, re-exports, compile_error! mutex

## Decisions Made
- monoio 0.2 does not have a `timer` feature -- timer is built-in (plan said to use features=["timer"] but it doesn't exist)
- FileSync for std::fs::File uses `cfg(not(feature = "runtime-tokio"))` in monoio_impl to avoid duplicate impl
- FusionDriver chosen for cross-platform support (io_uring on Linux, kqueue on macOS)
- `drop(monoio::spawn(future))` for fire-and-forget semantics (JoinHandle dropped)
- monoio RuntimeBuilder does not have thread_name -- name parameter used only in panic message

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed non-existent timer feature from monoio dependency**
- **Found during:** Task 1 (adding monoio dependency)
- **Issue:** Plan specified `features = ["timer"]` but monoio 0.2 has no timer feature (timer is built-in)
- **Fix:** Removed features array, used plain `monoio = { version = "0.2", optional = true }`
- **Files modified:** Cargo.toml
- **Verification:** cargo check --no-default-features --features runtime-monoio resolves dependencies
- **Committed in:** c5b47d1

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minor dependency spec correction. No scope creep.

## Issues Encountered
- Full `cargo check --no-default-features --features runtime-monoio` produces 151 errors from non-runtime code that directly uses tokio (expected -- rest of codebase not yet migrated). The monoio_impl.rs module itself compiles with zero errors.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Monoio backend ready for integration into server startup path
- Next steps: cfg-gate remaining direct tokio usage in server code, or add runtime-agnostic type aliases
- Consider adding monoio-specific optimizations (DMA file I/O, registered buffers)

---
*Phase: 30-monoio-runtime-migration*
*Completed: 2026-03-25*
