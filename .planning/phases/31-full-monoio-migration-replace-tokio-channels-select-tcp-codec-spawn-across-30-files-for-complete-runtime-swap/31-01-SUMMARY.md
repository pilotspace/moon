---
phase: 31-full-monoio-migration
plan: 01
subsystem: runtime
tags: [flume, channels, cancellation, codec, cfg-gate, runtime-agnostic]

requires:
  - phase: 30-monoio-runtime-migration
    provides: Runtime abstraction traits and monoio backend

provides:
  - Runtime-agnostic mpsc/oneshot/watch/Notify channel types via flume
  - Runtime-agnostic CancellationToken (AtomicBool + waker list)
  - Standalone decode_frame/encode_frame methods on RespCodec
  - Cfg-gated Decoder/Encoder trait impls and TokioDriver module

affects: [31-02, 31-03, 31-04, shard, connection, persistence, pubsub]

tech-stack:
  added: [flume 0.11]
  patterns: [cfg-gate runtime-specific trait impls, inherent methods for runtime-agnostic codec usage]

key-files:
  created:
    - src/runtime/channel.rs
    - src/runtime/cancel.rs
  modified:
    - Cargo.toml
    - src/runtime/mod.rs
    - src/server/shutdown.rs
    - src/server/codec.rs
    - src/io/mod.rs

key-decisions:
  - "flume as non-optional dependency (runtime-agnostic, no tokio dep)"
  - "CancellationToken uses AtomicBool + parking_lot::Mutex<Vec<Waker>> for zero-runtime cancellation"
  - "OneshotReceiver implements Future via flume recv_async for FuturesUnordered compatibility"
  - "RespCodec decode_frame/encode_frame as inherent methods; Decoder/Encoder behind cfg(runtime-tokio)"
  - "TokioDriver module gated in io/mod.rs rather than wrapping file contents"

patterns-established:
  - "cfg-gate pattern: runtime-specific trait impls behind feature flag, inherent methods always available"
  - "channel abstraction: flume wrappers with thin type aliases for mpsc, custom types for oneshot/watch"

requirements-completed: [MONOIO-FULL-01]

duration: 3min
completed: 2026-03-25
---

# Phase 31 Plan 01: Runtime-Agnostic Channel Primitives Summary

**flume-based mpsc/oneshot/watch/Notify channels, AtomicBool CancellationToken, and cfg-gated codec Decoder/Encoder for full runtime decoupling**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-25T12:28:15Z
- **Completed:** 2026-03-25T12:31:51Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Added flume 0.11 as runtime-agnostic channel crate with mpsc, oneshot, watch, Notify wrappers
- Created CancellationToken with async cancelled() future using AtomicBool + waker registration
- Added standalone decode_frame/encode_frame methods on RespCodec for runtime-agnostic usage
- Cfg-gated tokio_util Decoder/Encoder impls and TokioDriver module behind runtime-tokio feature
- All 1150 tests pass with zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Add flume crate and create runtime-agnostic channel + cancellation modules** - `e4815bc` (feat)
2. **Task 2: Cfg-gate codec and TokioDriver to compile without tokio_util** - `b798094` (feat)

## Files Created/Modified
- `Cargo.toml` - Added flume 0.11 dependency
- `src/runtime/channel.rs` - Runtime-agnostic mpsc, oneshot, watch, Notify channel types
- `src/runtime/cancel.rs` - CancellationToken with async cancelled() future
- `src/runtime/mod.rs` - Added channel and cancel module declarations
- `src/server/shutdown.rs` - Re-exports CancellationToken from runtime::cancel
- `src/server/codec.rs` - Added decode_frame/encode_frame inherent methods, cfg-gated trait impls
- `src/io/mod.rs` - Cfg-gated tokio_driver module behind runtime-tokio

## Decisions Made
- flume as non-optional dependency -- it has no runtime dependency, works with any async executor
- CancellationToken shares inner state for child_token() (clone semantics) matching codebase usage
- OneshotReceiver implements Future by delegating to flume's recv_async for proper waker registration
- TokioDriver gated at module level in io/mod.rs rather than wrapping file contents with cfg
- Codec tests rewritten to use inherent methods with separate tokio-specific test module

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Runtime-agnostic channel/cancel primitives ready for downstream migration (plans 31-02 through 31-04)
- 20+ files can now migrate from tokio::sync to runtime::channel types
- Codec usable from both runtimes via decode_frame/encode_frame

---
*Phase: 31-full-monoio-migration*
*Completed: 2026-03-25*
