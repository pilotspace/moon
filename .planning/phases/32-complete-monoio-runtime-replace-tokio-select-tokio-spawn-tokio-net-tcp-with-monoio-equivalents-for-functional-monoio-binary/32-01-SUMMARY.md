---
phase: 32-complete-monoio-runtime
plan: 01
subsystem: runtime
tags: [monoio, select, tcp-listener, ctrlc, event-loop, cfg-gate]

requires:
  - phase: 31-full-monoio-migration
    provides: runtime abstraction layer with channel/cancel/timer/spawn traits
provides:
  - Monoio listener with TcpListener::bind + accept + fd-based cross-thread handoff
  - Monoio shard event loop with 7 select! arms mirroring tokio path
  - ctrlc crate for runtime-agnostic signal handling
  - Stub connection handler placeholder for Plan 02
affects: [32-02-monoio-connection-handler, 32-03-monoio-smoke-test]

tech-stack:
  added: [ctrlc]
  patterns: [monoio::select! event loop, IntoRawFd/FromRawFd for cross-thread TcpStream handoff]

key-files:
  created: []
  modified:
    - Cargo.toml
    - src/server/listener.rs
    - src/main.rs
    - src/shard/mod.rs

key-decisions:
  - "ctrlc crate for runtime-agnostic Ctrl+C handling (already transitive dep of monoio)"
  - "IntoRawFd/FromRawFd for monoio TcpStream to std::net::TcpStream cross-thread handoff"
  - "Blocking flume send() in listener (fast enough, avoids async channel complexity)"
  - "Stub connection handler awaits shutdown (Plan 02 implements real handler)"

patterns-established:
  - "monoio::select! mirrors tokio::select! arm-for-arm with identical logic"
  - "monoio::net::TcpStream::from_std for shard-side stream reconstruction"

requirements-completed: [MONOIO-01, MONOIO-02, MONOIO-03]

duration: 10min
completed: 2026-03-25
---

# Phase 32 Plan 01: Monoio Listener, Signal Handling, and Shard Event Loop Summary

**Monoio server startup path with TcpListener accept loop, ctrlc signal handling, and 7-arm shard event loop replacing all stubs**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-25T15:39:12Z
- **Completed:** 2026-03-25T15:49:42Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Monoio listener accepts TCP connections and routes to shard threads via round-robin
- Shard event loop runs with all 7 arms: conn_rx, spsc_notify, periodic, wal_sync, block_timeout, expiry, shutdown
- Both `cargo check --features runtime-monoio` and `cargo check` (default tokio) compile cleanly
- 1036 lib tests pass under tokio with no regression

## Task Commits

Each task was committed atomically:

1. **Task 1: Add ctrlc crate + implement monoio listener and main block** - `b710f25` (feat)
2. **Task 2: Implement monoio select! event loop in shard/mod.rs** - `f3ed7a3` (feat)

## Files Created/Modified
- `Cargo.toml` - Added ctrlc = "3.4" dependency
- `src/server/listener.rs` - Added cfg(runtime-monoio) run_sharded with monoio::net::TcpListener + monoio::select!, run() bail stub
- `src/main.rs` - Replaced monoio stub with real run_sharded call and auto-save thread spawn
- `src/shard/mod.rs` - Added monoio::select! event loop with 7 arms and handle_connection_monoio_stub

## Decisions Made
- ctrlc crate for runtime-agnostic Ctrl+C handling (already transitive dep of monoio, adds no new code)
- IntoRawFd/FromRawFd for monoio TcpStream to std::net::TcpStream cross-thread handoff (monoio TcpStream is !Send due to Rc<SharedFd>)
- Blocking flume send() in listener instead of send_async -- fast enough for connection distribution, avoids async channel complexity
- Stub connection handler awaits shutdown signal -- Plan 02 implements real AsyncReadRent/AsyncWriteRent handler

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Server startup path fully implemented under monoio (main -> listener -> shard event loop)
- Connection handling is stubbed -- Plan 02 implements real handler with monoio I/O
- All tokio paths remain untouched and functional

---
*Phase: 32-complete-monoio-runtime*
*Completed: 2026-03-25*
