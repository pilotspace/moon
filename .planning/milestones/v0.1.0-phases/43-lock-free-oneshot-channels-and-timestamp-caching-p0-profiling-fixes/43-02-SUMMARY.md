---
phase: 43-lock-free-oneshot-channels-and-timestamp-caching-p0-profiling-fixes
plan: 02
subsystem: storage
tags: [atomic, timestamp-cache, clock_gettime, perf-optimization]

requires:
  - phase: none
    provides: none
provides:
  - CachedClock type with Arc<AtomicU64> for per-shard timestamp caching
  - Database::refresh_now_from_cache() reading cached atomics instead of syscalls
  - Shard event loop updates clock on 1ms periodic tick
affects: [profiling, benchmarks, expiry-checks]

tech-stack:
  added: []
  patterns: [per-shard cached clock with Relaxed atomics, cold-path fallback annotation]

key-files:
  created: []
  modified:
    - src/storage/entry.rs
    - src/storage/db.rs
    - src/shard/mod.rs
    - src/shard/coordinator.rs
    - src/server/connection.rs

key-decisions:
  - "Use two separate AtomicU64 (secs + ms) with Relaxed ordering instead of packed single atomic"
  - "Keep refresh_now() as #[cold] fallback for non-sharded legacy handler path"
  - "CachedClock is Clone (wraps Arc) -- each connection handler clones the shard's clock"
  - "Update clock only in periodic 1ms tick, not in SPSC notify arm (notify reads stale-by-1ms, acceptable)"

patterns-established:
  - "CachedClock pattern: per-shard Arc<AtomicU64> updated once per event-loop tick, read everywhere via Relaxed loads"

requirements-completed: [TIMESTAMP-CACHE-01]

duration: 19min
completed: 2026-03-26
---

# Phase 43 Plan 02: Timestamp Caching Summary

**Per-shard AtomicU64 cached clock eliminates clock_gettime syscalls from hot path (4% CPU / 66 samples)**

## Performance

- **Duration:** 19 min
- **Started:** 2026-03-26T16:26:39Z
- **Completed:** 2026-03-26T16:45:42Z
- **Tasks:** 2/2
- **Files modified:** 5

## Accomplishments

### Task 1: Add shared timestamp cache and wire into shard event loop
- **Commit:** 297ee2f
- Created `CachedClock` type in `src/storage/entry.rs` with `Arc<AtomicU64>` for seconds and milliseconds
- Added `Database::refresh_now_from_cache()` that reads from `CachedClock` via Relaxed atomic loads
- Marked legacy `refresh_now()` as `#[cold]` to hint compiler about slow path
- Created `CachedClock` in shard's `run()` before event loop
- Added `cached_clock.update()` at top of both Tokio and Monoio periodic 1ms tick arms
- Threaded `CachedClock` through `drain_spsc_shared`, `handle_shard_message_shared`, `handle_uring_event`
- Passed `CachedClock` to `handle_connection_sharded` and `handle_connection_sharded_monoio`
- Updated all coordinator functions (`coordinate_mget`, `coordinate_mset`, `coordinate_multi_del_or_exists`, `coordinate_keys`, `coordinate_scan`) to use cached timestamps
- Both `runtime-tokio` and `runtime-monoio` builds compile cleanly

### Task 2: Add tests and verify no regressions
- **Commit:** 1778f58
- Added `test_cached_clock_initial_values`: verifies cached secs/ms within 1s of real time
- Added `test_cached_clock_update`: verifies update advances ms after 5ms sleep
- Added `test_cached_clock_clone_shares_state`: verifies Arc sharing between clones
- Added `test_refresh_now_from_cache`: verifies Database reads match CachedClock values
- Fixed all test call sites for updated function signatures
- All 33 shard tests, 22 db tests, 21 entry tests pass

## Verification

- `cargo build --features runtime-tokio --no-default-features` -- clean (2 pre-existing warnings)
- `cargo build` (monoio) -- clean (2 pre-existing warnings)
- `cargo test --lib storage::entry` -- 21 passed
- `cargo test --lib storage::db` -- 22 passed
- `cargo test --lib shard` -- 33 passed
- `grep -c "CachedClock" src/shard/mod.rs` = 9 (creation + update + parameter passing)
- Hot path (`refresh_now_from_cache`) uses no `SystemTime::now()` syscalls

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed handle_uring_event missing cached_clock parameter**
- **Found during:** Task 1
- **Issue:** `handle_uring_event` is a separate static method that also calls `refresh_now()`, not covered in the plan
- **Fix:** Added `cached_clock: &CachedClock` parameter to `handle_uring_event` and updated call site + tests
- **Files modified:** src/shard/mod.rs
- **Commit:** 297ee2f

**2. [Rule 3 - Blocking] Used fully-qualified paths for std::sync types in CachedClock**
- **Found during:** Task 1
- **Issue:** Adding `use std::sync::{Arc, atomic::*}` imports triggered linter removal (unused import detection race)
- **Fix:** Used fully-qualified `std::sync::Arc` and `std::sync::atomic::AtomicU64` inline in struct definition
- **Files modified:** src/storage/entry.rs
- **Commit:** 297ee2f
