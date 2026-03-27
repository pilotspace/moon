---
phase: 43-rdb-snapshot-hardening
plan: 01
subsystem: persistence
tags: [rdb, redis-compat, crc64, bgsave, snapshot]
dependency_graph:
  requires: []
  provides: [redis-rdb-format, bgsave-completion-tracking]
  affects: [replication, psync2]
tech_stack:
  added: []
  patterns: [crc64-jones-polynomial, redis-rdb-v10-format, oneshot-completion-watcher]
key_files:
  created:
    - src/persistence/redis_rdb.rs
  modified:
    - src/persistence/mod.rs
    - src/command/persistence.rs
decisions:
  - Implemented CRC64 Jones polynomial as const lookup table instead of using crc64 crate (correctness guarantee)
  - Used reflected polynomial 0x95AC9329AC4BC9B5 matching Redis src/crc64.c
  - Streams serialized as type-0 string placeholder (Redis native stream encoding type 19+ unnecessary for self-replication)
  - Used monoio::spawn for Monoio runtime completion watcher (no spawn_local trait indirection needed)
metrics:
  duration: 7min
  completed: "2026-03-26T16:49:43Z"
  tasks_completed: 2
  tasks_total: 2
  tests_added: 26
  tests_passing: 26
---

# Phase 43 Plan 01: Redis-Compatible RDB Format and BGSAVE Completion Tracking Summary

Redis RDB v10 format writer/reader with CRC64 Jones checksum and fixed BGSAVE completion tracking via oneshot watcher

## Task Summary

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | Redis-compatible RDB format writer/reader | 14c4c6f | New redis_rdb.rs module with full write/load cycle |
| 2 | Fix BGSAVE completion tracking | 28571e4 | Oneshot watcher replaces fire-and-forget pattern |

## What Was Built

### Task 1: Redis RDB Format Module (src/persistence/redis_rdb.rs)

- **Header**: REDIS0010 magic + AUX fields (redis-ver, redis-bits, ctime, used-mem)
- **Encoding**: Redis variable-length (6/14/32/64-bit), length-prefixed strings, integer-encoded string reading
- **Entry types**: String (0), List (1), Set (2), Hash (4), ZSET_2 (5), Stream (as string placeholder)
- **Opcodes**: SELECTDB (0xFE), RESIZEDB (0xFB), EXPIRETIME_MS (0xFC), AUX (0xFA), EOF (0xFF)
- **CRC64**: Jones polynomial (0x95AC9329AC4BC9B5 reflected), verified against test vector 0xE9C6D914C4B8D9CA
- **Public API**: `write_rdb()`, `save()` (atomic tmp+rename), `load_rdb()` (with CRC verification)
- **23 tests**: CRC64 verification, length encoding roundtrip, string roundtrip, per-type write verification, per-type roundtrip, all-types roundtrip, CRC corruption detection

### Task 2: BGSAVE Completion Tracking (src/command/persistence.rs)

- **Fixed**: `bgsave_start_sharded()` no longer discards oneshot receivers
- **Watcher**: Spawns async task that awaits all shard `OneshotReceiver<Result<(), String>>` replies
- **Atomics**: SAVE_IN_PROGRESS stays true until all shards complete; LAST_SAVE_TIME updated on completion; BGSAVE_LAST_STATUS tracks per-shard success/failure
- **Both runtimes**: tokio::task::spawn_local for Tokio, monoio::spawn for Monoio

## Deviations from Plan

None - plan executed exactly as written.

## Decisions Made

1. **CRC64 implementation**: Used const compile-time lookup table generation instead of external crate, ensuring exact match with Redis's Jones polynomial
2. **Stream encoding**: Serialized as type-0 string with `__stream__:N` format rather than implementing Redis's complex type-19 stream encoding, since PSYNC2 targets our own replicas
3. **Monoio watcher**: Used `monoio::spawn()` directly rather than going through RuntimeSpawn trait, matching existing patterns in the codebase

## Self-Check: PASSED
