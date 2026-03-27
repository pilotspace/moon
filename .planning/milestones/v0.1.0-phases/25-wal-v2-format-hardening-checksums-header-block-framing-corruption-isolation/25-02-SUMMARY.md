---
phase: 25-wal-v2-format-hardening
plan: 02
title: "WAL v2 Replay with CRC Verification"
one_liner: "V2-aware WAL replay with per-block CRC32 verification, corruption isolation, and v1 backward compatibility"
subsystem: persistence
tags: [wal, replay, crc32, corruption-detection, backward-compat]
dependency_graph:
  requires: [25-01]
  provides: [wal-v2-replay, wal-v1-compat]
  affects: [persistence, crash-recovery]
tech_stack:
  added: []
  patterns: [magic-byte-detection, crc-verified-replay, corruption-isolation]
key_files:
  created: []
  modified:
    - src/persistence/wal.rs
decisions:
  - "CRC verification covers cmd_count(2B) + db_idx(1B) + payload, matching writer"
  - "Stop on first corrupted block (no skip) -- WAL ordering matters for correctness"
  - "V1 detection: if first 6 bytes != RRDWAL magic, delegate to replay_aof"
metrics:
  duration: "2min"
  completed: "2026-03-25"
  tasks_completed: 1
  tasks_total: 1
  tests_added: 5
  tests_total: 13
---

# Phase 25 Plan 02: WAL v2 Replay with CRC Verification Summary

V2-aware WAL replay with per-block CRC32 verification, corruption isolation (stop on first bad block), and v1 backward compatibility via magic byte auto-detection.

## What Was Done

### Task 1: Implement v2 replay with CRC verification and v1 auto-detection

**Commit:** e3e6634

Modified `replay_wal()` to auto-detect v1 vs v2 format by checking the first 6 bytes against the RRDWAL magic signature. V2 files are replayed via the new `replay_wal_v2()` function which:

1. Parses the 32-byte header (magic, version, shard_id, epoch)
2. Iterates over CRC32-checksummed block frames
3. Verifies CRC32 on each block before dispatching commands
4. Stops on first corrupted or truncated block, returning commands replayed so far
5. Parses RESP payloads within each block and dispatches via command::dispatch

V1 files (no RRDWAL header) fall back to the existing `replay_aof()` path.

**Tests added (5 new, 2 un-ignored):**
- `test_wal_replay_round_trip` -- v2 write + replay round-trip (un-ignored)
- `test_wal_replay_with_collections` -- HSET/LPUSH/SADD/ZADD round-trip (un-ignored)
- `test_wal_v1_backward_compat` -- raw RESP v1 file replays correctly
- `test_wal_v2_corruption_stops_replay` -- bad CRC in block 2 stops after block 1's 3 commands
- `test_wal_v2_truncated_block_stops` -- partial block at EOF stops gracefully
- `test_wal_v2_empty_after_header` -- header-only file returns Ok(0)

## Deviations from Plan

None -- plan executed exactly as written.

## Verification

- `cargo test --lib persistence::wal::tests`: 13 passed, 0 failed, 0 ignored
- `cargo test --lib`: 987 passed, 0 failed, 0 ignored
- All acceptance criteria met (grep counts verified)

## Files Modified

| File | Change |
|------|--------|
| src/persistence/wal.rs | Added replay_wal_v2(), v1/v2 auto-detection, 5 new tests, removed 2 #[ignore] |
