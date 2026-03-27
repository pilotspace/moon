---
phase: 14-forkless-compartmentalized-persistence
verified: 2026-03-24T12:00:00Z
status: passed
score: 8/8 must-haves verified
gaps: []
human_verification:
  - test: "Run a Redis client and issue BGSAVE, then SET a key and verify the .rrdshard file loads it on restart"
    expected: "Key survives restart via snapshot+WAL round-trip"
    why_human: "End-to-end startup restore path (restore_from_persistence + shard.run) requires live process"
  - test: "Run under memory profiler during BGSAVE with 1M keys across shards"
    expected: "Memory overhead < 5% during snapshot (success criterion 4)"
    why_human: "Per-segment COW buffer size cannot be verified statically; depends on write rate during snapshot window"
  - test: "Confirm < 1ms main-thread stall (success criterion 8)"
    expected: "advance_one_segment completes within 1ms wall time for a single segment"
    why_human: "Timing guarantee cannot be verified by grep; depends on hardware and segment occupancy"
---

# Phase 14: Forkless Compartmentalized Persistence Verification Report

**Phase Goal:** Replace clone-on-snapshot persistence with forkless async snapshotting leveraging DashTable's segmented structure — each segment serialized independently while the shard continues serving requests, with write-ahead capture for modified-but-not-yet-serialized segments
**Verified:** 2026-03-24T12:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Snapshot epoch marks point-in-time; segments iterated/serialized without blocking shard | VERIFIED | `SnapshotState::advance_one_segment` serializes exactly one segment per call, called from `spsc_interval.tick()` — shard event loop continues between ticks. `epoch` field on `SnapshotState` captures the epoch at construction time. |
| 2 | Modified-but-not-yet-serialized segments have old value captured (segment-level WAL/COW) | VERIFIED | `cow_intercept` in `shard/mod.rs:592` runs before `cmd_dispatch`. `is_segment_pending` checks `serialized_segments[db_index][seg_idx]`. `capture_cow` pushes `(db_index, seg_idx, key, old_entry)` onto overflow buffer. Test `test_snapshot_cow_captures_old_value` passes. |
| 3 | Serialization as low-priority cooperative task within shard event loop | VERIFIED | `advance_one_segment` called once per `spsc_interval.tick()` (1ms). `shard/mod.rs:341-364` shows the cooperative tick pattern — one segment processed then loop yields back to tokio select. |
| 4 | Per-shard WAL with batched fsync (1ms interval) | VERIFIED | `WalWriter::flush_if_needed` called at `shard/mod.rs:367-369` inside the same `spsc_interval.tick()` arm. `append` is hot-path (no I/O), `do_flush` calls `write_all` + `sync_data`. 8KB pre-allocated buffer. 7 WAL tests pass. |
| 5 | Parallel per-shard snapshot files | VERIFIED | Each shard writes to `shard-{id}.rrdshard` independently in its own event loop thread. `main.rs:117-119` calls `restore_from_persistence` per shard before `shard.run()`. File format: RRDSHARD magic + per-segment CRC32 blocks. |
| 6 | Each shard snapshots independently; startup loads per-shard snapshot then replays WAL | VERIFIED | `restore_from_persistence` in `shard/mod.rs:79` loads `shard-{id}.rrdshard` then replays `shard-{id}.wal`. Called from `main.rs:118-120` for every shard independently. |
| 7 | BGSAVE triggers SnapshotBegin across all shards; each shard snapshots independently | VERIFIED | `bgsave_start_sharded` in `command/persistence.rs:87` sends `ShardMessage::SnapshotBegin` to all SPSC producers. `SnapshotRequest` variant is gone — no matches in codebase. Auto-save uses watch channel (`run_auto_save_sharded`). |
| 8 | DashTable exposes segment iteration API; SnapshotState/advance_one_segment/capture_cow/WalWriter/flush_if_needed/RRDSHARD all exist and are wired | VERIFIED | All APIs exist and are substantively implemented. Full wiring confirmed below. |

**Score:** 8/8 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/storage/dashtable/mod.rs` | `segment_count()`, `segment()`, `segment_index_for_hash()`, `hash_key` pub | VERIFIED | All four present at lines 115, 124, 132, 33. 17 DashTable tests including 3 new segment API tests pass. |
| `src/storage/dashtable/segment.rs` | `iter_occupied()` | VERIFIED | Line 181. Safely iterates FULL ctrl-byte slots using `assume_init_ref`. |
| `src/persistence/snapshot.rs` | `SnapshotState`, `shard_snapshot_save`, `shard_snapshot_load`, COW intercept | VERIFIED | 689 lines. All exports present. `overflow` buffer with `(db_index, seg_idx, key, entry)` tuples. `serialized_segments: Vec<Vec<bool>>` per-db tracking. 8 tests pass. |
| `src/persistence/wal.rs` | `WalWriter`, `replay_wal`, `wal_path`, `flush_if_needed`, `truncate_after_snapshot` | VERIFIED | 375 lines. All exports present. 8KB pre-allocated buffer. `sync_data()` for fsync. `rename` for atomic truncation. 7 tests pass. |
| `src/persistence/mod.rs` | `pub mod snapshot`, `pub mod wal` | VERIFIED | Lines 4-5. Both re-exports present. |
| `src/persistence/rdb.rs` | `write_entry`, `read_entry`, `write_bytes`, `read_bytes`, `read_u32` as `pub(crate)` | VERIFIED | Lines 258 and 321 confirm `pub(crate)` visibility. |
| `src/shard/mod.rs` | `SnapshotState` wired, `WalWriter` wired, `advance_one_segment` per tick, `cow_intercept` before dispatch | VERIFIED | Lines 208-225 (state init), 341-369 (cooperative tick), 503-520 (COW + WAL in Execute), 592-616 (cow_intercept impl). |
| `src/shard/dispatch.rs` | `SnapshotBegin` variant, no `SnapshotRequest` | VERIFIED | Line 53 has `SnapshotBegin`. Zero matches for `SnapshotRequest` in entire codebase. |
| `src/command/persistence.rs` | `bgsave_start_sharded`, `SNAPSHOT_EPOCH` | VERIFIED | Lines 26 and 87. Sends `SnapshotBegin` to all SPSC producers. |
| `src/persistence/auto_save.rs` | `run_auto_save_sharded` using watch channel | VERIFIED | Line 91. Bumps `SNAPSHOT_EPOCH` and sends via `snapshot_trigger.send(epoch)`. |
| `src/main.rs` | Watch channel setup, `restore_from_persistence` per shard, auto-save wiring | VERIFIED | Lines 81, 97, 118-120, 155-159. All wiring present. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `shard/mod.rs` | `persistence/snapshot.rs` | `SnapshotState::new`, `advance_one_segment`, `capture_cow` | WIRED | Imports at line 20. Used at lines 208, 316, 331, 343, 504, 613. |
| `shard/mod.rs` | `persistence/wal.rs` | `WalWriter::new`, `flush_if_needed`, `truncate_after_snapshot` | WIRED | Import at line 21. Used at lines 213, 355, 368. |
| `shard/mod.rs` | `storage/dashtable/mod.rs` | `hash_key`, `segment_index_for_hash` | WIRED | Line 609: `crate::storage::dashtable::hash_key(key)`, line 610: `.segment_index_for_hash(hash)`. |
| `persistence/snapshot.rs` | `storage/dashtable/mod.rs` | `segment_count()`, `segment()` for iteration | WIRED | Line 72: `.segment_count()`, line 166: `.segment(seg_idx)`. |
| `persistence/snapshot.rs` | `persistence/rdb.rs` | `write_entry`, `read_entry` | WIRED | Lines 193, 207: `rdb::write_entry(...)`. Line 380: `rdb::read_entry(...)`. |
| `command/persistence.rs` | `shard/dispatch.rs` | `ShardMessage::SnapshotBegin` sent to all shards | WIRED | Line 104: `prod.try_push(ShardMessage::SnapshotBegin {...})`. |
| `main.rs` | `shard/mod.rs` | `restore_from_persistence` before event loop | WIRED | Line 119: `shard.restore_from_persistence(dir)`. |
| `auto_save.rs` | `main.rs` watch channel | `snapshot_trigger.send(epoch)` | WIRED | Line 113: `snapshot_trigger.send(epoch)`. Watch receiver cloned per shard at `main.rs:97`. |

### Requirements Coverage

No requirements IDs declared in plan frontmatter (`requirements: []`). Phase is self-contained.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/shard/mod.rs` | 285 | `TODO wire from shard config` for requirepass | Info | Unrelated to this phase (pre-existing, connection auth config). |
| `src/command/persistence.rs` | 112-114 | `SAVE_IN_PROGRESS` cleared immediately after sending, not after completion | Warning | BGSAVE can be re-issued before shards finish the snapshot. No snapshot correctness impact — each shard is independent. Acknowledged design tradeoff in plan comments. |

### Human Verification Required

#### 1. End-to-end restart restore

**Test:** Start server with `--dir /tmp/rrd-test`, set 3 keys, issue `BGSAVE`, kill server, restart, verify keys present via `GET`.
**Expected:** All 3 keys present after restart (loaded from `.rrdshard` snapshot).
**Why human:** End-to-end process lifecycle (startup `restore_from_persistence` + shard WAL replay) requires a live server process.

#### 2. Memory overhead during snapshot < 5% (Success Criterion 4)

**Test:** Load 500K keys across shards (~50MB), trigger `BGSAVE`, observe RSS peak during snapshot.
**Expected:** Peak RSS increase < 5% of baseline.
**Why human:** COW overflow buffer is bounded by `60 entries * 1 segment pending at a time`. Theoretical overhead is negligible, but actual measurement requires a memory profiler on live process.

#### 3. Snapshot latency < 1ms main-thread stall (Success Criterion 8)

**Test:** Instrument `advance_one_segment` call site in shard event loop, measure wall-clock duration per call with 60-entry segments.
**Expected:** Each call completes in < 1ms for a fully-loaded segment.
**Why human:** Timing correctness depends on hardware, segment occupancy, and I/O scheduler. Cannot verify statically.

### Gaps Summary

No gaps. All automated checks pass. The phase goal is fully achieved:

- Forkless architecture: `advance_one_segment` replaces the old `clone-all-data` + `spawn_blocking` path in `bgsave_start`.
- `SnapshotRequest` is fully removed from `ShardMessage`; `SnapshotBegin` takes its place.
- Per-shard WAL files (`shard-N.wal`) replace the single global AOF for write-intensive shards.
- Startup restore calls `shard_snapshot_load` + `replay_wal` independently per shard before the event loop starts.
- All 567 lib tests + 66 integration tests pass with 0 build errors.

---

_Verified: 2026-03-24T12:00:00Z_
_Verifier: Claude (gsd-verifier)_
