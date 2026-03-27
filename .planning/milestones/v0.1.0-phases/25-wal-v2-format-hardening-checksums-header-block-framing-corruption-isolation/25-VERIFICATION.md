---
phase: 25-wal-v2-format-hardening
verified: 2026-03-25T09:00:00Z
status: passed
score: 10/10 must-haves verified
gaps: []
---

# Phase 25: WAL v2 Format Hardening Verification Report

**Phase Goal:** Harden the per-shard WAL format with a versioned header (RRDWAL magic + version + shard_id + epoch), block-level CRC32 checksums, and corruption isolation. Current WAL is raw concatenated RESP with no integrity checks.
**Verified:** 2026-03-25T09:00:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | New WAL files start with a 32-byte header containing magic bytes RRDWAL | VERIFIED | `write_header()` at wal.rs:111-131 writes 32-byte header; `new()` calls it when `write_offset == 0`; test `test_wal_v2_header_format` validates all fields |
| 2 | Every flush produces a framed block with length prefix, cmd_count, db_idx, payload, and CRC32 | VERIFIED | `do_write()` at wal.rs:180-231 writes [block_len:4][cmd_count:2][db_idx:1][payload][crc32:4]; test `test_wal_v2_block_crc_valid` parses and verifies |
| 3 | CRC32 covers cmd_count(2B) + db_idx(1B) + RESP payload bytes | VERIFIED | Hasher at wal.rs:200-204 feeds cmd_count_bytes, db_idx, buf; verified by `test_wal_v2_block_crc_valid` recomputing and asserting match |
| 4 | Existing append() hot path remains allocation-free (extend_from_slice only) | VERIFIED | `append()` at wal.rs:138-141 is only `extend_from_slice` + `saturating_add` -- no allocations |
| 5 | truncate_after_snapshot writes the v2 header on the fresh WAL file | VERIFIED | `truncate_after_snapshot()` at wal.rs:251-285 sets `header_written = false`, calls `write_header()`; test `test_wal_truncate_after_snapshot` verifies new file has RRDWAL header |
| 6 | WAL replay auto-detects v1 (raw RESP) vs v2 (RRDWAL header) format | VERIFIED | `replay_wal()` at wal.rs:315-327 checks first 6 bytes against WAL_MAGIC; test `test_wal_v1_backward_compat` confirms v1 fallback |
| 7 | V2 replay verifies CRC32 on every block before dispatching commands | VERIFIED | `replay_wal_v2()` at wal.rs:388-403 computes CRC and compares with stored, breaks on mismatch |
| 8 | Replay stops on first corrupted block with warning and returns commands replayed so far | VERIFIED | CRC mismatch at wal.rs:393-402 logs warn and breaks; test `test_wal_v2_corruption_stops_replay` flips CRC in block 2, asserts only 3 commands from block 1 |
| 9 | V1 WAL files replay correctly via existing replay_aof path (backward compatibility) | VERIFIED | `replay_wal()` delegates to `replay_aof()` at wal.rs:325; test `test_wal_v1_backward_compat` writes raw RESP, replays, asserts keys present |
| 10 | Full round-trip works: write v2 -> replay v2 -> data matches | VERIFIED | Tests `test_wal_replay_round_trip` and `test_wal_replay_with_collections` exercise full write-replay cycle with SETs, HSET, LPUSH, SADD, ZADD |

**Score:** 10/10 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/persistence/wal.rs` | WAL v2 writer with header and block framing | VERIFIED | 882 lines, contains RRDWAL constants, write_header, CRC32 block framing in do_write, replay_wal_v2 with CRC verification |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `WalWriter::new` | `write_header` | called after file open | WIRED | wal.rs:102 calls `writer.write_header()` when `write_offset == 0` |
| `WalWriter::do_write` | `crc32fast` | CRC32 computation per block | WIRED | wal.rs:200 `crc32fast::Hasher::new()`, feeds cmd_count + db_idx + payload |
| `replay_wal` | `replay_wal_v2` | magic byte detection | WIRED | wal.rs:322 dispatches to `replay_wal_v2` when first 6 bytes match WAL_MAGIC |
| `replay_wal_v2` | `crc32fast` | CRC verification per block | WIRED | wal.rs:388-391 computes CRC with crc32fast::Hasher |
| `replay_wal_v2` | RESP parse | delegates payload parsing | WIRED | wal.rs:409 calls `crate::protocol::parse::parse` on payload BytesMut |
| `replay_wal` | shard startup | called from shard/mod.rs | WIRED | shard/mod.rs:111 calls `wal::replay_wal()` |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| WAL-V2-WRITE | 25-01 | WAL v2 writer with header + CRC32 block framing | SATISFIED | Header writing, block framing, CRC32 in do_write; 7 writer tests pass |
| WAL-V2-READ | 25-02 | WAL v2 replay with CRC verification | SATISFIED | replay_wal_v2 with per-block CRC check; corruption stops replay; 5 replay tests pass |
| WAL-V2-COMPAT | 25-02 | V1 backward compatibility | SATISFIED | replay_wal auto-detects v1 via magic bytes, falls back to replay_aof; test_wal_v1_backward_compat passes |

Note: WAL-V2-WRITE, WAL-V2-READ, WAL-V2-COMPAT are defined in ROADMAP.md but have no formal entries in REQUIREMENTS.md. This is acceptable as they are phase-scoped requirements fully covered by the implementation.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | - | - | - | - |

No TODOs, FIXMEs, placeholders, or stub implementations found. No ignored tests remain.

### Human Verification Required

None required. All behaviors are programmatically verified through unit tests covering header format, CRC validity, corruption detection, truncation, round-trip replay, and v1 backward compatibility.

### Gaps Summary

No gaps found. All 10 observable truths verified. All artifacts exist, are substantive, and are wired. All 13 tests pass with 0 ignored. Both commits (999b3b1 for Plan 01, e3e6634 for Plan 02) exist in the repository.

---

_Verified: 2026-03-25T09:00:00Z_
_Verifier: Claude (gsd-verifier)_
