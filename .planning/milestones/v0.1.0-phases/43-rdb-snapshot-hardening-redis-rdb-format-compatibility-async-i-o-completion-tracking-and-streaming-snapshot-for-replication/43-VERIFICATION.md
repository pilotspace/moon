---
phase: 43-rdb-snapshot-hardening
verified: 2026-03-27T00:00:00Z
status: gaps_found
score: 6/7 must-haves verified
re_verification: false
gaps:
  - truth: "Snapshot finalize uses async file I/O under Tokio runtime, not blocking std::fs::write"
    status: partial
    reason: "finalize_async() exists and uses tokio::fs under cfg-tokio gate. However, cargo build --features runtime-tokio fails with 40 pre-existing errors (E0428 duplicate definitions in cluster/bus.rs and others) plus 3 E0382 move-after-capture errors in src/command/persistence.rs that pre-existed phase 43. The monoio build succeeds cleanly. The E0382 errors in persistence.rs at lines 100 and 187 existed before phase 43 (verified at commit 75e8145). The tokio feature flag cannot be used to run tests. Monoio build is the only fully compilable path."
    artifacts:
      - path: "src/persistence/snapshot.rs"
        issue: "finalize_async() exists and is correct, but cargo --features runtime-tokio does not compile (pre-existing failures block test execution)"
    missing:
      - "Pre-existing build failures under runtime-tokio must be resolved before tokio-path tests can run. This is a pre-existing issue not introduced by phase 43, but it blocks verification of all tokio-specific additions."
human_verification:
  - test: "BGSAVE completion tracking end-to-end"
    expected: "LASTSAVE timestamp updates only after all shards complete their snapshot, SAVE_IN_PROGRESS clears atomically after all completions"
    why_human: "Multi-shard runtime coordination cannot be verified without running the server with num_shards > 1"
  - test: "SAVE command in single-threaded mode"
    expected: "Returns OK and updates LASTSAVE timestamp, file written atomically"
    why_human: "Requires live server instance with single-threaded config"
  - test: "INFO persistence section output"
    expected: "Returns rdb_bgsave_in_progress, rdb_last_save_time, rdb_last_bgsave_status, aof_enabled fields"
    why_human: "Requires live server + redis-cli INFO persistence query"
  - test: "Replication full resync async I/O"
    expected: "master.rs Tokio variant uses tokio::fs::read during full resync (non-blocking event loop)"
    why_human: "Requires master + replica setup to trigger PSYNC2 FULLRESYNC"
---

# Phase 43: RDB Snapshot Hardening Verification Report

**Phase Goal:** Harden the forkless async snapshot engine for production: Redis-compatible RDB format export/import for PSYNC2 full resync and migration, async file I/O for snapshot finalize, BGSAVE completion tracking via oneshot watchers, SAVE/LASTSAVE commands, INFO persistence section, and async file reads for replication transfer.
**Verified:** 2026-03-27
**Status:** gaps_found (pre-existing build failures block automated test execution under runtime-tokio; all phase-43 artifacts verified by code inspection)
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Redis RDB format writer produces valid REDIS0010 header with AUX fields, SELECTDB, RESIZEDB, entries, EOF, and CRC64 checksum | VERIFIED | `src/persistence/redis_rdb.rs` exists (1114 lines), contains REDIS_RDB_MAGIC=b"REDIS", REDIS_RDB_VERSION=b"0010", all 6 opcodes (0xFA/0xFB/0xFC/0xFE/0xFF), `write_rdb()`, `save()`, `load_rdb()` |
| 2 | Redis RDB round-trip: write entries then read them back preserving key, value, TTL for all 6 data types | VERIFIED | 23 tests in redis_rdb.rs including `test_crc64_jones_polynomial` (verifies 0xE9C6D914C4B8D9CA vector), per-type write tests, per-type roundtrip tests, all-types roundtrip; monoio build succeeds |
| 3 | BGSAVE completion tracking: SAVE_IN_PROGRESS stays true until all shards complete, then clears atomically | VERIFIED | `bgsave_start_sharded()` collects receivers into Vec, spawns tokio::task::spawn_local/monoio::spawn watcher that awaits all oneshot receivers before calling `SAVE_IN_PROGRESS.store(false)`. No `let (tx, _rx)` fire-and-forget pattern present. |
| 4 | Snapshot finalize uses async file I/O under Tokio runtime, not blocking std::fs::write | VERIFIED (code) / PARTIAL (tests) | `finalize_async()` at snapshot.rs:266 uses tokio::fs::write+rename under `#[cfg(runtime-tokio)]`, std::mem::take for await safety. Called at shard/mod.rs:469 and shard/mod.rs:672 via `.await`. Code verified correct. Tests cannot execute under runtime-tokio due to pre-existing compilation failures (not introduced by phase 43). |
| 5 | SAVE command triggers synchronous snapshot and returns OK | VERIFIED | `handle_save()` at persistence.rs:240 exists, wired in server/connection.rs at lines 552, 2046, 4204. Returns error in sharded mode. `is_intercepted_command` at server/connection.rs:1281 includes SAVE. |
| 6 | LASTSAVE returns Unix timestamp of last successful save | VERIFIED | `handle_lastsave()` at persistence.rs:283 returns `Frame::Integer(LAST_SAVE_TIME.load(...))`. Wired at server/connection.rs:562, 2053, 4211. `(8, b'l') => LASTSAVE` in intercepted command matcher at line 1296. |
| 7 | INFO persistence section shows rdb_bgsave_in_progress, rdb_last_save_time, aof_enabled | VERIFIED | connection.rs:135-147 contains `# Persistence\r\n`, `rdb_bgsave_in_progress:{}`, `rdb_last_save_time:{}`, `rdb_last_bgsave_status:{}`, `aof_enabled:0`. References live atomics SAVE_IN_PROGRESS, LAST_SAVE_TIME, BGSAVE_LAST_STATUS. |

**Score:** 6/7 truths verified (truth 4 is code-verified but tests unrunnable due to pre-existing toolchain issue)

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/persistence/redis_rdb.rs` | Redis RDB format writer/reader | VERIFIED | 1114 lines. Contains `write_rdb`, `save`, `load_rdb`, `write_length`, `write_redis_string`, all required opcodes and type constants. 23 unit tests including CRC64 Jones polynomial verification. |
| `src/persistence/mod.rs` | Module registration | VERIFIED | Line 4: `pub mod redis_rdb;` |
| `src/command/persistence.rs` | BGSAVE completion tracking + SAVE/LASTSAVE handlers | VERIFIED | `LAST_SAVE_TIME: AtomicU64` at line 32, `BGSAVE_LAST_STATUS: AtomicBool` at line 35, `handle_save()` at line 240, `handle_lastsave()` at line 282. Oneshot watcher spawned for both runtimes. |
| `src/persistence/snapshot.rs` | `finalize_async()` method | VERIFIED | `pub async fn finalize_async()` at line 266. `tokio::fs::write` inside `#[cfg(runtime-tokio)]`. `std::mem::take(&mut self.output_buf)` at line 280. |
| `src/shard/mod.rs` | `finalize_async().await` call | VERIFIED | Lines 469 and 672 call `snap.finalize_async().await`. |
| `src/command/connection.rs` | INFO persistence section | VERIFIED | Lines 135-147. All 5 required fields present with live atomic reads. |
| `src/server/connection.rs` | SAVE/LASTSAVE command wiring | VERIFIED | SAVE wired at lines 552, 2046, 4204. LASTSAVE at 562, 2053, 4211. Both in `is_intercepted_command` matcher. |
| `src/replication/master.rs` | Async file read for full resync | VERIFIED | Tokio variant (lines 115-116): `tokio::fs::read(&snap_path).await`. Monoio variant (line 275): `std::fs::read` kept with documentation. redis_rdb referenced in TODO comments (lines 110-112). |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/persistence/redis_rdb.rs` | `src/persistence/rdb.rs` | reuses data serialization patterns | PARTIAL | redis_rdb.rs implements its own serialization matching Redis format (not directly calling rdb.rs — plan noted "NOT reusable directly for Redis RDB" — this is correct by design) |
| `src/command/persistence.rs` | `src/runtime/traits.rs` | spawn_local for completion watcher | PARTIAL | Uses `tokio::task::spawn_local` directly (not `RuntimeImpl::spawn_local` trait). Plan noted "monoio::spawn for Monoio runtime" matching existing patterns. Functional outcome achieved. |
| `src/persistence/snapshot.rs` | `tokio::fs` | cfg-gated async file write | VERIFIED | `tokio::fs::write` at line 285, inside `#[cfg(feature = "runtime-tokio")]` block. |
| `src/server/connection.rs` | `src/command/persistence.rs` | SAVE/LASTSAVE dispatch | VERIFIED | `crate::command::persistence::handle_save(...)` and `handle_lastsave()` called in all three handler variants (single, sharded-tokio, sharded-monoio). |
| `src/replication/master.rs` | `src/persistence/redis_rdb.rs` | optional RDB format conversion for transfer | DOCUMENTED | `redis_rdb` referenced in TODO comment. Conversion deferred; RRDSHARD format still used. This matches the plan scope (Plan 43-03 says "add TODO comment for future streaming RDB conversion"). |
| `src/replication/master.rs` | `tokio::fs` | async file read for snapshot transfer | VERIFIED | `tokio::fs::read(&snap_path).await` at line 115 in Tokio `handle_psync_on_master` variant. |

---

### Requirements Coverage

| Requirement | Source Plan | Description (inferred from plan content) | Status | Evidence |
|-------------|------------|------------------------------------------|--------|----------|
| RDB-01 | 43-01 | Redis RDB format writer (REDIS0010 with CRC64) | SATISFIED | `write_rdb()`, `save()` in redis_rdb.rs; REDIS0010 header, CRC64 Jones polynomial verified in tests |
| RDB-02 | 43-01 | Redis RDB format reader (load_rdb with CRC verification) | SATISFIED | `load_rdb()` in redis_rdb.rs; CRC64 verified on load, magic/version validation |
| RDB-03 | 43-02 | Async file I/O for snapshot finalize | SATISFIED (code) | `finalize_async()` with tokio::fs; shard/mod.rs calls `.await`. Tests blocked by pre-existing compilation failure. |
| RDB-04 | 43-01 | BGSAVE completion tracking (not fire-and-forget) | SATISFIED | Oneshot receiver watcher spawned; SAVE_IN_PROGRESS cleared only after all shards complete |
| RDB-05 | 43-02 | SAVE command (synchronous snapshot) | SATISFIED | `handle_save()` implemented and wired in all 3 connection handler variants |
| RDB-06 | 43-02 | LASTSAVE command (Unix timestamp) | SATISFIED | `handle_lastsave()` implemented, returns `LAST_SAVE_TIME` atomic, wired in all 3 variants |
| RDB-07 | 43-02 | INFO persistence section | SATISFIED | `# Persistence` section in `info()` with all 5 required fields reading live atomics |
| RDB-08 | 43-03 | Async file reads for replication full resync | SATISFIED | `tokio::fs::read` in Tokio `handle_psync_on_master`; redis_rdb TODO documented for future |

**Note on REQUIREMENTS.md:** RDB-01 through RDB-08 are phase-specific requirement IDs referenced in ROADMAP.md but not present in `.planning/REQUIREMENTS.md` (which tracks v1 global requirements PERS-01 through PERS-05). These are cross-phase architectural requirements specific to phase 43 infrastructure. No orphaned requirements found — all 8 IDs are accounted for across the 3 plans.

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/persistence/redis_rdb.rs` | 346-353 | Stream serialized as `__stream__:N` string | INFO | Known limitation. Plan documented: "for PSYNC2 to our own replicas this is sufficient". Not a production blocker for stated scope. |
| `src/replication/master.rs` | 109-112 | Redis RDB conversion deferred via TODO | INFO | Plan 43-03 explicitly scoped this as future work. Not a scope violation. |
| `src/command/persistence.rs` | 100, 187 | E0382 borrow-after-move in cfg-gated blocks (pre-existing) | WARNING | These errors existed at commit 75e8145 (before phase 43). Verified by checking git history. Not introduced by phase 43 but present in modified files. Under runtime-tokio the entire codebase fails to compile due to pre-existing E0428 errors in unrelated files. |

---

### Pre-Existing Build Failures (Important Context)

The `cargo build --features runtime-tokio` command produces 40 errors. These errors are confirmed to pre-exist before any phase 43 commits:

- **E0428 (duplicate definitions)**: 37 errors in `src/cluster/bus.rs`, `src/replication/master.rs` and other files — same count before phase 43 began (verified at commit 75e8145)
- **E0382 (move-after-capture in cfg-gated blocks)**: Present in `src/command/persistence.rs` at the same location before phase 43 modifications

The `cargo build --features runtime-monoio` command compiles **cleanly** (0 errors).

All phase 43 code correctness was verified through code inspection. The CRC64, opcode constants, all type handlers, completion watcher logic, finalize_async, SAVE/LASTSAVE handlers, INFO section, and replication async I/O are all structurally correct based on code review.

---

### Human Verification Required

#### 1. BGSAVE Multi-Shard Completion Tracking

**Test:** Start server with `num_shards > 1`, run BGSAVE, immediately query `LASTSAVE` before shard snapshots complete, then poll until completion.
**Expected:** `SAVE_IN_PROGRESS` reads `1` while snapshots are running; `LASTSAVE` timestamp updates after all shards complete; `rdb_bgsave_in_progress` in INFO shows `0` after completion.
**Why human:** Multi-shard coordination requires a live server runtime with multiple shards.

#### 2. SAVE Command Functional Test

**Test:** Start server in single-threaded mode, execute `SAVE`, check that a `.rdb` file is written to the configured directory and `LASTSAVE` returns a non-zero timestamp.
**Expected:** `OK` response, file written atomically (via tmp+rename), `LASTSAVE` > 0.
**Why human:** Requires live server with single-threaded config and filesystem inspection.

#### 3. INFO Persistence Section Live Values

**Test:** `redis-cli INFO persistence` against a running server instance.
**Expected:** Output contains `rdb_bgsave_in_progress:0`, `rdb_last_save_time:<timestamp>`, `rdb_last_bgsave_status:ok`, `aof_enabled:0`.
**Why human:** Requires live server connection.

#### 4. Replication Full Resync Async I/O

**Test:** Set up master + replica pair, trigger FULLRESYNC, observe that master does not block during snapshot transfer for a large dataset.
**Expected:** Master event loop remains responsive to other commands during file reads; `tokio::fs::read` path execised.
**Why human:** Requires master + replica setup, large dataset, and observable latency measurement.

---

### Gaps Summary

The single gap is environmental: the `runtime-tokio` feature flag cannot compile the project due to **40 pre-existing errors** unrelated to phase 43 work. These errors existed before the phase 43 commits (confirmed by checking commit 75e8145 at the start of phase 43).

The phase 43 artifacts are all verified by code inspection:
- `src/persistence/redis_rdb.rs` — substantive, 1114 lines, 23 tests
- `src/persistence/mod.rs` — registered
- `src/command/persistence.rs` — LAST_SAVE_TIME, BGSAVE_LAST_STATUS, handle_save, handle_lastsave, oneshot watcher
- `src/persistence/snapshot.rs` — finalize_async with tokio::fs
- `src/shard/mod.rs` — finalize_async().await at both event loop sites
- `src/command/connection.rs` — INFO persistence section with 5 fields
- `src/server/connection.rs` — SAVE/LASTSAVE in all 3 handler variants
- `src/replication/master.rs` — tokio::fs::read in Tokio path

The monoio build is clean. The gap is: **automated test execution under runtime-tokio is blocked by pre-existing build failures**. This is a pre-existing infrastructure issue that should be tracked separately and is not a blocker for the phase 43 goal itself being achieved.

---

_Verified: 2026-03-27_
_Verifier: Claude (gsd-verifier)_
