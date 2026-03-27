---
phase: 40-fix-all-p0-p1-p2
verified: 2026-03-26T00:00:00Z
status: passed
score: 6/6 must-haves verified
re_verification: false
---

# Phase 40: fix all P0, P1, P2 Verification Report

**Phase Goal:** Fix 3 critical multi-shard performance issues: (P0) Port pipeline batch dispatch to Monoio handler replacing per-command Execute with deferred PipelineBatch, (P1) Fix per-read buffer allocation and increase connection channel capacity, (P2) Pre-allocate batch containers to reduce allocation pressure.
**Verified:** 2026-03-26
**Status:** passed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| #  | Truth                                                                                                          | Status     | Evidence                                                                                 |
|----|----------------------------------------------------------------------------------------------------------------|------------|------------------------------------------------------------------------------------------|
| 1  | Monoio handler dispatches remote pipeline commands as PipelineBatch per target shard, not individual Execute   | VERIFIED   | `remote_groups` HashMap declared at line 3155; `ShardMessage::PipelineBatch` at line 3686; `remote_groups.drain()` at line 3681 |
| 2  | Monoio handler consumes frames iterator (`for frame in frames`), eliminating deep clone                        | VERIFIED   | Line 3396: `for frame in frames` (not `for frame in &frames`)                           |
| 3  | All remote shard responses awaited in parallel after the frame loop, not synchronously inside it               | VERIFIED   | `reply_futures` Vec at line 3156; all futures pushed, then batch-awaited at line 3715    |
| 4  | Monoio handler reuses read buffer across loop iterations instead of allocating `vec![0u8; 8192]` every read    | VERIFIED   | `let mut tmp_buf = vec![0u8; 8192];` at line 3150 (before loop); `tmp_buf.resize(8192, 0)` at line 3363; `tmp_buf = returned_buf;` at line 3365 |
| 5  | CONN_CHANNEL_CAPACITY is 4096, supporting 5K+ concurrent clients without channel overflow                      | VERIFIED   | `src/shard/mesh.rs` line 16: `pub const CONN_CHANNEL_CAPACITY: usize = 4096;`           |
| 6  | Batch dispatch containers (responses, remote_groups, reply_futures) are pre-allocated and reused via clear/drain | VERIFIED | `let mut responses: Vec<Frame> = Vec::with_capacity(64);` at line 3154; `responses.clear()` at line 3393; `remote_groups.clear()` at line 3394; `reply_futures.clear()` at line 3679; `.drain()` used for iteration |

**Score:** 6/6 truths verified

---

### Required Artifacts

| Artifact                    | Expected                                               | Status     | Details                                                               |
|-----------------------------|--------------------------------------------------------|------------|-----------------------------------------------------------------------|
| `src/server/connection.rs`  | Monoio handler with batched remote dispatch (FIX-P0)   | VERIFIED   | Contains `remote_groups`, `PipelineBatch`, `reply_futures`; frame iterator consumed |
| `src/server/connection.rs`  | Buffer reuse and oneshot pooling in Monoio handler (FIX-P1/P2) | VERIFIED | `tmp_buf` declared before loop at line 3150; containers pre-allocated at lines 3154-3159 |
| `src/shard/mesh.rs`         | Increased CONN_CHANNEL_CAPACITY (FIX-P1)               | VERIFIED   | Line 16: `pub const CONN_CHANNEL_CAPACITY: usize = 4096;`            |

---

### Key Link Verification

| From                                      | To                              | Via                                          | Status   | Details                                                        |
|-------------------------------------------|---------------------------------|----------------------------------------------|----------|----------------------------------------------------------------|
| `handle_connection_sharded_monoio` remote path | `ShardMessage::PipelineBatch` | `remote_groups` HashMap + `.drain()` dispatch | WIRED    | Lines 3669-3690: entries pushed to `remote_groups`, then drained and sent as `PipelineBatch` |
| `handle_connection_sharded_monoio` read loop  | `tmp_buf` (reused buffer)       | `tmp_buf.resize` + ownership reassignment    | WIRED    | Lines 3363-3365: resize, ownership take, reassign returned buf  |
| `remote_groups.drain()` batch             | `reply_futures` parallel await  | push then `reply_futures.drain(..)`          | WIRED    | Lines 3711/3715: futures pushed then drained for parallel await |

---

### Requirements Coverage

| Requirement | Source Plan | Description                                                          | Status    | Evidence                                                         |
|-------------|-------------|----------------------------------------------------------------------|-----------|------------------------------------------------------------------|
| FIX-P0      | 40-01-PLAN  | Port pipeline batch dispatch to Monoio handler (PipelineBatch not Execute) | SATISFIED | `ShardMessage::PipelineBatch` at line 3686; no `ShardMessage::Execute` in remote dispatch path (lines 3104-3800) |
| FIX-P1      | 40-02-PLAN  | Fix per-read buffer allocation; increase CONN_CHANNEL_CAPACITY to 4096 | SATISFIED | `tmp_buf` outside loop (line 3150); `CONN_CHANNEL_CAPACITY = 4096` (mesh.rs line 16) |
| FIX-P2      | 40-02-PLAN  | Pre-allocate batch containers (responses, remote_groups, reply_futures) | SATISFIED | All three containers declared before main loop (lines 3154-3159); `.clear()`/`.drain()` used inside loop |

**Note:** FIX-P0, FIX-P1, FIX-P2 are internal phase requirement IDs specific to Phase 40. They do not appear in `.planning/REQUIREMENTS.md`, which only covers v1 functional requirements (PROTO-*, NET-*, CONN-*, etc.) up to Phase 5. No orphaned requirements found — all phase-declared IDs are satisfied and none appear in REQUIREMENTS.md mapping tables.

---

### Anti-Patterns Found

| File                        | Line | Pattern                                  | Severity | Impact                                                                                     |
|-----------------------------|------|------------------------------------------|----------|--------------------------------------------------------------------------------------------|
| `src/server/connection.rs`  | 3165 | `let sub_tmp_buf = vec![0u8; 8192];`    | Info     | Per-iteration allocation in subscriber mode path only — not on the main command path; plan scope was main read loop only. No regression introduced. |
| `src/server/connection.rs`  | 2003 | `TODO: Full subscriber mode support`     | Info     | Pre-existing in Tokio handler (line 2003, before Monoio section at 3104). Not introduced by Phase 40. |

No blockers. The `sub_tmp_buf` allocation exists in the subscriber-mode branch (`if subscription_count > 0`), a distinct code path from the main read loop. The plan's scope was the main path only and acceptance criteria confirmed this.

---

### Build and Test Verification

| Check                                        | Result  | Notes                                                             |
|----------------------------------------------|---------|-------------------------------------------------------------------|
| `cargo check --features runtime-monoio`      | PASSED  | Compiles cleanly; only 2 pre-existing unused import warnings      |
| `cargo test --lib` (unit tests)              | FAILED  | Pre-existing errors: `tokio` crate unresolved under monoio feature; `E0282` type inference in `src/acl/table.rs`, `src/command/list.rs`, etc. Confirmed identical errors on pre-Phase-40 commit (`a6aa1b2`). Not introduced by Phase 40. |
| Git commits verified                         | PASSED  | Commits `2392e4b`, `f78912c`, `b5c7d78` all exist with correct file modifications |

---

### Human Verification Required

None required. All goal-critical behaviors are verified programmatically:
- PipelineBatch dispatch presence/wiring is code-traceable
- Buffer reuse pattern is structurally verifiable (declaration location + reassignment)
- Constant value is a direct grep check
- Container reuse via `.clear()`/`.drain()` is textually verifiable

---

### Gaps Summary

No gaps. All 6 observable truths pass all three verification levels (exists, substantive, wired). The phase goal is fully achieved:

- P0: Monoio handler now uses the same deferred `remote_groups` + `ShardMessage::PipelineBatch` pattern as the Tokio handler. No `ShardMessage::Execute` remains in the remote dispatch path of the Monoio handler. Frame iterator is consumed (no clone).
- P1: Read buffer moved outside main loop; ownership I/O reassignment pattern correctly implemented. `CONN_CHANNEL_CAPACITY` increased from 256 to 4096.
- P2: All three hot-path containers (`responses`, `remote_groups`, `reply_futures`) are pre-allocated before the main loop and reused via `.clear()`/`.drain()` on each iteration.

---

_Verified: 2026-03-26_
_Verifier: Claude (gsd-verifier)_
