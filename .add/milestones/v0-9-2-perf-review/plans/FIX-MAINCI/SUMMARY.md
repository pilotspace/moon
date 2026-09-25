# FIX-MAINCI — SUMMARY (main's macOS/Windows reds, the #1242 CI reds, moon#1253, moon#1255)

- **Branch and base:** `fix/main-ci-platform`, base `07d9850`, head `ad56738`.
- **Design reasoning:** NOTES.md holds the mechanisms, designs, risks and 0–1 scores.
- **Hygiene:** no new `unsafe`, and no test was skipped, ignored, deleted or weakened.

## Verdicts

| Item | Commit(s) | Verdict | Evidence |
|---|---|---|---|
| 1. Golden checksum test fails on macOS/Windows | f10ed7c | FIXED (test) | Asserts streaming == one-shot on every platform; the goldens are pinned only where libm is pinned. Red on a streaming mutation; red on the Linux pin with correctly-rounded libm emulated. |
| 2. APPEND amortisation ratio fails on Windows | dc0b625 | FIXED (test) | Measures work per call, with a 64x Windows bound (Windows measured 14.4–20.6x). The old quadratic body measures 265–276x and goes red. |
| 3. WAL poison test fails on macOS | 18b6d52 | FIXED (test) | Root cause: `wait_durable` queues a duplicate fsync, which consumes the armed failure. With the ordering forced, old red 5/5, new green 5/5. |
| 4a. HRANDFIELD timing bound (#1242 red) | a254e7c | FIXED (perf plus per-form bound) | The count walk caches the next wanted position (4.4 → 3.4 ns/step release). The bound is derived per form. The `entries()` mutation measures 0.87–0.90x, which is red. |
| 4b. ws15 Windows "flake" → real resurrection, moon#1253 (P0) | 6e4a8eb (Fixes #1253), a71596e, b4b57c4 | FIXED (product) | Lib and real-server tests go red then green, on both runtimes and both layouts. New nightly suite `crash_recovery_cold_del_inflight_1253`. |
| 4c. LFU OBJECT FREQ | 4997d14 | FIXED (test) | The miss probability drops from 1.3e-4 to 1.4e-21; 300/300 green. |
| 4d. ws12 aborted-BGSAVE fixture | 50e5ee4 | FIXED (test) | Polls for the arm marker instead of sleeping 10 ms. |
| 4e. ws8 MSET-during-BGSAVE | 5bf5107 (test-only hold hook), 5b73454 | FIXED (deterministic) | Red 2/2 with the capture-free leg restored (135 keys wrong both times). |
| 4g. moon#466 regression from #1253 | 1fc77c3 | FIXED | Superseded handles have their own counter; `eviction_accounting` 5/5. |
| R1. SWAPDB strands a superseded request | 889a884 | FIXED | Fallback settle across the shard's dbs, only on a miss. Red before (3 left), green after. |
| R2. Bound the superseded set | — | **DEFERRED** to part 4, on WS19's spill thread | The watermark design is in NOTES R2. |
| R3. The crash suite flakes on `-OOM` | 8599cc1 | FIXED (test) | Retries `-OOM`, then judges a refused probe against its original value. The retry path has not run live. |
| R4. crash-matrix.yml comment | aac5887 | FIXED | The nightly is monoio: s4 is per-shard, s1 is TopLevel. |
| R5. Prove the window was hit | 9e531bb | FIXED (test) | INFO `spill_completion_superseded` rose in 33/36 rounds; an 8 s settle reads [0,0,0] and fails. |
| R6. moon#1255: expired in-flight record | a5b9627 (Fixes #1255) | FIXED (live half) | Mutation red on the write, read and completion cases. Replaying pre-fix logs is recorded on #1255 as a separate decision. |
| 4f. Pre-existing flakes | — | Cause only | See NOTES. |

## Gates at HEAD
- fmt, audit-unsafe, audit-unwrap, clippy `--all-targets` and tokio clippy/check: all clean.
- Full lib suite: monoio 6613 passed / 0 failed; tokio 5675 / 0.
- Real server, final debug binaries, monoio and tokio, `--shards 4` and `1`:
  - in-flight crash suite 3/3;
  - ws15 2/2, twice per runtime;
  - moon#1215 crash suite 8/8.

## Product consequence of item 1 (filed separately)
- EXACT-mode segment `metadata_checksum` hashes the Box–Muller QJL matrices, whose values depend on the host libm (`sinf`/`cosf`/`logf`, faithful rather than correctly rounded). Linux x86_64 → aarch64 glibc is likely portable but unmeasured; musl, macOS and Windows are not.
- A mismatch sends recovery to level 3: keys are attributed from headers, then re-indexed. Nothing is corrupted, but startup does a full HNSW rebuild.
- Fix direction: a libm-free checksum, or a versioned formula that does not hash the matrices.

## Risks on the hosted macOS/Windows legs
- The LIGHT and SQ8 goldens are pinned on every platform, on the argument that those modes touch no libm.
- The Windows APPEND ratio with 8-byte chunks has not been measured on Windows.
- The macOS WAL test now waits for the sync agent to go idle; hosted macOS scheduling is unmeasured.
- ws15 runs without its settle on Windows for the first time.
- The new crash suite's R5 check fails loudly if all 3 rounds miss the window. That happened in 0 of 12 runs here.
- Merging WS19 must keep settle-on-every-outcome, including R1's fallback. Merging WS16 must keep the hold-hook line.
- HRANDFIELD's 2x COUNT bound is untested on hosted runners.

## Residuals
- **R2:** the superseded set is unbounded if a completion never arrives (a dropped completion or a dead spill thread). Each entry costs one handle, counted in `spill_superseded_bytes`.
- **SWAPDB (moon#1237 territory):** a newest in-flight record moved by SWAPDB is ghosted by its completion and stays in the swapped db until the key is next written.

## CHANGELOG bullets
- **A key deleted, flushed or overwritten while its disk-offload spill was still in flight no longer comes back after BGREWRITEAOF + restart** (moon#1253).
  - The rewrite fold writes a head `DEL` for every spill request a write retired in flight.
  - Every completion outcome, including one after SWAPDB, settles its request.
  - FLUSHALL still leaves `used_memory` at 0.
- **An expired in-flight spill no longer drops a collection write that re-creates its key** (moon#1255). Before, an acknowledged RPUSH/HSET/SADD/ZADD could be lost on restart.
- **`HRANDFIELD key COUNT` resolves its picks in one sorted walk,** at one compare per pair.
- **Tests now pass on macOS and Windows:**
  - the vector checksum test is platform-correct;
  - the APPEND amortisation bound is per platform;
  - the WAL poison test is deterministic.
  - Four timing-dependent tests are made deterministic.

(Committed by the orchestrator from the FIX-MAINCI agent's final report, because the harness refused the agent's SUMMARY write.)
