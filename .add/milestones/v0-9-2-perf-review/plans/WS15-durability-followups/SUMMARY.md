# WS15-durability-followups SUMMARY

- Branch `perf/ws15-durability-followups`, base `ae21476` (+ `6537679`). NOTES.md in this directory has the design reasoning.
- Binaries (release-fast):
  - `/home/user/wt/bin/ws15-monoio-af103a2` and `/home/user/wt/bin/ws15-tokio-af103a2`. Both allowed release builds were used on these. Provenance was checked with branch-only strings and the startup line: monoio prints `(1 shards, monoio)`, tokio prints `(1 shards)`.
  - `/home/user/wt/bin/ws15-debug-monoio-1c36c11` is a debug build of the final code commit.
  - `/home/user/wt/bin/baseline-ae21476` is the reference for every red run.

(SUMMARY.md committed by the orchestrator. The harness refused the subagent's write, and TEAM-RULES §6 applies. The content is the agent's final report.)

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1223 (P1): a spill withdrawn after a fold leaves the key in no durable artifact | **FIXED** | `140049e` fix, `9867eeb` test | See "moon#1223 evidence" below. | The same class of bug remains in the barrier-after-flip window (Risk 5). The BGSAVE `SnapshotState` also omits in-flight payloads (WS16 / moon#1228). |
| moon#1230: `rdb_last_bgsave_status` stays `err` forever | **FIXED** | `aa36b7e` fix, `af103a2` WS12 test, `7584583` test | See "moon#1230 evidence" below. | Sharded `--save` never fires (Risk 2). A BGSAVE with no persistence dir should write to `--dir`, as redis does; that needs main.rs / event_loop plumbing outside this workstream. It now fails loudly instead of hanging. |
| moon#1215 (P0): a deleted cold key resurrects after a rewrite and any restart | **FIXED** | `ecc4ee1` cherry-picked proof tests, `529d606` harness, `7767956` fix, `38fc290` shard-count param, `1c36c11` perf | See "moon#1215 evidence" below. | The one-time first-upgrade capture in main.rs writes COLDCUT without the DELs. The dead-slot ledger's RAM and fold stall are unbounded until compaction exists. The ledger is not yet visible in INFO. |

### moon#1223 evidence
- **Fix.** The fold base image now includes keys whose spill is still in flight (the issue's "heavier" option; the argument is in NOTES). It covers all three ways a spill can fail to publish: the marker is refused, the pwrite fails, or the file id is re-issued.
- **In-process.** `shard::persistence_tick::fold_inflight_tests` adopts the review's `rv_integ_withdrawn_spill_after_fold_has_a_durable_source`. It adds three cases: pwrite failed, id rejected, and a restart through production recovery.
  - With only the fix reverted, which is identical to ae21476's `stream_fold_image`, all three paths are red: `live=Some("acked-value") in_base=false manifest_lists_file=false in_post_fold_log=false`.
  - All are green after the fix, on both runtimes.
- **Real server** (`tests/perf_ws15_spill_withdraw_after_fold.rs`).
  - On ae21476, `--shards 1` with kill -9 is red: "1067 of 63049 keys acknowledged before BGREWRITEAOF are missing or wrong" (46 withdrawals). The other three variants do not reproduce on base.
  - The fixed binaries pass 4/4 on monoio and 4/4 on tokio.

### moon#1230 evidence
- **Fix.** A per-save failure latch. The status is `ok` only if every shard succeeded. LASTSAVE and the dirty counter move only on success. `SAVE_IN_PROGRESS` is cleared last.
- **Sibling fixes.**
  - A shard that skips a save now reports a failure. That happens with no persistence dir (the default without `--save`) or with a lost data dir. Before, it left `rdb_bgsave_in_progress:1` forever.
  - A sharded auto-save now starts as a counted save.
- **Unit tests:**
  - `bgsave_status_is_per_save_and_lastsave_moves_only_on_success`
  - `a_refused_second_bgsave_does_not_clear_the_running_saves_failure`
  - `a_sharded_auto_save_is_a_counted_save`
- **Real server** (`tests/perf_ws15_bgsave_status.rs`). On ae21476 every case is red at both shard counts:
  - "a failed save advanced LASTSAVE"
  - "a failed save reset rdb_changes_since_last_save (600000 -> 0)"
  - "a clean BGSAVE after a failed one reports rdb_last_bgsave_status:err"
  - "SHUTDOWN SAVE after a successful save was refused: -ERR SHUTDOWN failed: background save error, check logs"
  - "the BGSAVE never finished (rdb_bgsave_in_progress stuck at 1)"

  After the fix: 3/3 on monoio and on tokio, and `perf_ws12_bgsave_split` passes 4/4 on both.
- **Correction.** `aa36b7e` wrongly blamed the base's auto-save failure on the accounting; the real cause is the trigger (Risk 2). `7584583` corrects this by replacing that server case with a unit test.

### moon#1215 evidence
- **Fix.** A dead-slot ledger in the cold index records every slot still on disk that is no longer its key's index entry. The sources are deletes, re-spills, FLUSH, both sweeps, released older copies, and the ghost slots of published files. Each new AOF generation now opens with plain `SELECT`/`DEL` records for the keys that are dead at the fold instant. This covers all four fold writers. The format does not change: every moon version replays `DEL`.
- **Recovery-level tests** (`storage::tiered::cold_del_rewrite_tests`, 12 tests, no longer ignored, built with the production fold and recovery).
  - The originally ignored case is red on ae21476: `k1 was DELeted before the rewrite; it must not come back from file 5 left: Some([118, 49]) right: None`.
  - With only the head DELs disabled, six cases go red (`left: Some("v1")`).
  - All are green after the fix, on both runtimes.
- **Real server** (`tests/crash_recovery_cold_del_rewrite.rs`, 8 cases, still `#[ignore]` per house convention). The cherry-picked harness first had to be fixed in `529d606`: on Linux it lost half its filler and never spilled.

  Deleted keys that came back on ae21476:

  | case | `--shards 4` | `--shards 1` |
  |---|---|---|
  | DEL + rewrite + kill-9 | 86/100 | 87 |
  | DEL + rewrite + clean SHUTDOWN | 72/100 | 62 |
  | DEL after orphan sweeps | 83/100 | 44 |
  | FLUSHDB + rewrite | 157/200 | 134 |

  - The control, overwrite, TTL and flush-after-sweep cases pass on ae21476.
  - The fixed binaries pass 8/8 at both `--shards 4` and `--shards 1` on monoio, 8/8 at both on tokio, and 8/8 on the debug build of the final commit.
- **Downgrade.** After a rewrite by the fixed binary, the base binary boots the same data with 0/100 deleted keys back and 0/100 live neighbours lost.
- **Other tests:** the ledger (9), ghost slots (2), fold selection, flat-file wiring, and head dedupe/batching.

## Measurements
**#1215 ledger cost.** Interleaved A/B, 3 reps, fresh server per run.
- Setup: monoio release-fast, `--shards 1 --maxmemory 16mb`. Write 300,000 keys of 200 B, then DEL 294,000 of them, keeping every 50th so their files stay listed. Then run BGREWRITEAOF with a PING every 0.5 ms.

| run | binary | used_memory | max PING | p99 PING | head DELs | incr bytes |
|---|---|---|---|---|---|---|
| A1 | ae21476 | 578,303 | 4.04 ms | 2.15 ms | 0 | 35 |
| B1 | af103a2 | 15,732,016 | 251.72 ms | 1.79 ms | 559 | 5,729,580 |
| A2 | ae21476 | 417,485 | 5.31 ms | 0.65 ms | 0 | 35 |
| B2 | af103a2 | 16,060,629 | 254.37 ms | 3.07 ms | 571 | 5,850,380 |
| A3 | ae21476 | 575,806 | 3.15 ms | 0.32 ms | 0 | 35 |
| B3 | af103a2 | 16,095,572 | 252.40 ms | 2.02 ms | 572 | 5,863,195 |

- That is about 54 B of RAM and 0.85 µs of fold stall per dead slot. The base's zero cost is the bug itself: those keys came back.
- `1c36c11` moved the dedupe off the shard thread. On the same 294K-slot ledger, the selection pass dropped from 916–979 ms to 325–413 ms (2.6–2.9×). That was measured on a debug build. Release was not re-measured because a third release build is not allowed.

**Promote-then-sweep reproduction on base.** Monoio `--shards 1`, orphan sweep every 1 s: 88 of 200 acknowledged, never-deleted keys were lost.

## Cross-ownership edits
- `tests/perf_ws12_bgsave_split.rs` (WS12's file), in the isolated commit `af103a2`. It now asserts that the post-abort BGSAVE reports `ok` instead of scanning the log, as the plan asked. The immediate retry, the abort log check and the restart check are unchanged.

## Risks / things the orchestrator must re-check at integration
1. **New finding (pre-existing, not fixed): promote-then-sweep loss.** Reproduced 88/200 on base.
   - A key that is cold when a rewrite cuts its base is not in the base.
   - If it is later only read back into memory (or read-modify-written), it leaves the cold index without a log record.
   - Once the rest of its file is gone, the orphan sweep deletes the file, and after a restart the key is gone.

   The fix needs a "fold committed" signal (`shard/timers.rs`) and a promotion hook (`storage/db/kv_ops.rs`). An issue needs filing.
2. **New finding: sharded `--save` never fires.** Its change counter is incremented only by `handler_single`. Making it fire turns on periodic snapshots for every `--save` user, which is a behaviour decision. An issue needs filing. Also, SHUTDOWN SAVE during a running auto-save is now refused as "already in progress", as it already is during a user BGSAVE.
3. **Ledger cost.** About 54 B RAM per dead slot, counted in the eviction budget, plus the fold stall above. A workload that deletes most cold keys but keeps a few per file holds nearly the whole deleted set until those files are reclaimed. Compacting mostly-dead files would bound it.
4. **`ColdIndex::resident_bytes()` now includes the ledger.** Tests that expect cold-index bytes to return to 0 after removals must subtract it (updated: 3 in cold_index.rs, 1 in kv_spill.rs). `ColdIndex::remove` now records the slot, which matters for WS10.
5. **Barrier-after-flip window (not fixed).** A crash between committing a new AOF generation and the async persist of a spill's manifest entry can lose a key that was cold at the fold. Moving the barrier before the commit makes it process-wide, so it would cross-abort rewrites in parallel tests (the moon#750 class). It needs per-manifest scoping first.
6. **Fold API change.** `write_fold_image` and `write_fold_image_file` return `(u64, ColdDeletes)`, and `FoldChunk` gains a `ColdDeletes` variant. `stream_fold_image` (WS8's call site) is unchanged.
7. **Line limit.** Files already over 1500 lines grew: `cold_index.rs` 1922→1985, `rewrite.rs` 1925→1964, `persistence_tick.rs` 3294→3332. New logic went into new modules.
8. **Lib tests.** Monoio: 3529 passed; tokio: 3341 passed (filters `storage:: persistence:: shard:: command::`). The only failure is the known root-container env test.

## Gates at the final code commit (1c36c11)
- `cargo fmt --check` is clean.
- audit-unsafe and audit-unwrap pass (0 new unsafe).
- `clippy --all-targets` (monoio), clippy on tokio, and tokio `check --all-targets` are all clean.
- Lib tests pass on both runtimes, apart from the known env failure.
- Integration suites by name, both runtimes, all green:
  - `crash_recovery_cold_del_rewrite` (`--ignored`, shards 4 and 1)
  - `perf_ws15_spill_withdraw_after_fold`
  - `perf_ws15_bgsave_status`
  - `perf_ws12_bgsave_split`

## CHANGELOG bullets
- **Fixed (P1, data loss):** a key could be lost on the next restart when:
  - its spill was in flight when a BGREWRITEAOF fold cut its base, and
  - the spill then did not publish (the marker was refused under AOF backpressure, the pwrite failed, or the file id was re-issued).

  The key was in no durable artifact. The fold's base image now includes in-flight spill payloads. (moon#1223)
- **Fixed (P0, data resurrection):** under the default config, a deleted key came back after an AOF rewrite and any restart when its spill file still held live neighbours. This applied to DEL, UNLINK, FLUSHDB, or an overwrite of a cold key that later expired.
  - The cold index now remembers every dead slot still on disk. Every rewrite opens its new generation with plain `DEL`s for the keys dead at the fold instant.
  - Every moon version replays those records, so a downgrade keeps the deletes.
  - Cost: about 54 B of RAM per dead slot until its file is reclaimed, plus a fold pass over them. (moon#1215)
- **Fixed:** `rdb_last_bgsave_status` now reports the last save and returns to `ok` after a successful one.
  - Before, one failed sharded BGSAVE latched `err` forever, and every later `SHUTDOWN SAVE` was refused.
  - `LASTSAVE`/`rdb_last_save_time` and `rdb_changes_since_last_save` now move only on a successful save.
  - A shard that cannot write a snapshot (no persistence dir, or a lost data dir) now fails the save instead of leaving `rdb_bgsave_in_progress:1` forever.
  - Sharded auto-saves start as counted saves. (moon#1230)

## Self-evaluation (0–1)
Completeness 0.95 · Clarity 0.9 · Practicality 0.9 · Optimization **0.8** · Edge cases 0.9 · Self-evaluation 0.9

- **Why Optimization cannot reach 0.9 here:**
  - The fold's stall and RAM grow with the number of dead slots, because deciding which slots are dead needs key bytes on the shard thread at the fold instant.
  - A live dead-set would add a probe to every hot write, which the hot-path rules forbid.
  - Bounding the ledger needs file compaction, which is a new feature.
  - The one cheap win, moving the dedupe off the shard thread, has landed.
- **Self-evaluation:** reading base runs line by line caught two harness defects: the cherry-picked filler on Linux, and the first #1223 test. One commit over-claimed; `7584583` corrects it.
