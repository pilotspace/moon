# WS16-snapshot-capture — SUMMARY

- **Branch:** `perf/ws16-snapshot-capture`, base `016ca5a`.
- **Personas:** storage-durability-engineer (lead) · ci-test-integrity-engineer.
- **Design reasoning:** NOTES.md holds the capture-site audit table and the moon#1185 re-assessment.
- **Audits:** no new `unsafe`, and the unwrap ratchet is unchanged.
- **Binaries:** every real-server result used a pinned debug build of this branch (`MOON_BIN`).

## Per-issue verdict

| Issue | Verdict | Commits | Evidence | Follow-ups |
|---|---|---|---|---|
| moon#1228 1a: MOVE / COPY … DB n captured nothing on either db | **FIXED** | `ea44ec0`, `c83598d` | **Unit** (`persistence::snapshot::capture_gap_tests`), red before: MOVE `Divergence { missing: 30, extra: 30 }`; COPY `{ extra: 20, wrong_value: 20 }`. **Real server** (`perf_ws16_bgsave_capture::move_and_copy_during_bgsave_*`), red on baseline-ae21476: at --shards 1, 2,027 db-0 keys missing or wrong, with 4,324 / 4,464 stray keys in db 1 / db 2; at --shards 4, 104 / 371 / 384. **Green** on both runtimes. | — |
| moon#1228 1b: WS DROP key sweep | **FIXED** | `035b774`, review 4: `f743bab2` | **Unit:** red with the capture removed (`missing: 471`). **Real server:** `workspace_drop_during_bgsave_*` red on baseline (200,047 / 200,545 of 204,000 keys restored). **Green** on both runtimes. Review 4: the replica's `apply_ws_drop` was a fourth sweep copy with no capture (proof: 497 of 500 dropped keys missing from a replica's own BGSAVE); it now calls `sweep_prefix`. | — |
| moon#1228 1c: MQ PUSH/POP/ACK, CREATE, DLQ, TXN MQ.PUBLISH, replica MQ apply | **FIXED** | `0328e7a`, `79c1ecd` | **Unit:** red without the captures ("63 queues reached the file at their post-epoch state"). **Real server:** `mq_during_bgsave_*` red on baseline (all 64 queues at s1, ~25/64 at s4). **Green** on both runtimes. | — |
| moon#1228 1d: stream wake path | **FIXED** | `fc1c863` | **Unit:** red without the captures ("48 of 48 streams reached the file with the mid-save group read"). No deterministic real-server trigger exists. | — |
| moon#1228 2a: FLUSHDB / FLUSHALL / SWAPDB discarded the whole epoch | **FIXED** (FLUSHDB, SWAPDB); FLUSHALL **aborts, as redis's does** | `64816f8`, `5f6728d`, `2037de5`; review 4: `ad3a795b`, `52544bf3`; review 5: this commit | **Unit:** 8 rewritten `table_swap_tests` are red on the pre-fix code. They include the liveness test `a_workload_flushing_faster_than_one_save_still_completes_it` (12 seeds of FLUSHDB/SWAPDB/DEL/SET/INCR every tick). **Real server:** `perf_ws12_bgsave_split::table_swaps_during_bgsave_keep_the_save_point_in_time` red on baseline (`"err"` vs `"ok"`). **Green** on both runtimes, with all 600K keys exact. Review 4: FLUSHALL fails the save again (redis `killRDBChild` parity, no ~2x RSS window); 3 unit tests red with the hook neutralized; review 4 claimed FLUSHDB of every database was bounded by the epoch-start dataset, but only the table count was bounded. Review 5: the drain trims a flushed table to its epoch-start rows. The real-server reproduction went from `current_cow_size` 344,782,240 and RSS +341 MB to 0 and +49..+58 MB. | A replica full resync and a FLUSHALL abort the save, by design. |
| moon#1228 2b: the per-tick budget did not converge under inserts; COW memory was invisible | **FIXED** (the Linux perf-host measurement is DEFERRED) | `864a3c8` | `cow_budget_tests::the_walk_keeps_up_with_an_insert_flood_the_constant_budget_cannot`, 50K keys plus 5,000 inserts/tick: scaled budget 7 ticks, peak 7,369 pre-images; constant budget 99 ticks, peak 73,708. INFO `current_cow_size:0` is asserted after every save. | Measure at pipelined insert rates on a Linux perf host. |
| moon#1250: MQ never charged to used_memory (added by the orchestrator) | **FIXED** | `396dc1e`; review 4: `d0bd7f4e` | `tests/perf_ws16_mq_billing.rs`, **red** on 864a3c8 and on baseline-ae21476: 20,000 of 20,000 pushes accepted, MEMORY USAGE 5,720,321, used_memory +24,534. **Green:** --shards 1 answers `-OOM` after 14,664 pushes (MEMORY USAGE 4,194,225 vs used_memory +4,208,922); --shards 4 after 3,665 (1,048,511 vs +1,038,467). Review 4: a POP's released surplus left the PEL untracked, so every POP over-charged and `used_memory` drifted up (proof: billed − true 286,500 B after 500 POPs). Released through `Stream::xack` now; `pop_ack_churn_keeps_the_charge_exact_*`: prefix +97,586..+279,991 B vs MEMORY USAGE +176, fix +176..+367 on both queues. Review 5 (moon#1261): the MqPop apply now bills its claims (`Stream::restore_claims`). Before, WAL replay and the replica billed a churned queue at 28,421 B against 124,097 live; now both are exact. | — |
| moon#1185 remainder: incremental COW AOF fold | **DEFERRED** | `12f4121` (NOTES) | Now covered: MOVE/COPY, WS DROP, MQ and stream-wake capture, FLUSH*/SWAPDB followed, resync aborts. **Remaining blocker:** eviction victims (`storage::eviction::evict_one*` → `db.remove`) take no pre-image. A key spilled after the fold cut, in an unwritten range, is absent from the hot base while its cold copy sits above the fold's watermark, so replay loses it. Fixing it needs the victim's db index threaded through `EvictionRun`. The TXN.ABORT undo writes also need a decision. | Add the eviction capture hook, then build the WS12 consumer-registry fold on top of the 2a slot map. |

## Review 4 (MERGE-AFTER-FIXES)
All six items are addressed, each in its own commit; NOTES.md has the table.
- **1 (BLOCKING):** `d0bd7f4e`, MQ billing stays exact through POP/ACK churn.
- **2:** `f743bab2`, replica `WS.DROP.APPLY` captures.
- **3:** `ad3a795b`, FLUSHALL aborts (parity), with the FLUSHDB-every-db bound.
- **4:** `3c4e73fc`, the sleeps became an armed-epoch wait.
- **5:** `236f563e`, `5487322e`. The rewritten F1 fixture was **not** red with the writer-cancel fix reverted. The FLUSHALL variant is red 5 of 5 runs alone, and green beside the other tests on the reverted binary: F1 is a race. It runs on both runtimes again. The resync variant is kept for the resync abort path; it is not an F1 regression, and is ignored on tokio (PSYNC needs a monoio master).
- **6:** `52544bf3` and `ad3a795b`.
- **Merge of `origin/main`:** `9915a665` (part 3b). The conflicts were `mq_exec::handle_push` and `perf_ws12_bgsave_split`.
- **After the merge:** 3b's snapshot-hold hook makes the F1 aborts and the first capture rounds deterministic (`5487322e`, `9e94d945`). The latter fixes a 4-shard MQ capture-test failure under parallel load.

## Review 5 (MERGE-AFTER-FIXES)
Every item is addressed, one commit each; NOTES.md has the table.
- **A (BLOCKING):** `11cf616e`.
  - Review 4's FLUSHDB bound was count-only. A table was frozen as flushed, with every post-epoch row, so the reviewer's reproduction held 344 MB under `--maxmemory 64mb`.
  - The drain now trims a flushed table to its epoch-start rows, restoring its pre-images; it rebuilds the table when post-epoch inserts at least doubled it. The save holds at most the epoch-start bills of the databases it has not written.
  - A second grown table flushed before one drain fails the save. Corrected by review 6 (S2): the check also fired for a table that did not grow; the rule is now re-derived for the budgeted trim (risk 1).
  - Real server: `current_cow_size` 344,782,240 and RSS +341 MB before; 0 and +49..+58 MB now. The save completes and restores the epoch-start keyspace.
- **B (moon#1261):** `6446832f`, `f9b89d65`. `Stream::restore_claims` bills the MqPop apply, on replay and on a replica. Before: 28,421 vs 124,097 B; now equal.
- **C:** `ac6a4896`, docs only. The 2b trade-off table (under Measurements) and a "Changed" CHANGELOG bullet; no doc claims dispatch starves during a save.
- **D:** `48414029`. POP's `COUNT + MAXDELIVERY` saturates.
- **E:** `9037ebdd`. The defensive branches are kept, with comments.
- **F:** `54d0f2c1`.
  - A refused MQ push or dead letter is no longer logged.
  - Dead letters are acked only once the DLQ took them.
  - `Stream::next_auto_id` wraps explicitly at the last ID (it used to panic a debug build).
- **File size:** `b559c1f1`. mq_exec.rs's tests moved out (1,616 to 1,094 lines).

## Review 6 (MERGE-AFTER-FIXES)
Fix A held under the reviewer's property tests (2,000 lib seeds, 80 real-server seeds). Every item is addressed, one commit each; NOTES.md has the table.
- **Property tests adopted:** `32e55cd9`. Lib `prop_tests` (16 seeds by default, env override) and real-server `perf_ws16_bgsave_prop` (3 seeds at `--shards 1` and 3 at `--shards 4`). The mutation evidence is in NOTES.
- **S1 (trim cost):** `103e996c`. The trim is budgeted at 512 row operations per drain; the bound holds after ceil(work / 512) drains. The walk waits only for a rebuild.
  - The property test caught an interleaving bug (seed 104); it is fixed.
  - Release server, 2M + 6M rows: PING p99.9 1.0 ms and max 9.2 ms during the trim. Main's FLUSHDB of that table stalls 193 ms; the unbudgeted trim took 4.33 s.
- **S2 (abort rule):** `e091ebe7`. A table that did not grow never fails the save. The rule is re-derived over the whole trim: a grown flush fails the save only while another grown table is still waiting or trimming and together they pass 8 MiB.
- **N1:** `66e06764`. A FLUSHDB during a save reclaims queued lazy-free charges before the table is billed.
- **N2:** `f837ad19`. `XADD <ms>-*` at the last sequence answers redis's error; it used to panic a debug build.
- **N3:** `06c7e3dd`. A POP of an empty queue creates no consumer, so master, replica and replay agree. The reviewer's B/F agreement test is adopted with it.
- **P1:** `cba154c5`. Documents that the RRDSHARD file holds hot keys only.

## Measurements
- **Real-server capture tests** restore from the RDB file alone after SIGKILL. Overlap is observed, not assumed: every `shard-<id>.rrdshard.tmp` must exist before the writes; each round is pipelined with `INFO persistence`; a round counts only if the save is still running. On baseline, 24–485 rounds landed inside saves of 72–221 ms.
- **MQ billing:** 2 consecutive green runs, with the numbers above.
- **No BGSAVE running:** the only added cost is one thread-local `bool` load on MOVE/COPY/MQ/WS DROP/stream-wake writes and `Database::clear`.
- **During a BGSAVE — the 2b trade-off (review 5, the reviewer's release A/B; decision: keep):**

  | Load during one BGSAVE (release, `--shards 1`) | Without 2b scaling | With it (WS16) |
  |---|---|---|
  | SET flood, light: write p99 | 804 µs | 1,818 µs |
  | SET flood, heavy: write p99 | 2,383 µs | 4,294 µs |
  | SET flood: write p99.9 | — | +25–80% |
  | SET flood: max latency | — | no worse |
  | SET flood: save duration | 2.26 s | 0.88 / 0.77 s (2.6–2.9x sooner) |
  | Reads only; and `--shards 4` | — | no regression |
  | Worst stall, 76 release saves | 39 ms (100 B), 92 ms (64 KiB) | the same |

  The scaled budget does more work per tick while writers are ahead of the walk. Write tails during the save roughly double; the save ends ~2.7x sooner, so pre-image memory is held for less time. The 0.7–1.7 s replica-link delay seen in the F1 fixtures was a debug-build and load artifact. It did not reproduce in release, where the worst stall across 76 saves was 39 ms (100 B values) and 92 ms (64 KiB), the same with and without the scaling.

## Cross-ownership edits
- **`ea44ec0`, index arguments only:** handler_monoio/mod.rs and handler_sharded/mod.rs (MOVE, COPY); spsc_two_db.rs; shared.rs (both MULTI executors); replication/apply.rs; replay.rs; handler_single.rs.
- **`035b774`:** new `workspace::sweep_prefix`. The WS DROP sweep blocks of handler_monoio/write.rs and handler_sharded/write.rs, and spsc_handler.rs's `WsDropCleanup` arm, each became one call to it.
- **`0328e7a`:** the TXN MQ.PUBLISH self legs (both txn.rs files) and spsc_handler.rs's `MqTxnMaterialize` call `mq_exec::materialize_mq_intents`. The `apply_mq_*` functions in shared_databases.rs capture.
- **`64816f8`:**
  - `Database::clear` in kv_ops.rs hands its old table to `snapshot_cow::note_cleared_table`.
  - One line in `persistence_tick.rs::advance_snapshot_segment` (WS19's file).
- **`5f6728d` / `2037de5`, test-only:**
  - `perf_ws12_bgsave_split`: the FLUSH/SWAPDB test now asserts the point-in-time contract. The F1 test is re-triggered by a replica full resync and is monoio-only.
  - `perf_ws15_bgsave_status`: the failed save is forced by a directory squatting on the snapshot path.
- **`864a3c8`:** a new INFO persistence field, `current_cow_size`, in command/connection.rs.
- **Review 4:** `f743bab2`, in `replication/apply.rs::apply_ws_drop`, calls `workspace::sweep_prefix`. `52544bf3` changes `Database::clear` in kv_ops.rs, one line: it passes `used_memory` as the frozen table's bill.
- **Review 6:**
  - `103e996c` re-exports `lazy_free_weight` pub(crate) from storage/db/mod.rs.
  - `66e06764` touches `Database::clear` in kv_ops.rs (3 lines: reclaim charged lazy-free items when a save is armed).
  - `f837ad19` touches `command/stream/stream_write.rs` (`XADD <ms>-*`).
- **`396dc1e`:**
  - The MQ local legs of both write.rs files pass the write gate (`handler_sharded::write::mq_write_gate`).
  - spsc_handler.rs's `MqCommand` arm gates through `spsc_eviction_gate`.
  - `apply_mq_*` bills.

## Risks for integration
1. **Intended behaviour change:** a BGSAVE crossed by FLUSHDB/SWAPDB now completes with the pre-change image, as redis does, instead of failing. A FLUSHALL still fails it, as redis's does. INFO `current_cow_size` reports what a FLUSHDB leaves the save holding:
   - **Review 4's bound was wrong.** "At most one per database, never more than the epoch-start dataset" held for the table count only. A table was frozen as flushed, with every post-epoch row, so fill-then-FLUSHDB of 8 databases held 344 MB under `--maxmemory 64mb`.
   - **Now (review 5):** the drains trim a flushed table to its epoch-start rows, so the save holds at most the epoch-start bills of the databases it has not written.
   - **Review 6:** the trim is budgeted, 512 row operations per drain. The bound holds once it is done: after ceil(work / 512) drains, where work = keys written since the save began + rows below the cursor + the kept rows if the table is rebuilt.
     - The reviewer's 8 x 10K-row case takes about 20-40 drains per table.
     - A 2M + 6M-row table takes about 12 s (release), during which PING stays at p99.9 1.0 ms and max 9.2 ms. Main's FLUSHDB stalls 193 ms on that table; the unbudgeted trim took 4.33 s.
   - **Until its trim's steps 1-2 are done,** a grown table holds its post-epoch rows. A flush of another GROWN database meanwhile fails the save once their post-epoch bytes together pass 8 MiB (review 6, S2): within one tick (a pipeline, MULTI, a script or two clients) or during the trim. A flush of a database that did not grow never fails it. It used to, whenever a grown table was waiting: a plain `SELECT 1; FLUSHDB; SELECT 2; FLUSHDB` pipeline returned `status err`.
2. **Merged with main (part 3b):** `persistence_tick.rs` and `kv_ops.rs::clear` auto-merged as the review expected. The gates below ran on the merged tree.
3. **Slot identity is by `Database` address,** recorded at arm time. This holds because slots are boxed and SWAPDB swaps contents, not addresses. An unidentifiable flush aborts the save as before; it never freezes the wrong table.
4. **Test fixtures:** the F1 end-to-end guard is FLUSHALL-based again and runs on both runtimes. It is red only on a quiet box: F1 is a race, and the deterministic guards are `stream_tests::an_aborted_snapshot_cannot_*`. The resync variant is `ignore`d on tokio. **Follow-up for the orchestrator:** a tokio-master PSYNC would let it run there too. Three WS16 tests now depend on 3b's `MOON_TEST_SNAPSHOT_HOLD_FILE`.
5. **MQ now refuses under maxmemory:** MQ CREATE and PUSH answer `-OOM` over the limit, as XADD does. This is a behaviour change for the CHANGELOG.
6. **Artifact aliasing:** one unpinned run executed another tree's binary. Every result above names a pinned binary.
7. **File sizes:** spsc_handler.rs (4,970), shared_databases.rs (2,724, +92 in review 5: the apply and its test) and storage/db/mod.rs (3,848, +11) were already over the limit. mq_exec.rs is at 1,094 after its tests moved out; stream.rs is at 1,462.
8. **By design, pre-existing (review 6, P1):** the RRDSHARD file holds hot keys only.
   - Cold-tier keys (`--disk-offload`) live in the cold tier's own heap files and manifest, and recovery reads them from there.
   - Spill-in-flight keys finish in the cold tier.
   - A restore from the RRDSHARD file alone has neither kind.
9. **Residuals** (in NOTES):
   - eviction takes no pre-image (the moon#1185 blocker, and a point-in-time gap for evicted keys);
   - the TXN.ABORT undo needs a decision;
   - ~~the replica MQ PEL bytes are untracked~~: review 4 called it a replica-only under-count; it was an over-credit on replay AND on the replica. Fixed in review 5 (moon#1261);
   - `Database.db_index` went stale after SWAPDB (fixed by FIX3B-CM in #1242, now merged).

## Test results after review 6 (production code `06c7e3dd`; P1 and this commit are docs)
- **Lint and checks:** fmt and the four audits PASS. clippy `--all-targets -D warnings` is clean on monoio and tokio.
- **Lib tests:**
  - full monoio 6,659 passed;
  - the touched modules (the 16 filters of review 5 plus `command::stream`): monoio 726, tokio 690;
  - the property test at 400 seeds: 200 with tiny budgets, 7,302 mid-trim observations, 101 rebuild waits, bill error 0 over 17,931 checks.
- **Integration, monoio (`/home/user/wt/bin/ws16-r6-final-monoio`):**
  - perf_ws12 5/5, perf_ws15 3/3, perf_ws8 1/1;
  - perf_ws16_bgsave_capture 7/7, perf_ws16_bgsave_prop 2/2 (and 12 seeds on a longer run), perf_ws16_mq_billing 10/10;
  - with ignored tests included: replication_ws 4/4, replication_readonly_ws_mq 1/1, replication_mq 4/4, replication_swapdb 3/3.
- **Integration, tokio (`/home/user/wt/bin/ws16-r6-final-tokio`):**
  - perf_ws12 4/4 + 1 ignored, perf_ws15 3/3, perf_ws8 1/1;
  - perf_ws16_bgsave_capture 7/7, perf_ws16_bgsave_prop 2/2, perf_ws16_mq_billing 6/6 + 4 ignored (they need a replica);
  - mq_integration 17/17, workspace_integration 13/13.

## Test results after review 5 (production code `54d0f2c1`; test/doc commits after it)
- **Lint and checks:** fmt and the four audits PASS. clippy `--all-targets -D warnings` is clean on monoio and tokio.
- **Lib tests:** full monoio 6,652 passed. The touched modules pass on both runtimes: monoio 677, tokio 641.
- **Integration, monoio (pinned):**
  - perf_ws12 5/5, perf_ws15 3/3, perf_ws8 1/1, perf_ws16_bgsave_capture 7/7, perf_ws16_mq_billing 7/7;
  - replication_ws 4/4, replication_readonly_ws_mq 1/1, replication_mq 4/4, replication_swapdb 3/3.
- **Integration, tokio (pinned):**
  - perf_ws12 4/4 + 1 ignored, perf_ws15 3/3, perf_ws8 1/1;
  - perf_ws16_bgsave_capture 7/7, perf_ws16_mq_billing 5/5 + 2 ignored (they need a replica, and PSYNC needs a monoio master);
  - mq_integration 17/17, workspace_integration 13/13.

## Test results after review 4 and the merge (code HEAD `9e94d945`)
- **Lint and checks:** fmt, audit-unsafe, audit-unwrap, audit-test-tempdirs and audit-encoding-limits PASS. clippy `--all-targets -D warnings` passes on monoio and on tokio.
- **Lib tests** (the same 14 module filters): monoio 598 passed / 1 ignored; tokio 562 passed / 1 ignored.
- **Integration, monoio** (merged debug build, pinned):
  - `perf_ws12_bgsave_split` 5/5, `perf_ws15_bgsave_status` 3/3, `perf_ws8_mset_bgsave_capture` 1/1;
  - `perf_ws16_bgsave_capture` 6/6, `perf_ws16_mq_billing` 4/4;
  - ignored-included: `replication_ws` 4/4, `replication_readonly_ws_mq` 1/1, `replication_mq` 4/4, `replication_swapdb` 3/3.
- **Integration, tokio:**
  - `perf_ws12_bgsave_split` 4/4 plus 1 ignored (the resync variant), `perf_ws15_bgsave_status` 3/3, `perf_ws8_mset_bgsave_capture` 1/1;
  - `perf_ws16_bgsave_capture` 6/6, `perf_ws16_mq_billing` 4/4;
  - `mq_integration` 17/17, `workspace_integration` 13/13 plus 1 ignored.
- **Review proof file** (registered temporarily): 5 of 6 green. `r4_red_plain_eviction_*` stays red; that is the eviction gap the orchestrator is filing.

## Test results before review 4 (HEAD `2037de5`, gates at `396dc1e`, where only test files changed after)
- **Lint and checks:** fmt, audit-unsafe and audit-unwrap PASS. clippy `--all-targets` passes, as do tokio clippy and tokio `check --all-targets`.
- **Lib tests** (snapshot, persistence_tick, mq_exec, stream_wake, wakeup, move_cmd, scripting::bridge, spsc_two_db, shared_databases, replication::apply, db_plane, workspace, server::conn::tests, storage::db): monoio 581 passed / 1 ignored; tokio 545 passed / 1 ignored.
- **Integration, monoio:**
  - `perf_ws16_bgsave_capture` 6/6, `perf_ws16_mq_billing` 2/2;
  - `perf_ws12_bgsave_split` 4/4, `perf_ws15_bgsave_status` 3/3, `perf_ws8_mset_bgsave_capture` 1/1;
  - `move_copy_db_crash_recovery_1046` 4/4, `bgsave_startup_race` 1/1;
  - ignored-included: `multi_move_copy_db_1062` 8/8, `script_move_copy_db_1068` 9/9, `replication_mq` 4/4, `replication_swapdb` 3/3, `crash_recovery_mq_effects` 3/3.
- **Integration, tokio:**
  - `perf_ws16_bgsave_capture` 6/6, `perf_ws16_mq_billing` 2/2;
  - `perf_ws12_bgsave_split` 3/3 plus 1 ignored (F1), `perf_ws15_bgsave_status` 3/3, `perf_ws8_mset_bgsave_capture` 1/1;
  - `mq_integration` 17/17, `workspace_integration` 13/13;
  - `multi_move_copy_db_1062` 6/6, `script_move_copy_db_1068` 6/6, `move_copy_db_crash_recovery_1046` 4/4;
  - `kill_snapshot` 4/4, `bgsave_startup_race` 1/1.

## CHANGELOG bullets
- **Fixed (moon#1228):** a BGSAVE no longer records a wrong image when one of these changes a key the save has not written yet:
  - `MOVE` or `COPY … DB n`;
  - `WS DROP`;
  - the `MQ` subcommands, TXN `MQ.PUBLISH` and replicated MQ records;
  - a stream waker's group read.

  Each now captures the key's pre-save state first. Before, a key moved during a save could come back in both databases or in neither, and queues and streams came back with post-save messages and pending entries.
- **Changed (moon#1228):** `FLUSHDB` and `SWAPDB` during a BGSAVE no longer fail the save. It completes with the keyspace as it was when the save started, as redis does, so a workload that flushes more often than one save takes can now save at all.
  - The save holds a flushed database's pre-save rows until it writes them, as redis's forked child does. INFO `current_cow_size` reports them.
  - A `FLUSHALL` still fails an in-flight save, as redis's does, and so does a replica full resync.
  - So does a `FLUSHDB` of a database that grew during the save, if another database that grew is still being trimmed and together they hold more than 8 MiB of rows written since the save began. This covers two such flushes within one tick (a pipeline, `MULTI`, a script or two clients) or during the trim.
- **Changed (moon#1228):** while writes outpace a BGSAVE, the save now does more work per tick, up to 16x the entries and segments and 4x the bytes, scaled by how far writers are ahead of it. Saves under a write flood finish sooner and hold less copy-on-write memory, at the cost of higher write tail latency while the save runs.
  - Release build, `--shards 1`, SET flood: the save finished 2.6–2.9x sooner (2.26 s to 0.88 / 0.77 s).
  - Write p99 during the save roughly doubled (804 to 1,818 µs light, 2,383 to 4,294 µs heavy), and p99.9 rose 25–80%. Max latency did not change.
  - Read-only load and `--shards 4` show no regression.
  - New INFO persistence field `current_cow_size`: the memory a running save holds for itself. As in redis, it is not part of `used_memory`.
- **Fixed (moon#1250):** MQ writes are now charged to `used_memory`, the same way `XADD` is. This covers `MQ CREATE`, `PUSH`, `POP`, `ACK`, TXN `MQ.PUBLISH`, stream-wake group reads and replicated MQ records.
  - Over `maxmemory`, `MQ CREATE` and `MQ PUSH` are refused with `-OOM` under noeviction, or evict under an evicting policy.
  - Before, 20,000 pushes of 100 B added about 24 KB to `used_memory` for a 5.7 MB queue.
  - `MQ POP`/`ACK` churn keeps the charge exact. A POP's released surplus used to stay charged, so `used_memory` drifted up.
- **Fixed (moon#1261):** after a restart's WAL replay and on a replica, `MQ` queues are billed as on the master. Replayed or replicated POPs never charged their pending entries while ACKs credited them, so a churned queue's bill drained toward 0: 28,421 B against 124,097 B live.
- **Fixed (moon#1228):** during a BGSAVE, the memory held for a flushed database is bounded by that database's size when the save began.
  - Rows written after the save began are trimmed away over the following ticks, at a bounded cost per tick. FLUSHDB of a 2M-row database grown by 6M rows kept PING under 10 ms.
  - A queued lazy-free value is no longer counted twice in `current_cow_size`.
- **Fixed:** `XADD key <ms>-*` when the stream's top ID already has the last possible sequence for `<ms>` now answers "ERR The ID specified in XADD is equal or smaller than the target stream top item". A debug build used to crash; a release build wrapped.
- **Fixed:** at the last possible stream ID, `XADD *` and `MQ PUSH` answer an error instead of crashing a debug build.
  - A TXN `MQ.PUBLISH` the stream refuses is no longer written to the WAL.
  - A dead letter whose dead-letter stream is full stays pending in its queue instead of being lost.
- **Fixed:** `MQ POP` of an empty queue no longer creates the internal consumer on the master only. Master, replicas and a restart now agree on `XINFO CONSUMERS` and `MEMORY USAGE`.
- **Fixed:** `MQ POP … COUNT` with a count near 2^64 no longer overflows (it crashed a debug build) and delivers every queued message.

(Committed by the orchestrator from the WS16 agent's final report, because the harness refused the agent's SUMMARY write.)
