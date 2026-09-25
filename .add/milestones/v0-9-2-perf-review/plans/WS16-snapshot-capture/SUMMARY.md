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
| moon#1228 1b: WS DROP key sweep | **FIXED** | `035b774` | **Unit:** red with the capture removed (`missing: 471`). **Real server:** `workspace_drop_during_bgsave_*` red on baseline (200,047 / 200,545 of 204,000 keys restored). **Green** on both runtimes. | — |
| moon#1228 1c: MQ PUSH/POP/ACK, CREATE, DLQ, TXN MQ.PUBLISH, replica MQ apply | **FIXED** | `0328e7a`, `79c1ecd` | **Unit:** red without the captures ("63 queues reached the file at their post-epoch state"). **Real server:** `mq_during_bgsave_*` red on baseline (all 64 queues at s1, ~25/64 at s4). **Green** on both runtimes. | — |
| moon#1228 1d: stream wake path | **FIXED** | `fc1c863` | **Unit:** red without the captures ("48 of 48 streams reached the file with the mid-save group read"). No deterministic real-server trigger exists. | — |
| moon#1228 2a: FLUSHDB / FLUSHALL / SWAPDB discarded the whole epoch | **FIXED** | `64816f8`, `5f6728d`, `2037de5` | **Unit:** 8 rewritten `table_swap_tests` are red on the pre-fix code. They include the liveness test `a_workload_flushing_faster_than_one_save_still_completes_it` (12 seeds of FLUSH/SWAPDB/DEL/SET/INCR every tick). **Real server:** `perf_ws12_bgsave_split::table_swaps_during_bgsave_keep_the_save_point_in_time` red on baseline (`"err"` vs `"ok"`). **Green** on both runtimes, with all 600K keys exact. | A replica full resync still aborts the save, by design. |
| moon#1228 2b: the per-tick budget did not converge under inserts; COW memory was invisible | **FIXED** (the Linux perf-host measurement is DEFERRED) | `864a3c8` | `cow_budget_tests::the_walk_keeps_up_with_an_insert_flood_the_constant_budget_cannot`, 50K keys plus 5,000 inserts/tick: scaled budget 7 ticks, peak 7,369 pre-images; constant budget 99 ticks, peak 73,708. INFO `current_cow_size:0` is asserted after every save. | Measure at pipelined insert rates on a Linux perf host. |
| moon#1250: MQ never charged to used_memory (added by the orchestrator) | **FIXED** | `396dc1e` | `tests/perf_ws16_mq_billing.rs`, **red** on 864a3c8 and on baseline-ae21476: 20,000 of 20,000 pushes accepted, MEMORY USAGE 5,720,321, used_memory +24,534. **Green:** --shards 1 answers `-OOM` after 14,664 pushes (MEMORY USAGE 4,194,225 vs used_memory +4,208,922); --shards 4 after 3,665 (1,048,511 vs +1,038,467). | The replica `apply_mq_pop` PEL bytes are untracked (replica-only under-count). |
| moon#1185 remainder: incremental COW AOF fold | **DEFERRED** | `12f4121` (NOTES) | Now covered: MOVE/COPY, WS DROP, MQ and stream-wake capture, FLUSH*/SWAPDB followed, resync aborts. **Remaining blocker:** eviction victims (`storage::eviction::evict_one*` → `db.remove`) take no pre-image. A key spilled after the fold cut, in an unwritten range, is absent from the hot base while its cold copy sits above the fold's watermark, so replay loses it. Fixing it needs the victim's db index threaded through `EvictionRun`. The TXN.ABORT undo writes also need a decision. | Add the eviction capture hook, then build the WS12 consumer-registry fold on top of the 2a slot map. |

## Measurements
- **Real-server capture tests** restore from the RDB file alone after SIGKILL. Overlap is observed, not assumed: every `shard-<id>.rrdshard.tmp` must exist before the writes; each round is pipelined with `INFO persistence`; a round counts only if the save is still running. On baseline, 24–485 rounds landed inside saves of 72–221 ms.
- **MQ billing:** 2 consecutive green runs, with the numbers above.
- **No throughput A/B.** When no BGSAVE is running, the only added cost is one thread-local `bool` load on MOVE/COPY/MQ/WS DROP/stream-wake writes and `Database::clear`.

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
- **`396dc1e`:**
  - The MQ local legs of both write.rs files pass the write gate (`handler_sharded::write::mq_write_gate`).
  - spsc_handler.rs's `MqCommand` arm gates through `spsc_eviction_gate`.
  - `apply_mq_*` bills.

## Risks for integration
1. **Intended behaviour change:** a BGSAVE crossed by FLUSHDB/FLUSHALL/SWAPDB now completes with the pre-change image, as redis does, instead of failing. A flushed table the save has not written yet stays allocated until the walk passes it; INFO `current_cow_size` reports it.
2. **Merge with WS19:** there may be conflicts in `persistence_tick.rs` (`advance_snapshot_segment`) and `kv_ops.rs` (`clear`, which #1253's `spill_inflight_supersede_all` also edits). Re-run `persistence::snapshot`, `perf_ws12_bgsave_split` and `perf_ws15_bgsave_status` on the merged tree, both runtimes, with `MOON_BIN` pinned.
3. **Slot identity is by `Database` address,** recorded at arm time. This holds because slots are boxed and SWAPDB swaps contents, not addresses. An unidentifiable flush aborts the save as before; it never freezes the wrong table.
4. **Test fixture changes:** the F1 end-to-end guard needs `REPLICAOF`, which the tokio master does not answer, so it is `ignore`d on tokio and **the tokio end-to-end F1 coverage is gone**. The runtime-independent unit guards `stream_tests::an_aborted_snapshot_cannot_*` still run on both runtimes.
5. **MQ now refuses under maxmemory:** MQ CREATE and PUSH answer `-OOM` over the limit, as XADD does. This is a behaviour change for the CHANGELOG.
6. **Artifact aliasing:** one unpinned run executed another tree's binary. Every result above names a pinned binary.
7. **File sizes:** spsc_handler.rs (4,954) and shared_databases.rs (2,632) were already over the limit. mq_exec.rs is at 1,453.
8. **Residuals** (in NOTES):
   - eviction takes no pre-image (the moon#1185 blocker, and a point-in-time gap for evicted keys);
   - the TXN.ABORT undo needs a decision;
   - the replica MQ PEL bytes are untracked;
   - `Database.db_index` goes stale after SWAPDB (FIX3B-CM is fixing that in #1242).

## Test results (HEAD `2037de5`, gates at `396dc1e`, where only test files changed after)
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
- **Changed (moon#1228):** `FLUSHDB`, `FLUSHALL` and `SWAPDB` during a BGSAVE no longer fail the save. It completes with the keyspace as it was when the save started, as redis does, so a workload that flushes more often than one save takes can now save at all. A replica full resync still fails an in-flight save.
- **Performance (moon#1228):** the BGSAVE walk's per-tick budget grows with the pre-image backlog, up to 16×. In an in-process insert flood the save went from 99 to 7 ticks, and peak pre-images from 73,708 to 7,369. New INFO persistence field `current_cow_size`; as in redis, it is not part of `used_memory`.
- **Fixed (moon#1250):** MQ writes are now charged to `used_memory`, the same way `XADD` is. This covers `MQ CREATE`, `PUSH`, `POP`, `ACK`, TXN `MQ.PUBLISH`, stream-wake group reads and replicated MQ records.
  - Over `maxmemory`, `MQ CREATE` and `MQ PUSH` are refused with `-OOM` under noeviction, or evict under an evicting policy.
  - Before, 20,000 pushes of 100 B added about 24 KB to `used_memory` for a 5.7 MB queue.

(Committed by the orchestrator from the WS16 agent's final report, because the harness refused the agent's SUMMARY write.)
