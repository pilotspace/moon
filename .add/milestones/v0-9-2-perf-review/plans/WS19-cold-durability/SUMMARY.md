# WS19-cold-durability — SUMMARY

- **Base:** `016ca5a` (int/part3b `4115798` plus the part-4 plan).
- **Design reasoning:** NOTES.md holds the mechanisms, the crash windows, the A/B data and the gate records.
- **Binaries:** release-fast monoio and tokio builds of `db3490a`. Their provenance was checked by a branch-only log string. They were deleted at the end; rebuild from the integrated tree.
- **Red runs:** on `baseline-ae21476`, and on the part-3b reviewers' `5b14b82` binary (code identical to `016ca5a`).

## Per-issue verdict

| Issue | Verdict | Commits | Evidence | Follow-ups |
|---|---|---|---|---|
| moon#1231: a promoted cold key is lost once the sweep unlinks its file | **FIXED** | `22c516b` (test move), `c0c8059` (cross-ownership), `478ab3e` | In-process red: `(k08, k09)` recovers as `(None, Some("x"))`. Real-server kill -9 red on both old binaries at `--shards 1` and 4 (table below). Green on both runtimes at both shard counts. | The no-AOF WAL/snapshot recovery path is not analysed; its behaviour is unchanged. ~~With `auto-aof-rewrite-percentage 0`, held files wait for a manual BGREWRITEAOF.~~ Phase 2 (`d9fb8bde`): held files get their fold at percentage 0 too. |
| moon#1232: sharded `--save` never fires | **FIXED** | `a3247ad`, `7f9ca7a` (cross-ownership) | `perf_ws19_save_rules` red on both old binaries at s1 and s4: "ten changes with --save \"1 10\" produced no snapshot in 15 s". Green on both runtimes. Unit tests: `a_rule_fires_on_its_change_count_and_its_time`, `a_failed_save_is_retried_after_the_redis_delay`, `changes_made_while_a_save_runs_stay_counted`. | Embedded mode starts no auto-save (`server/embedded.rs`). ~~The rule's clock starts at the last auto-save, not at `lastsave`.~~ Phase 2 (`55ce5d98`): the rule runs from the last successful save. Phase 2 (`a2fa8147`): collection writes are counted, over-counts fixed. |
| moon#1240: cold reclaim fsyncs and commits the manifest on the shard thread | **FIXED**; the Linux-perf-host number is **DEFERRED** | `4512085` (test move), `8822d1d` (cross-ownership), `0694e16` | `reclaim_offload_tests::the_reclaim_tick_never_does_the_disk_io_itself`, with a real spill thread and a 300 ms injected manifest fsync. Red: compaction reverted to inline gives "the tick compacted inline"; adoption reverted to blocking makes the tick block 301.9 ms. Green. `cold_reclaim_tests` (10), the crash suite and `perf_ws15_ledger_bound` are green. | Latency A/B on a Linux perf host. |
| moon#1241: `redis.call('DEL')` answers `-OOM` over budget | **FIXED** | `71f7016`, `db3490a` (cross-ownership) | `perf_ws19_script_oom` red on both old binaries at s1 and s4. Green on both runtimes. The unit test is red with only the bypass reverted. | Pre-existing, unchanged: redis's `SCRIPT_WRITE_DIRTY`; `allow-oom` growing commands; shebang EVAL. |

### moon#1231
- **The bug is wider than promotion.** A committed fold's generation needs a spill file below its cut for any key that was cold at the fold and is later read without being overwritten. That covers:
  - promotion;
  - logged commands that read cold keys without promoting them (SUNIONSTORE, COPY);
  - a read-modify-write followed by a DEL;
  - the moon#1140 older-copy fallback.
- **The fix** (`storage::tiered::unlink_hold`): a zero-ref spill file below the latest cut the shard has seen is held until a fold whose snapshot comes after that moment has committed. Files minted after the latest cut are still unlinked at once. With no AOF there is no hold.
- **A second instance, found and fixed:** the reclaim adoption unlinked an old file even when a compaction output was discarded. It now unlinks only when every output was listed. Test: `survivors_promoted_after_the_fold_keep_the_old_file`.
- **In-process tests:** the 4 `shard::timers::promote_sweep_tests` run the production fold, commands, sweep and recovery.
- **Real-server kill -9:** three new `#[ignore]` cases in `tests/crash_recovery_cold_del_rewrite.rs`: promote with GET, promote with APPEND, and held files released after a later rewrite.

| Binary | Shards | GET: probes absent | APPEND: probes wrong | Release case |
|---|---|---|---|---|
| baseline-ae21476 | 1 | 179/200 | 140/200 | 27 → 0 spill files before any later fold |
| baseline-ae21476 | 4 | 171/200 | 170/200 | 34 → 0 |
| 5b14b82 (= 016ca5a code) | 1 | 143/200 | 155/200 | 27 → 0 |
| 5b14b82 (= 016ca5a code) | 4 | 153/200 | 158/200 | 34 → 0 |

Green: 11/11 at s1 and 11/11 at s4, on both the monoio and tokio release binaries.

### moon#1232
- **No new counter.** The existing per-thread padded dirty slots, summed on read (the same number INFO shows as `rdb_changes_since_last_save`), now drive the trigger. There is no shared per-write `fetch_add`.
- **Counting parity:** redis 7.0.15 also counts AOF-replayed writes as dirty.
- **Retry:** after a failed save the rule waits redis's 5 s retry delay.
- **Counting during a save** (`7f9ca7a`): writes made while a save runs stay counted, like redis's `dirty_before_bgsave`.

### moon#1240
- **Compaction:** it now runs through the shard's spill thread (new `reclaim_io`, a second channel pair). The spill thread reads the old file; the shard picks the survivors; the spill thread writes the new file durably; the shard records the pending compaction.
- **Adoption:** it is split across ticks. The shard lists the new files with the new `commit_acked`, whose ack is polled, not awaited. Only after that listing is durable does it re-point the survivors and unlink the old files, with a deferred tombstone commit.
- **The WS15 crash-window argument still holds** (re-checked in NOTES).

### moon#1241: redis 7.0.15 parity (noeviction, over maxmemory)

| Call | redis 7.0.15 | moon before | moon after |
|---|---|---|---|
| `EVAL` with `redis.call('DEL')` | `:1` | `-OOM` | `:1` |
| `EVAL` with `redis.pcall('UNLINK')` | `:1` | `-OOM` | `:1` |
| `EVAL` with `redis.call('SET')` | `-OOM` | `-OOM` | `-OOM` |
| `FCALL`, DEL function without flags | `-OOM` (whole call refused) | `-OOM` | `-OOM` |
| `FCALL`, DEL function with `allow-oom` | `:1` | `-OOM` | `:1` |

## Measurements: moon#1240 PING latency A/B
- **Setup:** native, on this 4-vCPU container, so it is relative evidence only. The workload is `perf_ws15_ledger_bound` plus a PING every 1 ms, with runs interleaved A B | B A | A B on a fresh server each time.
- **Binaries:** A = `5b14b82`, B = `db3490a`, both monoio release-fast.

| Run | Pings | p50 ms | p99 ms | p99.9 ms | Max ms | PINGs ≥5 ms | PINGs ≥10 ms |
|---|---|---|---|---|---|---|---|
| A1 | 28,184 | 0.127 | 1.759 | 15.991 | 66.3 | 82 | 37 |
| B1 | 28,361 | 0.135 | 1.085 | 4.381 | 41.1 | 24 | 10 |
| B2 | 28,536 | 0.128 | 1.181 | 4.969 | 1,498.9 | 28 | 13 |
| A2 | 27,287 | 0.139 | 1.392 | 5.179 | 1,261.9 | 30 | 14 |
| A3 | 28,210 | 0.133 | 1.408 | 14.635 | 36.3 | 58 | 34 |
| B3 | 28,856 | 0.117 | 0.940 | 4.991 | 40.2 | 28 | 12 |

- **Medians:** p99 fell 1.41 → 1.09 ms (−23%), p99.9 fell 14.6 → 5.0 ms, and PINGs ≥10 ms fell 34 → 12.
- **The 1.2–1.5 s outliers** appear on both binaries, so they are not reclaim I/O. They look like host noise or the fold serializing its base on the shard thread.
- **Reclaim still completes on both:** the same 40,000 of 80,000 SETs were refused over budget in every run.
- **The Linux-perf-host number is DEFERRED.**

## Cross-ownership edits
- `src/shard/event_loop.rs` (`c0c8059`): the two orphan-sweep call sites pass `aof_pool` and `spill_file_id`.
- `src/persistence/manifest.rs` and `manifest_sync.rs` (`8822d1d`): `ShardManifest::commit_acked` and `CommitAck`. `commit_durable` becomes `commit_acked` plus a blocking wait, so its behaviour is unchanged.
- `src/admin/metrics_setup/mod.rs` and `src/command/persistence.rs` (`7f9ca7a`): `mark_save_started` at the three save starts; `mark_save_completed` marks the start value.
- `src/scripting/functions.rs` (`db3490a`): the function's `allow-oom` flag sets the bypass.
- **Within owned areas, relevant to merges:**
  - `src/shard/persistence_tick.rs`: one extra argument to `cold_reclaim_tick::run`, and one test-mod line.
  - `src/main.rs`: auto-save wiring.
  - `src/scripting/bridge.rs`: the gate function and its call, a thread-local with a setter and resets, and tests. The COPY/MOVE region is untouched.

## Risks for integration
1. **FIX-MAINCI (moon#1253):** `apply_completion_vec` and `rehydrate_unpublished_spill` are untouched, so the settle calls apply unchanged. Reclaim results use their own channel, applied at the top of `cold_reclaim_tick::run`.
2. **Behaviour changes for the CHANGELOG:**
   - sharded `--save` users start getting snapshots;
   - writes made during a save stay counted;
   - a spill file below the latest cut stays on disk (and in `INFO cold_files_pending_unlink`) until the next committed fold plus a sweep;
   - held files ask the auto-rewrite monitor for a fold while the ledger is over the reclaim threshold.
3. **Deferred tombstone commit:** a crash between unlinking an old file and persisting its tombstone leaves a listed-but-missing file. Recovery counts it as `files_missing` with an error-level degraded rebuild. The orphan sweep has the same window. Phase 2 (`f00e9903`): the first sweep retires such an entry (no hold), so the alarm appears on one boot only.
4. **Shared-target aliasing** happened four times; each result was re-run until provenance was proven. WS16 changes the `perf_ws12_bgsave_split` and `perf_ws15_bgsave_status` expectations, so re-run both on the merged tree.
5. **File sizes:**
   - Already over the limit and grew: `manifest.rs` 2473 → 2494, `event_loop.rs` +4, `persistence_tick.rs` 3402 → 3406.
   - Brought under the cap: `cold_index.rs` 2049 → 1451 and `spill_thread.rs` 1483 → 833 (verbatim test moves).
   - `bridge.rs` is 1470.
6. **Residuals:**
   - the no-AOF path for moon#1231;
   - embedded auto-save;
   - `SCRIPT_WRITE_DIRTY` and `allow-oom` growing commands;
   - shebang EVAL;
   - the Linux-perf-host latency number.
7. **The new crash cases are `#[ignore]`d** (nightly crash-matrix only). The per-PR guards are the lib tests.

## Gates at `db3490a`
- **Lint and checks:**
  - fmt, audit-unsafe and audit-unwrap clean, with no new `unsafe`;
  - clippy `--all-targets` and tokio clippy clean;
  - tokio `check --all-targets` clean.
- **Lib tests** `storage:: persistence:: shard:: command:: scripting:: admin::`: monoio 3779 passed / 0 failed; tokio 3590 / 0.
- **Integration** (release binaries, both runtimes):
  - `perf_ws19_save_rules` 2/2, `perf_ws19_script_oom` 2/2;
  - `perf_ws15_ledger_bound` 2/2, `perf_ws15_spanning_cold_del` 2/2;
  - `perf_ws15_bgsave_status` 3/3, `perf_ws12_bgsave_split` 4/4;
  - `crash_recovery_cold_del_rewrite --ignored` 11/11 at s1 and s4.

## CHANGELOG bullets
- **Fixed (data loss, moon#1231):** a key could be lost at the next restart, or come back with the wrong value, when it was cold at an AOF rewrite, then read back into memory or read-modify-written, and the orphan sweep later removed its spill file.
  - A spill file that a replayable AOF generation may still read is now kept until a later rewrite has committed.
  - The cold-reclaim adoption no longer removes a compacted file whose survivors changed after the rewrite.
  - Reproduced on 143–179 of 200 keys before the fix.
- **Fixed (behaviour change, moon#1232):** `--save "<seconds> <changes>"` rules never fired in the sharded server. They now trigger on the change count INFO reports as `rdb_changes_since_last_save`.
  - Deployments that pass `--save` now get the periodic snapshots the rule describes.
  - A failed save is retried after 5 s.
  - Writes made while a save runs stay counted, as in redis.
- **Performance (moon#1240):** cold-tier reclaim no longer reads, writes or fsyncs spill files, or waits for manifest fsyncs, on the shard thread. PING p99 during cold-delete churn fell 23%, and p99.9 about 3×, in a same-host A/B.
- **Fixed (redis parity, moon#1241):** inside EVAL/EVALSHA, and in functions registered with `allow-oom`, commands that can only free memory (DEL, UNLINK, HDEL, LPOP, EXPIRE, …) are no longer refused with `-OOM` on an over-budget shard. Eviction still runs, and growing commands are still refused. A function without `allow-oom` is still refused, as in redis.

(Phase 1 committed by the orchestrator from the WS19 agent's final report, because the harness refused the agent's SUMMARY write.)

## Phase 2: review follow-ups (MERGE-AFTER-FIXES)

`origin/main` (7ddc0cb, PR #1242 incl. FIX-MAINCI moon#1253) merged as `f6237b84`, both test mods kept. Then one commit per item:

| # | Item | Verdict | Commit | Evidence |
|---|---|---|---|---|
| 1 | moon#1232 counted string writes only; DEL/EXPIRE of a missing key, active expiry, RENAME and cold GETs over-counted | **FIXED** | `a2fa8147` (cross-ownership, see below) | Integration table vs redis 7.0.15 (the reviewer's 43 rows + 16): 34 rows differ on `baseline-ae21476`, 0 on the fix; `dirty_count_oracle_redis_agrees` (ignored) runs the same table on redis-server 7.0.15: green. 200 cold GETs: +161 before, 0 after. Lib table (63 rows) in `command::keyspace_changes::tests`. A collection-only `--save "1 10"` at `--shards 4` now saves. |
| 2 | moon#1231 `every_output_listed` rule untested | **FIXED** (test) | `b796bf68` | `a_partially_discarded_compaction_keeps_the_old_file`: red with the rule reverted, while every other cold_reclaim test stays green. |
| 3 | `UnlinkHold::admit` O(N^2) | **FIXED** | `962aa2c2` | Debug: 20k 0.67 s, 80k 10.4 s (15.6x) before; `admitting_a_large_batch_is_not_quadratic` green. |
| 4 | Held files never released at `auto-aof-rewrite-percentage 0` | **FIXED** | `d9fb8bde` (touches `aof/auto_rewrite.rs`) | Held-file pressure has its own counter, answered whatever the percentage: `held_files_pressure_gets_a_fold_even_with_rewrites_disabled`, `held_files_pressure_is_signalled_apart_from_compactions`. |
| 5 | Output whose survivors all change during the ack never reclaimed | **FIXED** | `2867ec35` | The reviewer's test, extended: held while no covering fold committed, gone with its ledger entries after two. Red with the fix removed. |
| 6 | SHUTDOWN during an auto-save refused | **FIXED** (waits, then saves; does not abort) | `8680eacf` (both sharded handlers) | `shutdown_during_an_auto_save_is_not_refused`: red on the previous binary, green 3/3; a key written after the auto-save started survives the restart. |
| 7 | 5 s retry only for rules with secs <= 5 | **FIXED** | `55ce5d98` | `save_rule_due` takes seconds since the last success and since the last attempt; `a_failed_save_is_retried_after_5s_under_a_60s_rule`. |
| 8 | Deferred tombstone gives a DEGRADED false alarm every boot until a fold | **BOUNDED to one boot** (not tagged) | `f00e9903` | A queued file already gone from disk is retired at the first sweep, bypassing the hold: `a_listed_file_gone_from_disk_is_retired_at_the_first_sweep`, red through the hold. Tagging would need a new durable manifest record. |
| 9 | Bound the moon#1253 superseded set (FIX-MAINCI R2) | **FIXED** | `cd118031` (refs moon#1253) | Watermark as designed; FIFO re-checked (ids minted from the shard's one counter before each `try_send`; reclaim jobs on their own channel). `a_superseded_request_whose_completion_never_arrives_is_pruned_by_the_watermark` (red without the prune), plus two spill-thread tests. |

### Phase 2 cross-ownership edits (all inside the commit of their item)
- Item 1 (`a2fa8147`): `src/command/mod.rs` (52 dispatch arms wrapped, 4 zset stores), new `src/command/keyspace_changes.rs`, handlers `hll.rs`, `geo/geo_cmd.rs`, `list/list_write.rs` (LTRIM), `set/set_write.rs` (SMOVE), `sorted_set/sorted_set_write.rs` (ZADD tally), `keyspace/move_cmd.rs`, `server_admin.rs` (comment), `src/server/conn/blocking.rs` (`try_immediate_pop`), `src/blocking/wakeup.rs` (mute), `src/storage/eviction.rs` (mute), `src/storage/db/kv_ops.rs` (funnels), `src/admin/metrics_setup/mod.rs` (mute + explicit count), `src/shard/persistence_tick.rs` (rehydrate mute).
- Item 4 (`d9fb8bde`): `src/persistence/aof/auto_rewrite.rs` (`reclaim_due` signature).
- Item 6 (`8680eacf`): `src/server/conn/handler_monoio/dispatch.rs`, `src/server/conn/handler_sharded/dispatch.rs`, `src/command/persistence.rs`.
- Item 9 (`cd118031`): `src/storage/db/mod.rs` (prune / clear), `src/shard/persistence_tick.rs` (`drain_and_apply`).

### For SUMMARY only (per the orchestrator)
- **Pre-existing, moon#1231 with `--appendonly no`:** without an AOF writer there is no hold, so the reviewer's no-AOF scenario (a snapshot, then GET-promotions, then the sweep, then a crash) still loses promoted keys. Filed separately by the orchestrator.
- **Unmeasured risk:** reclaim jobs are polled before every spill `recv_timeout` on the same thread, so a burst of reclaim reads/writes can delay a deep spill backlog (bounded: 2 jobs started per tick, 8 in flight per shard).
- **Unmeasured risk:** a dead spill thread leaves `compactions_in_flight` stuck at 8 (their jobs never answer), which stops new compactions for the shard; item 9 clears the superseded sets in that case but not the reclaim's in-flight set.

### Phase 2 gates at `cd118031`
- fmt, `audit-unsafe` (no new `unsafe`), `audit-unwrap` (within baseline), `audit-test-tempdirs`, `audit-encoding-limits`: clean.
- `cargo clippy --all-targets -- -D warnings` on monoio and on tokio (`--no-default-features --features runtime-tokio,jemalloc`): clean. Tokio `cargo check --all-targets`: clean.
- Full lib, no filter: monoio 6659 passed / 0 failed / 14 ignored; tokio 5721 / 0 / 13 (own binaries: this branch's new test names present).
- Integration, debug server binaries of `cd118031` pinned by `MOON_BIN` (monoio and tokio; the phase-1 release-build budget was spent), test binaries rebuilt from this tree and copied (names checked against the sources), all green on both runtimes:
  - `perf_ws19_save_rules` 7/7 including the ignored redis-server oracle; `perf_ws19_script_oom` 2/2;
  - `perf_ws15_ledger_bound` 2/2, `perf_ws15_spanning_cold_del` 2/2;
  - `perf_ws15_bgsave_status` 3/3, `perf_ws12_bgsave_split` 4/4;
  - `crash_recovery_cold_del_rewrite --ignored` 11/11 at `--shards 4` and at `--shards 1`;
  - `blocking_exec_wakeup` 3/3, `blocking_ready_key_wake` 8/8, `blocking_stream_read` 14/14, `keyspace_event_db_index` 4/4.

### Phase 2 CHANGELOG bullets
- **Fixed (redis parity, moon#1232):** `rdb_changes_since_last_save`, which the `--save` trigger reads, now counts what redis 7.0.15's `dirty` counts: collection writes by their redis rule (HSET field-value pairs, LPUSH elements, SADD members added, ZADD added plus rescored, pops the elements popped, …), and no longer counts a DEL or EXPIRE of a missing key, key expiry, eviction, or a read that brings a cold key back into memory. RENAME counts 1 and FLUSHDB the keys it removed.
- **Fixed (moon#1232):** `SHUTDOWN` (with save points, or `SHUTDOWN SAVE`) during a running auto-save or BGSAVE no longer fails with "Background save already in progress": it waits for that save and then saves.
- **Fixed (moon#1232):** after a failed save a `--save` rule retries after 5 s whatever its seconds; a rule's seconds run from the last successful save.
- **Fixed (moon#1231):** with `auto-aof-rewrite-percentage 0`, spill files held for a replayable AOF generation still get the rewrite that releases them when their ledger keeps write admission over budget.

### Phase 2 residuals and notes
- Item 1 known differences (documented in `command::keyspace_changes`): consumer creation by XREADGROUP/XCLAIM/XAUTOCLAIM not counted; SORT STORE counts 1; ZINCRBY of a new member by 0 counts 0; XGROUP DELCONSUMER of a missing consumer counts 1; SWAPDB 0; SETBIT of an unchanged bit 1. At `--shards 4` the cross-shard multi-key writes that moon accepts (MSET, DEL, UNLINK, MSETNX, COPY, FLUSHALL) match redis, and hash-tagged RENAME / SMOVE / ZUNIONSTORE match; the rest are refused with CROSSSLOT.
- Item 6 waits for a running save instead of killing it: a SHUTDOWN during a long save may take up to 20 s, one overall deadline (`SHUTDOWN_SAVE_DEADLINE_MS`; review 5 N1 — it was 4 x 10 s as first committed).
- `src/storage/db/kv_ops.rs` was 1510 lines on main after the merge and is 1537 now (over the 1500 cap before this phase).
- Disk: one errant `cargo build --tests` in this phase filled the shared disk for ~2 minutes (6.3 GB of test binaries); the binaries from that build window were deleted and the disk returned to 5.9 GB free.

## Review 5 (merged part-4 tree 340cfce1): MERGE-AFTER-FIXES

`origin/claude/confident-cerf-3n093x` (1421630b) merged as `1d839fb2`, no conflicts. One commit per item, red then green:

| Item | Verdict | Commit | Red -> green |
|---|---|---|---|
| SPLIT `kv_ops.rs` 1545 lines | Pure move into `cold_promote.rs`, `keyspace_scan.rs`, `bulk_load.rs`; kv_ops 772 | `b167d1e6` | moved lines == removed lines (sorted multiset); check both runtimes, storage::db lib tests both runtimes |
| S1 transiently missing held file tombstoned (data loss) | **FIXED** | `df050595` | reviewer proof (adopted in `promote_sweep_tests`): `(None, Some("x"), false, false)` -> `(Some("v08"), Some("v09x"), true, true)`; f00e9903's test green |
| S2 blocking pop served at remote registration counted 0 | **FIXED** | `96879a03` | s4 16 keys: `00 00 10 01 11 10 …` -> `11` x 16; lib test red 0 -> 1 |
| S3 XREADGROUP BLOCK served at once counted 0 | **FIXED** (new consumer still +1 short, listed) | `dafa7ad8` | table row 0 -> 1 (redis 1); proof 0 -> 1 |
| S4 SWAPDB counted 0 | **FIXED** | `f723750d` | delta 0 -> 1; after kill -9 the key was back in db 0 -> in db 1 (3/3) |
| S5 snapshot boot / replica full sync counted every key | **FIXED** (AOF boot unchanged, at parity, now pinned) | `67bbecac` | boot 1000 -> 0; full sync 1002 -> 0; lib 2 -> 0 |
| N1 SHUTDOWN bound 4 x 10 s | **FIXED**: one 20 s deadline | `9ecd604f` | "gave up after 543ms, one deadline is 300ms" -> within 1.5x |
| N2 `is_dead()` sampled after the drain | **FIXED** (refs moon#1253) | `6702b7cd` | contract test (no deterministic red: two-statement race) |
| N3 active hash-field expiry counted | **FIXED** | `060033be` | 3 -> 0 |
| N4 `awaits_fold` O(held) per tick | **FIXED**: O(1) `max_stamp` | `b9d475a0` | oracle-vs-walk test; red with `take`'s recompute removed |
| N5 missing known differences | geo stores and `RENAME k k` **FIXED**; PFCOUNT, XREADGROUP history read, SETBIT **listed** | `be2771ae` | RENAME k k 1 -> 0, GEOSEARCHSTORE / GEORADIUS STORE 1 -> 2 (redis 2) |
| N6 spill-thread death | **Mitigated** (refs moon#1265): in-flight compactions abandoned, one error line, INFO `spill_thread_alive` | `b78b38b3` | in-flight pinned 1 -> 0 |

### Review 5 cross-ownership edits
- S2: `src/blocking/{wakeup,group,stream_wake}.rs`, `src/shard/spsc_handler.rs` (the `BlockRegister` arm).
- S3: `src/server/conn/blocking.rs` (`stream_read_immediate`).
- S4: `src/server/conn/handler_monoio/dispatch.rs`, `handler_sharded/dispatch.rs`, `handler_single.rs`.
- S5: `src/persistence/snapshot.rs`, `src/persistence/redis_rdb.rs`, `src/replication/apply.rs`.
- N3: `src/server/expiration.rs`. N5: `src/command/geo/geo_cmd.rs`, `src/command/mod.rs` (RENAME arm). N6: `src/command/connection.rs` (INFO).
- Files already over the 1500-line cap that grew: `blocking/wakeup.rs` 2336 -> 2390, `redis_rdb.rs` +19, `apply.rs` +7, `server/conn/blocking.rs` +7, `handler_monoio/dispatch.rs` +6, `handler_single.rs` +4, `persistence_tick.rs` +6, `spsc_handler.rs` +9, `connection.rs` +4, `storage/db/mod.rs` +3. `cold_index.rs` stays under 1500.

### Review 5 observations (not changed here)
- With `--appendonly no` and no `--save`, a server does not load its per-shard snapshot at boot (`persistence_dir` exists only for AOF or save points).
- Under `appendfsync everysec`, SETs acknowledged immediately before a kill -9 of the process were missing after restart (5 of 105; `always` loses none). redis writes the AOF before it replies, so a process crash loses nothing there. For the AOF writer's owners.
- Still not counted like redis (documented in `command::keyspace_changes`): consumer creation by XREADGROUP/XCLAIM/XAUTOCLAIM; XREADGROUP history read of an empty PEL; SORT STORE (1 vs the stored length); ZINCRBY of a new member by 0; XGROUP DELCONSUMER of a missing consumer; SETBIT of an unchanged bit; PFCOUNT's cache rewrite.

### Review 5 gates (at `b78b38b3`, plus the test gate `2e09e831`)
- fmt, `audit-unsafe` (no new `unsafe`), `audit-unwrap` (within baseline), `audit-test-tempdirs`, `audit-encoding-limits`: clean.
- `cargo clippy --all-targets -- -D warnings` on monoio and on tokio: clean.
- Full lib, no filter: monoio 6693 passed / 0 failed / 14 ignored; tokio 5755 / 0 / 13 (this branch's new tests present in both).
- Integration, debug server binaries of `b78b38b3` pinned by `MOON_BIN`, test binaries rebuilt from this tree (names checked):
  - monoio, all green: `perf_ws19_save_rules` 13/13 with the redis-server 7.0.15 oracle; `perf_ws19_script_oom` 2/2; `perf_ws15_ledger_bound` 2/2; `perf_ws15_spanning_cold_del` 2/2; `perf_ws12_bgsave_split` 5/5; `perf_ws15_bgsave_status` 3/3; `perf_ws16_bgsave_capture` 7/7; `crash_recovery_cold_del_rewrite --ignored` 11/11 at s4 and s1; `crash_recovery_cold_del_inflight_1253 --ignored` 3/3 at s4 and s1; `blocking_exec_wakeup` 3/3, `blocking_ready_key_wake` 8/8, `blocking_stream_read` 14/14, `keyspace_event_db_index` 4/4.
  - tokio: the same list green. With monoio-built test binaries, the two full-resync tests failed against the tokio master (it answers no PSYNC); rebuilt with tokio features, `perf_ws12_bgsave_split` is 4/4 + 1 ignored and `perf_ws19_save_rules` 11/11 + 2 ignored (the oracle, run separately: green; the replica test, gated by `2e09e831`).
  - The adopted proofs: S1 in the lib run (`promote_sweep_tests`); S2–S5 in `perf_ws19_save_rules` above.

## WS19 CHANGELOG-ready bullets (all phases, supersedes the lists above)

### Fixed
- **Data loss (moon#1231):** a key could be lost at the next restart, or come back with the wrong value, when it was cold at an AOF rewrite, was then read back into memory or read-modify-written, and the orphan sweep later removed its spill file (reproduced on 143–179 of 200 keys). A spill file that a replayable AOF generation may still read is now kept until a later rewrite has committed, also when it is only briefly unreachable while the sweep runs; the cold-reclaim adoption no longer removes a compacted file whose survivors changed after the rewrite.
- **`--save` rules (moon#1232):** `--save "<seconds> <changes>"` never fired in the sharded server. Rules now trigger on `rdb_changes_since_last_save`, time from the last successful save, and retry a failed save after 5 s whatever their seconds.
- **Change counting, redis 7.0.15 parity (moon#1232):** `rdb_changes_since_last_save` now counts what redis counts — collection writes by their redis rule (HSET field-value pairs, LPUSH elements, SADD members added, ZADD added plus rescored, pops the elements popped, geo stores the members stored, …), blocking commands served at once as their non-blocking twins (also at `--shards` > 1), SWAPDB as one change (also when a replica applies it or the AOF replays it), and writes made while a save runs. It no longer counts a DEL or EXPIRE of a missing key, key or hash-field expiry, eviction, a read that brings a cold key back into memory, `RENAME k k`, booting from a snapshot, or a replica's full sync (which made `--save "3 100"` rewrite the whole snapshot 3 s after every boot).
- **SHUTDOWN (moon#1232):** `SHUTDOWN` with save points, or `SHUTDOWN SAVE`, during a running auto-save or BGSAVE no longer fails with "Background save already in progress": it waits for that save and then saves, within one 20 s deadline.
- **Scripts over maxmemory (moon#1241):** inside EVAL/EVALSHA, commands that can only free memory (DEL, UNLINK, HDEL, LPOP, EXPIRE, …) are no longer refused with `-OOM`, and a function registered with `allow-oom` runs any command past `maxmemory`, as in redis 7.0.15 (moon's per-database `--db-maxmemory` quota, which redis does not have, still refuses its growing writes). Growing commands in EVAL, and functions without `allow-oom`, are still refused, as in redis.
- **Cold tier housekeeping (moon#1231, moon#1240, refs moon#1253):** held spill files get the AOF rewrite that releases them even with `auto-aof-rewrite-percentage 0`; a compacted output whose keys all changed while its listing was committing is reclaimed instead of staying on disk until a restart; a listed spill file found missing at boot is retired by the first sweep, so the "cold index rebuild DEGRADED" alarm appears on one boot, not every boot until a rewrite; the set of in-flight spills retired by a write is bounded even when a completion never arrives.

### Changed
- **Performance (moon#1240):** the cold-tier reclaim no longer reads, writes or fsyncs spill files, or waits for manifest fsyncs, on the shard thread (same-host A/B: PING p99 during cold-delete churn -23%, p99.9 about 3x lower). Holding the spill files of a large cold tier after a FLUSHALL no longer costs O(N^2) on the shard thread, and the per-tick "does a held file need a rewrite" check is O(1).
- **Behaviour:** deployments that pass `--save` to the sharded server now get the periodic snapshots the rules describe. A spill file below the latest AOF rewrite's cut stays on disk (and in `INFO cold_files_pending_unlink`) until the next committed rewrite and sweep; while such files keep the dead-slot ledger over its threshold they ask the auto-rewrite monitor for a rewrite.
- **INFO (refs moon#1265):** new `spill_thread_alive` (0 once any shard's spill thread was found dead). A dead spill thread is logged once at `error`, and its in-flight compactions are abandoned so they no longer block the shard's reclaim; the thread is not yet respawned.

## PR #1268 review (CodeRabbit) and REVIEW7

PR head `b18c2e32` merged as `b673f52d`. One commit per item:

| Item | Verdict | Commit | Red -> green |
|---|---|---|---|
| F1 (CodeRabbit Major): TopLevel `overflow_for` maps every shard to `overflow[0]`; a TopLevel fold covers shard 0 only | **Not reachable in any shipped configuration**; guarded locally anyway (hold, never release, in that layout) | `359b20b7` | `a_top_level_fold_releases_nothing_on_another_shard`: "shard 0's fold released shard 1's file 5" -> held, on disk, listed |
| F2 (CodeRabbit Minor): `handle_save` loads `SAVE_IN_PROGRESS` without claiming it | **Fixed** (legacy single-listener path only; sharded SAVE is refused and every BGSAVE start already claims with `swap`) | `3c849adb` | "the SAVE ran without claiming SAVE_IN_PROGRESS" -> green |
| R1: SWAPDB counted only on client paths | **Fixed** (replica apply, AOF replay; the graph-WAL scan into throwaway dbs muted) | `1f84b537` | replica (1, 0) -> (1, 1); AOF boot 2 -> 3 (redis 3) |
| R2: `allow-oom` function refused growing commands | **Fixed**: `ScriptOomMode::{Compat, AllowOom, Deny}` | `8adc17a9` | FCALL allow-oom SET -OOM -> +OK; oracle test vs redis-server 7.0.15 green |
| R3: wall-clock bounds in tests | **Fixed**: conditions / virtual clock | `ea6c0870` | the N1 test red on the per-wait bug: "36s (virtual), one deadline is 20s" |

F1, what a TopLevel fold covers: the TopLevel writer's fold sends `AofFold` to shard 0 only and snapshots shard 0's databases (`do_rewrite_sharded` / `rewrite_aof_sharded_sync`). main.rs never builds a TopLevel pool with more than one shard: it refuses a TopLevel manifest at `--shards >= 2` (exit 2, both runtimes; `aof_toplevel_multishard_refusal` green on both) and builds the PerShard pool for a manifest-less multi-shard boot. With one shard, `overflow[0]` is that shard's own and the fold covers the whole keyspace. The embedded server (tokio) does build a TopLevel pool at any shard count, but its writer has no fold channels, so every rewrite aborts ("fold channels not wired") and `committed_floor` stays `INITIAL` (0), below which no stamp can fall: nothing is ever released. The guard makes this local: in that layout the sweep's view carries a committed floor of `INITIAL`, so a held file is never released and never unlinked — it can leak disk, never lose a key.

Gates at `ea6c0870`: fmt, audits, `clippy --all-targets -D warnings` on both runtimes clean; full lib monoio 6704/0, tokio 5766/0; on both runtimes `perf_ws19_save_rules` (monoio 14 + oracle, tokio 12 with the full-sync tests ignored), `perf_ws19_script_oom` 2/2 + its redis oracle, `aof_toplevel_multishard_refusal --ignored` 2/2, `crash_recovery_cold_del_rewrite --ignored` 11/11 at s4 and s1, `crash_recovery_cold_del_inflight_1253 --ignored` 3/3 at s4 and s1.

## PR #1268 re-review (CodeRabbit at 14c27f1e)

PR head merged as `25bc882e`.

| Item | Verdict | Commit | Red -> green |
|---|---|---|---|
| G1 (Major): `AllowOom` bypassed the per-db quota too | **Fixed**: allow-oom bypasses maxmemory only; the quota refuses growth and, as everywhere, never a shrink-only command | `18d4f310` | `an_allow_oom_function_does_not_write_past_a_db_quota`: allow-oom SET over db 1's quota +OK -> "db maxmemory exceeded"; allow-oom DEL and EVAL DEL still :1 |
| G2 (Minor): a save ending in the poll past the deadline lets the wait succeed | **Fixed for the harmful half**: no save starts past the deadline. **Kept** for SHUTDOWN's own save that completes in that poll (durable; bound = deadline + 5 ms poll) | `7cdc50f9` | `no_shutdown_save_starts_after_the_deadline`: "SHUTDOWN started its own save after its deadline had passed" -> timed out, nothing started; `a_shutdown_save_that_completes_at_the_deadline_counts` pins the kept half |

Gates at `7cdc50f9`: fmt and `clippy --all-targets -D warnings` on both runtimes clean; lib `scripting::`, `command::persistence`, `storage::db_quota` on both runtimes green (124 / 123); `perf_ws19_script_oom` 3/3 on both + its redis-server 7.0.15 oracle; `perf_ws19_save_rules` monoio 14/14, tokio 12/12 (the two full-sync tests need a monoio master) + its oracle.
