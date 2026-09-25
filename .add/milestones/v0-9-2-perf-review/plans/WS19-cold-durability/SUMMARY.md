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
- Item 6 waits for a running save instead of killing it: a SHUTDOWN during a long save may take up to two save bounds (2 x 10 s).
- `src/storage/db/kv_ops.rs` was 1510 lines on main after the merge and is 1537 now (over the 1500 cap before this phase).
- Disk: one errant `cargo build --tests` in this phase filled the shared disk for ~2 minutes (6.3 GB of test binaries); the binaries from that build window were deleted and the disk returned to 5.9 GB free.
