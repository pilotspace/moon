# WS19-cold-durability — SUMMARY

- **Base:** `016ca5a` (int/part3b `4115798` plus the part-4 plan).
- **Design reasoning:** NOTES.md holds the mechanisms, the crash windows, the A/B data and the gate records.
- **Binaries:** release-fast monoio and tokio builds of `db3490a`. Their provenance was checked by a branch-only log string. They were deleted at the end; rebuild from the integrated tree.
- **Red runs:** on `baseline-ae21476`, and on the part-3b reviewers' `5b14b82` binary (code identical to `016ca5a`).

## Per-issue verdict

| Issue | Verdict | Commits | Evidence | Follow-ups |
|---|---|---|---|---|
| moon#1231: a promoted cold key is lost once the sweep unlinks its file | **FIXED** | `22c516b` (test move), `c0c8059` (cross-ownership), `478ab3e` | In-process red: `(k08, k09)` recovers as `(None, Some("x"))`. Real-server kill -9 red on both old binaries at `--shards 1` and 4 (table below). Green on both runtimes at both shard counts. | The no-AOF WAL/snapshot recovery path is not analysed; its behaviour is unchanged. With `auto-aof-rewrite-percentage 0`, held files wait for a manual BGREWRITEAOF. |
| moon#1232: sharded `--save` never fires | **FIXED** | `a3247ad`, `7f9ca7a` (cross-ownership) | `perf_ws19_save_rules` red on both old binaries at s1 and s4: "ten changes with --save \"1 10\" produced no snapshot in 15 s". Green on both runtimes. Unit tests: `a_rule_fires_on_its_change_count_and_its_time`, `a_failed_save_is_retried_after_the_redis_delay`, `changes_made_while_a_save_runs_stay_counted`. | Embedded mode starts no auto-save (`server/embedded.rs`). The rule's clock starts at the last auto-save, not at `lastsave`. |
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
3. **Deferred tombstone commit:** a crash between unlinking an old file and persisting its tombstone leaves a listed-but-missing file. Recovery counts it as `files_missing` with an error-level degraded rebuild. The orphan sweep has the same window.
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

(Committed by the orchestrator from the WS19 agent's final report, because the harness refused the agent's SUMMARY write.)
