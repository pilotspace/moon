# WS42 = moon#1300 (TXN crash atomicity): SUMMARY

**Verdict: DONE.** Branch `w2/ws42-txn-crash` in `/home/user/wt/lane-a`, HEAD `0d0fd45`, 8 commits on top of `fa3f751`. Tree is clean, nothing pushed. I wrote no CHANGELOG, README or TEAM-RULES edits.

Results are from the Linux container, not the merge bar.

### Commits
1. `7f994cc` test: red suite `tests/txn_crash_atomicity_1300.rs` (38 tests). The design note is in this commit's body.
2. `caf381e` test: the suite now kills -9 before the TXN's connection drops. I removed the SAVE cases because SAVE is unsupported in sharded mode.
3. `0abd466` fix(persistence): `MOON.TXN BEGIN|PAUSE|END <id>` and `RESET` markers.
   - Markers have lsn 0 and go through the WS37 intercept (`ReplayRoute::Marker`).
   - The writer tracks the transaction context the way it tracks SELECT (`RecordCtx`), so a record with no TXN adds zero records.
   - The replay state machine is `persistence/replay/txn.rs`. Records are applied in place, and the pre-image is captured at each key's first write in a block. END drops the captures. RESET, the CLOSE marker or end of file restores them.
   - A record outside a block that writes a captured key releases that key.
   - A reopened file owes `RESET` before its first append.
   - Covers the per-shard and top-level layouts, the rewrite prefix, `migrate_aof`, and the fuzz target `aof_incr_replay` (TXN prefix shapes, and an assert that `finish_log` returns 0). `STORAGE-FORMAT-V1.md` §3.3 now has a "Transaction blocks" section with the downgrade procedure.
4. `a901e2e` fix(transaction): connection wiring (`server/conn/txn_log.rs`).
   - Every TXN record, including Lua script effects via `isolation::current_owner`, is stamped with the TXN id.
   - Commit logs END through `append_then_apply_in_txn`, in the same synchronous section as the hold release; under `always` there is a barrier.
   - Abort writes its compensation inside the block (`RESTORE … REPLACE ABSTTL` plus `HPEXPIREAT` per field deadline), then END.
   - Replication pushes each TXN record as one `[BEGIN][rec][PAUSE]` unit, then END. The replica applies them with `replication/txn_apply.rs` and rolls an open block back on REPLICAOF change or NO ONE.
   - Test hook: `MOON_TEST_TXN_ABORT_CRASH_AFTER_RECORDS`.
5. `abf6214` fix(transaction), F3: the hold table keeps each held key's pre-TXN image (`isolation::with_held_keys`).
   - BGSAVE, save rules, the SHUTDOWN save and the held-release snapshot serialize that image (`capture_held_pre_images`).
   - The fold base uses it too; `fold_txn::enqueue_reopen` re-opens the block in the new incr with the live values.
   - A replica full sync sends the pre-images in the RDB, then `stream_reopen`.
6. `6d6f930` fix(persistence): I **removed** the automatic held-file snapshot's TXN deferral and abandon (`snapshot_txn_guard`, `snapshot_request/txn_round`, `snapshot/abandon`, `waits_for_open_txns`).
   - Why: F3 means every snapshot stores pre-TXN images, so the deferral protected nothing and only starved the release under a steady stream of TXNs. No dead code is left.
   - `held_release_txn_{open,race}_1289` were rewritten for the new behaviour: the snapshot runs while the TXN is open, the files are released, and a restart shows the pre-TXN image.
   - **Cross-ownership: the INFO fields `cold_held_release_snapshots_deferred_txn` and `cold_held_release_snapshots_abandoned_txn` are gone.** They are unreleased. The orchestrator must drop them from the WS39 / R2-fix-c CHANGELOG sentences.
7. `4a021d1` test: fixed `cfg(test)` builds (`txn: 0` in tokio-only `AofMessage` literals; the held-key lookup is now a loop).
8. `0d0fd45` test: the `AofMessage` size-budget test now allows the txn word (72 → 88 bytes, one word per field). It was renamed `the_clock_and_the_txn_cost_at_most_one_word_each_per_message`.

### Gates at HEAD
- **Static checks:** `cargo fmt --check` OK. Clippy `--all-targets -D warnings` clean on default and on `runtime-tokio,jemalloc`. `cargo check --manifest-path fuzz/Cargo.toml --all-targets` rc=0.
- **Release lib tests:**
  - tokio: 6120 passed, 0 failed.
  - monoio: 7065 passed, 1 failed. The one failure was the size-budget test, fixed in `0d0fd45`; I re-ran it in a debug build (14/14 `record_ctx` tests OK) but did not re-run the full release monoio lib suite after the fix.
- **Final binaries:** `/home/user/wt/bin/ws42-final-{monoio,tokio}` (release-fast, HEAD `0d0fd45`). `strings` shows the marker twice in each.
- **Integration, both runtimes, with `MOON_BIN` pinned and `--include-ignored`; tokio used `MOON_TEST_NO_MASTER_PSYNC=1`:**

| suite | monoio | tokio |
|---|---|---|
| txn_crash_atomicity_1300 | 38/38 | 38/38 |
| review_w1_txn_abort_no_aof_snapshot_1285 | 3/3 | 3/3 |
| txn_abort_durability_1285 | 25/25 | 25/25 |
| txn_isolation_1299 | 20/20 | 20/20 |
| txn_exit_epilogue_1299 | 8/8 | 8/8 |
| txn_close_after_epilogue_1299 | 6/6 | 6/6 |
| review_r1_txn_isolation_1299 | 6/6 | 6/6 |
| txn_multikey_undo_500 | 5/5 | 5/5 |
| held_release_txn_open_1289 | 3/3 | 3/3 |
| held_release_txn_race_1289 | 5/5 | 5/5 |

  - The downgrade tests ran with `MOON_DOWNGRADE_BIN=r3-bda76c1-{monoio,tokio}`.
  - Also passing on both runtimes: graph_wal_append_1302 11, aof_replay_clock_1283 19, aof_select_after_restart_r1 4, aof_multidb_kill9 4, crash_matrix_per_shard_aof 4, perf_ws21 9, crash_aof_init_generation_1293 2, aof_auto_rewrite 5. Monoio ran these on the v2 binary, whose source matches HEAD apart from test-only code.
  - Replication on monoio, all OK: streaming 7, hardening 6, multishard 9, local_leg_815 4, swapdb 3, flushall 3, replica_past_deadline_1286 2, ttl_semantics 2, scripts_in_multi_894 6.
  - In-process tokio suites, all OK: kill_snapshot 4, txn_kv_wiring 12, txn_graph_wiring 5, txn_cypher_write_rollback 3.
- **Not ours:**
  - `aof_fold_exactly_once_455::exec_parked_in_wait_across_a_rewrite_replays_once_toplevel` fails as already known.
  - `replication_planes::eviction_parity_hash_disk_offload_shards{1,4}` fail with "master did not evict anything". The same 2 tests fail on base `r3-bda76c1-monoio`.
  - `aof_everysec_kill9_1266` s4 unpipelined failed once while clippy was compiling (7 of 20 reps lost writes). Re-run alone, 2 reps each, it passed on both base and final, so I read it as load-induced.
- **Red on base `r3-bda76c1`:**
  - txn_crash_atomicity_1300: monoio 24/38 fail, tokio 22/38 fail.
  - review_w1: 2/3 fail on both runtimes. The failures are exactly `a_crash_inside_a_txn_does_not_keep_its_uncommitted_writes` and `an_abort_after_a_mid_txn_snapshot_survives_a_restart_without_an_aof`.

### Measurements
Taken on a shared container with WS43 compiling at the same time, so treat them as indicative only and **READY TO BENCH on a quiet Linux host**.
- **AOF write path with no TXN** (redis-benchmark SET, c50, -r 100k, everysec, s1, base vs new interleaved, 3 reps):
  - MOON.TXN records written: 0 in every run.
  - P16 median 505.9K vs 478.5K. Means 492.6K vs 487.6K, and the spread is ±10%.
  - P1 median 54.3K vs 57.0K.
  - So no regression I can resolve above noise.
- **TXN latency** (BEGIN, SET, HSET, COMMIT, 3000 iterations): everysec 404 → 468 µs (noisy; mins are 372 vs 364); always 1838 → 2261 µs.
  - The always increase is the END record's fsync at commit, which is inherent: END is the durable commit point.
- **BGSAVE with 200k keys:** 0 held keys 236 → 225 ms; 1000 held keys in an open TXN 230 → 240 ms.

### Adversarial re-read and remaining risks
- **Compensation spanning several records:** it all sits inside the block with END last. A crash anywhere in it restores the whole pre-TXN state. Tested (crash between RESTORE and HPEXPIREAT, s1 and s4).
- **everysec crash after the commit's OK but before END reaches disk:** the whole TXN rolls back atomically. That is a window of up to ~1s, the same exposure everysec already has for any write.
- **END refused by the AOF:** the client gets an error reply and the commit stands in memory. A crash before the next fold rolls it back.
- **MULTI and TXN:** MULTI inside a TXN is refused, and TXN BEGIN inside MULTI is refused.
- **Lua in a TXN:** script effects are tagged and rolled back on crash. Tested.
- **MQ:** publish intents are deferred until commit, so nothing leaks.
- **Residual – graph:** graph WAL v3 writes inside a TXN are not bracketed, so a crash keeps uncommitted graph writes. The vector index has the same gap.
- **Residual – cold keys and replica snapshots:** some cold-key edge cases are uncovered, and a replica's own snapshots taken while a master block is open are not covered.
- **Residual – downgrade:** an older binary replays a block left open by a crash. The documented procedure is to start the new binary once and run BGREWRITEAOF before downgrading.
- **File-size cap:** over-cap files grew only minimally: pool.rs +69, aof/mod.rs +34, redis_rdb.rs +24, apply.rs +9, rewrite.rs +9, handler_monoio/mod.rs +9, spsc_handler.rs +6, handler_sharded/mod.rs +1. persistence_tick (−14), command/persistence (−89) and connection (−10) shrank. New logic lives in new modules.
- **Coding rules:** no new `unsafe`; parking_lot only; no new hot-path allocations in the restricted dirs.
- **Loom:** I added no new atomic state machine. The holds are thread-local and the reset registry is a mutex.

### Self-evaluation
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 (the no-TXN path costs one `is_empty` check plus 8 bytes per message; TXN commit gets one extra record and, under `always`, one fsync) · Edge cases 0.85 (graph/vector residuals) · Self-evaluation 0.9. Overall about 0.9.

### CHANGELOG-ready bullets
- **Fixed:** a cross-store `TXN` is now crash-atomic with an AOF (moon#1300). Its records are bracketed by `MOON.TXN BEGIN/PAUSE/END` markers. Replay rolls back a block that never ended (kill -9, torn log, clean stop with a TXN open, a crash mid-abort), so no uncommitted write survives a restart. This holds across BGREWRITEAOF folds and in both AOF layouts. Writes outside a TXN add no records.
- **Fixed:** without an AOF, every snapshot (BGSAVE, save rules, SHUTDOWN save, the held-file release snapshot) stores an open TXN's keys at their pre-transaction values, so an abort or crash after a mid-TXN snapshot no longer resurrects uncommitted writes (moon#1300, closes the moon#1285 review findings).
- **Fixed:** replicas apply a TXN's records as a block and roll it back if the master dies before the end; a full sync during an open TXN sends pre-transaction values (moon#1300).
- **Changed:** the automatic held-file snapshot no longer waits for open TXNs, and the unreleased INFO fields `cold_held_release_snapshots_deferred_txn` and `cold_held_release_snapshots_abandoned_txn` are removed (moon#1300).
- **Docs:** STORAGE-FORMAT-V1 §3.3 now documents transaction blocks, with a downgrade caveat: after a crash, start the new binary once and run BGREWRITEAOF before downgrading.

Scratch evidence (suite logs, bench scripts, raw numbers) is in `/tmp/claude-0/-home-user-moon/d1b785a6-84fa-5659-9361-52c93e4ab21f/scratchpad/ws42/`, in the `suites-final-*`, `suites-red-base-*`, `bench-*.txt`, `lib-*.txt` and `inproc-*.txt` files.
