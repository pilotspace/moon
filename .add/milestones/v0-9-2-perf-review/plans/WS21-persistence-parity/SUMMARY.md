# WS21-persistence-parity — SUMMARY

(Committed by the orchestrator from the agent's final report: the harness refused the agent's own write of this file. NOTES.md, committed by the agent as `a52ef0c7`, has the design reasoning, raw numbers and gate records.)

Branch `perf/ws21-persistence-parity`, base `4a96cd5f` + plan `b230d011`. Personas: storage-durability-engineer (lead), ci-test-integrity-engineer.

- **Red runs:** against `main-4a96cd5f-rel` and `baseline-ae21476`. Where the base binary lacks a test hook, against this tree with only that fix reverted.
- **Green runs:** debug builds of this tree, monoio (built by cargo) and tokio (`MOON_BIN`, provenance checked).
- **Audits:** no new `unsafe`; the unwrap baseline is unchanged.

## Per-issue verdict
| issue | verdict | commits | evidence (red → green) | follow-ups |
|---|---|---|---|---|
| moon#1271 | **FIXED** | `a54541c5`, `a378418e` (doc, cross-ownership) | **Unit:** `aof_manifest::orphans::tests::a_manifest_load_leaves_a_rewrite_in_flight_alone` is red with the old sweeping load: "the load deleted shard 0's temp base".<br>**Real server:** `perf_ws21_aof_writer_start` (`--shards 4`, shard 1's writer held by `MOON_TEST_AOF_WRITER_HOLD`) is red with only the fix reverted: "the committed seq 2 manifest names shard-0/moon.aof.2.base.rdb, which is gone". A restart of that dir: DBSIZE 35 of 200.<br>Green on monoio and tokio.<br>The hook is absent on the base binaries; the load and sweep code is byte-identical there. | — |
| moon#1257 | **FIXED** | `e87f99db` | **Unit:** `persistence::snapshot::eviction_capture_tests` (5 tests), red with the capture removed: `Divergence { missing: 890 }` for allkeys-random to half the budget; 584 for allkeys-lru; 885 after SWAPDB; 725 for a failed async spill.<br>**Real server:** `perf_ws21_eviction_bgsave` (save held, maxmemory halved) is red on 4a96cd5f: "9952 of the 9952 keys evicted during the save are missing" at `--shards 1`, 13404/13404 at `--shards 4`.<br>Green on both runtimes.<br>**Spill-failure trace:** proven at the file level with an injected write failure. It is not a restart loss in any shipped config: the async spill only runs under `appendonly yes`, whose AOF holds the key. | moon#1185 still needs the TXN.ABORT decision and the fold itself. |
| moon#1267 | **FIXED** | `af57e5a8`, `4516a9aa` (cross-ownership), `6fc6c0b5`, `e86342b1` + `e572a741` (tests, cross-ownership), `de8bf26e` | `perf_ws21_snapshot_without_save_rules` (9 cases).<br>**Red on 4a96cd5f:** "2000 of 2000 keys missing" after a restart (`--shards 1` and 4); "BGSAVE refused: -ERR background save unavailable…", also with the default `--disk-offload enable`; "SHUTDOWN SAVE answered instead of exiting".<br>**Red on ae21476:** 2000/2000 missing, the BGSAVE never finished, and SHUTDOWN SAVE hung.<br>Green on both runtimes. Replica and offload-only boots were checked by hand. | Sharded `SAVE` is still "not supported, use BGSAVE" in every config; that is not a save-rules gap. |
| moon#1263 | **FIXED**, but a failed save keeps the server up (redis parity) | `75b4301a` | `perf_ws21_signal_save` (6 tests).<br>Red on 4a96cd5f and ae21476:<br>- "200 of 200 keys written before the signal were lost" (SIGTERM at s1/s4, SIGINT at s4);<br>- "the server exited while a save was still running";<br>- "the server exited although its final save failed".<br>Green on both runtimes. A SIGTERM during a held BGSAVE waits for it, then saves exactly once.<br>redis 7.0.15, with `dump.rdb` squatted by a directory, logs "Error trying to save the DB, can't exit" and keeps serving. The plan's "exit non-zero" was wrong. | Writes acknowledged during the final save are not in it. SHUTDOWN already behaves this way; redis pauses writes during a shutdown. |
| moon#1264 (a) SHUTDOWN ABORT | **FIXED** | `14b9df5f`, `e36731ce` (script rows, cross-ownership) | Lib `command::persistence::shutdown_abort_tests` (4 tests; the argument table matches redis).<br>`perf_ws21_shutdown_abort` is red on both base binaries, and on this tree with only this commit reverted: `-ERR No shutdown in progress` instead of `+OK`.<br>The ignored redis oracle `shutdown_abort_oracle_redis_agrees` gives the same replies.<br>`test-commands.sh --category persistence`: 3/3 (1/3 on 4a96cd5f). | — |
| moon#1264 (b) FLUSHALL save | **FIXED** (plain, ASYNC, EXEC, scripts; both runtimes) | `8d0f0eae` | `perf_ws21_flushall_save` (8 tests), red on both base binaries in every shape: "200 of 200 flushed keys came back after a crash right after the reply".<br>Green; `rdb_changes_since_last_save` is 0 afterwards, as in redis.<br>Cost: FLUSHALL p50 goes from 0.1 ms to 19 ms (s1) / 24 ms (s4), which is one moon BGSAVE. | A replica applying the master's FLUSHALL, and the admin console's flush, do not save. |
| Item 6 (CodeRabbit nit on PR #1268) | **FIXED** | `6cc489d3` | Mutation proof: with EVAL's quota bypass removed, the old test passes 3/3 and the new one fails "an EVAL DEL over the quota". | — |

## Measurements
Relative numbers from a 4-vCPU container, interleaved A B B A A B, with a fresh server per run. A = `main-4a96cd5f-rel` (release profile); B = a release-fast build of `de8bf26e`.

**FLUSHALL** (p50/p99 ms, 1,000 keys, 300 calls per run):

| config | A | B |
|---|---|---|
| s1 with save points | 0.115/1.72, 0.130/0.64, 0.073/0.46 | 18.9/25.8, 18.7/24.9, 18.6/30.9 |
| s4 with save points | 0.145/4.05, 0.125/4.61, 0.129/5.14 | 24.2/29.2, 24.3/25.9, 24.3/26.0 |
| s4 without save points | 0.150/3.78, 0.129/4.33, 0.122/4.24 | 0.134/3.79, 0.132/5.00, 0.138/4.06 (unchanged) |

- A bare BGSAVE of an empty dataset takes 17.0 ms p50 on both builds, and 18.5 ms with `MOON_IDLE_PARK=0`.
- redis 7.0.15 FLUSHALL is 3.2 ms p50 with save points and 0.065 ms without. redis's save blocks every client; moon's blocks the calling connection only.

**Eviction with no save running:**
- Same-profile debug A/B at s1, 6 pairs: median 177.0k vs 185.4k rps, so no regression.
- Release vs release-fast is not a clean A/B, because the LTO settings differ: 864k vs 826k at s1 (9 pairs), 494k vs 519k at s4 (3 pairs).

**SIGTERM exit** (s4, ~126k keys):
- base with save points: 4–5 ms (no save);
- new with save points: 69–75 ms (the final save);
- new without save points: 4–6 ms.

## Cross-ownership edits
- `src/persistence/aof/auto_rewrite.rs`: one doc comment (`a378418e`).
- `src/shard/persistence_tick.rs`: the snapshot trigger falls back to the registered snapshot dir, net 0 lines (`4516a9aa`).
- `scripts/test-commands.sh`: 2 SHUTDOWN ABORT rows (`e36731ce`).
- `tests/perf_ws15_bgsave_status.rs`: it asserted the refusal that moon#1267 removes; now `bgsave_without_save_rules_runs_and_succeeds` (`e86342b1`).
- `tests/shutdown_integration.rs` (ignored suite): the save failure now comes from a directory squatting on the snapshot path (`e572a741`). The old chmod never failed a save: the server refused up front, and root ignores the mode anyway.
- The FLUSHALL paths it owned:
  - `handler_monoio/{mod,write}.rs`, `handler_sharded/{mod,write}.rs` and `shared.rs::finish_script_flush`;
  - a new `server/conn/flush_save.rs`, plus one line in `server/conn/mod.rs`.
- **File sizes:** the files already over the cap did not grow: handler_monoio/mod.rs 5115, handler_sharded/mod.rs 3793, shared.rs 6391, eviction.rs 4089, writer_task.rs 2151, persistence_tick.rs 3528. main.rs went 2614 → 2608 and aof_manifest/mod.rs 1662 → 1589.

## Risks to re-check at integration
1. **Behaviour changes** (all go in the CHANGELOG):
   - With no `--save` (the default), boot recovery always runs. With `--disk-offload disable`, a snapshot in `--dir` now loads, plus a legacy AOF/WAL replay there, as a `--save ""` server already did.
   - BGSAVE and SHUTDOWN SAVE work without save rules.
   - SIGTERM/SIGINT with save points save first; a failed save keeps the server running.
   - FLUSHALL with save points waits for a save, about 17–24 ms here.
   - SHUTDOWN ABORT replies changed. `NOSAVE NOSAVE` is now accepted; ABORT combined with another modifier is a syntax error.
2. **Orphan sweep:** `AofManifest::load` no longer sweeps orphans; only main.rs's boot load does. An aborted rewrite's `seq+1` files stay on disk until the next attempt overwrites them or the next boot sweeps them.
3. **WS20:** no file overlap. Re-run `eviction_capture_tests` and `perf_ws21_eviction_bgsave` on the merged tree, because the capture relies on `Database::db_index` being the shard slot.
4. **Known tokio failures, not regressions:** with monoio-built test binaries against a tokio server, `perf_ws12_bgsave_split::a_resync_…` and two `perf_ws19_save_rules` replica tests fail, and they fail the same way on the base tokio binary.
5. **Signals:** the sigterm thread keeps waiting after the first SIGTERM. A second SIGINT during the final save exits at once with `exit(1)`.
6. **Test hook:** new `MOON_TEST_AOF_WRITER_HOLD`.
7. **Residuals:** sharded SAVE; writes during a shutdown's final save; replica and console FLUSHALL do not save; the ~17 ms floor of a cooperative save; moon#1185.
8. **Watch:** `keyspace_event_db_index` (where moon#1271 was first seen) in the next loaded integration gate.

## Gates at `de8bf26e`
- `cargo fmt --check` and the four audits (unsafe, unwrap, test-tempdirs, encoding-limits) are clean.
- Clippy is clean with `--all-targets` on monoio, and on tokio both with and without `--all-targets`. The tokio `check --all-targets` is clean.
- Lib tests: monoio full run 6,723 passed / 0 failed; tokio on the touched modules 1,247 passed / 0 failed.
- All touched integration suites are green on both runtimes, apart from the tokio exceptions in risk 4.

## Self-evaluation (0–1)
Scores are in the order Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation.

| issue | scores |
|---|---|
| 1271 | .95 · .95 · .95 · .95 · .92 · .92 |
| 1257 | .93 · .90 · .95 · .92 · .92 · .90 |
| 1267 | .92 · .92 · .95 · .95 · .90 · .90 |
| 1263 | .92 · .93 · .92 · .90 · .90 · .92 |
| 1264a | .95 · .95 · .95 · .95 · .92 · .92 |
| 1264b | .90 · .92 · .90 · .90 · .90 · .90 |
| item 6 | 1.0 · .95 · 1.0 · 1.0 · .95 · .95 |

## CHANGELOG bullets (amended in the review round, F8)
- **Fixed (data loss, moon#1271):** a per-shard AOF writer that finished starting after a `BGREWRITEAOF` had been dispatched deleted the other shards' in-progress rewrite files. The rewrite then aborted, or it committed a manifest naming deleted files, and those shards' keys were gone after the next restart (165 of 200 in a forced reproduction). Loading the AOF manifest no longer deletes anything; stale rewrite files are swept at boot only.
- **Fixed (moon#1257):** a key evicted while a BGSAVE ran was missing from the snapshot, so the snapshot was not the point-in-time image the save started from. In a reproduction, all 9,952 (`--shards 1`) and 13,404 (`--shards 4`) keys evicted during a held save were missing. Eviction now hands the removed value itself to the save as the key's pre-save state; it is not copied, and it is freed off the shard thread once written. With no save running, the cost is one flag check per victim. During a save, an evicted value's memory is released when the save has written it, not at eviction; `INFO current_cow_size` reports it.
- **Fixed (redis parity, moon#1267):** with `--appendonly no` and no `--save`, `BGSAVE` and `SHUTDOWN SAVE` were refused with "background save unavailable", and a snapshot in `--dir` was ignored at boot. Snapshots now always go to and load from `--dir`, whatever the save rules; as in redis, the rules only schedule automatic saves.
- **Changed (redis parity, moon#1267 review):** with `--appendonly no`, boot loads the snapshot and no longer replays a KV log over it: neither a legacy `appendonly.aof` in `--dir` nor WAL v3 records left by an earlier `--appendonly yes` run. Nothing writes those logs in that mode, so they were older than the snapshot and replaying them reverted keys. This applies to every `--appendonly no` configuration, with or without `--save` rules and in both `--disk-offload` modes. The cold tier and the offload manifest still recover.
- **Fixed (data loss, moon#1263):** SIGTERM (`systemctl stop`) and SIGINT with save points exited without saving, losing every write since the last automatic save under `--appendonly no`. They now save first, as a plain `SHUTDOWN` does, waiting for a save already running; with `--appendonly yes` plus save points this also writes a final snapshot, which takes time proportional to the dataset. There is no deadline: while the save makes no progress for 20 s the server logs it and keeps the stop armed, and it exits once the save is on disk. As in redis 7, if that save fails the server logs it and keeps running; a second SIGINT exits at once, and `SHUTDOWN ABORT` cancels.
- **Changed (moon#1263 review):** `SHUTDOWN` and `FLUSHALL` wait for their save for as long as it makes progress, and fail only after 20 s without progress (`-ERR SHUTDOWN failed: background save made no progress for 20 s, check logs`). Previously one 20 s deadline failed any longer save.
- **Fixed (data loss, moon#1274):** with `--appendonly yes` (the default) and `appendfsync everysec`, SIGTERM, SIGINT or `SHUTDOWN` lost acknowledged writes still queued for the AOF writers (all of them at `--shards 1`). Shutdown now stops the shards first, then lets every AOF writer write its queue and fsync before the process exits, as redis's `prepareForShutdown` does.
- **Fixed (redis parity, moon#1264):** `SHUTDOWN ABORT` now cancels a `SHUTDOWN`, SIGTERM or SIGINT that is still saving. It answers `+OK` (and then the shutdown never happens), and the waiting client gets `-ERR Errors trying to SHUTDOWN. Check logs.`. With nothing in progress it answers `-ERR No shutdown in progress.` (the period is new). `ABORT` with another modifier is a syntax error; a repeated modifier such as `NOSAVE NOSAVE` is accepted.
- **Fixed (redis parity, moon#1264):** with save points, `FLUSHALL` (also `ASYNC`, in `MULTI`, or from a script) saves the empty dataset before it replies, so a crash right after it no longer restores the flushed keys. It blocks the calling connection and its pipeline for one save, about 17–24 ms on a test host. The save points are the ones configured now, so `CONFIG SET save` applies, as it does for `SHUTDOWN` and signals. A replica applying the master's `FLUSHALL` and the admin console's flush do not save.

## Commits (`git log --oneline b230d011..a52ef0c7`)
```
a52ef0c7 docs(add): WS21 NOTES — mechanisms, designs, red/green evidence, measurements, gates
de8bf26e test(persistence): the default config's BGSAVE without save rules (moon#1267)
e572a741 test(shutdown): the SHUTDOWN SAVE failure test squats the snapshot path (moon#1267)
e86342b1 test(persistence): perf_ws15 no-save BGSAVE now runs and succeeds (moon#1267)
6cc489d3 test(scripting): re-fill db 1 over its quota before the EVAL DEL check
e36731ce test(scripts): SHUTDOWN ABORT parity rows in test-commands.sh (moon#1264)
8d0f0eae fix(persistence): FLUSHALL with save points saves before it replies (moon#1264)
14b9df5f fix(persistence): SHUTDOWN ABORT cancels a shutdown still saving (moon#1264)
75b4301a fix(persistence): SIGTERM / SIGINT save before exiting with save points (moon#1263)
6fc6c0b5 fix(persistence): snapshots load and save without save rules (moon#1267)
4516a9aa fix(shard): the snapshot trigger falls back to the registered directory (moon#1267)
af57e5a8 feat(persistence): a registry for the snapshot directory (moon#1267)
e87f99db fix(eviction): capture a victim's pre-image before a BGSAVE loses it (moon#1257)
a378418e docs(aof): the size monitor's manifest load no longer sweeps orphans (moon#1271)
a54541c5 fix(aof): a manifest load no longer sweeps a rewrite in flight (moon#1271)
```

## Review round (REVIEW-WS21: MERGE-AFTER-FIXES)
Branch `perf/ws21-review-fixes` from `d49a2a98`. Binaries were built from this worktree and pinned with `MOON_BIN`: `ws21fix-debug`, `ws21fix-debug-tokio`, and `ws21fix-relfast` for M1. NOTES has the designs and file:line references.

| item | verdict | commit | red → green |
|---|---|---|---|
| F1 (moon#1257): the victim was cloned during a save | **FIXED** (moved, not cloned) | `82ea1c21` | `a_large_victim_is_moved_into_its_pre_image_not_cloned` is red with capture-then-remove (1 clone). Green, with `eviction_capture_tests` 7/7 and `perf_ws21_eviction_bgsave` on both runtimes. |
| F2 (moon#1264 a): ABORT `+OK` while exiting | **FIXED** (one lock decides) | `5955d642` | The reviewer's proof became a unit test, red without `commit()`. The copied `review_ws21_abort_race` is green. |
| F3 (moon#1267): stale AOF/WAL replayed with `appendonly no` | **FIXED** (`KvSources::SnapshotOnly`) | `572aff0c` | `perf_ws21_stale_log_boot` (5) is red on the pre-fix binary: 50/50 reverted from the legacy AOF (offload off, off+save, on); 37/50 from the WAL (offload off, on). Green on both runtimes. |
| F4 (moon#1263): fixed 20 s deadline | **FIXED** (stall bound; a signal's stop stays armed) | `cf8945eb` | `review_ws21_sigterm_deadline` (port 7521) is red on the pre-fix binary ("gave up after 20.017 s … still running 15 s later") and green now. Kept as `a_sigterm_outlasting_the_stall_limit_stays_armed`, plus 4 `save_wait_tests`. |
| moon#1274: SIGTERM lost queued AOF writes | **FIXED** | `201be8d1` | `perf_ws21_aof_drain` (7: TERM/INT/SHUTDOWN × s1/s4 + held writer) is red on the pre-fix monoio binary (300/300 lost at s1). Green 3/3 runs on monoio and on tokio. |
| F5, F6: flaky sleeps | **FIXED** | `3d2c93cd` | Poll ABORT until `+OK`; the SIGKILL phase runs under `appendfsync always`. Green on both runtimes. |
| F10: startup save config | **FIXED** for FLUSHALL, SHUTDOWN and signals. Replica FLUSHALL is a **residual** (documented). | `a14827df` | `flushall_follows_config_set_save` and `shutdown_and_sigterm_follow_config_set_save` are red on the pre-fix binary and green on both runtimes. |
| F8, F9: docs | done | (this commit) | CHANGELOG bullets amended above. The NOTES async-spill claim is reworded: `CONFIG SET appendonly yes` made it a restart-loss path, which the capture closes. |

**M1: a 1M-field victim during a held save.** `--shards 1`, volatile-lru, `maxmemory` halved, PING loop. Release-fast build of `a14827df` (B) vs `main-4a96cd5f-rel` (A), interleaved B A ×5; box load 2.4–3.9.

| victim | build | worst PING gap | `current_cow_size` | RSS across the eviction |
|---|---|---|---|---|
| 1M | before (review, head) | 198–255 ms | 128 MB | +victim transiently |
| 1M | fix | 22.2, 51.3, 46.6, 45.2, 42.7 ms | 152 MB (the held epoch-start value) | flat, 196 → 196 MB |
| 1M | main | 35.1, 36.8, 28.1, 30.2, 28.3 ms | ~0 (victim missing from the file) | 193 → 60 MB |
| 3M | before (review) | 459 ms | 384 MB | — |
| 3M | fix / main | 99.3 / 49.0 ms | 456 MB / 0 | flat 422 / 419 → 157 MB |

The remaining gap over main is one O(n) size estimate (`pre_image_bytes`) of the moved value on the next drain. The follow-up is to carry the removal's credited size into the pre-image. An earlier build of the same F1 logic measured the same range: 23–49 ms.

**Review-round risks**
1. **Behaviour changes** (CHANGELOG above):
   - `appendonly no` never replays a KV log at boot;
   - SHUTDOWN and FLUSHALL wait while a save progresses, with no fixed cap;
   - a signal never gives up on its final save;
   - graceful exit now waits for the AOF writers, up to 60 s before giving up with a log line;
   - `CONFIG SET save` now drives FLUSHALL, SHUTDOWN and signals.
2. **Hot path:** `snapshot_cow::note_progress` does one relaxed `fetch_add` per shard per snapshot tick (only while a save runs). Eviction with no save running is still one TLS bool.
3. **Residuals:**
   - the F1 size-estimate walk;
   - a replica applying FLUSHALL does not save;
   - the auto-save timer ignores `CONFIG SET save`;
   - `embedded.rs` keeps its writer as a child of `cancel` (it is `biased` now).
4. **WS20 overlap:**
   - no new shard-level snapshot start, finish or abort; every save starts via `bgsave_start_sharded` into the WS20-hooked `persistence_tick` sites;
   - `rdb::load` is untouched;
   - `handler_{monoio,sharded}/dispatch.rs` change 1 line each, in SHUTDOWN, far from WS20's SWAPDB hunk.
5. **Environmental:** `cold_index_rebuild_silent_drops_875` fails 4/4 identically on main, because root ignores `chmod 000`.

**Review-round commits (`git log --oneline d49a2a98..HEAD`, code):**
```
a14827df fix(persistence): FLUSHALL, SHUTDOWN and signals save per the runtime save config (moon#1264)
3d2c93cd test(persistence): poll for a pending shutdown; SIGKILL phase under appendfsync always (moon#1264, moon#1271)
201be8d1 fix(aof): drain, fsync and join the AOF writers before a graceful exit (moon#1274)
cf8945eb fix(shutdown): bound save waits by a stall, not a deadline; a signal's stop stays armed (moon#1263)
572aff0c fix(recovery): with appendonly no, boot loads the snapshot and replays no stale KV log (moon#1267)
5955d642 fix(shutdown): decide SHUTDOWN ABORT vs commit under one lock (moon#1264)
82ea1c21 fix(eviction): move a victim evicted during a save into its pre-image, not a clone (moon#1257)
```
