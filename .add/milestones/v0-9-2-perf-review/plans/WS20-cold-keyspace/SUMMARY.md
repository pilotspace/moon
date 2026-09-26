# WS20-cold-keyspace — SUMMARY

(Committed by the orchestrator from the agent's final report; the harness refused the agent's own write of this file. NOTES.md, committed by the agent as `4b9663f1`, holds the mechanisms, options, crash-window tables, red/green records and gates.)

**Review round (REVIEW-WS20, MERGE-AFTER-FIXES):** see "Review round" at the end. The verdicts below are the first round's; where the review round changed one, the row says so, and the CHANGELOG bullets are the corrected ones.

- **Branch:** `perf/ws20-cold-keyspace`, base `4a96cd5f` + `b230d011`.
- **Personas:** storage-durability-engineer (lead) and ci-test-integrity-engineer.
- **Red binaries:** `main-4a96cd5f-rel` (monoio), debug builds of `4a96cd5f` (monoio and tokio), and `baseline-ae21476`.
- **Green binaries:** debug builds of `da0716b4`, both runtimes, with provenance checked by the startup line.
- **Security:** no finding. No new `unsafe`.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1254 MOVE / COPY … DB of a cold key | **FIXED** | `91a820e3` | **Lib** `move_cold_tests` (7): red on 4a96cd5f, e.g. MOVE of a spilled key `(reply, in dst, in src) = (Integer(0), None, false)`; green after the fix.<br>**Real server** `moved_and_copied_*`, red: MOVEs not answering `:1`, out of 200 — monoio s1 86 / 69, s4 82 / 95; tokio s1 77; ae21476 s1 85. | MOVE of a hot key whose TTL has passed still answers `:1`, where redis answers `:0`. This is pre-existing and unchanged. |
| moon#1236 cold key overwritten with a TTL comes back OLD | **FIXED on every AOF path. PARTIAL overall** → review round: **FIXED** (F6 closes the `--appendonly no` quadrant) | `1f80900f` | **Lib** `cold_del_rewrite_tests` (4 new, production fold + recovery): red `Some("v1")` vs `None`. A PERSIST-after-fold case was red `Some("v1")` vs `Some("new")`.<br>**Lib** `rdb_expired_load_tests`: red with only the skip restored.<br>**Real server** `cold_keys_overwritten_with_a_ttl_*` (4 cases). The rewrite legs were red; even probes back OLD, out of 100 — monoio s1 55 / 46, s4 89 / 84; tokio s1 66; ae21476 75. The no-rewrite legs are green before and after (the issue's "100/100 without a rewrite" did not reproduce). | **The `--appendonly no` + snapshots quadrant is not fixed.** With the fix binary, 40/100 probes come back OLD. The cause is `snapshot.rs::shard_snapshot_load`, which skips expired entries and also serves the plain snapshot boot. |
| moon#1237 SWAPDB with cold keys | **FIXED** (refuses live, adds a replay rule), with residuals → review round: replica residual FIXED as moon#1278; the in-flight race documented (F4) | `9f5db3aa`, `77587806` (cross-ownership), `da0716b4` (tokio build fix for `9f5db3aa`) | **Lib** `swapdb_cold::tests` (11, production recovery): with only the replay fix reverted, 4 go red, e.g. `(Some("v2"), Some("v2"))` vs `(None, Some("v2"))`.<br>**Real server**, the reported shape (cold keys, SWAPDB, DEL, rewrite): red, keys wrong — monoio s1 178, s4 121; tokio s1 97.<br>**A second defect** (SWAPDB of empty dbs, then spills, no rewrite): red, keys wrong — monoio s1 86, s4 142. | See Risks. |
| moon#1260 no-AOF: an inherited cold key is lost after a read + sweep + kill -9 | **FIXED** → review round: the BLOCKING F1 regression it introduced is FIXED; applies with or without `--save` (F2) | `cf205439`, `e5b6c3c3` (cross-ownership hooks) | **Lib** `promote_sweep_tests::without_an_aof_a_promoted_key_keeps_its_file_until_a_later_snapshot`: red with only the fix reverted ("file 5 … must be held, not unlinked").<br>**Real server** `no_aof_promoted_*`: red, keys lost — monoio s1 179/179, s4 147/147; tokio s4 14/163 (not red on tokio s1). The control case stays green. | Nothing triggers a save when held files pile up (`auto_save.rs`). |

**Crash suite** at `da0716b4`: `crash_recovery_cold_del_rewrite --ignored`, 23 cases (11 existing + 12 WS20).

| runtime | s1 | s4 |
|---|---|---|
| monoio | 23/23 | 23/23 |
| tokio | 21/23 | 23/23 |

The two tokio s1 cases (`keys_spilled_after_a_swapdb_*`) fail on the pre-existing tokio SWAPDB/WAL defect in Risk 1.

## How each fix works
- **#1254:** both cores check the destination first, then promote the source the way `get` does, then move it. A cold-read fault answers `-IOERR` and consumes the fault flag. Every caller (both connection handlers, SPSC, MULTI, Lua, replica apply, replay, handler_single) goes through these cores. WS16's `capture_two_db` and WS19's change counting are unchanged.
- **#1236:** `rdb::load` and `rdb::load_from_bytes` now load an AOF base's expired entries, as redis does for an AOF preamble. The existing gated hot-wins resolution at replay close drops the stale cold copy, and active expiry removes the hot copy.
  - Rejected: removing the cold copy at the skip site. The callers detach the cold wiring it needs, and it would wrongly drop a copy spilled after the rewrite.
  - Rejected: recording the overwrite in the moon#1215 ledger. It does nothing, because the fold sees the key alive and writes no DEL.
- **#1237, live:** SWAPDB is refused while either db, on any shard, still has anything in the cold tier: live cold entries, older copies, ledger slots, queued or held files, compactions, in-flight or superseded spills.
  - The reply (review F10) is `ERR SWAPDB is not allowed while either database has keys or unreclaimed spill files in the disk-offload cold tier; after deleting or reading back its cold keys, run BGREWRITEAOF (appendonly yes) or BGSAVE (appendonly no) and retry`; a busy foreign db answers `ERR SWAPDB could not check the disk-offload cold tier of every shard, try again` after a bounded spin (F11).
  - Other shards are checked through the L4 read plane with `try_read`, which never blocks.
  - Re-tagging at swap time cannot be made crash-consistent with the logged SWAPDB record without a format change. NOTES has the proof, and a format-neutral "cold tag" design as the future alternative.
- **#1237, replay:** `storage::db::swap_replayed` moves only the cold entries whose files already existed at that point in the log. A file counts if it is below the generation's cut, or if its `MOON.SPILLED` marker has already replayed. Logs with no cut use a new per-db `ReplayMarkers` set.
- **#1260:** without an AOF (with or without `--save`, review F2), the moon#1231 hold uses the shard's snapshot history in place of AOF folds.
  - A held file is released only after a snapshot that started after the file went unreferenced has succeeded.
  - While a snapshot runs, every unreferenced file is held (`FoldView::hold_all`), because snapshots are incremental.
  - (review F1) A file emptied before a snapshot starts is held with the pre-start epoch at the start, and the shard sweeps right after a successful snapshot, so that snapshot releases it.

## Cross-ownership edits
- `src/shard/coordinator.rs` (`77587806`): one log-only call in the local leg of `coordinate_swapdb`.
- `src/shard/persistence_tick.rs` (`e5b6c3c3`): four one-line snapshot start/finish hooks.
- Inside the #1237 commit:
  - `src/persistence/replay.rs`: the SWAPDB intercept calls `swap_replayed`.
  - `src/server/conn/handler_single.rs`: the SWAPDB refusal.
- Over-cap files grew by call-site lines only:

  | file | lines |
  |---|---|
  | `handler_monoio/dispatch.rs` | 2240 → 2247 |
  | `handler_single.rs` | 3349 → 3351 |
  | `coordinator.rs` | 4485 → 4487 |
  | `persistence_tick.rs` | 3528 → 3532 |
  | `spsc_handler.rs` | 4979 → 4981 |

- `rdb.rs`, `storage/db/mod.rs` and `cold_index.rs` did not grow. New modules: `storage/db/swapdb_cold.rs`, `storage/tiered/cold_footprint.rs`, `storage/tiered/snapshot_hold.rs`.

## Risks to re-check at integration
1. **Pre-existing, filed as moon#1275, FIXED in the review round (`b3b029d3`):** on tokio `--shards 1` with disk offload, any SWAPDB followed by kill -9 lost every key.
   - Probe: `SET a; SWAPDB 0 1; SET b; SET c`, kill -9, restart → all keys absent, on 4a96cd5f and on this branch. monoio is correct.
   - Cause: the local leg of `coordinate_swapdb` writes SWAPDB to WAL v3 unconditionally, unlike the SPSC arm, which checks `wal_kv_log`. Recovery Phase 4b then treats the WAL as the KV authority and never replays `appendonly.aof`.
2. **WS21 overlap:** every snapshot start/abort path needs the two `snapshot_hold` hooks. Without them, held no-AOF files are never released (never lost). The no-AOF quadrant of #1236 is in the snapshot loader.
3. **Behaviour change:** with default disk offload, SWAPDB is refused while either db holds, or recently held, spilled keys. The refusal lasts until the files are reclaimed: after the next committed rewrite plus an orphan sweep with an AOF, or after the next successful snapshot plus a sweep without one.
4. **SWAPDB residuals:**
   - ~~a replica must apply the master's SWAPDB even when it has cold data of its own~~ — review round: it resyncs in full instead (moon#1278);
   - files swapped by a pre-fix binary keep their old tags;
   - a shard that spills a key between the check and its own swap still swaps, and logs it at `error`.
5. **Behaviour change, AOF load:** keys whose TTL passed during downtime are loaded, then expired with their DEL logged. `DBSIZE` counts them until they are reaped.
6. **Behaviour change, no-AOF:** unreferenced spill files stay on disk until the next successful snapshot. `INFO cold_files_pending_unlink` counts them.
7. **Bisect gap:** `9f5db3aa` does not build on tokio; `da0716b4` fixes it.
8. **The crash suites are `#[ignore]`d.** Nightly `crash-matrix.yml` runs them on monoio at s4 and s1 (the review round added `crash_recovery_cold_no_aof`), and nothing runs the tokio legs. The per-PR guards are the lib tests and `tests/perf_ws20_review.rs`.

## Gates at `da0716b4`
- Clean: `cargo fmt --check`, audit-unsafe, audit-unwrap, audit-test-tempdirs, audit-encoding-limits, monoio `clippy --all-targets -D warnings`, tokio `clippy -D warnings`, tokio `check --all-targets`.
- Lib tests, monoio: full run 6739 passed / 0 failed; filtered (storage, persistence, shard, command, scripting, server, replication) 4273 / 0.
- Lib tests, tokio, same filter: 4046 / 0.

## Self-evaluation (0–1)
Scores are Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation.

| issue | scores | note |
|---|---|---|
| #1254 | .95 · .95 · .95 · .95 · .9 · .95 | — |
| #1236 | .85 · .95 · .95 · .95 · .9 · .9 | The no-AOF quadrant needs a snapshot-loader decision. |
| #1237 | .9 · .9 · .85 · .9 · .9 · .9 | Refusal is a real restriction with offload on by default. |
| #1260 | .95 · .95 · .9 · .95 · .9 · .95 | — |

## CHANGELOG bullets (corrected in the review round)
### Fixed
- **Data loss (moon#1254):** `MOVE` of a key in the disk-offload cold tier, or whose spill was in flight, deleted it from both databases and answered `:0`. The key is now read back and moved. A `MOVE` or `COPY … DB n` of a cold key whose data cannot be read answers `-IOERR` instead of `:0`. Reproduced on 69–95 of 200 cold keys.
- **Data resurrection (moon#1236):** a cold key overwritten with a TTL came back with its OLD value after a restart past the TTL — after an AOF rewrite (46–89 of 100 keys), and with `--appendonly no` after a snapshot (77–95 of 100). An AOF base is now loaded with its expired keys (they are reaped after boot, see Changed), and a snapshot boot drops the cold copy of every key it holds as expired.
- **Data resurrection (moon#1277):** an AOF replay judged expiry by the clock at restart, so a TTL-preserving write logged while its key was alive (`APPEND`, `INCR`, `HSET`, `SETRANGE`, …) replayed onto an absent key after a downtime that outlasted the TTL: the key came back with a wrong value and no TTL (4 of 4 keys, both runtimes, `--shards` 1 and 4). Replay now judges expiry by the time the log was last written (its newest file's mtime, capped at the wall clock).
- **SWAPDB with cold keys (moon#1237):** after an AOF rewrite and a restart, cold keys reappeared in their old database and keys deleted after the swap came back. Separately, a restart without any rewrite moved every key spilled after a SWAPDB into the other database. A replayed SWAPDB now moves only the cold data that existed at that point in the log, and SWAPDB is refused while either database has cold-tier data.
- **SWAPDB on a replica (moon#1278):** a replica with its own cold tier applied its master's SWAPDB as a plain swap; after a failover and a restart its cold keys came back in the old database. Such a replica now resyncs from its master in full instead.
- **Data loss (moon#1275):** on tokio `--shards 1` with `--appendonly yes`, any SWAPDB followed by a kill -9 lost every key: the swap's WAL record made recovery skip `appendonly.aof`. The record is now written only with `--wal-kv-log`.
- **Data loss (moon#1260):** with `--appendonly no`, a cold key inherited from an AOF run was lost after it was read, the orphan sweep ran and the process was killed (179/179). Its spill file is now kept until a snapshot that started after the file emptied has succeeded; the shard reclaims it right after that snapshot.
### Changed
- **SWAPDB (moon#1237):** refused while either database, on any shard, holds spilled keys or spill files not yet reclaimed: `ERR SWAPDB is not allowed while either database has keys or unreclaimed spill files in the disk-offload cold tier; after deleting or reading back its cold keys, run BGREWRITEAOF (appendonly yes) or BGSAVE (appendonly no) and retry`. If another shard holds a database for the whole bounded check (up to 20,000 yields, a few ms), the reply is `ERR SWAPDB could not check the disk-offload cold tier of every shard, try again` and nothing is swapped. Redis never refuses SWAPDB. A replica whose own cold tier holds either database resyncs in full on its master's SWAPDB (moon#1278).
- **AOF load (moon#1236):** keys whose TTL passed during downtime are loaded, then reaped by the active expiry with their `DEL` logged. Until then they count in `DBSIZE` and in memory: measured, 200k expired keys in a base gave `DBSIZE` 199k and 49 MB right after boot, were reaped in ~95 s and added 5.29 MB of `DEL` records to the AOF; under `--maxmemory` with `noeviction`, writes can answer `-OOM` until the reap finishes.
- **`--appendonly no` (moon#1260):** with or without `--save`, a spill file no key uses any more is removed only after the next successful snapshot that started after it emptied (right after that snapshot), and is counted in `cold_files_pending_unlink` until then. Without save rules only a manual `BGSAVE` or `SHUTDOWN SAVE` releases such files, and a database holding them refuses SWAPDB until then.

## Commits (`git log --oneline b230d011..4b9663f1`)
```
4b9663f1 docs(add): WS20 NOTES — cold-tier keyspace integrity design, red/green records and gates
da0716b4 fix(conn): pass handler_single's database slice to the SWAPDB cold check (moon#1237)
e5b6c3c3 fix(shard): report snapshot start and finish to the no-AOF unlink hold (moon#1260)
cf205439 fix(tiered): without an AOF, hold a zero-ref spill file until a later snapshot (moon#1260)
77587806 fix(shard): log a SWAPDB local leg that raced a spill of a swapped db (moon#1237)
9f5db3aa fix(keyspace): SWAPDB keeps every cold key in its database (moon#1237)
1f80900f fix(persistence): load an AOF base's expired entries so a cold shadow cannot win (moon#1236)
91a820e3 fix(keyspace): MOVE and COPY … DB of a cold-tier key keep it (moon#1254)
```

## Review round (REVIEW-WS20)

Branch `perf/ws20-review-fixes` from `86cd20e8`. Red binaries: debug builds of `86cd20e8` (both runtimes), `main-4a96cd5f-rel`. Green: debug builds of each fix, and of `a4307d0e` (final code) for the gates. Records, mechanisms and designs: NOTES "Review round".

| item | verdict | commits | red → green |
|---|---|---|---|
| **F1** (BLOCKING) no-AOF hold released a file emptied before a snapshot only at the second one | **FIXED** | `c6380c47`, `ccb6129e` | Real server `no_aof_flushed_*` (FLUSHALL save / BGSAVE), probes back out of 200 on `86cd20e8`: monoio s1 141/119, s4 184/188; tokio s1 187/171, s4 167/146 → 0. Unit proof permanent (`snapshot_hold`, `promote_sweep_tests`). |
| **F2** hold applies without `--save` | **FIXED** (docs + dead gate removed) | `95812d99` | `no_aof_no_save_promoted_*`: red on main s1 (171 → 0 back), green here. |
| **F3 → moon#1278** replica applies SWAPDB over its cold tier | **FIXED** (full resync) | `34346305`, `a4307d0e` | `perf_ws20_review::replica_swapdb::moon_1278_*`: `(db0 keys, db0 probes, db1 keys, db1 probes, resynced)` `(4407,165,11848,35,false)` monoio / `(5376,92,16200,200,false)` tokio replica on `86cd20e8` → `(0,0,16200,200,true)` both. Unit twin. |
| **F4** check-to-swap race, in-flight variant | **DOCUMENTED**, residual | `13269329` | Not reproducible (review: 0/3,914). Design of the "swap pending" prepare in NOTES. |
| **F5 → moon#1277** replay expires lazily on the restart clock | **FIXED** (pinned log clock, not suppression — see NOTES) | `08d14818`, `3d2b1802` | `perf_ws20_review::moon_1277_*` s1/s4 ± rewrite: 4/4 keys back PTTL -1 on `86cd20e8` both runtimes → 0; lib `replay::clock` red with the pin disabled; WAL-only recovery probe red → green. |
| **F6** #1236 no-AOF quadrant | **FIXED**; #1236 now FIXED overall | `16cd41c7`, `95621f76` (cross-ownership) | `crash_recovery_cold_no_aof::no_aof_cold_keys_overwritten_with_a_ttl_*`: even probes back OLD on `86cd20e8` monoio s1 77, s4 95, tokio s1 84, s4 93 → 0. Unit twin. |
| **F7** #1236 boot cost | **DOCUMENTED** (CHANGELOG) | this file | — |
| **F8** 30 ms TTL flake | **FIXED** | `6f6ed4c4` | 50 ms injected before the save: old red (1 vs 2), new green. |
| **F9** sleep / everysec / setup-time flakes | **FIXED** | `1eb5b28e` (+ split `ad3edc6d`) | Promote case still red on main (121 → 0); both suites green, see gates. |
| **F10/F11** refusal text, bounded spin | **FIXED** / **DOCUMENTED** | `bd978a68` | Unit `the_cold_refusal_names_the_remedy_*`: red on the old text. |
| **moon#1275** tokio s1 SWAPDB local leg writes WAL | **FIXED** | `b3b029d3`, `67230735` (accessors out of the over-cap file) | `perf_ws20_review::moon_1275_*`: tokio s1 `86cd20e8` every key lost → green; the two tokio s1 `keys_spilled_after_a_swapdb_*` crash cases red (101 / 98) → green. |

### Cross-ownership edits (review round)
- WS21's `snapshot.rs` (`95621f76`): `shard_snapshot_load_noting_expired`; `shard_snapshot_load` keeps its signature (1491 → 1499 lines, under the cap). `recovery.rs` (`3d2b1802`, `95621f76`): 7 wiring lines (2499 → 2506, already over the cap). `aof_manifest/shard_replay.rs` (`3d2b1802`).
- `shard/{shared_databases,spsc_handler,coordinator}.rs` (`b3b029d3`, `67230735`); `shard/{persistence_tick,event_loop}.rs` (`ccb6129e`); `replication/{apply,replica}.rs` (`34346305`); `aof/mod.rs` (`08d14818`); `.github/workflows/crash-matrix.yml` (`ad3edc6d`: runs the new no-AOF suite).
- Over-cap files grew by wiring only: `recovery.rs` 2499 → 2506, `event_loop.rs` 3279 → 3296 (the post-snapshot sweep call, both runtimes), `shared_databases.rs` 2728 → 2734, `apply.rs` 1898 → 1903, `coordinator.rs` 4487 → 4491, `spsc_handler.rs` 4981 → 4983, `aof/mod.rs` 1989 → 1990. The review round also split the crash suite, which it had taken to 1629 lines.

### New findings (pre-existing, for filing)
- **N1:** tokio `--shards 1 --appendonly no` replays a leftover `appendonly.aof` at every boot (redis ignores it): FLUSHALL + BGSAVE + kill -9 brings the keys back.
- **N2:** without an AOF, cold-key removals are not durable: `DEL` of a cold key whose file backs other keys + BGSAVE + kill -9 → back (46/100 on main, 36/100 here). F6's limit is the same gap.
- #1277 residuals: the replica's live apply still judges expiry on the wall clock; an mtime later than the log errs to the old behaviour.
- `cold_tier_aof_double_apply_902::writes_to_a_cold_key_after_a_rewrite_survive_kill9` (`#[ignore]`) fails on tokio s1 on `86cd20e8` too: it reads `appendonlydir/moon.aof.manifest`, which the flat-file layout lacks (test assumption).

### Risks
1. moon#1278 costs one full transfer per master SWAPDB while the replica's own cold tier holds either db.
2. moon#1277 trusts file mtimes (capped at the wall clock); a restored backup with a fresh mtime behaves as before the fix.
3. F4 (b) remains possible in a one-SPSC-hop window; `note_swap_with_cold_footprint` logs it at `error`.
4. N2 bounds F6: after a later snapshot, a crash can bring a removed cold key back, as for any no-AOF cold `DEL`.

### Gates (final code `67230735`)
- Clean: `cargo fmt --check`, `audit-unsafe` (no new `unsafe`), `audit-unwrap` (baseline 0), `audit-test-tempdirs`, `audit-encoding-limits`, `clippy --all-targets -D warnings` on monoio and on tokio, tokio `check --all-targets`.
- Lib, filtered to `storage persistence shard replication server command scripting`: monoio 4294 passed / 0 failed; tokio 4067 / 0.
- `tests/perf_ws20_review.rs` (per-PR): monoio 7/7, tokio 6/6, and the tokio replica under a monoio master 1/1.
- `crash_recovery_cold_del_rewrite --ignored`: 21/21 at s1 and s4 on both runtimes. `crash_recovery_cold_no_aof --ignored`: 6/6 at s1 and s4 on both runtimes. These ran on `a4307d0e`; `67230735` only moves two accessors, and was re-verified with the lib and review suites above plus the four tokio s1 SWAPDB crash cases (4/4).
- Regression suites: replication `--ignored` on monoio (`replica_blocking_wake_1096` 2, `replication_flushall` 3, `replication_streaming` 7, `replication_swapdb` 3, `replication_ttl_semantics` 2), and the AOF / TTL replay suites on both runtimes (`aof_hash_ttl_red`, `aof_multidb_kill9`, `cold_tier_aof_double_apply_902`, `hash_field_ttl_red`, `legacy_aof_rewrite_on_boot_914`, `restart_preserves_compact_encoding`, `wal_last_resort_replay_1026`): all green, except the pre-existing tokio s1 failure of `cold_tier_aof_double_apply_902::writes_to_a_cold_key_after_a_rewrite_survive_kill9` listed above.

### Commits (`git log --oneline 86cd20e8..HEAD`, before the SUMMARY and NOTES commits)
```
67230735 fix(shard): keep the moon#1275 wal-kv-log accessors out of the over-cap shared_databases.rs (moon#1275)
a4307d0e fix(tests): gate the moon#1278 replica test's helpers with it (moon#1278)
34346305 fix(replication): a replica resyncs in full instead of swapping over its own cold tier (moon#1278)
13269329 fix(storage): document the in-flight variant of the SWAPDB check-to-swap race (moon#1237)
1eb5b28e fix(tests): the cold crash suites poll the sweep's decision and fsync every WS20 record (moon#1260)
bd978a68 fix(storage): the SWAPDB cold refusal names its remedy; document the bounded spin (moon#1237)
6f6ed4c4 fix(persistence): the rdb expired-load tests no longer race a 30 ms TTL (moon#1236)
95621f76 fix(persistence): the snapshot boot names the keys it skipped as expired (moon#1236)
16cd41c7 fix(tiered): without an AOF, drop the cold shadow of a key the boot snapshot holds as expired (moon#1236)
ad3edc6d fix(tests): split the no-AOF cold crash cases into their own suite (moon#1260)
3d2b1802 fix(persistence): pin the replay expiry clock in the incr and WAL passes (moon#1277)
08d14818 fix(persistence): judge replay expiry by the log's last-write time (moon#1277)
b3b029d3 fix(shard): gate the SWAPDB local leg's WAL record on --wal-kv-log (moon#1275)
95812d99 fix(tiered): the no-AOF hold applies with or without save points; drop the dead gate (moon#1260)
ccb6129e fix(shard): wire the no-AOF snapshot start hold and post-save sweep (moon#1260)
c6380c47 fix(tiered): a no-AOF snapshot releases the files emptied before it started (moon#1260)
```
