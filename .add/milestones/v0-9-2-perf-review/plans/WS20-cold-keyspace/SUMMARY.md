# WS20-cold-keyspace — SUMMARY

(Committed by the orchestrator from the agent's final report; the harness refused the agent's own write of this file. NOTES.md, committed by the agent as `4b9663f1`, holds the mechanisms, options, crash-window tables, red/green records and gates.)

- **Branch:** `perf/ws20-cold-keyspace`, base `4a96cd5f` + `b230d011`.
- **Personas:** storage-durability-engineer (lead) and ci-test-integrity-engineer.
- **Red binaries:** `main-4a96cd5f-rel` (monoio), debug builds of `4a96cd5f` (monoio and tokio), and `baseline-ae21476`.
- **Green binaries:** debug builds of `da0716b4`, both runtimes, with provenance checked by the startup line.
- **Security:** no finding. No new `unsafe`.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1254 MOVE / COPY … DB of a cold key | **FIXED** | `91a820e3` | **Lib** `move_cold_tests` (7): red on 4a96cd5f, e.g. MOVE of a spilled key `(reply, in dst, in src) = (Integer(0), None, false)`; green after the fix.<br>**Real server** `moved_and_copied_*`, red: MOVEs not answering `:1`, out of 200 — monoio s1 86 / 69, s4 82 / 95; tokio s1 77; ae21476 s1 85. | MOVE of a hot key whose TTL has passed still answers `:1`, where redis answers `:0`. This is pre-existing and unchanged. |
| moon#1236 cold key overwritten with a TTL comes back OLD | **FIXED on every AOF path. PARTIAL overall** | `1f80900f` | **Lib** `cold_del_rewrite_tests` (4 new, production fold + recovery): red `Some("v1")` vs `None`. A PERSIST-after-fold case was red `Some("v1")` vs `Some("new")`.<br>**Lib** `rdb_expired_load_tests`: red with only the skip restored.<br>**Real server** `cold_keys_overwritten_with_a_ttl_*` (4 cases). The rewrite legs were red; even probes back OLD, out of 100 — monoio s1 55 / 46, s4 89 / 84; tokio s1 66; ae21476 75. The no-rewrite legs are green before and after (the issue's "100/100 without a rewrite" did not reproduce). | **The `--appendonly no` + snapshots quadrant is not fixed.** With the fix binary, 40/100 probes come back OLD. The cause is `snapshot.rs::shard_snapshot_load`, which skips expired entries and also serves the plain snapshot boot. |
| moon#1237 SWAPDB with cold keys | **FIXED** (refuses live, adds a replay rule), with residuals | `9f5db3aa`, `77587806` (cross-ownership), `da0716b4` (tokio build fix for `9f5db3aa`) | **Lib** `swapdb_cold::tests` (11, production recovery): with only the replay fix reverted, 4 go red, e.g. `(Some("v2"), Some("v2"))` vs `(None, Some("v2"))`.<br>**Real server**, the reported shape (cold keys, SWAPDB, DEL, rewrite): red, keys wrong — monoio s1 178, s4 121; tokio s1 97.<br>**A second defect** (SWAPDB of empty dbs, then spills, no rewrite): red, keys wrong — monoio s1 86, s4 142. | See Risks. |
| moon#1260 no-AOF: an inherited cold key is lost after a read + sweep + kill -9 | **FIXED** | `cf205439`, `e5b6c3c3` (cross-ownership hooks) | **Lib** `promote_sweep_tests::without_an_aof_a_promoted_key_keeps_its_file_until_a_later_snapshot`: red with only the fix reverted ("file 5 … must be held, not unlinked").<br>**Real server** `no_aof_promoted_*`: red, keys lost — monoio s1 179/179, s4 147/147; tokio s4 14/163 (not red on tokio s1). The control case stays green. | Nothing triggers a save when held files pile up (`auto_save.rs`). |

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
  - The reply is `ERR SWAPDB is not allowed while either database has keys in the disk-offload cold tier`.
  - Other shards are checked through the L4 read plane with `try_read`, which never blocks.
  - Re-tagging at swap time cannot be made crash-consistent with the logged SWAPDB record without a format change. NOTES has the proof, and a format-neutral "cold tag" design as the future alternative.
- **#1237, replay:** `storage::db::swap_replayed` moves only the cold entries whose files already existed at that point in the log. A file counts if it is below the generation's cut, or if its `MOON.SPILLED` marker has already replayed. Logs with no cut use a new per-db `ReplayMarkers` set.
- **#1260:** without an AOF, and only when snapshots can be written, the moon#1231 hold uses the shard's snapshot history in place of AOF folds.
  - A held file is released only after a snapshot that started after the file went unreferenced has succeeded.
  - While a snapshot runs, every unreferenced file is held (`FoldView::hold_all`), because snapshots are incremental.
  - With neither an AOF nor a persistence dir, nothing changes.

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
1. **Pre-existing, P0 recommended, tracked separately:** on tokio `--shards 1` with disk offload, any SWAPDB followed by kill -9 loses every key.
   - Probe: `SET a; SWAPDB 0 1; SET b; SET c`, kill -9, restart → all keys absent, on 4a96cd5f and on this branch. monoio is correct.
   - Cause: the local leg of `coordinate_swapdb` writes SWAPDB to WAL v3 unconditionally, unlike the SPSC arm, which checks `wal_kv_log`. Recovery Phase 4b then treats the WAL as the KV authority and never replays `appendonly.aof`.
2. **WS21 overlap:** every snapshot start/abort path needs the two `snapshot_hold` hooks. Without them, held no-AOF files are never released (never lost). The no-AOF quadrant of #1236 is in the snapshot loader.
3. **Behaviour change:** with default disk offload, SWAPDB is refused while either db holds, or recently held, spilled keys. The refusal lasts until the files are reclaimed: after the next committed rewrite plus an orphan sweep with an AOF, or after the next successful snapshot plus a sweep without one.
4. **SWAPDB residuals:**
   - a replica must apply the master's SWAPDB even when it has cold data of its own;
   - files swapped by a pre-fix binary keep their old tags;
   - a shard that spills a key between the check and its own swap still swaps, and logs it at `error`.
5. **Behaviour change, AOF load:** keys whose TTL passed during downtime are loaded, then expired with their DEL logged. `DBSIZE` counts them until they are reaped.
6. **Behaviour change, no-AOF:** unreferenced spill files stay on disk until the next successful snapshot. `INFO cold_files_pending_unlink` counts them.
7. **Bisect gap:** `9f5db3aa` does not build on tokio; `da0716b4` fixes it.
8. **The crash suite is `#[ignore]`d.** Nightly `crash-matrix.yml` runs it on monoio at s4 and s1, and nothing runs the tokio legs. The per-PR guards are the lib tests.

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

## CHANGELOG bullets
### Fixed
- **Data loss (moon#1254):** `MOVE` of a key in the disk-offload cold tier, or whose spill was in flight, deleted it from both databases and answered `:0`. The key is now read back and moved. `COPY … DB n` of a cold key whose data cannot be read answers `-IOERR` instead of `:0`. Reproduced on 69–95 of 200 cold keys.
- **Data resurrection (moon#1236):** a cold key overwritten with a TTL came back with its OLD value after an AOF rewrite and a restart past the TTL. An AOF base is now loaded with its expired keys, as redis loads an AOF preamble. Reproduced on 46–89 of 100 keys.
- **SWAPDB with cold keys (moon#1237):** after an AOF rewrite and a restart, cold keys reappeared in their old database and keys deleted after the swap came back. Separately, a restart without any rewrite moved every key spilled after a SWAPDB into the other database. A replayed SWAPDB now moves only the cold data that existed at that point in the log, and SWAPDB is refused while either database has cold-tier data.
- **Data loss (moon#1260):** with `--appendonly no --save`, a cold key inherited from an AOF run was lost after it was read, the orphan sweep ran and the process was killed. Its spill file is now kept until a later snapshot succeeds. Reproduced on every probe (179/179).
### Changed
- **SWAPDB (moon#1237):** it answers `ERR SWAPDB is not allowed while either database has keys in the disk-offload cold tier` while either database, on any shard, holds spilled keys or spill files not yet reclaimed. Redis never refuses SWAPDB.
- **AOF load (moon#1236):** keys whose TTL passed during downtime are loaded, then expired with their DEL logged. `DBSIZE` counts them until they are reaped.
- **`--appendonly no` (moon#1260):** a spill file no key uses any more is removed only after the next successful snapshot, and is counted in `cold_files_pending_unlink` until then.

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
