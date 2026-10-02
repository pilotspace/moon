# R2b-fix-c SUMMARY

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| P1 MAJOR: tokio `--shards 1` replays the whole flat `appendonly.aof` on top of the snapshot | FIXED | 5aef54d, 0ce24a6 | `tests/flat_aof_snapshot_double_apply_r2b.rs` (8 cases; RPUSH/INCR/APPEND/HINCRBY/db-1 INCR; BGSAVE, kill -9, restart twice). Red on efe223b tokio 5/8 (`l=a b a b`; the no→yes switch case got `a b b`), efe223b monoio 1/8 (tokio dir booted by monoio), main cf6fa65 tokio 4/8. Green 8/8 on both r2bfc-v1 binaries. Unit tests: `a_snapshot_under_a_flat_aof_is_not_applied_twice` (v3 + v2 paths), `a_flat_aof_with_records_is_the_only_kv_source`, 4 `fresh_generation` tests, `a_flat_aof_is_not_replayed_over_the_snapshot` | Risks 1-3 |
| WS45 MINOR: SIGTERM with a writer parked on a stream takes 5 s and drops the write | FIXED | d408507 | `cow_stream_shutdown_1295::sigterm_releases_a_writer_parked_on_a_stream`: parked HSET answered `:1`, exit < 2 s, no "drain timed out". Red on efe223b both runtimes; green on both | — |
| WS45 NIT: RESETSTAT keeps `rdb_cow_*`; a re-parked writer counts twice | FIXED | baa888c | `cow_stream_shutdown_1295::a_writer_that_re_parks_is_one_wait_and_resetstat_zeroes_the_stream_counts` (base: 3 waits for 2 writers, RESETSTAT left them). Unit tests `resetstat_zeroes_the_cow_stream_counts`, `a_writer_that_re_parks_counts_once` | — |
| WS44 NIT: volatile-ttl test doc | FIXED (doc) | efe7dc3 | Reviewer's `vttl.py` on redis 7.2.7: 992 evicted, `contiguous False`. The doc says the test asserts moon's exact nearest-deadline order, stricter than redis, not a parity oracle | — |

## P1 mechanism
- `KvSources::AofOnly`: `SnapshotAndLogs` becomes `AofOnly` when `<dir>/appendonly.aof` holds at least one byte. The snapshot is not loaded and the disk-offload WAL v3 KV `Command` records are skipped (both hold a prefix of the AOF). Applies to the v3 path (`recover_shard_v3_pitr`; PITR unchanged) and the v2 path. Resolves the "Known follow-up" note in recovery Phase 4b. Redis's rule.
- The AOF must then be complete. A generation opened at a boot whose keyspace is not empty (snapshot loaded because no AOF held a record: `--appendonly no → yes`, a removed AOF, a monoio-manifest dir booted by tokio s1) now starts with an RDB preamble of that keyspace, then the `MOON.COLDCUT` head with its DELs — the shape a rewrite publishes (`aof::fresh_generation`). Written to tmp, fsynced, renamed, dir fsynced.
- The tokio TopLevel writer used to open its file at thread start, before recovery. It now waits on `hold_writer_open`, which main.rs drops after the generation is opened. The embedded server does the same at `--shards 1`.
- The base is serialized only when the file is fresh.
- Cold tier: the cold index is still rebuilt; the cold-graves trailer only exists on no-AOF snapshots. The head's DELs still tombstone dead slots.

| setup | before | after |
|---|---|---|
| tokio s1 (default) | double-applied | fixed |
| tokio s1 dir booted by monoio s1 | double-applied, folded into the first manifest base | fixed |
| monoio s1/s4, tokio s4 | not affected | green guard cases |
| BGREWRITEAOF, writes, newer snapshot | green | green |
| `--appendonly no` with a snapshot | `SnapshotOnly` | unchanged |

## Item 2 decision
Both runtimes' shutdown arms call `persistence_tick::abandon_snapshot_for_shutdown` before the connection drain. A save whose walk is still running cannot finish once the shard stops ticking, so it is aborted (BGSAVE reply sender told; held pre-images and frozen tables released; `snapshot_hold` notified) and `snapshot_cow` is disarmed, waking every stream waiter. A save already finalizing is left alone. The parked write runs and is answered (redis executes what it read before handling SIGTERM and kills its BGSAVE child). With save points set, SIGTERM still saves first (moon#1263).

## Item 3 decision
`rdb_cow_stream_waits` counts writes, not parks (`wait_for_streams`, once per write). A MULTI body counts each command that waited.

## Gates (Linux container, not merge bar)
- `cargo fmt --check` OK; clippy `--all-targets -D warnings` both feature sets exit 0 (forced recompile); fuzz check exit 0.
- `cargo test --release --lib -- persistence shard command::config`: monoio 1548, tokio 1521, 0 failed.
- Integration, `MOON_BIN` pinned to `r2bfc-v1-{monoio,tokio}`, green on both: flat_aof_snapshot_double_apply_r2b 8/8, cow_stream_shutdown_1295, cow_stream_1295, perf_ws21_snapshot_without_save_rules, aof_multidb_kill9, crash_matrix_per_shard_aof, crash_recovery_cold_no_aof, crash_recovery_cold_del_rewrite, crash_matrix_cold_graves_1281, aof_shard_write_1266, aof_everysec_kill9_1266, volatile_ttl_eviction_order_1298, cold_cut_single_shard_914, crash_aof_init_generation_1293, aof_replay_clock_1283. kill_snapshot green against both. legacy_aof_rewrite_on_boot_914 green on tokio (tokio-only by design).
- txn_crash_atomicity_1300: monoio 38/38; tokio 38/38 with `MOON_TEST_NO_MASTER_PSYNC=1` (the 5 `a_replica_*` cases are the known tokio no-PSYNC class).
- Shared-target aliasing once: lane-b's `aof_shard_write_1266` test binary replaced this lane's and failed 4 cases; rerun in its own build-and-run 6/6 green on both runtimes.

## Cross-ownership edits
- `src/server/embedded.rs`: writer open gate + fresh-generation fold at `--shards 1`.
- `src/persistence/replay/clock_tests.rs`: `a_snapshot_under_replayed_logs_keeps_a_key_the_log_saw_alive` now uses the no-AOF WAL v3 last resort; a sibling test pins the AOF rule.
- Wiring growth in over-cap files: main.rs +27, recovery.rs +58 (mostly the unit test), persistence_tick.rs +30, event_loop.rs +5, writer_task.rs +8. New logic in `src/persistence/aof/fresh_generation.rs`.

## Risks
1. A flat AOF opened over a loaded snapshot by a pre-fix binary holds no base; it now boots without the snapshot's untouched keys, as redis would. The opposite choice keeps double-applying every default tokio s1 deployment that ever saved.
2. A replica's own AOF does not carry the master stream (pre-existing, moon#1318). A tokio s1 replica restart no longer falls back to its snapshot when its AOF holds only a head; it re-syncs from the master.
3. Pre-existing, not fixed: a tokio s1 dir (flat AOF) booted with `--shards 4` replays the flat AOF into every shard (DBSIZE 16 for 4 keys); recovery reads `appendonly.aof` even when `--appendfilename` names another file.
4. The tokio writer opens its file only after recovery.
5. Re-run `flat_aof_snapshot_double_apply_r2b` and `cow_stream_shutdown_1295` on the integrated tree.

## CHANGELOG bullets
- Fixed — a snapshot and the single-file `appendonly.aof` were both applied at boot under tokio `--shards 1`: `RPUSH l a b; INCR c; BGSAVE; kill -9` came back as `a b a b`, `c = 2` (once on a monoio upgrade from such a dir too). Under `--appendonly yes` an AOF holding a record is now the only KV source, as in redis; a fresh AOF opened over a loaded snapshot starts with that dataset as its RDB preamble, written atomically.
- Fixed (moon#1295) — SIGTERM while a write waited for a BGSAVE stream stalled shutdown 5 s and dropped the write; shutdown now abandons the unfinishable save, the write runs and is answered.
- Fixed (moon#1295) — `CONFIG RESETSTAT` resets `rdb_cow_streamed_keys` / `rdb_cow_stream_waits`; a write that waits again behind another key's stream counts once.
- Docs (moon#1298) — `volatile_ttl_eviction_order_1298` asserts moon's exact order, stricter than redis; not a parity oracle.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
