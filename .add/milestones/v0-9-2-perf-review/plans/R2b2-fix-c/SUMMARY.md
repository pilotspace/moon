# R2b2-fix-c SUMMARY

Base int-2b 88e2e98, branch `w2/r2b2-fix-c`. Binaries `r2b2fc-v2-{monoio,tokio}` at HEAD 334c738 (markers verified). Commits: ea3a844 F1 · 5470ce3 F2 · 0cd009e F3 · a5fd033 N1 · dbd9126 N2 · bd5437c N5 · 7480155 N3 · d27fef9 R1 · 334c738 R1 follow-up.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| F1 MAJOR: AofOnly replay Err boots EMPTY and appends behind the bad bytes | FIXED | ea3a844 | `tests/flat_aof_unreadable_refusal_r2b2.rs` (v3 + v2 paths): exit 1; refusal names appendonly.aof and the remedies; file byte-identical afterwards; moving it aside boots the snapshot's 50 keys. Red on 88e2e98 both runtimes (booted, no exit in 30 s); green on v2. Unit `flat_file::the_refusal_names_the_file_the_error_and_the_remedy` | — |
| F2 MAJOR: monoio fresh-manifest boot leaves a head-only flat AOF; later tokio s1 boots empty | FIXED | 5470ce3 | `tests/flat_aof_retired_by_manifest_r2b2.rs` (tokio s1 → monoio s1/s4 → tokio s1): red on 88e2e98, green on v2. repro 0 → 20. Unit `retire_renames_and_never_overwrites_an_older_retired_file` | — |
| R1 MAJOR (moon#1318): promoted replica restart loses the dataset | FIXED (s1; replicas are single-shard, moon#406) | d27fef9, 334c738 | `tests/promoted_replica_restart_r2b2.rs` (monoio master; replica each runtime): 100 synced + `SET promoted` = 101; with 10 INCR + 10 RPUSH streamed after the sync, c=10 and LLEN 10 (applied once). Red on 88e2e98 both runtimes (DBSIZE 1), green on v2 | post-sync rewrite window; embedded has no monitor (Risks) |
| F3 MINOR: restored snapshot ignored silently; wrong "prefix" text | FIXED (doc + WARN) | 0cd009e | WARN when a skipped snapshot is newer than appendonly.aof and holds keys; skip line reworded; production-guide "Restore from backup". Unit `a_newer_snapshot_with_keys_is_flagged` | redis parity: the AOF stays the authority |
| N1 RESETSTAT unit tests race | FIXED | a5fd033 | test mutex | — |
| N2 shutdown abandon logged at ERROR | FIXED | dbd9126 | `SnapshotState::abort_for_shutdown` WARN | — |
| N3 duplicate base closure / "appendonly.aof" literals | FIXED | 7480155 | `fresh_generation::keyspace_base`; `flat_file::{FLAT_AOF_NAME, flat_aof_path}` | — |
| N5 process-global writer open gate | FIXED | bd5437c | `aof::open_gate` keyed by AOF path; unit `a_gate_closes_its_own_path_only_while_any_guard_lives` | — |
| N4 file size | DONE | — | new modules aof/flat_file.rs (229), aof/open_gate.rs (96), replication/replica_aof.rs (137); main.rs 2727→2704 | — |

## Mechanisms
- **F1**: a replay Err (v3 Phase 4b, v2 path) becomes `flat_file::UnreadableAof`; main.rs exits 1 right after shard recovery, while the tokio writer's open gate is still held and before any manifest exists, so nothing opened or appended to the file. The embedded server returns Err and `keep_closed()`s the gate. Truncated tails and mid-stream corruption keep the valid prefix as before.
- **F2**: `flat_file::retire[_logged]` runs in all five manifest branches (replacing three copied rename blocks; adds the monoio s1 fresh and multi-shard fresh branches); never overwrites an older `.legacy` (falls back to `.legacy.N`); fsyncs the directory.
- **R1**: `replica_aof::after_full_sync` runs right after `load_snapshot` and calls `bgrewriteaof_start_sharded`; if a rewrite is running, the request goes to the auto-rewrite monitor (`auto_rewrite::request_rewrite`), which retries until one completes. `replica_aof::log_applied` appends each applied KV write and `MOON.TXN` marker to the replica's AOF right after `apply_local` with no await between (the #455 fold stamp stays correct; the post-sync rewrite stays exactly-once). A master TXN the replica never saw end is rolled back on replay. FT/GRAPH/MQ/TEMPORAL/WS records are not logged. A promotion that rolls back open master TXNs requests a rewrite. `ReplicaTaskConfig.aof_pool` wired at all four REPLICAOF spawn sites. 334c738 restores the "boot-time moon#914 rewrite complete" wording counted by legacy_aof_rewrite_on_boot_914.

## Gates (Linux container, not merge bar)
- fmt OK; clippy `--all-targets -D warnings` both feature sets exit 0; fuzz check exit 0.
- `cargo test --release --lib -- persistence replication shard command::config`: monoio 1692, tokio 1661, 0 failed (37 new/affected unit tests pass on both).
- Integration (`MOON_BIN` pinned to r2b2fc-v1; v2 differs only in log wording): green on both runtimes — the 3 new suites, flat_aof_snapshot_double_apply_r2b, crash_matrix_per_shard_aof, crash_aof_init_generation_1293, cold_cut_single_shard_914, crash_matrix_cold_graves_1281, crash_recovery_cold_no_aof, crash_recovery_cold_del_rewrite, cow_stream_shutdown_1295, aof_replay_clock_1283, kill_snapshot. Green on monoio: replication_streaming, replication_multishard, replication_hardening, replication_ttl_semantics; tokio fails 7/9/5/2 identically on base 88e2e98 (MOON_BIN is the master; tokio has no master-side PSYNC). legacy_aof_rewrite_on_boot_914 green on tokio v2 (tokio-only by design).
- v2 reruns: the 3 new suites + flat_aof_snapshot_double_apply both runtimes, legacy_aof_rewrite_on_boot_914 tokio, repro_all.

## Reviewer repro_all.sh
| case | base 88e2e98 | v2 |
|---|---|---|
| F1 corrupt preamble | DBSIZE 0 (boots empty) | refuses, exit 1, names the file |
| F2 tokio after one monoio boot | 0 | 20 |
| F3 promoted replica restart | 1 | 101 |
| F4 restore snapshot backup | 0 | 0 — documented AOF-first restore + WARN |

## Cross-ownership edits
Replication: replica.rs, txn_apply.rs (one call), mod.rs; REPLICAOF spawn sites in handler_monoio/dispatch.rs, handler_sharded/dispatch.rs, handler_sharded/txn_intercepts.rs. Persistence: auto_rewrite.rs, snapshot.rs (`abort_for_shutdown`), config.rs tests. Not touched: coordinator.rs, pool.rs (called only), blocking.rs.

## Risks
1. R1 window: a crash after promotion inside the post-sync rewrite still loses the synced base (ms for small datasets). A replica restarting as a replica full-syncs again.
2. R1 TXN residual: a post-sync fold landing while a master TXN is open on the replica, then promotion before END, leaves that TXN's uncommitted writes in the base; the promotion's rollback rewrite request narrows but does not close it.
3. Embedded server: no auto-rewrite monitor, so a deferred post-sync rewrite is not retried; with `--shards` > 1 its writer is not gated.
4. F3 WARN false positive after a crash right after a BGSAVE with no later write (one line per boot).
5. Re-run promoted_replica_restart_r2b2 and aof_shard_write_1266 on the integrated tree (R1 calls `send_append_bounded_blocking` from the replica apply path).

## CHANGELOG bullets
- Fixed — an `appendonly.aof` that cannot be replayed (e.g. a damaged RDB preamble) no longer boots an empty server that appends behind the bad bytes; moon exits 1 naming the file and the remedies, as redis does.
- Fixed — every boot that creates an AOF manifest (monoio, or `--shards N`) retires a leftover flat `appendonly.aof`, so a later tokio `--shards 1` boot no longer loads it as the whole dataset.
- Fixed (moon#1318) — a promoted replica keeps the master's dataset and the stream it applied across a restart: after a full sync the replica rewrites its own AOF with the synced dataset as base and appends every applied write, as redis replicas do.
- Changed — with `--appendonly yes`, restoring a snapshot backup requires moving the AOF aside first (documented); moon logs a WARN when it skips a snapshot that looks restored.
- Internal — shutdown-abandoned BGSAVE logs at WARN; the tokio AOF writer's open gate is per path; CONFIG RESETSTAT unit tests serialized.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
