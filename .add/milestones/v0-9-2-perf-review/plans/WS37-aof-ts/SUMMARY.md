# WS37-aof-ts SUMMARY

Wave 2, lane B. Branch `w2/ws37-aof-ts`, base `2e99254`, 6 commits. Linux container, not the merge bar.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1283, option (a): `MOON.TS` stamp | FIXED for the AOF, both runtimes, `--shards` 1 and 4 | `a59469a` test, `20b232b` replay, `df445d6` writer, `396a1f0` fuzz, `8da9cf6` docs, `6e48adf` comments | `tests/aof_replay_clock_1283.rs`, 11 tests. They fail on the base binaries and pass on ws37 (numbers below). | The WAL v3 KV log (`--appendonly no`) is not stamped, because it carries no AOF commands. Option (d) is its own epic. |

**Red on base (`lane-b-base-*`): keys judged wrong**

| test | monoio s1 | monoio s4 | tokio s1 | tokio s4 |
|---|---|---|---|---|
| touchback (mtime set 1 h back) | 20/40 | 17/40 | 12/40 | 12/40 |
| touchforward (mtime 1 h forward) | 10/40 | 10/40 | 10/40 | 10/40 |
| touchback after BGREWRITEAOF | 12/60 | 18/60 | 20/60 | 13/60 |
| mixed old/new log | 20/70 | 11/70 | 16/70 | 19/70 |

- `the_log_carries_ts_records` also fails on base, which writes 0 stamps.
- Counts vary between runs: active expiry sometimes logs a DEL in the few ms a key sits expired. The rounds are spread across phases of the 100 ms cycle so a regression cannot pass by luck.

**Green:**
- `lane-b-ws37-*` passes 11/11 on both runtimes. Each binary contains the marker `MOON.TS`; the base binaries do not.
- The touch tests passed 3 more times in a row on each runtime.
- The mixed test passes with the real base binary writing the old part of the log (`MOON_OLD_BIN`).
- The downgrade test passes with the base binaries replaying ws37 logs (`MOON_DOWNGRADE_BIN`).

## Mechanism
**Stamp on each message.**
- `AofMessage::{Append,AppendSync}` carry `clock_ms`, in memory only.
- `AofWriterPool::fold_stamp()` returns an `AppendStamp`: the fold epoch plus the thread's cached clock, read in the mutation's synchronous section.
- A `FoldEpoch` converts with clock 0, which means "unknown" and emits no stamp.

**Writer: `aof/record_ctx.rs`.** `RecordCtx::prefix(db, clock_ms, …)` emits `MOON.TS` when the clock differs from the last stamp, and then `SELECT` when the db differs.
- It is used by the batch loops (`inject_record_prefixes`), the rewrite drains and the overflow drains.
- It resets at each generation.
- Stamps come from a 4 KiB arena, with lsn 0.
- Each generation head gets `MOON.TS <now>` right after `MOON.COLDCUT`.

**Replay.**
- `replay/pseudo.rs` is the single intercept for every `MOON.*` record (TS, COLDCUT, SPILLED). It returns `ReplayRoute::Marker`, which never counts as KV history.
- `replay/clock.rs` pins the judgment clock to the **last** stamp read (not a running max). The file mtime applies until the file's first stamp.
- Stamps that are 0, beyond year 9999, malformed, or read outside a replay scope are ignored. Stamps are deliberately not capped at the wall clock.
- **Q6:** stamps are applied before any skip decision.

**Migration.** `migrate_aof` copies `MOON.TS` to every shard.

**Downgrade (verified).**
- An older binary answers "unknown command" (Unhandled) for `MOON.TS` and replays all data under the mtime judgment.
- If an old binary then appends to the file, its records inherit the last stamp. A BGREWRITEAOF clears this. It is documented in STORAGE-FORMAT §3.3.

## Measurements
**Message size.** `size_of::<AofMessage>()` goes from 72 to 80 bytes, pinned by a unit test.

**Stamp volume.** About 500–750 `MOON.TS`/s per shard, at most 1,000/s (≤44 KB/s per shard). Extra records:

| config | extra records |
|---|---|
| everysec s1 p16 | 0.11–0.16% |
| everysec s4 p16 | 0.9% |
| everysec s1 p1 | 1.3% |
| everysec s4 p1 | 6.5% |
| always s4 p1 | 13% (only ~6 writes/ms per shard) |

**A/B, base vs ws37: noisy.** Load rose from 1.45 to 3.9 during the runs, so this is not a clean window. Monoio medians:

| config | base rps | ws37 rps |
|---|---|---|
| everysec s4 p16 | 273.5K | 279.5K |
| everysec s1 p16 | 920K | 897K |
| always s4 p16 | 151.8K | 147.2K |

- Server CPU ticks were within base's own spread.
- On tokio, throughput was roughly equal.
- **The orchestrator reruns the A/B at R1.** The script is `bench_cpu.sh` in the session scratchpad.

## Gates
- fmt OK. clippy with all targets: 0 on both feature sets. fuzz check: 0.
- The libFuzzer target was not run here (no nightly toolchain). A seeded 6,000-mutation smoke test runs both readers instead.
- `cargo test --lib persistence`: monoio 1004, tokio 1006.
- Full lib: monoio 6940 passed. Tokio had 1 failure in untouched code: `…eviction_capture_tests::a_failed_save_frees_a_held_victim_off_the_shard_thread`. It passes alone (3/3), so it looks like parallel-run interference. **Re-check it at R1.**
- These integration suites are green on both runtimes:
  - aof_replay_clock_1283, perf_ws20_review (moon_1277_*), crash_aof_init_generation_1293, wal_group_commit
  - cold_cut_single_shard_914, legacy_aof_rewrite_on_boot_914, aof_multidb_kill9
  - crash_matrix_per_shard_aof, crash_matrix_per_shard_bgrewriteaof, crash_recovery_cold_del_rewrite
  - perf_ws6_aof_record_alloc, perf_ws21_aof_drain, perf_ws21_aof_writer_start
  - aof_hash_ttl_red, single_handler_aof_order_1099, recovery_matrix_w1, move_copy_db_crash_recovery_1046
  - aof_auto_rewrite, aof_everysec_backpressure_769, aof_backpressure_reply_1272
- These fail on base too:
  - `aof_fold_exactly_once_455::exec_parked_in_wait_across_a_rewrite_replays_once_toplevel` (both runtimes);
  - tokio `cold_tier_aof_double_apply_902::writes_to_a_cold_key_after_a_rewrite_survive_kill9` (it expects a manifest the tokio s1 layout does not have).

## Cross-ownership edits
- One-line type changes from `FoldEpoch` to `AppendStamp`:
  - `server/conn/handler_monoio/{mod.rs, write.rs}`
  - `server/conn/handler_sharded/{mod.rs, write.rs}`
  - `server/conn/handler_single.rs`, `server/conn/shared.rs`, `server/conn/txn_abort.rs`
  - `shard/coordinator.rs`
- `clock_ms: 0` added in test literals.
- `aof_manifest/mod.rs`: `shard_replay` made `pub`.
- `fuzz.yml`: both matrices.
- Growth in over-cap files: aof/mod.rs +16, pool.rs +37, writer_task.rs +3, rewrite.rs +15, rewrite_overflow.rs +24.

## Risks
- Trivial conflicts are likely with lane A in `txn_abort.rs` and `handler_*`, where the edits here are single lines.
- On tokio worker threads without a cached shard clock, the stamp can run up to ~1 ms ahead of the judgment clock. This is the same tolerance the cached clock already has.
- `replay_ordered_merge` is still unpinned. It has no production emitter.

## Interface for WS42 (`MOON.TXN BEGIN|END <id>`)
- **Encoding:** add `pub const TXN: &[u8] = b"MOON.TXN"` in `replay/pseudo.rs`. Emit the markers as ordinary pool records carrying the TXN's `AppendStamp`; `RecordCtx` handles stamps and `SELECT`.
- **Classify:** add `Pseudo::TxnBegin(u64)`, `TxnEnd(u64)` and `MalformedTxn` to `classify`, and route them as `ReplayRoute::Marker`.
- **State:** add a `RefCell<TxnReplay>` field to `DispatchReplayEngine` (the `graph_collector` pattern).
  - Order: classify first. While a block is open, buffer every record, stamps included.
  - On a matching END, replay the buffer; each buffered `Pseudo::Ts` goes through `pseudo::apply`.
  - When discarding an unterminated block, still apply its last stamp (Q6).
  - Keep the intercept ahead of `clock::pinned_replay_clock_ms()`.
- **End of file:** one pin guard covers one file, so flush or discard an open block where each reader's guard drops, or add `DispatchReplayEngine::finish_file()`.
- **Fuzz:** use the free selector bits of byte 0 in `aof_incr_replay` to prepend BEGIN and END markers (unmatched ids, END without BEGIN). Extend the smoke corpus in `shard_replay_fuzz.rs`.
- **WS46:** if framing moves onto the shard thread, the `RecordCtx` rule (stamp, then `SELECT`, reset per generation) must move with it.

## CHANGELOG bullet
- **Fixed (persistence):** AOF replay now judges key expiry by the shard clock each record was written under, not by the log file's mtime (moon#1283).
  - The writer emits a `MOON.TS <ms>` record whenever that clock changes, and at every generation head.
  - Keys that expired while the server ran, and were rewritten before their `DEL` was logged, no longer come back with old values when a log's mtime is earlier than its last write. That happens with a clock stepped back, a lagging network filesystem, or a `touch -d` restore.
  - Logs written before this change replay as before. An older binary skips `MOON.TS` as an unknown command.
  - `size_of::<AofMessage>()` is now 80 bytes (was 72). New fuzz target `aof_incr_replay`.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.92 · Practicality 0.93 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
