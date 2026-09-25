# WS19-cold-durability — PLAN (wave 2, part 4 — base: int/part3b @ 4115798, which holds parts 3a and 3b)
personas: `.add/personas/storage-durability-engineer.md` (lead) · `.add/personas/ci-test-integrity-engineer.md`

Read first: `plans/WS15-durability-followups/NOTES.md` (dead-slot ledger, "promote-then-sweep", cold reclaim and its crash-window argument) and `plans/WS15-durability-followups/SUMMARY.md`.

## Issues (in this order)
1. **moon#1231 (BUG, data loss, pre-existing).** A cold key promoted back to RAM after a fold leaves the cold index with no log record. Promotion happens in `promote_cold_outcome` and on every RMW promote. Once the file's other keys are gone, the orphan sweep unlinks the file, and a restart loses the key. WS15 reproduced this 88/200 on `ae21476`.
   - Fix direction (from the issue): a file below the COMMITTED generation's cut may be unlinked only when every key it held is dead or no longer derives its value from it. That takes a fold-committed signal for the sweep (`src/shard/timers.rs`) and a promotion hook (`src/storage/db/kv_ops.rs`).
   - Build on WS15's dead-slot ledger and on the post-fold reclaim adoption (`src/shard/persistence_tick/cold_reclaim_tick.rs`).
   - Test: a real-server kill-9 case at `--shards 1` and 4, both runtimes. Extend `tests/crash_recovery_cold_del_rewrite.rs`, which is `#[ignore]`d and runs nightly from `crash-matrix.yml`.
   - Also add a recovery-level lib test that runs per-PR.
2. **moon#1232 (BUG).** `--save` rules never fire in the sharded server: `change_counter` in `main.rs` is incremented only in `handler_single`.
   - Keep dirty counts per shard and sum them on read. Never use a shared per-write `fetch_add` (moon#1176 removed exactly that).
   - One number feeds both the save trigger and `rdb_changes_since_last_save`.
   - Integration test at `--shards 1` and 4, both runtimes: with `--save "1 10"`, 10 writes produce a snapshot and 9 do not.
   - CHANGELOG bullet must call out the behaviour change: existing `--save` users start getting snapshots.
3. **moon#1240 (perf).** Cold reclaim does its compaction fsyncs and manifest commits on the shard thread.
   - Move the compaction writes and fsyncs to the spill thread, or to the manifest-sync thread.
   - Adoption's manifest commit uses the deferred / manifest-sync path that spill publication already uses.
   - The crash-window argument must still hold: a compacted file is adopted only after a committed fold whose snapshot is later than the compaction.
   - `src/storage/tiered/cold_reclaim_tests.rs` stays green.
   - Acceptance: a PING p99/max latency A/B during sustained cold-delete churn against `4115798`, run as a native measurement and labelled as such. The Linux-perf-host number is DEFERRED, and you must say so.
   - Also required green: `perf_ws15_ledger_bound`, and `crash_recovery_cold_del_rewrite --ignored` at `--shards 1` and 4.
4. **moon#1241 (BUG, parity).** `redis.call('DEL')` and the other `db_quota::is_shrink_only_command` commands answer `-OOM` inside a script on an over-budget shard.
   - Cause: `ScriptEvictionCtx::gate` (`src/scripting/bridge.rs`) lacks the shrink-only bypass that `run_write_eviction_gate` and `spsc_eviction_gate` apply.
   - Apply the same bypass to both the maxmemory gate and the per-db quota. Eviction still runs.
   - Test (integration, `--shards 1` and 4): `noeviction`, filled past the limit. Script DEL returns 1; script SET still returns `-OOM`; a FUNCTION equivalent too. Compare with redis-server 7.0.15 where the reply is observable.

## Owned files
`src/shard/timers.rs` (sweep gate), `src/storage/tiered/**`, `src/shard/persistence_tick/**`, the promotion path in `src/storage/db/kv_ops.rs`, `src/persistence/auto_save.rs`, `src/main.rs` (auto-save wiring only), `src/scripting/bridge.rs` (the `ScriptEvictionCtx::gate` function and its tests ONLY — the COPY/MOVE region is WS16's), `tests/crash_recovery_cold_del_rewrite.rs`, tests `tests/perf_ws19_*.rs`.
Cross-ownership (small, ISOLATED commits, listed in SUMMARY): the per-shard dirty-count increment sites for moon#1232, which live in files other workstreams own. Touch only the increment lines.
WS16 runs in parallel and owns `src/persistence/snapshot_cow.rs`, `src/persistence/snapshot.rs`, `src/command/keyspace/move_cmd.rs`, `src/shard/mq_exec.rs`, `src/blocking/stream_wake.rs`, and the COPY/MOVE region of `src/scripting/bridge.rs`.
