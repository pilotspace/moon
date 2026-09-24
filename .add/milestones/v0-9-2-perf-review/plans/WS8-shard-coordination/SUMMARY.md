# WS8-shard-coordination SUMMARY

Branch `perf/ws8-shard-coordination`, base `ae21476`. `NOTES.md` (same dir) has the design reasoning for each issue.

(Committed by the orchestrator. The harness refused the subagent's own write of this file twice, and TEAM-RULES §6 says to report the content instead. What follows is the agent's final report.)

## Per-issue verdict
| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| moon#1162: spanning DEL/UNLINK never tombstones vectors. The same root cause also broke FLUSHALL/FLUSHDB fan-out and MQ drops | FIXED | fd4a790 | See "moon#1162" below | The coordinator doc comment D-2 is now accurate. Text-only indexes rely on the recovery-ledger delete (the moon#1124 path) |
| moon#1229: SCRIPT FLUSH clears only the connection's shard | FIXED | 19d5d33 | See "moon#1229" below | — |
| moon#1228 item 1: the MSET coordinator's local leg bypasses BGSAVE pre-image capture | FIXED | 61dce1a | See "moon#1228 item 1" below | The other moon#1228 items belong to WS16 |
| moon#1184: spanning MSET/DEL/UNLINK split per key | FIXED | f9d596d | See "moon#1184" below | Moving spanning writes into the slotted batch is still moon#513 |
| moon#1198 (shard items 2 and 7) | FIXED | de09be0, 3256fce | See "moon#1198" below | The connection-handler double guards belong to WS7 |
| moon#1177: redundant cross-shard write work | FIXED | c04464e | See "moon#1177" below | Hoisting the per-batch `batch` and `responses` Vecs on tokio falls in WS7's region |
| moon#1183: WATCH sequential round trips and exclusive guards | FIXED | 37a9eab, 099fc2a | See "moon#1183" below | On tokio the fast path follows `--cross-shard-fast-path`, whose `auto` setting means off there. The `main.rs` comment saying `on` "does nothing" on tokio is now stale |
| moon#1182: FT.SEARCH searches locally first, and the remote legs run synchronously | FIXED | 468c580, 7acb13c | See "moon#1182" below | — |
| moon#1214 item 1: SPSC Notify takes the flume lock on every push | DEFERRED (profiled) | — | See "moon#1214 item 1" below | Re-profile at `--shards 8` on the GCE t2a-standard-8 rig with a dedicated load generator. Build the fix, with a loom model, only if `Shared::send` self-time is above about 2% |

### moon#1162
- `tests/perf_ws8_spanning_del_hooks.rs` has 3 tests.
- RED on the baseline:
  - all 20 deleted docs were still returned (40 hits);
  - FLUSHALL left 32 docs;
  - all 8 queues came back after a restart.
- GREEN on monoio and tokio.
- Unit tests are in `shard::write_hooks::tests`.

### moon#1229
- `tests/perf_ws8_script_flush_fanout.rs` has 2 tests.
- RED: EVALSHA still ran for 23 of 48 keys at `--shards 2` and 36 of 48 at `--shards 4`.
- GREEN at both shard counts on both runtimes.
- `SCRIPT FLUSH BOGUS` now returns Redis's error text, covered by a unit test in `scripting::`.

### moon#1228 item 1
- The unit test is `coordinator::multikey_leg_tests`. RED: `left: []`.
- The end-to-end test is `tests/perf_ws8_mset_bgsave_capture.rs`. RED: 138 of 400,000 keys written after the epoch started appeared in the snapshot.
- GREEN on both runtimes.

### moon#1184
`tests/perf_ws8_spanning_write_merge.rs` counts AOF records per spanning write:

| what | before | after |
|---|---|---|
| MSET records | 1 | 4 (one per owner) |
| per-key SET records | 30 | 0 |
| DEL records | 52 | 4 |
| UNLINK records | 9 | 4 |
| mentions of absent keys | 35 | 0 |

- The unit test was RED with `left: 5, right: 1`.
- AOF bytes: MSET with 10 pairs is 23% smaller. DEL of absent keys went from 51.7 MB to about 0.3 KB.

### moon#1198
- **Item 7:** three SPSC variants that nothing produced are removed, together with about 690 lines of their handler arms.
- **Item 2:** `guard_count_tests::batch_arms_take_one_exclusive_guard_per_command` checks exclusive guard acquisitions:
  - a MultiExecute of 1 command: 2 before, 1 after;
  - a batch of 3 commands: 4 before, 3 after.

### moon#1177
- `fanout_record_tests`:
  - pointer identity: the AOF record is passed on without a copy (RED: `0x…b70 != 0x…c40`);
  - no backlog lock without a replica (RED: `Err(Timeout)`).
- `remote_batch::tests`: `RemoteMeta` is at most 32 B.
- These suites are green:

  | suite | tests |
  |---|---|
  | client_tracking_invalidation | 10 |
  | pipeline_cross_shard_ordering | 18 |
  | multikey_read_cross_shard | 5 |
  | resp3_type_fidelity | 26 |
  | flush_cross_shard_scatter | 3 |
  | cross_shard_command_telemetry_982 | 5 |
  | script_function_fanout | 13 |
  | msetnx_cross_shard_reject | 2 |

### moon#1183
- `tests/perf_ws8_watch_versions.rs`:
  - owner-loop wakes for 200 WATCHes across 3 remote owners went from 603 to 3;
  - the same test pins CAS semantics.
- Unit `watch_versions_tests`:
  - exclusive holds went from 1 to 0;
  - the barrier went from `[false,false,true]` to `[true,true,true]`.
- watch_cas_transactions (10 tests) and watch_container_mutation_926 (14 tests) are green.

### moon#1182
- `tests/perf_ws8_ft_scatter.rs` runs with `MOON_FT_YIELD_CHUNK=64`. Cooperative yields went from 0 to more than 0 (RED: "yielded 0 times").
- Results match HEAD on 29 of 29 queries.
  - KNN and KEYS match exactly.
  - Text and hybrid queries are compared by total and by score sequence. Tied scores come back in a nondeterministic order, even baseline against baseline.
- These suites are green:

  | suite | tests |
  |---|---|
  | vector_db_isolation | 10 |
  | inverted_search_shard_consistency | 6 |
  | ft_search_yield_red | 4 |
  | ft_search_multi_shard_as_of | 2 |

### moon#1214 item 1
- Profiled with perf 6.8: `--shards 4`, SET, pipeline 1, 200 clients, 6,005 samples.
- `flume::Shared::send` under `notify_one` is 5.35% inclusive. Of that, 4.23% is `AsyncSignal::fire`, which is the wake itself and is required.
- At most 0.83% is removable, which is below the noise floor, so no fix was built.

## Measurements (method, reps, raw numbers)
- The numbers are relative only. The host is a 4-vCPU container, the client runs on the same host, and other agents were building at the same time.
- Method (`ab.sh`):
  - a fresh server and data dir for every run;
  - `--shards 4 --appendonly yes`;
  - A = `baseline-ae21476` and B = `ws8-rf-1` (a release-fast build of 7acb13c), run alternately.

| workload | A rps | B rps | AOF bytes A → B |
|---|---|---|---|
| SET -d 256 -P 16 -c 50 -r 100000 (6 reps) | 228.2k 242.5k 264.7k 242.2k 271.7k 225.2k | 246.6k 297.2k 258.4k 298.2k 240.3k 249.9k | n/a |
| MSET ×10 pairs P1 c50 (3 reps) | 19.7k 21.5k 19.1k | 20.9k 20.4k 21.6k | 73.8 MB → 56.8 MB |
| MSET ×10 pairs P16 c50 (3 reps) | 75.7k 104.2k 100.5k | 135.3k 113.1k 62.0k | 98.4 MB → 75.7 MB |
| DEL ×10 absent keys P1 c50 (3 reps) | 22.2k 21.0k 21.5k | 19.6k 20.9k 20.8k | 51.7 MB → ~0.3 KB |

No throughput change is claimed; every requests-per-second difference is within run-to-run noise. The claimed effects are the deterministic ones measured above:
- fewer AOF bytes and fewer AOF records;
- fewer exclusive db guards;
- fewer owner-shard wakes;
- AOF records passed on without a copy;
- no backlog lock when there are no replicas.

A parity probe against redis 7.0.15 covered EXISTS, TOUCH, DEL, UNLINK, MSET and the SCRIPT FLUSH variants. All 15 replies were identical.

## Cross-ownership edits
- **`src/command/vector_search/ft_search/dispatch.rs`** (468c580): `capture_dense_knn_snapshot` becomes `pub(crate)`. The function body is unchanged.
- **moon#1229 carve-out** (19d5d33):
  - FLUSH mode validation in `src/scripting/mod.rs`;
  - `script_flush_fanout` and `is_accepted_script_flush` in `src/server/conn/shared.rs`;
  - a fan-out call in both `try_handle_script` functions (`handler_monoio/dispatch.rs` and `handler_sharded/txn_intercepts.rs`).
- **Remote-dispatch blocks of the connection handlers only** (c04464e):
  - `handler_monoio/mod.rs` uses RemoteBatch and RemoteMeta, moves the request frame instead of deep-cloning it, and drops the `HashMap` import;
  - `handler_sharded/mod.rs` hoists RemoteBatch and `reply_futures`, and builds `aof_bytes` only for local two-db commands.
- **Test producers** (de09be0): the tests in `spsc_handler/aof_admission_tests.rs` and `drain_cap_tests` now send a one-command PipelineBatchSlotted.

## Risks / things the orchestrator must re-check at integration
1. **WS7 merge conflicts** are likely in the remote-dispatch blocks of both handlers (c04464e) and in `try_handle_script` (19d5d33).
2. **`ShardMessage` shape changes.** Any other branch that constructs these variants must adapt:
   - `PipelineBatchSlotted.commands` is now `Vec<Frame>` instead of an `Arc`;
   - `ExecuteSlotted`, `MultiExecuteSlotted` and `PipelineBatch` are removed;
   - `ScriptFlush { ack }` is new.
3. **MQ drop helpers.** `src/shard/write_hooks.rs` calls `mq_exec::auto_drop_mq_streams` and `auto_drop_mq_streams_on_flush`.
4. **Backlog gate invariant (moon#1177).** The backlog is now locked only when `!replica_txs.is_empty() || fanout_hint_active()`. Re-run the replication suites (psync, backlog, replica fan-out) on the merged tree.
5. **The VectorSearch SPSC arm** spawns a local task (`spawn_knn_reply`). It falls back to a synchronous search when there is nothing to capture.
6. **`db_plane::exclusive_count`** is `#[cfg(test)]` only.
7. **A stale comment in `main.rs`** says `--cross-shard-fast-path on` does nothing on tokio. WATCH now follows that flag.
8. **Pre-existing oversize files.** `spsc_handler.rs` is about 4.1K lines (this wave cut about 700), and `coordinator.rs` is about 4.5K lines.
9. **Re-run on the merged tree:**
   - the six `perf_ws8_*` suites;
   - watch_cas_transactions;
   - client_tracking_invalidation;
   - pipeline_cross_shard_ordering;
   - ft_search_yield_red.
10. **Off-convention commit types:** de09be0 and 468c580 are `refactor(...)`, and 099fc2a is `test(shard)`.
11. **No consistency-script rows were added.** No command was added and no reply shape changed. The `tests/perf_ws8_*` suites pin the behaviour.

## Gates at the final code tree
- `cargo fmt --check` passes.
- audit-unsafe: 244/244.
- audit-unwrap: 0.
- `clippy --all-targets -D warnings` and tokio clippy both pass.
- tokio `check --all-targets` passes.
- `cargo test --lib` filtered to `shard::`, `server::conn` and `scripting::`:
  - monoio: 598 passed;
  - tokio: 548 passed;
  - both counts include 16 new unit tests.
- Integration tests were run by name on both runtimes. `pipeline_auto_index` and `hybrid_filter_multishard` need `text-index`, so they run no tests on tokio.

## CHANGELOG bullets
- **Fixed:** a `DEL`/`UNLINK` whose keys span shards, a `FLUSHDB`/`FLUSHALL` fanned out to other shards, and an `HSET`/`HDEL` run on another shard now update vector and text indexes and drop durable queues on every owner shard (moon#1162).
- **Fixed:** `SCRIPT FLUSH` now empties every shard's script cache before it replies. An invalid mode returns Redis's error (moon#1229).
- **Fixed:** the MSET coordinator's local leg now captures BGSAVE pre-images, so a snapshot taken during a spanning MSET stays point-in-time (moon#1228).
- **Performance:** a spanning `MSET`/`DEL`/`UNLINK` sends one sub-command per owner shard and logs one AOF record per owner. A `DEL`/`UNLINK` that removes nothing is no longer logged (moon#1184).
- **Performance:** SPSC batch arms take one exclusive db guard per command. Three shard message variants that nothing produced are removed (moon#1198).
- **Performance:** cross-shard writes move request frames instead of cloning them, and pass AOF records on without a copy. The replication backlog lock is skipped when there are no replicas (moon#1177).
- **Performance:** `WATCH` reads remote key versions through the foreign-read fast path, asks every remaining owner at once, and uses shared guards (moon#1183).
- **Performance:** a multi-shard `FT.SEARCH` sends its remote legs before it searches locally, and every KNN leg runs on the cooperative yielding path (moon#1182).

## Self-evaluation (0–1)
| dimension | score | note |
|---|---|---|
| Completeness | 0.92 | 8 of 9 items fixed. #1214 item 1 is deferred on a measured profile, as the brief allows |
| Clarity | 0.92 | One issue per commit. NOTES covers mechanism, design and risks for each issue |
| Practicality | 0.90 | Every fix has a test that failed before and passes after, on both runtimes. The cross-ownership edits are small and listed |
| Optimization | 0.88 | The deterministic reductions are measured, but a shared 4-vCPU container is too noisy to show a throughput change. Reaching 0.9 needs a clean Linux rig |
| Edge cases | 0.90 | Covers DEL of absent keys, SELECT prefixes, FLUSH modes, CROSSSLOT under WATCH, the tokio fast-path policy and tied scores |
| Self-evaluation | 0.90 | Deviations are stated: commit types, no consistency rows, the tokio WATCH policy |
