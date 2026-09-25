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

---

# PR #1233 review fixes (FIX3-ws8)

- **Branch:** `fix3/ws8`, created from the PR head `d4a2fd3`.
- **Worktree:** `/home/user/wt/FIX3-ws8`. Ports 7480–7499.
- **Notes:** mechanism, design and risks per item are in `NOTES.md`, section "PR #1233 review fixes".
- **Binaries (debug):** each was built with `touch src/lib.rs` first, because the shared target's fingerprints use mtimes and workspace-relative paths, so a plain `cargo build` can relink another worktree's lib.

  | binary | what it is |
  |---|---|
  | `bin/fix3ws8-dbg-d4a2fd3` | the PR head |
  | `bin/fix3ws8-dbg-notify` | R1 applied |
  | `bin/fix3ws8-dbg-nocapture` | R3 red build, with the MSET capture temporarily reverted; never committed |
  | `bin/fix3ws8-dbg-final` (monoio) | built at `2b61945` |
  | `bin/fix3ws8-dbg-tokio-final` | built at `2b61945` |

  `e78da69` changes a unit test only, so the server binaries are the same.

## Per-issue verdict
| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| R1 (BLOCKING, moon#1184): a spanning MSET emits no keyspace `set` event for remote-owned keys | FIXED | 536abe4, 85f9091 | See "R1" below | SET with options (`EX`/`PX`/`NX`/`XX`/`GET`/`KEEPTTL`) emits no `set` event. Redis emits `set`, and also `expire` for `EX`. This predates the PR and is out of scope (probed) |
| R2 (moon#1177): the backlog-lock test was vacuous under `cargo test --lib` | FIXED | 53dd79d | See "R2" below | — |
| R3 (moon#1228): `perf_ws8_mset_bgsave_capture` was flaky | FIXED | 2b61945 | See "R3" below | — |
| R4: nits | FIXED | 86cc5bc, 8903f23 | See "R4" below | Route the four copies of the hook list through `run_post_write_hooks` |
| R5 (found by the gates): WS8's coordinator unit tests poison `slice::tests::foreign_write_applies_and_is_visible_to_a_foreign_read` in one process | FIXED | e78da69 | See "R5" below | — |

### R1 — MSET/MSETNX keyspace `set` events (moon#1184)
**Cause.** `string::mset` and `string::msetnx` never sent a notification.
- On `ae21476`, the only MSET keys notified were the remote pairs, because each went out as its own `SET` leg.
- On `d4a2fd3`, both the per-owner `MSET` legs and the `run_local` slice run `string::mset`, so no key is notified at all.

**Fix.** `notify_set` emits `notify_keyspace_event(STRING, "set", key, db)` once per pair, after `set_string`, in `mset` and in `msetnx`'s set phase. This is the loop redis's `msetGenericCommand` runs: a duplicated pair fires twice, and an MSETNX that sets nothing fires nothing.

**Exactly-once audit.** Each path runs the body exactly once per pair it owns. The per-path table is in NOTES. The paths are:
- local dispatch;
- the coordinator fast path;
- the coordinator's local slice and per-owner legs (`group_by_owner` puts each pair in one group);
- MSETNX on its single owner;
- MULTI/EXEC;
- Lua;
- replica apply, as for SET;
- AOF replay, which queues nothing because there is no listener.

**Test.** `tests/perf_ws8_mset_notify.rs` is the reviewer's test, tightened to redis parity:
- every pair must be notified, and exact `(event, key)` counts are compared;
- a 500 ms grace window after the last expected event, so a duplicate event also fails;
- runs at `--shards 4` and at `--shards 1`.

It covers:
- a spanning MSET with a duplicated key;
- one single-owner MSET per shard, so the all-local fast path is always hit;
- MSETNX that succeeds on each shard, fails on an existing key, and is refused with CROSSSLOT;
- MULTI{MSET, MSETNX};
- `EVAL redis.call('MSET')`.

**Results.**
- RED on `d4a2fd3`:
  - `--shards 4: 0 events for 54 MSET/MSETNX pairs; 53 keys differ from redis (first: ["nk:0: 0 set events, redis fires 2", …])`
  - `--shards 1: 0 events for 42 …; 41 keys differ`
- RED on `ae21476`: 20 of 54 at `--shards 4`, 0 of 42 at `--shards 1`.
- GREEN: monoio `fix3ws8-dbg-final` 2 passed; tokio `fix3ws8-dbg-tokio-final` 2 passed (54/54 and 42/42 events).

**Redis parity.** `scratchpad/probe_notify.sh` subscribes to `__keyevent@0__:*` and runs MSET with a duplicated key, MSETNX success and failure, EVAL MSET and MULTI.
- The event multiset matches redis 7.0.15 exactly at `--shards 4` and at `--shards 1`.
- Replica check: a moon master/replica pair at `--shards 1` versus a redis 7.0.15 master/replica pair. Both replicas emit s1, r1×2, r2 and the MSETNX keys, one event per pair.
- Replies are identical.

**Consistency scripts.** No rows were added. Neither `scripts/test-consistency.sh` nor `scripts/test-commands.sh` has a keyspace-notification section; `notify-keyspace-events` appears in neither.

### R2 — backlog gate predicate (moon#1177)
**Change.**
- `backlog_append_wanted(replicas_empty, fanout_hint)` is pure and is the gate's only definition; the call site uses it.
- `wal_append_and_fanout_bytes` keeps its signature. It reads the hint once and hands it to the private `wal_append_and_fanout_hinted`.

**Tests.**
- The full 4-row truth table.
- The no-lock test, which now passes `hint = false` explicitly and never skips.
- A new test that the hint opens the gate.

**Proof.** Run with `--test-threads=1` plus the `replication::state::` filter, so `test_mark_fanout_active_sets_hint` sets the global first.
- With the predicate inverted, all 3 are RED:
  - `replicas_empty=true fanout_hint=false left: true right: false`;
  - `the write blocked on the replication backlog mutex … left: Err(Timeout)`;
  - `the write must take the backlog mutex … left: Ok(true)`.
- Restored, GREEN: 32 passed, `wal_append_tests` included.

### R3 — de-flaked BGSAVE capture test (moon#1228)
**Cause.** A 30 ms sleep, then blocks of 16 MSETs, lost the race against a save of about 130 ms: a spanning MSET takes about 14 ms during a save on debug.

**What the test does now.**
- **Arming is observed.** It waits until every `shard-<id>.rrdshard.tmp` exists. The writer file and the capture arm are created in one synchronous stretch of the shard thread.
- **Probe after every MSET.** `rdb_bgsave_in_progress` is read through an `INFO persistence` pipelined on the same connection.
- **Retries on a larger keyspace.** Up to 5 attempts, doubling from 400K keys to 3.2M.
- **Only overlapping attempts are judged.** An attempt counts only if at least 16 MSETs landed inside the epoch.
- The capture assertion is unchanged, and the test is not `#[ignore]`d.

**Results.**
- GREEN, debug monoio: 10/10 consecutive runs, each finishing in 1 or 2 attempts (4 runs needed only 1). Per run:
  - 6–52 MSETs landed inside a 400K-key save;
  - 19–114 inside an 800K-key save.
- GREEN, tokio: 1 run under load average ~10, with 3 attempts (3, 6, then 29 MSETs overlapped at 1.6M keys).
- RED with the capture reverted (`fix3ws8-dbg-nocapture`, where both MSET local legs call `string::mset` directly):
  - `11 of 400000 keys hold a post-epoch value … under 16 spanning MSETs`;
  - `77 of 800000 …`.
- RED on `ae21476`: `237 of 400000 …` and `418 of 400000 …`.

**Why the final version pipelines the probe.** A first version used a separate probe connection. It also went 10/10, but two runs needed the 1.6M attempt, and one of them passed with exactly 16. That margin was too thin, so the probe was pipelined and the key cap raised.

### R4 — nits
- **`db_plane.rs`:** `exclusive_count` moved above `guard_depth`'s doc, so the re-entrancy contract documents `guard_depth` again.
- **`write_hooks.rs` module doc** now names both sets of paths:
  - these use `run_post_write_hooks`:
    - the SPSC `Execute`, `MultiExecute` and `PipelineBatchSlotted` arms;
    - every `coordinator::run_local` leg;
  - these four still carry their own list:
    - `handle_connection_sharded_monoio`;
    - `handle_connection_sharded_inner`;
    - `execute_transaction_sharded`;
    - `apply_index_parity_hooks`.

  Routing them through the function is left as a follow-up.
- **Stale names of the removed SPSC variants** (whole-repo `git grep`):
  - fixed:
    - 6 test files;
    - `aof_admission.rs` and `spsc_handler.rs`;
    - `persistence/aof/pool.rs`, in its own commit.
  - kept:
    - comments that describe the removal itself (`dispatch.rs`, `spsc_two_db.rs`);
    - historical records (`CHANGELOG.md`, `.add/tasks/**`, `docs/reviews/**`).
- **Redundant `#[allow(clippy::too_many_arguments)]`:** removed from the three items WS8 added (`coordinate_mset`, `coordinate_multi_del_or_exists`, `wal_append_and_fanout_bytes`). `lib.rs` allows the lint crate-wide, and clippy stays clean on both runtimes.

### R5 — single-process test isolation (found by the gates)
**Symptom.** `cargo test --lib -- shard::` is RED on `d4a2fd3`, 3 of 3 runs, on monoio and on tokio: `foreign_write_applies_and_is_visible_to_a_foreign_read` fails with `left: Some(4) right: Some(1)` (`slice.rs:981`). It passes when run alone.

**Bisect.**
- with `coordinator::multikey_leg_tests`: `Some(4)`;
- with `coordinator::watch_versions_tests`: `Some(2)`;
- with any other `shard::` module: green.

**Mechanism.** PR #1233 added both of those tests. Their `ShardDatabases::new` can win the process-wide L4-registry `OnceLock`, and they write keys into it. The slice test assumed the registered database was empty.

**Fix.** The closure counts what its own write added, under the exclusive guard.

**Order dependence.** On `d4a2fd3` the full single-process tokio gate happened to pass (5534 passed / 1 env failure), so this is latent, not a guaranteed CI red.

## Measurements (method, reps, raw numbers)
No throughput claims. R1 costs one Relaxed load per MSET pair when notifications are off, the same as SET.

R3 overlap data (debug monoio, 10 consecutive runs, spanning MSETs inside the epoch per attempt):

| attempts per run | runs | overlap per attempt |
|---|---|---|
| one (400K keys) | 4 | 19; 52; 50; 20 |
| two (400K, then 800K) | 6 | 9/24; 13/26; 9/114; 14/19; 13/19; 6/23 |

- Save time: about 130 ms at 400K keys, about 240 ms at 800K.
- Arming took 3–62 ms.
- The debug RDB is 14.6 MB per shard per 1.6M keys.

## Gates at the final code commit `e78da69`
- `cargo fmt --check`: pass.
- `audit-unsafe`: 244/244, 0 missing SAFETY.
- `audit-unwrap`: 0.
- `cargo clippy --all-targets -- -D warnings`: pass.
- tokio `clippy -D warnings`: pass.
- tokio `check --all-targets`: pass.

**Lib tests.**
- `cargo test --lib -- shard:: command::string replication::state notify`:
  - monoio: 477 passed;
  - tokio: 466 passed.
- Full single-process lib runs:
  - monoio `cargo test --lib`: 6441 passed / 1 failed;
  - tokio `libtest-singleproc-gate.sh`: 5536 passed / 1 failed.

  In both, the one failure is the root-only `cold_index_rebuild_tests::unreadable_file_…` (TEAM-RULES §0).

**Integration tests, by name.**

| runtime | binary | suites (tests passed) |
|---|---|---|
| monoio | `fix3ws8-dbg-final` | perf_ws8_mset_notify (2), perf_ws8_mset_bgsave_capture (1), keyspace_notifications (11), perf_ws9_keyspace_listener (3), perf_ws8_spanning_write_merge (1), msetnx_cross_shard_reject (2), spsc_two_db (3), oom_bypass_closure (8), wal_kv_db_context_1039 (4), spsc_wake_floor_red (5, 1 ignored), crash_matrix_per_shard_aof `--ignored` (4) |
| tokio | `fix3ws8-dbg-tokio-final` | perf_ws8_mset_notify (2), perf_ws8_mset_bgsave_capture (1), keyspace_notifications (11), perf_ws8_spanning_write_merge (1), spsc_two_db (3), oom_bypass_closure (8), wal_kv_db_context_1039 (4), msetnx_cross_shard_reject (2), spsc_wake_floor_red (4, 1 ignored) |
| tokio + text-index + graph | in-process | pipeline_auto_index (3) |

On tokio, msetnx_cross_shard_reject and spsc_wake_floor_red first failed with `MOONERR diskfull`. This box had 3.4% free disk, below the default `--disk-free-min-pct 5`, and those two suites do not pass `--disk-free-min-pct 0`. With `MOON_DISK_FREE_MIN_PCT=0` both pass. The failure is environmental.

## Cross-ownership edits
- **`src/command/string/string_write.rs`** (536abe4, isolated). A WS10 command-write file; the objective names it for R1.
- **`src/persistence/aof/pool.rs`** (8903f23, isolated). One comment line in WS15's directory.
- **Comment-only edits to other workstreams' integration tests** (86cc5bc):
  - `tests/spsc_two_db.rs`;
  - `crash_matrix_per_shard_aof.rs`;
  - `pipeline_auto_index.rs`;
  - `oom_bypass_closure.rs`;
  - `spsc_wake_floor_red.rs`;
  - `wal_kv_db_context_1039.rs`.

## Risks / things the orchestrator must re-check at integration
1. **WS10 merge in `string_write.rs`.** It gains `notify_set` and two call sites. Any WS10 edit to `mset`/`msetnx` must keep exactly one `notify_set` per pair.
2. **Behaviour change for subscribers.** MSET/MSETNX now publish `set` events, one per pair: for the local slice, at `--shards 1`, inside MULTI and from Lua. This is redis 7.0.15 parity. A consumer that relied on moon's silence will now see the events.
3. **R3 depends on the snapshot temp name** `<path>.rrdshard.tmp` (`snapshot_stream::run_helper`). If it is renamed, every attempt reports "the save ended before every shard armed". That is a loud failure, never a false green.
4. **Disk space.** On this box, disk free near or below 5% trips the default disk-full guard in suites that do not pass `--disk-free-min-pct 0`. Re-run with `MOON_DISK_FREE_MIN_PCT=0` before blaming a change.
5. **File size.** `spsc_handler.rs` went from 4893 to 4938 lines (R2). It was already over the limit before this PR; splitting it is a follow-up.
6. **Re-run on the merged tree:**
   - perf_ws8_mset_notify;
   - perf_ws8_mset_bgsave_capture;
   - keyspace_notifications;
   - perf_ws9_keyspace_listener;
   - `cargo test --lib -- shard::` (the R5 shape);
   - `libtest-singleproc-gate.sh`.

## CHANGELOG bullets
- **Fixed:** `MSET` and `MSETNX` emit one keyspace `set` event per key/value pair, as redis does. This holds on every path: a single shard, keys spread across shards, `MULTI`, Lua, and replicas. A spanning `MSET` had stopped notifying keys owned by other shards (moon#1184).
- **Internal:** the replication-backlog gate is a pure predicate, `backlog_append_wanted`, and its tests no longer depend on the process-wide fan-out hint that other tests set (moon#1177).
- **Internal:** the MSET-during-BGSAVE snapshot test observes when every shard is armed and when writes overlap the save, and retries on a larger keyspace instead of relying on a fixed sleep. Its assertion is unchanged (moon#1228).
- **Internal:** doc and comment fixes in `db_plane`, `write_hooks` and six test files, which also drop names of the removed SPSC variants, plus removal of redundant clippy allows (moon#1198).
- **Internal:** a shard unit test no longer assumes the process-wide database registry starts empty, so `cargo test --lib -- shard::` passes in a single process.

## Self-evaluation (0–1)
| dimension | score | note |
|---|---|---|
| Completeness | 0.95 | Every item is fixed. R5 was found and fixed because the prescribed gate was red. Consistency rows were skipped because neither script has a notification section |
| Clarity | 0.92 | One issue per commit, with the cross-ownership edits isolated. NOTES records the mechanism, design and risks for each item |
| Practicality | 0.92 | Each fix has a red test and a green test. Both runtimes were run by name, and the redis and replica probes match |
| Optimization | 0.90 | R1 costs one Relaxed load per pair when notifications are off. R2 moves one Relaxed load ahead of the bypass. No hot-path allocations |
| Edge cases | 0.92 | Covered: duplicated pairs, a refused MSETNX, CROSSSLOT, MULTI/Lua/replica paths, arming versus a save that publishes early, and restoring dirty keys between attempts |
| Self-evaluation | 0.90 | R3's margin was measured and then widened. R5's order dependence is stated, including the full-run pass on `d4a2fd3` |
