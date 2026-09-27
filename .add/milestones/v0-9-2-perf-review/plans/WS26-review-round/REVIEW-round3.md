# Review round 3: the round-2b fix commits (`00a2148..4e06039`)

**Scope.** Worktree `/home/user/wt/review4`, branch `review/round3` (based on `4e06039`). Commits: 4d271f0 (moon#1280 per-tick scale), 387402b (moon#1276 harness guard), 0ad8b17 + 4c8a16d (moon#1272 barrier reply), a14a14b (moon#1281 R2-1/R2-2), cc26019 (tokio eviction DEL), be2a30f + 61ee9de (moon#1265 give-up, monotonic budget), 843cd4f (test fix). No production code was committed.

**Environment.** Linux container, 4 vCPU x86_64, shared with the orchestrator's gate. **Linux container, not merge bar.** Oracle: redis-server/redis-cli 7.0.15.

**Binaries.**
- `final3-{monoio,tokio}`: HEAD 4e06039. `strings | grep cold_reclaim_files_given_up` confirms the new tree.
- `final2-{monoio,tokio}`: the c672cbd tree, i.e. before this range.
- `review4-luafix-tokio`: a throwaway 5-line fix, used only to show the new Lua test goes green. It was not committed; the tree is clean.

The shared target is `/home/user/moon/target`. The integration test sources compared there are byte-identical to this worktree's (checked with `diff`), and every real-server run pinned `MOON_BIN`.

**Personas applied:** storage-durability (primary), performance, ci-test-integrity, routing-dispatch.

## Verdict

| commit | issue | verdict | severity of worst finding |
|---|---|---|---|
| 4d271f0 | moon#1280 per-tick scale | Restores the duty cycle: a BGSAVE under flood runs 2.2x faster and the lazy-free ratio drops from 33x to 2.4–5x. It costs tail latency, which the commit does not report. Its acceptance test depends on host speed. | MINOR |
| 387402b | moon#1276 harness guard | Holds. A hung listener is cut at 2.006 s (rc 124). | none |
| 0ad8b17 + 4c8a16d | moon#1272 barrier reply | Holds at the 11 sites it names. Two SWAPDB barrier sites and the timeout mapping remain. | NIT |
| a14a14b | moon#1281 R2-1/R2-2 | Holds on all four quadrants. One crash window exists, but it is pre-existing and boot-only. | MINOR (pre-existing) |
| cc26019 | tokio eviction DEL | Correct for the two gates it wires, but **misses the third tokio gate, the script bridge**. | **MAJOR** |
| be2a30f + 61ee9de | moon#1265 give-up, monotonic budget | Closes the one-file case. **Three poisoned files still degrade the shard.** | **MAJOR** |
| 843cd4f | test fix | Correct in spirit, but the retry masks acked-then-lost keys in the range that crosses the death. | MINOR |

**No BLOCKING finding.** Nothing in the range loses an acknowledged write or resurrects a deleted key that the base did not already lose or resurrect. Both MAJORs are incomplete coverage of a round-2b MAJOR, the persona's "read the sibling path" class, and both have red tests.

---

## MAJOR

### MAJOR-1 (cc26019): tokio evictions triggered inside a script never reach the AOF, so they come back after a restart

**Trace.**
- cc26019 made `record_reason_del_conn` available on both runtimes (`src/replication/reason_del.rs:179`). It wired that function into `handler_sharded/mod.rs:2586` and `handler_sharded/write.rs:323`.
- The **third** tokio write gate is the script bridge's `LuaEvictionCtx` eviction gate, `src/scripting/bridge.rs:226-245`. It still has this sink:
  ```rust
  #[cfg(feature = "runtime-monoio")]
  crate::replication::reason_del::record_reason_del_conn(..);
  #[cfg(not(feature = "runtime-monoio"))]
  { let _ = key; }
  ```
  It is passed to both `EvictionRun::async_spill(..).report(..)` (`:257`) and `EvictionRun::plain().report(..)` (`:267`).
- Under tokio, every victim that a script's `redis.call` write evicts is plain-dropped with no `DEL`. The AOF replays each of them back.
- The commit title says "log a DEL for every evicted key". This is the round-2b MAJOR-3 class through a path the fix did not read.
- It is pre-existing and not a regression. Now that the function is ungated, the fix is five deleted lines. The `capture_txn_del` gate is untouched, because its body stays monoio-only.

**Red test.** `tests/review_r3_lua_eviction_aof.rs`, commit 910a12e, `#[ignore]` with `MOON_BIN` pinned. The server runs with `--shards 1 --maxmemory 8mb allkeys-lru --appendonly yes --disk-offload disable`. The test sends 40,000 SETs through 80 `EVAL`s, records which keys read nil, waits 2.5 s, sends SIGKILL, and restarts with `--maxmemory 0`.

| binary | evicted live | came back after restart | result |
|---|---|---|---|
| final3-tokio (4e06039) | 29,175 | **29,171** | **FAILED** |
| final3-monoio (control) | 29,175 | 0 | ok |
| review4-luafix-tokio (throwaway fix: drop the two `cfg` lines) | 29,176 | 0 | ok |

I reproduced it by hand as well (`lua_evict.sh`): tokio DBSIZE was 10,825 live and **39,994 after restart**, twice. Monoio went from 10,825 to 10,824.

```
MOON_BIN=/home/user/wt/bin/final3-tokio cargo test --test review_r3_lua_eviction_aof -- --include-ignored --test-threads 1 --nocapture
```

### MAJOR-2 (be2a30f): three poisoned reclaim files still degrade the shard's spilling

**Trace.**
- A poisoned file is now given up on its **second** death (`ColdIndex::abandon_compactions_after_thread_death`, `src/storage/tiered/cold_reclaim.rs:382-399`).
- Both deaths still go through `take_death` into `RestartSupervisor::on_death` and spend the spill thread's restart budget. That budget is 5 respawns per 10 minutes (`RestartPolicy::DEFAULT`, `src/storage/tiered/spill_thread/supervisor.rs`).
- The verdict is decided in `spill_supervise::after_drain` (`src/shard/persistence_tick.rs:921`). That runs *before* the reclaim tick even takes the culprit, so nothing can forgive the death.
- So N poisoned files cost 2N deaths, and **N = 3 degrades the shard for the life of the process**. That is the same outcome as round-2b MAJOR-2: spilling stops, `allkeys-*` evictions plain-drop keys the operator configured to spill, and reclaim stops.
- Several poisoned files is the likely case, not an exotic one. A reclaim panic is at least as likely to be a decode bug hit by every file of one shape as one bit-rotted file, and the reclaim starts `FILES_PER_TICK = 2` files per tick.

**Red test.** `src/shard/persistence_tick/review_r3_tests.rs::three_poisoned_reclaim_files_do_not_degrade_spilling`, commit ca3d84f. It uses a three-file fixture (ids 5, 6, 7; 10 keys each, 8 deleted) and `PanicPlan::times(ReclaimWrite, 1000)`. It drives the real tick functions and the real respawns.
- The failure on both monoio and tokio (`--no-default-features --features runtime-tokio,jemalloc`) is: "3 poisoned reclaim files DEGRADED the shard's spilling after 5 respawns (files given up process-wide: 3)".
- The surviving keys (2 per file) stay readable. The test checks that before the degrade assertion, so this is availability, not loss.
- The reviewer's one-file test and be2a30f's own tests stay green, since two files cost only 4 deaths.

**Suggested fix (not applied).**
- A death whose `reclaim_running` culprit is set should not count against the spill budget. Peek the atomic in `take_death` before `on_death`, or push the attempt and pop it back.
- Or: after K culprits, stop cold reclaim for the process instead of degrading spilling.
- The round-2b report already offered the first option as "optionally".

```
cargo test --lib review_r3_tests
cargo test --no-default-features --features runtime-tokio,jemalloc --lib review_r3_tests
```

---

## MINOR

### MINOR-1 (4d271f0): the per-tick scale doubles tail latency during a BGSAVE under a write flood; the cost is unmeasured in the commit

**Mechanism.** `advance_snapshot_segments` calls `advance_snapshot_segment` up to 8 times (`src/shard/persistence_tick.rs:247`). Each call already scales its entry and segment budgets up to 16x under a pre-image backlog (moon#1228, `MAX_TICK_BUDGET_SCALE`, `src/persistence/snapshot.rs:125`). That constant's own comment bounds "the worst tick (~1.6 ms of serialization instead of ~100 µs)".

The two scales multiply, to 128x the base budget per tick. The byte budget is capped at 4x per call, which gives 32x. `stream_backlogged` still stops a tick.

There is also a feedback loop. The scale is milliseconds since the previous tick *fired* (`TickLateness::observe`, `src/shard/tick_cadence.rs:199`), and that gap includes the previous tick's own duty time. A tick whose scaled duties take 8 ms or more therefore keeps the next tick at scale 8.

**Measurement.** Script: `tickbench.py`. Setup:
- `--shards 1`, about 2.59M keys × 64 B.
- A saturating flood of `redis-benchmark -t set -P 128 -c 50` on existing keys.
- BGSAVE, then PING RTT on a separate connection until the save finishes.
- Interleaved, 3 reps each.

| binary | BGSAVE s | PING p50 during | p90 during | max during | flood-only p50 |
|---|---|---|---|---|---|
| final2 r1 / r2 / r3 | 2.58 / 2.34 / 2.48 | 16.1 / 14.9 / 15.5 ms | 25.8 / 19.5 / 19.8 | 48.1 / 44.2 / 44.9 | 6.5 / 5.4 / 4.4 |
| final3 r1 / r2 / r3 | **1.14 / 1.18 / 1.07** | **5.6 / 6.8 / 5.2** | **40.1 / 52.0 / 41.6** | **55.2 / 63.6 / 53.3** | 5.2 / 4.8 / 4.6 |

- **Spread:** BGSAVE ±5%; max ±8% within each binary.
- **Sample size:** only 70–160 PINGs per window, so p99 equals the max here and p90 is the steadier statistic.

**Reading.** The fix does what it says: the snapshot window is 2.2x shorter and the median is lower. But p90 rises about 2x and the worst stall rises by about 10–20 ms (+20–40%). This is exactly the trade round-2b warned about ("both extremes are wrong"). The commit reports only the lazy-free ratio.

**Suggested change.** Cap the *composed* budget, for example a per-tick total of at most 16x base whichever scale grants it. Or bound the scaled walk by a deadline, as the lazy-free drain already is. Either way, state the measured tail cost in the commit.

### MINOR-2 (4d271f0, test integrity): the lazy-free acceptance guard is host-speed dependent

With the cap, the drain gets at most 8 × 250 µs per round, so the flooded/idle ratio grows with the length of the test's EVAL.

| binary | EVAL duration | ratio | result (bound 3x) |
|---|---|---|---|
| final2 | 33.9 ms | 33.04 | red (expected) |
| final3 | **61.0 ms** (box loaded by the orchestrator's gate) | **5.15** | **FAILED** |
| final3 | 28.7 ms | 2.35 | pass |

- The commit quoted 2.08.
- `tests/review_r2b_lazy_free_under_flood_1280.rs` is `#[ignore]`d, so it is not in any hosted gate. Whoever runs it on a loaded host gets a red that is not a regression.
- Normalize the bound by the observed EVAL duration: the expected ratio is about `EVAL_ms / 8`, capped. Or state the cap's ceiling as the acceptance bound.

### MINOR-3 (843cd4f, test integrity): the settling retry masks an acked-then-lost key in the range that crosses the death

`set_range_settling` (`tests/spill_thread_supervision_1265.rs:133-158`) re-`SET`s **every** key of the range that fails `EXISTS` after the burst, not just the keys whose reply was `-OOM`.

`a_spill_thread_panic_mid_flight_is_survived` now routes the death-crossing `second` range through it (`:216`). A key in that range that was acked `+OK`, evicted into the dying thread's flush and then **lost** by a broken reconcile/rehydrate would be re-written with the same value before `judge` reads it. That is exactly the #1265 defect the test guards.

Loss in the first range is still caught, and victims are mostly first-range keys (older in LRU order). So the guard is weakened, not disabled. I did not demonstrate this with a mutation run.

**Fix.** Have `set_range` return the indexes whose reply was `-OOM`, and retry only those.

### MINOR-4 (cc26019, pre-existing on monoio, now also on tokio): a per-key 500 ms AOF bound inside the eviction gate, under the db write guard

`record_bytes_conn` (`src/replication/reason_del.rs:300-303`) mints a fresh `AOF_REASON_DEL_BACKPRESSURE_BOUND` (500 ms) **per key** and calls `send_append_bounded_blocking`. The tokio gate runs it inside `do_write`, which holds `s.databases.try_write(sel)` and `ctx.runtime_config.read()` (`handler_sharded/mod.rs:2556-2620`).

With a full writer channel, a write that evicts k victims stalls the shard thread for up to k × 500 ms while holding both locks. parking_lot is fair, so a queued `CONFIG SET` then blocks every other shard's `runtime_config.read()`.

The shard-context `record_reason_del` shares one budget per sweep (#454 review P2.8) for precisely this reason. The monoio `run_write_eviction_gate` has the same shape, so this is parity with an existing defect rather than a new one. It is reachable only when the writer is more than 10k appends behind.

**Fix.** Thread one budget per `evict_to_budget` run into the sink, as `timers.rs:231` does.

### MINOR-5 (a14a14b, pre-existing, boot-time only): a crash between the manifest commit and the generation head re-opens R2-1

**Trace.**
1. `initialize_multi_with_bases` (and `initialize_with_base` at s1) commits the manifest (`src/persistence/aof_manifest/shard_rewrite.rs:120`, `write_manifest`).
2. `main.rs:1897-1902` then appends `MOON.COLDCUT` plus the ledger DELs to each shard's incr, one shard at a time.

A crash between steps 1 and 2 (or between two shards' heads) leaves a committed generation with an empty incr and no cut. The next boot then goes wrong in three ways:
- It is `KvSources::Elsewhere`, so the snapshot and its graves trailer are skipped.
- The rebuilt cold index lists every dead slot, and nothing DELs it. The no-AOF-era deleted cold keys are back.
- No gate and no marker selects the task #56 cold-wins `demote_replayed_cold_shadows`. With a non-empty base, that is a stale read for any key that is hot and cold at once.

**Not a regression.** Before a14a14b, the same window at s>=2 lost every hot key (R2-2), and at s1 it was already there.

**Fix.** Write the heads into the incr files *before* `write_manifest`, which is the commit point. The incr files already exist at that moment.

---

## NIT

1. **moon#1272 residual.**
   - The two SWAPDB barriers in the coordinator (`src/shard/coordinator.rs:3913, 3961`) still answer "fsync barrier failed" on `ChannelFull` and do not use `barrier_refusal_reply`. The "all eleven barrier sites" count excludes them.
   - `fsync_barrier` still maps `AckOutcome::TimedOut` to `FsyncFailed` (`pool.rs:574`). Under a merely slow writer, the reply therefore still reads "fsync failed" (round-2b MINOR-3, second half). This is consistent with the record-carrying always path (`:328`).
2. **moon#1272:** `AOF_BARRIER_BACKLOG_ERR` says "queued". That holds for local legs, where only enqueued indexes join the barrier (`handler_monoio/mod.rs:3448-3463`, `shared.rs:990-999`). It may be false for a remote MULTI leg (`handler_monoio/write.rs:879`, `handler_sharded/write.rs:752`), whose records the remote SPSC arm appends under its own 5 ms bound and can drop. Trace only; not run live.
3. **moon#1272 tests:**
   - `fsync_barrier_always_on_full_channel_is_a_backpressure_refusal` asserts a rise in the process-global `AOF_APPEND_BACKPRESSURE_REFUSALS`. A concurrent lib test can satisfy it, which weakens the red but never causes a false failure.
   - No test pins the reply at any of the 11 sites. Reverting one site to `AOF_FSYNC_ERR` stays green.
4. **cc26019:** the module doc `src/replication/reason_del.rs:60-64` still says "the `record_reason_del*` flavors keep their whole-function gate because every one of their call sites is itself monoio-only". That is now false.
5. **cc26019:** `handler_single.rs:1206, 2669` build `EvictionRun::plain()` with no sink. They are only reachable through `listener::run_with_shutdown`, whose only callers are tests (production uses `run_sharded`), so this is test-only.
6. **4d271f0:**
   - The idle park's one-shot does not update `prev_fired`, so the first timer tick after a park gets scale up to 8. That is harmless, because the loop is idle.
   - `a_late_tick_scales_the_per_tick_duties_up_to_the_cap` tests `observe` alone. Nothing but the ignored real-server test pins that `event_loop.rs` passes the scale on both runtimes.
7. **a14a14b:** `initialize_multi_with_bases` builds each shard's full RDB in one `Vec` at boot, a transient of about one shard's dataset, one shard at a time. This matches the s1 `initialize_with_base`. The only guard of the `main.rs` wiring is the ignored `review_r2a_cold_graves` suite.

---

## Checked and holding (with proof)

### a14a14b (storage-durability, primary)

**Base contents** (`save_to_bytes`, `rdb.rs:63`):
- Hot keys only; cold keys are covered by `MOON.COLDCUT` authorizing listed files.
- Keys expired at `now_ms` are filtered out.
- Every database is written behind its `DB_SELECTOR`. `db_idx as u8` is safe because `MAX_DATABASES` caps it (`config.rs:1081`).
- The cold wiring is still detached while the base is serialized, the same as at s1.

**Head DELs** (`for_each_cold_delete_chunk`, `fold_stream.rs:272`):
- Only non-alive ledger keys (hot, in flight or cold elsewhere are skipped) and hot-expired shadows are DEL'd. A hot key in the base is therefore never DEL'd by the head.
- Hot∩cold keys resolve hot-wins at the end of replay (`finish_replay_cold_reconcile`).
- A key re-spilled after the head (`DEL k`, then `SET k`, then `MOON.SPILLED F9 k`) replays to the right value. The same shape as a fold's head.

**Moved reattach.** The only code that now runs with the wiring attached, where it was detached before, is `seed_generation_head` / `seed_generation_head_if_fresh` and their closures (`cold_file_watermark` reads no db; `fresh_deletes` is the intended reader). Nothing between the new and old reattach points assumed `cold_index == None`.

**Crash between base and manifest.** No manifest means the next boot redoes the whole initialization: tmp+rename overwrites, and incr files are truncated. The snapshot is still there. Safe.

**no→yes switch, multi-db + TTL** (`switch.sh`). Setup: 200 keys in db0, 200 in db7, 50 `SETEX 100000` in db3, and 50 `PSETEX 2500` that expire before boot B. Then BGSAVE, boot A with `yes`, write `after:*`, SIGKILL, boot B.

| binary | s4 boot B | s1 boot B |
|---|---|---|
| final2-monoio | **db0=1 db7=1 db3=0** (R2-2) | all kept |
| final3-monoio | db0=201 db7=201 db3=50, TTL 99995, short keys expired | same |
| final2-tokio | **db0=1 db7=1 db3=0** | all kept |
| final3-tokio | db0=201 db7=201 db3=50, TTL 99995, short keys expired | same |

**`review_r2a_cold_graves` on final3, all 4 quadrants: 12/12 green**, 0 probes back at boot A and B, and `hot:control` kept. The commit claimed only monoio; tokio s4 and s1 are also green. `r2a_second_aof_boot_keeps_cold_dels` on final2-monoio s4 is red (104/104 back, `hot:control=None`), so the suite discriminates.

### cc26019 (the two wired gates)

**The report sink fires for plain drops only** (`report_plain_drop` call sites, `eviction.rs:1200`). Spilled victims never get a DEL: the `eviction.rs:3062` test pins this. So a spilled key cannot be deleted by replay.

**Ordering and DB selection.**
- The DEL is enqueued synchronously under the db guard, before `dispatch` (`handler_sharded/mod.rs:2586-2620`). Any later write to the same key is logged after it.
- `sel_db` is the evicted db; the victims come from `try_write(conn.selected_db)`.
- The shard is the local one (`with_shard`), and the per-shard writer matches it.
- The replication leg stays monoio-only (`record_bytes_conn` `cfg`). tokio has no master PSYNC.

**Results.** `review_r2b_spill_degraded_aof_1265` on final3-tokio: 2/2 ok. The degraded case had 25,115 keys nil live and 0 came back; the plain case had 29,175 nil and 0 came back.

**Residual race** (observation, not ranked; strictly better than before). Connection B's `SET k` may park on a full channel after releasing the guard. Connection A then evicts `k` and its bounded-blocking DEL enqueues first, so replay resurrects `k`. monoio has the same shape.

### be2a30f + 61ee9de

**Culprit attribution.**
- `reclaim_running` is set before `run_job` and cleared after the answer is sent (`spill_thread.rs:806-813`).
- It is read with `swap` only when `is_dead()`, after that function's Acquire fence.
- `cold_reclaim_tick` runs before `respawn_if_due` in the same function (`persistence_tick.rs:763-778`), so the first dead tick always takes the culprit before a respawn.
- Later backoff ticks read `None`.
- A spill-side panic leaves `NO_RECLAIM_JOB`, so nothing is blamed.

**Give-up safety.** Giving up only adds the file to `skip`. The live keys stay readable and the dead slots stay in the ledger, whose DELs go out with every fold. No correctness depends on compaction. My MAJOR-2 test checks that the surviving keys read back.

**SWAPDB/FLUSHALL.** `suspects` lives in the per-db `ReclaimState`, travels with the index and is matched only against `in_flight`.

**Monotonic clock.**
- `clock_ms` is `Instant`-based. Every production caller passes it (`spill_supervise.rs:88, 105`); the fixed numbers in the unit tests are self-consistent.
- `observe` never runs backwards.
- A VM suspend does not age the window, which is the conservative direction.

### 4d271f0

**Hot-path allocation.** The rule covers `src/command`, `src/protocol`, `src/shard/event_loop.rs` and `src/io`. The diff adds none there: `event_loop.rs` gains one `u32`, and `connection.rs` adds a field to the existing INFO `format!`, which is not a hot path. The tokio gate closures in cc26019 live on the stack, and `serialize_del` allocates only per dropped victim with AOF on, the same as monoio.

**Runtime parity.** Both tick arms pass the scale. On monoio the one-shot is scale 1. tokio's interval arm is unchanged otherwise.

### 387402b

`bash -n` is clean. A Python listener that accepts and never answers made `cli_bounded 2 -p 7690 PING` return rc 124 after 2.006 s in `timeout` mode. The native `-t` path is now only a fallback, with a note.

---

## Tests added (branch `review/round3`)

| commit | test | at 4e06039 |
|---|---|---|
| ca3d84f | `src/shard/persistence_tick/review_r3_tests.rs::three_poisoned_reclaim_files_do_not_degrade_spilling` (+1 `mod` line in `persistence_tick.rs`) | **red** on monoio and tokio (degraded after 5 respawns) |
| 910a12e | `tests/review_r3_lua_eviction_aof.rs` (`#[ignore]`, `MOON_BIN`) | **red** on tokio (29,171 of 29,175 back); green on monoio (control); green on the throwaway-fix tokio binary |

- Both are `rustfmt`-clean, and `cargo clippy --lib --tests --test review_r3_lua_eviction_aof -- -D warnings` (monoio) is clean.
- Scripts used (scratchpad, not committed): `tickbench.py`, `switch.sh`, `lua_evict.sh`.
- Hand-started servers used ports 7601–7690, and all were stopped.

## Not checked

- Windows and MSRV 1.94.
- The hosted matrix.
- A kill inside the MINOR-5 window: trace only.
- MINOR-3 by mutation, i.e. a broken rehydrate with the new retry.
- NIT 2 live.
- A redis-cli 7.4+ binary.
- A green variant for MAJOR-2's fix: the fix shape is described, not built.
- Replication full-sync interplay with the give-up.
- A perf profile or objdump of the scaled tick; the latency numbers are black-box.

## Self-evaluation

| | score | note |
|---|---|---|
| Completeness | 0.91 | All six persona steps were run. The durability hunt covered all four quadrants for a14a14b. Sibling-path reading found MAJOR-1. |
| Clarity | 0.92 | |
| Practicality | 0.92 | Both MAJORs have red tests. MAJOR-1 has a checked 5-line fix; MAJOR-2 has a concrete fix shape. |
| Optimization | 0.90 | The per-tick scale's trade-off is quantified with interleaved A/B (3 reps) and spread stated. The sample count per window is small, so p90 is used rather than p99. |
| Edge cases | 0.90 | Covered: multi-db, TTL/expiry, crash windows, clock steps, culprit attribution across respawn, and ordering races. |
| Self-evaluation | 0.90 | Two items are labelled trace-only (MINOR-3's masking and NIT 2) rather than overstated. The severity of MAJOR-2 is a judgment call: same consequence as round-2b MAJOR-2, with a narrower trigger. |
