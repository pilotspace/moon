# Review round 2b: moon#1280, moon#1265, moon#1272, moon#1276

**Scope.** Worktree `/home/user/wt/review3`, detached at `ad55d3d`; base main `273e6bc`. The review tests are on local branch `review/round2b` (5 commits, listed at the end). Nothing was pushed, and no production code was changed.

**Environment.** Linux container, 4 vCPU x86_64, shared with other agents. This is **not the merge bar**: no moon-dev VM, no hosted matrix. Tools: redis-server and redis-cli 7.0.15, bash 5.2, and shellcheck 0.11 from pip.

**Binaries.**
- `/home/user/wt/bin/r2b-{monoio,tokio}` are release-fast builds of `ad55d3d`.
- Because the target dir is shared, cargo reused artifacts from `/home/user/wt/review2`. That tree is at the same `ad55d3d`, plus one `#[cfg(test)] mod` line, so the binaries are functionally `ad55d3d`.
- The baselines are `base-{monoio,tokio}`, built at `273e6bc`.

**Timing numbers.** They come from a noisy shared host. Every A/B was interleaved and/or taken as a same-binary ratio.

## Verdict

| commit(s) | issue | verdict | blocking? |
|---|---|---|---|
| 80404b6 + 463039c | moon#1280 tick catch-up | The stall fix is correct: no duty is dropped, and each is at most one period late. But it **trades away the duty cycle of the per-tick budgeted work under a saturated loop**, with no measurement of that cost (MAJOR-1). | Needs a capped catch-up scale, or a measured decision to accept the cost |
| 26618c3 + 8bc0a7a | moon#1265 spill supervision | The state machine, the reconcile and the moon#1253 invariants hold under every scenario I tried. Two gaps: **one poisoned reclaim file degrades a shard in about 3 s** (MAJOR-2), and **on tokio a degraded shard's plain drops never reach the AOF** (MAJOR-3; the root cause is pre-existing) | Fix MAJOR-2 before merge; file MAJOR-3 |
| 6590ad0 | moon#1272 AOF backpressure reply | Correct for everysec/no. `always` barrier refusals still answer with the fsync text (MINOR-3). | No |
| 3f08174 | moon#1276 harness guard | Correct and shellcheck-neutral. The native `-t` path only bounds the connect (MINOR-2). | No |

No BLOCKING findings: nothing loses data that the base did not already lose, and nothing crashes.

---

## MAJOR

### MAJOR-1 (moon#1280): under a saturated loop, the lazy-free drain and the snapshot walk get one budget per round instead of one per millisecond

**Mechanism.**
- The lazy-free drain frees at most `LAZY_FREE_TICK_BUDGET` (250 µs) per tick. The snapshot walk advances one entry/segment/byte budget per tick.
- A loop round longer than the 5 ms grace makes every tick late. Examples: back-to-back long commands, or a heavy pipelined flood; at `-P 128 -c 50`, INFO `shard_tick_late_total` grew by about 100 per second.
- Under Burst, the late interval replayed the missed ticks, so both duties kept their wall-clock duty cycle. Under Skip they get one slice per round.
- That is exactly the moon#1221 review-F2 condition: "at one 250 µs slice per 10 ms the drain ran at 2.5% duty and held the memory ~10x longer". It is now reached through saturation instead of through the idle park.
- WS24's SUMMARY says the effect is "intended", but it never measured it.

**Failure scenario.** A workload with 20–30 ms commands does an UNLINK of large values, or runs a BGSAVE. Memory stays charged 10–20× longer, and the snapshot's pre-image/COW window grows 5×. With maxmemory set, the late lazy-free keeps `used_memory` high, so evictions or OOM refusals start earlier than on 273e6bc.

**Reproducing test.** `tests/review_r2b_lazy_free_under_flood_1280.rs`, marked `#[ignore]`, with MOON_BIN pinned. It UNLINKs 8 hashes × 400K fields (about 380 MB) and times how long 70% of that memory takes to be freed. It measures once idle and once while one client runs roughly 25 ms `EVAL` loops back to back, on the same binary. Bound: 3×.

| binary | idle | under long commands | ratio | verdict |
|---|---|---|---|---|
| base-monoio (273e6bc) | 0.37–0.39 s | 0.40–0.43 s | 1.07–1.10 | pass |
| **r2b-monoio (ad55d3d)** | 0.33–0.38 s | **3.61–8.06 s** | **10.8–21.4** | **FAIL** |
| base-tokio | 0.38 s | 0.30 s | 0.78 | pass |
| **r2b-tokio** | 0.36 s | **7.68 s** | **21.5** | **FAIL** |

**The snapshot walk measured by hand** (`--shards 1`, 632K keys, BGSAVE, same EVAL stream):

| binary | BGSAVE idle | BGSAVE under long commands |
|---|---|---|
| 273e6bc | 0.73–0.76 s | 0.74–0.76 s |
| ad55d3d | 0.70–0.78 s | **4.02–4.21 s** |

**The trade-off, for the fix discussion.** In the same window, base ran 38 EVALs and the new binary ran 274. Burst's replay (about 5 ms of drain per round) was stealing foreground time. Both extremes are wrong.

**Suggested fix.** Give the budgeted per-tick duties the same treatment expiry already gets:
- Compute a per-tick `catch_up_scale(ms_since_prev_tick, 1, CAP)` with a small cap, 4–8.
- Multiply the lazy-free deadline budget and the snapshot entry/segment budget by it. Keep the byte budget cap.

A 3 s stall then costs at most 8 × 250 µs = 2 ms of catch-up instead of 3,000 ticks, and a saturated loop keeps 4–8× more duty. Re-run this test and `perf_ws24_tick_catchup` after the change.

### MAJOR-2 (moon#1265): a fault confined to one cold-reclaim job degrades the shard's spilling within about 3 s

**Mechanism.**
- The issue itself names "a corrupt spill file read by cold reclaim" (moon#1240) as the new panic source, and such a fault is deterministic.
- On a death, `cold_reclaim_tick` calls `abandon_compactions_in_flight()`, which only clears `in_flight`. The comment says so: "the files are not given up, so the next incarnation compacts them".
- So every respawn re-plans the same file and dies again. After 5 respawns (backoffs of 100 + 200 + 400 + 800 + 1,600 ms) the shard is **DEGRADED for the life of the process**:
  - spilling stops;
  - `allkeys-*` evictions plain-drop keys the operator configured to spill;
  - `noeviction` answers `-OOM`;
  - cold reclaim stops, so dead slots are never compacted.
- A reclaim-only fault takes down an essential duty (spilling) to protect an optional one (compaction).

**Reproducing test.** `src/shard/persistence_tick/review_r2b_tests.rs::a_poisoned_reclaim_job_does_not_degrade_spilling`. It uses the WS25 fixtures, `PanicPlan::times(ReclaimWrite, 1000)` standing in for one bad file, the real tick functions and the real respawns.
- **Red at ad55d3d on monoio and tokio:** "DEGRADED the shard's spilling after 5 respawns".
- **Green** with a throwaway variant where `abandon_compactions_in_flight` also inserts those ids into `reclaim.skip` (checked locally, not committed).

**Suggested fix.**
- Remember which file ids were in flight at each death. On the second death with the same file in flight, give that file up (`abandon_compaction(id, true)`).
- Giving up on the first death would break WS25's own `a_reclaim_write_in_flight_at_the_panic_...`, which expects a retry.
- Optionally, do not count a death during reclaim I/O against the spill budget until the file has been given up.

### MAJOR-3 (moon#1265 on tokio; the root cause is pre-existing): plain-drop evictions on the tokio write gate never reach the AOF, so evicted keys come back after a restart

**Mechanism.**
- #1265 routes a degraded shard's async-spill arm to `evict_one_with_spill(.., on_plain_drop)` when `sender.is_disconnected()`. The commit says "(evicting policies plain-drop with the DEL reported)".
- Two tokio call sites build `EvictionRun::async_spill(..)` **without `.report(..)`**, so the no-op sink runs:
  - `src/server/conn/handler_sharded/mod.rs:2589`, the per-command write gate;
  - `src/server/conn/handler_sharded/write.rs:329`, `mq_write_gate`.
- No `DEL` is appended, and the AOF replays the evicted keys back. redis propagates every eviction as a DEL.
- The monoio gates (`run_write_eviction_gate`, `spsc_handler`, `scripting/bridge.rs`) all pass a sink.

**Reproducing test.** `tests/review_r2b_spill_degraded_aof_1265.rs`. Steps:
1. Crash-loop the spill thread until the shard is degraded.
2. Write 6,000 more keys under 8 MiB `allkeys-lru` with `appendonly yes`.
3. Record which keys read nil live.
4. Wait 2.5 s, SIGKILL, and restart with `--maxmemory 0`.

Results:
- **tokio: FAILED 3/3.** 4,861–5,432 of about 25K evicted keys came back.
- **monoio: passed.** 0 of 26,580 came back.

**Pre-existing without #1265.** The plain arm on the same tokio gate (`handler_sharded/mod.rs:2601`, `write.rs:334`, `EvictionRun::plain()` with no report) has the same bug with disk-offload off. By hand: `--maxmemory 8mb allkeys-lru --appendonly yes`, SET flood, SIGKILL, restart without maxmemory:

| binary | live DBSIZE | DBSIZE after restart | evicted_keys |
|---|---|---|---|
| base-tokio (273e6bc) | 10,700 | **39,185** | 28,927 |
| r2b-tokio | 10,700 | **39,212** | 28,948 |
| r2b-monoio | 10,700 | 10,699 | — |

**Suggested fix.** Add `.report(&mut |key| record_reason_del_conn(&ctx.repl_state, ctx.shard_id, ctx.num_shards, ctx.aof_pool.as_ref(), db, key))` to all four tokio sites, copying `run_write_eviction_gate`. This is not a one-liner, so I did not apply it. File it separately; it is the tokio leg, not the one that ships, but it breaks durability parity.

---

## MINOR

### MINOR-1 (moon#1265): a wall-clock step back erases the restart budget

The supervisor is driven by `storage::entry::current_time_ms()`, the shard's cached `SystemTime`. `forget_before` pops every attempt stamped after `now`. After an NTP step back or a VM restore, a crash loop gets a fresh 5-respawn budget; each step back grants another 5. A step forward of 10 minutes or more does the same.

- **Test:** `review_r2b_tests::a_wall_clock_step_back_does_not_reset_the_restart_budget`. It is red at ad55d3d: a 60 s step back after 5 respawns gives `RespawnAt` instead of `Degrade`.
- **Fix:** feed the pure supervisor the monotonic `tick_cadence::LoopClock` milliseconds (it is clock-injected already).

### MINOR-2 (moon#1276): the native `-t` path does not bound a hung server, and `-t` arrived in 7.4, not 7.2

**The version.** `-t` is absent from redis 7.2.5's `redis-cli.c`. It is present in 7.4.0 and 8.0.0, at `config.connect_timeout` → `redisConnectWrapper` → hiredis `redisConnectWithTimeout`. The NOTE/WARNING texts ("added in 7.2", "Install redis 7.2+") are therefore wrong. A 7.2.x user sees "redis-cli 7.2.x has no '-t' (added in 7.2)".

**The bound.** `-t` is a **connect** timeout only; nothing sets a command/read timeout. With redis-cli 7.4+, which is brew's default 8.x on macOS (the primary dev platform), `harness_probe_redis_cli` picks `native`. A server that accepts but never answers then hangs `cli_bounded`, `aux_start`'s PING wait and the moon#600 liveness rows forever. That is exactly the failure those rows exist for.

- Only the fallback path is a whole-command bound. I verified it: redis-cli 7.0.15 plus GNU timeout against a Python listener that accepts but never answers returned rc 124 after 2.00 s.
- The native-path claim rests on the source; I had no 7.4+ binary to run.

**Fix.** Prefer `timeout`/`gtimeout` whenever one is present, and add `-t` (when supported) only as an extra connect bound. Correct the version strings.

### MINOR-3 (moon#1272): `appendfsync always` refusals still say "fsync failed"

There are 11 raw `AOF_FSYNC_ERR` sites left, all on `fsync_barrier` failures:
- `handler_monoio/{mod.rs:4829, write.rs:882, dispatch.rs:2050}`
- `handler_sharded/{mod.rs:1786,3391, write.rs:735}`
- `handler_single.rs:2271`
- `shared.rs:1002,4934,4970`

`fsync_barrier` maps `try_send_append_sync`'s `ChannelFull` (writer backlog) and its `TimedOut` (slow fsync) to errors that answer `-ERR AOF fsync failed; write not durable`. They are also not counted in `aof_append_backpressure_refusals`. Under `always` this is the same operator-misleading text #1272 is about.

The new `AOF_BACKLOG_ERR` text ("not queued for persistence") would be false there, because the records were queued. It needs a third text, for example "write queued but not confirmed durable: AOF writer backlogged". It is deliberately out of WS23's scope, but the issue's "check every mapping site" includes these.

### MINOR-4 (moon#1280; the root cause is pre-existing): the expired-key backlog drains at 1% duty; the 4× cap is immaterial

I loaded 1.84M keys with `PX 4000`, SIGSTOPped for 6 s so all of them expired during the stall, then SIGCONTed.

| binary | DBSIZE drop rate | time to clear the backlog |
|---|---|---|
| base-monoio | ~7K/s | ≈ 4 min |
| r2b-monoio | ~7K/s | ≈ 4 min |

Burst's 60 replayed cycles on base did not visibly help, and the new 4× first sweep changes nothing that matters.
- **Correctness is unaffected:** reads lazily expire, and redis's DBSIZE also counts expired-but-present keys.
- **Memory:** it stays charged for minutes. redis's active expire is adaptive, up to 25% CPU in the slow cycle.
- **Recommendation:** file this separately (expiry duty cycle), not against #1280.

---

## NIT

1. **moon#1276: a TERM trap waits for the foreground child.** bash defers the trapped `exit 143` until the foreground child exits. Measured: SIGTERM 2 s into a `sleep 8` gave a 6.34 s cleanup delay. It then worked (rc 143, the aux server was stopped). During a long `redis-benchmark` row, a CI that escalates to SIGKILL would still leak. There is also a sub-millisecond untracked window between `"$@" &` and `AUX_PIDS+=`.
2. **moon#1272: three look-alike INFO names.** `aof_backpressure_refused` (not executed), `aof_backpressure_dropped` (records) and `aof_append_backpressure_refusals` (applied writes) are easy to mix up; one line in `docs/guides/monitoring.md` should disambiguate them. For a multi-record command refused mid-way, "not queued for persistence" is only partly true; this is pre-existing partial-application behavior.
3. **moon#1280: `instantaneous_ops_per_sec` samples differently from redis.** It is a 1 s delta on shard 0; redis averages 16 samples of 100 ms. On tokio the first `wal_sync` tick fires at t≈0 with 0 ms elapsed, so `rate_per_sec` returns the raw delta (tiny, harmless). Acceptable; document it.
4. **moon#1265: `spill_thread_alive` changed meaning.** It now means "none down right now", so it flaps during a backoff. That is documented. Alerts should move to `spill_thread_degraded`.
5. **Pre-existing: the OOM text differs from redis.** moon's `oom_error()` has no trailing period; redis 7.0.15 answers `-OOM command not allowed when used memory > 'maxmemory'.`, which I confirmed live.
6. **Evidence caveat.** Against the base binaries, WS24's and WS25's real-server suites are red because their INFO fields are absent (the 1265 suite fails in 0.40 s). The **behavioral** red (Burst restored, 1,502 / 1,507 ticks) is only in the authors' notes; I did not rebuild with Burst restored.

---

## Checked and holding (with the proof used)

### moon#1280

**Duty inventory.** I traced every tick site on both runtimes.

| duty | why it holds |
|---|---|
| WAL 1 ms flush | Buffer-based: one tick flushes everything. |
| WAL fsync | 1 s `Cadence` on monoio, a Skip interval on tokio. |
| appendfsync everysec | The AOF writer's own deadline loop; untouched. |
| Blocked-client timeouts | Deadline-based, 10 ms cadence. |
| Active expiry | Elapsed-based 100 ms with a scale ≥ 1. |
| Auto-save | `last_save` Instant. |
| Replication heartbeat/ACK | Sleep loops; untouched. |
| Cluster gossip | Skip interval. |
| Client idle timeout | 1 s chore over timestamps. |
| Spin governor, autovacuum | Own Instant windows. |
| Cold orphan sweep | First run one interval after loop entry, as the counter did; tokio still sweeps at t=0 (unchanged divergence). |

**Liveness.**
- `race2` polls the tick first (`runtime/race.rs:49`), so a due tick always wins against a constant stream of notify wakes.
- On idle exit a fresh interval is created (`event_loop.rs:2260`), and `tick_deadline=None` on the idle one-shot keeps the burst stat clean.
- `Cadence::new(period.max(1))`. `warm_poll_ms` is clamped to ≥ 1000 and `autovacuum_interval_secs` to ≥ 1 (`event_loop.rs:968,994`), so no period collapses to 1 ms.

**Clock behavior.**
- `LoopClock` is `Instant` (CLOCK_MONOTONIC) with `saturating_duration_since`, so a backwards step is impossible.
- A VM suspend does not advance it, so there is no catch-up at all, the same as Burst. Wall-clock TTLs then expire lazily or at the base rate (MINOR-4).

**`clippy.toml`.**
- It is new, with no prior file, and only sets `disallowed-methods`, so no default changes.
- No raw constructor is left in `src/`, `tests/`, `benches/` or `examples/`.
- `console/` is JS. `fuzz/` and `sdk/rust` are not clippied in CI. fuzz would inherit the root file (clippy walks up) and resolve `tokio::` through moon.
- My review tests are clean under `cargo clippy --lib --tests -D warnings` (monoio).

**moon#1279 interplay.** `observe_boot_fold_view` (`event_loop.rs:843`) runs before loop entry and `ChoreCadences::new` (`:1224`); no spill can mint an id in between (no connections are accepted yet). The baseline is the boot counter on both runtimes, independent of the sweep cadence.

**Suites.** `perf_ws24_tick_catchup` (3), the `tick_cadence` units (10) and the `runtime::interval` units (4) are green on r2b-monoio and r2b-tokio. `perf_ws16_bgsave_capture` (8) is green on monoio.

### moon#1265

**Death at each point.**
- Completions are **per file** (`flush_buffer` pushes one per DataFile). A death between completion sends therefore splits only between files: a lost request's slot is always in an unlisted file, as the reconcile assumes.
- The drain is unbounded (`drain_completions` loops `try_recv`), and `was_dead` is sampled before it. After the drain nothing of the dead incarnation is still queued.
- `after-send`: the completions are applied and the watermark is left behind; the next incarnation raises it.

**Channel order.** Channel order equals id order: all ids are minted on the shard thread, and the dead thread took the head of the queue. So a requeued id is never below `done_below`, and the prune cannot eat a requeued superseded entry.

**Degraded mode.**
- A double death while in backoff is impossible: `take_death` returns `None` outside `Running`.
- A failed spawn that spends the budget reconciles as a degrade.
- Dropping `ThreadEnds` disconnects every clone, and `is_disconnected` is one atomic load per victim.
- `noeviction` still OOMs, through `evict_one_with_spill(.., None)`.

**Shutdown in backoff.**
- The queue is not flushed (documented); `appendonly yes` covers it, since async spill only runs under AOF (`evict_to_budget`: `appendonly != "yes"` → durable batch/plain).
- The gauges stay balanced (`retire_for_shutdown`).
- The panic hook in `main.rs:116` aborts only for `shard-*` and `cluster-ctl` threads; the spill thread is `spill-N`. No profile sets `panic = "abort"`.

**Memory accounting.** Rehydration clears `spill_inflight` (un-bills it) and then `db.set` bills the entry. WS25's test asserts `pending_spill_bytes == 0` after a degrade.

**moon#1269 / moon#1257 interplay (rehydrate outside dispatch).**
- A victim evicted under an armed epoch had its pre-image captured at eviction (moon#1257), so a later `db.set` rehydrate cannot corrupt the epoch image. This is the same path as the pre-existing failed-write rehydrate, covered by `eviction_capture_tests` (async-spill write failure).
- *Residual, not a finding:* consider a key that was in flight before the epoch armed and is rehydrated after the walk passed its range. It is in neither the `.rrdshard` rows (in-flight payloads are never serialized) nor the cold manifest.
  - AOF recovery covers it: the rewrite base includes in-flight payloads (moon#1223).
  - Whether a replication full-sync snapshot can miss it was **not verified**.

**moon#1281 interplay.** The dead thread's file is unlisted, so no grave is involved. The startup orphan sweep removes it, and no `MOON.SPILLED` marker was written, because markers are written on apply.

**Suites, both runtimes.**
- `spill_thread_supervision_1265` (2): green.
- `crash_recovery_cold_del_inflight_1253` with `MOON_TEST_COLD_DEL_SPILL_PANIC=1` (3): green.
- Units on monoio: `spill_supervise` (5), `spill_thread` (33).

### moon#1272

**Sites.** Every `send_append_group` / `try_send_append_durable` producer maps through `append_refusal_reply`/`_frame`: monoio, sharded and single handlers, `shared::persist_txn_aof` (MULTI and scripts), and the coordinator local legs (7 sites). The replication stream receives the record before the AOF append, so "applied in memory" is accurate.

**Client behavior.**
- `MOONERR` already prefixes moon's other backlog replies (`AOF_APPEND_LOST_ERR`, moon#769), so there is no new prefix.
- Clients: redis-py raises `ResponseError`; Jedis `JedisDataException`; Lettuce `RedisCommandExecutionException`. go-redis retries only LOADING, READONLY, CLUSTERDOWN, TRYAGAIN and MASTERDOWN, so no client auto-retries an applied write. That is correct given "applied in memory".

**Suites.** `aof_backpressure_reply_1272` is green on r2b-monoio and r2b-tokio and red on base-monoio; the `aof::refusal` units (4) are green.

### moon#1276

**Static checks.**
- `bash -n` passes on all three files.
- shellcheck 0.11: the library is clean. Relative to 273e6bc, the two scripts gain **zero new warnings** (diffed per message class).
- Scan for bash-4-only idioms (`mapfile`, `declare -A`, `[[ -v`, `${x,,}`, `local -n`, `|&`, `&>>`, `wait -n`): none added.
- bash 3.2 itself was not executed; the download was blocked by the egress policy. By inspection `local a=()`, `a+=()`, `${a[@]+"${a[@]}"}` and `${#a[@]}` are 3.2-safe.

**Cleanup paths, run for real.**
- SIGTERM of a sourced mini-harness mid-leg: rc 143, the aux server stopped.
- A `set -e` death inside a function: FATAL line, the aux server stopped.
- SIGINT of the real `test-consistency.sh` at 0.05 s and 0.3 s: rc 130, nothing left on 7731/7732.
- I first suspected an unbound `$RUST_PID` in the EXIT trap; retracted, because `RUST_PID=""` is initialized at lines 69/75.

**`kill_port_servers`.** It is anchored `^<escaped bin> --port N( |$)`, so it does not match port 64001 for 6400, the harness's own argv, or a MOON_BIN spelled differently. It can only hit the same binary on the same port, which already conflicts under SO_REUSEPORT.

---

## Review tests (branch `review/round2b`, based on `ad55d3d`)

| commit | test | at ad55d3d |
|---|---|---|
| aed8abf | `tests/review_r2b_spill_degraded_aof_1265.rs` (ignored; MOON_BIN) | tokio FAILED 3/3; monoio passes (control) |
| 015ff7b | `src/shard/persistence_tick/review_r2b_tests.rs::a_poisoned_reclaim_job_does_not_degrade_spilling` | FAILED on monoio and tokio |
| d3634a1 | `tests/review_r2b_lazy_free_under_flood_1280.rs` (ignored; MOON_BIN, release build) | FAILED on monoio (10.8–21.4×) and tokio (21.5×); base passes |
| b342d02 | `review_r2b_tests::a_wall_clock_step_back_does_not_reset_the_restart_budget` | FAILED on monoio (pure logic) |
| be782cf | rustfmt | — |

The only production-tree touch is one `#[cfg(test)] mod review_r2b_tests;` line in `src/shard/persistence_tick.rs`. All hand-started servers used ports 7720–7734 and were stopped. Disk stayed above 20 GB.

## Confidence

- **Completeness 0.9.** Every item in the brief was addressed. Not done: the Windows/MSRV matrix, bash 3.2 execution, a redis-cli 7.4+ run, and the full-sync residual.
- **Clarity 0.9.**
- **Practicality 0.92.** Every MAJOR has a red test and a concrete fix, and MAJOR-2's fix was checked green.
- **Optimization 0.9.** MAJOR-1 comes with a quantified trade-off and a capped-scale proposal.
- **Edge cases 0.9.**
- **Self-evaluation 0.9.** One suspicion was retracted after testing (the unbound `RUST_PID`). The in-test write flood failed to saturate the loop, so the test switched to a deterministic long-command load; the earlier shell-flood numbers (2.2–2.4×) were not reproducible on a quieter host and are not used as evidence.
