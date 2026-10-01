# R3-fix-e SUMMARY

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| wave-2a write-path CPU regression (+10.6% CPU/op claimed) | **NOT REPRODUCED.** Measured wave-2a cost is +11.1 Ir/op (+0.4%). Nothing in wave-2a code was worth changing. | none | callgrind shard thread: main 2795.9 → 7271e74 2806.9 Ir/op. 12-rep rotating cpu.sh: 7271e74 vs main −0.5%, 95% CI [−5.0%, +7.1%]. | none |
| Pre-existing: `ColdIndex::remove` runs on every hot overwrite even when nothing is spilled | **FIXED** | `9c71f75` perf(storage) | 2806.9 → 2605.9 Ir/op (−7.2%). `set_recording` inclusive 1052.6 → 844.7. Test `cold_index::tests::remove_on_an_empty_index_is_a_no_op_and_still_releases_older_copies`. | Needs an issue number. |

**Where wave 2a's +11.1 Ir/op comes from** (callgrind self-cost diff, names normalized across the two worktrees):
- **`set_recording` +7.7:** the moon#1286 expired-overwrite compare (`old_ttl != 0 && now_ms >= old_ttl`) plus the `cached_now_ms` load.
- **`try_inline_dispatch_loop` +5.5:** the moon#1299 `any_held()` thread-local gate on the inline SET arm.
- **About +5:** the moon#1299 exit wrapper (`exit.rs`) and reaching the body's `conn` through `&mut ConnectionState`.
- **−5.7:** savings elsewhere.

The suspects listed in the brief do not touch this path:
- `check_write`, `OwnerScope`/`BypassScope` and `conn_capture` never run, because SET takes the inline path (`try_inline_dispatch`), which only does the single `any_held()` load.
- The cold-index probe added by moon#1286 is already gated on `ci.len() > 0`.
- No AOF record is built: the effect log is empty when there is no pool and replication is inactive.
- The WS40 warm poll only exists inside the AOF writer.

I left this cost alone because it is the minimum price of moon#1299 and moon#1286, and it is about 3% of the noise floor.

**What the fix does:** with disk offload on (the default), every hot-key overwrite calls `set_recording` → `ColdIndex::remove`. With nothing spilled, that call still hashed the key and probed an empty BTreeMap before returning `false`: 206 Ir/op inclusive, about 94 of them in xxh64 alone. It now returns `false` at once when both `map` and `older_copies` are empty. In exactly that state the old code did nothing else, so behaviour is unchanged. The full path moved unchanged into `remove_present` (`#[inline(never)]`). No allocation, no `unsafe`, no locks.

## Measurements (method, reps, raw numbers)

**Callgrind**
- Binaries: `release-with-debug` builds of base (lane-a at cf6fa65), head (lane-e at 7271e74) and fix (lane-e after the edit).
- Run: `valgrind --tool=callgrind --separate-threads=yes`, `--shards 1 --appendonly no --save "" --maxmemory 0`.
- Workload: 100k SET to warm the keyspace, then `callgrind_control -z`, then 300k SET at p16, c10, `-r 100000 -d 64`, then dump.
- Results, shard thread:

| build | Ir/op |
|---|---|
| main | 2795.9 |
| 7271e74 | 2806.9 |
| fix | 2605.9 |

- All threads together: 2806.6 / 2817.8 / 2616.7 Ir/op.
- With cache and branch simulation on (200k SET), base vs final:
  - I1 misses: 17.85 vs 17.62 per op
  - D1 read misses: 15.66 vs 15.73 per op
  - LL misses: 0.455 vs 0.456 per op
  - branch mispredicts: 5.28 vs 5.41 per op
  - So wave 2a also adds no cache or branch-prediction cost.
- **Caveat:** these runs used `MOON_NO_URING=1`, because under valgrind with io_uring the server accepts connections but never answers a ping.
  - The handler and dispatch code is the same under both drivers.
  - To cover io_uring natively, I counted `perf_event_open` tracepoints (root, tracefs mounted) over 1M SET: `io_uring_submit_req` and `io_uring_complete` were exactly 125,206 on both main and wave 2a.
  - The box has no hardware PMU and no `perf`.

**Native CPU per op**
- Method: utime+stime from `/proc/<pid>/stat` / 1M; the cpu.sh workload (1M SET, p16, c50, `-r 100000`, `-d 64`); io_uring monoio; all arms built with the `release` profile.
- Same binary twice (`main` vs `main`, fixed order, 8 reps): +0.6%, 95% CI [−7.2%, +4.1%]. So one arm-to-arm comparison carries about ±6% noise on this box.
- `main` vs `r3-bda76c1`, 12 reps, ABBA order: +1.6%, CI [−6.4%, +4.4%].
- 12 reps, rotating order:

| arm | raw ns/op | median | vs main (95% CI) |
|---|---|---|---|
| main | 940, 980, 1010, 1070, 1020, 990, 1000, 950, 1060, 940, 970, 980 | 985 | — |
| 7271e74 | 1160, 970, 1040, 1180, 1020, 960, 990, 1060, 890, 950, 960, 890 | 980 | −0.5% [−5.0%, +7.1%] |
| fix | 960, 980, 970, 1030, 940, 820, 960, 1150, 960, 950, 1010, 930 | 960 | −2.5% [−5.5%, +1.6%] |

- The original cpu.sh, 5 reps in its fixed order:
  - main 920, 980, 1060, 860, 880 vs fix 920, 970, 900, 910, 920 (medians 920 / 920).
  - Re-running main vs bda76c1: 900, 910, 950, 960, 890 vs 960, 930, 930, 930, 980 (medians 910 / 930, +2.2%).
- With the legacy epoll driver (4 reps): wave 2a was not slower (median user time 702 ms vs 677 ms).
- **tokio**, `release --no-default-features --features runtime-tokio,jemalloc`, 8 reps rotating:

| arm | raw ns/op | median |
|---|---|---|
| main | 2470, 2390, 2410, 2510, 2190, 2370, 2460, 2460 | 2435 |
| bda76c1 | 2490, 2300, 2270, 2480, 2450, 2350, 2350, 2360 | 2355 |
| fix | 2250, 2300, 2390, 2470, 2200, 2450, 2570, 2440 | 2415 |

  - No regression. At about 2.4 µs/op the fix's roughly 60 ns is lost in the noise.
  - bda76c1 has the same `src/` as 7271e74 (empty diff), so it stands in for head.

**Gates (Linux container, not the merge bar)**
- `cargo fmt --check`: OK.
- `clippy --all-targets -D warnings`: OK on monoio and on tokio.
- `cargo test --release --lib storage::`, filtered to the touched module tree: monoio 965 passed, tokio 961 passed.
- Integration, `--include-ignored`, `MOON_BIN` pinned to `r3fixe-{monoio,tokio}`, all green on both runtimes:
  - `txn_isolation_1299`, `txn_exit_epilogue_1299`, `review_r1_txn_isolation_1299`
  - `expired_keys_parity_1286`, `info_expired_keys_1286`
  - `held_release_txn_race_1289`, `held_release_txn_open_1289`
  - `crash_recovery_disk_offload_no_aof`, `cold_tier_observability`, `spill_inflight_visibility`, `zset_read_cold_tier_928`
  - `inline_write_spill_gate_660` (15 tests on monoio; 0 on tokio, by design)
  - `cold_tier_aof_double_apply_902` on monoio (3 passed)
- **Two failures already present on main**, with the same assertion on main, bda76c1 and the fix:
  - `cold_shadow_overwrite_resurrection` on both runtimes: the precondition fails ("no heap-*.mpf files — filler did not force a spill").
  - `cold_tier_aof_double_apply_902::writes_to_a_cold_key_after_a_rewrite_survive_kill9` on tokio only: line 340, `appendonlydir/moon.aof.manifest` not found.

**Binaries**
- Kept: `/home/user/wt/bin/r3head-7271e74-monoio`, `/home/user/wt/bin/r3fixe-monoio`, `/home/user/wt/bin/r3fixe-tokio`.
- Deleted: the `prof-*` binaries. The callgrind output files are kept in `<scratchpad>/cg/`.

## Cross-ownership edits
- `src/storage/tiered/cold_index.rs` and `src/storage/tiered/cold_index/tests.rs` (storage tier, not wave-2a code); 1261 lines after the change, under the 1500-line cap.
- No edits to CHANGELOG, README or orchestrator artifacts.
- lane-a is still detached at cf6fa65 and clean.

## Risks / things the orchestrator must re-check at integration
- **The claimed +10.6% does not hold up.** On this box the noise between two identical binaries is about ±6%. Please re-baseline any later claim with at least 10 reps in rotating order before acting on it.
- **The fix's speedup depends on what is being measured.**
  - Callgrind shows about 7% fewer instructions; native CPU per op improves only about 2.5%, because roughly 35% of the time is kernel time.
  - The gain only appears while disk offload is on and nothing is spilled. Under `--disk-offload disable` the cold index is absent and the fix has no effect.
- **Shared target-dir hazard:** cargo treats a worktree whose files are older than the last artifact as fresh. My first lane-e debug build came out byte-identical to the base build. I touched the sources and rebuilt; compare binaries with `cmp`, or check a marker with `strings` (e.g. `TXNCONFLICT`).

## Self-evaluation (0–1)
- **Completeness 0.9:** both runtimes measured, every suspect checked against callgrind; io_uring could not run under valgrind, so it is covered by tracepoint counts instead.
- **Clarity 0.9.**
- **Practicality 0.95:** the fix is a 3-line early return plus a test.
- **Optimization 0.9:** −7.2% instructions per op; the native gain is small next to the noise.
- **Edge cases 0.9:** the test covers a recovery-only older copy with an empty `map`.
- **Self-evaluation 0.9.**

Nothing scores below 0.9.
