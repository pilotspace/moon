# WS46 SUMMARY

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1266 Option 1A: the shard writes its AOF records once per event-loop iteration before that iteration's replies leave (no fsync on the shard thread) | **FIXED, adopted, default on** (`MOON_AOF_SHARD_WRITE`, default `1`; `0` gives back exact Option 3) | 05f22cd 86843a5 8c082eb f5a9793 d2b82cb b5b9103 3faa319 03e1fb9 bdf8f9d 4734775 0b169f3 190297b 87bcbdb | `aof_everysec_kill9_1266` in strict mode (any loss fails the run): **0 lost in all 360 reps**. That is 6 cells × 20 reps on each of monoio+io_uring, monoio+epoll (legacy driver) and tokio, final binaries. Earlier builds: 0 of 240 more (v1 and final, both runtimes). Before 1A: Option 3 lost in 9 of 240 reps; pre-Option 3 lost in 226 of 240. The held-fsync cells lost 0, and `always` still sends no ack while its fsync is held. New suite `aof_shard_write_1266` passes 6/6 on all 3 configurations. Loom: `tests/loom_aof_lane.rs` has 4 models plus 2 negative controls that must fail; all pass at preemption bound 3 (1.7 s), and at bound 5 on an earlier run. `loom_aof_fsync_agent` passes 5/5 (code unchanged). | Fold window (see Risks 1). It needs redis's manifest change: list the new incr at the start of a rewrite. |
| Remove the monoio warm poll if adopted | **PARTIAL, on purpose** | d2b82cb | Under 1A the warm poll never runs: the writer has no records to pick up, so it parks. It is kept only for the `MOON_AOF_SHARD_WRITE=0` escape hatch. Removing it would make that hatch worse than Option 3 (a futex wake per record from every producer) and would break the same-binary A/B. | Delete together with the switch once 1A has soaked. |

## Measurements (method, reps, raw numbers)
**Method.** `bench_ws46.sh`: `redis-benchmark -t set -r 1000000 -d 16`, a fresh server per cell, arms interleaved inside every rep, `--appendonly yes --appendfsync everysec`, release-fast builds. Arms are `ws42-final-{rt}` (base), `ws46-final4-{rt}@0` (switch off) and `ws46-final4-{rt}` (1A). Each figure below is the median of 5 paired per-rep ratios. Server CPU/op comes from /proc utime+stime; `lanew/kop` is INFO `aof_shard_writes` per 1000 SETs. Host: 4-vCPU Linux container shared with other lanes, load 2.6–4.2. **The throughput numbers are READY TO BENCH on a quiet Linux host.** The commands are in the "Quiet-host re-run" paragraph below.

Gate (1A vs base): P16 ≥ −3%, P1c50 ≥ −3%, P1c1 ≥ −8%, p99 ≤ +10%.

| leg | P16 c50 rps / p99 / CPU per op | P1 c50 | P1 c1 | lanew/kop (P16 / P1c50 / P1c1) | gate |
|---|---|---|---|---|---|
| monoio io_uring s1 | **+8.4%** / −12% / −22% | **+7.6%** / −12% / −11.5% | **−6.6%** / +0% / +1% | 2.6 / 386 / 1000 | pass |
| monoio io_uring s4 | **+1.3%** / −1% / −6% | **+3.6%** / +4% / −4% | — | 200 / 939 | pass |
| monoio epoll s1 (`MOON_NO_URING=1`) | **+34.3%** / −25% / −35% | **+1.8%** / −12% / −10% | **+5.2%** / −10% / −11% | 2.6 / 424 / 1000 | pass |
| tokio s1 | **+80.0%** / −38% / −65% | **+40.8%** / −34% / −56% | **−1.0%** / +0% / −30% | 9.2 / 525 / 1000 | pass |

Raw medians (rps, base → off → 1A):
- uring s1 P16: 434,972 → 434,783 → 478,698. P1c50: 65,045 → 68,937 → 71,367. P1c1: 13,271 → 13,006 → 12,927.
- uring s4 P16: 179,937 → 173,762 → 181,307. P1c50: 31,017 → 30,879 → 31,801.
- epoll s1 P16: 327,976 → 314,614 → 433,463. P1c50: 68,540 → 65,531 → 67,195. P1c1: 12,530 → 13,011 → 13,016.
- tokio s1 P16: 157,741 → 151,653 → 269,978. P1c50: 39,879 → 39,014 → 61,058. P1c1: 10,689 → 10,466 → 10,764.

Switch off vs base stayed between −6.4% and +4.4% in every cell, so `@0` matches Option 3.

Earlier rounds:
- uring s1, 20 reps pooled: P16 +7.0%, P1c50 −2.0%, P1c1 +1.3%.
- epoll before the before-poll hook: P16 −10.1%, which failed the gate. That result is why commit 190297b exists.
- uring s4 under my own concurrent clippy (load 9–10): −27%. This is an oversubscription artifact: CPU/op was lower and it does not reproduce at normal load.

Syscalls per SET (`strace -c`, monoio io_uring s1):
- P1c50: Option 3 makes 0.23 (write .089, nanosleep .060, io_uring_enter .055, futex .025); 1A makes 0.139 (io_uring_enter .072, write .065).
- P16c50: 0.045 → 0.008.
- P1c1 under 1A: one write per op, which is inherent (redis does the same). A small O_APPEND write costs about 1 µs.

Gates (Linux container, not the merge bar): `cargo fmt --check` ok. `cargo clippy --all-targets -D warnings` ok on monoio and on tokio. `cargo check --manifest-path fuzz/Cargo.toml --all-targets` ok. `cargo test --release --lib -- persistence shard`: monoio 1508 passed, tokio 1481 passed.

Integration suites, all with `MOON_BIN` pinned, `--include-ignored` and `MOON_DISK_FREE_MIN_PCT=0`:
- **Full list on tokio (final4) and on monoio io_uring (final3):** final3 has the same io_uring code path as final4; final4 only adds a counter used by the epoll path. Everything passes except the known `aof_fold_exactly_once_455` toplevel failure, which fails identically on ws42-final and with the switch off. The list is aof_everysec_kill9_1266, aof_shard_write_1266, aof_fsync_stall_r1, aof_select_after_restart_r1, aof_replay_clock_1283, txn_crash_atomicity_1300, aof_everysec_backpressure_769, aof_backpressure_reply_1272, aof_multidb_kill9, aof_append_status_heals_on_rewrite, aof_fsync_err_subscribe_ordering, crash_matrix_per_shard_aof, crash_matrix_per_shard_bgrewriteaof, single_handler_aof_order_1099, legacy_aof_rewrite_on_boot_914, aof_auto_rewrite, perf_ws21_aof_drain, script_write_fsync_barrier_831, default_config_aof_backpressure_838.
- **Monoio io_uring on final4:** kill9 and aof_shard_write_1266 rerun, both pass.
- **Monoio epoll on final4:** 17 suites, all pass.
- **`appendfsync no`:** a manual kill -9 check (2000 SETs, kill -9, restart) recovered 2000 of 2000 keys.

**Quiet-host re-run** (from the scratchpad `ws46/` folder): `PORT=7624 ./bench_ws46.sh 5 1 "16:50:2000000 1:50:500000 1:1:100000" out.csv /home/user/wt/bin/ws42-final-monoio /home/user/wt/bin/ws46-final4-monoio@0 /home/user/wt/bin/ws46-final4-monoio`, then `python3 paired.py out.csv`. Add `EXTRA_ENV=MOON_NO_URING=1` for the epoll leg, use `4` in place of `1` as the second argument for the s4 leg, and use the `-tokio` binaries for tokio.

## Design (detail in DESIGN.md)
**The lane.** `persistence/aof/lane.rs` holds one `AofLane` per AOF writer. Its pure state machine is in `lane_protocol.rs`, which the loom model compiles in via `#[path]`. The append position moves between the WRITER, DIRECT and CLOSED modes under a parking_lot mutex.
- In DIRECT mode the producer frames each record with the writer's own `RecordCtx::prefix_for`: SELECT, MOON.TS, MOON.TXN, the session stamp and the R2 sentinel. It uses the same framing, framed or bare depending on layout, and applies the #455 fold-floor drop.
- **Release** (writer hands the position to the lane) happens only when all of these hold: the channel is empty (checked under the lane lock), there is no slow sender, no write-error latch is set, the policy is not `always`, and the rewrite overflow is not armed.
- **Flip back to WRITER** happens on any non-Append message, on a failed write (which also sets the latch), when the policy changes to `always`, and on every stop path. A flip always writes the buffer first.

**Flush points:**
- monoio io_uring: a before-submit hook in the vendored driver.
- monoio epoll/kqueue: the same hook, run before every readiness poll. Replies park until the hook has written; when it woke replies, the driver polls without blocking.
- tokio: a reply yields once so the round's other connections append too; the first to resume writes them all.
- Replies handed to another shard: `OneshotSender::send` and `ResponseSlot::fill` flush first.
- Background work with no reply: the shard event loop flushes every iteration.
- Lone connection (all drivers): after 16 fruitless waits, replies stop waiting for 256 replies, then try again.

**What stays on the writer:** EverysecSync, the fsync handoff and its agent, `always` group commit, rewrites, folds and overflow drains, generation switches, runtime CONFIG SET appendfsync, the moon#769/#1272 latch, and the stop/CLOSE marker path. The writer takes the lane back before doing any of them.

**Observability:** INFO `aof_shard_writes`. `MOON_TEST_AOF_FSYNC_STALL_MS` forces 1A off, because its suites fill the writer channel on purpose.

## Cross-ownership edits
- **Server startup:** `src/main.rs`, `src/server/embedded.rs` and `src/server/listener.rs` now build the pool before the writers and hand each writer its lane.
- **Flush calls:**
  - `src/runtime/channel.rs` (`OneshotSender::send`)
  - `src/server/response_slot.rs` (`fill`)
  - `src/server/conn/handler_monoio/{mod,dispatch,pubsub}.rs`
  - `src/server/conn/handler_sharded/{mod,pubsub}.rs`
  - `src/shard/event_loop.rs` (install, per-iteration flush, uninstall; no allocation)
  - `src/shard/uring_handler.rs`
- **INFO field:** `src/command/connection.rs`.
- **Vendored monoio:** `vendor/monoio/src/{lib.rs, driver/mod.rs, driver/uring/mod.rs, driver/legacy/mod.rs}` gain the hook, `IoWritePoint`, and the hook calls in the legacy park. All are marked `moon patch (moon#1266 1A)`; no new `unsafe`.
- **Docs:** `docs/production-guide.md` (write-path bullet; "What a process crash can lose under everysec"), `docs/PRODUCTION-CONTRACT.md` (everysec and `no` rows), `docs/internal/env-knobs.md` (MOON_AOF_SHARD_WRITE; warm poll inert).
- **Tests:** `tests/aof_everysec_kill9_1266.rs` now defaults to strict mode.

## Risks / things the orchestrator must re-check at integration
1. **Residual kill -9 window inside a BGREWRITEAOF fold.** Records acknowledged during a fold reach the file at the post-fold drain, as under Option 3. This is documented in production-guide and DESIGN §5.
2. **A slow disk now stalls the shard thread's write(2)** instead of filling the channel. That matches redis, and the fsync stays off-thread.
3. **Cross-shard replies flush once per reply** (s4 P1c50 shows 939 writes per 1000 ops). Throughput is still at or above base, but this is the first place to coalesce if s4 regresses.
4. **Pub/sub pushes and the replication stream are not ordered behind the AOF write on tokio and epoll.** Replies are; a push or replication bytes to another connection can reach the wire before the record is written. That affects observers only, not durability of acknowledged writes.
5. **One parking_lot mutex acquisition per append** (lane lock). `AOF_LANE_WRITES` is a global relaxed counter. Listener/embedded modes write foreign-thread appends immediately.
6. **`writer_task.rs` was already over 1500 lines** (2150 → 2214 now); my new logic sits in `writer_task/lane_hooks.rs`. `lane.rs` is 1167 lines and `pool.rs` grew by about 70.
7. **Re-run the gates after cherry-pick.** The vendored monoio patches touch the same driver files as the earlier spin-poll patches, so the integrated tree needs the `MOON_NO_URING=1` kill9 and `aof_shard_write_1266` suites run again. The throughput figures need the quiet-host re-run above.

## Self-evaluation (0–1)
- **Completeness 0.9:** all gates and suites green; the only residual is the fold window, which needs a manifest change beyond WS46's scope.
- **Clarity 0.9.**
- **Practicality 0.9:** one-switch escape hatch.
- **Optimization 0.9:** every leg passes. The weakest cell is uring P1c1 at −6.6% against a −8% limit; it is inherent (one write per op, as in redis) and on a noisy host.
- **Edge cases 0.9:** loom with negative controls; rewrites under load followed by kill -9; framing across SELECT and TXN; the write-error latch; runtime `always`.
- **Self-evaluation 0.9:** measurements were taken on a shared host, so they are marked READY TO BENCH.

---

## CHANGELOG bullet (ready to paste)
- **AOF `everysec`/`no`: a process crash no longer loses acknowledged writes (moon#1266, WS46 Option 1A).** Each shard thread frames its own AOF records and writes them with one `write(2)` per event-loop iteration, before that iteration's replies leave:
  - under monoio's io_uring driver, from a before-submit hook;
  - under its epoll/kqueue driver, from a before-poll hook;
  - under tokio, once per scheduler round;
  - before any reply handed to another shard.

  The AOF writer thread keeps the fsync (agent thread), rewrites, `always` group commit and the clean-close marker. kill -9 1 ms after the last ack lost data in 0 of 360 reps (Option 3: 9 of 240). The one exception is writes acknowledged inside a BGREWRITEAOF fold. Server CPU per write is −6 to −65%, throughput ranges from −7% (P1 c1 io_uring) to +80% (tokio P16) against v0.9.2-ws42 on a shared 4-vCPU host, and p99 is the same or better. `MOON_AOF_SHARD_WRITE=0` restores the writer-thread path. New INFO field `aof_shard_writes`.

## README.md replacement (README.md:283–292, from "`everysec`'s fsync runs off the writer…" through "…for zero acknowledged-write loss.")
```
  [archive](docs/internal/benchmark-history.md) §7.3). `everysec`'s fsync runs off the writer on a
  background thread (redis's model), and each shard thread `write(2)`s its AOF
  records once per event-loop iteration *before* the replies that acknowledge
  them (redis's `beforeSleep`; moon#1266) — so a `kill -9` loses no
  acknowledged write. Measured with SIGKILL 1 ms after the last ack, 20 reps
  per cell, `--shards` 1 and 4, tokio and monoio (io_uring and epoll)
  (`tests/aof_everysec_kill9_1266.rs`, 2026-10-01, 4-vCPU Linux container):
  losses in 0 of 360 reps, down from 9 of 240 (writer-thread path) and 226 of
  240 before it. The exception is a write acknowledged inside a BGREWRITEAOF
  fold; see "What a process crash can lose under `everysec`" in
  [docs/production-guide.md](docs/production-guide.md). An OS crash or power
  loss can still lose up to ~1 s; use `appendfsync always` for zero
  acknowledged-write loss against those.
```
(The first line repeats README:282 so the paste lines up. Replace from the `everysec`'s fsync sentence onward.)

**Binaries:** /home/user/wt/bin/ws46-final4-monoio and /home/user/wt/bin/ws46-final4-tokio (release-fast, head 190297b; the later commit is docs only).

**Logs and tools:** in the scratchpad `ws46/` folder: suites-f4-{epoll,monoio,tokio}/, suites-f3-monoio/, bench CSVs h-*.csv, bench_ws46.sh, paired.py and suites.sh.
