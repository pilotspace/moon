# R2b-fix-b SUMMARY — W2B-1 (moon#1266 Option 1A reply-before-write windows)

Branch `w2/r2b-fix-b`, base `efe223b`. Commits: cf63109 (fix), ca909a1 (tests), 0667716 (docs), 4963c67 (tests), 6ee60a8 (fix), 902eac7 (fix). Binaries `r2bfb-v3-{monoio,tokio}`, markers verified.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| W2B-1 MAJOR: in WRITER mode (boot before the first hand-over; right after `always` → `everysec`) replies left before their AOF `write(2)` | FIXED | cf63109 ca909a1 4963c67 6ee60a8 902eac7 | `tests/aof_shard_write_1266.rs`: `boot_window_kill_on_ack_loses_nothing_s{1,4}`, `after_always_kill_on_ack_loses_nothing_s{1,4}`, `leaving_always_under_load_hands_over_s{1,4}`; 20 reps each (10 × c1, 10 × c16). Red on efe223b in all 12 runtime/driver/window cells; 0 lost on monoio io_uring, monoio epoll and tokio after. Loom `model_replies` plus 2 negative controls. | Fold window remains (documented) |
| NIT docs: "post-fold drain normally milliseconds" | FIXED | 0667716 6ee60a8 902eac7 | production-guide, PRODUCTION-CONTRACT, env-knobs: the exposure is the whole fold (>0.9 s at ~150 MB; automatic rewrites open it); no boot or policy-switch window remains | — |
| NIT loom: write-before-reply | FIXED (modelled) | cf63109 | `tests/loom_aof_lane.rs` `model_replies` (held boot → `always` re-hold → hand-over); negative controls `loom_ignoring_the_hold_is_caught`, `loom_appendsync_flip_without_rehold_is_caught` fail as required. Loom found a real bug (take-back and hold under two locks), fixed with `AofLane::take_back_held`. Fold and write-error-latch replies documented as outside the model | — |

## Mechanism
1. **The hold** (`lane_protocol.rs`, `lane.rs`): WRITER mode carries `held`. Set when a writer is attached (boot until its first hand-over), under `always` (take-back and hold under one lock), and when a producer's `AppendSync` flips a DIRECT lane back to WRITER. Cleared by a successful `release` and by `close`. A latched write error drops it without a hand-over (`unhold`) — Option-3 behaviour, the moon#1314 class.
2. **Producers' view** (`pool.rs`): `fsync_policy_for(shard)` reports `Always` while that shard's lane is held; used by `try_send_append_durable`, `send_append_group`, `append_then_apply_in_txn`, `fsync_barrier`, so each reply waits for the writer's ack of a barrier queued after its record. Per lane (a pool-wide flag would drag every shard onto the barrier path). `fsync_policy()` keeps a pool-wide count for callers that don't know their shard.
3. **Offer before the receive** (`writer_task/lane_hooks.rs::top_of_wake`): all four writer loops offer the append position at the top of every wake. `writer_task.rs` 2214 → 2243 lines.
4. **Boot wait** (`main.rs`): `pool.await_hand_over(2 s)` before the shards spawn; skipped under `always`, warns on timeout; correctness does not depend on it. Manifest poll 50 ms → 5 ms.
5. **A held lane's barrier costs no fsync outside `always`** (`group_commit::batch_needs_fsync`): the batch fsync follows the policy in force at commit, re-read there. The old "any `AppendSync` ⇒ fsync" rule made every reply after leaving `always` wait for an fsync (`aof_fsync_stall_r1::config_set_appendfsync_reaches_the_writers_s{1,4}` caught it). The everysec→always race stays closed (a producer that read `always` sends before the writer re-reads). Entering `everysec` owes one fsync (`EverysecSync::set_policy`, `dirty: true`).
6. **monoio writer park**: warm-polls only while records reach it with no ack waiting (WRITER and unheld: a fold, or a latched error).
7. **`MOON_TEST_AOF_WRITER_HOLD` keeps 1A off**, as `MOON_TEST_AOF_FSYNC_STALL_MS` already did, so `perf_ws21_aof_drain` keeps testing the shutdown drain (moon#1274).

## Measurements
Kill-on-ack: c1 = one connection pipelining 200 SETs right after the first PONG or right after `always` → `everysec`; c16 = 16 connections × 200 SETs, one hash tag each. SIGKILL the instant the first connection has all its acks; a rep is lossy if any received ack is missing after restart ("+" = lower bound).

| lossy reps / 10 | efe223b c1 | efe223b c16 | fixed c1 / c16 |
|---|---|---|---|
| monoio io_uring, boot, s1 | 9 | 6+ | 0 / 0 |
| monoio io_uring, boot, s4 | 8 | 7+ | 0 / 0 |
| monoio io_uring, after-always, s1 | 6 | 3 | 0 / 0 |
| monoio io_uring, after-always, s4 | 8 | 5 | 0 / 0 |
| monoio epoll, boot, s1 | 10 | 5+ | 0 / 0 |
| monoio epoll, boot, s4 | 10 | 5+ | 0 / 0 |
| monoio epoll, after-always, s1 | 9 | 2 | 0 / 0 |
| monoio epoll, after-always, s4 | 8 | 6 | 0 / 0 |
| tokio, boot, s1 | 7 | 3 | 0 / 0 |
| tokio, boot, s4 | 10 | 5+ | 0 / 0 |
| tokio, after-always, s1 | 9 | 2 | 0 / 0 |
| tokio, after-always, s4 | 10 | 5+ | 0 / 0 |

A lossy efe223b rep typically lost all 200 acked keys (c1) or 200–2,600 (c16).

Hold convergence: `leaving_always_under_load_hands_over_s{1,4}` green on all three configs (8 writers; shard threads write their own records again within 5 s).

Boot to first PONG (everysec, 6 interleaved reps, measured while suites ran): s1 30 → 33 ms, s4 27 → 47 ms.

Throughput: not re-benchmarked — READY TO BENCH. Steady state adds one Acquire load per write decision; held windows cost one barrier round trip per reply, no fsync.

## Gates (Linux container, not merge bar), HEAD 902eac7
- `cargo fmt --check`; clippy `--all-targets -D warnings` both feature sets; fuzz check: exit 0.
- `cargo test --release --lib -- persistence shard`: monoio 1523, tokio 1496 passed.
- `loom_aof_lane` smoke 11 passed; loom: all 7 models pass at preemption bound 3 (2.9 s) and 5 (273 s); negative controls fail as required (standalone `rustc --cfg loom` build; the test does not link moon). `loom_aof_fsync_agent` not re-run (file unchanged).
- Integration (`MOON_BIN` pinned, `--include-ignored`): aof_shard_write_1266, aof_everysec_kill9_1266 (strict), aof_fsync_stall_r1, aof_select_after_restart_r1, aof_replay_clock_1283, txn_crash_atomicity_1300, aof_everysec_backpressure_769, aof_backpressure_reply_1272, aof_multidb_kill9, crash_matrix_per_shard_aof, aof_auto_rewrite, perf_ws21_aof_drain, script_write_fsync_barrier_831, perf_ws21_aof_writer_start. monoio io_uring and epoll: all pass (full list on v2; v3 re-ran the hook suites plus aof_shard_write_1266 and aof_fsync_stall_r1). tokio v3 full list: all pass except the 5 known no-master-PSYNC `txn_crash_atomicity_1300::a_replica_*` tests (identical on efe223b-tokio).

## Cross-ownership edits
`src/main.rs` (boot wait), `src/persistence/aof/group_commit.rs` (`batch_needs_fsync`), `src/persistence/aof/fsync_agent.rs` (`set_policy` dirty), `src/persistence/aof/runtime_fsync.rs` (doc); docs production-guide, PRODUCTION-CONTRACT, env-knobs.

## Risks
1. R1 fsync rule relaxed: an `AppendSync` queued under `always` but committed after leaving `always` is acked once written without an fsync (redis's `beforeSleep` does the same). Under `always` every batch is still fsynced before its acks.
2. Remaining windows: the fold (whole fold); the write-error latch (moon#1314); graceful shutdown (`broadcast_shutdown` flips the lane without holding it — a kill -9 during the graceful stop can lose writes acked after the flip).
3. A fold overlapping a held window parks replies until the post-fold boundary (safe; rare latency).
4. A writer that never starts: its shard's writes now fail with `AOF_FSYNC_ERR` at `--aof-fsync-timeout-ms` instead of being acked unwritten.
5. `MOON_TEST_AOF_WRITER_HOLD` forces 1A off; its suites no longer exercise 1A.
6. `writer_task.rs` is 2243 lines (already over cap; +29).

## CHANGELOG bullet (replaces the WS46 one)
- AOF `everysec`/`no`: a process crash no longer loses acknowledged writes (moon#1266, Option 1A with R2b W2B-1). Each shard thread writes its own AOF records with one `write(2)` per event-loop iteration before that iteration's replies leave (monoio io_uring before-submit hook, epoll/kqueue before-poll hook, tokio once per scheduler round, and before any reply handed to another shard). The writer thread keeps the fsync agent, rewrites, `always` group commit and the clean-close marker. Whenever the writer owns the append position outside a rewrite (boot until its first hand-over, under `always`, right after leaving `always`) the lane is held and each reply waits until the writer has written its record (fsync only under `always`); the server waits up to 2 s for the first hand-over before its shards start. Measured (4-vCPU Linux container): kill -9 1 ms after the last ack 0/360 lossy reps (Option 3: 9/240); kill on ack right after the first PING or after leaving `always`: 0 lost on monoio io_uring, epoll and tokio (before: 6–10 of 10 c1 reps). Exception: writes acked during a BGREWRITEAOF fold, exposed for the whole fold (>0.9 s at ~150 MB; automatic rewrites open it). `CONFIG SET appendfsync` leaving `always` applies to records still queued, as in redis. `MOON_AOF_SHARD_WRITE=0` restores the writer-thread path. New INFO field `aof_shard_writes`.

## README.md replacement (lines 282–292)
```
  [archive](docs/internal/benchmark-history.md) §7.3). `everysec`'s fsync runs off the writer on a
  background thread (redis's model), and each shard thread `write(2)`s its AOF
  records once per event-loop iteration *before* the replies that acknowledge
  them (redis's `beforeSleep`; moon#1266) — also from boot until the AOF writer
  first hands its file to the shard threads and right after a `CONFIG SET
  appendfsync always` → `everysec`, when each reply waits until the writer has
  written its record — so a `kill -9` loses no acknowledged write. Measured
  with SIGKILL 1 ms after the last ack, 20 reps per cell, `--shards` 1 and 4,
  tokio and monoio (io_uring and epoll) (`tests/aof_everysec_kill9_1266.rs`,
  2026-10-01, 4-vCPU Linux container): losses in 0 of 360 reps, down from 9 of
  240 (writer-thread path) and 226 of 240 before it; and with SIGKILL on the
  ack right after the first `PING` or right after leaving `always`
  (`tests/aof_shard_write_1266.rs`): 0 lost. The exception is a write
  acknowledged during a BGREWRITEAOF fold — exposed for the whole fold; see
  "What a process crash can lose under `everysec`" in
  [docs/production-guide.md](docs/production-guide.md). An OS crash or power
  loss can still lose up to ~1 s; use `appendfsync always` for zero
  acknowledged-write loss against those.
```

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
