# R2b3-fix-c SUMMARY (R2b round 3, lane C)

Branch `w2/r2b3-fix-c`, 7 commits on int-2b 1e36186 (HEAD 633ccff). Binaries `r2b3fc-v1-{monoio,tokio}` (markers verified).

## Per-issue verdict
| finding | commit | verdict |
|---|---|---|
| F-A MAJOR: promotion rolled back the dead master's open MOON.TXN only in memory | 15ba5e8 | FIXED. `bump_replica_task_epoch(aof_pool)`; when txn state is not idle, `roll_back_open` synchronously appends `MOON.TXN RESET` via `send_append_bounded_blocking` (10 s budget) before rolling back; `discard_logged` does the same before each full-sync `load_snapshot` (both runtimes). All 6 handler call sites pass `ctx.aof_pool`; `handler_single` passes `None` (replicas are single-shard, moon#406). Append failure → ERROR + `request_rewrite()`. |
| X1 MAJOR (+ nit): replica AOF append blocked the shard and dropped records | 530ae71 | FIXED. When `will_log`, `replica_aof::admit(pool).await` loops on `await_append_room(0,1)` before `apply_local` (ChannelFull: one WARN, retry, never drop); re-checks `superseded(epoch)` after the await; after the apply `log_applied` enqueues with non-blocking `try_send_append` (keeps the #455 fold stamp). Failure → `aof_last_write_status` err, ERROR, rewrite requested. |
| F-D MINOR: unclamped db logged | 3d34c5f | FIXED. Logged with `applied_db(rc) = db_index.min(db_count-1)`. |
| F-E MINOR: post-sync rewrite not retried; embedded no-op | 6c0a2ff, 633ccff | FIXED. A requested forced rewrite that finds one in progress stays pending and is retried, so the rewrite that runs started after the load; `request_rewrite` unparks the monitor; `warn_if_no_rewriter` WARNs when a replica has an AOF pool but no monitor (embedded); embedded.rs doc states an embedded replica cannot publish its synced dataset to its AOF. |
| F-F MINOR: monitor skipped pending forced dispatch on completion tick | 119f507 | FIXED. No `continue` while `forced_pending`. |
| F-G MINOR: keep_closed mem::forget leak | f91f617 | FIXED. `OpenGateGuard::refuse()` marks the path refused, `wait_writer_open` returns false and the writer exits; embedded refusal calls `refuse()`, joins `aof_join`, drops the gate. Unit `open_gate::a_refused_gate_stops_its_writer_and_is_released_on_drop`. |

F-A proof that a local TXN id cannot collide with a dead block: every promotion caller runs `bump_replica_task_epoch` → `roll_back_open` synchronously before `set_role(Master)`; local writes are refused until master; a replica is single-shard so all appends go through writer 0's ordered lane; so RESET precedes every local write in the log, and replay's `Reset` arm rolls back all open blocks and sets ctx = 0. A RESET stamped below a fold floor is dropped together with the dead block's BEGIN.

## Red/green
`tests/promoted_replica_r2b3.rs` (master monoio via `MOON_BIN_MONOIO`, replica `MOON_BIN`), RED on 1e36186 and GREEN on v1, both runtimes: `local_writes_after_a_promotion_do_not_replay_against_the_dead_txn`, `a_local_txn_after_a_promotion_does_not_commit_the_dead_txn`, `a_stream_cut_inside_a_txn_block_does_not_swallow_later_local_writes` (in-test fake master), `a_stalled_replica_aof_writer_throttles_the_link_and_drops_nothing` (fsync gate held, 15k SETs, PING < 1.5 s, DBSIZE 16001 after restart), `a_clamped_db_is_logged_as_applied`.

Reviewer repros (base i2b-1e36186 → v1, both runtimes): T1 correct → correct; T1 local=1 a=new n=1 → a=old n=(empty) loc=1; T1b a=newX n=2 → a=oldX n=1; FM b=(empty) keep=1 → b=acked keep=2; H db15 empty, db0 overwritten → db15=master-db20, db0=master-db0; D pass → pass; STALL did not finish in 600 s (reviewer measured 15–29 keys lost) → caught up 0.2 s after release, 0 drops, DBSIZE 16001/16001.

## Gates (Linux container, not merge bar)
- fmt OK; clippy `--all-targets -D warnings` monoio and tokio exit 0; fuzz check exit 0.
- lib: monoio 2001 passed, tokio 1937 passed, 0 failed.
- Integration (`MOON_BIN` pinned, `--include-ignored`): promoted_replica_r2b3 5/5 both; promoted_replica_restart_r2b2 2/2 both; aof_shard_write_1266 12/12 both; crash_aof_init_generation_1293 2/2 both; txn_crash_atomicity_1300 39/39 both (tokio with `MOON_TEST_NO_MASTER_PSYNC=1`); replication_streaming 7/7, replication_multishard 9/9, replication_hardening 6/6, replication_ttl_semantics 2/2 (monoio); aof_fold_exactly_once_455 all pass except the known `toplevel` test (identical on both runtimes).
- Every touched file ≤ 1193 lines; no new unsafe, no hot-path allocation, parking_lot only, no unwrap/expect in library code.

## Edits outside lane
`src/server/conn/handler_single.rs` one line (`bump_replica_task_epoch(None)`); `src/server/embedded.rs` (F-G refusal path, F-E doc).

## Risks / residuals
1. A fold taken while a master TXN block is open puts the uncommitted values in the base; RESET cannot undo them; the rewrite requested after the rollback replaces the base later, but a crash before it finishes replays them.
2. A crash after the full-sync load but before the requested rewrite completes restarts with the former dataset plus the streamed tail (redis `restartAOFAfterSYNC` behaviour).
3. Embedded has no rewrite monitor; WARN + doc.
4. RESET has a 10 s budget; a writer stalled past it → ERROR "MOON.TXN RESET was NOT appended" + rewrite requested; hazard as on base until the rewrite runs.
5. A stalled AOF writer now stalls the replication link (raises replica lag) instead of dropping records — intended.

## CHANGELOG bullets
- Fixed: a promoted replica appends `MOON.TXN RESET` to its AOF before accepting writes, so local writes and local transactions are no longer swallowed or committed by the dead master's open TXN block on restart.
- Fixed: a replica no longer drops replicated AOF records or blocks its shard when its AOF writer is slow; the replication stream waits for writer room before applying.
- Fixed: a replicated record is logged in the database the replica applied it to when the master's db index exceeds the replica's `databases`.
- Fixed: the post-sync AOF rewrite is retried until one that starts after the load completes; the auto-rewrite monitor no longer skips a pending forced rewrite on the tick a rewrite finishes; an embedded replica warns that it cannot rewrite.
- Fixed: a refused embedded boot releases its AOF writer gate instead of leaking it.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9 (residual 1 narrowed, not closed)
