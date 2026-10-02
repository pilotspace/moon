# R2b4-fix-c SUMMARY (R2b round 4, lane C)

Branch `w2/r2b4-fix-c`, 9 commits on int-2b 5238df3 (HEAD 8f1e645). Each fix commit compiles alone (`cargo check --lib`, monoio). Binaries `r2b4fc-v1-{monoio,tokio}`; markers ("Already connected to specified master", "during the snapshot transfer; not loading it", "Further records of this incident are counted", "its outcome is checked when it ends") present in both, absent in base.

## Per-issue verdict
| finding | commit | verdict |
|---|---|---|
| X1-DBL MAJOR | 7526132 | FIXED. Each drained record carries `ReplCommand::end_offset`; new `replication/applied_prefix.rs` (`AppliedPrefix`) advances `master_repl_offset` and `stream_db` right after each applied record (no await between); `finish(consumed, selected_db)` covers trailing SELECT/PING. Every early exit (supersede in `admit`, Poisoned, NoShardSlice, dropped link) leaves the offset at the end of the last applied record. redis 7.2 `replicaofCommand` semantics (replication.c 3180–3189): `REPLICAOF host port` naming the current master (same host string case-insensitive, same port; connected, syncing or reconnecting) answers `+OK Already connected to specified master` with no epoch bump/new task/role reset; wired in handler_monoio and handler_sharded. |
| PROMO-RDB MAJOR | dd43c58, 8f1e645 | FIXED. `ensure_live(epoch)` with no await before the next mutation: before the handshake's first state write; before PSYNC; right after the PSYNC reply line (before repl_id/offset/Streaming); right after each `read_rdb_bulk` (before discard_logged, load_snapshot, after_full_sync). Other sync-path awaits audited. |
| MON-SPIN MINOR | cb185bb | FIXED. Incident latch (`NOT_LOGGED`): only the first unlogged record ERRORs and calls `request_rewrite()`; a gone writer is counted per record without per-record warns; `admit` returns `()`; `auto_rewrite::wait_for_tick(busy)` — an unpark cuts an idle tick short, never a busy one. |
| N1-REPL MINOR | 479febd | FIXED. handler_single SWAPDB emits `record_local_write_global` inside the `append_then_apply` closure right after the swap (only when applied, before the `always` barrier await). |
| X1-RACE nit | f04848b | FIXED. `log_applied` uses `send_append_bounded_blocking` with `AOF_SPSC_BACKPRESSURE_BOUND` (5 ms) instead of `try_send_append` (stamp still read synchronously, #455); doc names the 5 ms bound and the 256 MiB spill cap instead of "never drops". |
| F-E-DUP nit | 3537c94 | FIXED. A requested rewrite still running after REWRITE_WAIT_BOUND is recorded as outstanding; `settle_outstanding` judges it on the first tick with no rewrite in progress (OK serves, failure re-dispatches). `MONITOR_THREAD` is a `parking_lot::Mutex<Option<Thread>>` holding the latest monitor. |
| F-G-SHARE nit | ded0d37 | FIXED. No shared refusal flag; a refused embedded boot cancels its own writer's token, joins via `tokio::task::spawn_blocking(move || handle.join()).await`, then drops its guard. |
| Residual docs | 3f134f2 | `replica_aof.rs` module doc states residuals 1 and 2 below. |

## Red/green (base `i2b-5238df3-*` → fix `r2b4fc-v1-*`)
- `tests/replica_resync_r2b4.rs`: `replicaof_the_current_master_is_a_no_op_and_applies_nothing_twice` RED (reply `+OK`) → green; `a_task_superseded_while_parked_resumes_after_what_it_applied` (alias `localhost`, includes restart) RED c=20588 monoio / 20656 tokio → green.
- `tests/replica_promotion_sync_r2b4.rs` (in-test slow fake master): `a_promotion_during_the_rdb_transfer_is_not_wiped_by_the_snapshot` RED acked1=nil → green; `a_promotion_before_the_fullresync_reply_keeps_the_node_a_master` RED replid overwritten → green.
- Unit: `applied_prefix::a_record_ends_where_its_frame_ends`, `applied_prefix::an_abandoned_batch_commits_exactly_its_applied_prefix`, `replica::replicaof_the_current_master_is_recognised`, `auto_rewrite::an_outstanding_requested_rewrite_is_settled_by_its_own_outcome`, `auto_rewrite::a_request_wakes_an_idle_monitor_but_not_a_busy_one`, `open_gate::a_refusal_stops_only_the_refused_boots_writer`.

Reviewer scripts: dbl.py base c=22024 monoio / 20353 tokio DIVERGED → 20000 both (`+OK Already connected…` on monoio); RESTART=1 21007 / 20642 → 20000 both, after restart too; slowsync.py acked1=None → 1 both (restart same); fm2.sh, fm2.sh BGRW=1, foldy.py (1M keys, 60k INCR) correct on base and fix (no regression).

## Gates (Linux container, not merge bar)
- fmt OK; clippy `-D warnings` monoio + tokio exit 0 (after 8f1e645); fuzz check exit 0.
- lib: monoio 2016 passed, tokio 1953 passed, 0 failed.
- Integration (MOON_BIN / MOON_BIN_MONOIO / MOON_BIN_TOKIO pinned): replica_resync_r2b4 2/2 both; replica_promotion_sync_r2b4 2/2 both; promoted_replica_r2b3 5/5 both; promoted_replica_restart_r2b2 2/2 both; txn_crash_atomicity_1300 39/39 both (tokio with MOON_TEST_NO_MASTER_PSYNC=1); replication_streaming 7/7, replication_multishard 9/9, replication_hardening 6/6, replication_ttl_semantics 2/2 (monoio; the first hardening run collided with gateR7's fixed ports and was re-run); aof_fold_exactly_once_455 only the known `toplevel` failure on both runtimes.

## Cross-ownership edits
handler_single.rs SWAPDB arm; handler_monoio/dispatch.rs and handler_sharded/dispatch.rs (REPLICAOF same-master check); apply.rs (one field + constructor/test literals; logic in applied_prefix.rs); embedded.rs.

## Risks / residuals
1. Fold mid-block then promotion: the fold writes the uncommitted values into the new base and later block records land in the new generation with no BEGIN; RESET undoes neither; only the rewrite requested after the rollback does — a crash before it commits restarts with them.
2. RESET append blocks the shard thread for writer room up to `RESET_BUDGET` (10 s): with a stalled writer, a promotion freezes PING/INFO/every client for up to 10 s.
3. A crash before the post-sync rewrite commits, then promotion, restarts with the former dataset; embedded cannot rewrite at all (WARN only).
4. A record is still dropped past the 5 ms block or past the 256 MiB spill cap (pool-wide policy).
5. Pre-existing: a REPLICAOF naming the same master by a different string (or after NO ONE) rolls back open TXN blocks while the offset may sit mid-block; a `+CONTINUE` then resumes inside the block. The same-master no-op removes the common case.
6. Observed, not changed: handler_single SWAPDB passes `issue_append_lsn(...)` as its LSN — possibly the moon#815 double count that coordinate_swapdb avoids with lsn 0.
7. A request arriving while the monitor is busy waits up to one 1 s tick.

## CHANGELOG bullets
- Fixed: a replica re-pointed while it waits for its AOF writer no longer applies (and logs) part of the master's stream twice; the replication offset counts exactly the applied records; `REPLICAOF <current master>` answers `+OK Already connected to specified master` and restarts nothing, as in redis.
- Fixed: `REPLICAOF NO ONE` during a full sync is no longer undone by the old master's snapshot or `+FULLRESYNC` reply arriving late; writes acknowledged after the promotion are kept.
- Fixed: a replica whose AOF cannot take records logs one ERROR and requests one rewrite per incident; the auto-rewrite monitor no longer spins while a rewrite runs.
- Fixed: a replica record whose admitted writer room vanished blocks up to 5 ms instead of being dropped.
- Fixed: a requested AOF rewrite that outlives the monitor's wait is no longer repeated after it succeeds.
- Fixed: a refused embedded boot stops only its own AOF writer and no longer blocks the async runtime while joining it.
- Fixed: the single-handler SWAPDB emits its replication record inside the swap.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9 (N1-REPL has no integration test; residuals 1 and 5 documented, not closed)
