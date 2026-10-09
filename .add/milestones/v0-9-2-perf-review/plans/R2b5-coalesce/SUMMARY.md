# R2b5 coalesce — moon#1322: one barrier set per pipeline batch

Branch `w2/r2b5-coalesce` (3 commits on 9d85dbc): 34435c6 BarrierDebt · 9d50bed handler wiring · 9c1921f test. Binaries `b-final-{monoio,tokio}`. Linux container, not merge bar.

## Cause
In 2b every spanning write (`coordinate_multi_key` → `confirm_multi_key`) and every pipelined remote write (H1) awaited its own barrier set before the next command of the batch ran: a P16 batch paid 16 serial barrier sets. Evidence: new test (2b never runs the batch's 2nd MSET while the 1st's barrier is held); strace fsync calls/batch (40×P16, 6-key spanning MSET, s4 always) monoio 2a/2b/fix 123.7/234.9/122.5, tokio 82.3/216.0/82.9. Side finding: same-shard `{tag}` MSET at P16 was also −50–70% in 2b (local barrier per command on the owner shard), now recovered.

## Design
`PendingBarriers` stores `(shard, rx)`; `wait_each()` reports every failed shard. `BarrierDebt` (inline ≤16 shards + 64-bit mask; waiters `(reply idx, shard mask)`, per-connection on both runtimes, cleared at batch start): `owe(Some(idx)|None, shards)`, `push(idx)` = own shard, `settle()` = ONE parallel set under one deadline. Coordinated writes record via `remote_barrier::owe_multi_key`; H1 remote writes `owe(Some(resp_idx),[target])`; scripts/FCALL/MOVE/COPY/local legs `push`. `settle_barrier_debt` runs at batch end and before every early flush.

Flush paths (both runtimes): batch end ✔; blocking ✔ (settle → flush → clear); SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE ✔; PSYNC hijack ✔ (monoio; tokio has none); MULTI/EXEC no flush (joins batch end); QUIT/SHUTDOWN via batch end ✔; write error / peer gone: nothing flushed; protocol fault: only already-barriered output; inline dispatch refuses writes under `always`, held lane via `held_reply::take_owed` ✔; no write-buffer-size flush exists; handler_single has its own `SingleAofLog` settle. `responses` is replaced only after a settle.

Failure mapping: every waiter whose mask holds a failed shard gets `barrier_refusal_frame(ack)` (first failed shard in send order); others keep `+OK`; error replies are never overwritten but their shards are still confirmed; >64 shards alias bits → only conservative extra failures.

## Tradeoff
Contract unchanged (no reply before every shard it wrote is written, and fsynced under `always`). Later commands of the batch execute while an earlier barrier is pending — same as local group commit. Floor: a batch waits max-of-owed-shards fsync once; a single spanning MSET still pays leg RTT + fsync (only shard-side acked appends would overlap them — deferred).

## Evidence
- Red→green `a_pipeline_of_spanning_writes_pays_one_barrier_set_before_any_reply`: RED on i2b-fe6fb20 both runtimes; GREEN on fix, 12/12 repeats at load 3. `an_early_flush_waits_for_the_batch_debt_before_replying` (MSET + SUBSCRIBE, then BLPOP) guard green on both.
- sc3 (reconstructed run3.sh/resp.py/xxh.py): 128 scenarios, 23050 checks, 0 violations (always/after_always/boot; tokio + monoio MOON_NO_URING=1). Control w2a mset/always: 2 NOSYNC violations.
- Bench (lane-run, loadavg 5.5–6, relative only), fix vs 2a / vs 2b: monoio span P16 105.1K (−7% / +191%), tokio span P16 63.9K (−15% / +140%); same-shard P16 −5..−7% vs 2a; P1 and everysec inside 2b's spread. Quieter 5-rep (load ~3): monoio span P16 2a 105.6K → fix 96.9K, tokio 70.3K → 66.3K; `MOON_AOF_SHARD_WRITE=0` unchanged. Single MSET p50 equal to 2b within spread.
- Suites green both runtimes: cross_shard_write_barrier_1322, script_write_fsync_barrier_831, aof_shard_write_1266, aof_everysec_kill9_1266, aof_fsync_stall_r1, aof_backpressure_reply_1272, aof_everysec_backpressure_769, crash_matrix_per_shard_aof, single_handler_aof_order_1099 (tokio), txn_crash_atomicity_1300 (non-replica); lib persistence::aof, shard::coordinator, server::conn; loom_aof_lane 17/17; fmt, clippy ×2, fuzz check.
- `barrier_needed`: no mismatch — `fsync_policy()` returns Always also while a lane is held; doc rewritten to say so.

## Deferred
Shard-side acked appends (overlap leg RTT and fsync); EXEC / cross-shard EXEC owing into the debt; blocking pops (own flush point), SWAPDB (rare) stay per command; tokio same-shard P16 residual −5..−12% vs 2a not isolated.

## CHANGELOG
- Pipelined cross-shard writes under `appendfsync always` now pay one fsync barrier per pipeline batch instead of one per command (moon#1322). A batch of spanning `MSET`/`DEL`/`UNLINK`/`BITOP`/`COPY` (and pipelined writes to other shards) confirms every shard it wrote with a single parallel barrier before any reply of the batch is sent, so a P16 spanning `MSET` runs within about 5–8% of the speed it had before replies waited on remote shards' fsyncs, up from a 57–68% loss. A single spanning write still waits for its shards' fsyncs after its legs reply, so its latency is unchanged. Durability is unchanged: no reply leaves before every shard the batch wrote is on disk, and a failed barrier fails exactly the replies that depended on the failed shard.
