# WS31-txn-abort-review-fixes SUMMARY

The two MAJOR CodeRabbit findings on PR #1301 against WS27's TXN.ABORT (moon#1285). Built on
`perf/ws31` (from `ec4e971`), cherry-picked onto `claude/gifted-mendel-e9wiz5` as `effb016` and
`8314999`. All results come from the **Linux container, not the merge bar**, with `MOON_BIN`
pinned for every run.

## Per-issue verdict
| finding | verdict | commit (branch) | evidence |
|---|---|---|---|
| 1. Graph rollback WAL records could be dropped while TXN.ABORT answered `+OK` (local leg and remote `GraphRollback` leg) | FIXED | `effb016` | `graph_rollback_wal_overflow_is_never_acked_shards_1` and `graph_remote_rollback_wal_overflow_is_never_acked_shards_4`. **Base (ws30-monoio):** `+OK`, 0 nodes live, **1904 nodes back after kill -9** (6000 − 4096 channel slots), both legs. **Fixed:** `-MOONERR WAL backpressure: …`, `txn_rollback_wal_dropped` 0 → 1904. Unit tests `transaction::abort::rollback_wal_tests` (3). |
| 2. Lua writes inside an open TXN were not rolled back | FIXED | `8314999` | Red on ws30 monoio **and** tokio, green on both after: `script_writes_are_rolled_back_shards_{1,4}` (EVAL/EVALSHA/FCALL; live and after kill -9), `script_writes_the_txn_cannot_undo_are_refused`, `routed_script_writes_inside_a_txn_are_refused_shards_4`. `script_abort_reaches_the_replica` is red→green on monoio; tokio skips it (no master-side PSYNC). Unit tests `scripting::bridge::txn_capture::tests` (4). |

The red runs fail for the real reason, e.g. `left: "$2\r\nx2\r\n" right: "original"` after `TXN ABORT`, and a routed EVAL answering `:1` while the key stays `x`.

## Finding 1: mechanism
- `ShardDatabases::try_wal_append_all` appends a record sequence checked and in order, stopping at the first refusal: the WAL always holds a prefix of the rollback, never a gap.
- `transaction::abort::append_graph_rollback_wal` is the one checked append for both legs. A refusal is counted (`INFO persistence` `txn_rollback_wal_dropped`), logged, and answered with `ROLLBACK_WAL_REFUSED_ERR`. The rollback stays applied in memory, like WS27's AOF-refusal path.
- The `GraphRollback` owner replies an error instead of `OK`. `send_remote_graph_rollbacks` returns `Result`, and fails a leg that was never delivered or whose reply was lost (`REMOTE_ROLLBACK_UNDELIVERED_ERR`).
- `abort_logged` answers the first refusal in log order: KV AOF, local graph WAL, remote legs.

**Durability parity (why no LSN wait):** forward graph writes are also enqueued fire-and-forget, drained into WAL-v3 on the 1 ms tick and fsynced by the off-loop sync agent; no forward reply waits for a durable WAL LSN. An LSN wait on the abort alone would be a stronger guarantee than the write it undoes. The rollback records now get exactly the forward durability, plus the check. TXN.ABORT latency is unchanged.

## Finding 2: mechanism
- **Captured**, both runtimes, in the local EVAL / EVALSHA / FCALL arms of `handler_monoio` and `handler_sharded` (shared entry point `server::conn::txn_script_undo::run_local_script`):
  - every write `redis.call`, through the connection write leg's key walker and its insert/update/delete rule, after the eviction gate and right before the write;
  - a key the script rewrites is captured once; only the first pre-image per key is restored;
  - when the script returns, in the same synchronous stretch, the pre-images join `txn.kv_undo` at the script's position and the keys get write intents. TXN.ABORT then restores them and emits the WS27 DEL / RESTORE compensation (AOF and replication).
- **Refused** inside the script, before any effect, and the TXN is poisoned so COMMIT answers EXECABORT: keyless writes (FLUSHDB, FLUSHALL, SWAPDB, any argv the key walker cannot enumerate) and second-database writes (MOVE, `COPY … DB n`). Error: `ERR TXN cannot roll back this command from a script (keyless or second-database write) -- run it outside the TXN`.
- **Routed scripts** (`--shards > 1`, keys on another shard): a read-write EVAL/EVALSHA/FCALL is refused with `ERR_TXN_CROSS_SHARD` before routing and poisons the TXN — the same rule as a cross-shard write, because the undo log is only applied on the connection's shard. `_RO` variants still route. Behaviour change: a routed read-write script that only reads is now refused inside a TXN.
- **Pre-image by copy, not move:** a write mutates the value in place, so there is nothing to move at capture time. The move happens at abort, when the replaced value is handed to an armed snapshot (WS27's `undo_one`).

## Measurements
Interleaved, base = ws30-monoio, fix = ws31-monoio, `--shards 1`.

| measurement | base | fix | result |
|---|---|---|---|
| EVALSHA of a 1000-SET script, outside a TXN (6 reps × 300 calls, median) | 1.817 ms | 1.818 ms | no change |
| Same script inside a TXN (3 reps) | 1.779 ms | 2.479 ms | ~0.7 µs per captured write (the pre-image copy) |
| TXN.ABORT, 10 KV + 10 graph nodes, everysec (4 reps × 300) | 0.214 ms | 0.187 ms | ranges overlap; no wait was added |

## Residual risks
1. **Forward graph writes have the same silent drop (pre-existing).** One Cypher `CREATE` of 6000 nodes answers OK and shows 6000 live, then 4096 after kill -9. That path still uses the unchecked `wal_append`; filed as moon#1302.
2. After a graph WAL refusal at `--shards 1`, replicas already hold the rollback while the master's WAL lacks it; the client is told, but master and replicas can diverge after a master restart.
3. A graph abort larger than ~4096 records per shard deterministically answers the WAL error. A real fix needs a bigger or batched WAL channel (design decision; moon#1302).
4. Pre-existing, unchanged: a local script can write an undeclared key owned by another shard into the local slice (captured and restored there); pre-image capture of a cold-tier key relies on `peek` promoting it.
5. **Crash between a hash's `RESTORE` and its `HPEXPIREAT` records** (from CodeRabbit's PR summary, WS27 design): the restored hash can come back without its field deadlines. Same partial-sequence crash window as moon#1300 (TXN crash atomicity), and fixed there.
6. `handler_sharded/mod.rs` and `shared_databases.rs` were already over the 1500-line cap and grew slightly.

## Gates (agent run, `perf/ws31`)
`cargo fmt --check`; clippy `--all-targets -D warnings` on monoio and tokio,jemalloc; clippy `--lib` on tokio+text-index and tokio+graph; fuzz `cargo check --all-targets`; `cargo test --lib -- transaction graph scripting snapshot aof` (monoio 1199, tokio 617); integration `txn_abort_durability_1285` (monoio 16/16; tokio 12 pass, 4 skip), `review_r3_lua_eviction_aof`, `review_w1_routed_eval_tiering_1290`, `txn_kv_wiring`, `txn_graph_wiring`. `replication_streaming` is 7/7 on monoio and 0/7 on tokio — identical 0/7 on the ws30-tokio base. The orchestrator re-gated the integrated tree (see the PR).

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
