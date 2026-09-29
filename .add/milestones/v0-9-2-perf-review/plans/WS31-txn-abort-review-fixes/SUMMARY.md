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

## Round 2: adversarial review of effb016 / 8314999 (WS32, `ca35be0..cf836ef`)
The review found no BLOCKER. Its one MAJOR is residual risks 2–3 above, extended with a pipelining case (the limit is the *free* channel capacity) and moved to moon#1302. The three MINORs and four NITs are fixed:

- **MINOR-1 (data loss):** a script write that `dispatch` cannot run was captured anyway. The primary-key fallback took `args[0]` (a graph name, or a literal such as FLUSH / PUBLISH), and the key walker named MQ's queue key and the blocking pops' keys. TXN.ABORT then deleted or RESTOREd another client's key, logged to the AOF and replicas, and the write intent hid it from other TXNs. Such writes (FT.*, GRAPH.*, MQ, WS, FUNCTION, FCALL, TEMPORAL.*, blocking pops) are now inert inside a TXN. The fallback applies only when `first_key > 0`. Guards pin `SCRIPT_UNDISPATCHED_WRITES` to exactly the writes `dispatch` answers `unknown command`, in both directions.
- **MINOR-2:** a short argv (`redis.pcall('DEL')`) gets the arity error instead of poisoning the TXN. The docs now list what is actually refused.
- **MINOR-3:** a `no-writes` FCALL routes inside a TXN, sent as FCALL_RO. `no-writes` is now enforced under plain FCALL, as in Redis — a behaviour change outside TXNs too. moon does not parse EVAL shebang flags.
- **NITs:**
  - rollback-owner errors are passed through verbatim;
  - `txn_rollback_wal_dropped` is documented;
  - every refused script write is counted;
  - the double key walk is removed.
- **Evidence:** red on `2861785` (monoio and tokio), green after. `txn_abort_durability_1285` is 19/19 on both runtimes. Tokio replica-suite failures reproduce on base (no master-side PSYNC on tokio).
- **Remaining risks:**
  - the undispatched set must track the registry; the guards catch drift for registered commands only;
  - an arity-valid but malformed movable-key write (`LMPOP 5 a b LEFT`) is refused and poisons the TXN;
  - the FUNCTION LOAD REPLACE race behind the FCALL_RO rewrite has no deterministic test.

Also in this round: `47cda75` fsyncs the parent of every new ancestor of the data, offload, migration and AOF directories (CodeRabbit; an EACCES ancestor is logged and skipped). `779ebe8` fixes a sampling flake in WS28's F9 unit test.

## Round 3: review of the round-2 fixes plus CodeRabbit (WS34, `afd9b54..18ad969`)
The TXN undo capture recorded keys that no write touched, so TXN.ABORT restored their pre-images over other clients' writes (logged to the AOF and replicas). Three cases are fixed:

- **Too many arguments:** an exact-arity script write with extra arguments (`SETNX k v extra`) is inert. A probe of all 33 exact-arity dispatched writes (495 argvs) showed that dispatch always rejects such an argv.
- **Error replies:** a script write that answers any error (`SET k v BADOPT`, `ZMPOP 1 k JUNK`, WRONGTYPE) takes its capture back (`txn_undo_discard`). The guard found this once its fillers became well-formed numbers.
- **Read-only keys:** a write-flagged command that only reads its keys (`SORT src`, or `GEORADIUS src` without STORE) captures nothing. This applies on the connection legs of both runtimes and from scripts. The primary-key fallback now runs only when the key walker cannot enumerate the argv (`written_keys_if_known`).

Side effects: `MOVE k <same db>` and `XGROUP HELP` are inert, where they used to be captured or refused.

Other fixes in this round:
- `redis.pcall` catches the read-only refusal (EVAL_RO, FCALL_RO, no-writes FCALL). The error text is Redis 7's exact wording, checked byte for byte against redis 7.0.15.
- The embedded entry and `Config::resolve_dir` create directories durably.
- `create_dir_all_durable` handles fsync errors on a pre-existing ancestor:
  - it skips EACCES, EINVAL, EBADF and ENOTSUP there, so boot no longer fails on squashfs, vboxsf, WSL1 or procfs;
  - it warns only for entries it created;
  - EIO, and any error on a directory it created, stay fatal.

Evidence: every fix is red on the base binaries or by mutation, and green on both runtimes (`txn_abort_durability_1285` 23/23 on each).

Residual: a *connection* write that answers an error still keeps its capture. This predates the PR (moon#500) and is filed as moon#1303.

## Round 4: review of the round-3 fixes (WS35, follow-up after PR #1301 merged as `9d72003`)
The review found no BLOCKER.

**Deferred to moon#1299 / moon#1303 as acceptance cases.** Its MAJORs and MINORs are over-capture shapes. Each is harmful only while other clients can write keys that an open TXN holds, which is what moon#1299 closes. On the connection leg they are pre-existing. The shapes are:
- LMPOP/ZMPOP candidates;
- connection writes that answer an error;
- successful writes that change nothing;
- `GEORADIUSBYMEMBER g STORE 100 km`;
- `XGROUP HELP <x>`.

**Fixed:**
- **m4 (moon#1293, regression from `c55924b`):** a directory fsync that the filesystem cannot perform is now skipped on created and pre-existing directories alike, with one warning per call. That covers EINVAL, EROFS, EBADF, ENOTSUP, EOPNOTSUPP, ENOTTY and `Unsupported`, matched on the raw errno.
  - `--dir /mnt/x/moon/data` with two missing levels on vboxsf or WSL1 drvfs no longer fails boot.
  - EACCES is tolerated only on a pre-existing ancestor. EIO stays fatal.
  - `resolve_dir` keeps a user-data dir that exists but could not be made durable, instead of falling back to `.`.
- **m3 (moon#1285, pre-existing data loss):** FLUSHDB / FLUSHALL sent on the connection inside a TXN used to run, and `TXN ABORT` answered +OK with the data gone. Both runtimes now refuse them before routing with `ERR TXN cannot roll back this command (whole-database write)` and poison the TXN, so COMMIT answers EXECABORT. The command list is shared with the script path through `transaction::TXN_WHOLE_DB_WRITES`. The test is red on `18ad969` and green now, on both runtimes at `--shards 1` and `--shards 4`, including after kill -9.
- **n1, n2:** a misplaced doc comment is restored. The CHANGELOG capture claim is narrowed, and its residuals are named.

**Gates (Linux container, not the merge bar):** fmt, clippy on both runtimes, the fuzz check and unit tests all exit 0. The integration suites pass on both runtimes except the three known tokio replica tests, which fail because tokio has no master-side PSYNC.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
