# WS41-graph-wal SUMMARY

Wave 2, lane A. Branch `w2/ws41-graph-wal`, base `2e3fa83` (WS36). Linux container, not the merge bar. `MOON_BIN` was pinned and `MOON_DISK_FREE_MIN_PCT=0` set.

## Per-issue verdict
| issue | verdict | commits | evidence |
|---|---|---|---|
| moon#1302: the forward path, the rollback path, replication order, and MQ | FIXED | 26e894f (red suite), f537902, 4f633f3, 5c4faca | `tests/graph_wal_append_1302.rs`, 11 real-server tests; see below |

| case | base ws36 (red) | ws41 (green) |
|---|---|---|
| Cypher `CREATE` of 6000 nodes, then kill -9 | 4096 nodes survive | 6000 survive (s1, s4) |
| 2,500 pipelined `GRAPH.ADDNODE` + `TXN ABORT` | tokio+graph: MOONERR (s1, s4); monoio: already green, because 1024-frame batches let the tick drain in between | `+OK`, 0 nodes after kill -9 |
| The same TXN written as one 2,500-node Cypher `CREATE` | MOONERR on monoio and tokio+graph | `+OK`, 0 nodes |
| A TXN with 6000 rollback records | MOONERR every time | `+OK`, `txn_rollback_wal_dropped` 0 |
| Master and replica at s1 after a large abort and a master restart | master 1905 nodes, replica 1 | both 1 |
| `TXN.COMMIT` of 6000 `MQ PUBLISH` (the same defect) | 1904 dropped, `XLEN` 4096 | `XLEN` 6000 |

## Mechanism
**New module `src/shard/wal_append.rs`.**
- Records still go through the bounded 4096-slot channel.
- When the channel is full, the record is moved (not cloned) into an unbounded queue owned by the shard thread. This only happens on the thread registered as the shard's owner.
- The tick's `drain_into` appends the channel's records and then the queue's, in one synchronous stretch. It also runs on both shutdown paths before the final flush; before this change, records queued in the last tick before a graceful shutdown were lost.
- Order is preserved: once one record goes to the queue, every later record follows it until the next drain.
- Durability is the same as before: the same tick appends it, and it waits for the same flush.
- The on-disk format does not change.

**Remaining refusals.** The append is still refused, counted and logged in two cases: the writer has exited (the channel is closed), or a record is produced on another shard's thread while the channel is full.

**New INFO field.** `reclamation_wal_append_overflow_total`, which reads 1904 for the 6000-node CREATE.

**Abort path.** `abort_logged` now appends the graph rollback **before** replicating, and replicates only `log.graph[..accepted]`. WS36's `EndOnDrop` guard is unchanged.

**Deviation from the letter of option (b).** Commands do not append to the WAL writer themselves.
- The writer is a local of the event loop, lent by `&mut` to the SPSC drain and to the ticks. The SPSC `GRAPH.*` arm is itself a producer.
- Sharing the writer would put a `RefCell` borrow on about 40 sites, and a producer running under that borrow would still need a queue.
- The shard-thread queue gives the same result: the same order, the same durability, and no per-command capacity limit.

**No loom model.** The queue is thread-local and the counters are `Relaxed` statistics.

## `wal_append` call sites
Every call site is now lossless on the owning thread, except the one residual case in the second row.

| call sites | status |
|---|---|
| Live graph commands, MULTI graph legs, `XactCommit`, `GraphTemporal`, the TXN `MqPush` self-fold (was 1904 dropped), SPSC routed graph, graph+temporal and cross-shard EXEC graph legs, `GraphRollback`, `MqTxnMaterialize`, the WS registry, `mq_exec` (including `emit_mq_drops`), the local rollback in `txn_abort.rs` | lossless on the owner thread |
| Tokio `handler_sharded` and io_uring `WorkspaceCreate`/`Drop` sent to shard 0 from another thread | refused and counted, instead of silently dropped, only if shard 0's channel is full at that instant. **Residual.** |
| `coordinator.rs` SWAPDB `try_wal_append_required` | checked; refuses only when the channel is closed |

## Measurements
**Perf A/B, ws36 vs ws41** (monoio, everysec, 3 reps, loads 1.4 to 5.6). Medians swung by up to ±33% in both directions, which is noise. A rerun of the s1 P16 addnode outlier gave ws36 210.1k vs ws41 221.2k (5 reps, +5%). The fast path is the same `try_send` plus a match on its error. **The orchestrator reruns it at R1** with `scratchpad/ws41/bench_graph.sh`.

## Gates
**Lint and checks:**
- fmt: 0.
- clippy with all targets: 0 on monoio and on tokio,jemalloc.
- clippy `--lib` on tokio,jemalloc,graph: 0.
- fuzz check: 0.

**Lib tests** (graph, shard, transaction, wal_v3): monoio 1330 passed, tokio 730 passed.

**Integration suites, monoio and tokio binaries:**
- graph_wal_append_1302: 11/11 on both.
- On tokio+graph binaries: 10/11. The replica test fails because tokio has no master-side PSYNC. The base scored 0/11.
- txn_abort_durability_1285: 25/25. txn_isolation_1299: 20/20.
- Graph durability and replication suites: all pass on monoio. On tokio they fail identically on base (no graph, or no master-side PSYNC).

**In-process suites** built with tokio+graph features: txn_graph_wiring 5/5, txn_cypher_write_rollback 3/3, mq_integration 17/17, workspace_integration 15/15.

## Cross-ownership edits
- **Over-cap files:** `shared_databases.rs` +8 (docs), `spsc_handler.rs` +5, `event_loop.rs` −2.
- **Other files:** `mq_exec.rs`, `info_reclamation.rs`, `transaction/abort.rs`, `server/conn/txn_abort.rs`, `shard/mod.rs`.
- **Tests:** in `tests/txn_abort_durability_1285.rs`, the overflow tests now require `+OK`.

## Risks
1. **KV record order.** KV records written directly during the SPSC drain were already out of order relative to channel records; this is unchanged. Graph and MQ order is preserved.
2. **Cross-thread workspace records** on tokio and io_uring are still refused when shard 0's channel is full. A complete fix would route them over the SPSC hop, as monoio already does.
3. **Queue growth.** The queue is unbounded within one batch, but it grows only with work already done in memory.
4. **Test coverage.** The "replicate only the accepted prefix" path has a unit test only, because a real-server refusal needs a dead writer.
5. **WS42 dependency.** WS42 wraps `abort_logged`'s records in F4 markers, and the WAL append now comes before replication.

## CHANGELOG bullets
- **Fixed (moon#1302):** graph, MQ, workspace and temporal WAL records are no longer lost past the shard's 4096-slot WAL append channel.
  - Before, a 6000-node Cypher `CREATE` kept 4096 nodes after kill -9, and a `TXN COMMIT` of 6000 `MQ PUBLISH` kept 4096.
  - Records past the channel now wait in an in-memory queue on the shard's thread, which the next 1 ms tick appends in order.
  - The on-disk format does not change. New `INFO reclamation` field: `reclamation_wal_append_overflow_total`.
  - A graceful shutdown now appends queued records before its final WAL flush.
- **Fixed (moon#1302):** `TXN ABORT` no longer answers `MOONERR WAL backpressure` for a large graph rollback, or for one pipelined with its writes. The rollback is written to the WAL before it is replicated, and only what the WAL accepted is replicated, so a master and its replica agree after a restart.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.95 · Optimization 0.85 (quiet-window A/B pending) · Edge cases 0.9 · Self-evaluation 0.9
