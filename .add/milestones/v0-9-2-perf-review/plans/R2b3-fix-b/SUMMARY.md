# R2b3-fix-b SUMMARY — R2b round-3 fixes (P1, N1, N2, N3)

Branch `w2/r2b3-fix-b`, base int-2b `1e36186`. Commits: aff3e82 (N1 SWAPDB), 2ec20d7 (P1 parallel barriers), 04eeca9 (N3 16 inline targets), 2ab207d (N2 agent spawn retry). New P1 logic in a new module; pool.rs +0 lines (one `pub(super)`), writer_task.rs untouched. Lane code untouched, so loom not re-run.

## Per-issue verdict
| issue | verdict | commit | evidence |
|---|---|---|---|
| P1 MINOR: cross-shard barriers sequential (sum of fsyncs; stalled disk (targets+1) × fsync_timeout) | FIXED | 2ec20d7 | 4-shard MSET p50 under `always` ~22–26% lower; sc3.py 0 violations |
| N1 NIT: single-shard SWAPDB awaited between enqueue and swap | FIXED | aff3e82 | `append_then_apply` (enqueue + swap in one synchronous section, then the barrier if owed), the shape of `coordinate_swapdb`; sc3.py swapdb s1+s4 in always / after-always / boot: 0 violations |
| N2 NIT: failed agent spawn never retried | FIXED | 2ab207d | `EverysecSync::claim` retries at the everysec deadline, at most once per 60 s; unit test checks a due retry hands the next fsync to the respawned agent |
| N3 NIT: `Targets` spilled to heap above 8 shards | FIXED | 04eeca9 | `SmallVec<[usize; 16]>`, matching `PendingBarriers` receivers |

## Mechanism
- P1: `aof/barrier_set.rs` `PendingBarriers`. `begin(shard)` sends a zero-length AppendSync only when owed (`always`, or lane held — the `fsync_barrier` test); `wait()` awaits every ack under one `fsync_timeout` deadline and returns the first failure; receivers inline up to 16 shards. `remote_barrier::barrier_targets` (multi-key coordinators, FLUSH broadcast) sends every barrier before awaiting any. `confirm_multi_key` adds the local leg's barrier to the same set when `local_barrier_pending`, then clears the flag so the handler doesn't barrier again. Zero-cost (one Acquire load) when nothing is owed.
- Behaviour change: inside a pipeline under `always`, each spanning write confirms its own local leg with its remote legs (before, one local barrier covered the batch) — more local fsyncs in a pipeline of spanning writes; per-command latency is the max not the sum, and the writer group-commits concurrent barriers.
- N1 failure semantics: enqueue refused → command aborts, both dbs untouched; barrier failure → swap stays applied, reply `swapdb_barrier_refusal_frame` like every other `always` write.
- N2 logging: one WARN at first failure, debug per failed retry, info when a retry succeeds; the writer fsyncs inline meanwhile.

## Measurements
lat.py, 4-shard spanning MSET p50 under `always`, six alternating base (`i2b-1e36186-*`) / fixed (`r2bfb3-v1-*`) pairs on a box shared with gateR6:

| runtime | base p50 µs | fixed p50 µs | change |
|---|---|---|---|
| monoio | 1005, 1100, 1199, 1162, 992, 1075 (median ≈ 1088) | 1081, 825, 898, 872, 759, 720 (median ≈ 848) | ≈ −22%, faster in 5/6 pairs |
| tokio | 2186, 1469, 1279, 1288, 1598, 1352 (median ≈ 1420) | 1082, 1020, 923, 1116, 1021, 1101 (median ≈ 1051) | ≈ −26%, faster in 6/6 pairs |

Single-shard MSET (untouched) moved within noise (488–750 µs on both binaries).

sc3.py sweep (tokio + monoio epoll; always / after-always / boot; s4 + s1; mset, msetnx, del, unlink, bitop, copy, set, incr, eval, multi, txn, flush family, swapdb): 128 scenarios, 45,860 checked acks, 0 violations. One tokio boot `txn` rep printed "NO CHECKS" (every TXN refused as cross-shard); the other rep checked 80 acks.

## Gates (Linux container, not merge bar)
- `cargo fmt --check`; clippy `-j2 --all-targets -D warnings` monoio + tokio: pass.
- Lib tests (`persistence::aof shard::coordinator server::conn`): monoio 493, tokio 464 passed (incl. new barrier_set, remote_barrier, fsync_agent tests).
- Integration, `MOON_BIN` = `r2bfb3-v1-{monoio,tokio}` (built at 2ab207d; marker verified; tokio binary has no `monoio::` symbols), each on monoio io_uring, monoio epoll and tokio, all pass: cross_shard_write_barrier_1322 1, aof_shard_write_1266 12, aof_fsync_stall_r1 4, script_write_fsync_barrier_831 3, crash_matrix_per_shard_aof 4.

## Cross-ownership edits
`src/server/conn/handler_single.rs` (SWAPDB), `src/shard/coordinator.rs` (call into `confirm_multi_key`), `src/persistence/aof/{pool.rs,mod.rs,fsync_agent.rs}`, new `src/persistence/aof/barrier_set.rs`.

## Risks
1. A host that cannot spawn the agent thread retries once a minute, quietly at debug level.
2. Latency figures come from a shared box; re-run lat.py on a quiet host if they matter.

## CHANGELOG bullet (extends the moon#1322 entry)
- Cross-shard writes under `appendfsync always` send every shard's fsync barrier (remote legs and the local one) at once and await them together under one `--aof-fsync-timeout-ms` deadline: the reply waits for the slowest fsync instead of the sum (4-shard MSET p50 ≈1.09 → 0.85 ms monoio, ≈1.42 → 1.05 ms tokio, shared 4-vCPU host); a stalled disk costs one timeout, not one per shard. Single-shard `SWAPDB` logs and applies the swap in one step. An everysec AOF writer whose fsync thread failed to start retries the start every 60 s.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
