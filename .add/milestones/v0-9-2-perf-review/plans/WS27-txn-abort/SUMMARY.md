# WS27-txn-abort SUMMARY

Wave 1 of the next round (base main `ce65400`, branch `perf/ws27`, integrated into
`claude/gifted-mendel-e9wiz5`). Issues: moon#1285, moon#1185 (maintainer pick: option b, all engines).
All results are from a **Linux container, not the merge bar**.

## Correction to the brief
The brief said graph rollback "already logs compensating WAL records". **It doesn't.** `MemGraph`
never pushes to `wal_pending`, so a rollback logged nothing. On `ce65400`, aborted Cypher
CREATE/SET/DELETE came back after a restart.

## Per-issue verdict
| issue | verdict | commits (perf/ws27) | evidence |
|---|---|---|---|
| moon#1285: an aborted KV write comes back after kill -9; replicas keep the aborted value | FIXED | fdee444 (test), 5943f40, 4c945b3 | `kv_abort_survives_restart_shards_{1,4}` and `kv_disconnect_abort_survives_restart` fail on the base binaries (both runtimes) and pass after (both runtimes). The KV half of `replica_converges_to_the_aborted_to_state` passes on monoio. |
| New: a `SELECT` inside a TXN made the abort restore the db selected at abort time | FIXED | 5943f40, 4c945b3 | `kv_abort_with_select_inside_the_txn_shards_{1,4}` fails on base and passes after, both runtimes. |
| #1185 KV: pre-images are moved, not cloned; BGSAVE stays point-in-time | FIXED | 4c945b3, 1f7a8d2 | `snapshot::txn_abort_tests` (4). Reverting only the capture makes it fail. The clone counter is unchanged. |
| #1185 vector | FIXED (no new WAL payload) | e43cacd, cdf870d | `vector_abort_is_live_correct_and_survives_restart` fails on base (monoio: 0 docs live; tokio: doc:2 lost) and passes after, both runtimes. |
| #1185 graph (audit found a bug) | FIXED | 5cbb34b, c0a39c9 | `graph_abort_survives_restart` fails on base and passes after (monoio). Unit tests: replaying the rollback gives the same result as the live abort, for both restart and replica apply. |
| #1185 MQ | PUBLISH is correct (held until commit) | fdee444 | `mq_publish_intents_do_not_leak_on_abort` passes on base and after. MQ PUSH/POP/ACK inside a TXN still need a maintainer decision (they are not transactional). |
| CI wiring | DONE | f6cc36f | `integration-tests.yml` runs the suite with `MOON_BIN` pinned. |

## Design
- **KV** (`src/transaction/kv_compensation.rs`):
  - Undo records keep their db.
  - Only the first undo per (db, key) is applied, which is the pre-TXN state.
  - The abort logs compensating records: `DEL` for an undone insert; `RESTORE … REPLACE ABSTTL` plus `HPEXPIREAT` per field deadline for an undone update or delete.
  - The replaced value goes to the snapshot by move (`capture_removed_sized`, the moon#1269 pattern).
- **Orchestration:** `server/conn/txn_abort::abort_logged` is the single caller on both runtimes (ABORT, dirty-COMMIT rollback, disconnect).
  - In one stretch with no await it stamps the fold epoch, records replication and appends the graph WAL records.
  - It then calls `persist_txn_aof`.
  - If the AOF refuses the records, the reply carries that refusal instead of `+OK`.
- **Vector / text:**
  - The documents of every restored key are rebuilt through the HSET path. Their MVCC tombstone keeps `AS_OF` history.
  - `RESTORE` now rebuilds FT documents on every write path, which is what lets replicas converge.
  - Restart needs no vector WAL record: recovery rebuilds vectors from the keyspace.
- **Graph:**
  - The rollback emits `REMOVENODE` / `REMOVEEDGE` / `SETPROP <old>`, plus three new records: `GRAPH.DELPROP`, `GRAPH.UNDELETENODE`, `GRAPH.UNDELETEEDGE`.
  - Replay now follows WAL order.
  - New fuzz target `graph_wal_replay`, in both fuzz.yml matrices.

## Measurements
TXN.ABORT latency, base vs ws27. 3 alternating reps × 5 aborts, `--shards 1`, AOF everysec, box load average ~6.5 (relative numbers only):

| restored value | base median | after median |
|---|---|---|
| 200k-field hash | 6.51 ms | 17.59 ms |
| 1k-field hash | 0.05 ms | 0.08 ms |

The extra ~11 ms at 200k fields is the DUMP that `RESTORE` needs. It is O(value) and inherent to absolute compensation records.

## Residual risks
1. The abort costs O(value) per restored key.
2. The three new graph WAL records are forward-only. Downgrade was not tested.
3. Graph and MQ records flush on the WAL-v3 tick, not behind the AOF barrier.
4. Behaviour change: TXN.ABORT can now answer an AOF refusal instead of `+OK`.
5. Still open, pre-existing:
   - MQ PUSH/POP/ACK are not transactional.
   - TXN.COMMIT on a killed snapshot leaves intents unreleased (found by reading the code, not tested).
   - The abort's restores do not trigger client-tracking invalidation, keyspace notifications or blocking wakeups.
   - Lua writes inside a TXN are not undo-captured.
6. The fuzz target type-checks but was not run: no cargo-fuzz here.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.85 · Edge cases 0.9 · Self-evaluation 0.9
