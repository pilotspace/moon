# WS36-txn-isolation SUMMARY

Wave 2, lane A. Branch `w2/ws36-txn-isolation`: base `2e99254`, plus the three review tests cherry-picked from `review/wave1`. All results come from the **Linux container, not the merge bar**. `MOON_BIN` was pinned for every run.

## Per-issue verdict
| issue | verdict | commits | evidence |
|---|---|---|---|
| moon#1299 | FIXED | 87bb93d, 8de2698 | `tests/txn_isolation_1299.rs`: 20 real-server tests on monoio and tokio, at `--shards` 1 and 4.<br>• 18 fail on base on both runtimes; all 20 pass after.<br>• The 2 `disconnect_releases_*` tests pass trivially on base; they are regression guards.<br>• The review test `an_abort_does_not_overwrite_another_clients_acknowledged_write` goes red→green on both runtimes.<br>• Over-capture shapes from the PR #1301 review: on base 10/10 lose another client's write at s1, and 9/10 at s4 (there `SET 100` routes to another shard). After the fix, 0 do.<br>• Unit tests `transaction::isolation::tests` and the loom model `txn_isolation_view`. |
| moon#1303 | FIXED | 42f8e60 (the hold release on an error reply is in 87bb93d) | `an_erroring_write_holds_nothing_{1,4}_shard(s)`, both runtimes.<br>• Shapes: `SET k v BADOPT`, INCR of a non-number, and 2 WRONGTYPE shapes.<br>• On base, ABORT restored the pre-image over another client's write. After the fix, B's value survives.<br>• Unit tests `conn_capture::tests::*` and `kv_mvcc::tests::restore_undoes_record_write`. |

## Mechanism
**Hold table.** `src/transaction/isolation.rs` keeps a per-shard, thread-local hold table mapping `(db, key)` to a txn id. A TXN writes only on its own shard (#499), so no cross-thread state is needed. Every check is gated on one thread-local `Cell<usize>` load.

**Connection capture.** `src/transaction/conn_capture.rs` is shared by both handlers.
- Before the write runs, it refuses a key held by another TXN and poisons that TXN.
- If the write goes ahead, it records the pre-image, the write intent and the hold.
- After an error reply, it truncates the undo log back to the mark, restores the previous intents newest-first, and releases the holds this write created.

**Scopes and release points.**
- The TXN's own writes run in an `OwnerScope`; replica apply runs in a `BypassScope`.
- Holds are released at COMMIT and on the killed-snapshot commit path, which used to leak intents.
- ABORT, disconnect and dirty-commit all go through `abort_logged` with an `EndOnDrop` guard. It releases the holds only after the compensating records are enqueued, and also if the future is dropped mid-await.

**Errors:**
- `-TXNCONFLICT key held by an open transaction`
- `-TXNCONFLICT database has keys held by an open transaction`

Both are documented in `docs/guides/transactions.md`. A refused TXN is poisoned, so its COMMIT answers EXECABORT.

**Write paths covered:**
- **`command::dispatch`, at the top.** This covers local legs on both runtimes, routed SPSC legs, coordinator legs (MSET, DEL, COPY destination), MULTI/EXEC bodies from other clients, and script `redis.call`.
- **`dispatch_read`.** Reads only, so nothing to refuse.
- **`try_inline_dispatch`.** The inline SET falls back to generic dispatch while anything on the shard is held.
- **Blocking.**
  - `immediate_serve` and the MULTI BLPOP rewrite refuse to pop a held key.
  - The wakers (`serve_ready_key`, `serve_list_key` including a BLMOVE whose destination is held, `serve_zset_key`, stream wake) skip held keys and re-wake through `defer_wake` when the hold is released.
- **Writes outside dispatch.** `move_core` / `copy_core`, `MQ.*` via `execute_mq_on_owner`, and the workspace prefix sweep.
- **Whole-database commands.** FLUSHDB / FLUSHALL are checked in their dispatch arms. SWAPDB is checked in both `try_handle_swapdb`s. Both read the published per-shard view, so the refusal happens before anything is cleared or fanned out.
- **Eviction and expiry.** Eviction (`sample_victim`, volatile-ttl) and active expiry (whole-key sweep, lazy drain, hash-field sweep) skip held keys. A held key that has expired is reaped after release and does not latch the moon#1288 backlog.
- **Replica apply** bypasses all of this.

**INFO stats:** `txn_open`, `txn_oldest_age_ms`, `txn_held_keys`, `txn_conflicts_refused`.

## Measurements
**Hot path: not conclusive.** Load rose from 1.5 to 8–9 while the other lanes built, and medians swung ±30–46% in both directions. With no TXN open, the change adds one thread-local `Cell` load and a branch per `dispatch` and per inline SET, with no atomics and no allocations. `perf` is not installed here. **The orchestrator reruns the A/B at R1 in a quiet window.** The script is in the session scratchpad (`bench.sh`): interleaved base vs ws36, monoio, s1 and s4, P1 and P16, 3 reps, set/incr/hset.

## Gates
- fmt: 0.
- clippy `--all-targets -D warnings`: monoio 0, tokio 0.
- fuzz check: 0.
- `cargo test --lib`, full: monoio 6938 passed, tokio 5996 passed.
- Loom `txn_isolation_view` under `--cfg loom` in a standalone crate: 4/4.

| suite (`--include-ignored`) | monoio | tokio |
|---|---|---|
| txn_isolation_1299 | 20/20 | 20/20 |
| txn_abort_durability_1285 | 25/25 | 25/25 |
| txn_multikey_undo_500 | 5/5 | 5/5 |
| txn_partial_reject_monoio / txn_partial_reject | 2/2 | 2/2 |
| inline_read_txn_visibility_807 | 3/3 | — |
| txn_kv_wiring | — | 12/12 |
| script_key_routing | 5/5 | 5/5 |
| eviction_reason_del_run_budget_1294 | 1/1 | 1/1 |
| active_expiry_backlog_drain_1288 | 1/1 | 1/1 |
| scripts_in_multi_894 | 6/6 | 5/6 (known: no master-side PSYNC on tokio) |
| review_w1_txn_abort_no_aof_snapshot_1285 | 1/3 | 1/3 (the other 2 are red until WS42 / #1300) |

## Cross-ownership edits
**Over-cap files:**

| file | change |
|---|---|
| `handler_monoio/mod.rs` | −46 net |
| `handler_sharded/mod.rs` | −21 net |
| `blocking.rs` | +8 |
| `spsc_handler.rs`, `shared.rs` | untouched |

**Other files:**
- `command/mod.rs` (+21), `storage/eviction.rs` (+12), `server/expiration.rs`, `command/connection.rs` (INFO), `workspace/mod.rs`.
- `blocking/{wakeup,stream_wake}.rs`, `command/keyspace/move_cmd.rs`, `shard/mq_exec.rs`, `replication/apply.rs`.
- `scripting/bridge/{redis_call,txn_capture}.rs`, `server/conn/{txn_abort,txn_script_undo,blocking_txn}.rs`, `handler_*/{txn,dispatch}.rs`.
- `transaction/{mod,kv_mvcc,undo_log}.rs`.
- `tests/loom_response_slot.rs` (a module appended) and `docs/guides/transactions.md`.

## Risks / re-check at integration
1. **Cross-shard multi-key writes:** a leg that meets a held key is refused while the other legs apply. This is the same non-atomic shape as any failed leg. MULTI bodies from other clients get a per-command TXNCONFLICT element, which matches redis's runtime-error semantics.
2. **FLUSH / SWAPDB race:** a hold taken on another shard between the view check and the fan-out.
   - For FLUSH, each leg re-checks, so a held key is never cleared, but the flush can be partial.
   - The SWAPDB apply legs do not re-check.
3. **volatile-ttl eviction** finds no victim while the nearest deadline belongs to a held key. That can answer OOM for as long as the TXN is open.
4. **Hash-field expiry** stops for the tick when its head is a held hash. This delays reclaim only; reads already filter expired fields.
5. **Blocking pops inside the TXN itself** are still not undo-captured (pre-existing). On a key the TXN holds, they now answer TXNCONFLICT.
6. **A connection-task panic** between capture and COMMIT/ABORT leaks the hold, as intents already leaked before this change. Cancellation of `abort_logged` itself is covered by the drop guard.
7. **Databases ≥ 63** share one bit in the published view, so refusals there can be conservative. They are never missed.
8. **Shared files with later workstreams.** WS41 and WS42 share `txn_abort.rs` (`EndOnDrop` at the top of `abort_logged`) and `handler_*/txn.rs`. WS45 can reuse `isolation::{hold, is_held, check_write}`.

## CHANGELOG bullets
- **Fixed (moon#1299):** a key written inside an open cross-store `TXN` is now held until `TXN COMMIT` / `TXN ABORT`.
  - Another client's write to it is refused with `-TXNCONFLICT key held by an open transaction`, instead of being silently overwritten by the abort. This covers plain, inline, MULTI/EXEC, script, blocking-pop, MOVE/COPY … DB, MQ and routed writes.
  - `FLUSHDB` / `FLUSHALL` / `SWAPDB` on a database with held keys answer `-TXNCONFLICT database has keys held by an open transaction`.
  - Eviction and active expiry skip held keys, and blocked clients are not served from a held key until it is released. A replica applies its master's stream unconditionally.
  - New `INFO stats` fields: `txn_open`, `txn_oldest_age_ms`, `txn_held_keys`, `txn_conflicts_refused`.
- **Fixed (moon#1303):** a TXN connection write that answers an error (`SET k v BADOPT`, `INCR` of a non-number, WRONGTYPE) no longer keeps its undo capture, write intent or key hold. `TXN ABORT` can therefore no longer restore a stale pre-image over another client's write.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.85 (the hot-path A/B is pending a quiet window) · Edge cases 0.9 · Self-evaluation 0.9
