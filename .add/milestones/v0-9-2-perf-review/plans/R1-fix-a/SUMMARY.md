# R1-fix-a SUMMARY (wave-2a R1 review, area 1: WS36 moon#1299/#1303 + WS41 moon#1302)

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| F1 BLOCKER: TXN hold leaked on early connection exits | FIXED | 0a9023d | `tests/txn_exit_epilogue_1299.rs`, 8 tests: protocol fault s1+s4, BLPOP then peer RST s1+s4, reply over output-buffer limit, SUBSCRIBE+QUIT, SUBSCRIBE+protocol fault, PSYNC in the TXN. Each checks the write is rolled back, `txn_open:0`, `txn_held_keys:0`, the key is writable and FLUSHALL is OK. Red on f766fc2: monoio 7/8, tokio 4/8 (matches the review's matrix). Green 8/8 on both. | Pub/sub, MONITOR and tracking disconnect cleanup is still at the body tail, so the same early exits skip it. Pre-existing and not TXN; can move into the same epilogue (restricted to `Done` results). |
| F2 MAJOR: cross-shard DEL/UNLINK dropped a refused local leg | FIXED | 695c595 | `cross_shard_del_with_a_refused_local_slice_answers_the_conflict` (s4). A client on the held key's shard and one on another shard both get `-TXNCONFLICT`; held and co-located keys are not deleted. Red on both runtimes (`:1`). | none |
| F3 MINOR: INFO `txn_held_keys` stale | FIXED | 092c523 | Unit `published_held_keys_follow_every_hold_and_release`; integration `info_txn_held_keys_counts_every_held_key` (5→5, 7→7, db2→8, erroring writes stay 8, 0 after COMMIT and after ABORT). Integration red on both runtimes (`Some(1)`). | none |
| RESET leaves TXN open | FIXED | c0aee6c | `reset_ends_the_open_txn_and_releases_its_keys`: pipelined BEGIN/SET/RESET/GET reads the pre-TXN value; also from subscriber mode; `RESET extra` leaves the TXN open. Red on both runtimes. | none |
| Partial cross-shard MSET wording | FIXED | 162340e | Unit test in refused_leg_tests; integration `partially_applied_cross_shard_writes_say_so` (MSET and DEL say "partially executed"; an all-local refusal keeps the plain text). Red on both runtimes. | none |
| Killed-snapshot TXN.COMMIT keeps writes (pre-existing) | FIXED | e8992d3 | `killed_snapshot_commit_rolls_the_transaction_back`: the overwrite and the created key are both rolled back, INFO is 0/0. Red on both runtimes (GET read `txn`). | none |
| WS41 doc nit | FIXED (doc only) | 6147187 | Doc now scopes the FIFO guarantee to the owner thread's records and describes the foreign workspace-record case. | An optional behaviour fix is noted in the commit body: snapshot the channel length in `drain_into`. Not done because it cannot be tested deterministically and no harm was found. |

### Design notes
- **F1: why every exit now passes the epilogue.** `ConnectionState` moved out of the body.
  - Each runtime's entry point keeps its name and signature (`handle_connection_sharded_monoio`, `handle_connection_sharded_inner`), so callers are unchanged. It now lives in a new `handler_{monoio,sharded}/exit.rs`.
  - The wrapper builds the state, awaits `handle_connection_body(.., &mut conn)`, then runs `txn_abort::end_open_txn(.., Disconnect)`.
  - Every `return`, `break` and hand-off in the body hands control back to that `.await`, with the state still owned by the wrapper. A future early exit cannot skip the abort without editing exit.rs.
  - No caller wraps the handler future in a select or timeout.
- **F1: hand-offs.**
  - PSYNC hijack (monoio): the TXN is aborted before the stream is returned to the replica-sync caller. I chose this over refusing PSYNC: the socket stops being a client for good, the outcome equals a disconnect, and it needs no per-site check (the class of bug F1 is).
  - Migration and task-park already require `active_cross_txn.is_none()`, so the epilogue is a no-op there (debug-asserted). If that gate ever regressed, the TXN would be rolled back, not leaked.
- **Double abort** cannot happen: `end_open_txn` takes the TXN first, as do TXN.ABORT, COMMIT, RESET and the killed path.
- **Ordering change on tokio:** the stream is now dropped before the rollback instead of after. A client cannot observe this, because its own close always races the server's EOF.
- **F3:** `hold`/`unhold` store the held count on every change with one Relaxed store (`publish_held_keys`). The full `publish()` still runs on per-db 0↔1 transitions.
- **Partial wording:** new `ERR_TXN_CONFLICT_PARTIAL` = "TXNCONFLICT key held by an open transaction: command partially executed; its keys on the shard holding that key were left unchanged, the rest were applied". It keeps the TXNCONFLICT code, so clients' retry handling is unchanged.
- **F2 sibling audit:** no other fan-in drops a local error.
  - MSET already handled `local_err`.
  - MSETNX, BITOP and COPY run on one owner and return its reply.
  - The MGET/EXISTS pipeline split propagates the first error.
- **RESET:** `txn_abort::try_handle_reset` wraps the shared `shared::try_handle_reset`, which is unchanged. It is used at all 4 sharded call sites (monoio normal and subscriber arms, tokio normal and subscriber loop). The rollback is awaited before the reset, and the reply is always `+RESET`.
- **Killed commit:** the killed arm now calls `abort_logged(.., KilledCommit)`. `abort_local` retires the TXN via `txn_manager.abort` and releases intents and holds, as a dirty commit does. The reply text is unchanged.

## Measurements
No performance claims. The no-TXN path gains nothing, and F3 adds one Relaxed store per new hold.

Handler files shrank: monoio `mod.rs` 5108→5074 lines, tokio `mod.rs` 3876→3856. Grown: `coordinator.rs` 4478→4490, `pubsub.rs` 777→778, `isolation.rs` 806→868, `txn_abort.rs` 177→248.

## Gates (Linux container, not merge bar; shared target-c)
- `cargo fmt --check`: OK.
- `cargo clippy --all-targets -- -D warnings`: OK on both feature sets. Re-run after the final commit.
- `cargo test --release --lib`: monoio 6999 passed, tokio (`runtime-tokio,jemalloc`) 6054 passed, 0 failed on both. This ran before the allow-removal commit, which changes no code.
- Integration, monoio binary (all green): txn_isolation_1299 20, txn_abort_durability_1285 25, txn_multikey_undo_500, scripts_in_multi_894, inline_read_txn_visibility_807, graph_wal_append_1302 11, txn_partial_reject_monoio, blocking_peer_eof, subscriber_client_state, pubsub_resp3_push, pubsub_kv_ordering, pubsub_burst_delivery, multi_exec_queue_semantics, client_identity_introspection, monitor_command_feed, batch_protocol_version, protocol_error_lifetime, perf_ws18_proto_fault_defer, migration_batch_tail, parked_idle_parity, replication_streaming, replication_multishard, replication_hardening, and both new suites.
- Integration, tokio binary: the same list plus the in-process tokio suites (txn_kv_wiring 12, kill_snapshot 4, txn_partial_reject 2) and tokio+graph in-process suites (txn_completeness_edge_cases 7, txn_graph_wiring 5, txn_cypher_write_rollback 3), all green, except:
  - `scripts_in_multi_894::script_effects_in_exec_reach_the_replica_in_order`: known.
  - replication_streaming 0/7, replication_multishard 0/9, replication_hardening 1/6: tokio has no master-side PSYNC.
  - inline_read_txn_visibility_807 0/3.
  - All of these fail identically on the base `r1-f766fc2-tokio` binary; I ran each against it.
- Known on both runtimes: `review_w1_txn_abort_no_aof_snapshot_1285` has its two WS42 tests red (as expected).
- The reviewer's `run_r1.sh` repros (F1a–F1f, F2, F3, F3b, P1) are all clean on both new binaries.
- **Not run:**
  - `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: no parser or ShardSlice code touched, and disk is tight.
  - graph_wal_append_1302 on tokio: needs a tokio+graph server binary; that module's change is doc-only.
  - `txn_kv_wiring` itself (the suite named in the task) is in-process tokio only and ran 0 tests on monoio; it passed 12/12 on tokio.
  - Intermediate commits were not each built to a binary; red and green are proven on base versus final.

## Cross-ownership edits
- `src/shard/coordinator.rs` and `refused_leg_tests.rs` (F2, partial wording).
- `src/shard/wal_append.rs` (WS41 doc).
- `src/server/conn/handler_sharded/pubsub.rs` (RESET call site).
- `src/server/conn/handler_{monoio,sharded}/txn.rs` (killed commit).

## Risks / re-check at integration
- Anything integrated later that constructs `ConnectionState` inside the handler bodies, or calls the old `_inner`/body directly, must go through the exit wrappers.
- Merge conflicts are likely in handler `mod.rs`: the body now takes `conn: &mut ConnectionState`, so `&mut conn` became `conn` and `&conn` became `&*conn` throughout.
- `-TXNCONFLICT …partially executed…` is new user-visible text.
- The RESET behaviour change and the killed-commit rollback are user-visible.
- Pre-existing and not fixed: pub/sub, monitor and tracking cleanup skipped on the same early exits (follow-up).

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.92 · Edge cases 0.9 · Self-evaluation 0.9.
Edge cases stays at 0.9 because tokio's fatal cross-shard reply (a 30 s timeout) and subscriber write-error exits have no black-box test. They are covered by construction, since they are `return`s from the same body.

## CHANGELOG adjustment for moon#1299 (ready to paste)
- `TXN` (moon#1299): a connection that leaves with a transaction open now always rolls it back and releases its key holds, whatever the exit: protocol error, blocked `BLPOP` whose client vanished, output-buffer-limit disconnect, `QUIT` or error from subscriber mode, `PSYNC`. Previously such exits left the keys locked (`-TXNCONFLICT`, `FLUSHALL` refused) until restart.
- `RESET` now ends an open `TXN` (rolls it back and releases its keys), as redis `RESET` discards MULTI state.
- `TXN.COMMIT` answered `snapshot too old` (after `KILL SNAPSHOT`) now rolls the transaction's writes back; previously they stayed applied.
- A cross-shard `DEL`/`UNLINK` that includes a key held by an open transaction now answers `-TXNCONFLICT` instead of a count that hid unapplied keys. A cross-shard `MSET`/`DEL`/`UNLINK` refused on one shard while other shards applied answers `-TXNCONFLICT key held by an open transaction: command partially executed; …`.
- `INFO` `txn_held_keys` now counts every held key.
- Wording: holds cover the **KV plane** only (graph writes are not isolated).
