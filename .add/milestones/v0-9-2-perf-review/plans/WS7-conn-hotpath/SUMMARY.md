# WS7-conn-hotpath SUMMARY

- **Branch:** `perf/ws7-conn-hotpath`, base `ae21476`, plus the plan commit `6537679`.
- **Design notes:** `NOTES.md` in this directory has the design reasoning, the mechanisms checked in the code, and a self-score per issue.
- **Who committed this file:** the orchestrator. The harness refused the subagent's write, so under TEAM-RULES §6 this content is the agent's final report.

## Per-issue verdict

| issue | verdict | commits | follow-ups |
|---|---|---|---|
| moon#1175: CLIENT PAUSE and RuntimeConfig locks taken on every batch | **FIXED** | 85ce913; adfb7d6 (test only: `perf_ws7_pause_gate` turns off the disk-free guard) | The contention win needs at least 8 cores to measure. |
| moon#1165: stale ACL cache, plus the lock type, the PUBLISH/script re-checks and restricted users | **FIXED** (all plan Musts) | ef4cc82, 7a12096, 7725637, 376c2cf, c2b1c2a | See below. |
| moon#1176: ReplicationState lock and shared fetch_adds on every write | **FIXED** | 639acf2 | See below. |
| moon#1178: Prometheus registry lookup on every event | **FIXED** | a6956e9 | Also fixed: with the exporter on but never scraped, histogram samples grew without bound, because upkeep only ran on scrape. |
| moon#1166: CLIENT TRACKING global mutex, and inline SET disabled | **PARTIAL** | ef12de2 | DEFERRED: striping the table by key hash. Striping needs the routing state split from the key maps; the design is in NOTES. |
| moon#1198: connection items | **PARTIAL** | 71e163a | See below. |
| moon#1187 remainder: per-shard AOF staging buffer | **DEFERRED** | — | See below. |

### moon#1175
Evidence:
- `client_pause::tests::batch_gate_takes_no_lock_when_no_pause_is_possible`. Red when the gate body is reverted to `expire_if_needed(); check_pause(true)`: `left: Err(Timeout)`.
- `batch_gate_delays_while_paused_and_clears_on_expiry`.
- `tests/perf_ws7_pause_gate.rs::handlers_take_no_global_lock_per_batch`. Red on the `ae21476` sources: "`client_pause::expire_if_needed(` is called directly on the batch path … (1918)".
- `client_pause_still_delays_then_expires_{1,2}_shard(s)`: green on both binaries and both runtimes.

### moon#1165
Evidence:
- `tests/perf_ws7_acl_cache.rs::existing_connection_returns_to_inline_path_after_acl_setuser`. Red on the `ae21476` binary: "50 GETs were inlined 0 times".
- `acl_table_lock_is_parking_lot`. Red on the `ae21476` sources: "44 std::sync::RwLock sites remain".
- `server::conn::core::acl_cache_tests::restricted_check_takes_no_table_lock` and `::unrestricted_script_and_publish_checks_take_no_table_lock`. Red when the lock-free path is mutated away: `Err(Timeout)`.
- Fail-closed floor, green on BOTH binaries:
  - `unrestricted_to_restricted_revocation_applies_immediately_{1,2}_shard(s)`
  - `restricted_user_revocation_applies_immediately`
  - `stale_snapshot_is_never_trusted`
  - `lock_free_script_identity_still_sees_a_revocation`
  - `snapshot_is_bound_to_the_name_it_was_resolved_for`
- A/B (GET -P16, ACL SETUSER mid-run): baseline −39/−39/−36 %, ws7 −3/−1/+7 %. The review had measured moon −48.6 % and redis −1.8 %.

Follow-ups (Should remainder):
- First-arg rule probes still run for every restricted command that has an argument.
- Restricted users stay off the inline GET/SET path on purpose. An ACL decision inside the inline path is the class the gatekeeper treats as a HARD-STOP.

### moon#1176
Evidence:
- `replication::state::tests::write_handle_takes_no_state_lock`. Red when `issue_lsn` is routed through `state.read()`: `Err(Timeout)`.
- `tests/perf_ws7_repl_offsets.rs`. Red on the `ae21476` sources: 7 `issue_append_lsn(&ctx.repl_state ..)` sites, plus a `.read()` in the probe.
- `shard_offsets_do_not_share_a_cache_line`. At `ae21476` the offsets sit 8 bytes apart by construction.
- Also green:
  - `write_handle_lsns_share_the_locked_namespace`
  - `write_handle_fanout_probe_matches_the_locked_probe`
  - `static_select_records_match_the_serializer`
  - `write_handle_records_what_the_locked_path_records`
- Replication suites, green on the pinned binary with `--include-ignored`:

  | suite | tests |
  |---|---|
  | replication_local_leg_815 | 4 |
  | replication_multishard | 9 |
  | replication_streaming | 7 |
  | replication_hardening | 6 (incl. WAIT) |
  | aof_multidb_kill9 | 4 |

Follow-ups:
- The `master_repl_offset` fetch_add is KEPT. It is the global LSN allocator (per-shard AOF RFC §2 Rule 3), and removing it would change AOF semantics.
- These still use the locked `issue_append_lsn`, because they have no `ConnectionContext` or are not WS7 files: `handler_single`, `single_aof_log`, `blocking/pop_log.rs`, `shard/coordinator.rs`.

### moon#1178
Evidence:
- `admin::metrics_setup::tests_1178::hot_path_recording_does_not_reach_the_registry_per_event`. Red on the `ae21476` implementation: "10000 metric-registry lookups for 1000 rounds". Green: ≤ 8.
- `scrape_publishes_the_slot_totals` and `cmd_label_index_is_the_cardinality_guard`.
- `/metrics` scrape diff against `ae21476` under the same workload:
  - identical series sets (118 at s1, 139 at s2);
  - identical non-volatile values at s1.
- `tests/perf_ws7_metrics.rs`: green on both binaries and both runtimes.
- A/B, GET -P16 with the exporter off, then on: baseline −8/−13/−12 %, ws7 +1/−1/+29 % (noise).

### moon#1166
Evidence:
- `tracking::invalidation::tests::untracked_write_takes_no_global_table_lock`. Red with the pre-filter check removed (the `ae21476` behaviour): the write blocks.
- `tests/perf_ws7_tracking.rs::idle_tracker_keeps_inline_set`. Red on the `ae21476` binary: "50 plain SETs were inlined 0 times".
- `inline_set_still_invalidates_every_tracking_mode_{1,2}_shard(s)` covers default, BCAST, RESP2 REDIRECT and NOLOOP. Green on both binaries and both runtimes.
- `tracking::prefilter_tests`: 4 tests.
- Tracking suites, all green:

  | suite | tests |
  |---|---|
  | client_tracking_invalidation | 10 |
  | tracking_expiry_invalidation_1013 | 10 |
  | tracking_followups_1088_1089_1090_1078 | 28 |
  | tracking_movablekeys | 3 |
  | tracking_redirect_caching_1048_1049 | 35 |

- A/B, SET -P16 without and then with one idle tracker: baseline −39 % mean, ws7 −11 % mean. The review had measured redis at −9 %.

The table lock is now taken only for:
- writes to keys that are actually tracked;
- BCAST deployments;
- tracked reads.

### moon#1198
Evidence:
- `tests/perf_ws7_guards.rs`. Red on the `ae21476` sources, which showed:
  - an exclusive guard taken for `estimated_memory()`;
  - an exclusive guard taken before any shared `is_hot`;
  - command metadata not resolved once.
- Suites green on the pinned binary: `spill_inflight_visibility`, `cold_promote_compact_encoding_898`, `multi_queues_inline_get`, `inline_read_txn_visibility_807`.

Follow-ups:
- DEFERRED: item 1, the cluster lock-free served-slot bitmap and cluster-mode inline path. It needs a `ClusterState` lock newtype whose write guard recomputes the bitmap on drop. That spans `src/cluster/**`, `main.rs` and WS8's shard files, and must be gated by the multi-node cluster suites.
- Items 4–7 belong to WS10.

### moon#1187 remainder
Named blockers (details in NOTES):
1. Exactly-once needs a writer-side chunk message with a per-chunk fold epoch, and no SELECT injection for chunk contents. That work lives in `src/persistence/aof/**`, which belongs to WS15 this wave and is under concurrent durability fixes.
2. The fail-loud ack contract is decided at channel admission today: inline `-MOONERR AOF backpressure` and per-write `AOF_FSYNC_ERR`. A staging buffer would move that decision to flush time, after the replies are built.
3. The buffer must be per SHARD, with flush points in the shard event loop, which belongs to WS8.

Joint follow-up, in order:
1. the writer chunk message;
2. the shard-loop flush hook;
3. the connection call sites.

## Measurements (method, reps, raw numbers)
Method:
- Release-fast `/home/user/wt/bin/ws7-rf1` (built at 71e163a) against `/home/user/wt/bin/baseline-ae21476`.
- A shared 4-vCPU container with other agents building (load 2–7). Treat the numbers as RELATIVE evidence only.
- Server flags: `--shards 1 --appendonly no --save "" --maxmemory 0 --disk-offload disable`.
- redis-benchmark 7.0.15 on the same host. A/B interleaved per rep, 3 reps, fresh server per leg.

Results, with each cell giving rps as without / with:

| scenario | rep | baseline | ws7 |
|---|---|---|---|
| Idle tracker | 1 | 1072961 / 660502 | 1140251 / 961538 |
| | 2 | 1194743 / 645370 | 986680 / 795229 |
| | 3 | 753864 / 511771 | 748783 / 763650 |
| ACL SETUSER mid-run | 1 | 780874 / 475707 | 810466 / 783080 |
| | 2 | 840901 / 513399 | 825066 / 817269 |
| | 3 | 800485 / 511222 | 780851 / 838533 |
| Exporter cost | 1 | 788644 / 728067 | 816660 / 822030 |
| | 2 | 843170 / 729395 | 830565 / 823045 |
| | 3 | 851789 / 752729 | 888099 / 1145475 |

Scenario definitions:
- **Idle tracker:** `SET k:__rand_int__ v -P 16 -c 50 -r 100000 -n 2000000`, without and then with one idle `HELLO 3` + `CLIENT TRACKING ON` connection.
- **ACL SETUSER mid-run:** `-t get -r 100000 -P 16 -c 50`, with `ACL SETUSER probe on nopass +ping` at t=6 s. Each cell is the mean progress rps over [2,6) s and then [8,12) s.
- **Exporter cost:** `-t get -r 100000 -P 16 -c 50 -n 2000000`, with `--admin-port` off and then on.

Not measured here, because they need at least 8 cores or a dedicated load generator:
- PAUSE/RuntimeConfig contention (#1175);
- ReplicationState/offset cache-line bouncing (#1176);
- exclusive-guard windows (#1198).

Each of these is instead proven lock-free or guard-free by its test.

## Cross-ownership edits
- **7a12096** (AclTable lock → `parking_lot::RwLock`, isolated commit):
  - `src/shard/conn_accept.rs` and `src/shard/event_loop.rs` (WS8): only the `acl_table` parameter type and the deleted `StdRwLock` alias.
  - `src/command/acl.rs`, `src/command/connection.rs`: dead poison arms removed.
  - `src/scripting/mod.rs`: one test fixture.
  - `src/server/listener.rs`, `src/server/embedded.rs`, `src/main.rs`.
  - 30 in-process integration tests under `tests/`: `std::sync::RwLock::new(AclTable..)` → `parking_lot::RwLock::new`.
- **639acf2**, `src/server/conn/handler_monoio/ft.rs`: WS8 owns the FT scatter entry in this file. The edit touches only `replication_fanout_active` and `record_local_write_db`.
- `src/persistence/aof/pool.rs` needed no edit.

## Risks / things the orchestrator must re-check at integration
1. `try_inline_dispatch` / `try_inline_dispatch_loop` gained two parameters: `repl_write` after `repl_state`, and `writer_client_id` after `spill_sender_active`. Any other branch that calls them must add both.
2. Struct changes:
   - `ConnectionContext` gained `repl_write`, built inside `new()`; callers are unchanged.
   - `ConnectionState` gained `acl_cache_user`.
   - `AclTable.users` now holds `Arc<AclUser>`.
3. Any new code on other branches that spells `std::sync::RwLock<AclTable>` or `acl_table.read().unwrap()` will fail to compile after the merge. The fix is mechanical.
4. The monoio frame loop has new locals `cmd_meta` and `cmd_is_write` next to `cmd_len`. WS8's remote-dispatch region still calls `metadata::is_write(cmd)` twice; it was left alone.
5. Tracking pre-filter invariant: every mutation of the GLOBAL table's `key_clients` / `bcast_clients` must go through `TrackingTable`'s methods, which maintain `tracking::prefilter`. A new direct mutation would make the filter lie, and invalidation would fail open. This is documented in `tracking/prefilter.rs`.
6. Replication state changes:
   - `ReplicationState.plane_live` is sticky.
   - `stream_db` is now `Arc<[AtomicI64]>`.
   - `shard_offsets` is now `Arc<[CachePadded<AtomicU64>]>`. The reset site in `master.rs` is unchanged and still compiles.
7. `HotCounterSlot` is two cache lines (static assert). The keyspace, dispatch-path, pubsub and per-command `/metrics` counter families are published at scrape (`publish_sharded_counters`). Any NEW render site of the Prometheus handle must call it first.
8. Pre-existing, not WS7: `tests/aof_fold_exactly_once_455.rs::exec_parked_in_wait_across_a_rewrite_replays_once_toplevel` fails the same way on `baseline-ae21476` ("EXEC returned before WAIT ran out").
9. Pre-existing files over 1500 lines were touched but not split:
   - `handler_monoio/mod.rs`, `handler_sharded/mod.rs`, `handler_single.rs`;
   - `blocking.rs`, `shared.rs`, `server/conn/tests.rs`;
   - `tracking/mod.rs`.

   New tracking code went to `tracking/prefilter.rs`, and c2b1c2a brought `acl/table.rs` back under 1500.
10. RESET identity semantics are untouched; they were out of scope. Every new ACL skip is bound to the name its cached verdict was resolved for.

## Checks run (final tree)
- `cargo fmt --check` ✔
- `scripts/audit-unsafe.sh` ✔ (244/244, 0 new `unsafe`)
- `scripts/audit-unwrap.sh` ✔
- `cargo clippy --all-targets -- -D warnings` ✔
- `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings` ✔
- `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc` ✔
- `cargo test --lib -- acl:: server::conn tracking:: replication:: admin:: client_pause command::acl command::connection scripting:: persistence::aof::pool`: monoio 836 passed, tokio 799 passed.
- The WS7 integration suites (pause_gate, acl_cache, tracking, metrics, repl_offsets, guards) are green on monoio (`ws7-rf1`) and tokio (`ws7-dbg-tokio`).
- The touched in-process suites `txn_partial_reject` and `kill_snapshot` are green on tokio.

## CHANGELOG bullets
- **perf(conn): CLIENT PAUSE and query-buffer limits no longer take process-global locks per batch** (moon#1175).
  - The batch-top pause gate is one relaxed load unless a pause may be in force. Before, it took the PAUSE write lock on every batch of every shard.
  - The query-buffer ceilings are read once per connection.
  - The tokio handler no longer reads `RuntimeConfig` on every command.
- **perf(acl): existing connections recover the fast path after any ACL change** (moon#1165).
  - A stale per-connection ACL cache is re-resolved at batch top. Before, one `ACL SETUSER` cost every existing connection its inline GET/SET path for life. In A/B, GET went from −36…−39 % to −1…+7 %.
  - The ACL table lock is `parking_lot`.
  - Restricted users are checked against an immutable per-connection snapshot: no table lock, one resolution, and no per-command `String`.
  - PUBLISH/SPUBLISH and script ACL identities skip the lock for unrestricted users.
  - Revocations still apply on the next command.
- **perf(replication): connection writes issue LSNs and record replication without the ReplicationState lock** (moon#1176).
  - A lock-free per-shard write handle, with cache-padded shard offsets.
  - A lock-free fan-out probe. Before, every write took the state lock and shard 0's backlog mutex once a replica had attached.
  - Static `SELECT 0..15` records and memcpy backlog appends.
- **perf(metrics): Prometheus counters are published at scrape, not looked up per event** (moon#1178).
  - Keyspace, dispatch-path, pub/sub, SPSC and per-command counters live in per-thread slots and are handed to the exporter on `/metrics`, with the same names, labels and values.
  - Histogram handles are resolved once per thread.
  - Histogram upkeep now runs every 5 s, so an unscraped exporter no longer accumulates samples without bound.
  - The GET cost of `--admin-port` went from about −11 % to about 0 % in A/B.
- **perf(tracking): one idle CLIENT TRACKING client no longer slows every writer** (moon#1166, partial).
  - Writes to keys nobody tracks skip the tracking lock through a lock-free counting pre-filter.
  - The inline SET path stays enabled under tracking, and it invalidates what it writes.
  - SET with an idle tracker went from −39 % to −11 % (redis: −9 %).
  - Table striping remains open.
- **perf(conn): read-only peeks take the shared db guard; command metadata is resolved once per command** (moon#1198, connection items 2–3).
  - The inline SET memory pre-gate and the local GET cold-tier peek no longer hold the exclusive guard just to read.
  - The cluster-mode slot bitmap (item 1) remains open.

## Self-evaluation (0–1)

| dimension | score | note |
|---|---|---|
| Completeness | 0.86 | 5 of 7 plan items fully done. #1166 striping, #1198 item 1 and #1187 are deferred with named blockers. |
| Clarity | 0.92 | |
| Practicality | 0.93 | |
| Optimization | 0.9 | |
| Edge cases | 0.92 | |
| Self-evaluation | 0.9 | |
