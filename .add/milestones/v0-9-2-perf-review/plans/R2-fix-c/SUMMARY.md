# R2-fix-c SUMMARY

Round-2 fixes for wave-2a, area 3. Branch `w2/r2-fix-c` in `/home/user/wt/lane-c`, based on `057598f`. It has 7 commits, `61e5737..2d1fea3`, and the tree is clean.

Everything below ran in a Linux container, so none of it is the merge bar.

Binaries:
- Final: `/home/user/wt/bin/r2fixc-fin-{monoio,tokio}`. Marker: the string `cold_held_release_snapshots_abandoned_txn`.
- Red proof: `r2fixc-hookonly-{monoio,tokio}`, which is 057598f plus only the test hook.
- Intermediate: `r2fixc-n1-{monoio,tokio}` and `r2fixc-n2-monoio`.

I did not edit CHANGELOG.md or any other orchestrator artifact.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| **N1 MAJOR** moon#1289: the automatic held-file snapshot captured a TXN that began between the pre-check and a shard's start | FIXED | 61e5737, 5466d09 (clippy nit) | New suite `tests/held_release_txn_race_1289.rs`, 5 tests; details in N1 evidence below. Reviewer's `txnrace.sh` (my copy on port 7645) on the fin binaries: **12/12 restarts `k=original`**. The same scripts captured 12/12 on 057598f in round 2. | WS42 (moon#1300) still owns BGSAVE, SAVE, the save rules and the SHUTDOWN save |
| **N2 MINOR** moon#1286: F4's immediate delete ran on a replica applying its master's stream | FIXED | 5448129 | New suite `tests/replica_past_deadline_1286.rs`:<br>• `a_lagging_replica_does_not_delete_what_its_master_kept`: red 3/3 on r1b-057598f-monoio (`del` published for a, b, c), green 2/2.<br>• `a_past_deadline_on_the_master_deletes_on_the_replica_too`: green on both. It guards the DEL half.<br>• Unit: `a_past_absolute_deadline_propagates_as_del`, `a_replica_applying_its_master_stream_stores_a_past_deadline`. | The replica still judges expiry by its own clock on master-stream lookups (see Risks) |
| **N4 MINOR** moon#1286: a write over an expired cold-only key was not counted | FIXED, no I/O | be90b30, 2d1fea3 | Unit test `a_write_over_an_expired_cold_only_key_counts_it_once`: red, then green.<br>Real server (reviewer's shape: 1,500 × 2 KB keys with PX 6000, maxmemory 512 KB, SET after expiry):<br>• r1b-057598f: `expired_keys` 186.<br>• fin: 1500, which is what redis counts. | — |
| **N3 NIT** docs | DOCUMENTED | cbbbd0a | `docs/guides/persistence.md`: exact guarantee, scope and starvation | CHANGELOG sentences below |
| **Item 5**: flaky tokio unit test `a_failed_save_frees_a_held_victim_off_the_shard_thread` | FIXED; root cause differs from the brief | 418ffde | See the item 5 notes below.<br>Full tokio lib suite (`--release --lib`, forced rebuild, my own test names confirmed in the output): **3 consecutive clean runs**, 6102 passed each, plus 2 clean runs earlier. | — |

**N1 evidence**
- `a_txn_between_the_request_and_the_start_is_not_saved_{1_shard,4_shards}` holds every shard at its start with `MOON_TEST_SNAPSHOT_START_HOLD_FILE`.
  - Red on hookonly monoio and tokio: the restart answers `k=aborted new=inserted`.
  - Plain 057598f fails the "start is held" precondition.
  - Green on the fin binaries, both runtimes. After the round: `abandoned=1`, no shard file renamed, no `.tmp` left, LASTSAVE unmoved, status ok, held files still on disk.
- `an_abandoned_round_is_retried_once_the_txn_ends`: green. The retry released the files about 0.8 s after the ABORT.
- `a_busy_txn_workload_never_reaches_the_automatic_snapshot_{1,4}`: red at s1 on r1b-057598f, green on the fix.
- Unit tests: `txn_round` (6), `snapshot_request` give-back (1), the fan-in test `an_abandoned_round_ends_without_an_outcome`, and `txn_after_start_tests` (2).

**Item 5 notes**
- **The cause is not shared state.** It is randomness in `sample_victim`. The fixture has one TTL key among 301 keys spread over 8 DashTable segments. Volatile sampling draws random segments and gives up after 40 draws, so it misses that key with probability (7/8)^40 = 0.48 %.
  - A temporary single-thread stress run of the fixture on the unfixed code, with nothing else running: **14 of 3000 runs answered OOM**. Three tests share the fixture.
- **This is a product bug:** volatile-* policies answered OOM while a volatile key existed. Redis cannot miss one.
- **Fix:** volatile-lru, volatile-lfu and volatile-random fall back to the exact nearest-deadline victim (`find_victim_volatile_ttl`) when sampling finds nothing.
- **New test** `volatile_fallback_tests::a_lone_volatile_key_is_always_found`: red with the fallback reverted ("volatile-lru round 1: OOM"), green with it.
- **Isolation hardening:** `footprint_correction()` really is process-global (the footprint round-trip test stores 2.5 in it). It is now pinned per thread, cfg(test) only, in the four tests whose budgets have no slack. No assertion was loosened.

## Design notes
**N1**
- `snapshot_request::request()` registers an automatic round through `bgsave_start_sharded_announcing`, before the epoch is broadcast. This only happens for reasons where `waits_for_open_txns()` is true, so BGSAVE, SAVE, the save rules and the SHUTDOWN save never register one.
- At the moment each shard would start its part (`check_auto_save_trigger`, before any state, temp file or hold stamp exists), `snapshot_txn_guard::skip_start` checks the shard's own `isolation::any_held()`. A hold abandons the whole round.
- Each shard's writer thread renames its own file, so a shard whose walk is done waits before `begin_finalize` until every shard has passed its start check. Then all shards go ahead, or all drop their part.
- An abandoned part goes through `SnapshotState::abandon`, a quiet abort in the new `snapshot/abandon.rs`. Then:
  - copy-on-write is disarmed;
  - `note_snapshot_finished(false)` is called, so no held file is released;
  - the state is dropped, and the stream writer's cancel path removes the temp file.
- A walk still in progress stops early (`is_abandoned`, one atomic load per tick).
- Fan-in: `bgsave_shard_abandoned()`. A fully abandoned round records no outcome (LASTSAVE, status and the dirty counter are untouched) unless a shard genuinely failed.
- The gate slot is given back (`SnapshotGate::give_back`). The abandon is counted once in the new INFO field `cold_held_release_snapshots_abandoned_txn` (reset by RESETSTAT) and logged once at warn.
- The round state is a `parking_lot::Mutex<Option<Round>>` plus one published epoch. That is not an atomic state machine, so there is no loom model.

**Why N1 is complete** (verified in code):
- A TXN writes only on its connection's shard (#499).
- The hold is taken before the write is dispatched (`capture_conn_write`, both runtimes; the script leg holds within the same synchronous run). It is released only after an abort's restore has been applied (`txn_end`).
- So a shard with no hold at its start has no uncommitted write in memory.
- Later TXN writes all capture their pre-image first, and the first capture wins. The paths are `command::dispatch` (both TXN legs call it), `redis.call` capture, and the abort's `undo_one`.
- `txn_after_start_tests` pins this, including an abort that lands mid-walk.

**Adversarial check of N1**
- `handle_pending_snapshot` / SnapshotBegin has no sender and is not a round. The replication full-sync snapshot does not publish or release held files.
- A shard that fails its start (lost or absent directory) still counts as started, so the barrier cannot wedge.
- A stream failure bypasses the barrier and reports as a failure.
- An abandon cannot happen after every shard has started.

**N2**
- `command::key::deadline_already_past` is the F4 check, and it answers false while `applying_master_stream()`. The four F4 arms use it: EXPIREAT, PEXPIREAT, GETEX EXAT/PXAT, and RESTORE ABSTTL.
- The master now propagates its immediate delete as `DEL` (`effect_rewrite`), as redis's `rewriteClientCommandVector` does:
  - EXPIREAT or PEXPIREAT with a past deadline that answered 1;
  - GETEX EXAT/PXAT with a past deadline that answered the value;
  - RESTORE ABSTTL past with REPLACE. Without REPLACE it propagates nothing, because nothing was written.
- Without the DEL half, the replica would now keep an invisible key forever.
- **AOF replay keeps F4.** It is not a master stream, and the clock is pinned to the log's time (moon#1277), so a replayed deadline is judged exactly as it was live. Redis skips while loading only because its clock is not pinned.

**N4**
- On a miss in `set_recording`, if the cold index is non-empty, do one in-RAM lookup (`ColdIndex::expired_at`, the sweep's rule `now > ttl`). An expired entry is counted and removed.
- It runs only under `counts_expiry()`: not during replay, not on a replica. The follow-up commit 2d1fea3 added that gate after the full monoio gate caught `cold_del_rewrite_tests::a_deleted_cold_key_whose_ttl_passed_…`: during replay, the cold slot is the replayed write's own spill.

## Gates
All at HEAD `2d1fea3`, with a forced rebuild. I verified that my own test names appear in each lib run.
- `cargo fmt --check`: 0.
- `cargo clippy --all-targets -- -D warnings`: 0. The same with `--no-default-features --features runtime-tokio,jemalloc`: 0.
- `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: 0.
- `cargo test --release --lib`:
  - monoio: 7048 passed, 0 failed.
  - tokio: 6102 passed in each of 3 consecutive runs (b1–b3), and 2 more clean runs earlier.
  - One earlier tokio run failed `command::geo::search_tests::geosearch_does_not_scale_with_n`: a wall-clock ratio of 10.1x against a bound of 8x, under load from other lanes, in code I did not touch.
- Integration on `r2fixc-fin-{monoio,tokio}` with `--include-ignored`:
  - `held_release_txn_race_1289`: 5/5 on both runtimes.
  - `held_release_txn_open_1289`: 3/3 on both.
  - `cold_held_files_release_1289`: 6/6 on both.
  - `expired_keys_parity_1286`: 7/7 on both.
  - `info_expired_keys_1286`: 7/7 on both.
  - `perf_ws21_snapshot_without_save_rules`: 9/9 on both.
  - `crash_recovery_cold_no_aof`: 10/10 on both.
  - `kill_snapshot`: 4/4 against both binaries. The suite is tokio-cfg-gated, so it ran with a tokio harness.
  - `review_w1_txn_abort_no_aof_snapshot_1285`: 1/3. These are the two known WS42 failures.
- Replication (monoio): `replica_past_deadline_1286` 2/2, `replication_ttl_semantics` 2/2. `replication_test` is tokio-cfg-gated and I did not run it.

## Measurements
None. There were no performance changes worth benchmarking:
- the N4 lookup runs only on a new-key insert while something is spilled;
- the N1 lock is taken once per shard per snapshot, plus one load per tick during a walk.

## Cross-ownership edits
- `src/storage/eviction.rs`: +13 lines in `select_victim`, plus a `mod` line. The file was already 4190 lines. The tests are in the new `eviction/volatile_fallback_tests.rs`.
- `src/admin/footprint.rs`: the cfg(test) pin.
- `src/persistence/snapshot.rs`: +3 `mod` lines. The file was already over the 1500-line cap.
- `src/shard/persistence_tick.rs`: about 20 lines. Over the cap.
- `src/command/connection.rs`: one INFO field. Over the cap.
- `src/command/key.rs`: a 12-line helper. Over the cap.
- `src/replication/effect_rewrite.rs`.
- `src/replication/apply.rs`: a cfg(test) scope helper.

## Risks / re-check at integration
- **Starvation (documented):** an open or constantly busy TXN keeps held files on disk and SWAPDB refused. There is no timeout. The reviewer's 1 ms / 0.2 ms flood at s1 went 60 s without publishing once. It never captured.
- **Mixed-version replication:** an old master sends `PEXPIREAT <past>` verbatim. A new replica now keeps that key, invisible, until the master writes it again.
- **Still open, pre-existing:** the replica judges expiry by its own clock on master-stream lookups. So in the lag repro, the `PERSIST` / `PEXPIRE` that follows misses. The keys stay resident (DBSIZE matches redis) but read as nil. Fixing it needs every `is_expired_at` lookup reached from apply to treat keys as live, which is not a one-line change.
- **Barrier:** a shard that never picks up the round's epoch (for example, it exits during shutdown) makes the other shards wait. This is the same wedge the fan-in counter already had.
- **N4:** one extra BTreeMap probe per new-key SET while the cold index is non-empty.
- **N4 history:** it spans two commits (be90b30 and the fix 2d1fea3). Squash them if one commit per item is required.
- **Shared target:** `target-c` aliases artifacts across lanes. My first "Finished in 0.28s" release build and a first clippy/lib gate had picked up another lane's artifacts, so I reran everything with `touch src/lib.rs` and marker or test-name checks. Other lanes should verify their gates the same way.

## CHANGELOG adjustments (ready to paste)
- **moon#1289** (replace the "never captures uncommitted writes" sentence):
  - The automatic held-file snapshot never contains a TXN's uncommitted writes. It waits while any TXN is open.
  - It is abandoned whole, and retried at a later sweep, if any shard holds an uncommitted TXN write when that shard starts its part. When that happens, no shard file is replaced, LASTSAVE does not move and no held file is released.
  - TXN writes after a shard's start are saved at their pre-transaction value.
  - New INFO field `cold_held_release_snapshots_abandoned_txn`.
  - An open TXN, or unbroken TXN traffic, keeps held files on disk and SWAPDB refused until a snapshot runs. BGSAVE, SAVE and the save rules are not covered yet (moon#1300).
- **moon#1286** (F4 sentence): an absolute deadline already past deletes the key at once and publishes `del`, as redis does. On a replica applying its master's stream the deadline is stored instead, and the master now propagates its immediate delete as `DEL`.
- **moon#1286** (F3 sentence): …a write that lands on an expired key is counted, including a key only the cold tier held.
- **moon#1286**: drop "hash-field expiry … as redis 7.2.7 does". Redis 7.2.7 has no hash-field TTL.
- **New bullet (eviction):** volatile-lru, volatile-lfu and volatile-random no longer answer OOM when random sampling misses the database's few TTL keys. They fall back to the nearest-deadline key.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9

- **Completeness:** the replica's own-clock lookups are out of scope and reported as a risk.
- **Edge cases:** the N4 replay regression was caught by the gate and fixed in 2d1fea3.
