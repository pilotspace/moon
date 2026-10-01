# R1-fix-c SUMMARY

Wave-2a R1, area 3. Branch `w2/r1-fix-c` in `/home/user/wt/lane-c`, based on `f766fc2`. It has 9 commits, one per finding (8e77811..7f76f83). The WIP commits are folded in: the scratch branch `w2/r1-fix-c-wip` was only a parking place, and I have deleted it. The final tree is byte-identical to the tree the gates ran on. I did not build the intermediate commits one by one.

All of this ran in a Linux container, so none of it is the merge bar.

Binaries are `/home/user/wt/bin/r1fixc-fin2-{monoio,tokio}`. The marker is the string `invalid expire time in 'getex' command`.

## Per-issue verdict
| issue | verdict | commit | evidence (red on f766fc2 → green) | follow-ups |
|---|---|---|---|---|
| F1 moon#1289 (MAJOR): the auto held-file snapshot captured an open TXN's writes | FIXED | 8e77811 | New suite `tests/held_release_txn_open_1289.rs`, 3 tests: abort + kill -9 at s1, the same at s4, and "the deferred snapshot runs once the TXN ends".<br>• r1 monoio and tokio: 3/3 red. The restart answers `k=aborted new=inserted`, and 1 snapshot was requested during the TXN.<br>• Fix, both runtimes: 3/3 green.<br>• Unit test: `an_open_txn_defers_an_automatic_snapshot_without_consuming_the_slot`. | WS42 (moon#1300) for BGSAVE, SAVE and the save rules |
| F2 moon#1286 (MAJOR): AOF replay re-counted `expired_keys` | FIXED | 18dad88 | `aof_replay_does_not_recount_*`, s1 and s4, graceful and kill -9 restart.<br>• Oracle: 1050 before, 0 after.<br>• r1 monoio and tokio: (1050, 1050).<br>• Fix, both runtimes: (0, 0).<br>• Unit test is red with the gate removed (it counts 2). | — |
| F3 moon#1286: writes over an expired key were not counted | FIXED | cd1939a | Oracle matrix of 37 cases, 1000 keys each: redis 7.2.7 counts 1000 in every case.<br>• r1: 0 or partial counts in 22 cases.<br>• Fix, monoio: 37/37 match at s1 and s4, including SET KEEPTTL = 1000 and SET PXAT past = 2000, both equal to redis.<br>• Rust suite `a_write_over_an_expired_key_counts_it_*` (22 cases + 5 controls): red on both r1 runtimes, green on both fix runtimes. | — |
| F4 + F9 NITs moon#1286: an absolute expiry already in the past | FIXED | 415aa27 | `a_past_absolute_deadline_*` has 19 cases. Each compares the reply bytes, EXISTS, the keyevents and the `expired_keys` delta with redis 7.2.7.<br>• r1: 17 of 19 wrong.<br>• Fix, both runtimes: all 19 match.<br>• Also fixed: EXPIREAT or EXPIRE -1 on an already-expired key now answers 0 and the reap is counted, as redis does.<br>• 6 unit tests. | Moon publishes no `expire` or `restore` keyspace event on a future deadline. This is pre-existing and was not in scope. |
| F5 moon#1286: CONFIG RESETSTAT | FIXED | b28a3bb | `config_resetstat_zeroes_expired_keys`.<br>• r1 monoio and tokio: 50 after RESETSTAT.<br>• Fix: 0, and `txn_conflicts_refused` goes from 1 to 0.<br>• 1 unit test. | RESETSTAT still does not reset other redis stats (`keyspace_hits`, `evicted_keys`, …) |
| F6 moon#1289: docs for `save ""` | DOCUMENTED | 9418883 | Fixed the stale doc comment in `snapshot_hold.rs`. Added the section "Automatic snapshots with disk offload" to `docs/guides/persistence.md`. | The CHANGELOG sentence is below |
| F7 moon#1296: ACL SAVE→LOAD was unstable | FIXED | 2125024 | `grants_under_all_survive_save_and_load_*`: 11 cases × 2 SAVE/LOAD rounds, byte-equal to redis.<br>• r1: red on both runtimes; the finding's case reloads as `+@all -set`.<br>• Fix: green on both runtimes, and the existing 14 cases still pass.<br>• Oracle diff: only the moon#1306 category cases still differ, and they are now stable too.<br>• 2 unit tests. | moon#1306 |
| F10 moon#1289 NIT: a failed fold dispatch was counted | FIXED | 592bc9b | Unit test `only_a_dispatched_held_release_fold_counts_and_arms_the_spacing`. The red proof is structural: the monitor loop has no seam, and a real dispatch failure cannot be forced from outside. | — |
| F11 moon#1286 NIT: an unlink error lost the count | FIXED | 7f76f83 | Unit test with an injected manifest persist error: the old `cold_index.rs` counts 0 of 2, the fix counts 2. | — |

## Design notes
- **F1:**
  - The signal is the process-wide published view behind `INFO txn_open` (`transaction::isolation::info`). It does not read another shard's thread-local.
  - `SnapshotReason::waits_for_open_txns()` is true for `HeldColdFiles`. There is no ColdReclaim reason in this tree yet; WS43 must mark its reason too.
  - `request()` answers `TxnOpen` before it touches the gate, so the deferral does not consume the spacing slot.
  - I added no atomic state machine, so no loom model is needed. The only new atomic is a plain per-reason statistics counter, which feeds the new INFO field `cold_held_release_snapshots_deferred_txn`.
- **F2:**
  - `ReplayScope` is entered in `DispatchReplayEngine::replay_command`, which every AOF, manifest and WAL v3 replay goes through.
  - `counts_expiry()` (not replaying, and not applying a master stream) gates `record_expired_keys` itself, so every call site is covered.
- **F3:** the count happens in the hit arm of `Database::set_recording` when the overwritten entry is expired.
  - Hot path cost: one compare on values already loaded, no allocation.
  - Counting at overwrite rather than at hide time is what prevents a double count: the lazy drain re-verifies the key and skips the fresh value.
  - All three dispatch paths reach it; the monoio inline SET uses `Database::set`.
- **F4:** "past" is judged on `db.now_ms()`, which is pinned to the log's time during replay (moon#1277).
- **F5 reset rule:** reset what redis's `resetServerStats` resets among the wave-2a counters (`expired_keys`), plus moon-only monotonic statistics from WS36, WS39 and this fix: `txn_conflicts_refused`, `cold_held_release_folds_requested`, `cold_held_release_snapshots_requested`, `cold_held_release_snapshots_deferred_txn`. Gauges are never reset (`txn_open`, `txn_oldest_age_ms`, `txn_held_keys`, `cold_held_files_stale_databases`), and neither is the gate's spacing slot.
- **F7:**
  - A bare or `cmd|sub` grant under `+@all` is now recorded as `Specific{base_allow:true}`.
  - A category grant there is still a no-op, because moon expands categories (moon#1306).
  - The `unrestricted` fast-path cache treats `+@all` followed only by grants as `+@all`, so permissions and the fast path are unchanged.

## Gates
All ran on the final tree.
- `cargo fmt --check`: 0.
- `cargo clippy --all-targets -- -D warnings`: 0.
- The same clippy with `--no-default-features --features runtime-tokio,jemalloc`: 0.
- `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: 0.
- `cargo test --release --lib`:
  - monoio: 7012 passed, 0 failed.
  - tokio: 6066 passed, 1 failed. The failure is `vector::store::bg_compact_tests::test_bg_compact_pool_parallelism`, a wall-clock parallelism assertion that tripped under load. It passed 3/3 when rerun, and my changes do not touch vector code.
- Integration, on both `r1fixc-fin2` binaries with `--include-ignored`: all pass except the three below.
  - Passing: `held_release_txn_open_1289` 3/3, `expired_keys_parity_1286` 7/7, `acl_rule_order_1296` 4/4, `info_expired_keys_1286` 7/7, `cold_held_files_release_1289` 6/6, `active_expiry_backlog_drain_1288` 1/1, `tracking_expiry_invalidation_1013` 10/10, `acl_subcommand_rules` 2/2, `acl_user_revocation` 6/6, `perf_ws21_snapshot_without_save_rules` 9/9.
  - `review_w1_txn_abort_no_aof_snapshot_1285`: 1/3, the two known WS42-pending failures.
  - `aof_fold_exactly_once_455`: 0/1 ("EXEC returned before WAIT ran out"). It fails identically on `r1-f766fc2-{monoio,tokio}`, so it is pre-existing and not mine. Please triage it.
- For the integration runs, the test harness was built once with default features and `MOON_BIN` pointed at each runtime's binary.

## Measurements
There are no performance measurements. The F3 write-path change is one compare; I ran no A/B.

Oracle scripts and outputs are in `scratchpad/fixc/`:
- `ow.py`: the 37-case overwrite matrix.
- `f4.py`: 40 past-deadline cases.
- `aclrt.py`: 16 ACL round-trip cases.
- `*_redis.txt` / `*_fin2.txt`: redis 7.2.7 and fix output.
- `msg/`: the commit bodies.

## Cross-ownership edits
- `src/transaction/isolation.rs` (WS36): a 4-line `reset_stats()` for RESETSTAT.
- `src/command/connection.rs`: one INFO field, +5 lines. The file was already over the cap.
- `src/command/key.rs`: +19 lines. Already over the cap; the new tests are in `expire_past_tests.rs`.

## Risks / re-check at integration
- **F1 starvation:** a no-AOF server that always has some TXN open defers the held-file snapshot indefinitely. Held files then stay on disk and SWAPDB stays refused for those databases. This is visible in `cold_held_release_snapshots_deferred_txn`. There is no timeout, per the maintainer's no-idle-timeout decision.
- **F1 race:** a TXN that begins between the check and the shards starting their part of the snapshot can still be captured. The window is sub-tick. WS42 closes it.
- **F4 on replicas:**
  - Redis's `checkAlreadyExpired` skips the immediate delete while loading and on replicas. Moon deletes during replay and on the replica too. The final state is the same, since moon propagates absolute forms verbatim and not as DEL.
  - Under master/replica clock skew of δ, a deadline issued within δ of the master's "now" can leave a TTL'd key on a replica whose clock is behind, and no DEL follows. This is rare.
- **F4 GETEX:**
  - Moon still looks the key up before parsing options. Redis parses first, so `GETEX missing EX -1` answers nil in moon and an error in redis. This is pre-existing.
  - Relative EX/PX overflow is now an error, as in redis. It used to saturate.
- **WS27 TXN abort compensation:** `RESTORE … ABSTTL REPLACE` with a pre-image deadline already past now deletes the key instead of writing an expired entry. The key ends up absent either way.
- **F3:** tests that assert an absolute `expired_keys` value will see more counts now. Every SET over an expired key counts.
- **F7:**
  - `+@all +get` users become `Specific{base_allow:true}` internally. Any code that matches `AllAllowed` for meaning rather than going through `unrestricted()` or `permits()` would see them differently. I found none.
  - ACL files re-save with the recorded grants.

## CHANGELOG adjustments (ready to paste)
- **moon#1286** (extend the WS38 bullet):
  - `expired_keys` now also counts a write that lands on an expired key (SET, SETNX, GETSET, APPEND, INCR*, SETBIT, PFADD, MSET, a COPY/RENAME/STORE destination, and a key a read had hidden). It no longer re-counts the AOF's logged reaps on every restart; redis counts nothing while loading.
  - An absolute deadline already in the past (`EXPIREAT`/`PEXPIREAT`, `GETEX … EXAT/PXAT`, `RESTORE … ABSTTL`) now deletes the key at once and publishes `del`, as redis does. It is no longer counted as expired. `EXPIRE k -1` now publishes `del`, and `GETEX k EX -1` answers `ERR invalid expire time in 'getex' command`.
  - `CONFIG RESETSTAT` resets `expired_keys` and `txn_conflicts_refused`, and the held-file request counters.
- **moon#1296** (extend the WS38 bullet): a command grant applied under `+@all` (`+@all +get -set`) is kept, as in redis 7.2. ACL SAVE / ACL LOAD now reproduces the rules exactly. Before, the grant disappeared on reload.
- **moon#1289** (extend the WS39 bullet):
  - Without an AOF, the automatic snapshot that releases held cold files runs **even with `save ""`** and overwrites the dump file like any `BGSAVE`.
  - It waits while any `TXN` is open, so it never captures uncommitted writes. New INFO field: `cold_held_release_snapshots_deferred_txn`.
  - A failed fold dispatch is no longer counted as a held-release fold.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9

- **Completeness:** the gaps are the F10 red proof, which is structural only, and the out-of-scope `expire`/`restore` keyspace events.
- **Edge cases:** the F1 residual race and the starvation risk are documented, not solved; per the brief, WS42 owns them.
