# WS43-block-reclaim SUMMARY (ported onto WS42)

Branch `w2/ws43-port2`, base `cbcb9ee` (WS42 head), HEAD `88641f6`. Results are from the Linux container, not the merge bar.

**Commits (12):**
- **WS43's 7 commits**, cherry-picked from the originals with their messages unchanged. They applied with no conflicts, because WS42's `snapshot_request.rs` is back to WS43's original shape. They compiled with no resolution needed:
  - `d4dcdec` feat
  - `3a04797` test
  - `e397305` docs
  - `0c2c385` test
  - `d4243d5` fix
  - `679e7b2` test
  - `78d9323` test
- **Integration commits (5):**
  - `ae7da85` test/docs: a TXN open across the reclaim.
  - `6faa864` test: R3-fix-e pin.
  - `b7e387c` fix: RESETSTAT.
  - `1f3b6a9` and `88641f6`: two test-harness fixes, both found by the red runs.

**Binaries:**
- **Final:** `/home/user/wt/bin/ws43p2-fin-{monoio,tokio}`.
  - Built at `b7e387c`; the two later commits are test-only.
  - Markers: `cold_reclaim_snapshots_requested` (WS43) and `MOON.TXN` (WS42).
- **Red:** `ws43p-port-{monoio,tokio}`. This is wave-2a plus WS43 as built, without WS42; it has no `MOON.TXN`.
- **Mutant (never ship):** `ws43p2-mut-monoio`.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1297 no-AOF block reclaim, port | FIXED (throughput A/B: READY TO BENCH) | `d4dcdec`..`78d9323` | Suite `cold_block_reclaim_no_aof_1297`, 20 tests, green in all four quadrants; details below | Throughput A/B on a quiet box |
| Point 1 (new meaning): a ColdReclaim snapshot taken while a TXN holds keys stores the pre-TXN image | DONE (verification; no product change needed) | `ae7da85`, `1f3b6a9` | 3 automatic-round tests; details below | — |
| Point 2 (new meaning): kill -9 at every WS43 kill point with a TXN open | DONE | `ae7da85`, `88641f6` | 7 `kill_*_with_a_txn_open` tests; details below | — |
| Point 3: RESETSTAT rule | FIXED | `b7e387c` | details below | — |
| Point 4: WS43 suite re-run + mutant | DONE | — | details below | — |
| R3-fix-e `ColdIndex::remove` early return vs. compaction state | VERIFIED, pinned | `6faa864` | details below | — |

### Point 1: the automatic ColdReclaim round with a TXN open
- **Setup.** A TXN on `{t}k`'s shard writes `{t}k`, `{t}new` and 16 live fillers on that shard (SET one, DEL the next). The fillers are mostly cold survivors in the files being compacted. Every shard is held at its snapshot start (`MOON_TEST_SNAPSHOT_START_HOLD_FILE`) until the TXN has written. The sweep then requests the round, which publishes while the TXN is still open.
- **Three endings:**
  - stop at `adopt_ready`;
  - kill -9 after the adoption;
  - `SHUTDOWN` (default save) with the TXN open.
- **Green on port2, all quadrants:**
  - the restart reads `k=original`, `new=nil`;
  - all 16 TXN-written survivors are at their pre-TXN value;
  - 0 resurrected, 0 lost;
  - `cold_reclaim_snapshots_requested` 0→1, `cold_held_release_snapshots_requested` 0.
- **Red on `ws43p-port`, both runtimes, 3/3:**
  - `k=aborted new=inserted`;
  - 8 TXN-DEL'd survivors lost, 8 with the TXN's value.
- **A race the red run caught (fixed in `1f3b6a9`).** The round could commit during the 2 s compaction-stability wait, before the TXN opened. `Adopted` then passed even on the red binary, and `AdoptReady` stopped the server before the TXN could open. The start-hold closes that window.

### Point 2: every kill point with a TXN open
- **Setup.** `run_case` gains a `txn` flag. The TXN opens after the compactions are recorded (before the release at the `compacted` point) and stays open through the kill.
- **Green, all quadrants:** 0 resurrected, 0 lost, 0 wrong values, 0 trailer faults at all 7 points; `{t}k=original`, `{t}new=nil`.
- **Red on `ws43p-port` s4, both runtimes:** `adopt_ready`, `listed`, `unlinked` and the after-next-snapshot point each show 8 lost + 8 wrong values. The three points before the adopting snapshot commits are green there too, since S1 is still the authority.
- **Harness fix (`88641f6`).** `{t}k` itself gets spilled and compacted, then promoted by the TXN's SET. Its compacted slot is then rightly graved, so the trailer-exactness check now exempts it; the restart oracle still checks its value.

### Point 3: RESETSTAT
- **Reset:** `cold_reclaim_compactions`, `_files_unlinked`, `_bytes_unlinked` (new `cold_reclaim::reset_stats`). `_snapshots_requested` was already reset through `snapshot_request::reset_stats`.
- **Kept:** the `cold_reclaim_compactions_pending` gauge (`AWAITING_FOLD`). Every adoption and every dropped index subtracts from it, so zeroing it would wrap it, and the AOF monitor would then dispatch folds for compactions that don't exist.
- **Tests:**
  - Unit test: the million it adds is gone after a reset, and the gauge still counts its pending compaction.
  - Real-server test: green on port2, all quadrants.
  - Red on a binary without the reset: 28 / 28 / 348160 after RESETSTAT.

### Point 4: WS43 suite re-run and mutant
- **Kill suite and disk test:** reproduced in all four quadrants on port2 (numbers under Measurements).
- **Mutant, monoio s4.** It drops both WS42's pre-image capture and WS43's compacted graves:
  - all 3 round tests go red;
  - every committed kill point goes red, with and without a TXN: 749–838 resurrected, plus the TXN's writes kept;
  - the `snapshot_start` hook never fires;
  - only `compacted` and `during snapshot` stay green.

### R3-fix-e `ColdIndex::remove` early return
WS43 adds no per-key state that `remove` must update. "Changed survivor" is judged when the trailer is encoded (`lookup != from`), so the early return needs no extra condition. Pinned by `removes_on_an_emptied_index_leave_a_pending_compactions_graves_exact`, which is green before and after.

## Measurements (method, reps, raw numbers)

**Kill suite on port2** (`cold_block_reclaim_no_aof_1297`, 20 tests, `--include-ignored`, `--test-threads 1`, `MOON_BIN` pinned): 20/20 in each quadrant.
- monoio s4: 357 s. tokio s4: 347 s. monoio s1: 347 s. tokio s1: 346 s.
- The loss self-check (compacted files removed after `unlinked`) reports, as it should:
  - monoio s4: 281 lost + 79 probes.
  - tokio s4: 156 lost + 33 probes.
  - monoio s1: 851 lost + 161 probes.
  - tokio s1: 849 lost + 176 probes.

**Disk held** (6 rounds × 16K SETs, 600 B incompressible values, 3 of 4 deleted per round, 8 MB maxmemory, 1 s sweep):

| quadrant | spill files after settling | ratio to live cold value bytes | dead slots |
|---|---|---|---|
| monoio s4 | 16.9 MB | 1.29x | 2,399 |
| tokio s4 | 17.0 MB | 1.29x | 2,419 |
| monoio s1 | 15.9 MB | 1.22x | 1,529 |
| tokio s1 | 16.4 MB | 1.26x | 2,111 |

WS43's original runs measured 1.25–1.31x, against 2.55–2.67x on the ws39 base. I did not re-measure the base.

**Throughput: READY TO BENCH.** Not run, because other lanes were building. Command: `ab.sh <shards> 3 <base> ws43p2-fin-monoio` with `-t set -r 50000 -d 600 -n 100000 -c 16 -P 16 --save "3600 100000000"`, shards 1 and 4.

## Gates (Linux container, not the merge bar), on `w2/ws43-port2`
| gate | result |
|---|---|
| `cargo fmt --check` | 0 |
| clippy `--all-targets -D warnings`, monoio | 0 |
| same, tokio | 0 |
| `cargo check --manifest-path fuzz/Cargo.toml --all-targets` | 0 |
| `cargo test --release --lib -- storage::tiered persistence shard`, monoio | 0 (1742 passed) |
| same, tokio | 1713 passed, 1 failed (below) |

**The tokio lib failure** is `storage::tiered::unlink_hold::tests::admitting_a_large_batch_is_not_quadratic`, a wall-clock ratio test (8.1x against an 8x bound) that ran while integration servers were up. Re-run alone it passed 6 of 8 times; both failures were the first run after a build, at 8.5x and 9.4x. `unlink_hold.rs` is identical to the base (0-line diff from `cbcb9ee`); it was last changed in `06154be`. My new unit tests appear by name in both lib logs.

**Integration, both runtimes, with `fin` binaries, `--include-ignored`, `MOON_DISK_FREE_MIN_PCT=0` (tokio also with `MOON_TEST_NO_MASTER_PSYNC=1`): all pass.**
- `cold_block_reclaim_no_aof_1297`: 20 (four quadrants, above)
- `held_release_txn_race_1289`: 5
- `held_release_txn_open_1289`: 3
- `cold_held_files_release_1289`: 6
- `crash_recovery_cold_no_aof`: 10
- `crash_recovery_disk_offload_no_aof`: 1
- `cold_graves_reduced_databases_1291`: 1
- `tiering_no_aof_write_gate_1290`: 2
- `crash_matrix_cold_graves_1281`: 1
- `review_r2a_cold_graves`: 3
- `review_ws22_cold_graves`: 2
- `perf_ws21_snapshot_without_save_rules`: 9
- `cold_tier_observability`: 2
- `crash_recovery_cold_del_rewrite`: 21
- `cold_file_id_orphan_sweep_1114`: 1
- `txn_crash_atomicity_1300`: 38 (WS42's suite)

**Not run:** the hosted Windows / MSRV matrix.

## Cross-ownership edits
**WS43's original edits, unchanged:**
- `src/shard/timers.rs`: +4 lines (now 1474 lines).
- `src/shard/held_release_tick.rs`: `request_reclaim_snapshot`.
- `src/command/connection.rs`: +20 lines, 5 INFO fields.
- `src/persistence/snapshot_request.rs`: the `ColdReclaim` reason.
- `src/storage/tiered/slot_graves.rs`: `files()`.
- `docs/STORAGE-FORMAT-V1.md` §3.2.

**Mine:**
- `src/command/config.rs`: one RESETSTAT call plus its doc.
- `docs/guides/persistence.md`: one paragraph on the reclaim's automatic snapshot, including that it runs with TXNs open.

Every touched file is under 1500 lines (the test suite is 1270).

## Risks / things the orchestrator must re-check at integration
- **The script leg is untested.** The `no_aof` doc claims the pre-image only for a connection's TXN write, which reads (and so promotes) the key first. Script writes take their holds from the bridge's undo records after the script runs; I did not verify those for cold survivors.
- **A possible WS42 cold-key gap, unverified.** Suppose a key a TXN holds is spilled again while held, carrying the TXN's value into a new file G. The image then holds the pre-TXN value, but the cold index also has the key in G. A restart reads the hot image, as my tests show. A later DEL followed by a GET could expose that stale cold copy. I did not test this; it looks like the "cold keys" residual in WS42's report rather than something WS43 introduces.
- **Throughput not measured** (READY TO BENCH). The candidate mitigation, if the A/B shows a cost, is to pause no-AOF compaction starts on a tick that evicted.
- **Stale CHANGELOG/SUMMARY text.** The superseded `w2/ws43-port` added INFO fields `cold_reclaim_snapshots_{deferred,abandoned}_txn`. They do not exist on port2, so no CHANGELOG text should mention them.
- **Housekeeping.** I removed only my own `r1297` test directories, by exact path, and stopped my own leftover servers after killing the superseded gate run. Nothing is left running.

## CHANGELOG bullet (ready to paste)
- **feat(tiered):** without an AOF, mostly-dead cold spill files are now reclaimed (moon#1297).
  - When at least a third of a file's slots are dead, its live keys are compacted into a new file. The new file is adopted, and the old one unlinked, once a snapshot that started after the compaction has committed.
  - Every snapshot's cold-graves trailer carries the compacted copies of keys that changed in the meantime, so a kill -9 at any point resurrects and loses nothing. This also holds with a `TXN` open: the snapshot stores the keys it holds, including cold ones it wrote, at their pre-transaction values (moon#1300).
  - When compactions have waited three orphan sweeps with no snapshot committing, one is requested under the moon#1289 spacing, even with `save ""`.
  - In a no-AOF flood with DEL churn, spill-file bytes fell from about 2.6x to 1.2–1.3x the live cold value bytes.
  - New INFO fields: `cold_reclaim_compactions`, `cold_reclaim_compactions_pending`, `cold_reclaim_files_unlinked`, `cold_reclaim_bytes_unlinked`, `cold_reclaim_snapshots_requested`. `CONFIG RESETSTAT` resets all of them except the `cold_reclaim_compactions_pending` gauge.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.92 · Optimization 0.85 · Edge cases 0.9 · Self-evaluation 0.9

- **Optimization** is below 0.9 only because the throughput A/B needs a quiet window; there is no known code gap.
- **Edge cases:** the red runs caught two harness flaws (the round race, and `{t}k`'s spurious trailer fault), both fixed and re-proven red→green. The script-leg and re-spill gaps are listed as risks, not claimed.
