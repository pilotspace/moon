# WS15-durability-followups SUMMARY

- Branch `perf/ws15-durability-followups`, base `ae21476` (+ `6537679`). NOTES.md in this directory has the design reasoning.
- Binaries (release-fast):
  - `/home/user/wt/bin/ws15-monoio-af103a2` and `/home/user/wt/bin/ws15-tokio-af103a2`. Both allowed release builds were used on these. Provenance was checked with branch-only strings and the startup line: monoio prints `(1 shards, monoio)`, tokio prints `(1 shards)`.
  - `/home/user/wt/bin/ws15-debug-monoio-1c36c11` is a debug build of the final code commit.
  - `/home/user/wt/bin/baseline-ae21476` is the reference for every red run.

(SUMMARY.md committed by the orchestrator. The harness refused the subagent's write, and TEAM-RULES §6 applies. The content is the agent's final report.)

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1223 (P1): a spill withdrawn after a fold leaves the key in no durable artifact | **FIXED** | `140049e` fix, `9867eeb` test | See "moon#1223 evidence" below. | The same class of bug remains in the barrier-after-flip window (Risk 5). The BGSAVE `SnapshotState` also omits in-flight payloads (WS16 / moon#1228). |
| moon#1230: `rdb_last_bgsave_status` stays `err` forever | **FIXED** | `aa36b7e` fix, `af103a2` WS12 test, `7584583` test | See "moon#1230 evidence" below. | Sharded `--save` never fires (Risk 2). A BGSAVE with no persistence dir should write to `--dir`, as redis does; that needs main.rs / event_loop plumbing outside this workstream. It now fails loudly instead of hanging. |
| moon#1215 (P0): a deleted cold key resurrects after a rewrite and any restart | **FIXED** | `ecc4ee1` cherry-picked proof tests, `529d606` harness, `7767956` fix, `38fc290` shard-count param, `1c36c11` perf | See "moon#1215 evidence" below. | The one-time first-upgrade capture in main.rs writes COLDCUT without the DELs. The ledger's RAM was unbounded and not charged to the write gate (PR #1233 review — fixed, see "PR #1233 review fixes"). |

### moon#1223 evidence
- **Fix.** The fold base image now includes keys whose spill is still in flight (the issue's "heavier" option; the argument is in NOTES). It covers all three ways a spill can fail to publish: the marker is refused, the pwrite fails, or the file id is re-issued.
- **In-process.** `shard::persistence_tick::fold_inflight_tests` adopts the review's `rv_integ_withdrawn_spill_after_fold_has_a_durable_source`. It adds three cases: pwrite failed, id rejected, and a restart through production recovery.
  - With only the fix reverted, which is identical to ae21476's `stream_fold_image`, all three paths are red: `live=Some("acked-value") in_base=false manifest_lists_file=false in_post_fold_log=false`.
  - All are green after the fix, on both runtimes.
- **Real server** (`tests/perf_ws15_spill_withdraw_after_fold.rs`) — a smoke test, NOT red/green proof (corrected in the PR #1233 review: it passed on ae21476 in 4 of 5 runs).
  - One run on ae21476, `--shards 1` with kill -9, was red: "1067 of 63049 keys acknowledged before BGREWRITEAOF are missing or wrong" (46 withdrawals). The other three variants do not reproduce on base.
  - The fixed binaries pass 4/4 on monoio and 4/4 on tokio.
  - The deterministic guard is `fold_inflight_tests` above (red on ae21476 in 4 of 4 runs).

### moon#1230 evidence
- **Fix.** A per-save failure latch. The status is `ok` only if every shard succeeded. LASTSAVE and the dirty counter move only on success. `SAVE_IN_PROGRESS` is cleared last.
- **Sibling fixes.**
  - A shard that skips a save now reports a failure. That happens with no persistence dir (the default without `--save`) or with a lost data dir. Before, it left `rdb_bgsave_in_progress:1` forever.
  - A sharded auto-save now starts as a counted save.
- **Unit tests:**
  - `bgsave_status_is_per_save_and_lastsave_moves_only_on_success`
  - `a_refused_second_bgsave_does_not_clear_the_running_saves_failure`
  - `a_sharded_auto_save_is_a_counted_save`
- **Real server** (`tests/perf_ws15_bgsave_status.rs`). On ae21476 every case is red at both shard counts:
  - "a failed save advanced LASTSAVE"
  - "a failed save reset rdb_changes_since_last_save (600000 -> 0)"
  - "a clean BGSAVE after a failed one reports rdb_last_bgsave_status:err"
  - "SHUTDOWN SAVE after a successful save was refused: -ERR SHUTDOWN failed: background save error, check logs"
  - "the BGSAVE never finished (rdb_bgsave_in_progress stuck at 1)"

  After the fix: 3/3 on monoio and on tokio, and `perf_ws12_bgsave_split` passes 4/4 on both.
- **Correction.** `aa36b7e` wrongly blamed the base's auto-save failure on the accounting; the real cause is the trigger (Risk 2). `7584583` corrects this by replacing that server case with a unit test.

### moon#1215 evidence
- **Fix.** A dead-slot ledger in the cold index records every slot still on disk that is no longer its key's index entry. The sources are deletes, re-spills, FLUSH, both sweeps, released older copies, and the ghost slots of published files. Each new AOF generation now opens with plain `SELECT`/`DEL` records for the keys that are dead at the fold instant. This covers all four fold writers. The format does not change: every moon version replays `DEL`.
- **Recovery-level tests** (`storage::tiered::cold_del_rewrite_tests`, 12 tests, no longer ignored, built with the production fold and recovery).
  - The originally ignored case is red on ae21476: `k1 was DELeted before the rewrite; it must not come back from file 5 left: Some([118, 49]) right: None`.
  - With only the head DELs disabled, six cases go red (`left: Some("v1")`).
  - All are green after the fix, on both runtimes.
- **Real server** (`tests/crash_recovery_cold_del_rewrite.rs`, 8 cases, still `#[ignore]` per house convention). The cherry-picked harness first had to be fixed in `529d606`: on Linux it lost half its filler and never spilled.

  Deleted keys that came back on ae21476:

  | case | `--shards 4` | `--shards 1` |
  |---|---|---|
  | DEL + rewrite + kill-9 | 86/100 | 87 |
  | DEL + rewrite + clean SHUTDOWN | 72/100 | 62 |
  | DEL after orphan sweeps | 83/100 | 44 |
  | FLUSHDB + rewrite | 157/200 | 134 |

  - The control, overwrite, TTL and flush-after-sweep cases pass on ae21476.
  - The fixed binaries pass 8/8 at both `--shards 4` and `--shards 1` on monoio, 8/8 at both on tokio, and 8/8 on the debug build of the final commit.
- **Downgrade.** After a rewrite by the fixed binary, the base binary boots the same data with 0/100 deleted keys back and 0/100 live neighbours lost — until the base binary runs its own rewrite (no ledger, no DELs): then 37/100 came back after a boot (PR #1233 review). Safe only until the first rewrite on the older binary.
- **Other tests:** the ledger (9), ghost slots (2), fold selection, flat-file wiring, and head dedupe/batching.

## Measurements
**#1215 ledger cost.** Interleaved A/B, 3 reps, fresh server per run.
- Setup: monoio release-fast, `--shards 1 --maxmemory 16mb`. Write 300,000 keys of 200 B, then DEL 294,000 of them, keeping every 50th so their files stay listed. Then run BGREWRITEAOF with a PING every 0.5 ms.

| run | binary | used_memory | max PING | p99 PING | head DELs | incr bytes |
|---|---|---|---|---|---|---|
| A1 | ae21476 | 578,303 | 4.04 ms | 2.15 ms | 0 | 35 |
| B1 | af103a2 | 15,732,016 | 251.72 ms | 1.79 ms | 559 | 5,729,580 |
| A2 | ae21476 | 417,485 | 5.31 ms | 0.65 ms | 0 | 35 |
| B2 | af103a2 | 16,060,629 | 254.37 ms | 3.07 ms | 571 | 5,850,380 |
| A3 | ae21476 | 575,806 | 3.15 ms | 0.32 ms | 0 | 35 |
| B3 | af103a2 | 16,095,572 | 252.40 ms | 2.02 ms | 572 | 5,863,195 |

- That is about 54 B of RAM and 0.85 µs of fold stall per dead slot. The base's zero cost is the bug itself: those keys came back.
- `1c36c11` moved the dedupe off the shard thread. On the same 294K-slot ledger, the selection pass dropped from 916–979 ms to 325–413 ms (2.6–2.9×). That was measured on a debug build. Release was not re-measured because a third release build is not allowed.

**Promote-then-sweep reproduction on base.** Monoio `--shards 1`, orphan sweep every 1 s: 88 of 200 acknowledged, never-deleted keys were lost.

## Cross-ownership edits
- `tests/perf_ws12_bgsave_split.rs` (WS12's file), in the isolated commit `af103a2`. It now asserts that the post-abort BGSAVE reports `ok` instead of scanning the log, as the plan asked. The immediate retry, the abort log check and the restart check are unchanged.

## Risks / things the orchestrator must re-check at integration
1. **New finding (pre-existing, not fixed): promote-then-sweep loss.** Reproduced 88/200 on base.
   - A key that is cold when a rewrite cuts its base is not in the base.
   - If it is later only read back into memory (or read-modify-written), it leaves the cold index without a log record.
   - Once the rest of its file is gone, the orphan sweep deletes the file, and after a restart the key is gone.

   The fix needs a "fold committed" signal (`shard/timers.rs`) and a promotion hook (`storage/db/kv_ops.rs`). An issue needs filing.
2. **New finding: sharded `--save` never fires.** Its change counter is incremented only by `handler_single`. Making it fire turns on periodic snapshots for every `--save` user, which is a behaviour decision. An issue needs filing. Also, SHUTDOWN SAVE during a running auto-save is now refused as "already in progress", as it already is during a user BGSAVE.
3. **Ledger cost.** About 54 B RAM per dead slot, plus the fold stall above. It was counted in `used_memory` but NOT in the eviction budget (corrected: the PR #1233 review measured 5.0x maxmemory with zero writes refused). A workload that deletes most cold keys but keeps a few per file held nearly the whole deleted set until those files were reclaimed. Fixed in the PR #1233 review (admission charge + reclaim of mostly-dead files).
4. **`ColdIndex::resident_bytes()` now includes the ledger.** Tests that expect cold-index bytes to return to 0 after removals must subtract it (updated: 3 in cold_index.rs, 1 in kv_spill.rs). `ColdIndex::remove` now records the slot, which matters for WS10.
5. **Barrier-after-flip window (not fixed).** A crash between committing a new AOF generation and the async persist of a spill's manifest entry can lose a key that was cold at the fold. Moving the barrier before the commit makes it process-wide, so it would cross-abort rewrites in parallel tests (the moon#750 class). It needs per-manifest scoping first.
6. **Fold API change.** `write_fold_image` and `write_fold_image_file` return `(u64, ColdDeletes)`, and `FoldChunk` gains a `ColdDeletes` variant. `stream_fold_image` (WS8's call site) is unchanged.
7. **Line limit.** Files already over 1500 lines grew: `cold_index.rs` 1922→1985, `rewrite.rs` 1925→1964, `persistence_tick.rs` 3294→3332. New logic went into new modules.
8. **Lib tests.** Monoio: 3529 passed; tokio: 3341 passed (filters `storage:: persistence:: shard:: command::`). The only failure is the known root-container env test.

## Gates at the final code commit (1c36c11)
- `cargo fmt --check` is clean.
- audit-unsafe and audit-unwrap pass (0 new unsafe).
- `clippy --all-targets` (monoio), clippy on tokio, and tokio `check --all-targets` are all clean.
- Lib tests pass on both runtimes, apart from the known env failure.
- Integration suites by name, both runtimes, all green:
  - `crash_recovery_cold_del_rewrite` (`--ignored`, shards 4 and 1)
  - `perf_ws15_spill_withdraw_after_fold`
  - `perf_ws15_bgsave_status`
  - `perf_ws12_bgsave_split`

## CHANGELOG bullets
- **Fixed (P1, data loss):** a key could be lost on the next restart when:
  - its spill was in flight when a BGREWRITEAOF fold cut its base, and
  - the spill then did not publish (the marker was refused under AOF backpressure, the pwrite failed, or the file id was re-issued).

  The key was in no durable artifact. The fold's base image now includes in-flight spill payloads. (moon#1223)
- **Fixed (P0, data resurrection):** under the default config, a deleted key came back after an AOF rewrite and any restart when its spill file still held live neighbours. This applied to DEL, UNLINK and FLUSHDB, and to an overwrite of a cold key whose new TTL had passed BEFORE the rewrite. (A cold key overwritten with `SET k v PX ttl` still comes back with its old value after a restart once that TTL passes, with or without a rewrite — a separate, pre-existing issue.)
  - The cold index now remembers every dead slot still on disk that can come back. Every rewrite opens its new generation with plain `DEL`s for the keys dead at the fold instant.
  - Every moon version replays those records, so a downgrade keeps the deletes until the older binary runs a rewrite of its own.
  - Cost: RAM per dead slot until its file is reclaimed, charged at write admission and bounded by reclaim (see the PR #1233 review fixes), plus a fold pass over them. (moon#1215)
- **Fixed:** `rdb_last_bgsave_status` now reports the last save and returns to `ok` after a successful one.
  - Before, one failed sharded BGSAVE latched `err` forever, and every later `SHUTDOWN SAVE` was refused.
  - `LASTSAVE`/`rdb_last_save_time` and `rdb_changes_since_last_save` now move only on a successful save.
  - A shard that cannot write a snapshot (no persistence dir, or a lost data dir) now fails the save instead of leaving `rdb_bgsave_in_progress:1` forever.
  - Sharded auto-saves start as counted saves. (moon#1230)

## Self-evaluation (0–1)
Completeness 0.95 · Clarity 0.9 · Practicality 0.9 · Optimization **0.8** · Edge cases 0.9 · Self-evaluation 0.9

- **Why Optimization cannot reach 0.9 here:**
  - The fold's stall and RAM grow with the number of dead slots, because deciding which slots are dead needs key bytes on the shard thread at the fold instant.
  - A live dead-set would add a probe to every hot write, which the hot-path rules forbid.
  - Bounding the ledger needs file compaction, which is a new feature.
  - The one cheap win, moving the dedupe off the shard thread, has landed.
- **Self-evaluation:** reading base runs line by line caught two harness defects: the cherry-picked filler on Linux, and the first #1223 test. One commit over-claimed; `7584583` corrects it.

---

# PR #1233 review fixes (FIX3-ws15)

- Branch `fix3/ws15-ledger`, from the PR head `d4a2fd3` (main `ae21476` + WS15 + WS8 + review fixes). Design reasoning, the verified mechanisms and every crash window: "PR #1233 review fixes" at the end of NOTES.md.
- Binaries (release-fast, both allowed builds used, copied immediately; runtime checked from the startup line: `(1 shards, monoio)` / `(1 shards)`; provenance by branch-only strings):
  `/home/user/wt/bin/fix3-ws15-monoio-082e521`, `/home/user/wt/bin/fix3-ws15-tokio-082e521`.
  Debug, final code (`2c8b465`): `/home/user/wt/bin/fix3-ws15-dbg-monoio-final`, `/home/user/wt/bin/fix3-ws15-dbg-tokio-final` (commit in `fix3-ws15-dbg-final.rev`); every integration gate below ran on these.
  Red binaries for the ledger workload: `/home/user/wt/bin/ws15-{monoio,tokio}-af103a2` (the WS15 branch with the ledger, before these fixes); base `/home/user/wt/bin/baseline-ae21476`.
- The release binaries (`082e521`) contain every code change except the two made last: `5e3be4c` (routed deletes) and `2c8b465` (CodeRabbit fold failure). Every release measurement below concerns code those two do not touch (the ledger bound, reclaim, the SET path); the final debug binaries carry both.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| 1. Record only slots that can come back | **FIXED** | `31013ab` | Dropped classes, each tested: a slot whose own TTL passed (the expiry sweep no longer records; later-expiring entries pruned; the fold skips them — `a_dead_slot_is_pruned_once_its_own_ttl_passes`, `slots_expired_at_the_fold_instant_get_no_del`, recovery-level `a_deleted_cold_key_whose_ttl_passed_needs_no_del_and_stays_dead`); everything in a process with no AOF writer (`no_path_records_without_an_aof_consumer`). Kept classes keep their tests (DEL/promotion, re-spill, FLUSH, shadow sweep, older copies, ghosts). Verified: the rebuild indexes an expired slot but every value read treats it as expired (`a_cold_key_expired_before_a_rewrite_stays_expired`). | A wall clock stepped backwards can revive an "expired" slot (same exposure as an absolute `PEXPIREAT` in the base). |
| 2. maxmemory binds on the ledger (+ reviewer A) | **FIXED** | `6fc8e48`, `06e44ad` (cross-ownership, inline pre-gate) | (i) admission: `evict_to_budget` answers OOM when evictable + ledger > budget; the inline pre-gate compares the same figure (`noeviction_refuses_a_write_once_evictable_plus_ledger_is_over_budget`, `a_small_ledger_is_paid_for_by_eviction_and_the_write_is_admitted`). (ii) eviction never pushes the evictable part below half the budget for ledger bytes (`a_ledger_past_half_the_budget_refuses_writes_without_draining_the_hot_set`), and the pressure cascade ignores the ledger (`the_pressure_cascade_ignores_the_dead_slot_ledger`). O(1) per write: an `Option` check, a field and a relaxed load. | The routed (SPSC) leg refused deletes for memory; fixed in `5e3be4c` (row F). A `redis.call('DEL')` inside a script is still refused over budget (Risk 3). |
| 2b. Head-DEL emission bounded | **FIXED** | `3f71fc2` | Deletes leave the shard as `FoldChunk::ColdDeletes` chunks of <= 512 keys after the image; the writer streams one `DEL` record per chunk into the incr head and drops the chunk; each chunk's handles are charged to its ledger until the rewrite ends (`cold_deletes_stream_in_bounded_chunks_charged_to_the_ledger`). | The writer holds the (charged) chunk handles until the incr is open; a spool file would remove even that (Risk 6). |
| 3. INFO fields | **FIXED** | `ec69cbb` | `INFO` Memory `cold_dead_slots`, `cold_dead_slot_bytes`, process-wide sums maintained per mutation (`info_memory_reports_the_ledger`; read end to end by `perf_ws15_ledger_bound`). | — |
| 4. A real bound (reclaim) | **FIXED** | `9dd2088`, `2669a90` (cadence) | Design (a) made safe: compact into an UNLISTED file, adopt (list, re-point, unlink the old file with its ledger) only after a fold cut after the compaction COMMITTED. Why (a)/(b) as written would recreate moon#1231, and every crash window: NOTES "Item 4". 9 `cold_reclaim_tests` incl. three production-fold + production-recovery crash windows; `the_committed_floor_follows_committed_folds_only`; `reclaim_rewrite_is_due_only_when_compactions_wait`. Real server: see Measurements. | Needs a fold per reclaim cycle (dispatched by the auto-rewrite monitor; none with `auto-aof-rewrite-percentage 0` until a manual BGREWRITEAOF). Legacy multi-shard TopLevel layout not reclaimed. |
| 5. Tests adopted | **DONE** | `dfb0c1b`, `082e521` | `tests/perf_ws15_ledger_bound.rs` (bound + data kept live and after kill -9); `tests/perf_ws15_spanning_cold_del.rs` (db 3, `--shards 4` spanning and `--shards 1`). Both green on both runtimes. Crash suite re-run: 8/8 at shards 4 and 1, both runtimes. | — |
| 6. spill-withdraw test is a smoke test | **DONE** | `252c9a3` | Module doc says so and names `fold_inflight_tests` as the guard. | — |
| 7 + D. Doc corrections | **DONE** | (this commit) | "counted in the eviction budget" corrected in NOTES + SUMMARY; the spill-withdraw test no longer cited as red/green proof; the downgrade claim qualified (safe only until the older binary's own rewrite: 0/100 back on its first boot, 37/100 after its own rewrite + a boot); the "overwrite of a cold key that later expired" claim limited to a TTL that passed BEFORE the fold (the `SET k v PX ttl` over a cold key case is pre-existing and filed separately). | — |
| B. BGSAVE with no persistence dir | **FIXED (minimum)** | `23b51b4`, `20aa7bd` (cross-ownership, main.rs) | BGSAVE and SHUTDOWN SAVE answer `-ERR background save unavailable: …` up front; nothing marked in progress, status stays `ok` (`bgsave_without_a_persistence_dir_is_refused_up_front`, unit + real server at `--shards 1` and 4, both runtimes). SAVE in sharded mode already errors. | Writing to `--dir` as redis does is NOT done: the shards' persistence dir also turns on per-shard WAL writers — a behaviour change beyond this finding. |
| C. BGSAVE fixture hardening | **DONE** | `8d306d1` | Polls `rdb_bgsave_in_progress:1` before FLUSHALL; re-arms the failing save (<= 3 attempts, logged) if it outran the FLUSHALL. Green 3/3 both runtimes. | — |
| E. db-3 proof | **DROPPED (orchestrator)** | — | Covered by the `--shards 1` case of `perf_ws15_spanning_cold_del`. | — |
| F. A delete routed to another shard was refused for memory (found while writing the risks) | **FIXED** | `5e3be4c` (cross-ownership: `shard/spsc_handler.rs`, `shard/spsc_two_db.rs`) | `spsc_eviction_gate` now gives shrink-only commands (DEL, UNLINK, HDEL, LPOP, ...) the connection gate's bypass (eviction runs, the reject is skipped), for maxmemory and the per-db quota. Pre-existing under `noeviction` (base: 144/200 single-key DELs `-OOM` at `--shards 4`); item 2 made it reachable under every policy while a ledger holds a shard over budget. `perf_ws15_ledger_bound::a_delete_routed_to_another_shard_is_never_refused_for_memory`: red on `baseline-ae21476` and `fix3-ws15-monoio-082e521`, green after. | Lua bridge gate unchanged (Risk 3). |
| G. CodeRabbit (Major): an unencodable in-flight payload committed a lossy generation | **FIXED** | `2c8b465` | `for_each_in_flight_base_entry` now returns `AofError::RewriteFailed` (after the counter bump + log) instead of publishing a base without the key; every fold path (per-shard, TopLevel, tokio flat, legacy `do_rewrite_single`) propagates it before anything is published. `an_undecodable_in_flight_payload_fails_the_fold`, `an_unencodable_in_flight_payload_aborts_the_rewrite_on_the_committed_generation` (manifest never flips, memory + disk; no new incr; appends land in the committed incr): both red with only the `return Err` reverted. | Behaviour change: while such a payload is in flight, rewrites fail loudly (retried by the monitor) instead of committing a lossy generation. |

### Red runs (fix reverted, test kept)
- Reviewer's run on the PR head (`d4a2fd3`, INTEG): used_memory 20.7 -> 82.4 MB = 5.0x (tokio 4.9x), RSS 143 MB, zero writes refused; `ae21476` 1.75 MB.
- Same box, same workload (the reviewer's `review3_integ_ledger_growth`, `--ignored`), interleaved: `ws15-monoio-af103a2` 82.6 / 82.9 MB = **4.9x, 0 refused, FAILED**; `ws15-tokio-af103a2` 82.7 MB = **4.9x, FAILED**; this branch monoio 2.68 / 2.25 MB, tokio 6.70 MB (all pass); `baseline-ae21476` 1.75 MB.
- Unit guards, one build with each fix's logic reverted (ledger always on / no prune / expiry sweep records / fold ignores TTL; no ledger at admission; cascade counts the ledger; one unbounded uncharged chunk; adoption ignores the fold; committed floor frozen; no-dir flag ignored): **17 FAILED** — `bgsave_without_a_persistence_dir_is_refused_up_front`, `cold_deletes_stream_in_bounded_chunks_charged_to_the_ledger`, `the_committed_floor_follows_committed_folds_only`, `a_small_ledger_is_paid_for_by_eviction_and_the_write_is_admitted`, `slots_expired_at_the_fold_instant_get_no_del`, `the_pressure_cascade_ignores_the_dead_slot_ledger`, `noeviction_refuses_a_write_once_evictable_plus_ledger_is_over_budget`, `a_ledger_past_half_the_budget_refuses_writes_without_draining_the_hot_set`, `a_deleted_cold_key_whose_ttl_passed_needs_no_del_and_stays_dead`, `adoption_waits_for_a_committed_fold_past_the_compaction`, `a_dead_slot_is_pruned_once_its_own_ttl_passes`, `no_path_records_without_an_aof_consumer`, `keys_live_at_skips_slots_expired_at_that_instant`, `the_shadow_sweep_records_what_it_takes_the_expiry_sweep_does_not`, `merge_keeps_every_entry`, `nothing_is_recorded_without_an_aof_consumer`, `prune_drops_exactly_the_slots_whose_own_ttl_has_passed`. `info_memory_reports_the_ledger` is red on the PR head because the fields do not exist there.
- Row F: `a_delete_routed_to_another_shard_is_never_refused_for_memory` against `baseline-ae21476` and `fix3-ws15-monoio-082e521`: FAILED, `DEL answered "-OOM command not allowed when used memory > 'maxmemory'"`; green on `fix3-ws15-dbg-monoio-3` (`5e3be4c`).
- Row G, `return Err` reverted: `an_undecodable_in_flight_payload_fails_the_fold` FAILED (`... must fail the fold: (35, ColdDeletes { chunks: [] })`), `an_unencodable_in_flight_payload_aborts_the_rewrite_on_the_committed_generation` FAILED (`... must fail, not commit a base without the key: Committed { floor: FoldEpoch(1) }`).

## Measurements

**Ledger bound, real server** (`tests/perf_ws15_ledger_bound.rs`: `--shards 1 --maxmemory 16mb` allkeys-lru, disk offload, AOF on; 4 rounds of 20,000 SETs of ~1 KB keys, then DEL of all but 1 in 50). Release binaries at `082e521`; the debug row is `fix3-ws15-dbg-monoio-2` (same reclaim code).

| binary | right after each round's DELs | 3 s later | settled used_memory | settled ledger (peak) | RSS settled / peak | SETs refused |
|---|---|---|---|---|---|---|
| release monoio | 1.27–1.40x | 0.08–0.15x | 0.15x | 732 slots, 760 KB (20,119 / 20.9 MB) | 61.6 / 68.4 MB | 0 |
| release tokio | 1.28–1.39x | 0.05–0.15x | 0.15x | 678 slots, 704 KB (20,200 / 21.0 MB) | 55.6 / 74.8 MB | 0 |
| debug monoio | 1.44–1.69x | 0.21–0.40x | 0.30x | 3,093 slots, 3.2 MB (20,881 / 21.7 MB) | 77.1 / 84.3 MB | 0 |
| red: PR head `d4a2fd3` (reviewer) | — | — | 5.0x (tokio 4.9x), growing each round | whole deleted set | 143 MB | 0 |

Every acknowledged survivor read back, live and after `kill -9` + restart, and no sampled deleted key came back, in every run. The "right after the DELs" figure exceeds 1.0x because a DEL is never refused and each deleted cold key adds its key bytes to the ledger (~1 KB keys here). While it is over budget, writes answer OOM; reclaim brings it under 0.15x within about 3 s (release).

**The reviewer's own workload** (`review3_integ_ledger_growth`, `--ignored`, same box, interleaved; final used_memory after 4 rounds):

| binary | used_memory | verdict |
|---|---|---|
| `ws15-monoio-af103a2` (ledger, before these fixes) | 82.6 / 82.9 MB = 4.9x, 0 refused | FAILED |
| `ws15-tokio-af103a2` | 82.7 MB = 4.9x | FAILED |
| `fix3-ws15-monoio-082e521` | 2.68 / 2.25 MB (114 SETs refused in one rep, at round 3) | ok |
| `fix3-ws15-tokio-082e521` | 6.70 MB | ok |
| `baseline-ae21476` (no ledger) | 1.75 MB | ok |

**Write-path cost of the admission charge** (the inline pre-gate reads `admission_memory` on every SET). `redis-benchmark -t set -n 2000000 -P 16 -c 50 -r 100000 -d 64`, `--shards 1 --maxmemory 1gb` allkeys-lru, disk offload on, AOF off; 6 interleaved reps per binary, order alternated:

| binary | mean SET rps | range |
|---|---|---|
| A `ws15-monoio-af103a2` | 823,361 | 759,013 – 880,669 |
| B `fix3-ws15-monoio-082e521` | 855,378 | 813,339 – 962,001 |

B is +3.9%, inside the noise: no regression measured. With AOF off the ledger is disabled, but the read is the same (`Option` check, a field, a relaxed load).

**Reclaim latency** (compaction reads and writes spill files on the shard thread, at most 16 files / 16 MiB read per 100 ms tick). The same workload was driven with `redis-cli --pipe` against release binaries, while `redis-cli --latency-history -i 1` sampled PING (one run each; script `scratchpad/reclaim_latency.sh`):

| binary | 1 s windows | windows with a PING >= 10 ms | worst PING | ledger 8 s after the last round's DELs |
|---|---|---|---|---|
| `fix3-ws15-monoio-082e521` (reclaim) | 44 | 14 (12–50 ms) | 50 ms | 576 slots, used_memory 2.3 MB |
| `ws15-monoio-af103a2` (no reclaim) | 45 | 6 (four of them 57–229 ms) | 229 ms | whole set, used_memory 82.9 MB |

The reclaim adds frequent 10–50 ms stalls while it compacts, but it did not cause the worst stall, and the workload stalls without it too. One run each, so this is an order of magnitude, not a benchmark. All 80,000 SETs were accepted on both.

## Cross-ownership edits
Sanctioned by the task: `src/storage/eviction.rs` (the gate charge; `6fc8e48`, with the new `src/storage/eviction/ledger_admission_tests.rs`) and the INFO Memory builder in `src/command/connection.rs` (two fields; `ec69cbb`). Outside WS15's files, each in its own small commit:
- `src/server/conn/blocking.rs` (WS7), `06e44ad`: the inline write pre-gate compares `eviction::admission_memory(db)` (evictable + ledger) instead of `db.estimated_memory()`. One expression plus a comment.
- `src/main.rs`, `20aa7bd`: stores `command::persistence::SNAPSHOT_DIR_ABSENT` once `persistence_dir` is known (6 lines).
- `src/shard/spsc_handler.rs` + `src/shard/spsc_two_db.rs` (WS8), `5e3be4c`: `spsc_eviction_gate` takes the routed command and applies the shrink-only bypass; four call sites pass `cmd`. The `fix3/ws8` branch does not touch these hunks (its `spsc_handler.rs` change is in `wal_append_and_fanout_bytes` and test docs). The same commit adds the test to `tests/perf_ws15_ledger_bound.rs` (WS15's).

## Risks / things the orchestrator must re-check at integration
1. **The ledger can exceed the budget briefly, by design.** A DEL is never refused, and each deleted cold key adds its key bytes to the ledger. Until the next reclaim fold, `used_memory` can exceed maxmemory by the cold key bytes deleted since the last one (measured: 1.27–1.40x release, 1.69x debug, with ~1 KB keys). Meanwhile writes answer OOM. That is the "or OOM" of the item-2 bound, not a hole in it.
2. **Reclaim needs a fold per cycle.** The auto-rewrite monitor dispatches it: once compaction settles, at most one every 3 s, after at most 5 s of deferral. Each such fold rewrites the whole base, so under sustained cold-delete churn on a large instance, rewrite frequency rises. Compaction runs only while a shard's ledger exceeds a quarter of its budget (64 MiB per shard without maxmemory), so it takes about budget/4 of deleted cold-key bytes to trigger one. That was measured only at the 16 MiB scale.
   - With `auto-aof-rewrite-percentage 0`, no reclaim fold runs until a manual BGREWRITEAOF. The ledger is then bounded only by admission: writes are refused while it holds the shard over budget.
3. **Lua scripts:** `scripting/bridge.rs::gate` still refuses `redis.call('DEL', ...)` over budget. This is pre-existing (the gate has no command in scope), and the ledger widens it the same way it widened the routed leg (row F). It needs the command threaded into `gate`, which lives in another workstream's file and was not done here.
4. **Compaction does blocking file I/O on the shard thread**, from the 100 ms eviction tick (`run_eviction_tick`): at most 16 files / 16 MiB read per tick, each one small write with 2 fsyncs. Measured: 14 of 44 one-second windows had a PING of 10–50 ms, against 6 of 45 without reclaim, whose worst was 229 ms ("Reclaim latency"). A spill-thread compaction would remove the stall.
5. **Not reclaimed, bounded by admission only:**
   - the legacy multi-shard TopLevel layout (one fold epoch cannot say which shard's compaction a commit covers);
   - a file with a live slot that does not decode (kept in `skip` for the process lifetime);
   - a process without an AOF, which has no ledger at all (`enable_ledger` is called by every AOF writer pool). A future ledger consumer without an AOF must enable it.
6. **Head DELs in flight:** the writer holds each chunk's key handles until the new incr is open. They are charged to the ledger (`charge_in_transit`) and cleared on the first persistence tick with no rewrite in progress. That flag is process-wide (`AOF_REWRITE_IN_PROGRESS`), so with overlapping shard rewrites the charge can outlive its rewrite. That errs toward refusing writes early. A spool file would remove the holding entirely.
7. **Fold API change** (WS8's AofFold arm and any fold caller):
   - `FoldChunk::ColdDeletes(ColdDeleteChunk)` is sent after the image;
   - `write_fold_image*` returns `(u64, ColdDeletes)` with chunks;
   - `write_generation_head` / `open_new_incr` take `ColdDeletes` by value;
   - `RewriteOverflow` has a `committed_floor()`, raised only by `FoldOutcome::adopt` on Committed;
   - `spsc_eviction_gate` takes the command first.
8. **`ColdIndex` internals are `pub(super)`** (map, file_refs, pending_unlink, older_copies, dead, reclaim) for `cold_reclaim.rs`. A WS10 change to `ColdIndex` must keep that module in step.
9. **Behaviour change (row G):** while an in-flight spill payload that does not rehydrate exists, every rewrite fails (logged, `FOLD_IN_FLIGHT_UNENCODABLE` counted, retried after the monitor's cooldown) instead of committing a generation without the key.
10. **BGSAVE without a persistence directory** gets the minimum fix: a truthful immediate `-ERR`. Writing to `--dir` as Redis does needs a decision, because a persistence dir also turns on per-shard WAL writers.
11. **moon#1231 (promote-then-sweep) is untouched** (WS16). Reclaim unlinks only the files it adopted (`ColdIndex::unlink_now`), never one the committed generation reads. The orphan sweep's own policy is unchanged, and the bound test turns that sweep off for its window because of moon#1231.
12. **Clock:** a wall clock stepped backwards can make an "expired" dead slot readable again after a restart. This is the same exposure as an absolute `PEXPIREAT` in the base.
13. **Line limits:** files already over 1500 lines grew:
    - `cold_index.rs` 1985→2049
    - `rewrite.rs` 1964→2026
    - `persistence_tick.rs` 3332→3379
    - `eviction.rs` 4003→4083
    - `blocking.rs` 3400→3405

    New logic went into new modules: `cold_reclaim.rs` (497), `persistence_tick/cold_reclaim_tick.rs` (129), and the test files.
14. **Shared target:** every build and gate here ran after `touch src/lib.rs src/main.rs`, and binaries were checked for branch-only strings. Rebuild from the integrated tree; do not reuse these binaries.

## Gates at the final code commit (`2c8b465`)
These ran after the container restart, in one script (`scratchpad/fix3_gates.sh`). Each exit code was captured directly, and `git status` was clean for `src/` and `tests/` at the start and at the end. Every build ran after `touch src/lib.rs src/main.rs`. Disk stayed above 6.7 GB free.
- `cargo fmt --check`: clean.
- `bash scripts/audit-unsafe.sh` and `bash scripts/audit-unwrap.sh`: pass (0 new `unsafe`).
- `cargo clippy --all-targets -- -D warnings` (monoio): clean. A first run caught a `useless_vec` in the new test; it was fixed in `2c8b465` before this run.
- `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings`: clean.
- `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc`: clean.
- `cargo test --lib -- storage:: persistence:: shard:: command::`: monoio 3574 passed, tokio 3385 passed. On both runtimes the one failure is `cold_index_rebuild_tests::unreadable_file_is_counted_and_skipped_never_queued_for_unlink`. It is the known root-container environment failure: as uid 0, `chmod 000` does not block the read. The test file is untouched since `14a2642`.
- Integration tests by name, `MOON_BIN` pinned to `fix3-ws15-dbg-{monoio,tokio}-final` (built from `2c8b465`, checked by branch-only strings), `MOON_DISK_FREE_MIN_PCT=0`. All green on both runtimes:
  - `perf_ws15_ledger_bound`: 2/2 (the bound test and the routed-delete test)
  - `perf_ws15_spanning_cold_del`: 2/2
  - `perf_ws15_bgsave_status`: 3/3
  - `perf_ws15_spill_withdraw_after_fold`: 4/4
  - `perf_ws12_bgsave_split`: 4/4
  - `crash_recovery_cold_del_rewrite --ignored`: 8/8 at `MOON_TEST_COLD_DEL_SHARDS=4` and 8/8 at `1`, each runtime.

## CHANGELOG bullets (for the orchestrator's CHANGELOG; they replace the WS15 moon#1215 bullet above)
- **Fixed (P0, data resurrection):** under the default config, a deleted key came back after an AOF rewrite and any restart when its spill file still held live neighbours. This applied to DEL, UNLINK and FLUSHDB, and to an overwrite of a cold key whose new TTL had passed BEFORE the rewrite. (A cold key overwritten with `SET k v PX ttl` still comes back with its old value after a restart once that TTL passes, with or without a rewrite — a separate, pre-existing issue.)
  - The cold index now remembers each deleted slot still on disk that could come back. Every rewrite opens its new generation with plain `DEL`s for the keys dead at the fold instant, streamed in chunks of at most 512 keys.
  - Every moon version replays those records, so a downgrade keeps the deletes until the older binary runs a rewrite of its own.
  - The ledger records nothing for a slot whose own TTL has passed, and nothing in a process without an AOF.
  - It counts toward maxmemory at write admission: a write is refused while evictable memory plus the ledger exceeds the budget, and eviction never drains the live keys below half the budget to pay for it.
  - Its size is reported as `INFO` Memory `cold_dead_slots` / `cold_dead_slot_bytes`.
  - It is bounded by a reclaim: while a shard's ledger exceeds a quarter of its budget (64 MiB per shard without maxmemory), the shard compacts mostly-dead spill files into new ones, lists them after the next committed AOF rewrite (which the auto-rewrite monitor starts, at most every 3 s, unless `auto-aof-rewrite-percentage` is 0), then unlinks the old files and their ledger entries. (moon#1215)
- **Fixed:** a delete (`DEL`, `UNLINK`, `HDEL`, `LPOP`, ...) of a key owned by another shard answered `-OOM` when that shard was over maxmemory. This hit `noeviction` before, and any policy while the moon#1215 ledger holds a shard over budget. Commands that only shrink memory are now admitted on every shard, as they already were on the connection's own shard. (refs moon#1215)
- **Fixed:** an AOF rewrite whose base could not encode an in-flight spill payload published a generation without the key. A restart then lost it if the spill did not publish. The rewrite now fails and the previous generation stays committed; it is retried once the spill completes. (refs moon#1223)
- **Fixed:** `BGSAVE` and `SHUTDOWN SAVE` on a server started without a persistence directory now answer an immediate `-ERR background save unavailable: ...`, instead of reporting a background save that could never write. (moon#1230)
- Unchanged from the WS15 bullets above: the moon#1223 in-flight-spill fix and the moon#1230 `rdb_last_bgsave_status` fix.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.85 · Edge cases 0.9 · Self-evaluation 0.9

- **Completeness:** every assigned item, A–D, and two later findings are FIXED or DONE, each with a test that is red without the fix. Item 4 is a real bound, crash-safe by construction and tested at the recovery level in three crash windows. What remains is listed as risks: the Lua gate, the TopLevel multi-shard layout, and `--dir` for BGSAVE.
- **Why Optimization is 0.85, not 0.9:**
  - Reclaim trades a full AOF rewrite per cycle for its safety. That cost grows with the hot dataset, and I measured it only at 16 MiB.
  - Compaction also does blocking I/O on the shard thread (bounded per 100 ms tick; see "Reclaim latency").
  - Doing better needs an incremental fold, or a spill-thread compaction with a completion message. Either is a larger change than this review round.
  - The admission charge itself costs nothing measurable (SET A/B +3.9%, inside the noise).
- **Self-evaluation:** two things I would have claimed wrongly without checking.
  - The first reclaim cadence "passed" but refused every write of a round. The per-round output showed it; the pass/fail line did not (`2669a90`).
  - Writing the risks turned up that the routed leg refused deletes (row F). I measured it on the base before calling it pre-existing.
