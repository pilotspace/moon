# Adversarial review, round 2: moon#1269 (0aa15c5), moon#1281 round-1 fixes (03bf3c1, 5d0713b, 33be29c, ad55d3d), moon#1279 (bb6909c)

Reviewer lens: storage-durability-engineer and ci-test-integrity-engineer.

Environment: Linux container (4 vCPU, shared). **This is not the merge bar**: there is no moon-dev VM and no hosted matrix.

Binaries were built from ad55d3d in this worktree:
- `/home/user/wt/bin/r2a-monoio` and `r2a-tokio` (release-fast);
- baselines: `/home/user/wt/bin/base-{monoio,tokio}` (273e6bc).

Review tests are on branch `review/round2a`:
- `a39dca2`: unit tests, #1269;
- `feb1b8b`: black-box tests, #1281.

## Verdict

**No BLOCKING finding in the reviewed commits. No regression against 273e6bc.** I found no path that loses a live key and no drift in `used_memory`.

- **#1269 is sound.** Every DEL and UNLINK funnels through one dispatch point. The epoch slot cannot go stale. The stored pre-image size equals what the old code recomputed.
- **bb6909c is sound.**
- **The round-1 fixes close what they claim at the first boot.** However, F1's fix is incomplete one boot later (R2-1, MAJOR, red on every quadrant but one).
- A pre-existing hot-key loss sits next to it (R2-2).

| # | Sev | Finding | Repro, red at ad55d3d |
|---|---|---|---|
| R2-1 | MAJOR | Round-1 F1 is closed only at the first `--appendonly yes` boot. The seeded ledger reaches disk only through an AOF rewrite fold, and an AOF process's snapshot carries no trailer. So the next `yes` boot brings back every cold key deleted in the no-AOF era, unless a BGREWRITEAOF ran in between. | `tests/review_r2a_cold_graves.rs`: `r2a_second_aof_boot_keeps_cold_dels` and `r2a_second_aof_boot_after_bgsave_keeps_cold_dels` (table below) |
| R2-2 | MAJOR (pre-existing, adjacent) | At `--shards >1`, a no-AOF snapshot booted with `--appendonly yes` serves its hot keys (boot A). `initialize_multi` then creates an **empty** base, so boot B, where the AOF is the authority, drops every hot key the snapshot held. | same tests: `hot:control = None` at boot B, s4, both runtimes, on base **and** ad55d3d |
| R2-3 | MINOR (test integrity) | Round-1 F5 is only half closed. `WholeFilesSweep` never kills inside the sweep: heap files went N → **0 before every kill** (16/16 iterations). It also asserts neither that anything was unlinked nor that deleted fillers stay deleted. | matrix log below |
| R2-4 | NIT | #1279: the new boot-view test calls `observe_boot_fold_view` directly. It discriminates at function level, but still passes if the `event_loop.rs:843` call is deleted (round-1 F4, narrowed). | trace |
| R2-5 | NIT (perf) | #1269: a held UNLINK walks the whole value on the shard thread (`entry_overhead_len` → `estimate_memory`) to credit it. That walk is the remaining 65–83 ms of the 5M-field measurement. The lazy path never walked it. | trace + commit's own numbers |
| R2-6 | NIT | `REMOVAL_SLOT` is redundant on every production path. `db_plane::swap` keeps each slot's `db_index`, so the fallback equals the hook's slot. It is harmless defence; the doc could say so. | trace |
| R2-7 | NIT (observation, round-1 N6 persists) | With disk offload and no AOF, `allkeys-lru` still plain-drops 3,117–5,384 keys in every re-spill guard run. | ws22 guard logs |

## Round-1 findings: are they closed?

Unit tests: `cargo test --lib -- review_ws22 review_r2a removal_move_tests multi_key_cow table_swap cold_graves slot_graves superseded_spill promote_sweep` gave **76 passed, 0 failed, 2 ignored** (the two measurement probes).

| R1 | Status at ad55d3d | Evidence |
|---|---|---|
| F1 | **Closed at the first boot; open at the second** (R2-1) | `review_no_aof_snapshot_then_appendonly_yes_restart_keeps_cold_dels`: green on monoio/tokio × s1/s4. The R2-1 tests are red at boot B. |
| F2 | Closed | `review_graves_of_an_unreadable_listed_file_survive_the_rebuild` is green. Trace below: carried graves are noted exactly once and never double-counted with the scanned ones. |
| F3 | Closed | `review_fuzz_target_round_trip_panics_on_an_empty_valid_trailer` is green. `decode(&[])` is `Ok(empty)`. The only other caller, `read_trailer`, short-circuits on empty input before calling `decode`, so the loader still maps "no bytes" to `None`. |
| F4 | Narrowed (R2-4) | bb6909c adds `a_file_spilled_after_boot_is_not_held_as_if_inherited` with a no-boot-view control arm. |
| F5 | Half closed (R2-3) | whole-file iterations: 36→0, 28→0, 35→0 and 27→0 heap files, all before the kill |
| F6 | Closed | `Encoder::add_file` writes straight from `SlotGraves::by_file` with no per-file clone. It is still O(graves) under the db write guards, which is acceptable up to about 1M graves (round-1 table). |
| F7 | Closed | `GRAVE_SLOTS_TOTAL` stays consistent through `note`, `note_unconditionally`, `forget_file`, `merge` (which zeroes `other.len` before `Drop`), `split_off_files` (moves the length, total unchanged) and `Drop`. No `by_file.clear()` bypasses it. |
| F8, F9 | Documented, not changed | Acceptable as documented. |
| Live-key guard | Green | `review_no_aof_graves_never_drop_a_live_respilled_key`: DBSIZE is equal at the kill and after on all four quadrants (18108, 16790, 19060, 16850), and every present probe holds its current value. |

## R2-1 (MAJOR): one boot later, the F1 fix no longer holds

03bf3c1's claim was: "the fold's key ledger is also seeded for every dropped slot it could decode, **so the first AOF-led boot cannot re-index it**". That holds only if a fold ran first. Three mechanisms combine:

1. **The fresh AOF generation is initialized without a fold.** At boot A, with no manifest yet, main.rs does one of two things:
   - `initialize_with_base(save_to_bytes(hot keys))` plus `seed_cold_cut` (s1 monoio, `main.rs:1810-1820`);
   - `initialize_multi` plus `seed_cold_cut` (s>1, `main.rs:1851`).

   `MOON.COLDCUT` authorizes every listed file below the watermark as a whole. Neither path writes the seeded ledger's DELs. Only `fold_stream` does (`fold_stream.rs:318-330`).
2. **Boot B skips the snapshot.** When the AOF manifest is the KV authority (`KvSources::Elsewhere`), the snapshot is not loaded, so its trailer is never read (`recovery.rs:283`, graves at `:463`). The rebuild then indexes every slot of every listed file.
3. **Legacy tokio s1 is also exposed.** That path reloads the snapshot at every boot (`SnapshotAndLogs`). But once the AOF process runs a BGSAVE, the new snapshot has **no trailer** (`timers.rs:664`, `if snapshot_hold::applies()`). The graves carried in RAM are therefore never written again.

Deleted cold probes back at **boot B**. Every run had 0 back at boot A.

| quadrant | no fold (`Nothing`) | BGSAVE under yes | BGREWRITEAOF under yes (control) | base 273e6bc, boot A / boot B |
|---|---|---|---|---|
| monoio s4 | **133/133** | **115/115** | 0/155 | all back / all back |
| monoio s1 | **157/157** | **102/102** | 0/135 | all back / all back |
| tokio s4 | **120/120** | **115/115** | 0/144 | all back / all back |
| tokio s1 | 0/88 | **168/168** | 0/100 | all back / all back |

**Not a regression.** 273e6bc resurrects them already at boot A and even after a rewrite, because it neither applies the graves nor seeds the ledger. The rewrite column shows that the ledger seed itself works. The fix is still incomplete, and its commit message states the opposite.

Operator scenario: switch `--appendonly no` to `yes` and restart. The server looks right. Then a crash or a routine restart happens before the first auto-rewrite, which needs 64 MB of AOF by default. Every cold key deleted before the switch is back, and nothing is logged.

**Suggested fix** (not applied; not a one-liner):
- **Fresh AOF generation after a snapshot boot.** When main.rs creates the manifest and the boot seeded the ledger from graves, write the ledger's DELs as the new generation's head, using the same `fold_cold_deletes` + `ColdDeletes` path a rewrite uses. Alternatively, run one synchronous fold before accepting clients.
- **Snapshots taken in AOF mode.** Keep writing the trailer whenever any `SlotGraves` is non-empty (i.e. drop the `applies()` gate for the encode, keep it for the hold). Graves are only ever correct-dead, so an AOF process carrying them is safe. This covers legacy tokio s1 and PITR-style snapshot boots.

## R2-2 (MAJOR, pre-existing): the s>1 no-to-yes switch loses the snapshot's hot keys at the second boot

Same tests, and the same result on the base binary:

1. `hot:control` (set before the no-AOF BGSAVE) is served at boot A.
2. It is `None` at boot B on monoio s4 and tokio s4.
3. `after:a`, written at boot A, survives.

The cause is at `main.rs:1851`. The multi-shard fresh-boot branch creates the PerShard manifest with an empty base. The single-shard monoio branch (`has_state`) captures a base; this branch has no equivalent. Boot B is `Elsewhere`, so the keyspace is wiped and the empty base plus the incrs are replayed.

Redis 7 boots EMPTY at A in this situation (round-1 oracle note). Moon serves the data at A and then loses it at B, which is worse than either consistent choice. This is outside the reviewed commits: file a separate issue. The fix mirrors the s1 branch: `initialize_multi` with a per-shard base captured from the loaded databases.

## R2-3 (MINOR, test integrity): `WholeFilesSweep` is an after-sweep point

`sweep_after_snapshot` runs **before** `bgsave_shard_done(true)` (`timers.rs:679-687`). So by the time `bgsave_and_wait` returns, the unlinks and the tombstone commit are already done. The random 0–2.5 s sleep only moves the kill later.

Every whole-file iteration printed `heap files N before, 0 at the kill`, with N = 36, 28, 35 and 27 across the four quadrants. The window between the unlink and the manifest commit is still never killed.

In addition:
- The point only `eprintln!`s the file counts and asserts nothing about them.
- It deletes every filler but checks only probes for resurrection.

The outcome is green on all 96 runs, so this is coverage, not a bug. Suggested changes:
- assert `files_at_kill < files_before`;
- MGET a sample of fillers after the restart;
- for the in-window kill, add a `MOON_TEST_*` hold between `unlink` and `manifest.commit` in the sweep, or kill from inside BGSAVE.

## #1269 hunt (persona step 2): nothing found

**(a) Every DEL/UNLINK path.** The only production callers of `key::del` and `key::unlink` are the arms at `command/mod.rs:220` and `:737` inside `dispatch_inner_unchecked`. Its first statement (`:194`) is `capture_dispatch_pre_image(db, *selected_db, ..)`, which sets the slot when armed. Everything reaches that point:
- the monoio and tokio local arms;
- `handler_single`;
- both MULTI/EXEC executors;
- the coordinator's `run_local` (`coordinator.rs:179`);
- the SPSC arms (`cow_intercept` followed by `cmd_dispatch`, `spsc_handler.rs:1078/1316/1557`);
- Lua (`capture_command_pre_image`, then `execute_command` → `dispatch`, `engine.rs:30`);
- replica apply (`replication/apply.rs:473`).

`try_inline_dispatch` frames only GET/SET. The other callers of `key::del` (`cold_read*.rs`, `db/mod.rs`) are tests. No path removes a key for DEL semantics outside `remove_counting_cold_costed` / `unlink_key_capturing`: grep finds only `key.rs:44` and `Database::unlink`, and `Database::unlink` has test callers only.

**(b) Stale `REMOVAL_SLOT`.** The hook sets it only when armed, and the next statement run on that call chain is the DEL/UNLINK arm. Nothing can refuse the command in between:
- every OOM, eviction, ACL or quota refusal happens before `dispatch`;
- the SPSC OOM `return`/`continue` sits before `cow_intercept`;
- in Lua, the gate is before the capture.

`cow_intercept` followed by `dispatch` sets the same value twice. When disarmed, `capture_removed_then` returns `Dispose` before looking at the slot. `clear()` resets it. The production fallback `db.db_index` equals the slot too (`db_plane::swap` swaps contents and keeps stamps). SWAPDB mid-epoch is pinned by `a_removal_after_a_mid_epoch_swapdb_files_under_the_epoch_database`.

**(c) Commands the hook still clones for.** Only DEL and UNLINK are skipped (`removes_only`). GETDEL, RENAME, MOVE, LMPOP, SPOP, SREM, ZREM, HDEL, EXPIRE-in-the-past and SET all still take the walker's copy. `remove_counting_cold` keeps its signature and result for its other callers.

**(d) Accounting.**
- The stored size is `entry_overhead_len + 64`, which equals `pre_image_bytes`. Every `overflow_bytes` subtraction uses the stored size: `take range` at `snapshot.rs:912`, `trim_step` at `frozen.rs:349`, and the reset at `:639`. So `current_cow_size` cannot drift.
- UNLINK's held credit equals what the lazy-free drain would credit (the drain's debug-asserted `expected`).
- A tombstone followed by a held move is safe: the tombstone is FIFO-first, the drain disposes the held entry, and it is credited once. Pinned by `a_key_created_then_removed_mid_epoch_stays_absent_and_is_credited_once`, which checks `estimated_memory` against an unarmed run and cow = 0.
- FLUSHDB after a held removal is pinned by `a_flushdb_after_a_held_removal_keeps_the_pre_image`.
- `DEL k k` is pinned by `a_repeated_key_in_one_removal_is_captured_once`.
- The abort paths dispose held entries through `frozen::dispose`. The bytes were already credited at removal, so nothing is double-freed or double-credited.

## bb6909c (persona step 4): nothing found

**One completion per file id.**
- A successful sub-batch uses `chunk[0].file_id`.
- The failure fallback calls `spill_single_entry(req, req.file_id)`: the first request rewrites the same id, and the batch itself sent no completion.
- A respawned thread drops its buffer and never re-sends (`spill_thread.rs:671-686`).

So no second completion can publish into a file that the all-superseded path already unlinked.

**Nothing else references the unlinked file.**
- `MOON.SPILLED` markers go out only for published groups, and `published_any == false` means none went out.
- A withdrawn group's keys are rehydrated hot.
- Ghosts are noted only after `add_file`.
- Reclaim reads listed files only.

**The unlinked path is the right directory.** The manifest lives at `offload/shard-N/shard-N.manifest` (`event_loop.rs:696`), and the spill writes to `shard_dir/data/heap-*.mpf` (`kv_spill.rs:439-442`) under the same `shard-N` dir. So `m.path().parent()` is the shard dir on every layout; the cold manifest is per shard whatever the AOF layout is. With `shard_manifest == None`, nothing is unlinked and the startup orphan sweep reclaims the file.

## Crash matrix (persona step 5)

Command: `crash_matrix_cold_graves_1281`, `MOON_TEST_MATRIX_RUNS=24`, run against the r2a binaries.

| quadrant | result | resurrected | lost/wrong |
|---|---|---|---|
| monoio s4 | ok, 24/24 | 0 | 0 |
| monoio s1 | ok, 24/24 | 0 | 0 |
| tokio s4 | ok, 24/24 | 0 | 0 |
| tokio s1 | ok, 24/24 | 0 | 0 |

Each ran in 64–72 s. Every `WholeFilesSweep` iteration showed N→0 files before the kill (R2-3).

## Tests added (branch `review/round2a`)

- **`src/persistence/snapshot/review_r2a_tests.rs`** (a39dca2): four #1269 guards, all green. The only change to `snapshot.rs` is the `mod` line.
- **`tests/review_r2a_cold_graves.rs`** (feb1b8b): R2-1 and R2-2.
  - `r2a_second_aof_boot_keeps_cold_dels` is red on monoio s4/s1 and tokio s4, and green on tokio s1.
  - `r2a_second_aof_boot_after_bgsave_keeps_cold_dels` is red on all four quadrants.
  - `r2a_second_aof_boot_after_rewrite_keeps_cold_dels` is green on all four quadrants on ad55d3d and red on base, so it is the regression guard for the ledger seed.
  - Both red tests stay red until R2-1 is fixed, and at s4 also R2-2. Merge them with the fix, or `#[ignore]` them with an issue link. Do not merge them red.

Commands:

```
cargo test --lib -- review_r2a removal_move_tests
MOON_BIN=<bin> [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test review_r2a_cold_graves -- --ignored --test-threads 1 --nocapture
```

No production code was changed.

## Confidence

| Area | Score | Note |
|---|---|---|
| Completeness | 0.91 | Every persona step was run on four quadrants. Not covered: a real power loss, Windows, and a kill inside the unlink→commit window (R2-3 says why the matrix cannot reach it). |
| Clarity | 0.92 | |
| Practicality | 0.91 | Each MAJOR has a red black-box repro, a base comparison, a code trace and a fix shape. |
| Optimization | 0.90 | R2-5 is traced from code and the commit's numbers, not re-measured. |
| Edge cases | 0.91 | The #1269 interleavings are pinned by tests. The live-key-loss direction is re-verified green. |
| Self-evaluation | 0.90 | |
