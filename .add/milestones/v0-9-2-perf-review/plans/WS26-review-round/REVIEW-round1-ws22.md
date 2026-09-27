# Adversarial review — moon#1279 (06154be) + moon#1281 (5b5593d)

Reviewer lens: storage-durability-engineer + ci-test-integrity-engineer.
Environment: Linux container (4 vCPU, shared with other builders), **not the merge bar** (no moon-dev VM, no hosted matrix).
Binaries: `/home/user/wt/bin/{base,ws22}-{monoio,tokio}` (release-fast). Provenance caveat: the `ws22-*` files are dated 04:12/04:14, before the commit timestamps (04:41 / 05:09); they do contain the #1281 log strings and pass every fix test, but the merge evidence should be re-run on a binary built from 5b5593d.
Review tests: branch `review/ws22` in this worktree — `884eb3f` (unit) and `79ca2bc` (black-box).

## Verdict

**No BLOCKING finding. Nothing I tried loses a LIVE key.** Both fixes do what they claim, on both runtimes, at `--shards 1` and `4`, and the new tests go red on 273e6bc for the real reason. Two MAJOR coverage gaps remain in #1281: two recovery paths still bring deleted cold keys back, and a red test is committed for each. Neither is a regression against 273e6bc, which brings the keys back on every path. Both should be tracked as follow-ups; neither needs to hold the merge.

| # | Sev | Finding | Repro (red on 5b5593d) |
|---|---|---|---|
| F1 | MAJOR | A no-AOF snapshot booted with `--appendonly yes` loads its hot keys but ignores its graves. All deleted cold keys come back. | `review_no_aof_snapshot_then_appendonly_yes_restart_keeps_cold_dels`: 118/118, 164/164, 185/185, 128/128 back (monoio s4, tokio s4, monoio s1, tokio s1). The `--appendonly no` control on the same bytes brings back 0. |
| F2 | MAJOR | Boot re-records only the graves it actually scanned. When a listed file is unreadable at boot (or missing and later returns), its graves are dropped. The next snapshot has no trailer for it, and the boot after the file is fixed brings its deleted keys back. | `review_graves_of_an_unreadable_listed_file_survive_the_rebuild` (unit) |
| F3 | MINOR | The `snapshot_cold_graves` fuzz target panics on a valid 13-byte empty trailer. The next nightly run can go red for a reason that is not a bug. | `review_fuzz_target_round_trip_panics_on_an_empty_valid_trailer` (panics with `an encoded trailer decodes: Truncated`) |
| F4 | MINOR | The #1279 unit test cannot tell fixed from unfixed code. The only discriminating guard runs on monoio, which the per-PR gate does not run. | proof by construction (below) |
| F5 | MINOR | The matrix's `DuringSweep` point never unlinks a file and never commits a tombstone. | `review_crash_matrix_during_sweep_unlinks_nothing`: 35 → 35 heap files, `cold_files_pending_unlink:0` |
| F6 | MINOR | The trailer is built on the shard thread under the database write guards. Cost is O(graves): 5.7 ms at 1M graves, 91 ms at 10M. | standalone `-O` bench of the exact codec (below) |
| F7 | MINOR | `SlotGraves` memory appears in no INFO field and no maxmemory or admission check, and nothing reclaims it without an AOF. | trace |
| F8 | MINOR | With `--databases` below the manifest's db count, that db's graves are dropped with its index. Raising `--databases` again brings its deleted keys back. | trace |
| F9 | MINOR (pre-existing, adjacent) | `evict_batch_durable`: when the manifest commit fails, the file stays in the in-memory manifest. A later commit lists it. Its slots are neither live nor graves, so a later hot DEL plus a restart brings them back. | trace |
| N1–N7 | NIT | Doc-comment splice, redundant trailer CRC claim, upgrade semantics, observability, `evicted_keys` observation. | — |

## Evidence that the fixes work (Linux container, not the merge bar)

- `crash_matrix_cold_graves_1281`, `MOON_TEST_MATRIX_RUNS=10`, s4:
  - ws22-monoio and ws22-tokio: every point resurrects 0, and 0 are lost or wrong.
  - base-monoio: 75 even probes resurrected at `AfterPublish`, `DuringSweep` and `SecondGeneration`. This is the right reason: all of the cold even probes, every time.
- `crash_recovery_cold_no_aof` (the two new tests): ws22-monoio 2/2 pass, base-monoio 2/2 fail.
- `cold_file_id_reuse_1067::…clean_restart`:
  - ws22-monoio passes in 4.2 s.
  - base-monoio fails after 120 s with `timed out … waiting for boot 1's cold files to be reclaimed`, `cold_files_pending_unlink:8`. This is exactly the #1279 symptom.
- **Live-key-loss guard** (`review_no_aof_graves_never_drop_a_live_respilled_key`, new). Setup:
  1. The inherited files keep live fillers.
  2. The odd probes are read-promoted and the even probes overwritten, so their old slots become graves.
  3. Writing *new* keys re-spills them through the no-AOF `evict_batch_durable`.
  4. BGSAVE, kill -9, restart.

  Every probe that existed at the kill must read its current value, and DBSIZE must be unchanged.
  - ws22: green on monoio/tokio × s1/s4. The boot applied graves ("moon#1281" boot lines: 4 at s4, 1 at s1).
  - base-monoio: red. 9 probes read their OLD value and DBSIZE went 18746 → 18826.

  This is the regression guard the matrix is missing (see F5).

## Live-key-loss hunt (persona step 2) — all traced safe

A grave hides a live key only if a slot that was recorded dead later becomes some key's entry again. Slots are immutable, and file ids are never reissued: the manifest keeps the highest tombstone, and `next_file_id_seed` takes the max of the manifest and the disk. So this needs an `insert` of a location that was recorded dead earlier. I checked every insert and move of a location:

- **`ColdIndex::insert`** callers:
  - spill completion publishes fresh slots;
  - `evict_batch_durable` inserts after `victim::remove`, which records the *old* slot;
  - `cold_reclaim::finish_adoptions` inserts `m.to` only if the survivor is still at `m.from`, and records a ghost otherwise. Each output is processed once, and reclaim is AOF-only.
- **`merge_newer` / `split_off_files` / `promote_orphaned_older_copies`** (SWAPDB replay): these move live entries and older copies, which are never graves. Older copies become graves only in `release_older_copies*`, which also removes them.
- **`ColdIndex::merge`** (recovery): `existing` is always `None` in production, so this is a straight attach.
- **Promote, MOVE, COPY** (moon#1254): `move_cmd.rs:79` promotes first and removes the cold entry, so the slot becomes a grave while the key is hot and in the next snapshot image.
- **Ghosts** are recorded only after `manifest.add_file` succeeds, with the completion's exact `(page, slot)`. Two slots for the same key in one batch are distinct.
- **A snapshot starting during a spill**: no-AOF eviction is synchronous (`eviction.rs:1040`, where `appendonly != "yes"` leads to `evict_batch_durable`). No slot is published after the trailer is taken that the trailer could name, and the trailer holds only slots that were already dead at start.
- **Graves are applied before newest-wins**, so a dead newer slot cannot hide a live older copy. This is proven by the guard test above.

## Findings in detail

### F1 — MAJOR: `--appendonly no` → `yes` restart ignores the trailer

`recovery.rs:458-460` applies the graves only when `snapshot_hold::applies()`, meaning the **booting** process has no AOF writer:

```rust
let graves = snapshot_graves.as_ref().filter(|_| crate::storage::tiered::snapshot_hold::applies());
```

The AOF pool (and `enable_ledger`) is created at `main.rs:939`, before `restore_from_persistence` at `main.rs:1424`.

Consider a directory written by a no-AOF process: a snapshot with a trailer plus spill files, and no `appendonlydir`. Boot it with `--appendonly yes`:

1. `aof_manifest_is_kv_authority` is false, so `KvSources::SnapshotAndLogs` applies (`kv_sources.rs:36`).
2. The snapshot's hot keys **load**.
3. Its graves are thrown away, and every slot of every listed file is indexed.

The result is a keyspace that neither the snapshot, Redis, nor the pre-crash server ever had. Repro: `tests/review_ws22_cold_graves.rs`, red in all four quadrants, with a green single-variable control.

Redis oracle (7.0.15, port 7690): with `appendonly yes` and no AOF, Redis ignores `dump.rdb` and boots EMPTY ("Creating AOF base file … on server start"). Moon deliberately loads the snapshot instead. Once it does, it must honour the snapshot's graves.

Suggested fix (not a one-liner, not applied):
- Apply the graves whenever the loaded snapshot carries them (drop the `applies()` filter). This is safe because graves are only ever correct-dead: a slot never comes back to life.
- When the booting process has an AOF, also seed the key ledger (`dead`) for each buried slot. Decode the key before the `continue` at `rebuild.rs:211`. Otherwise the first boot is right, but the next AOF-authority boot (snapshot skipped, so no graves) re-indexes those slots unless a fold has written their DELs.

### F2 — MAJOR: graves of an unscanned listed file are dropped

`rebuild.rs` re-seeds `index.graves` from `buried_per_db`, which holds only the slots it actually read. Two paths skip the file:
- `files_unreadable` (`rebuild.rs:143`, `continue`);
- `files_missing` (`:121`). The index code explicitly models this file coming back (moon#875: "a remount, an operator `mv` — the bytes come back").

The manifest entry stays Active, so the next boot that can read the file indexes every slot in it, deleted ones included. The trailer from the boot in between is empty for that file. The rebuild's own error text promises the user a recovery ("until the file is readable and the server restarts"), and that recovery then brings deleted keys back with no log line.

The unit repro uses a directory in place of the heap file (EISDIR; the tests run as root, so `chmod 000` does not work).

Suggested fix: after pass 1, carry every trailer grave whose `file_id` is listed Active but was not scanned into that db's index (`note_unconditionally`).

### F3 — MINOR: the fuzz target has a false crash

`fuzz/fuzz_targets/snapshot_cold_graves.rs:17`:
1. `decode` accepts `MCGV|1|file_count=0|crc` (13 bytes), or any trailer whose files all have `slot_count=0`, and returns EMPTY graves.
2. `encode(&[])` is zero bytes by design (no trailer).
3. `decode(&[])` returns `Err(Truncated)`, so the target's `.expect(...)` panics.

libFuzzer's CMP tracing solves a 4-byte CRC compare, so the 5 h nightly run will likely find this.

One-line fix (not applied, to keep the repro red): `if graves.is_empty() { return; }` before the round trip. Alternatively, compare `to_files()` instead of re-decoding.

### F4 — MINOR (test integrity): the #1279 unit test is not a regression guard

`unlink_hold::tests::the_first_view_fixes_the_hold_baseline_so_the_boot_view_must_come_first` only calls `UnlinkHold`, and 06154be changed `unlink_hold.rs` **only** in that test (`@@ -422,4 +422,42 @@ mod tests`). So the test passes on 273e6bc too, and it still passes if the `observe_boot_fold_view` call in `event_loop.rs:843` is deleted.

The discriminating test is `cold_file_id_reuse_1067` on **monoio** (red on base, reproduced above). The per-PR tokio leg is green with or without the fix, because tokio's sweep ticks at t=0. So only the local `ci-local.sh` and the post-merge monoio run guard #1279.

Recommend: say so in the test's doc, or add a shard-level test that asserts `ColdIndex` has observed a view before the first sweep.

### F5 — MINOR (test integrity): `DuringSweep` does not test what its table says

The matrix doc describes `DuringSweep` as "the post-save sweep, its unlinks and manifest commit". But the iteration deletes only the EVEN probes, so every spill file keeps its odd probes and fillers. Nothing goes zero-ref, so there is no unlink and no tombstone commit. The review test measured 35 → 35 heap files and `cold_files_pending_unlink:0` after DEL, BGSAVE and 5 s. The point is therefore the same as `AfterPublish` plus a sleep.

Also, "every ODD probe comes back" guards only keys whose slots never died. That is trivially safe against a grave bug; the guard test above covers the direction that could lose data.

Recommend:
- a point that DELs every key of some files (all probes and some fillers, for example), so the post-save sweep really unlinks, forgets the graves and commits tombstones inside the kill window;
- adopting the re-spill guard.

### F6 — MINOR: trailer build cost on the shard thread

`note_snapshot_started` runs inside `with_shard_db`, which holds the write guard, so foreign-shard reads of that db wait too. For every db it clones one `Vec<u64>` per file and then encodes. The exact codec, compiled standalone at `-O`, with 255 graves per file:

| graves | collect + encode (stall per BGSAVE / save point) | trailer size | boot decode |
|---|---|---|---|
| 100 k | 0.37 ms | 0.6 MB | 1.9 ms |
| 1 M | 5.7 ms | 6.0 MB | 25 ms |
| 4 M | 28 ms | 24 MB | 85 ms |
| 10 M | 91 ms | 60 MB | 213 ms |

The unit probe `review_trailer_build_cost_at_1m_graves` measures the same code in the dev profile, which is about 12× slower (71 ms at 1M).

No chunking is needed up to about 1M graves. Past that, drop the per-file clone by encoding straight from `by_file` into one pre-sized buffer, which roughly halves the time. Better, move the copy off the thread: swap the graves into an `Arc` snapshot at start and let the stream writer encode at finalize.

Nothing bounds graves without an AOF (see F7), so 10M is reachable: 10M cold keys under delete churn, with one survivor per 256-slot file.

### F7 — MINOR: `SlotGraves` memory is invisible

`SlotGraves::resident_bytes()` is dead code: its only callers are my probe and its own tests.
- `ColdIndex::resident_bytes()` counts the AOF ledger (`dead_slot_bytes`) but not graves.
- Neither INFO nor `evict_to_budget` admission sees graves.
- Without an AOF there is no cold reclaim (`cold_reclaim` is ledger-driven), so graves live as long as one key keeps each file.

At 8 B/slot that is about 1/50 of the disk those slots pin. Acceptable as a design choice, but it should be counted, for example `cold_graves_slots` / `cold_graves_bytes` in INFO, so an operator can see why RSS grows under delete churn.

### F8 — MINOR: `--databases` reduced drops graves

At `recovery.rs:537-547`, for a manifest db beyond `databases.len()`, `cold_idx`, including its re-seeded `graves`, is dropped with a warning. The next snapshot's trailer omits those slots. Restarting later with the original `--databases` brings back every key in those files that was deleted before the earlier snapshot. The same fix as F2 applies: keep graves for listed files that were not attached.

### F9 — MINOR (pre-existing, adjacent): commit failure leaves slots neither live nor graves

At `eviction.rs:1616-1624` (`evict_batch_durable`), when `manifest.commit()` fails, the loop does `continue` and the keys stay hot. The entry pushed by `add_file` stays in `active_root` (`manifest.rs:807`), and the next successful commit lists the file durably. None of its slots is in the index, and none is a grave. If those keys are later DELeted while hot (no cold entry, so no grave), the next restart indexes them from this file.

With graves this has a cheap fix: on commit failure, note every slot of the completion as a grave, or `remove_file` the entry again.

### NITs

- **N1** `timers.rs:412-416`: the new `fold_view_now` was inserted *inside* `run_cold_orphan_sweep`'s doc block. The whole sweep doc, including "In a TopLevel layout … (see the guard below)", now documents `fold_view_now`, and `run_cold_orphan_sweep` (`:479`) has no doc.
- **N2** The trailer's own CRC is effectively redundant. The global CRC covers the trailer and is a hard error (`snapshot.rs` load: "Verify global CRC32 … hard error"). So the cold_graves.rs doc line "a trailer that fails its own checks is logged and ignored — never a reason to refuse the key data" only describes a trailer that is self-inconsistent but globally valid, which only a writer bug produces. A flipped byte in the trailer refuses the whole snapshot, like any other byte.
- **N3** `entries_tombstoned` shows up only in a log line (when > 0), not in INFO. That is fine for correctness (`is_degraded` correctly excludes it), but a monitor cannot see it.
- **N4** Upgrade semantics, which should go in the release note: keys deleted under a pre-#1281 binary before the upgrade have no grave anywhere. The first new-binary boot indexes them as live, and nothing will ever make them graves. The same applies to a downgrade followed by an upgrade.
- **N5** `SlotGraves` keeps duplicates (`len` counts them twice). This wastes bytes and nothing else; `decode` deduplicates.
- **N6** (observation, pre-existing, cause not traced) Under `--appendonly no` with disk offload and a manifest, `allkeys-lru` still **plain-dropped** 2–5 k of 16 k keys in every review run (`evicted_keys` 2061–5401 on both base and ws22). `evicted_keys` counts only real drops (`record_eviction`). That looks like a sink with no spill context (`evict_one_with_spill(None)`), and deserves a storage-owner look: the user asked for tiering, not eviction.
- **N7** `CONFIG SET appendonly yes` only changes `runtime_config.appendonly` (`config.rs:265`, "accepted but don't take live effect"). But `eviction.rs:1040` reads that string, so a no-AOF process routes eviction to `evict_one_async_spill` (the in-flight window) with no AOF writer behind it. This is pre-existing and not needed by #1281, but it breaks the "no-AOF eviction is synchronous" premise the trailer relies on. I did not build a repro for the #1281 interaction.

## #1279 specifics (persona step 5)

- **Inherited files in AOF mode:** `observe()` raises `hold_below` to the current counter on every epoch change, so the boot view sets the baseline to the boot seed. Every file inherited at boot (id < seed) is still held until a committed fold covers it. Files minted after boot are unlinked without a hold, exactly as tokio already did with its t=0 sweep. **Parity, not a new rule.**
- **tokio double observe** (boot view, then the t=0 sweep): same epoch, so `hold_below = max(...)` is unchanged. Harmless.
- **Hook placement:** runs after `slice::init_shard` (`event_loop.rs:115` < `:843`) and only when `disk_offload_enabled()`.
- **Index attached later:** a `Database` rebuilt wholesale (replica full sync keeps its index through `clear()`) gets its first view at the next sweep. That holds *more*, which is the safe direction.
- **Replica role:** same code path.
- **TopLevel with more than one shard** (never shipped): unchanged.
- **Interaction with #1281:** a fully-graved inherited file queued at boot is below the boot seed, so it is held until the next snapshot commits. That snapshot re-carries its graves. Consistent.

## Tests on `review/ws22`

- **`src/storage/tiered/review_ws22_tests.rs`** (commit `884eb3f`): F2 repro (red), F3 repro (red), and the F6 probe (ignored).
- **`tests/review_ws22_cold_graves.rs`** (commit `79ca2bc`):
  - F1 repro (red, four quadrants);
  - F5 evidence (green);
  - live re-spill guard (green on ws22 in four quadrants, red on base).

Run:

```
cargo test --lib review_ws22
MOON_BIN=<bin> [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test review_ws22_cold_graves -- --ignored --test-threads 1 --nocapture
```

The three red tests stay red until F1, F2 and F3 are fixed. Merge them together with those fixes, or mark them `#[ignore]` with an issue link. Do not merge them to main red.

## Confidence

| Area | Score |
|---|---|
| Completeness | 0.90 — every persona step covered. What I skipped: a real power-loss (non kill -9) run, and a Windows run. |
| Clarity | 0.92 |
| Practicality | 0.91 — each MAJOR has a red repro, a code trace and a concrete fix shape. |
| Optimization | 0.90 — measured optimized cost, with a concrete remedy. |
| Edge cases | 0.90 — the live-loss vectors are traced and guarded by a test. The N6/N7 causes are not fully traced and are marked so. |
| Self-evaluation | 0.90 |
