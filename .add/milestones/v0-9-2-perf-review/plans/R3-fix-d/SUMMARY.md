## Result

**Verdict: (c) test logic, exposed by a wave-2a change that works as intended (WS39, moon#1289). There is no product bug.** The fix touches the test only. It is commit `4b54a83` on `w2/r3-fix-d` (worktree `/home/user/wt/lane-d`, based on `bda76c1`). Only `tests/crash_recovery_cold_del_rewrite.rs` changed.

**What happens (from a driver that replays the scenario and samples INFO every 500 ms, r2-81fda0c, --shards 4):**
1. moon#1289's automatic fold (`d75fe54`) commits about 3.5 s after the touches: three 1 s sweeps mark the files stale, then the monitor's 1 s tick dispatches the fold. That is inside the test's 5 s wait. The sweep after it releases every held file: `cold_files_pending_unlink` 13 → 0, AOF generation 8 → 12.
2. On tokio, a GET can serve a cold key without promoting it (the read-only `get_cold_value` path). After a GET of every probe, 33–38 of the 200 probes are still cold (`cold_keys:38`, and the same at restart). Their spill files stay *referenced*, so the heap-file count never reaches 0. This is the same on main (`lane-b-base-tokio`, `cold_keys=33`).
3. The old test treated "a spill file is still on disk" as "files are still held". So on tokio it ran the manual second BGREWRITEAOF with nothing held (`pending_unlink:0`). That rewrite committed (generation 12 → 16) but had nothing to release, so the count stayed at 22 → 22 and the assertion failed.
4. **monoio** promotes on every GET (`cold_keys:0`), so the count reaches 0 and the auto-fold branch from f6033bd/b55c304 handles the case.
5. **Main** has no automatic fold. The files are still held when the manual rewrite runs, and it releases them.
6. Whether the old test passes depends on whether the automatic fold lands inside the 5 s window. That explains the mixed r1b results. On this box it now lands there every time.
7. A side finding: b55c304's ordering check only ran once *no* spill file was left. Tokio never gets there, so on tokio that check verified nothing.

**What the test now asserts (meaning kept, and stricter):**
- **Ordering (moon#1231):** the test lists the spill files right after the fold, since the hold covers every one of them. Every 100 ms during the wait, if any of those files is gone, a later AOF generation must already have committed. The file listing is taken before the generation is read.
- **If INFO `cold_files_pending_unlink` > 0 after the wait:**
  - The second BGREWRITEAOF must commit a *new* generation. It is resent if an automatic fold holds the in-progress flag. The old `rewrite_and_wait` could not check this, because every shard already had a compacted base.
  - Within 10 s, `pending_unlink` must reach 0 and the file count must drop. This replaces the old blind 4 s sleep.
- **Otherwise:** at least one at-fold file must be gone (after a later commit), or the test fails with "nothing was ever held".
- The kill -9 recovery check is unchanged.
- I also moved the scenario doc comment, which b55c304 had attached to `base_generation`, back onto `run_promote_scenario`. One diagnostic `eprintln` says which path ran.

**Gates (Linux container, not the merge bar):**
- `cargo fmt --check`: OK.
- clippy `--all-targets -D warnings`, both feature sets: OK.
- `cargo test --release --lib` filtered to `held_release` / `unlink_hold` / `auto_rewrite`, both feature sets: 7 / 10 / 19 passed each. No `src` was touched.
- Suites against r3fixd-v1 binaries:

| suite | tokio | monoio |
|---|---|---|
| `crash_recovery_cold_del_rewrite` | 21/21 | 21/21 |
| `cold_held_files_release_1289` | 6/6 | 6/6 |
| `held_release_txn_open_1289` | 3/3 | 3/3 |
| `held_release_txn_race_1289` | 5/5 | 5/5 |
| `cold_orphan_sweep` | 5/5 | 5/5 |
| `aof_fold_exactly_once_455` | **FAIL** | **FAIL** |

**`aof_fold_exactly_once_455` fails, and it is not from this change.** The only test, `exec_parked_in_wait_across_a_rewrite_replays_once_toplevel`, fails with "EXEC returned before WAIT ran out — the window under test never opened". I got the same failure with this branch's harness against `lane-b-base-monoio` (main-equivalent), `r1-f766fc2-monoio` and `r2-81fda0c-monoio`. Not chased further.

Housekeeping: no servers left running. I removed only test directories my own runs created.

---

# R3-fix-d SUMMARY

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| `held_spill_files_are_released_after_the_next_rewrite` red on tokio (wave 2a): "the second rewrite committed but the sweep released no held spill file (N before, N after)" | FIXED (test logic; no product bug) | `4b54a83` | Before, held_spill only: `lane-b-base-tokio` 8/8 pass; `r2-81fda0c-tokio` 0/9 (N/N, N = 14–22). After: `r2-81fda0c-tokio` 8/8, `r2-81fda0c-monoio` 8/8, `r3fixd-v1-tokio` 10/10, `r3fixd-v1-monoio` 10/10; base binaries tokio 3/3 and monoio 2/2 via the manual path ("36 held after the wait, 0 held after the second BGREWRITEAOF"); `MOON_TEST_COLD_DEL_SHARDS=1` promote cases green on both runtimes. Whole suite 21/21 on both runtimes. Mutants, tokio: hold released at once → "7 of the 32 spill files on disk at the fold … were unlinked before any later fold committed (AOF generation 8 -> 8)" (the old test blamed this on the second rewrite); hold never released → "did not release every held spill file within 10s (33 files and 16 held before; 33 files and 16 held after)". | A tokio GET can leave a cold key cold (read-only `get_cold_value` path; 33–38 of 200 probes after one GET each, on main too). This is pre-existing and not a durability issue, but it differs from monoio. Worth a decision on whether tokio GET should promote. |

## Measurements (method, reps, raw numbers)
- **Repro:** copied tokio harness at `bda76c1`, `--include-ignored --test-threads=1 held_spill`, `MOON_DISK_FREE_MIN_PCT=0`, `MOON_BIN` pinned. Base `lane-b-base-tokio`: 8/8 pass. HEAD `r2-81fda0c-tokio`: 9/9 fail (16/16, 21/21, 14/14, 18/18, 19/19, 17/17, 16/16, 19/19, 20/20).
- **Driver** (python, same flags as the suite, INFO sampled every 500 ms after the touches):

| binary | `cold_keys` after GETs | automatic fold | held files after the 5 s wait | heap files after the 5 s wait |
|---|---|---|---|---|
| HEAD tokio | 38 | generation 8 → 12 at about +3.5 s; `pending_unlink` 13 → 10 → 0 | 0 | 22, unchanged after manual rewrite to generation 16 |
| HEAD monoio | 0 | same | 0 | 0 |
| base tokio | 33 | none | 15 (released by the manual rewrite) | 33 |

- A second GET pass on tokio promoted 38 → 10 of the cold probes.
- At restart, the HEAD tokio directory held 200 probes, 38 of them cold, and 0 fillers.

## Cross-ownership edits
`tests/crash_recovery_cold_del_rewrite.rs` (`run_promote_scenario` and three new local helpers). The shared harness `crash_recovery_cold_support/` is untouched.

## Risks / things the orchestrator must re-check at integration
- `aof_fold_exactly_once_455` fails on main-equivalent `lane-b-base-monoio`, `r1-f766fc2-monoio`, `r2-81fda0c-monoio` and `r3fixd-v1-{tokio,monoio}` ("EXEC returned before WAIT ran out"). This is pre-existing and not caused by this change.
- The manual path now reads INFO `cold_files_pending_unlink`, which each sweep publishes. There is a microsecond window inside one sweep, between unlink and publish, where the file count and the held count could disagree. This was not hit in any run.
- Binaries: `/home/user/wt/bin/r3fixd-v1-{tokio,monoio}` (release-fast, `bda76c1` product code, marker "WITHOUT their clean-close marker"). Test-only mutants, never to be shipped: `/home/user/wt/bin/r3fixd-mut{premature,leak}-tokio`.
- Results are from a Linux container, not the merge bar. The Windows/MSRV hosted matrix was not dispatched.

## Self-evaluation (0–1)
- **Completeness 0.92:** root cause, attribution to `d75fe54` plus tokio's non-promoting GET, fix, at least 8 passes on both runtimes, mutants, and the requested suites are all done. The 455 failure is shown to be pre-existing.
- **Clarity 0.93**
- **Practicality 0.95:** test-only change; total runtime is about the same.
- **Optimization 0.9:** polls with a bound instead of a fixed sleep.
- **Edge cases 0.9:** handles the flat `--shards 1` layout, an automatic fold holding the in-progress flag, a partial release, and the "nothing ever held" case.
- **Self-evaluation 0.9.**
