# WS12-snapshot-integrity SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws12-snapshot-integrity`, base `f32546c`, 9 commits.
> Measurement binary `/home/user/wt/bin/ws12-final-344270b` (release-fast, built and copied in one
> command; branch strings present, absent from `/home/user/wt/bin/ws13-base-f32546c`). Two
> release-fast builds used (budget 2). No new `unsafe` (244/244 SAFETY), unwrap ratchet 0.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1216 BGSAVE drops keys when a segment splits mid-save (P0) | **FIXED** | 5e18e5a (hash-space walk, absent-key tombstones, single capture queue) · 344270b (per-tick budget so the walk converges) · 2ef0d49 (docs) · 7562ebb, 1b00cc1 (real-server suite) | `split_epoch_tests` (9): verbatim repro **1,744/2,000 → 0**; splits of already-written segments 1,735 missing → 0, no duplicates; pre-image queued across a split 2,726 → 0; keys created mid-save 864 missing / 1,091 extra → 0; randomized 12/12 seeds red → 0/12, with coverage guards and mutation checks; bounded-tick and insert-flood convergence. Real server `perf_ws12_bgsave_split` (both runtimes, shards 1 and 4): base 111,743/200,000 and 199,521/800,000 missing → 0. | none |
| moon#1224 FLUSH*/SWAPDB during a BGSAVE panics the shard (P1) | **FIXED** | 650ba7e | `table_swap_tests` (12): the review's proof no longer panics; FLUSHDB, FLUSHDB ASYNC, pending-db FLUSHDB, FLUSHALL, FLUSHALL ASYNC and SWAPDB abort the save with the previous file intact; the real `ShardDbSet::swap` hook fires; harmless cases (db already written, empty db, refused args) replay to exactly the live keyspace. Real server: base dies on FLUSHALL; fixed keeps serving and reports the save as `err`. A no-op-abort mutant turns 7/7 abort tests red. | make FLUSHDB exact to the save's start by handing the old table to the save (needs `Database::clear`); a replica full resync during a BGSAVE is not detected |
| moon#1217 COW captures only `command[1]` | **FIXED** (MOVE / COPY … DB n not covered) | 02f2887 · f4a9d2b (blocking waker) | `multi_key_cow_tests`: 13/23 families diverged before (LMOVE, RPOPLPUSH, SMOVE, MSET, MSETNX, RENAME, RENAMENX, COPY REPLACE, SORT STORE, LMPOP, ZMPOP, DEL, UNLINK), 23/23 after. Routed arm and script `redis.call`: `[a]` → `[a, b, c, d]`. A woken BLMOVE's destination held `[x, v]`; now its save-start value `[x]`. | capture both databases in the MOVE / COPY … DB n intercepts (WS7/WS8 files) |
| moon#1185 remainder: incremental COW fold | **DEFERRED** | d561f1b (design in NOTES.md) | The exactly-once audit finds three mutation paths with no capture: MOVE / COPY … DB n, eviction with spill (silent loss), replica full resync — all in other workstreams' files. WS6's interim streaming fold stays; the shard stall remains O(dataset). | add those three hooks, then a consumer registry and a budgeted fold with a deterministic replay model test |

## Measurements
`bench_ws12.py one <bin> 1000000 <shards> <tag>`, one run per process, base (A) and final (B)
interleaved: load 1M `pre:` keys, run `redis-benchmark -t set -r 1e9 -P16 -c4`, BGSAVE, poll PING +
INFO until done, record peak RSS, SIGKILL, restart from the snapshot alone, GET every `pre:` key.

| run | binary | shards | inserts during save | BGSAVE | max PING | peak RSS | pre-save keys missing/wrong |
|---|---|---|---|---|---|---|---|
| A0 | base | 1 | 9.35M | 35.3 s | 40.1 ms | 954 MiB | 726,429 |
| B0 | final | 1 | 0.40M | 1.5 s | 1.1 ms | 205 MiB | **0** |
| A1 | base | 1 | 6.74M | 34.3 s | 419.9 ms | 852 MiB | 697,457 |
| B1 | final | 1 | 0.36M | 1.5 s | 2.7 ms | 197 MiB | **0** |
| A0s4 | base | 4 | 0.41M | 7.8 s | 9.2 ms | 142 MiB | 41,610 |
| B0s4 | final | 4 | 0.03M | 0.3 s | 1.1 ms | 131 MiB | **0** |

- Point-in-time: the final binary restores exactly the keyspace at the BGSAVE command (1,447,659 restored vs 1,447,451 at the command, 1.85M live by the end); keys created during the save are left out.
- A build without the per-tick budget never converged: the insert flood out-split one segment per tick (11.7 GB RSS, 60 s PING stall). 344270b fixes it; `the_walk_converges_under_a_sustained_insert_flood` pins it, with a one-segment-per-tick control that never finishes.
- Harness artifact (not moon): a second server started inside one Python process had its preload connection closed (base binary too); worked around with one measurement per process.
- Host: 4 vCPUs shared with other agents — relative numbers.

## Cross-ownership edits
1. `src/shard/persistence_tick.rs`: publishes the cursor after each advance (5e18e5a); calls `advance_budgeted_db` instead of `advance_one_segment_db` (344270b). ~2 lines.
2. `src/shard/db_plane.rs`: `ShardDbSet::swap` calls `snapshot_cow::note_swapdb` (650ba7e). 1 call.
3. `src/blocking/wakeup.rs`: 4 `capture_wake_pre_image` calls before the waker pops/pushes (f4a9d2b). No logic change.
4. `src/shard/spsc_handler.rs`: only `cow_intercept` (owned region); now captures through the same queue as dispatch, fixing a capture-ordering hazard.

## Risks / things the orchestrator must re-check at integration
1. **Merge onto PR #1221.** `snapshot.rs` is heavily rewritten (new fields, `advance_segment_inner`, `is_hash_pending`/`is_key_pending`, `capture_cow(db, key, Option<Entry>)` replaces `is_segment_pending`/`capture_cow(db, seg, …)`), tests moved to `src/persistence/snapshot/tests.rs`. Keep the hash-space walk; re-run `persistence::snapshot` on both runtimes plus `perf_ws12_bgsave_split` with `MOON_BIN` pinned.
2. **Behaviour change:** a BGSAVE crossed by FLUSHDB/FLUSHALL/SWAPDB on a db it hasn't finished now fails (error log, `rdb_last_bgsave_status:err`, previous file kept). Redis cancels a BGSAVE on FLUSHALL without flagging an error.
3. **File format unchanged, order changed:** segment blocks in hash order, pre-images within a block ordered by (hash, key). The loader ignores the segment index field.
4. **Memory during a save:** each key written in a not-yet-saved range holds its old value (or a small absent marker) until the range is written — not in `used_memory` (pre-existing).
5. Pre-existing gate failures on base `f32546c` (`useless_vec` in `tests/perf_ws6_aof_record_alloc.rs`; `benches/text_search.rs` on tokio) — both fixed on PR #1221's head.
6. Known gaps: MOVE / COPY … DB n capture nothing on either db; replica full resync during a BGSAVE undetected; moon#1185 stall O(dataset).
7. `aof_fold_exactly_once_455` is still `#[ignore]`d and red (moon#1134).

## Test results
- Unit: monoio 133 passed (`persistence::snapshot`, `script_write_captures`, `inline_set_captures`, `spsc_handler`, `persistence_tick`, `blocking::wakeup`); tokio 171 passed (same + `command::server_admin`).
- Integration, monoio (`MOON_BIN` = final): perf_ws12_bgsave_split 3, bgsave_startup_race 1, move_copy_db_crash_recovery_1046 4, wal_kv_db_context_1039 4, incr_in_place_942 12, crash_recovery_disk_offload_no_aof (`--ignored`) 1, recovery_matrix_w1 (`--ignored`) 1, crash_matrix_per_shard_bgrewriteaof (`--ignored`) 2.
- Integration, tokio: perf_ws12_bgsave_split 3, kill_snapshot 4, bgsave_startup_race 1, move_copy_db_crash_recovery_1046 4, incr_in_place_942 12.
- Red: `perf_ws12_bgsave_split` against the base binary fails 3/3.
- Gates: fmt, tokio clippy `-D warnings`, `check --lib --tests`, tokio `check --lib --tests --bins --examples` pass; clippy `--all-targets --keep-going` and tokio `check --all-targets` fail only on the two pre-existing base files above; unsafe, unwrap and test-tempdir audits pass.

## Self-evaluation (0–1)
Completeness 0.85 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 ·
Self-evaluation 0.9 — completeness is capped by moon#1185's fold, which is exactly-once only once
MOVE/COPY … DB n, eviction-with-spill and replica resync capture pre-images; those hooks live in
other workstreams' files, so the fold is deferred with the design written down.
