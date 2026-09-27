# WS22-cold-authority SUMMARY

Orchestrator workstream of the 2026-09 review round (base main `273e6bc`, branch
`claude/gifted-mendel-e9wiz5`). Issues: moon#1281 (data-loss class), moon#1279, moon#1269.
All results: **Linux container (4 vCPU x86_64, io_uring available), not the moon-dev VM, not the merge bar.**
Binaries: release-fast `base-{monoio,tokio}` (273e6bc), `ws22*-{monoio,tokio}` (this tree), all
under `/home/user/wt/bin/`.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1281 | FIXED | `5b5593d`, review round `03bf3c1` (+ review tests `5d0713b`, `33be29c`, `ad55d3d`), round 2 `c672cbd`, round 2b `a14a14b` | Baseline 273e6bc: `crash_recovery_cold_no_aof::no_aof_deleted_cold_keys_stay_deleted_after_a_bgsave_and_crash` brings back 113/113 (monoio s4), 114/114 (monoio s1), 178/178 (tokio s4), 105/105 (tokio s1) deleted cold probes; `…_second_bgsave_and_crash` 61–96 of 100 OLD values at boot 2. Fixed: 0 on all. Kill -9 matrix `crash_matrix_cold_graves_1281`: baseline resurrects every cold even probe at every must-stay-deleted point (71–84 per run); fixed 20/20 runs × {monoio, tokio} × {s1, s4}, 0 resurrected, 0 lost/wrong (before the `WholeFilesSweep` point was added in the review round; re-run at 24 runs on the final tree — see the PR). 13 + 3 unit tests; fuzz target `snapshot_cold_graves` (both matrices). | F8, F9 → moon#1291. N6/N7 → moon#1290. Slots that die after a snapshot starts come back after a crash before the next one — the snapshot RPO. |
| moon#1279 | FIXED | `06154be`, `bb6909c` | `cold_file_id_reuse_1067`: 273e6bc monoio 0/3 (120 s timeouts), tokio 3/3; fixed 3/3 on both. Unit `timers::promote_sweep_tests::a_file_spilled_after_boot_is_not_held_as_if_inherited` (drives the real boot hook + sweep; runs on the tokio per-PR leg). Superseded-spill variant: `persistence_tick::superseded_spill_tests::…` (red on 273e6bc). | — |
| moon#1269 | PARTIAL (removals fixed; in-place mutations still copy) | `0aa15c5` | 5M-field hash, save held mid-walk, interleaved A/B, restored HLEN 5M every run: monoio UNLINK 642/676/586 → 83/69/79 ms (max PING gap 841/898/807 → 83/70/79); DEL 876/2392/1052 → 75/101/99 ms (gap 950/2392/1047 → 152/212/183, before the DEL size hint); tokio UNLINK 1624/852 → 65/80, DEL 999/916 → 71/79. `removal_move_tests` (5). The randomized epoch property test caught an unstamped-db slot mismatch in the first cut (fixed: the slot comes from the dispatch hook). | HSET/LPUSH/… on a large collection still deep-clone it (needs chunked/persistent collections or a streaming per-key serializer — see the PR's design notes). |

## Review rounds 2 and 2b (reports: `../WS26-review-round/REVIEW-round2a.md`, `REVIEW-round2b.md`)
- **R2-1 (MAJOR):** round-1 F1 held only at the first `--appendonly yes` boot; the seeded dead-slot ledger reached disk only through a fold. `c672cbd` writes the ledger's DELs into a fresh generation's head. The round-2 gate then showed the head still carried no DELs (154/154 probes back at monoio s1): the cold wiring was detached for replay when the head was built. `a14a14b` re-attaches it first.
- **R2-2 (MAJOR, pre-existing):** at `--shards >= 2` the no -> yes switch created EMPTY per-shard bases, so the snapshot's hot keys were gone at the second AOF boot. `a14a14b`: `AofManifest::initialize_multi_with_bases` writes each shard's loaded state.
- `tests/review_r2a_cold_graves.rs`: 3/3 on monoio s1/s4 and tokio s1/s4 at `a14a14b` (was 1/3 on each at `c672cbd`).

## Design notes
- **moon#1281 — why a trailer, not a manifest barrier.** The graves ride the same atomic rename as the snapshot they describe, so there is no crash window between the two. Slots are identified by `(file_id, page, slot)` (exact, 6 B on disk, 8 B in RAM, no key copy; file ids are never reissued). Placed after EOF, before the global CRC: every v1–v3 reader stops at EOF, so the version byte stays 3 and a downgrade degrades to the old behaviour instead of refusing the file.
- **Why not "the snapshot references every live cold slot".** That needs an O(cold keys) point-in-time copy of the cold index at snapshot start on the shard thread (seconds at 10M+ cold keys), or a COW scheme for the cold index. The dead-slot record is O(dead slots) and is already maintained at the same call sites as the AOF ledger.
- **moon#1269 — options evaluated.** (1) refcount/Arc values: a pre-image becomes a refcount bump, but the first in-place write then pays `Arc::make_mut` = the same O(n) clone unless the collection is chunked (HAMT / chunked B-tree with shared nodes) — a large storage change; (2) serialize-on-write: removes the retained copy (RSS) but the O(n) serialization stays on the shard thread unless it streams per chunk with the key write-locked; (3) Dragonfly-style versioned buckets: removes per-key pre-images, but a segment holding one 5M-field value still serializes O(n) on the first write. Removals are the one class where the value leaves the keyspace anyway: moving it is free, which is what shipped. Interaction with #1185: a TXN.ABORT undo that restores a value mid-epoch must capture (by move) the uncommitted value it replaces — see the decision brief.

## Measurements (method)
- #1281/#1279: real-server crash suites (`tests/crash_recovery_cold_support` harness), `--appendonly no --save "3600 100000000"`, cold probes inherited from an `--appendonly yes` phase; kill -9 via `ServerGuard::kill_now`.
- #1269: `bench.py` (scratchpad `m1269/`): HSET 5M fields + 20K filler keys, `MOON_TEST_SNAPSHOT_HOLD_FILE` touched, BGSAVE, PING loop on a second connection, UNLINK/DEL, release the hold, restart from the snapshot and check HLEN. `--shards 1 --appendonly no --save "" --disk-offload disable`. Ports rotated per run: restarting a monoio server on the same port right after a kill -9 intermittently reset the next client connection (observed twice; likely the io_uring-owned listener outliving the process under SO_REUSEPORT — not investigated further).

## Cross-ownership edits
`src/command/key.rs` (DEL/UNLINK), `src/storage/db/kv_ops.rs`, `src/shard/event_loop.rs` (one boot hook), `src/shard/persistence_tick.rs` (ghost slot locations; all-superseded completion), `src/command/connection.rs` (INFO `cold_grave_*`), `fuzz/`, `.github/workflows/fuzz.yml`, `docs/STORAGE-FORMAT-V1.md`.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.92 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
(#1269 is partial by design; the in-place-mutation class is documented, not solved.)
