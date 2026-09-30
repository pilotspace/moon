# WS39-held-files SUMMARY

Wave 2, lane C, phase 2: branch `w2/ws39-held-files`, based on the WS38 head. Linux container, not the merge bar.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1289 held cold files have no release trigger | FIXED | 0b8e2f8 | `tests/cold_held_files_release_1289.rs`, 6 tests in the 1067 shape.<br>• Base, both runtimes: 4 of 6 fail. After 60 s, `cold_files_pending_unlink:8` and a 63 KiB ledger remain.<br>• Fix, both runtimes: 6/6 pass. Held files are released 5.0 s after they are held with an AOF, and 2.0 s after without one.<br>• A steady-state control (no held file) requests 0 folds and 0 snapshots on base and on the fix.<br>• 12 unit tests. | moon#1297 (WS43) reuses `persistence::snapshot_request`. |

## Design
- **Maintainer options:** option 1 with an AOF, option 2 without one.
- **Where it runs:** `held_release_tick::after_sweep` runs after each orphan sweep on both runtimes. It needs no new timer and no `% N` counter.
- **When a database goes stale:** after 3 consecutive sweeps that each saw a held file awaiting a fold at the same committed floor. A change of floor restarts the count.
- **With an AOF:** the auto-rewrite monitor folds through the existing ColdReclaim path. This works even with `auto-aof-rewrite-percentage 0`.
- **Without an AOF:** a reusable `persistence::snapshot_request::request(tx, n_shards, SnapshotReason, spacing)` asks for a snapshot. It uses one process-wide gate, so racing shards start a single snapshot, and it goes through `bgsave_start_sharded`.
- **Defaults:** stale after 3 sweeps (at least 2 min) and at most one fold or snapshot per 10 sweep intervals (10 min). These are counted in units of the existing `--cold-orphan-sweep-interval-secs`.
- **New INFO fields:** `cold_held_files_stale_databases`, `cold_held_release_folds_requested`, `cold_held_release_snapshots_requested`.

## Measurements
All 14 touched cold, AOF, snapshot and consistency suites pass on both runtimes. `perf_ws12_bgsave_split::a_resync_mid_bgsave…` fails on tokio, identically on base, because tokio has no master-side PSYNC. The consistency suite shows no change from WS38.

## Cross-ownership edits
- `src/shard/event_loop.rs`: 2 call sites added in the sweep arms, 7 lines each.
- `src/shard/timers.rs`: `fold_view_now` is now `pub(crate)`.
- `src/storage/tiered/cold_reclaim.rs`: one field added.
- `src/command/connection.rs`: 3 INFO lines.

## Risks
- WS43 shares `snapshot_request`, `cold_reclaim_tick` and `auto_save`. Merge WS39 first.
- A no-AOF server with continuous cold-file churn now snapshots at most every 10 min. This is the intended trade-off.

## CHANGELOG bullet
- **fix(tiered):** held cold spill files are now released without a manual `BGREWRITEAOF` or `BGSAVE` (moon#1289).
  - After three orphan sweeps (about two minutes by default) with no committed fold, moon releases them: with an AOF the auto-rewrite monitor folds, and without one a rate-limited snapshot is requested.
  - Such folds or snapshots are spaced at least 10 sweep intervals apart (10 minutes by default).
  - New INFO fields: `cold_held_files_stale_databases`, `cold_held_release_folds_requested`, `cold_held_release_snapshots_requested`.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.88 · Self-evaluation 0.9
