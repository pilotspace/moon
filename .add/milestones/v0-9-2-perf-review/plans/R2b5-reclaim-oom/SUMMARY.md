# R2b5 reclaim-oom — moon#1297: the no-AOF reclaim no longer answers -OOM under steady writes

Branch `w2/r2b5-reclaim-oom` (3 commits on 9d85dbc): 06c3cd1 INFO gauge · f7adcce fix · 00aeab5 test. Binaries `a-final-{monoio,tokio}` (from f7adcce; 00aeab5 changes only a test doc comment). Linux container, not merge bar.

## Root cause (measured)
A no-AOF compaction's record (each survivor's key + both locations, ~110 B) is charged to write admission until a snapshot that started after it commits — minutes without a save rule — and eviction cannot free it. Only a count bounded it (64/db). `evict_to_budget` refuses a write needing room once the ledger passes half the per-shard budget. A gauge-only build hit -OOM 6/6 (3 monoio, 3 tokio) with `cold_reclaim_pending_bytes` at 4.08–4.42 MB (152–176 pending) ≈ 1.0–1.1 MB/shard vs the 1 MB refusal line (2 MB budget / 2). With compactions held (`MOON_TEST_COLD_RECLAIM_HOLD_FILE`): 0/10 (handoff). Pre-fix 2b: -OOM 14/14 loaded, 7/7 quiet. Aborted pre-fix runs spent 0.4–0.6 µs/op on spill threads (2a and fix ≈ 0).

## Fix (`cold_reclaim_tick::no_aof_starts`)
- RAM cap: a shard starts a no-AOF compaction only while its records (+ an average record per job in flight) are under 1/16 of its budget; no cap without maxmemory; ≤2 jobs in flight.
- Spacing: while the shard is spilling (minted a spill file since the last tick), ≤1 start per second; full pace once writes stop.
- `SPILL_MARK` is `take()`n at the top of `run()`, so an early return leaves no stale mark.
- Chosen over: cap only (rps 0.946/0.928, CPU 1.040/1.059 vs 2a, monoio/tokio) — cap + spacing gave 0.993/0.992 rps, 1.013/1.019 CPU; "no start at maxmemory" (never reclaims under the load that needs it); keeping the records out of the eviction target (hides real RAM).

## Perf (quiet box, loadavg 1.5–2.2, 7 interleaved reps, fresh server, `-t set -r 50000 -d 600 -c 16 -P 16 -n 400000`, s4)
| rt | arm | rps median [min–max] | CPU/op µs (shard/spill) | -OOM | compactions |
|---|---|---|---|---|---|
| monoio | 2a fa3f751 | 149,198 [146.7K–157.6K] | 10.35 (9.97/0.00) | 0/7 | n/a |
| monoio | fix | 146,520 [131.4K–150.0K] | 10.68 (10.20/0.00) | 0/7 | 12 |
| monoio | 2b fe6fb20 | aborted | — | 7/7 | 150–190 |
| tokio | 2a | 132,231 [129.3K–141.5K] | 11.97 (11.55/0.00) | 0/7 | n/a |
| tokio | fix | 132,057 [126.6K–140.9K] | 12.22 (11.80/0.00) | 0/7 | 12 |
| tokio | 2b | aborted | — | 7/7 | 160–180 |

Paired fix/2a: monoio rps 0.991 CPU 1.026; tokio rps 0.996 CPU 1.018. The prior session's 66.1K rps was one run on a shared, loaded box (2a measured 43K–86K under the same load; interleaved ratio 0.98–0.99).

## Red → green (`tests/cold_reclaim_no_aof_oom_1297.rs`)
- `a_set_flood_without_an_aof_is_never_refused_by_the_reclaim`: 800K-SET flood, pause until the reclaim stops compacting, then a 64K-SET burst of new keys; every SET `+OK`, records < maxmemory/8 (skipped if the gauge is absent). RED 5/5 on i2b-fe6fb20 monoio (120K–241K refused) and 5/5 tokio (352K–412K); GREEN 5/5 each on the fix (records 623–760 KB total).
- `the_reclaim_frees_disk_after_a_flood_ends` (starvation guard, green on both): unlinked files/bytes > 0 within 90 s after the flood. Dir after a 400K flood (sweeps 2 s): monoio 208 → 131 → 108 → 74 MB at t+0/11/31/51 s; tokio 215 → 130 → 122 → 103 MB.
- Spill dir during the flood equals 2a (max 193 vs 192 MB monoio, 209 vs 208 MB tokio).

## Suites (both runtimes, all green)
cold_reclaim_no_aof_oom_1297, cold_block_reclaim_no_aof_1297 (+ gauge checks), crash_recovery_cold_no_aof, crash_matrix_cold_graves_1281, cold_orphan_sweep, cold_file_id_orphan_sweep_1114, cold_graves_reduced_databases_1291, tiering_no_aof_write_gate_1290, spill_thread_supervision_1265, cold_held_files_release_1289, held_release_txn_open_1289, held_release_txn_race_1289, crash_recovery_cold_del_rewrite; lib storage::tiered 251, shard::persistence_tick 61; fmt, clippy ×2.

## Deferred
- With 1 s sweeps and long floods the dir peaks at ~380–410 MB vs ~209–240 MB for 2a: a requested snapshot holds spill files that die meanwhile (held-compaction runs equal 2a; default 60 s sweeps equal 2a). Reclaim + snapshot-hold design, not the throttle.
- At tiny budgets (8 MB) the cap limits a snapshot cycle to ~10–11 compactions per shard (was 64/db); binds only below ~100 MB maxmemory at 4 shards (estimate).
- Records overshoot the cap slightly (~175 KB/shard vs 128 KB): in-flight jobs counted at the average record size.

## CHANGELOG
- A steady write flood at `maxmemory` on a server with disk offload and no AOF no longer answers `-OOM` (moon#1297): the bookkeeping of compactions waiting for a snapshot counted toward `maxmemory` without a RAM bound. A shard now keeps it under a sixteenth of its memory budget and, while it is evicting to disk, starts at most one compaction a second; once the writes stop the reclaim runs at full pace and frees the old spill files as before. New `INFO` gauge `cold_reclaim_pending_bytes` shows the RAM those records hold.
