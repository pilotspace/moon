# WS29-perf-followups SUMMARY

Wave 1 of the next round (base main `ce65400`, branch `perf/ws29`, integrated into
`claude/gifted-mendel-e9wiz5`). Issues: moon#1294, moon#1288, moon#1287.

All numbers are from a **4-vCPU Linux container, not the merge bar**, shared with WS27 and WS28 (load average 3–7 during runs). Builds are `release-fast`, `--shards 1`, and every A/B interleaves base and fix.

## Per-issue verdict
| issue | verdict | commits (perf/ws29) | evidence |
|---|---|---|---|
| moon#1294 | FIXED | 74479d1, 5a2892d | The new real-server test `eviction_reason_del_run_budget_1294` fails on `ce65400` tokio (39.6 s stall, 79 victims) and passes on both runtimes. Two unit tests. One evicting write against a stalled writer: 21.5–45.1 s → 0.524–0.526 s. Under a flood, the max PING gap went from 1.0–25.0 s to 1.006–1.010 s. |
| moon#1288 | FIXED | 616bd55, f2a933d, 059fcf7, fc3e627, d568377 | The new test `active_expiry_backlog_drain_1288` fails on `ce65400` (14.9 s for 191K keys) and passes on both runtimes (~0.41 s for 200K). 1.84M keys `PX 4000` after a 6 s SIGSTOP: 7.1–8.4K keys/s → 298–371K keys/s; time to clear went from ~217–258 s (estimated) to 4.95–6.15 s (redis 7.0.15: 3.8 s). Probe p99 36–135 → 286–399 µs; p99.9 up to 1.4 ms; redis during its own drain: p99 1.3 ms. |
| moon#1287 | PARTIAL: SSCAN fixed, HSCAN deferred to moon#1171 | a1ed595, 5a2892d, d568377 | Property test `every_member_present_for_the_whole_scan_is_returned`: the old code fails it (seed 0 skipped 62 members). Full SSCAN of 1M members: estimated 525–602 s → 2.10–2.69 s (redis 3.0–3.3 s). |

## Design
- **#1294:** `record_reason_del_conn` and `record_bytes_conn` take the caller's `&mut Duration`. Each gate mints one `AOF_REASON_DEL_BACKPRESSURE_BOUND` per `evict_to_budget` run: the monoio write gate, the tokio per-command and MQ gates, the script bridge and the inline SET. Past that bound, the remaining DELs fail fast into `AOF_REASON_DEL_DROPPED`.
- **#1288** (`src/server/expire_adaptive.rs`, `shard::timers::run_active_expiry_fast`):
  - A cycle that ends with the head still due latches a backlog flag.
  - The 1 ms tick then runs fast slices from a token bucket that earns 25% of wall time and holds at most 1 ms.
  - Databases are visited round-robin, and deadlines come from the shard-cached clock.
  - The fast path does not run on a replica, and stands down when the AOF channel has fewer than 2048 free slots.
  - `expiration.rs` tests moved out to stay under the 1500-line cap.
- **#1287:** on the full encoding, the SSCAN cursor counts the positions still to visit. Each page walks down from `cursor-1`, O(COUNT). The guarantee holds because SREM, SPOP and SMOVE `swap_remove` (the last member moves down) and SADD appends. Compact sets answer in one call with cursor 0, as redis does. HSCAN stays unchanged, because a std `HashMap` has no stable order.

## Gates
- `cargo fmt` and clippy with `-D warnings`: clean on both feature sets.
- Lib tests: monoio 595 + 796, tokio 558 + 785 (touched modules).
- Integration, both runtimes: the two new tests plus `perf_ws24_tick_catchup`, `review_r2b_lazy_free_under_flood_1280`, `aof_backpressure_reply_1272`, `review_r3_lua_eviction_aof`, `review_r2b_spill_degraded_aof_1265`, `info_observability`: all green.
- `test-consistency.sh`, with `MOON_DISK_FREE_MIN_PCT=0`: 76 failures on WS29 vs 81 on base, and no new failure. The new SSCAN rows fail on base and pass on WS29.
- The hot-path SET/GET benchmark (P16) is within noise (±25% box noise).

## Residual risks
1. HSCAN is still O(N log N) per page, and can still skip a field (870× redis per page at 1M fields). It needs moon#1171. An interim option would be a keyed-hash-order cursor.
2. SSCAN now ignores COUNT on compact sets (as redis does), and pages the full encoding in descending position.
3. `expired_time_cap_reached_count` also counts fast slices: about 1000/s during a drain.
4. #1294: once a run's budget is spent, the remaining DELs are dropped (fail-loud). A pipeline of evicting writes can still pay one bound per write.
5. **Environment:** the disk had about 5% free, and moon's default `--disk-free-min-pct 5` flapped into `diskfull`. Run the scripts with `MOON_DISK_FREE_MIN_PCT=0` on this box.
6. `src/shard/event_loop.rs` was already over the 1500-line cap and grew by 46 lines.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
