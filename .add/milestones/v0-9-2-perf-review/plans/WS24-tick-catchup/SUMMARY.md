# WS24-tick-catchup SUMMARY
Branch `perf/ws24`, base main `273e6bc`. Personas: performance-engineer (lead), routing-dispatch-engineer.
Issue: moon#1280 (the likely root cause of moon#1273's Windows "FLUSHDB took 85ms" flake).
All numbers: **Linux container (4 vCPU x86_64), not the GCE rig, not the merge bar.**

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1280 | FIXED (both runtimes, both monoio drivers) | `58dd986` perf(runtime), `66ee101` perf(shard) | Unit `runtime::interval::tests::{tokio,monoio legacy,monoio io_uring}_fires_one_catch_up_tick_after_a_stall`: 201 catch-up ticks for a 200 ms stall with Burst restored, ≤ 1 after. `shard::tick_cadence::tests` (10). Real server `perf_ws24_tick_catchup::a_stall_costs_one_catch_up_tick_not_a_burst` (1.5 s SIGSTOP, INFO `shard_tick_burst_max` ≤ 3): 1,502 monoio / 1,507 tokio with Burst restored; the field is absent on 273e6bc. Guards `chores_still_fire_after_a_stall` (BLPOP timeout + active expiry still fire) and `instantaneous_ops_per_sec_is_a_live_rate` (red on 273e6bc: always 0). | INFO `expired_keys` is never incremented in production (pre-existing, not fixed here). |

## Root cause
- `src/runtime/tokio_impl.rs:25` and `src/runtime/monoio_impl.rs:26` built every interval with the default `MissedTickBehavior::Burst`.
- Vendored monoio: `Interval::poll_tick` resets a late interval to `timeout + period`, and `time/driver/mod.rs` `reregister` fires an elapsed timer synchronously, so the shard task replayed every missed 1 ms tick without yielding. tokio's coop budget only split the replay into 128-tick chunks.
- Each tick's bounded work (250 µs lazy-free slice, 1 ms/db expiry, snapshot byte budget) ran N times in one piece.
- Latent: the monoio chores were count-based (`monoio_tick_counter % N`); once ticks are skipped a count no longer measures wall time.

## Duty inventory (at 273e6bc)
| duty | period | tokio | monoio | count-based | catch-up before → after |
|---|---|---|---|---|---|
| lazy-free, SPSC drain, WAL append+flush, snapshot advance, checkpoint, CDC, auto-save epoch check | 1 ms | interval :941, arm :1443 | race2 :2187, body :2356–2480, lazy-free :2726 | tick | Burst → 1 tick; per-tick bounds kept |
| blocked-client timeouts | 10 ms | :943 / :1783 | `%10` :2484 | yes | → 1 run (deadline-based: fires every expired waiter) |
| active expiry + MQ triggers | 100 ms | :938 / :1787 | `%100` :2488 | yes | 30 cycles → 1, budget × elapsed/100 ms, capped 4× |
| eviction + memory cascade | 100 ms | :940 / :1815 | `%100` :2508 | yes | → 1 run (acts on the current ledger) |
| WAL fsync, idle-client kill, MVCC sweep, text postings, P6 checkpoint, idle-park read sweep, spin governor | 1 s | :945 / :1672 | `%1000` :2547 | yes | → 1 run (state/timestamp-based) |
| `instantaneous_ops_per_sec` | 1 s | no caller | no caller | — | read 0 forever → shard 0's 1 s chore, delta ÷ elapsed |
| warm check / disk watchdog / autovacuum / cold orphan sweep | various | :960 :980 :1007 :966 (orphan at t=0) | `%` :2635 :2665 :2671 :2695 | yes | → 1 run each; the orphan sweep's first-view timing is moon#1279's (fixed separately) |
| auto-save, non-sharded expiry, gossip, prometheus upkeep, rate-limit cleanup, SSE/memory publishers | 1 s … 60 s | `auto_save.rs:59,224`, `expiration.rs:38`, `gossip.rs:611`, `http_server.rs:815`, `rate_limit.rs:66`, `publishers.rs:32,82` | auto-save: `sleep` loop | no | Burst → Skip; SSE ops/s ÷ elapsed |
| AOF writer, replica, replication master | — | deadline / `sleep` loops | same | no | never burst; unchanged |

## Fix
- `src/runtime/interval.rs`: `tokio_interval` / `monoio_interval` set `Skip`; `TimerImpl::interval` delegates to them; every direct `tokio::time::interval` caller migrated. Root `clippy.toml` disallows the raw constructors (`clippy::disallowed_methods`). No vendor patch: vendored monoio already has `set_missed_tick_behavior`.
- Skip over Delay: both bound a stall to one catch-up tick; Skip keeps the fixed-rate grid.
- `src/shard/tick_cadence.rs`: `Cadence` / `ChoreCadences` make monoio chores due by monotonic elapsed time (one `Instant` per timer tick, immune to wall-clock steps). A run ≥ one period late re-anchors to now + period. Created at loop entry: first run one period after start, as before.
- Active expiry's catch-up sweep scales by elapsed/100 ms, capped at 4×. Every other duty verified state- or deadline-based.
- `instantaneous_ops_per_sec` sampled from shard 0's 1 s chore ÷ elapsed; SSE ops/s ÷ elapsed.
- New INFO: `shard_tick_late_total`, `shard_tick_burst_max`.

## Measurements
(a) Most ticks fired back to back after one 3 s SIGSTOP (debug, `MOON_IDLE_PARK=0`, 3 reps × {1,2} shards): fixed monoio 1,1,1 / 1,1,2 and tokio 1,1,1 / 1,1,1; Burst restored monoio 3,006–3,012, tokio 3,014–3,037.

(b) PING for 10 s after a 3 s SIGSTOP with an 8-hash / 4M-field UNLINK backlog (release-fast, `--shards 1`, interleaved base/new × 3, spread over reps):

| config | build | first PING ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|
| monoio io_uring | base | 177–207 | 0.11–0.13 | 0.40–0.56 | 177–207 |
| | new | 0.71–0.74 | 0.10–0.15 | 0.35–0.60 | 5.8–17.3 |
| monoio epoll | base | 1.9–169 | 0.10–0.12 | 0.32–0.34 | 106–194 |
| | new | 0.32–0.73 | 0.10–0.13 | 0.36–0.45 | 6.2–44 |
| tokio | base | 23–40 | 0.12–0.15 | 0.49–2.68 | 46–88 |
| | new | 0.67–0.88 | 0.10–0.15 | 0.36–0.55 | 7.7–10.8 |

No-stall control max: 5.9–12.8 ms (host noise floor). One epoll set run alongside two clippy builds was discarded (both legs contaminated). The release binaries were built one edit before `66ee101` (only the burst detector's definition and field name differ).

## Gates
fmt --check; clippy --all-targets -D warnings on both feature sets; filtered lib tests 132 (monoio) / 102 (tokio); `perf_ws24_tick_catchup` green on both runtimes (MOON_BIN pinned — an unpinned run picked up another worktree's `target/debug/moon`); related suites green: cold_orphan_sweep, cold_file_id_orphan_sweep_1114, idle_timeout_sweep, busy_poll_idle, tracking_expiry_invalidation_1013, idle_downshift_parity, spsc_wake_floor_red (monoio), subset on tokio. Not exercised: blocking_list_timeout, crash_recovery_orphan_sweep_readiness (all `#[ignore]`).

## Cross-ownership edits
runtime/{interval.rs (new), mod.rs, traits.rs, tokio_impl.rs, monoio_impl.rs}; shard/{tick_cadence.rs (new), event_loop.rs, timers.rs, idle_park.rs (docs), mod.rs}; server/expiration.rs; command/connection.rs (2 INFO fields); admin/metrics_setup/{mod.rs, publishers.rs}; one-line interval swaps in admin/{http_server.rs, rate_limit.rs}, cluster/gossip.rs, persistence/auto_save.rs; root clippy.toml; tests/perf_ws24_tick_catchup.rs.

## Risks
- A saturated loop (ticks > 5 ms late continuously) no longer replays per-tick duties: the snapshot walk and lazy-free drain progress more slowly under saturation (intended; the walk already scales with its pre-image backlog, moon#1228).
- Idle park: a chore may run up to one 10 ms park late.
- The 5 ms grace can register a burst of 2 (test bound 3).
- One `Instant::now()` per fast-path tick (not per key; not while idle-parked).
- Any new monoio chore must use `Cadence`; any raw interval now fails clippy.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.90 · Edge cases 0.90 · Self-evaluation 0.90
