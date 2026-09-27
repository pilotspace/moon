# WS25-spill-supervision — SUMMARY

- **Base:** main `273e6bc`. Branch `perf/ws25`, worktree `/home/user/wt/ws25`. Commits `79c7c88` (fix + unit tests), `016720c` (real-server suites).
- **Host:** 4 vCPU Linux x86_64 container shared by other builders. **Not the merge bar** (no moon-dev VM; hosted Windows/MSRV matrix not dispatched by this workstream).
- **Binaries:** release-fast `/home/user/wt/bin/ws25-{monoio,tokio}`; HEAD reference `/home/user/wt/bin/base-monoio`.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1265 | **FIXED** | `79c7c88`, `016720c` | Unit, both runtimes: `spill_thread::supervisor::tests` (6), `spill_thread::supervision_tests` (4, real thread), `persistence_tick::spill_supervise_tests` (5), 2 reworked `spill_thread::tests`. Real server: `spill_thread_supervision_1265` (2) on monoio and tokio. `crash_recovery_cold_del_inflight_1253` with `MOON_TEST_COLD_DEL_SPILL_PANIC=1`: 3 tests × 3 rounds, `--shards 1` and 4, both runtimes — every round respawned its injected deaths and reached the moon#1253 window, 0 wrong probes after kill -9. Red: the base binary fails the new suite; mutation — no reconcile → 4/5 shard tests fail; no degraded fallback → the crash-loop test gets `-OOM`. | Death is observed on the 100 ms eviction tick only: a saturated shard gets transient `-OOM` once the 4096-deep queue fills (1061 of 16,000 pipelined SETs in a hand run; permanent on HEAD). A mid-flight panic re-spills ≤ 256 keys per death. |

## Root cause
`SpillThread::new` gave the spawned thread the only receiver of the request channel. A panic dropped it, every connection's cloned `spill_sender` then got `Disconnected`, and `evict_one_async_spill` kept the key and answered `-OOM` for the life of the process. The dead thread's `spill_inflight` payloads stayed pinned and charged; its reclaim jobs never answered. The #1268 mitigation only abandoned compactions and logged.

## State machine
```
             death, budget left                 due, spawn ok
  Running ─────────────────────────▶ Backoff ──────────────────▶ Running
     │                                  │ ▲
     │ death, budget spent              │ │ due, spawn failed, budget left
     ▼                                  ▼ │
  Degraded ◀─────────────────────────── (spawn failed, budget spent)
```
`RestartSupervisor` (`storage/tiered/spill_thread/supervisor.rs`) is pure (clock from the caller, no atomics): backoff 100 ms × 2^streak capped at 30 s; budget 5 attempts per sliding 10 min (a failed spawn counts); ≥ 60 s of uptime resets the streak, not the budget; a clock stepped back never strands a respawn. Owned by the shard behind a `parking_lot::Mutex` locked briefly on the tick, never across `.await`.

| state | owner | on death |
|---|---|---|
| request / completion / reclaim channels | `SpillThread` keeps the thread-side ends, each incarnation gets clones | nothing disconnects |
| queued requests | channel | re-queued in order with the SAME ids (no file carries them) |
| requests the dead thread held | died with it | payloads rehydrated into RAM; superseded entries forgotten |
| completions sent before the panic | channel | applied by the drain; death sampled BEFORE the drain (moon#1253 review-5 rule) |
| `done_below` | shared `Arc` | kept; only moves up |
| reclaim jobs | channel / thread | queued dropped; compactions abandoned (not given up) |
| files written, never announced | disk, unlisted | startup orphan sweep (moon#1114); no id reissued (moon#1067) |

## Fix
- `catch_unwind(AssertUnwindSafe)` around the whole thread body; payload logged when the shard reaps the thread. No profile sets `panic="abort"`.
- Reconcile (`shard/persistence_tick/spill_supervise.rs`): take queued requests; on restart re-queue them, on degrade drop them; every in-flight record not re-queued is rehydrated through the failed-write guards; `spill_superseded_retain(|id| live.contains(id))` rebuilds the moon#1253 sets. SWAPDB/FLUSHALL-safe, idempotent.
- Rehydrate rather than re-send (one guarded path; the AOF record that authorized the eviction still describes the value).
- Respawn after `cold_reclaim_tick::run`; `try_submit_reclaim` refuses while no thread runs.
- Degraded: channel closed; `evict_to_budget`'s async arm takes the no-spill path on `sender.is_disconnected()` (evicting policies plain-drop with the DEL reported, `noeviction` OOMs); cold reads (`cold_read_pool`) unaffected; WARN.
- INFO: `spill_thread_alive` now means "no shard's thread is down right now"; new `spill_thread_restarts`, `spill_thread_degraded`, `spill_thread_rehydrated`. Metrics: `moon_spill_thread_deaths_total`, `moon_spill_thread_restarts_total`, `moon_spill_threads_degraded`. Hook: `MOON_TEST_SPILL_PANIC_FILE` (`start`, `after-write`, `after-send`, `reclaim-write`, optional `once`).

## Measurements
- Reconcile per death (`--shards 1`): one mid-flight panic `requeued=3794 rehydrated=256`; crash loop 5 restarts (`requeued` 862 → 4096, `rehydrated=256` each), the degrading reconcile `rehydrated=4352`, `pending_spill_bytes` back to 0; ~3.1 s of backoff to degrade.
- Transient `-OOM` while dead (16,000 × 600 B pipelined SETs, 8 MiB maxmemory): 1061 with the panic, 0 without; HEAD refuses permanently.
- moon#1253 with the panic injected (superseded completions / respawns per round): monoio s4 del [18,47,19]/[2,2,2], flushall [256,1157,2825]/[2,2,2], overwrite [23,9,2]/[1,2,2]; monoio s1 del [27,24,7], flushall [1536,2560,4096], overwrite [18,17,20]; tokio s4 del [14,3,3], flushall [1024,2048,512], overwrite [1,5,22]; tokio s1 del [14,35,18], flushall [5120,1280,1280], overwrite [13,14,15] (2 respawns per round except where noted).
- Hot path: one `is_disconnected()` load per eviction victim (async arm only); nothing on the command path.

## Gates
fmt --check; clippy --all-targets -D warnings on both feature sets; `cargo test --lib -- storage::tiered shard:: storage::eviction storage::db` 870 (monoio) / 859 (tokio); unwrap/unsafe/tempdir audits pass. Integration (both runtimes): `spill_thread_supervision_1265`, `crash_recovery_cold_del_inflight_1253` default and panic mode at s1/s4, `crash_recovery_spill_batch_kill9`, `cold_orphan_sweep`, `spill_inflight_visibility`; monoio only: `inline_write_spill_gate_660` (15). `cold_file_id_reuse_1067` failed identically on base (moon#1279, fixed on the orchestrator branch).

## Cross-ownership edits
`src/shard/persistence_tick.rs` (outside the moon#1281 hunks: `respawn_if_due` after the reclaim tick; `take_prune(done_below)`; `spill_supervise::after_drain` replaces `report_death_once`; `prune_superseded(Option<u64>)`; module decls), `persistence_tick/reclaim_offload_tests.rs` (fixtures `pub(super)`), `storage/eviction.rs` (one `is_disconnected()` arm), `storage/db/mod.rs` (`spill_superseded_retain`, `spill_inflight_records`), `command/connection.rs` (3 INFO fields), `admin/metrics_setup/recorders.rs` (3 recorders).

## Risks
1. Death observation is tick-bound (100 ms eviction tick): transient `-OOM` on a saturated shard until the reconcile runs.
2. Admission while in backoff: up to one queue above budget for ≤ 30 s (same bound as a slow spill thread).
3. `spill_thread_alive` changed meaning: alert on `spill_thread_degraded > 0` or the rate of `moon_spill_thread_deaths_total`.
4. Degraded is terminal until restart (no admin reset).
5. `shutdown()` in backoff flushes nothing still queued (AOF-backed, as in a crash).
6. No loom model: single-owner machine under a mutex; death detection keeps the existing `is_finished` + `Acquire` fence.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
