# R1-fix-b SUMMARY

Wave 2a, R1 review area 2 (AOF persistence). Branch `w2/r1-fix-b`, base `f766fc2`. Linux container, not the merge bar.
Binaries: `/home/user/wt/bin/r1fixb-v1-{monoio,tokio}`, release-fast, built at `110ded9`. The commits after that change tests only. Marker strings `Asynchronous AOF fsync is taking too long` and `was appended to` are present in both and absent from `r1-f766fc2-*`.

## Per-issue verdict

| finding | verdict | commit | evidence | follow-ups |
|---|---|---|---|---|
| 2, BLOCKER (pre-existing): db-0 writes replayed into the previous run's last db | FIXED | `c1e4a13`, plus test follow-up `951e3a5` | New suite `tests/aof_select_after_restart_r1.rs` (s1/s4 × graceful/kill -9). Red on `r1-f766fc2`: 8/8 tests fail, 64/64 keys in the wrong db each. Green 4/4 on each runtime. Unit test `record_ctx::a_reopened_stream_selects_its_first_records_db_even_db_0` | — |
| 1, MAJOR (moon#1283): downgrade then re-upgrade loses keys | FIXED (option a, plus a forced rewrite) | `757f20e` | New leg `moon_1283_downgrade_then_reupgrade_{s1,s4}` in `tests/aof_replay_clock_1283.rs` (`MOON_DOWNGRADE_BIN` = `lane-b-base-*`). Red on r1: monoio 364/364 (s1) and 215/364 (s4) live keys missing; tokio 351/351 and 351/351. Green: 0 missing on both runtimes and shard counts, also on one more restart after the forced rewrite. Suite 13/13 on each runtime. Unit tests below | Residual window, see Risks |
| 3 + 4, MAJOR/MINOR (moon#1266): silent stalled fsync; `aof_delayed_fsync` semantics | FIXED | `34d3a8e` | New suite `tests/aof_fsync_stall_r1.rs`: 8 s gated fsync, s1/s4, 4/4 green on each runtime. r4 shape numbers below | — |
| 6, MINOR: failed post-fold fsync not retried | FIXED | `7e15e03` | Unit test `fsync_agent::a_failed_boundary_fsync_is_recorded_and_retried_soon` | — |
| 7, MINOR (pre-existing): fsync-failure status self-heals | FIXED | `d0bd3f4` | Adjusted `a_failed_fsync_is_retried_without_new_writes`; new `an_inline_retry_heals_only_after_a_new_write` | redis-style `-MISCONF` refusal is out of scope (orchestrator files it) |
| 8, MAJOR (pre-existing): `CONFIG SET appendfsync` not applied | FIXED (plumbed) | `0b1bbca` | `config_set_appendfsync_reaches_the_writers_{s1,s4}` in `aof_fsync_stall_r1.rs`. r10 shape numbers below. Unit tests `a_runtime_policy_switch_drains_then_restarts_the_agent`, `runtime_fsync::*`, `config_set_appendfsync_rejects_what_redis_rejects` | — |
| 5, MINOR perf: slow client keeps the writer warm-polling | FIXED | `f8f353a` | `r5` numbers below. Unit test `poll::a_trickle_parks_between_records_and_a_stream_stays_warm` | WS46 removes the warm poll |
| 10, NITs | FIXED | `6734c6f`, test hardening `74ba22e` | Unit test `an_agent_that_unwinds_mid_fsync_does_not_stick_the_handoff` | — |
| 9, docs | FIXED (production guide) | `110ded9` | Guide now gives "9 of 240" for the final binaries and "16 of 360 across both tokio runs" | README.md:280/473 still needs the WS40 replacement text with the same correction (orchestrator) |

### Mechanisms
- **Finding 2.** `RecordCtx::appending()` starts the stream at an unknown db (`UNKNOWN_DB = usize::MAX`, redis's `aof_selected_db = -1`), so the first record always carries a `SELECT`.
  - Used by all four writer loops. The rewrite and overflow drains share that context, so they are covered too.
  - A committed generation switch still resets the context to db 0.
- **Finding 1.** The replay judges a tail an older binary appended by the file's mtime.
  - The rule applies per AOF file (`pin_replay_clock_to_log`). When a stamp is read that the mtime pin exceeds by more than `FOREIGN_TAIL_TOLERANCE_MS` (1 s), the file's last well-formed stamp is found once by a backward scan (`replay/log_tail.rs`).
  - The scan reads only what follows that stamp, and never runs on a file without stamps.
  - Records covered by that stamp are judged by `max(stamp, pin)`.
  - An mtime earlier than the last stamp (the #1283 case) never engages the rule. The unit test `an_mtime_earlier_than_the_last_stamp_keeps_the_stamps` covers this, and the touchback probes still pass.
  - `clock::foreign_tail_replayed()` makes `main.rs` force one boot-time rewrite (the moon#914 path). Once this binary appends behind the old tail, it is no longer the tail, so the rewrite is what protects later boots.
  - `docs/STORAGE-FORMAT-V1.md` §3.3 is corrected.
  - Unit tests: `a_tail_appended_after_the_last_stamp_is_judged_by_the_mtime` and `only_the_records_after_the_last_stamp_form_the_tail` fail with the rule disabled. The earlier-mtime and within-tolerance tests guard the other side. `log_tail::*` covers chunk straddle, malformed/torn stamps and the framed layout.
- **Findings 3 + 4.**
  - The writer logs redis's "Asynchronous AOF fsync is taking too long (disk is busy?)" while an fsync has been in flight ≥ 2 s, at most once per 2 s process-wide. The agent WARNs when such an fsync completes.
  - New INFO fields: `aof_pending_bio_fsync` (writers with an fsync in flight) and `aof_fsync_in_flight_ms` (age of the oldest). Each agent has an in-flight slot in a registry locked only at agent start/stop and by INFO.
  - `aof_delayed_fsync` now counts once per 2 s the fsync stays in flight while a deadline waits (redis's cadence).
  - The `FsyncHandoff` state machine is untouched, so the loom model needs no change.
- **Finding 7 (heal rule).** A writer learns of a failure at its next Owned claim, or directly on the inline path. Only a write batch issued after that makes a later successful fsync "healing", and only a healing success clears the `err` bit; the heal flag rides with the agent job. `inline_done()` records the status itself, so the four inline sites no longer do.
- **Finding 6.** `EverysecSync::boundary_fsync_failed()` records the failure and backdates the deadline so the retry comes within about 100 ms.
  - Applied to the monoio per-shard post-fold drain/fsync, which also covers a drain error that used to be ignored silently.
  - Applied to the overflow-drain boundary-fsync error in the other five writer arms, which had the same gap.
- **Finding 8.**
  - `aof/runtime_fsync.rs` holds a process-wide override (one relaxed load; unset until a `CONFIG SET`), read by `AofWriterPool::fsync_policy()` and by each writer loop once per wake.
  - `EverysecSync::set_policy()`: leaving everysec joins the agent after its in-flight fsync (redis's `bioDrainWorker`); entering everysec starts one.
  - `group_commit::batch_needs_fsync()`: a batch holding an `AppendSync` is fsynced before its acks whatever the writer's current policy, which closes the race in both directions.
  - `CONFIG SET` validates the value and rejects anything but `always|everysec|no`.
  - A BGREWRITEAOF in progress is unaffected: fold drains already fsync before resolving `AppendSync`, and the switch applies at the writer's next wake.
- **Finding 5.** The writer is warm only if the message it just received arrived within two poll steps (1 ms by default) of the receive starting. `on_message` no longer grants warmth.
- **Finding 10.**
  - A `SettleOnUnwind` guard in the agent marks the agent dead (Release) and then calls `finish(false)`. The writer's next claim acquires that, refuses to dispatch, drops the agent and fsyncs inline from then on.
  - The warm-poll sleep is clamped to the remaining span.
  - An unparsable `MOON_AOF_WARM_POLL_US` logs one WARN.
  - `MULTI; MOON.TS 1; SET a 1; EXEC` is pre-existing and not changed. The pre-WS37 binaries `lane-b-base-*` also QUEUE it and run EXEC. `FOOBAR 1` is refused at queue time, so the queuing looks specific to `MOON.*` names.

## Measurements

**r4 shape** (`r4_gate.sh`, s1, 8 s gate under 100 SET/s):
- Base r1: `aof_delayed_fsync:1` from t=2 to t=8, 0 WARN lines, status ok.
- Fix, both runtimes: `aof_pending_bio_fsync:1` throughout; `aof_fsync_in_flight_ms` rising 555 → 7710; `aof_delayed_fsync` 0, 0, 1, 1, 2, 2, 3, 3; 3 WARN lines.
- SHUTDOWN while the gate is held hangs until release on both base and fix (pre-existing inline final sync).

**r10 shape** (gate held 3000 ms, `CONFIG SET appendfsync always`, then `SET`):

| binary | monoio s1 | monoio s4 | tokio s1 | tokio s4 |
|---|---|---|---|---|
| r1 | 12 ms | 11 ms | 11 ms | 10 ms |
| fix | 3173 ms | 3024 ms | 3112 ms | 3016 ms |

**r5** (monoio s1, writer-thread counters over 5 s, 3 interleaved reps, same box while other lanes built; relative only):

| binary | slow client (1 SET/4 ms): vol_cs / ticks | idle: vol_cs | busy p1 c50: vol_cs / rps |
|---|---|---|---|
| base | 1509–1565 / 3–6 | 104–105 | 560–943 / 61–110K |
| r1 | 7306–8010 / 22–25 | 5 | 2818–3386 / 82–106K |
| fix | 1119–1160 / 5 | 5 | 2729–3406 / 81–109K |

The fix brings the slow-client case back to base level, keeps r1's idle win, and leaves busy-writer behaviour unchanged within noise.

## Gates (exit codes, Linux container)
- `cargo fmt --check`: 0.
- `cargo clippy --all-targets -D warnings`: 0 on monoio and on tokio (re-run after the last commit).
- `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: 0.
- `cargo test --release --lib -- persistence command::config`: monoio 1058 passed; tokio 1056 passed after `951e3a5`. That commit fixed my own regression in the tokio-only test `tokio_per_shard_writer_latches_after_torn_write`, which now expects the new SELECT 0 frame.
- Integration, `--include-ignored --test-threads 1`, `MOON_BIN` pinned to `r1fixb-v1-*`. Tokio suites were compiled with tokio features. All exit 0 on both runtimes:
  - aof_select_after_restart_r1, aof_fsync_stall_r1, aof_replay_clock_1283 (13/13), aof_everysec_kill9_1266 (10/10, 20 reps)
  - aof_everysec_backpressure_769, aof_backpressure_reply_1272, aof_multidb_kill9, aof_fsync_err_subscribe_ordering
  - aof_append_status_heals_on_rewrite, default_config_aof_backpressure_838 (tokio runs 0 tests), single_handler_aof_order_1099 (monoio runs 0), crash_matrix_per_shard_aof
  - legacy_aof_rewrite_on_boot_914 (monoio runs 0), wal_kv_db_context_1039
- **Not run:**
  - The loom model: `fsync_handoff.rs` is unchanged, and running it needs a separate `--cfg loom` target dir.
  - The libFuzzer target: no nightly toolchain here.
  - None of the known failures from the brief was hit.

## Cross-ownership edits
- `src/main.rs`: about 20 lines, the forced rewrite after a foreign tail.
- `src/command/config.rs`: `CONFIG SET appendfsync` is now live and validated.
- `src/command/connection.rs`: two INFO fields.
- `tests/aof_everysec_kill9_1266.rs`: the held-fsync test window grew from 2.5 s to 3.5 s, because the first `aof_delayed_fsync` count now comes after 2 s in flight.
- Docs: `docs/STORAGE-FORMAT-V1.md` §3.3 and `docs/production-guide.md`.
- `writer_task.rs` grew from 2079 to 2132 lines (already over the cap). New logic went into new modules: `runtime_fsync.rs`, `replay/log_tail.rs`, and additions to `fsync_agent.rs` (now 974 lines).

## Risks / things the orchestrator must re-check at integration
- **Finding 1 residual.** If the re-upgraded server crashes before its forced rewrite commits, the older binary's tail is no longer at the end of the file, and the next boot judges it by the stale stamp again. This is documented in §3.3, with the advice to run BGREWRITEAOF if the boot log shows no completed rewrite.
- **Finding 1 price.** An mtime moved more than 1 s forward, or a writer of this binary stalled more than 1 s just before a crash, re-judges the file's last clock tick by the mtime (pre-#1283 behaviour for those records only) and triggers one extra rewrite at boot.
- **Every AOF restart now writes one SELECT record** before its first write, including SELECT 0. Any byte-exact test of a reopened incr needs the extra frame, as `951e3a5` shows.
- **`aof_delayed_fsync` changed meaning** to redis's 2 s cadence. `aof_pending_bio_fsync` counts writers, so it can exceed 1 at `--shards` > 1.
- **The appendfsync override is process-wide**, like the LFU parameters: embedded multi-server processes share it. Unit tests never set it.
- **After a failed everysec fsync**, `aof_last_fsync_status` now stays `err` until a write batch issued after the failure is fsynced. An idle server stays `err` until its next write.

## CHANGELOG bullets (ready to paste)
- **Fixed (persistence) (R1 review, pre-existing):** after a restart, writes to db 0 no longer replay into the db the previous run's AOF ended in. A reopened AOF writer starts with an unknown db (as redis does), so its first record always carries a `SELECT`. This affected every layout and both graceful and kill -9 restarts.
- **Fixed (persistence) (R1 review, pre-existing):** `CONFIG SET appendfsync` now takes effect at once, as in redis. Before, it answered `OK` and `CONFIG GET` showed the new value while the writers kept their startup policy, so writes were acknowledged without the fsync `always` promises. Leaving `everysec` first waits for an in-flight background fsync. Invalid values are rejected.
- **Fixed (persistence) (R1 review, pre-existing):** after a failed `everysec` fsync, `aof_last_fsync_status` no longer returns to `ok` on a retry with nothing new written. It clears only when a write issued after the failure has been fsynced.
- **Adjustment to moon#1283:** a tail an older binary appended after a downgrade is judged by the log's modification time on re-upgrade (not by the stale last `MOON.TS`), and boot runs one AOF rewrite to fold it. Keys the older binary saw expire and restarted are no longer lost. STORAGE-FORMAT §3.3 is corrected.
- **Adjustments to moon#1266:**
  - A stalled `everysec` fsync is loud again: redis's "Asynchronous AOF fsync is taking too long (disk is busy?)" log line, new INFO `aof_pending_bio_fsync` and `aof_fsync_in_flight_ms`, and `aof_delayed_fsync` now counted redis's way (once per 2 s of an ongoing stall).
  - A failed post-rewrite fsync is recorded and retried within about 100 ms.
  - A slow client no longer keeps the monoio AOF writer warm-polling.
  - The fsync agent cannot leave its hand-off stuck if it unwinds.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9

Everything is at or above 0.9. The two stated limits on edge cases are the Finding 1 crash-before-rewrite window and the touch-forward price above. The only fully robust answer is a manifest-recorded tail cut, which is too heavy for this wave.
