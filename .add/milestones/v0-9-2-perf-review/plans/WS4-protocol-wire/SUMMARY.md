# WS4-protocol-wire SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws4-protocol-wire`, 12 commits over `a925e64`. Design
> notes and measurement method: `NOTES.md` in this directory.

## Per-issue verdict
| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| moon#1164 multibulk re-scan | **FIXED** | f922aca · 48226b2 (replica, cross-ownership) | RPUSH in 64 KiB writes, `--shards 1`, 3 reps: 1M **15.7–22.7 s → 121–126 ms** (redis 112–126 ms); 300K 1.30–2.26 s → 33–43 ms; 100K 167–182 ms → 10–22 ms (linear). Tests: `protocol::flat::tests::{every_prefix_resumable_agrees_with_two_pass, chunked_resumable_agrees_with_two_pass, chunked_upload_work_is_linear, stale_cursor_never_builds_a_wrong_frame, production_thresholds_and_reset, pending_len_hint}`, `codec::decode_frame_resumes_a_large_frame_across_reads`, `apply::large_frame_across_reads_resumes_and_accounts_whole_frames`, `util::hinted_read_len_grows_geometrically_and_respects_the_ceilings`, `tests/perf_ws4_multibulk_linear.rs` (8× input: 7.8× time on the branch; **60.6×** on base, fails: 125.7 ms → 7.62 s), `tests/perf_ws4_wire_e2e.rs::large_multibulk_in_64k_writes_is_answered`. New fuzz target `resp_parse_resumable` in both fuzz.yml matrices. | `shard/uring_handler.rs` (tokio io_uring path) still calls the stateless `parse`: tri-state halving but no resumption |
| moon#1179 wire-path items 1–7 | **FIXED** | c79fd03 (1) · 456ab25 (2) · 2fc54ea + 0293360 (3) · 3d96c78 + 1693ade (4) · 0bce56b (5) · b41fc9a (6) · 899bfaa (7) | `SET key:__rand_int__ xxx EX 100 -P 16` **+8.2%** (252.7/246.5/254.3K → 274.5/277.7/263.0K; redis 290.7K). `-d 65536 -t set` **+60%** (14.3/14.3/15.8K → 24.0/22.9/24.3K; p50 2.5–2.9 → 1.4 ms; redis 17.4K). Server CPU per request: P1 −5.9%, P16 −11.0%, SET EX P16 −7.8%. Deferral-heavy pipeline `--shards 4`: server CPU −29%, wall −22%. `size_of::<Frame>()` **72 → 40**. Tests: `framevec_is_an_unboxed_vec_and_adopts_without_copying`, `frame_size_measurement`, `verbatim_encoding_tag_round_trips_byte_identical`, `downshift_releases_only_empty_scratch`, `test_pool_creates_slots_lazily_per_target`, `spare_read_target_covers_the_spare_capacity`, `spare_read_target_packs_a_shared_tail_instead_of_reallocating`, `io_buf_shrink_has_hysteresis`, `tests/perf_ws4_wire_e2e.rs` (5 incl. `p1_collection_writes_do_not_pin_a_buffer_per_read`), `spilled_frames_reparse_identically_ahead_of_the_remainder`, `tests/perf_ws4_deferral_carry.rs` (3, both runtimes), `fused_header_parse_matches_the_two_step_parse`, `inline_args_alias_one_frozen_line`. Item 7: clippy-clean with the blanket `#![allow(dead_code)]` removed. | risks 4 and 9 |

## Measurements
Shared 4-vCPU container (load 3–5 from other agents), release-fast. Baseline
`/home/user/wt/bin/baseline-935c555`, final `/home/user/wt/bin/ws4-1693ade` (symbols verified),
redis-server 7.0.15. `--shards 1 --appendonly no --save "" --maxmemory 0 --disk-offload disable`
unless noted. Each rep runs baseline → ws4 → redis.
- **RPUSH sweep** (64 KiB writes, concurrent 1 ms PING probe): 100K baseline 171.3/166.7/182.0 ms · ws4 22.3/9.5/12.5 · redis 15.2/15.7/9.5. 300K 2264.1/1392.0/1295.0 · 39.5/32.9/42.9 · 37.4/37.9/32.1. 1M 22723.5/15840.2/15700.0 · 120.9/121.5/126.4 · 112.1/126.4/112.2. Max PING during 1M: baseline 246/194/160 ms, ws4 118/105/110, redis 49/47/47 — the baseline's ~875 re-scans each blocked the shard 100–250 ms; the ~110 ms left is RPUSH execution itself (list code, WS3's area).
- **SET EX P16** (`-P 16 -r 1000000 -n 2000000 -c 50`): baseline 252.7/246.5/254.3K, ws4 274.5/277.7/263.0K, redis 290.7K.
- **64 KiB SET** (`-d 65536 -t set -n 60000 -c 50`): baseline 14.3/14.3/15.8K, ws4 24.0/22.9/24.3K, redis 17.4K (first round under lower load: 2.15×).
- **Server CPU µs/request** (utime+stime, fixed count, 3 reps): P1 7.425/7.800/7.200 → 6.975/6.900/7.225; P16 1.345/1.295/1.410 → 1.165/1.240/1.200; SET EX P16 2.930/2.740/3.040 → 2.560/2.790/2.680. Raw P1/P16 small-command throughput within noise (same binary ±25%).
- **Deferral-heavy** (`--shards 4`, 30 × 500 × [SET; PING], ~11.2K deferrals): server CPU 2410/2340/2430 → 1680/1770/1650 ms; wall 3337/3155/3091 → 2269/2896/2343 ms.
- **Memory:** 30K one-at-a-time SADDs RSS +4.7 MB baseline vs +5.2 MB final (the pre-1693ade cut was +107.2 MB — caught and fixed). Pipelined 100K × (SET 4 KiB; SADD) +557.7 vs +560.1 MB — moon#1160's pinning, WS10's.

## Cross-ownership edits
- 48226b2 `src/replication/replica.rs`: both replica loops keep a `ParseState` and call `drain_replicated_commands_resumable`.
- 0293360 `src/server/response_slot.rs`: `ResponseSlotPool` internals lazy via `OnceCell`; API unchanged.
- 456ab25 `src/admin/console_gateway.rs`: one `*b"txt"` literal forced by the `Frame` layout.
- `src/server/conn/blocking.rs`: only the two inline-GET `split_to` sites the plan assigns to WS4.

## Risks / things the orchestrator must re-check at integration
1. **Wave-2 adjacency in `handler_monoio/mod.rs`:** the inline block's outer `if` gained `!frames_carried &&`; two lines around `try_inline_dispatch_loop` reset the codec's parser state; all-inlined and batch flushes use `flush_write_buf_bounded!`; RESP3 subscriber / MONITOR / tracking select arms gained a 3-line buffer resize; the migration block writes a carried tail back into the read buffer. No `can_inline_*`, ACL, PAUSE, AOF, metrics or remote-dispatch lines changed. Tokio side: read arm, batch seeding, deferral block, a reset after the subscriber step, buffer shrink.
2. **`ParseState` contract:** code that consumes or prepends to the front of a read buffer outside the parser must call `reset`. A missed reset costs latency (a false "incomplete"), never a wrong frame.
3. **`Frame` is 40 bytes:** code building a `VerbatimString` from `Bytes` needs `*b"txt"`.
4. **moon#1160:** inline-protocol arguments now alias their line buffer like RESP arguments alias their frame — WS10's storage-side copy must cover both.
5. `shard/uring_handler.rs` is not resumable.
6. Fuzzers were not executed (no nightly/cargo-fuzz here) — run the `ci-fuzz` label.
7. f922aca's message quotes 60.3× / 7.66 s (measured by running the test binary directly); the `cargo test` figures are **60.6× / 7.62 s**.
8. Tests: monoio lib 6040 passed, 1 failed (root-only `cold_index_rebuild_tests::unreadable_file_is_counted_and_skipped_never_queued_for_unlink`); tokio `protocol::`/`server::`/`replication::apply` 457 passed.
9. Pre-existing, unchanged: a batch with both a deferral and a protocol fault reports the fault and closes before the deferred tail runs.
10. `parse.rs` 2086 → 1750 lines; its ~720-line test module keeps it over the 1500-line guideline.

## Checks run
fmt; tokio `check --lib`; `clippy --lib -D warnings` ×2 runtimes; `check --lib --tests --benches`;
unsafe audit 0 new (252/252 SAFETY); unwrap audit 0. Integration, monoio, all green:
perf_ws4_multibulk_linear, perf_ws4_wire_e2e, perf_ws4_deferral_carry,
pipeline_cross_shard_ordering, migration_batch_tail, idle_downshift_parity, parked_idle_parity,
tls_idle_downshift_parity, acl_inline_read_enforcement, multi_queues_inline_get,
inline_line_termination, client_tracking_invalidation, monitor_command_feed, pubsub_resp3_push,
subscriber_client_state, pubsub_burst_delivery, resp3_verbatim_parity, batch_protocol_version.
Tokio: perf_ws4_deferral_carry, perf_ws4_wire_e2e, pipeline_cross_shard_ordering,
migration_batch_tail, perf_ws4_multibulk_linear.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.90 · Practicality 0.92 · Optimization 0.91 · Edge cases 0.92 ·
Self-evaluation 0.93. Short of 1.0: fuzzers not executed, uring path not resumable,
small-command wins judged by CPU/request on a noisy box.
