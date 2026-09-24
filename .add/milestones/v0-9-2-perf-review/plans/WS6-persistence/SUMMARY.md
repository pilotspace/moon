# WS6-persistence SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws6-persistence`, base `a925e64`. Measurement binary
> `/home/user/wt/bin/ws6-persistence-a` (release-fast, built+copied in one command, verified by
> branch-unique strings, source identical to final HEAD) vs `/home/user/wt/bin/baseline-935c555`.
> One release-fast build used (budget 2).

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1187 AOF append path | PARTIAL — every Must; staging buffer (Should) DEFERRED | 0d048b0, c440921 | Exactly 1 allocation per record (`tests/perf_ws6_aof_record_alloc.rs`); controlled red run with HEAD's sources: SET 3 / SET EX 6 / EXPIRE effect rewrite 6 allocations — all three fail on HEAD. `encode_matches_head_for_corpus`: output byte-identical to a verbatim copy of HEAD's encoder; `batch_buf_tests` pass. Throughput SET EX +6.3%, HSET +3.2% (noise ±5%). | Staging buffer needs every `send_append_group` / `try_send_append_durable` call site in `server/conn/**` + flush points before `AofFold` / `fsync_barrier`; changes PerShard framing, TopLevel SELECT injection and the #455 per-record epoch filter — design in NOTES.md → WS7 (wave 2) |
| moon#1188 WAL rotation fsync | FIXED | 63f5e0a | 6 tests in `segment/rotation_tests.rs` (5 fail with only the fix reverted); 2 new loom models pass under `--cfg loom` and fail when ordering is weakened to Relaxed. Max PING during a 1 KB SET flood 38.9/19.2/26.0 → 9.5/7.6/5.6 ms. | Segment preallocation not done |
| moon#1185 rewrite deep copy | PARTIAL — interim Must done; incremental COW fold (Should) DEFERRED | 0d9f87f | Output byte-identical to HEAD's clone + `save_snapshot_to_bytes`. Peak RSS growth during BGREWRITEAOF, 1.5M keys: +567/+559/+571 MiB → +4/+9/+6 MiB (4 shards ~+510 → +12/+23 MiB). Rewrite wall 2.2–2.6 s → 0.6–0.75 s. **Shard stall unchanged** (max PING 400–481 → 476–538 ms): serialization is as slow as the old clone. | Incremental fold blocked by moon#1217 (COW captures only the first key) and moon#1216 (split data loss) |
| moon#1186 BGSAVE | FIXED | 13ec480 | Block encoding byte-checked against HEAD's encoder; streamed file == in-memory file. 1.5M keys: max PING stall 1334/1134/1376 → 5/10/12 ms; peak RSS growth +257 MiB → +1–2 MiB. | moon#1216 (pre-existing split data loss) |
| moon#1181 CDC.READ | FIXED | 1c604b2, 3a49f1c (cross-ownership) | HEAD's reader kept verbatim as the oracle; replies byte-identical. Tail poll: ~100K records 551–583 ms → 0.15 ms; ~1M 5765 ms → 0.17 ms (first poll before a position hint exists: 45 ms). PING on the same shard: up to 5.8 s → ≤ 36 ms. | Reader still stops at a gap in the segment chain (same as HEAD) |

## Found (filed)
- moon#1216 **P0** — BGSAVE drops pre-snapshot keys when a DashTable segment splits mid-epoch (1744/2000 in `split_probe_repro.rs.txt`); pre-existing, unchanged by this branch.
- moon#1217 — BGSAVE COW captures only `command[1]` of multi-key writes.

## Measurements
`bench_ws6.py <scenario> <binA> <binB> [reps] [arg]` (scenarios rewrite, bgsave, cdc, walrot, aof): fresh server + data dir per rep, A/B alternating, 2–3 reps. This VM disk's fsync is nearly free, so #1188/#1186 stall savings are a LOWER bound of a network disk's and group-commit effects under `always` are not measurable here.

## Cross-ownership edits
1. `src/shard/dispatch.rs` (inside 0d9f87f): `AofFoldSnapshot.dbs` → `image: FoldImage` (sole producer/consumers are WS6 files; a split commit would not compile).
2. 3a49f1c: `handler_monoio` / `handler_sharded` `dispatch.rs` make `try_handle_cdc_read` async; each `mod.rs` call site gains `.await` (WS4 owns those read loops — expect a one-token conflict at most).

## Risks / things the orchestrator must re-check at integration
1. moon#1216 split data loss (pre-existing).
2. moon#1185 rewrite stall remains O(dataset).
3. tokio AOF writers use an 8 MiB `BufWriter`; a flush at the end of each batch keeps the SIGKILL loss bound under everysec/no at the old 8 KiB.
4. WAL rotation is asynchronous: the segment number advances one tick later.
5. CDC.READ keeps a 64-entry position-hint cache; a corrupt record in a segment entirely below the consumer's cursor no longer blocks it forever (it did at HEAD).
6. WS8 must merge around the `AofFoldSnapshot` change.

## Test results
Unit: 766 monoio + 779 tokio in persistence/CDC/persistence-tick modules, green. Integration: 21 monoio (MOON_BIN-pinned) + 18 tokio via `cargo test`; only failures: `aof_fold_exactly_once_455` (fails on the baseline binary too; `#[ignore]`d in CI — moon#1134), `t43` (needs graph; absent on tokio), tokio `heals_one_shard` flaked once under load and passed 4/4 alone. Every in-process suite was re-run after `touch src/lib.rs` so it linked this branch's lib (shared-target aliasing). Gates: fmt, clippy ×2, `check --lib --tests` ×2, unsafe audit (0 new), unwrap audit — pass. Servers shut down.

## Self-evaluation
Completeness 0.85 · Clarity 0.9 · Practicality 0.9 · Optimization 0.85 · Edge cases 0.9 · Self-evaluation 0.9 — below 0.9 because of the two deferrals (staging buffer needs WS7's files; the incremental fold would risk exactly-once until moon#1216/#1217 are fixed).
