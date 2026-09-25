# WS16-snapshot-capture — PLAN (wave 2, part 4 — base: int/part3b @ 4115798, which holds parts 3a and 3b)
personas: `.add/personas/storage-durability-engineer.md` (lead) · `.add/personas/ci-test-integrity-engineer.md`

Read first: `plans/WS12-snapshot-integrity/NOTES.md` (COW epoch design, capture-site audit table, the `capture_two_db` design at ~L117-160).

## Issues
1. **moon#1228 (BUG, snapshot) — writers that bypass pre-image capture under an armed BGSAVE epoch** (the MSET coordinator leg is WS8's — not yours):
   - MOVE and `COPY … DB n`: `src/command/keyspace/move_cmd.rs:~289` (`TwoDbOp::apply`) and `src/scripting/bridge.rs:~697-705` — implement `snapshot_cow::capture_two_db` per the WS12 NOTES design.
   - Workspace drop sweep: `src/server/conn/handler_monoio/write.rs:~258-270`, `handler_sharded/write.rs:~137-149`, `src/shard/spsc_handler.rs:~3128` — capture per key, or abort the epoch for a prefix sweep (justify).
   - MQ PUSH/POP/ACK via `get_stream_mut`: `src/shard/mq_exec.rs:~486-499`.
   - Stream wake path: `src/shard/spsc_handler.rs:~2522` → `src/blocking/stream_wake.rs:~57`.
   - One test per writer: armed epoch + the write before its key is serialized → the RDB holds the pre-image (no resurrection / duplication for MOVE/COPY). Add each site to YOUR NOTES.md capture audit table.
2. **moon#1228 — epoch liveness and budget**:
   - FLUSHDB of a non-empty unfinished db / SWAPDB discard the whole epoch (`src/persistence/snapshot_cow.rs:~242-271`): hand the detached table to the epoch instead (redis does not kill its child on FLUSHDB). Test: a workload that FLUSHDBs more often than one full save still completes BGSAVE with a correct point-in-time image.
   - Convergence under insert rate: per-tick budget grows with `pending_pre_images()`; tombstones / pre-images / `PENDING_KEYS` become visible (count them in `used_memory` or at minimum an INFO field — pick with a written argument). Deterministic in-process convergence test; the Linux-perf-host measurement at pipelined insert rates is DEFERRED (say so).

3. **moon#1185 remainder (Should, only after item 1)** the incremental COW AOF fold was blocked on the #1228 capture gaps (MOVE/COPY … DB n, eviction-with-spill, replica full resync) — re-assess once they are closed; implement if the WS12 NOTES design now holds, else DEFER with the exact remaining blocker.

Items 4 and 5 (moon#1231 promote-then-sweep, moon#1232 sharded `--save`) moved to WS19-cold-durability, which runs in parallel.

## Owned files
`src/command/keyspace/move_cmd.rs`, `src/scripting/bridge.rs` (COPY/MOVE region only), `src/shard/mq_exec.rs`, `src/blocking/stream_wake.rs`, `src/persistence/snapshot_cow.rs` + `src/persistence/snapshot.rs`, tests `tests/perf_ws16_*.rs`. NOT yours (WS19): `src/shard/timers.rs`, `src/storage/tiered/**`, `src/storage/db/kv_ops.rs`, `src/persistence/auto_save.rs`, `src/main.rs`, `src/shard/persistence_tick/**`, and the `ScriptEvictionCtx::gate` function in `src/scripting/bridge.rs`.
Cross-ownership (small, ISOLATED commits, listed in SUMMARY): the workspace-drop-sweep blocks in `handler_monoio/write.rs` / `handler_sharded/write.rs` and the two `spsc_handler.rs` regions named above — touch nothing else in those files.
