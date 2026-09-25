# WS16-snapshot-capture — PLAN (wave 2, part 3 — QUEUED: starts after WS7, WS8 and WS10 are integrated, because it edits regions of their files)
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

4. **moon#1231 (BUG, data loss, pre-existing)** a cold key promoted back to RAM after a fold (`promote_cold_outcome`, every RMW promote) leaves the cold index with no log record; once its file's other keys are gone the orphan sweep unlinks the file and a restart loses the key (WS15 reproduced 88/200 on `ae21476`). Fix direction in the issue: a file below the COMMITTED generation's cut is unlinkable only when every key it held is dead or no longer derives its value from it — a fold-committed signal for the sweep (`src/shard/timers.rs`) + a promotion hook (`src/storage/db/kv_ops.rs`). Read `plans/WS15-durability-followups/NOTES.md` ("promote-then-sweep") first; build on WS15's dead-slot ledger. Real-server kill-9 case at --shards 1/4, both runtimes (extend `tests/crash_recovery_cold_del_rewrite.rs`).
5. **moon#1232 (BUG)** `--save` rules never fire in the sharded server: `main.rs:2204` `change_counter` is never incremented outside `handler_single`. Per-shard dirty counts summed on read (never a shared per-write `fetch_add` — moon#1176), one number for the trigger and `rdb_changes_since_last_save`; integration test at --shards 1/4 both runtimes (`--save "1 10"`: 10 writes snapshot, 9 do not). CHANGELOG must call out the behaviour change (existing `--save` users start getting snapshots).

## Owned files
`src/command/keyspace/move_cmd.rs`, `src/scripting/bridge.rs` (COPY/MOVE region only), `src/shard/mq_exec.rs`, `src/blocking/stream_wake.rs`, `src/persistence/snapshot_cow.rs` + `src/persistence/snapshot.rs`, `src/shard/timers.rs` (sweep gate), `src/storage/tiered/**` (sweep policy), the promotion path in `src/storage/db/kv_ops.rs`, `src/persistence/auto_save.rs`, `src/main.rs` (auto-save wiring only), tests `tests/perf_ws16_*.rs`.
Cross-ownership (small, ISOLATED commits, listed in SUMMARY): the workspace-drop-sweep blocks in `handler_monoio/write.rs` / `handler_sharded/write.rs` and the two `spsc_handler.rs` regions named above — touch nothing else in those files.
