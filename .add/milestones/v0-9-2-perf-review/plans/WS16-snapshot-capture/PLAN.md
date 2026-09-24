# WS16-snapshot-capture — PLAN (wave 2, part 3 — QUEUED: starts after WS7 and WS8 are integrated, because it edits regions of their files)
personas: `.add/personas/storage-durability-engineer.md` (lead) · `.add/personas/ci-test-integrity-engineer.md` · `.add/personas/performance-engineer.md` (vector items only)

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

3. **moon#1228 — vector small items (Should, after 1–2)**:
   - `MOON_VECTOR_PAYLOAD_SCHEMA=declared` BM25 TextMatch resolver scores every matching doc and allocates per hit (`src/command/vector_search/ft_search/payload_filter.rs:~77-83`): intersect postings into a bitmap; result identity test vs `ae21476`.
   - HNSW prefetch line count ignores row misalignment (`src/vector/hnsw/graph.rs:~310`, an unaligned 516 B row spans 10 lines, 9 hinted): compute from `(addr & 63) + bpc`; unit test of the line arithmetic. The aarch64 A/B is DEFERRED (no hardware here — say so in SUMMARY).

4. **moon#1222 (test flake)** `a_read_is_one_forced_claim_per_stream_that_replays_exactly`: pin the thread-local clock (`tl_clock_set`) in the test and audit the sibling tests in `stream_effect.rs` for the same unpinned-clock hazard.
5. **moon#1185 remainder (Should, only after item 1)** the incremental COW AOF fold was blocked on the #1228 capture gaps (MOVE/COPY … DB n, eviction-with-spill, replica full resync) — re-assess once they are closed; implement if the WS12 NOTES design now holds, else DEFER with the exact remaining blocker.

## Owned files
`src/command/keyspace/move_cmd.rs`, `src/scripting/bridge.rs` (COPY/MOVE region only), `src/shard/mq_exec.rs`, `src/blocking/stream_wake.rs`, `src/persistence/snapshot_cow.rs` + `src/persistence/snapshot.rs`, `src/command/vector_search/ft_search/payload_filter.rs`, `src/vector/hnsw/graph.rs` (prefetch only), the replication stream_effect tests, tests `tests/perf_ws16_*.rs`.
Cross-ownership (small, ISOLATED commits, listed in SUMMARY): the workspace-drop-sweep blocks in `handler_monoio/write.rs` / `handler_sharded/write.rs` and the two `spsc_handler.rs` regions named above — touch nothing else in those files.
