# WS8-shard-coordination — PLAN (wave 2)
personas: `.add/personas/routing-dispatch-engineer.md` (lead) · `.add/personas/performance-engineer.md` (read `docs/internal/cross-shard-cost-model.md` fully — dead ends listed there must not be re-proposed)

## Issues (bug first)
1. **moon#1162 (BUG)** multi-key DEL/UNLINK spanning shards never tombstones vectors (reproduced: 20/20 deleted docs still returned by FT.SEARCH at --shards 4).
   - Must: coordinator local leg + `MultiExecute` arm run the same post-write hooks as the other arms (`auto_delete_vectors`, `auto_drop_mq_streams`, text-index cleanup — enumerate by reading the `Execute`/`PipelineBatchSlotted` arms); tokio handler checked too. Regression integration test at --shards 4 (the table in the issue).
2. **moon#1184** spanning MSET/DEL/UNLINK split per key → one sub-command per owner shard (`MSET k1 v1 …` / `DEL k1 …`), static command-name bytes, replies combined (sum for DEL/UNLINK, OK for MSET). AOF record count per spanning command ≤ #owner shards (test). Replay of the merged records must work (existing `serialize_local_mset` precedent).
3. **moon#1177** cross-shard write redundancy: drop the discarded origin-side `serialize_command_for_log` (bool instead of `Option<Bytes>` in `RemoteMeta`, both handlers); `wal_append_and_fanout(data: Bytes)` moves the record into the AOF pool (no `copy_from_slice`); backlog mutex only when fan-out is active (fix the lying comment); move `frame` instead of deep-cloning; `Vec<Frame>` instead of `Vec<Arc<Frame>>`; `remote_groups` as a reused `Vec<Vec<_>>` indexed by shard; boxed tracking keys.
4. **moon#1183** WATCH: foreign fast-path read of versions (same `pending_mask` gate as GET), else send-all-then-await; shared guards for `get_version` (local group + `ReadVersions` arm). WATCH semantics tests by name green; parks per WATCH measured via INFO counters.
5. **moon#1182** multi-shard FT.SEARCH: send remote legs BEFORE the local search (vector, text phase 2, hybrid DFS, KEYS); local leg via `ft_search_capture` → `search_mvcc_yielding`; `VectorSearch` SPSC arm spawns a local task on the owned snapshot instead of searching synchronously in the drain. Result identity vs HEAD on a fixture at --shards 4; `ft_search_cooperative_yields_total` > 0 at --shards 4.
6. **moon#1198 (shard items)**: SPSC batch arms refresh the db clock inside the per-command guard (no extra exclusive acquisition); remove `ExecuteSlotted` / `MultiExecuteSlotted` / `PipelineBatch` if the whole-repo grep proves no producer (src + tests + benches + fuzz). Commits `refs moon#1198`.

## Owned files
`src/shard/**` (all — WS5a/WS6 wave-1 edits are merged by now), `src/server/conn/handler_monoio/mod.rs` + `handler_sharded/mod.rs` ONLY the remote-dispatch / `remote_groups` / `RemoteMeta` / coordinator-call blocks, `src/server/conn/handler_monoio/ft.rs` (FT scatter entry), `src/server/conn/handler_monoio/dispatch.rs` ONLY the `coordinate_multi_key` call block, `src/server/conn/watch.rs`, tests `tests/perf_ws8_*.rs`.

## Not yours
ACL/PAUSE/tracking/metrics/inline gates in the connection handlers (WS7), `src/scripting/**`, `src/pubsub/**` (WS9), `src/storage/**`, command write sites (WS10).
