# WS10-memory-ownership — PLAN (wave 2)
personas: `.add/personas/storage-durability-engineer.md` (lead: memory accounting and replay correctness) · `.add/personas/performance-engineer.md`

Review measurements: 100K × (SET scratch 4 KiB; SADD s member:i) → moon RSS 18.8 → 578.2 MB with used_memory 8.2 MB (redis 20.4 MB); `DEL s` → 25.2 MB (the pin is the set members); control with strings 34.6 MB. 200K XADD → moon used_memory Δ 0 B, MEMORY USAGE 114 B (redis 8.4 MB / 9.3 MB).

## Issues
1. **moon#1160** collection elements keep slices of the connection read buffer (and of the whole AOF replay buffer).
   - Must: every site that stores a request-argument `Bytes` into a long-lived container (hash field/value, set member, zset member (map + tree), list element, stream field/value, HashWithTtl ttls, and any other found by grep of `.clone()` on `extract_bytes` results feeding inserts) stores an exact-size owned copy — via a `Database`-side / storage helper so `src/command/` keeps the hot-path allocation rule; listpack/intset/string paths unchanged (they already copy).
   - Must: AOF replay no longer holds two full copies of the incr file and replayed elements do not pin the replay buffer (bounded chunked reader, or detach at insert — the same helper).
   - Tests: a unit test proving a stored element does NOT share the frame's allocation (e.g. pointer range check against the frozen buffer) for each collection type; an integration test reproducing the review scenario with an RSS bound (RSS after ≤ ~3× used_memory + baseline) — use release-fast; `used_memory` accounting unchanged.
2. **moon#1163** streams never charged to `used_memory`.
   - Must: XADD/XTRIM/XDEL/XSETID/consumer-group PEL + XCLAIM/XAUTOCLAIM growth charge/credit O(1) deltas consistent with `estimate_memory`; `MEMORY USAGE` of a stream reflects its size; DEL returns used_memory to baseline (no saturation-at-0 under-count). Integration test: N XADDs grow used_memory ≈ N × per-entry cost; DEL returns to baseline; maxmemory now binds on a stream workload.
3. **moon#1198 (storage/misc items)**: `HotKeySketch::tick` → thread-local sampling counter (no shared fetch_add ticked by foreign readers); `StreamId::to_bytes` without `format!`; `key::keys` without per-key clone + extra probes; `RECL_SEGMENT_STALL_ACTIVE` / `RECL_MVCC_*` per-shard contributions instead of last-writer-wins overwrite. Commits `refs moon#1198`.

## Owned files
Collection write sites in `src/command/{hash,set,sorted_set,list,stream}/**` (ownership-copy edits only — keep diffs minimal; the wave-1 algorithmic rewrites are merged), storage insert helpers in `src/storage/**` (new file preferred, e.g. `src/storage/owned_bytes.rs`), `src/storage/stream.rs`, `src/storage/hotkey.rs`, `src/persistence/aof_manifest/shard_replay.rs` + `src/persistence/aof/mod.rs` replay readers + `src/persistence/replay.rs` (replay buffer only), `src/command/key.rs` (`keys` fn), `src/shard/segment_stall.rs`, `src/shard/timers.rs` (RECL gauges only), tests `tests/perf_ws10_*.rs`.

## Not yours
Connection handlers (WS7/WS8), `src/shard/**` other than the two files above (WS8), `src/scripting/**`, `src/pubsub/**` (WS9).
