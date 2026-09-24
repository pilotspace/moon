# WS1-storage-core — PLAN (wave 1)
personas: `.add/personas/performance-engineer.md` (lead) · `.add/personas/storage-durability-engineer.md` (advisor on removal/expiry/eviction correctness)

## Issues (do in this order — later items build on earlier ones)
1. **moon#1159** DashTable H2 fingerprint shares the directory's top hash bits.
   - Must: `h2()` from bits no other consumer reads (review A/B used bits 32–38: +18% GET / +11% SET at 1M keys, --shards 1).
   - Must: a test-only key-compare counter + a test asserting mean compares per hit ≤ ~1.1 on a 100K-key table (fails on HEAD: ~6–8).
   - Should (separate commit, `refs moon#1159`): in-place `split_segment` (move only entries whose new-depth bit is 1; no heap Vec) and `insert_or_update_slice(&[u8], …)` so SET overwrite of a >23-byte key does not build/drop a `CompactKey` (also removes the `from_raw_parts` over a moved key in `dashtable/mod.rs:532-538` — reducing unsafe is allowed; adding is not).
2. **moon#1161** reads never refresh LRU/LFU metadata; sampler is heavy.
   - Must: GET/`get_if_alive`/inline-GET-reachable read paths update access time (and the LFU Morris counter under `*-lfu`), gated so there is zero cost when no LRU/LFU policy or `maxmemory == 0`. `&self` update via atomics (e.g. `AtomicU32` last_access with a manual `Clone`; LFU bits updated without disturbing the WATCH version bits in `metadata`). Entry stays 32 B (assert `size_of`).
   - Must: `OBJECT IDLETIME` resets on read, `OBJECT FREQ` grows under LFU, `TOUCH` really touches — match redis.
   - Must: integration test reproducing the review's hit-ratio scenario (maxmemory 64MB, allkeys-lru, 5K hot keys read between cold writes): hot-key retention ≥ 90% (HEAD: 0.6%, redis: 100%).
   - Should: one-pass victim sampler (compare idle/freq from the scanned `v`, clone only the winner), policy from its atomic (no `from_str` per call).
3. **moon#1190** removing a large collection stalls the shard.
   - Must: `entry_overhead` O(1) for collections (e.g. a running byte count inside the boxed container, maintained by the existing O(1) write-site deltas) — prove equality with `estimate_memory()` in debug/test builds.
   - Must: monoio UNLINK (and FLUSHALL ASYNC if the same helper applies) hands large values to a per-shard lazy-free queue drained under a time budget; tokio keeps `spawn_blocking` or uses the same queue. PING p100 during UNLINK of a 1M-field hash must not include the drop.
   - Should: active expiry / eviction of large values go through the same queue.
4. **moon#1189 (expiry-index part only)** — `expiry_index: BTreeSet<(u64, CompactKey)>`.
   - Must: sweep uses `pop_first`-style draining; removal without building a `CompactKey` (borrowed query newtype); single-probe remove-if-expired.
   - Could: structural `(deadline_ms, key_hash)` index (16 B/entry) if it stays correct under hash collisions — only if time allows; otherwise DEFERRED with design notes. (The B+tree and element-type parts of #1189 belong to WS2 / are deferred.)

## Owned files
`src/storage/dashtable/**`, `src/storage/entry.rs`, `src/storage/db/**` (WS2 may ADD `src/storage/db/string_mut.rs` + one `mod` line), `src/storage/eviction.rs`, `src/storage/mem_size.rs`, `src/server/expiration.rs`, `src/command/key.rs` (UNLINK / OBJECT / TOUCH paths only), `benches/dashtable_*.rs`, new tests `tests/perf_ws1_*.rs`.
Minimal cross-ownership allowed (own commit + SUMMARY note): the lazy-free drain hook in `src/shard/event_loop.rs` / `persistence_tick.rs`; the inline-GET touch call in `src/server/conn/blocking.rs` (one call site).

## Not yours (other workstreams are editing them right now)
`src/storage/bptree*.rs`, `src/storage/db_read.rs`, `src/storage/listpack.rs`, `src/storage/compact_value.rs`, `src/command/{string,set,sorted_set,geo,list,hash}/**`, `src/protocol/**`, `src/persistence/**`, `src/vector/**`, `src/text/**`.
