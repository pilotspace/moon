# WS2-datatype-commands — PLAN (wave 1)
personas: `.add/personas/routing-dispatch-engineer.md` (lead: parity on all three dispatch paths) · `.add/personas/performance-engineer.md`

Review measurements (moon vs redis 7.0.15, --shards 1, p=1): SINTER small(10)∩big(1M) 205.7 ms vs 83 µs; SDIFF 208.8 ms vs 65 µs; ZRANGEBYSCORE -inf +inf LIMIT 0 10 on 1M 45.0 ms vs 82 µs; ZREVRANGEBYSCORE 46.0 ms vs 78 µs; ZCOUNT 25.1 ms vs 63 µs; ZRANDMEMBER 1M 17.1 ms vs 65 µs; SETBIT 12.5 MB bitmap 8.3 ms vs 39 µs; APPEND 40K×100 B 11.2 s vs 0.12 s; GEOSEARCH 200K pts 20.9 ms vs 84 µs.

## Issues (order: primitives first)
1. **moon#1170** sorted-set range reads O(range).
   - Must: BYSCORE/REV with LIMIT = O(log N + offset + count) via lazy `range`/`range_rev` + skip/take (keep moon#961/#966/#967 edge cases); ZCOUNT O(log N) via new `BPTree::count_lt` using subtree `counts`; BYLEX/ZLEXCOUNT seek when all scores equal (else keep scan); rank ranges via one descent + leaf walk (`iter_from_rank`).
   - Tests: results byte-identical on randomized fixtures vs a naive oracle (both directions, exclusive/inf bounds, negative offset/count), plus a size-sweep bound (1K vs 1M members, LIMIT 0 10 must not scale with N).
2. **moon#1171** ZRANDMEMBER / HRANDFIELD O(N).
   - Must: ZRANDMEMBER via `get_by_rank` (count>0 distinct via index sampling, count<0 with repeats), listpack arm without sort.
   - HRANDFIELD: remove the full `entries()` clone + index Vec; if O(1) needs `IndexMap` for full hashes (a storage/entry.rs change owned by WS1), do the allocation-free O(N) walk now and write the IndexMap design as DEFERRED — do NOT edit entry.rs.
3. **moon#1189 (B+tree part)**: borrowed `(f64, &[u8])` search keys (no `Bytes::copy_from_slice` per ZREM/ZRANK/rescore); split by `mem::take` not clone; lopsided split for rightmost-leaf appends; separate leaf/internal arenas if it fits (measure `node_capacity()*NODE_BYTES/len` for rising + random scores in `benches/bptree_memory.rs`).
4. **moon#1169** SINTER/SINTERCARD/SDIFF(/STORE) materialize every input.
   - Must: work on `SetRef`s: sort by len, walk smallest with a borrowed iterator, probe others with `SetRef::contains`; SINTERCARD LIMIT early-exit; SDIFF iterate first set; *STORE sources via `get_set_ref_if_alive` (sources keep their listpack/intset encoding — test `OBJECT ENCODING`).
   - Both dispatch paths (`command::dispatch` mutable and `dispatch_read`) must use the new code.
5. **moon#1168** SETBIT/SETRANGE/BITFIELD/APPEND whole-string copy.
   - Must: in-place mutation for in-bounds writes through a new `Database` helper that preserves WATCH version stamping, `spill_inflight_forget`, cold-shadow removal and `used_memory` deltas (put it in NEW file `src/storage/db/string_mut.rs`, one `mod` line in `db/mod.rs`); growth amortised (keep capacity like sds, or document the realloc policy + prove amortisation with a size sweep); expiry and LFU metadata preserved (today the entry is rebuilt).
   - Tests: SETBIT on a 12.5 MB bitmap per-op cost independent of size; APPEND of 40K×100 B linear.
6. **moon#1172** GEOSEARCH/GEORADIUS O(N); mutable GEODIST/GEOHASH clone the map.
   - Must: mutable geo read commands delegate to the `_readonly` twins (no map clone, no listpack flattening); neighbour-cell search (redis `geohashGetAreasByShapeWGS84` equivalent) over `BPTree::range`; COUNT without ANY uses select_nth before sort. Results must equal redis (distance rounding, ordering, ANY semantics) — diff against redis-server on random fixtures.
7. **moon#1174 §4** listpack zset ZRANK/ZCOUNT decode+parse+sort per call → one allocation-free counting pass (keeping insertion-order storage; sorted listpack is DEFERRED unless trivial).

## Owned files
`src/command/{string,set,sorted_set,geo}/**`, `src/command/hash/hash_read.rs` (HRANDFIELD functions only), `src/storage/bptree.rs`, `src/storage/bptree_iter.rs`, `src/storage/compact_value.rs` (string mutation helpers only), NEW `src/storage/db/string_mut.rs` (+1 `mod` line in `src/storage/db/mod.rs`), `src/storage/db_read.rs` (`SetRef` / `SortedSetRef` impl blocks only), `benches/bptree_*.rs`, new tests `tests/perf_ws2_*.rs`.

## Not yours
`src/storage/entry.rs`, `src/storage/db/**` (other than the new file), `src/storage/dashtable/**`, `src/storage/listpack.rs`, `src/command/list/**`, `src/command/hash/hash_write.rs`, `src/protocol/**`, `src/server/**`.
