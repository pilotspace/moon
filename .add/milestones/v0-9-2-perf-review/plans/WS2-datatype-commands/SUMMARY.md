# WS2-datatype-commands SUMMARY

Branch `perf/ws2-datatype-commands`, base `a925e64` (code = HEAD `935c555`). Personas:
routing-dispatch-engineer (lead), performance-engineer. Zero new `unsafe`. `storage/entry.rs`
untouched.

## Per-issue verdict

| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| **NEW — B+tree structural corruption** (found here, no issue yet) | FIXED | `229215e` | `storage::bptree::invariants::random_insert_remove_keeps_every_invariant` (RED on HEAD). Server probe on baseline vs redis 7.0.15: 300 random-score ZADDs -> 69 members with `ZRANK` nil; ZREM of half -> `ZRANGE 0 -1` returned 172 entries for a 150-member zset (20K: 12,770 nil ranks, 13,572 vs 10,000). Fixed binary: 0 / exact. | **Orchestrator: open an issue** (data integrity: wrong ZRANK, deleted members resurrected in range replies for any >128-member zset built from non-monotone scores). Live trees heal on restart (persistence serializes the member map). |
| moon#1170 zset range reads | FIXED | `d396d3d` (+ `229215e`) | `range_rank_tests` differential (ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANGE BYSCORE REV/ZCOUNT with 20 bound spellings, LIMIT -1..100000, rank ranges, BYLEX one-score & mixed) on both dispatch paths; `limit_and_count_do_not_scale_with_n`, `lex_limit_and_count_do_not_scale_with_n` (2K vs 200K < 8x; RED on HEAD). Server (1M): LIMIT 0 10 48.7 ms -> 118 µs, REV 51.6 ms -> 109 µs, ZCOUNT 32.4 ms -> 55 µs, ZRANGEBYLEX 200K 4.0 ms -> 78 µs, ZLEXCOUNT 3.5 ms -> 87 µs. | Mixed-score BYLEX keeps the O(N) scan (redis semantics undefined there). ZRANGESTORE still reads its source via `get_sorted_set` (flattens a listpack) — out of scope. |
| moon#1171 ZRANDMEMBER / HRANDFIELD | PARTIAL (ZRANDMEMBER FIXED; HRANDFIELD 84x faster, still O(N)) | `b5bfa32`, `d2ab296` | `zrandmember_does_not_scale_with_n` (HEAD 99.9x -> <8x), `zrandmember_contract_on_both_encodings`, `hrandfield_does_not_materialize_the_hash` (HEAD 393 ms vs 385 ms for `entries()`; now 7.9 ms vs 424 ms), `hrandfield_contract_on_every_representation` (Map/listpack/field-TTL). Server: ZRANDMEMBER 1M 20.3 ms -> 56 µs (redis 51); HRANDFIELD 1M 103.7 ms -> 1.23 ms (redis 54 µs, still 22.7x). | **DEFERRED — O(1) HRANDFIELD**: store full hashes as `IndexMap<Bytes, Bytes>` (and `HashWithTtl.fields` likewise) so `get_index(i)` is O(1), exactly as sets moved to `IndexSet`. Needs `storage/entry.rs` (`RedisValue::Hash`, estimators, RDB/AOF codecs, `HashRef`) — WS1-owned; HDEL must use `swap_remove` (O(1), reorders — fine for hashes) and the `hash_*_cost` constants must add indexmap's entries `Vec` (≈ 8 B hash + 64 B pair per field). WS2's `hrandfield_readonly` then only swaps `walk_live_pairs` for `get_index` on the Map arm. |
| moon#1189 (B+tree part) | FIXED | `8af787e` | borrowed `(f64,&[u8])` probes (no `copy_from_slice` per ZREM/rescore/ZRANK); moves not clones in splits/borrows/merges; lopsided splits at the chain ends; separate leaf (576 B) / internal (784 B) arenas in fixed chunks (256 / 64 nodes, never reallocated). `memory_1189::*`, `arena::tests`. 1M members, B/entry `node_capacity*NODE_BYTES/len` -> `memory_bytes/len`: rising 205.5/234.9 -> 44.1/50.2, random 102.8/117.4 -> 63.7/73.2; fill rising 7.00 -> 14.00. Server `used_memory` of a 1M rising zset: 363 MB -> 180 MB (redis 126). | Measured CPU cost, see Risks: random ZADD / rescore ~+6% (paired median) vs the bug-fixed baseline; rising ZADD -13%. |
| moon#1169 set algebra | FIXED | `e94b7b9` | `algebra_tests::*` (400 random 1-4-key combos over intset/listpack/hashtable/missing, all 7 commands vs a BTreeSet model on both paths; sources never re-encoded; WRONGTYPE precedence; `small_against_big_does_not_scale_with_big` HEAD 171.5x -> <8x). Server: SINTER small big 232 ms -> 85 µs (redis 73), SDIFF 240 ms -> 63 µs (redis 50), SINTERCARD ... LIMIT 1 254 ms -> 57 µs. | *STORE destination stays `hashtable` (redis writes intset/listpack when small) — pre-existing, out of scope. |
| moon#1168 string in place | FIXED | `5a0616c` | new `storage/db/string_mut.rs` (`Database::mutate_string`, effect table after `incr.rs`); `inplace_tests::*` (3000-step differential of SETBIT/SETRANGE/APPEND/BITFIELD with TTL, LFU and WATCH-version checks; `setbit_cost_is_independent_of_bitmap_size` HEAD 548.6x -> <8x; `append_growth_is_amortized_linear` HEAD 35.1x -> <8x; `bitfield_grows_before_running_like_redis`); `string_mut::tests`. Server: SETBIT 12.5 MB 8.75 ms -> 59 µs (redis 51); APPEND 40K x 100 B 12.0 s -> 0.2 s (redis 0.1). | Growth is exact-`realloc` (the 16-byte `CompactValue` has no room for an sds capacity); amortization rests on geometric allocator size classes and is proven by the sweep, not by a stored capacity. |
| moon#1172 geo | FIXED | `fa0fe41`, `1017135` | new `command/geo/geo_search.rs` = port of redis 7.0.15 `geohashCalculateAreasByShapeWGS84` + `membersOfAllNeighbors` + `geoWithinShape` (+ exact `geohashGetDistance`, `fixedpoint_d2string`). `search_tests::*` (300-query brute-force differential incl. BYBOX — HEAD missed box-edge members; ANY/COUNT/lon-lat errors; mutable GEOPOS/GEODIST/GEOHASH/GEOSEARCH/GEORADIUS STORE keep a listpack; `geosearch_does_not_scale_with_n` HEAD 104.6x -> <8x). Server: GEOSEARCH 200K 50 km COUNT 10 22.4 ms -> 84 µs (redis 82); GEORADIUS 500 km WITHDIST 24.9 ms -> 1.5 ms (redis 1.6). | FROMMEMBER of a missing member still answers `[]` (redis: `ERR could not decode requested zset member`) — pre-existing. WITHCOORD digits follow redis 8.x (7.0.15 prints `%.17Lf`; same doubles). |
| moon#1174 §4 listpack zset | FIXED (§4); sorted listpack DEFERRED | `c63e6ce` | `listpack_count_tests` (60 random listpack zsets: ZRANK/ZREVRANK/WITHSCORE/ZCOUNT/ZLEXCOUNT vs oracle, and the test-only `entries_sorted` counter stays 0 — 87 on the old code). Server ZRANK listpack(100) 62 -> 54 µs (≈ client floor). | Keeping the listpack sorted (as redis does) would make listpack ZRANGE a slice too; needs ZADD in-place ordered insert — DEFERRED. |

Behaviour changes, all TOWARD redis 7.0.15 (verified live) and pinned by rows added to
`scripts/test-consistency.sh` + `scripts/test-commands.sh` (`4bf31a8`): SINTERCARD answers
WRONGTYPE when any key is wrong-typed even if an earlier key is missing (was 0); BITFIELD with
SET/INCRBY grows/creates the string to the highest written bit even when OVERFLOW FAIL applies
nothing (was: nothing created); ZRANDMEMBER count>=size returns the whole zset highest-first
(was a random permutation); HRANDFIELD count>=size returns the hash in iteration order;
GEOSEARCH without ASC/DESC/COUNT keeps redis's cell-walk order (was always sorted), honours ANY
(was ignored), and errors on ANY-without-COUNT / `COUNT 0` / `COUNT 1.5` / an out-of-range
FROMLONLAT centre; BYBOX uses redis's point-latitude longitude leg; WITHDIST/GEODIST use
`llrint(d*1e4)`; GEODIST/WITHDIST use `deg*(PI/180)`. SETBIT/SETRANGE/APPEND/BITFIELD keep the
key's LFU counter (was reset to 5) and answer `-IOERR` over an unreadable cold copy (SETBIT and
BITFIELD used to build a value from nothing).

Dispatch paths: every rewritten read runs one body on `command::dispatch` (mutable; SPSC cross-shard,
MULTI, Lua) and `dispatch_read` — the mutable entry points delegate to the `_readonly` twins
(HRANDFIELD, GEOPOS/GEODIST/GEOHASH newly; GEOSEARCH/GEORADIUS*/GEOSEARCHSTORE read through
`get_sorted_set_ref_if_alive`). `try_inline_dispatch` does not handle any WS2 command (checked);
tests assert both paths agree.

## Measurements (method, reps, raw numbers)

Host: the shared 4-vCPU x86_64 Linux container (other agents building concurrently) — RELATIVE
evidence only. Binaries: `/home/user/wt/bin/baseline-935c555`, `/home/user/wt/bin/ws2-c63e6ce`
(release-fast build #1 = all issue commits; the later `d2ab296` only reorders ZRANDMEMBER's
count>=size reply), `/home/user/wt/bin/ws2-fixonly-229215e` (build #2 = bug fix only, the honest
control for write-path CPU). redis-server 7.0.15. All `--shards 1 --appendonly no --save ""
--maxmemory 0 --disk-offload disable`, separate `--dir`s.

1. `.bench/measure_ws2.py` (review harness shape): p=1 per-op latency via a Python RESP client
   (~50 µs client floor), 3 reps, servers interleaved base -> ws2 -> redis per rep; medians
   (µs/op): see the per-issue table; raw per-rep values in `.bench/measure_c63e6ce.txt`, e.g.
   `ZRANGEBYSCORE LIMIT 0 10` b=[44063, 48705, 56057] w=[118, 135, 69] r=[96, 63, 65];
   `SINTER` b=[222317, 232224, 243177] w=[137, 63, 85] r=[73, 74, 66];
   `SETBIT 12.5 MB` b=[8880, 8750, 8484] w=[74, 59, 51] r=[51, 49, 60];
   `APPEND 40K x 100 B` (s) b=[12.0, 13.8, 12.0] w=[0.2, 0.2, 0.2] r=[0.2, 0.1, 0.1].
   Unchanged-by-design controls: `ZRANGE z 0 99` 180 vs 168 vs 185 µs; `SETBIT 1 KiB` 57 vs 52 vs 51.
2. Size sweeps (complexity proof, debug unit tests, best-of-5/7): every O(N) path is RED on HEAD
   at 2K vs 200K (ratios 35x-549x) and < 8x after (see table).
3. Write-path A/B vs the bug-fixed control, `.bench/zadd_ab.py`, 7 interleaved reps
   (`.bench/zadd_ab_fixonly_vs_ws2.txt`): ZADD 300K random: paired ws2/fix ratios
   [1.06, 1.10, 1.17, 1.00, 1.00, 0.99, 1.08] (median +6%); ZADD 300K rising: 0.87x (-13%);
   rescore 100K: [1.11, 1.04, 1.06, 1.13, 1.01, 0.99, 1.23] (median +6%); ZREM 30K: ~1.0
   (noise band ±15%). Against the corrupt HEAD baseline the comparison is not meaningful (its
   broken tree skips work).
4. Memory: `used_memory` delta of loading 1M rising-score members: 363.4 MB (base) -> 179.7 MB
   (ws2), redis 125.7 MB. Structural: see the #1189 row.
5. Parity: `.bench/diff_redis.py` — 21,898 cases on random fixtures vs redis-server 7.0.15
   (`.bench/diff_7122.txt`): 0 mismatches in ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANGE BYSCORE REV
   (1500 each), ZCOUNT, ZRANGEBYLEX/ZREVRANGEBYLEX/ZRANGE BYLEX REV/ZLEXCOUNT (800 each), ZRANGE
   rank/ZREVRANGE (600 each), SINTER/SUNION/SDIFF/SINTERCARD/*STORE (500 each + results),
   WRONGTYPE precedence, SETBIT/SETRANGE/APPEND/BITFIELD (~500 each, values checked), GEOSEARCH
   (700, byte-exact incl. unsorted order, ANY, WITHDIST, WITHHASH), GEORADIUS (700), GEODIST (300),
   GEOHASH (300), WITHCOORD (100, numeric). Remaining differences are oracle-version skew only:
   `ZRANK/ZREVRANK … WITHSCORE` (a 7.2 option; 494 cases), set `listpack` encoding (7.2), and
   ZRANDMEMBER count>=size order on build #1 (6 cases, fixed by `d2ab296`). Scores are compared as
   doubles (7.0.15 prints `%.17g`). The same script on the HEAD baseline: 3,203 mismatches.
6. `scripts/test-consistency.sh --shards 1` (and 4) with MOON_BIN=build #1 vs the same run with the
   baseline binary: WS2-only failures: none; baseline-only failures: exactly the 7 new WS2 rows.
   Failures common to both (redis 7.0.15 vs moon's 8.x target, or other workstreams' areas):
   OBJECT ENCODING listpack sets, GEOPOS/WITHCOORD digits, unknown-command/GETKEYS text,
   tracking #1013, ROLE; plus the ZRANDMEMBER row on build #1 (fixed after).

Gates on the final tree (all with `--config profile.dev.package.moon.debug="limited"`, a
private moon fingerprint in the shared target): `cargo fmt --check` OK; `cargo clippy
--all-targets -- -D warnings` OK; `cargo clippy --no-default-features --features
runtime-tokio,jemalloc -- -D warnings` OK; `cargo check --all-targets --no-default-features
--features runtime-tokio,jemalloc` OK (so `benches/bptree_memory.rs` and every new test build on
the tokio leg; none uses graph/text-index). Final `cargo test --lib -- bptree sorted_set zset
command::set command::string string_mut command::geo command::hash db_read accounting_788
ledger`: 630 passed, 0 failed (binary `moon-43da8381d6aa49eb`, which contains WS2-only test
modules such as `range_rank_tests` — i.e. built from this worktree). Earlier
`storage:: persistence::` run: 1412/1413 — the one failure,
`cold_index_rebuild_tests::unreadable_file…`, and integration
`cold_index_rebuild_silent_drops_875` fail identically on the baseline (chmod 000 does not stop
root). Integration (MOON_BIN = build #1):
read_preserves_compact_encoding, zset_read_cold_tier_928, shard_routing_parity,
two_key_write_cross_shard, cold_promote_compact_encoding_898 — green.

## Cross-ownership edits

- `src/storage/db/mod.rs`: exactly one line, `pub(crate) mod string_mut;` (planned; `5a0616c`).
- `src/storage/db_read.rs`: `SetRef`/`SortedSetRef` impl blocks (`any_tree`,
  `entries_unordered`, a `#[cfg(test)]` counter bump in `entries_sorted`) plus, just above
  `pub enum SortedSetRef`, a `#[cfg(test)]` thread-local `ENTRIES_SORTED_CALLS` and
  `take_entries_sorted_calls()` (module level, test-only) — `b5bfa32`, `c63e6ce`.
- `src/command/hash/hash_read.rs`: the HRANDFIELD section only, and a `#[cfg(test)] mod
  hrandfield_1171` appended at the END of the file (`b5bfa32`) — WS3 edits other parts of it.
- `src/storage/compact_value.rs`: string-mutation helpers only (`5a0616c`).
- `scripts/test-consistency.sh`, `scripts/test-commands.sh`: new rows only (`4bf31a8`).

## Risks / things the orchestrator must re-check at integration

1. **WS1 (storage-core) overlap.** `db/string_mut.rs` reproduces `Database::set`'s effects the
   way `db/incr.rs` does and touches private `Database` fields (`data`, `cached_now*`,
   `cold_index`, `maybe_has_expiring_keys`) and helpers (`note_lazy_expired`,
   `promote_cold_known_absent`, `cold_fault_pending`, `spill_inflight_*`, `adjust_memory`). If WS1
   changes `set`'s effect list (e.g. #1189's expiry-index rework), mirror it in both `incr.rs` and
   `string_mut.rs`. `storage/entry.rs`'s `zset_estimate_covers_the_real_btree_arena` uses
   `node_capacity() * NODE_BYTES` as a floor: still true (NODE_BYTES is now the leaf slot).
2. **WS3 merge points**: `hash_read.rs` (HRANDFIELD region + trailing test module) and
   `db_read.rs` (counter placed right above `pub enum SortedSetRef`).
3. **CPU trade in #1189**: ~+6% on random ZADD/rescore (paired medians, noisy shared box) for
   -50% (rising) / -37% (random) arena bytes, no shard-thread arena reallocs, and -13% on rising
   ZADD. Plausible cause: the chunked arena's second index on every node access. Re-measure on a
   quiet Linux host; if it holds, options are larger chunks or a single-level fast path for trees
   that fit in the first chunk.
4. **Behaviour changes toward redis** (list above) — CHANGELOG material; especially GEOSEARCH's
   unsorted reply order and SINTERCARD's WRONGTYPE precedence.
5. **Corrupted live zsets** from the B+tree bug persist in memory until restart on any node that
   ran the old binary; replicas rebuild from RDB/AOF member maps (correct).
6. **Shared-target aliasing** (see NOTES.md): test/clippy results here come from a private moon
   fingerprint (`profile.dev.package.moon.debug="limited"` override); a gate run WITHOUT such
   isolation can execute another worktree's moon artifacts.

## Self-evaluation (0–1)

Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.9 · Edge cases 0.93 ·
Self-evaluation 0.92.

- Completeness: every PLAN item done; HRANDFIELD O(1) and sorted listpacks are DEFERRED for
  concrete ownership/scope reasons with a design note; the one plan item not literally met is
  HRANDFIELD's O(1), which needs WS1's `entry.rs`.
- Optimization: all measured O(N)/quadratic paths now within 1.0-1.8x of redis at the client
  floor; the #1189 CPU trade (+6% random writes) is reported, not hidden.
- Edge cases: ±inf / exclusive / -0 bounds, negative LIMIT, ties, integer-encoded listpack
  members, WRONGTYPE ordering, expired/cold/cold-fault keys (string_mut), 512 MB limits, both
  dispatch paths, listpack vs B+tree vs Owned vs intset encodings — each with a test; verified
  byte-exact against a live redis on 21,898 random cases.

## PR #1227 review fixes (orchestrator-committed from the fix agent's report)
Cherry-picked as `b6e7926` (geo argument validation + bounded step estimator), `48b13c5` (decoded positions
clamped like redis; saturating cell successor), `1021483` (ZRANDMEMBER / HRANDFIELD count parsed before the key
like redis), `c5979ed` (a cloned arena keeps its first chunk within the chunk size), `af02033` (doc).
- GEO: `geosearch_core` parses per command form exactly like redis 7.0.15's `georadiusGeneric`, with redis's
  error texts (verified live); `estimate_steps_by_radius` is total and capped. `command::geo::validation_tests`
  (20 forms × 13 bad radii, both handler twins) and `tests/geo_input_validation.rs` (4 shards, MULTI/EXEC, Lua)
  red on fea9469, green after. 44 new rows in both consistency scripts match redis byte-for-byte.
- Residual: hex floats (`0x10`) are accepted by redis's `strtod` but not by moon (pre-existing everywhere); no
  fuzz target for the rewritten geo parser yet.
