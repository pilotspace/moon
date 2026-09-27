# moon#1199 leftovers: perf triage at main 273e6bc (2026-09-27)

Every listed issue is still **OPEN**. None changed after the part-5 comment: the last activity on any of them is 09-25 13:12, and no commit after #1242 references them. Status comes from reading the code at HEAD. Costs are quoted from the issues and the `plans/*/SUMMARY.md` / `NOTES.md` files; this pass measured nothing new.

## Summary table
| # | What remains at HEAD (file:line) | Measured cost | Verdict |
|---|---|---|---|
| 1190a | FLUSHALL/FLUSHDB ASYNC = SYNC (`command/server_admin.rs:61-93`). `Database::clear` swaps the table out and drops it on the shard thread (`storage/db/kv_ops.rs:327-353`) | unmeasured. O(keys) drop inline; the per-value lazy-free exists only for single values of 65,536+ elements | **DO-NOW #1** |
| 1190b | O(1) `entry_overhead` (`db/mod.rs:86` walks `estimate_memory`, called from `kv_ops.rs:239` overwrite and `:577` DEL) | the walk is ~5 ms of a 105-173 ms 1M-field free (≈3-5%). Redis also frees DEL/overwrite synchronously | DEFER (~90 write sites, ledger-corruption risk, ~4% gain) |
| 1189 | Expiry index is still `BTreeSet<ExpiryPair{u64, CompactKey}>` (`db/mod.rs:615`). Keys over 23 B are copied to the heap. §1 (SSO element type for hash/set members) was never scoped by any WS | ~55-75 B per volatile key vs ~25-30 designed (arithmetic only). §1: hash field ~148 B vs redis ~91 B | NEEDS-MEASUREMENT (index); DEFER §1 (large refactor) |
| 1171 | HRANDFIELD is an O(N) borrowed walk. `RedisValue::Hash(Box<HashMap<Bytes,Bytes>>)` (`storage/entry.rs:229`) | 1M fields: 1.23 ms vs redis 54 µs (22.7×) | NEEDS-MEASUREMENT (IndexMap memory/HSET cost). Stronger case with **N1** below |
| 1194 | (a) key_hash-map merge not done: 231 refs in 31 files. (b) declared-schema payload indexing is opt-in only (`MOON_VECTOR_PAYLOAD_SCHEMA=declared`). (c) `doc_values` is still a HashMap, not a dense Vec | (a) 21-41 B per vector, against ~2.4 KB per vector at 768d = 0.9-1.7%. (b) **−5,136 B/doc** RSS when on | (a) **DROP** (within noise). (b) maintainer decision on the default. (c) DEFER |
| 1239 | Multi-shard KNN merge still `0, usize::MAX` (`shard/vector_scatter.rs:86,110`); `ERR FILTER not supported in multi-shard` (`handler_monoio/ft.rs:397`, `handler_sharded/ft.rs:280`). `merge_search_results` skips errored legs (`ft_search/response.rs:107`) | correctness | DO-NOW (LIMIT, errors, FILTER lift); @field/RANGE/SESSION plumbing is medium |
| 1243 | DEL/UNLINK hook tombstones vectors only (`shard/spsc_handler.rs:3263-3273`). Nothing calls `TextStore::remove_doc_by_doc_id` (`text/store.rs:2070`) outside persist/tests | correctness. Also a leak: postings grow with delete churn until restart | **DO-NOW** |
| 1244 | CACHESEARCH calls `search_local_filtered(.., None filter ..)` with no text store (`vector_search/cache_search.rs:234,374`) | correctness. See **N2** for the perf problem | DO-NOW (route through FT.SEARCH's filter path) |
| 1245 | (1) tag scanner accepts an unclosed `{`. (2) HYBRID RRF fuses per shard. (3) merge sort is stable on score only (`response.rs:130`), so ties come out in leg order | correctness | (1)+(3) DO-NOW, one-liners. (2) DEFER, or document as approximate |
| 1246 | Nightly release leg runs only `/recall/` (`crash-matrix.yml:203`); ratio guards gated on `debug_assertions` run nowhere. Files still over limit (`holder.rs` 2055, `hnsw/search.rs` 1917, `handler_monoio/mod.rs` 5115, `db/mod.rs` 3887). Port-collision item already covered by the flock `PORT_LOCKS` (`tests/common/mod.rs:60-90`) | CI only | DO-NOW (release ratio-guard leg). Splits: move-only, opportunistic |
| 1247 | Pause checked per batch (`client_pause.rs`, `handler_monoio/mod.rs` batch top) | correctness | DO-NOW (per-write re-check gated on the #1175 hint) + SET -P16 no-regress A/B |
| 1248 | Foreign fast-path read registers tracking **after** the read (`handler_monoio/mod.rs:4318` read → `:4354` register). The SPSC path registers first (`:4397`) | correctness, window narrow (0/300k stress) | **DO-NOW** (move the register above `try_foreign_db_read`) |
| 1251 | Lists never demote (`list/list_write.rs:350,540`). Small zset listpack is unsorted | shrink-back unmeasured. Sorted-listpack gain ≤128-entry scan ≈ sub-µs | shrink-back NEEDS-MEASUREMENT. Sorted listpack **DROP** unless measured |
| 1252 | LIGHT-from-f16 not shipped | compaction −17-24%; recall −0.035/−0.020 at ef 24/64 (random 768d) | NEEDS-MEASUREMENT (MiniLM, multi-seed) |
| 1256 | `compute_checksum` streams libm-derived QJL matrices (`turbo_quant/collection.rs:292-330`) | full HNSW rebuild at boot across libms | NEEDS-MEASUREMENT (aarch64 digest), then a libm-free generator. See note |
| 1258 | Positions are a `PosColumn` per run, not per term | already 107.6 → 48.4 B per (term, doc), −37% RSS. What remains is run headers only | likely DROP. Cheap check first (below) |
| 1259 | (1) mutable Cosine ADC scale. (2) ACORN vs post-filter with immutable segments | (1) only rows with a component beyond ±65,504 | (1) DROP/doc. (2) DEFER (vector owner, correctness) |
| 1166 | Tracking table not striped (`tracking/table.rs`); tracked reads and writes to tracked keys take one global mutex | idle-tracker SET −11% vs redis −9% at s1 (the residual is noise). Contention at s≥4 unmeasured | NEEDS-MEASUREMENT (needs 8+ cores) |
| 1198 | Item 1 only: cluster mode takes `cs.read()` per keyed command (`handler_monoio/dispatch.rs:434`, `handler_sharded/mod.rs:1408`) and has no inline path (`blocking.rs:3331`) | unmeasured | NEEDS-MEASUREMENT. The high-risk part is #485 misroute |
| 1214 | Item 1 SPSC notify lock; item 3 `Database::get` re-probe | item 1 ≤ **0.83%** removable (s4 perf); item 3 **0.61%** (100K keys) / **1.01%** (hot) with GETSET, 0% for plain GET | **DROP both**, close with the data |
| 1226 | Only "SESSION on the RANGE path copies": the hybrid path RANGE does `drain→SmallVec→Vec` (`ft_search/dispatch.rs:261-264`); the rest was filed as #1239/#1244/#1245/#1246 | one ≤k-element copy | **DROP**, close #1226 |

## New findings (not filed; seen while checking)
- **N1. HSCAN/SSCAN clone and sort the whole collection on every call.** `hash_read.rs:258-262` and `:525-528` (`entries()` + sort); `set_read.rs:487-491` (`members()` + sort). That is O(N log N) per page, so a full scan at COUNT 10 is O(N²/10·log N). Removal before the cursor also shifts positions, which breaks the SCAN guarantee.
  - Fix for sets (`IndexSet`, SREM uses `swap_remove`, `set_write.rs:425`): a **descending** positional cursor. `swap_remove` only moves the tail element, which is already visited, downward, so an element present for the whole scan is never missed (at worst returned twice). O(COUNT) per call.
  - Small encodings return everything in one reply with cursor 0, as redis does.
  - Verdict: **DO-NOW #2** (sets). HSCAN gets the same once #1171's IndexMap lands, which raises #1171's value. ZSCAN is #934.
- **N2. FT.CACHESEARCH probe cost.** Each call counts cache keys with an O(N_index) scan (`cache_search.rs:216-219`), then runs an unfiltered KNN at k = clamp(3·cache, 100, 10,000) and re-parses score strings (`:229-300`). Cache hits outside the global top-k are silently missed.
  - Fix: a key-prefix prefilter bitmap (the #1238 inline prefilter machinery) at small k, plus an incrementally kept prefix count. Fold into #1244.
  - Verdict: NEEDS-MEASUREMENT. FT.CACHESEARCH p50 at 10K/100K/1M docs with a 1% cache, `--shards 1`, 3 reps.

## DO-NOW ranking (value ÷ risk)
1. **#1190a FLUSH offload.** When `note_cleared_table` answers Drop/None, hand `old` to a `spawn_dropper` thread (the pattern `moon-snapdrop` already uses for whole tables, `persistence/snapshot/frozen.rs:60-73`) when it holds ≥ ~64K entries. That is ~20 lines, no ASYNC flag plumbing, and the ledger is already reset.
   - Bench: 5M keys (`redis-benchmark -r 5000000 -n 5000000 -P16 SET k:__rand_int__ v`), then time `FLUSHALL ASYNC`, with a parallel same-shard PING p100.
   - `--shards 1`, 3 reps, A/B.
2. **N1 SSCAN descending cursor.** Small, local change.
   - Bench: 1M-member set, `SSCAN s <c> COUNT 10` p50, and a full-iteration wall time.
   - Parity: a full scan returns every member, under concurrent SREM/SADD.
3. **#1248.** Move one call up. Deterministic hook test at `--shards 2`.
4. **#1243.** Text removal in the Delete hook (`write_hooks.rs`), plus the 4 hand copies (monoio/tokio conn, `shared.rs`, `replication/apply.rs`) and expiry/eviction. Correctness, and it stops posting growth.
5. **#1245 (1)+(3).** Require `}`; `.then_with(|| a.1.cmp(&b.1))` in the merge.
6. **#1239 core.** Apply LIMIT after the merge, propagate leg errors, lift the FILTER refusal.
7. **#1246.** Nightly `cargo nextest run --cargo-profile release -E 'binary(/perf_ws/) | test(/ratio/)'`.
8. **#1247.** Per-write re-check. Gate: SET -P16 c50 A/B shows no regression beyond noise.
9. **#1244** plus N2.

## Exact benchmarks for the NEEDS-MEASUREMENT items
All use a fresh server, `--shards 1 --appendonly no --maxmemory 0` unless noted, ≥3 interleaved A/B reps, and a Linux host (aarch64 for ship numbers).
- **#1189 expiry index**
  - Load: `redis-benchmark -r 1000000 -n 3000000 -P16 SETEX k:__rand_int__ 3600 v`. Repeat with key names padded to 40 B.
  - Measure `used_memory`/RSS ÷ keys against redis 7.0.15 and against the same load without a TTL. That difference is the index cost.
  - Also check the SETEX rps no-regress and the active-expiry keys/s sweep rate.
  - Build if the index is over 40 B per key above the design.
- **#1171 IndexMap hash**
  - Sizes 1K, 100K and 1M, loaded with `HSET h f:__rand_int__ v` (`-r 100000000`).
  - Measure bytes per field, HSET -P16 rps, HDEL rps, HRANDFIELD and HSCAN (N1) p50, HGETALL rps.
  - Go if memory is within +10% and HSET within noise.
- **#1166 tracking stripes:** 8+ core box, `--shards 4` and `8`. 50 RESP3 `CLIENT TRACKING ON` clients GET 100K keys while `redis-benchmark SET -P16 -c50 -r100000` runs. Record `perf record -g` `lock_slow`/futex share. Build if it is over 5%, or tracked-GET throughput is over 15% below the untracked GET baseline.
- **#1198 cluster:** the same binary with `--cluster-enabled yes` and without, at `--shards 4`. GET/SET at P1 and P16, c50, `-r 1000000`. Build the bitmap only if the gap is over 10%; it must pass the formation, failover and migration suites.
- **#1251 shrink-back:** 10K lists, each pushed to 1,000 elements then popped to 10. Compare `used_memory` and RSS per key with redis, plus LPUSH/RPOP rps.
- **#1252:** MiniLM 384d, 100K docs, 5 seeds. R@10 at ef 24/64/128, LIGHT built from f16 vs from f32. Ship only if the mean drop is ≤ 0.005.
- **#1256:** compute the pinned EXACT digests on GCE c4a (aarch64 glibc) and compare them with the x86_64 goldens.
- **#1258:** on the 200K-doc corpus, count runs per term and compute run headers (~48 B each) as a share of posting bytes. DROP if under 5%.

## Notes
- **#1256 risk.** QJL matrices are regenerated from the seed. A checksum over only the seed and params would hide a cross-libm matrix mismatch against the persisted QJL bits. The failing checksum is protective. The only safe fix is a libm-free, versioned Gaussian generator; old segments still rebuild once.
- **#1190b.** Redis's default `lazyfree-lazy-user-del`, `lazyfree-lazy-server-del` and `lazyfree-lazy-eviction` are all `no`, so DEL, overwrite and eviction are already at parity. Only the extra walk (~4%) remains.
- **Close with the data:** #1214 (0.83% and ≤1.01%) and #1226 (k-sized copy). Close the #1194 key_hash-map part as DROP (≤1.7%).
