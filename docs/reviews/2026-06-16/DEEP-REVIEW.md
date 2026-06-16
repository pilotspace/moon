# Moon Deep Review — 4 Core Features (2026-06-16)

Architecture map + code audit of Moon's four core subsystems, against the CLAUDE.md coding rubric
(hot-path allocations, lock handling, error handling, parser defensiveness, unsafe/SAFETY, file-size,
perf invariants). Reviewed at `feat/ft-yield-costfree-monoio` (db61973). Per-feature detail lives in
`review-kv.md`, `review-vector.md`, `review-graph.md`, `review-fts.md`.

Method: 4 parallel principal-Rust review agents, read-only, serena-driven, each citing real `file.rs:line`.
Severity: **P0** correctness/crash/security/data-loss · **P1** perf-hot-path / lock-across-await / unsafe-gap /
unwrap-in-lib · **P2** maintainability/file-size · **P3** nit.

---

## Cross-cutting verdict

Moon is a genuinely strong, idiomatic thread-per-core datastore. The **shared-nothing model is clean across
all four features** — per-shard ownership, ArcSwap/lock-free reads, no lock held across `.await` anywhere in
any subsystem reviewed. **Parser defensiveness is excellent** (RESP, Cypher, FT query — all fuzz-aware,
return errors not panics). **Unsafe discipline is 100%** (every block has an accurate `// SAFETY:`, every SIMD
kernel has a scalar fallback + cfg guard + tests for both paths). No P0 security findings.

The recurring themes across features:
1. **Latent correctness traps that are masked today by an upstream guard** — broken invariants that any future
   refactor activates (Vector `code_len`, FTS `expect()`, Graph incoming-edge gap). These are the highest-value fixes.
2. **A small set of hot-path allocations** that violate the project's own no-alloc rule (KV `INCR` String,
   Vector per-search HashMap clone, FTS dispatch `Vec`).
3. **`std::sync::RwLock` on the dispatch path** (KV) — a direct violation of the parking_lot-only rule.
4. **File-size debt** — 13 files exceed the 1500-line cap across the four features.

Net: no shipping-blocker P0s for the *current* call paths, but several latent P0s worth fixing before they bite,
and a clear, cheap P1 perf list. Detail below.

---

## 1. KV — `src/storage/` + `src/command/{string,hash,set,zset,list}` + dispatch

**Architecture.** TCP bytes → `parse_frame_zerocopy` (zero-copy `Bytes::slice`, returns `Frame::Null` on any
malformed input) → one of three dispatch paths (`dispatch`/`dispatch_read`/inline) → `DashTable` (extendible
hashing, segments × SIMD-probed groups, +1 depth over-provision, 90% load split, h2 fingerprints) holding
`CompactKey`(≤23B inline)→`CompactValue`(≤12B inline). Per-shard state owned by `Rc<RefCell>` in the event loop;
cross-shard strictly via flume SPSC. `CachedClock` thread-local removes per-key `clock_gettime`.

**Findings.**
- **P1** `INCR`/`DECR`/`INCRBY`/`DECRBY` allocate a `String` per call — `string_write.rs:312,317`. Fix: `itoa::Buffer` + the CompactValue SSO inline path (these results are always ≤20 bytes → never heap).
- **P1** `INCRBYFLOAT` redundant clone of the formatted value — `string_write.rs:389,391`.
- **P1** `AclTable` / `ClusterState` read via **`std::sync::RwLock`** on every command dispatch —
  `event_loop.rs:65–67`, `handler_single.rs:1211`, `handler_monoio/dispatch.rs:281`. Direct violation of the
  parking_lot-only rule; also a poisoning + fairness liability. Fix: migrate to `parking_lot::RwLock`.
- **P2** Five files over the 1500-line cap: `db.rs` 2197, `command/mod.rs` 2200, `event_loop.rs` 2185,
  `sorted_set_read.rs` 2091, `key.rs` 1973.
- **P3** `format_float()` double-allocates (`format!` then `to_string()`) — `string/mod.rs:36–44`.
- **Clean bills:** parser defensiveness, timestamp caching, 100% unsafe SAFETY, SIMD scalar fallbacks,
  cross-shard flume (not Arc<Mutex>), Frame::Error in dispatch (no Result).

**Top-3:** itoa for integer-arith commands · parking_lot migration for AclTable/ClusterState · split `db.rs`+`command/mod.rs`.

---

## 2. Vector — `src/vector/` + `src/command/vector_search/`

**Architecture.** Segment lifecycle mutable (brute-force) → `FT.COMPACT`/`force_compact` → immutable
(HNSW graph + SQ8/TurboQuant codes), segments under `ArcSwap<SegmentSnapshot>` for lock-free search; merge
concatenates TQ codes verbatim (never lossy re-encode), rebuilds only graph topology. `key_hash_to_key`
populated in both insert paths before `mutable.append()`. Distance kernels: AVX2/AVX-512/NEON + scalar, all guarded.

**Findings.**
- **P0 (latent)** `code_len = bytes_per_code - 4` computed unconditionally before the SQ8 branch guard —
  `hnsw/search.rs:366`. For SQ8 the trailer is 8 bytes, so this is `dim+4`, wrong. An early-return at :396 masks
  the OOB *today*, but the invariant is broken at construction; the correct pattern is `compaction.rs:1079`.
  Same family as the deferred `compaction.rs:554`.
- **P0** `FT.INFO num_docs` sums only `mutable.len()` + `imm.live_count()` — `ft_info.rs:42–45`. IVF/warm/cold
  segment vectors are silently omitted. `holder.rs:207 total_vectors()` does it right; `ft_info` should mirror it.
- **P1** Every `FT.SEARCH` clones the whole `key_hash_to_key: HashMap<u64,Bytes>` — 4 sites (`execute.rs:135/160`,
  `dispatch.rs:295/660`); ~3.2 MB/search at N=100k. Fix: `Arc<HashMap>` → O(1) clones.
- **P1** Unsafe sub-centroid ILP loop missing a `debug_assert!` for the sign-array bound (`search.rs:417–458`);
  LUT/code bounds already have `debug_assert_eq!` (:374–385).
- **P1** `expect()` in hot-path `table()` — `distance/mod.rs:154` (unreachable behind the init guard, still a policy violation).
- **Clean bills:** all SIMD SAFETY correct, key_hash populated, no lock across await, FT.COMPACT routes through
  force_compact (no-op trap fixed), SQ8 affine math + NaN guard correct, merge never re-encodes.

**Top-3:** fix `code_len` for SQ8 (search.rs:366 + compaction.rs:554) · sum num_docs across all segments ·
Arc-wrap key_hash_to_key.

---

## 3. Graph — `src/graph/` + `src/command/graph/`

**Architecture.** SlotMap `MemGraph` (mutable) → `FrozenMemGraph` → `CsrSegment` (immutable, heap or mmap, CRC32
integrity) under `ArcSwap<SegmentSnapshot>`. Logos zero-alloc lexer → recursive-descent Cypher subset →
`PhysicalPlan` cached by xxhash64 → `Vec<Row>` pipeline with `SegmentMergeReader` unifying MemGraph + CSR views.
Fully per-shard; `parking_lot::Mutex` only on the plan cache, never across `.await`.

**Findings.**
- **P0** CSR incoming/both-direction traversal gap — `traversal.rs:191`. `Direction::Incoming` and `Both` skip CSR
  for the incoming half; after compaction, undirected/incoming traversals return *silently incomplete* results, no error.
- **P0** Label bitmap truncates at label ≥ 32 — `csr/mod.rs:161` (`if label < 32`). Graphs with >32 distinct labels
  silently lose label data on freeze.
- **P1** Cypher double-parsed per `GRAPH.QUERY` (`mod.rs:36` then `mod.rs:111`); **P1** `Instant::now()` per query
  (`executor/read.rs:12`, should use shard-cached `current_time_ms()`); **P1** parallel BFS clones `Vec<MergedNeighbor>`
  per frontier node (`traversal.rs:453`); **P1** `traversal.rs` = 1586 lines (>cap).
- **P2** plan cache eviction is HashMap key-order not LRU (`planner.rs:131`); CSR edges carry a synthetic null
  `EdgeKey` so `WHERE r.prop` on CSR edges evaluates Null (`traversal.rs:222`); empty `RoaringBitmap::new()` per operator.
- **Benchmark-confirmed behavior:** Moon's Cypher `MATCH (a {id:N})` does **not** narrow on an inline node-property
  filter — it full-scans the label (verified: returns ~all edges). Per-node lookups must use native `GRAPH.NEIGHBORS`.
- **Clean bills:** parser depth-limited/EOF-safe/injection-proof/fuzz-covered, unsafe SAFETY complete, SIMD fallback,
  no lock across await, CRC32 on CSR, SlotMap generational safety.

**Top-3:** CSR incoming-edge reverse index · u64 label bitmap or error-on-overflow · split `traversal.rs`.

---

## 4. Full-Text Search — `src/text/` + `src/command/vector_search/` (FT.* text path)

**Architecture.** `AnalyzerPipeline` (NFKD → lowercase → UAX#29 segmentation → stopword → Snowball English stem)
→ `TermDictionary` (term→id) → `PostingStore` (RoaringBitmap doc_ids + parallel `term_freqs[]`) → `bm25_score()`
(k1=1.2, b=0.75 — textbook). TAG and NUMERIC stores bypass the analyzer; FST term dictionary built on `FT.COMPACT`.
Per-shard, lock-free.

**Findings.**
- **P0** `is_text_query()` SPARSE blind spot — `ft_text_search.rs:1004`. Docstring says it excludes `"SPARSE "`
  queries; the body only checks `*` and `KNN `, so a SPARSE query is misrouted to the BM25 text path.
- **P0** `expect()` ×3 in `search_field()` — `store.rs:463,470,500` ("posting exists: checked above"). Safe under
  today's single-threaded invariant; panics in library code violate the rule and break under any check/use split.
- **P1** **O(N) term-frequency lookup** — `store.rs:504–509,773–775`:
  `posting.doc_ids.iter().position(|id| id==doc_id)` linear-scans per doc×term scored → O(M²) on high-DF terms.
  **Cross-validated by this run's benchmark:** a top-5 Zipf term took 82ms p50 at just 3000 docs. Fix:
  `RoaringBitmap::rank(doc_id)-1` → O(log N).
- **P1** O(V) upsert scan on re-index — `posting.rs:139` (`remove_doc()` iterates all terms); fix: per-doc reverse index.
- **P1** `Vec<u8>` allocated per `FT.SEARCH` in the dispatch hot path — `ft_text_search.rs:1011`.
- **P2** TAG/NUMERIC field schemas **not persisted** in TMIX — `index_persist.rs:40`, `store.rs:1481`: after restart
  `tag_fields`/`numeric_fields` are empty → `search_tag()` returns empty (data-loss-on-restart for filters).
- **P2** Three files over cap: `store.rs` 2007, `ft_text_search.rs` 2816, `ft_aggregate.rs` 2043; language hardcoded
  English (`store.rs:166`); `find_field_value()` case-sensitive (`store.rs:1399` — `HSET TITLE` on `title` schema drops it);
  `apply_filter()` silent no-op (`aggregate.rs:326`).
- **P3** IDF can go negative under `global_n < local df`; no directory fsync after TMIX rename.

**Top-3:** RoaringBitmap::rank for TF lookup (kills the O(M²) high-DF cliff) · persist TAG/NUMERIC schema in TMIX ·
fix `is_text_query()` SPARSE routing.

---

## Consolidated fix priority (across all 4)

| # | Fix | Feature | Severity | Why first |
|---|-----|---------|----------|-----------|
| 1 | RoaringBitmap::rank for TF lookup | FTS | P1 (perf cliff) | Benchmark-proven O(M²); high-DF queries unusable |
| 2 | `code_len` SQ8 correctness (search.rs:366 + compaction.rs:554) | Vector | P0 latent | Masked today, corrupts on refactor |
| 3 | CSR incoming-edge gap | Graph | P0 | Silent wrong results post-compaction |
| 4 | Persist TAG/NUMERIC schema (TMIX) | FTS | P2 (data-loss) | Filters silently break after restart |
| 5 | FT.INFO num_docs sum all segments | Vector | P0 | Wrong user-facing count |
| 6 | Arc-wrap key_hash_to_key | Vector | P1 | 3.2 MB alloc per search |
| 7 | parking_lot for AclTable/ClusterState | KV | P1 | Rule violation on every dispatch |
| 8 | itoa for INCR/DECR family | KV | P1 | Hot-path String alloc |
| 9 | label bitmap >32 | Graph | P0 | Silent label loss |
| 10 | File-size splits (13 files) | all | P2 | Maintainability debt |

None of these are shipping blockers for current call paths; #1–#3 + #5 + #9 are the latent-correctness/perf items
worth scheduling before they activate.
