# WS5b-text-graph — PLAN (wave 1)
persona: `.add/personas/performance-engineer.md` (+ `routing-dispatch-engineer.md` lens for FT.SEARCH/GRAPH.QUERY reply parity)

## Issues
1. **moon#1191** text FT.SEARCH scores every match twice, clones a key per match, sorts the full set.
   - Must: `eval_set` builds membership from posting bitmaps (no scoring); single scoring pass with per-term IDF + `&PostingList` hoisted and a doc-ordered tf cursor instead of per-doc `rank()`; intersection starts from the rarest posting; bounded top-k heap, keys resolved/cloned only for the returned page; `total` from set cardinality minus unresolvable ids.
   - Must: result set, scores (within f32 tolerance) and ordering (score DESC, doc_id ASC) identical to HEAD on randomized corpora, both `search_field` and `search_field_or`, AS_OF wrappers, DFS global_df path, shards 1 and 4 (existing fts integration suites must stay green — run them by name).
   - Evidence: add `benches/text_search.rs` (Zipf corpus) OR a release-fast `#[test]`-scale timing showing broad-term query cost no longer ∝ match count × 2.
2. **moon#1195** text upsert O(Σ posting length) memmoves → fresh doc id on upsert + dead-doc bitmap masked at search + background/lazy compaction, OR container-chunked postings; latency of updating doc 0 vs doc N−1 in an N-doc index must be flat. Persistence/recovery of text postings (`docs/internal/text-postings-persistence.md`) must stay correct — read it first.
3. **moon#1194 (text part)**: `doc_tag_entries` / `doc_numeric_entries` compact representation (`(u16 field_idx, value)` pairs, small inline capacity), dense-id side maps → `Vec` columns where ids are dense; billing (`tag_entries_cost` etc.) matches the real size (assert with a size test).
4. **moon#1197** Cypher: early exit for LIMIT when no ORDER BY / aggregation / DISTINCT downstream; fused Sort+Limit top-k; sort keys computed once per row. Result sets identical (graph test suites by name).

## Owned files
`src/text/**`, `src/graph/**`, `src/command/graph/**`, `src/command/vector_search/ft_text_search.rs`, BM25 paths in `src/command/vector_search/ft_aggregate.rs`, new `benches/text_search.rs` (+ its `[[bench]]` entry in `Cargo.toml` — cross-ownership, own commit, note it), tests `tests/perf_ws5b_*.rs`.

## Not yours
`src/vector/**`, the rest of `src/command/vector_search/**` (WS5a), `src/shard/**`.
