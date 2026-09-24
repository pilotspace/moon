# WS5b-text-graph SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws5b-text-graph`, base `a925e64` (code-identical to
> `935c555`), 13 commits. Persona: performance-engineer (lead) + routing-dispatch lens for
> FT.SEARCH / GRAPH.QUERY reply parity. Zero new `unsafe`.

## Per-issue verdict
| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| moon#1191 FT.SEARCH double scoring / key per match / full sort / term-0 intersection | **FIXED** | `b3008f4` fix · `c4dd514` determinism fix (found by the parity harness) · `1086ac1` public red test · `4ad1825` bench (cross-ownership) | Membership = posting/TAG/NUMERIC bitmap algebra (Cow-borrowed, AND smallest-first); each TEXT leaf resolved once; ONE scoring pass with doc-ordered `PostingCursor`s, IDF hoisted per term, length norm per (doc, field); bounded top-k heap; keys cloned for the page only; `total` = \|matched ∩ live\|. Scores bit-identical to HEAD (`bm25_score` split literally; every f32 sum keeps HEAD's order). `text::query::eval::oracle_tests` keeps HEAD's evaluator verbatim as the oracle: ~2.9K query×top_k pairs (OR/AND/grouping/@field/prefix/fuzzy/TAG/NUMERIC/`*`/stop words/DFS global IDF/AS_OF) match on ids, keys, order, total and score bits. Wire parity vs baseline: 150 randomized FT.SEARCH × LIMIT variants, 0 mismatches at shards 1 and 4. Red on HEAD: `tests/perf_ws5b_text_search.rs` (HEAD top-10 39.3 ms vs one full pass 9.4 ms). | 50-term prefix query still 120 ms at 200K matches (per-doc cursor seeks ×50) — term-at-a-time accumulator would help |
| moon#1195 upsert O(Σ posting length) memmoves | **FIXED** (O(N) gone; older docs still somewhat slower) | `d01b9cd` fix · `f9311a7` 256-entry runs + public red test · `d780898` measured comment | Rank-aligned tf/positions columns: flat up to 256 entries, else runs of ≤ 256 (split on overflow, merge < 32, back to flat < 64), located by binary search over run starts. Doc ids never change ⇒ tie order unchanged, no dead-doc masking for df/IDF/DFS, `.tpost` byte-identical. Tests: `upsert_cost_is_flat_across_doc_position`, `chunked_columns_match_the_model_under_churn`, `from_parts_chunks_long_postings_identically`, `chunked_postings_round_trip_through_tpost`. Red on HEAD: `tests/perf_ws5b_text_upsert.rs` (doc 0 16.4 ms vs doc N−1 37.5 µs, 438×). | oldest/newest still 1.29× at 50K and 1.67× at 200K (HEAD 14× / 51×), dominated by each position list's 24 B `Vec<u32>` header; contiguous per-run positions (with moon#884) would flatten it and save ~30 B per (term, doc). NB: `f9311a7`'s message says "within noise" — wrong; the numbers here are the measured ones |
| moon#1194 (text part) | **FIXED** | `04bc989` | New `src/text/doc_columns.rs`: `DocKeys` (keys + live bitmap), `DocU64s` (insert LSN, content checksum; 0 = absent), `DocLengths` (flat u32 rows), `DocSlices<T>` (exact-size boxed slices). A TAG entry is `(u32 field idx, Bytes)` sharing the `tag_indexes` key allocation: 16 B slot + 40 B/value vs 528 B; NUMERIC 16 B slot + 16 B/value. Billing matches real size (slices bill `size_of_val`, columns `capacity × slot`); HashMap method names kept so external call sites compile. Red on HEAD: `tests/perf_ws5b_text_billing.rs` (VmRSS vs billed, 60K docs: HEAD TAG real 1,267 B/doc vs billed 164.5 = 0.13) → TAG 153 vs 125 (0.82), NUMERIC 45 vs 55 (1.24). `store::tag_tests::side_table_layout_is_compact_and_billed_exactly` pins `size_of` and value sharing. | Ids removed by FT.INVALIDATE_RANGE / the boot deletion probe leave ~70–84 B holes across columns — compact ids on `.tpost` rewrite; `PostingStore::doc_terms` is another dense-id map left alone |
| moon#1197 Cypher LIMIT / ORDER BY | **FIXED** | `c41d57b` fix · `13be1d4` public red test · `072c25e` lint · `b111dba` pure move | Backward pass computes rows each operator must produce; a leading `scan → (Filter\|Expand\|Unwind)* → [1:1 Project]` run whose LIMIT needs ≤ 100K rows is fed in growing chunks via `MergedNodeView::try_for_each_visible_node` and stops early. ORDER BY: keys computed once per row, compared by reference, only the needed page kept (`select_nth` + sort by (key, index)) when keys are totally ordered; NaN or ints > 2^53 beside floats fall back to HEAD's exact stable sort. Plan shape and PhysicalOp set unchanged. `limit_sort_tests` uses `execute_profile` (full evaluation) as the oracle: 29 queries × 3 tier layouts identical (≥ 70% non-empty asserted); NaN/huge-int fallback identical. Red on HEAD: `tests/perf_ws5b_graph_limit.rs` (LIMIT 10 34.0 ms vs 36.2 ms full). | ORDER BY after RETURN still projects every row (377 → 181 ms at 200K) — planner could project only the top-k; range `IndexScan` still collects all keys before streaming |

## Found (pre-existing on HEAD)
- Capped (50-term) fuzzy/prefix expansion broke df ties randomly per process, so two baseline servers disagree on e.g. `loz*` (`det_check.py`: 12454 vs 12453). Fixed by `c4dd514` (order by df DESC, term id ASC).
- `#[ignore]`'d `inverted_search_shard_consistency.rs` / `inverted_search_numeric_shard_consistency.rs` are broken at their seed on HEAD (`let _: i64 = hset_multiple(..)` receives HMSET's `+OK` — the CONVENTIONS carried follow-up); they never ran green against either binary.

## Measurements
4-vCPU container shared with other agents — relative only. Binaries `/home/user/wt/bin/baseline-935c555` vs `/home/user/wt/bin/ws5b-text-graph-final` (release-fast @ `072c25e`; later commits are a comment and a pure move). Raw-RESP Python client, one connection; `--appendonly no --save "" --maxmemory 0 --disk-offload disable`, ports 7201–7218.

`FT.SEARCH <q> LIMIT 0 10` on 200K docs (30-token Zipf(1.1) bodies over 2,000 words + TAG + NUMERIC), 3 interleaved blocks, each the median of 8–20 calls (ms):

| query | matches | s1 baseline | s1 new | × | s4 baseline | s4 new | × |
|---|---|---|---|---|---|---|---|
| rank-0 term | 199,249 | 681/778/518 | 7.1/6.6/6.6 | 103 | 215/215/219 | 7.8/8.0/8.9 | 27 |
| rank-3 | 134,930 | 548/427/369 | 5.2/4.8/5.2 | 82 | 160/174/167 | 7.0/7.7/6.9 | 24 |
| rank-30 | 21,979 | 74.9/74.5/55.6 | 1.04/0.96/1.01 | 74 | 29.5/33.9/34.6 | 1.0/1.4/3.4 | 25 |
| rank-300 | 1,843 | 2.6/4.4/4.1 | 0.25/0.30/0.41 | 14 | 4.9/3.5/1.8 | 0.8/1.4/0.6 | 4.5 |
| rank-1999 | 270 | 0.51/0.62/0.62 | 0.30/0.30/0.23 | 2.1 | 0.78/0.65/0.67 | 0.56/0.50/0.39 | 1.3 |
| 2-term AND | 182,274 | 1335/1274/932 | 13.7/14.7/11.4 | 93 | 401/407/385 | 13.2/15.9/15.7 | 26 |
| rare ∩ broad | 309 | 469/461/457 | 0.36/0.33/0.43 | 1,291 | 207/220/206 | 0.62/1.17/0.66 | 312 |
| prefix (50 terms) | 200,000 | 3096/3567/3124 | 120/126/120 | 26 | 1486/1522/1760 | 99/127/86 | 15 |
| `*` | 200,000 | 29.6/33.2/33.7 | 0.15/0.10/0.15 | 222 | 21.1/22.6/30.2 | 1.19/0.78/0.64 | 29 |

Cost per matched doc (rank-0, s1): baseline ~3.1–3.9 µs → ~34 ns. Parity: 150 randomized queries (term, AND, OR, grouping, `@field`, 5-char prefix, `%fuzzy%`, TAG, NUMERIC+term, `*`, stop words) × LIMIT offset 0/3/50 × count 1/10/100 — 0 mismatches at shards 1 and 4, all non-empty.

Upsert (HSET of a new 30-token body, body-only index), 300 reps interleaved across baseline / control baseline / new, median µs [p25–p75]: 50K docs — base doc0 3,583 [3,307–3,924], docN−1 259; ctrl 3,590 / 261; new doc0 339 [261–390], docN−1 262. 200K docs — base doc0 15,533 [14,367–16,694], docN−1 302; ctrl 15,461 / 296; new doc0 420 [317–494], docN−1 252. Noise floor ≤ 2%. Shards 4 (50K/shard): doc 0 3.3–3.6 ms → 0.26–0.36 ms.

RSS for 100K tagged docs (TEXT title + 2 TAG + 2 NUMERIC, keys included, fresh server, 2 runs): baseline RSS +165.5/+165.3 MB (1,654 B/doc), used_memory +90.2 MB (902 B/doc) → new RSS +75.4/+74.8 MB (751 B/doc), used_memory +66.6 MB (666 B/doc): −90 MB (−55%); unbilled gap 752 → 85 B/doc; tag query totals identical.

`GRAPH.RO_QUERY` on a 200K-node label (result/plan caches bypassed), 3 blocks × median of 5, ms, rows identical: `MATCH (n:L) RETURN n.x LIMIT 10` 146.3/142.8/140.7 → 0.36/0.17/0.18 (786×); `… WHERE n.x > 500 RETURN n.y LIMIT 10` 116.3/117.9/112.9 → 12.0/10.9/10.7 (10.7×); `RETURN n.x, n.y ORDER BY n.x DESC, n.y LIMIT 10` 376.7/381.6/372.6 → 185.9/173.9/180.8 (2.1×); `WITH n ORDER BY n.x, n.y LIMIT 10 RETURN n.y` 1130/1343/1150 → 225/235/211 (5.1×).

Red → green: the four `tests/perf_ws5b_*.rs` fail on a HEAD source tree and pass here.

Suites run: lib `text:: command::vector_search graph::text_index` 564 (incl. the oracle differential), `graph:: command::graph` 553, `graph::cypher` 142; default-feature integration fts_query_eval_e2e 15, fts_posting_rank_tf 7, fts_query_parse 20, fts_stopwords_690 8, ft_match_all_693 6, ft_query_error_vocabulary_691 6, ft_text_meta_tag_numeric_restart 1 (MOON_BIN), graph_freeze_boundary 38, graph_result_cache 18, graph_segment_merge 5, graph_restart_id_aliasing 2, perf_ws5b_* 4; tokio+graph+text-index (MOON_BIN = final) adversarial_g1_g2 10, adversarial_v0110_fix04 4, adversarial_v0110_fix06 2, ft_search_as_of_boundary 3, ft_search_as_of_filter 1, ft_search_multi_shard_as_of 2, graph_cypher_inline_filter 4, graph_integration 13, lunaris_cypher_shortest_path 1, lunaris_cypher_temporal 1, lunaris_hybrid_ft_search 1, txn_cypher_write_rollback 3, txn_ft_search_snapshot 3, perf_ws5b_graph_limit 1, perf_ws5b_text_search 1. Gates clean: `cargo fmt --check`, clippy `--lib -D warnings` ×2 feature sets, clippy `--lib --tests`, tokio `check --lib`, `scripts/audit-unsafe.sh`.

## Cross-ownership edits
- `Cargo.toml`: one `[[bench]] name = "text_search"` entry (own commit `4ad1825`) — merge as a union with other bench entries.
- `src/command/vector_search/ft_aggregate.rs`: one line in the BM25 match-all path (`doc_id_to_key.keys().collect()`) — within plan ownership, listed for visibility.

## Risks / things the orchestrator must re-check at integration
1. Changed `TextIndex` types: `doc_id_to_key` is `DocKeys` (`keys()` yields `u32`, `iter()` yields `(u32, &Bytes)`, no `entry()`); `doc_id_to_insert_lsn` / `doc_id_to_content_checksum` are `DocU64s`; `doc_field_lengths` is `DocLengths` (`get(doc, field)`); `doc_tag_entries` / `doc_numeric_entries` are `DocSlices<(u32, …)>`. `PostingList.term_freqs` / `positions` are private (`tf_values()`, `position_lists()`, `has_positions()`). Re-run `cargo check --lib --tests` and `text::query::eval::oracle_tests` after merging.
2. Intentional reply differences vs HEAD (each nondeterministic or degenerate there): capped fuzzy/prefix expansion ordered by (df DESC, term id ASC); FT.AGGREGATE `*` candidates in ascending doc id instead of HashMap order; NaN BM25 scores sort last (only with a non-finite WEIGHT).
3. Memory ledger: `resident_bytes()` bills the dense columns by capacity; text `used_memory` goes down (902 → 666 B/doc on the tagged corpus), RSS more.
4. Pre-broken `#[ignore]`'d `inverted_search_*_consistency` suites (seed bug) — the shards-4 parity run covers cross-shard TAG/NUMERIC instead.
5. Shared target dir: every result came from a single `cargo test` invocation whose output showed this worktree compiling.
6. PROFILE vs QUERY: `execute_profile` still evaluates every row (the oracle); GRAPH.QUERY stops early.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.92 · Self-evaluation 0.9. Not done: term-at-a-time scoring for 50-term expansions; top-k-only projection in the Cypher planner; contiguous per-run position storage.

## PR #1221 review fixes (orchestrator-committed from the fix agent's report)
Branch `fix/pr1221-ws5b`, cherry-picked onto the PR as `bd47cc0` (F1), `0415bda` (F2), `40b744b` (F5), `02478f2` (F3).
Every new test is red with only its fix reverted (`scratchpad/ws5b-revert-red-keep.log`).
- **F1 (MAJOR)** freed doc ids are reused smallest-first (`free_doc_ids` = `[0, next_doc_id) \ live`) and dense columns shrink when the top ids go. 12 invalidate→re-index cycles × 4K docs, 0 live docs: resident bytes **4,456,869 → 421 B**; a 1-doc index reloaded from a 168 B `.tpost` **2,496,427 → 583 B**. `reused_doc_ids_answer_exactly_like_a_fresh_index` checks FT.SEARCH keys, score bits, totals, N/avgdl/df, TAG, NUMERIC and AS_OF across a reused id.
- **F2** load-time density guard `next_doc_id ≤ 2 × docs + 65,536` (else `Invalid` → rebuild): the 107-byte file that billed 72 MB is refused; the `text_postings_file` fuzz target now also installs.
- **F3** the var-length Expand cap spans streamed chunks (`emitted_before` per op): GRAPH.QUERY == GRAPH.PROFILE; expansion ≤ 100K + N rows (was 200,114).
- **F5** capped fuzzy/prefix expansion ranks df DESC then TERM BYTES ASC in one bounded selection over FST and post-FST terms — independent of term-id assignment, so reproducible across rebuilds and replicas.
- Residual: equal-score tie order after churn differs from HEAD (reused low ids); one-time rebuild of sparse `.tpost` files; up to 65,536 empty slots in an accepted file; pre-existing Damerau (FST) vs Levenshtein (post-FST) fuzzy difference; fuzzer not run locally; `read.rs` 1815 lines.
