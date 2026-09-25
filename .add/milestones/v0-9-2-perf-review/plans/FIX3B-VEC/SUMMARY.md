# FIX3B-VEC — SUMMARY (PR #1242 review fixes: vector / FT.SEARCH)

Base `07d9850` (the PR #1242 head at launch). These fix the REVIEW3B-VEC findings SHOULD-1, NIT-2 and NIT-3, and add a KNN prefilter condition limit.

## Verdicts

| Item | Verdict | Commits | Red → green |
|---|---|---|---|
| 1. SHOULD-1: the filtered yielding scan reranked at the wrong depth | **FIXED** | 8c59a93 (+ e920441, 1bcf2ed) | The existing sync/MVCC/yielding identity test now has a 90 % filter under all three filtered strategies. Red on base: 1 of 80 queries differ under HnswPostFilter. New `post_filter_strategy_scans_agree_with_search_mvcc` (20,500 × 64d): red 19/30 for the yielding scan and 19/30 for `search_filtered`; green after. |
| 2. KNN prefilter condition limit | **FIXED** (`MAX_KNN_FILTER_CONDITIONS = 128`) | ed7fb26 | parse.rs unit tests: 1, 2, 127 and 128 parse; 129 and 130 give `Invalid` with the exact wire text. New integration row in `tests/perf_fix_1238_inline_prefilter.rs`, shards 1/2/4, inline and FILTER. Red with the check disabled: 129 conditions returned 4 rows at `--shards 1`. |
| 3. NIT-3: in-place SESSION comment and a self-comparing test | **FIXED** (the claim is now true) | a7097eb | The test compares against a plain filter written in the test and checks the `Vec` buffer pointer. New e2e `sparse_and_hybrid_session_searches_skip_seen_documents`. Mutation checks confirm both tests bite. |
| 4. NIT-2: tiny L2 rows lost recall in the f16 rerank | **FIXED** (L2 only; the cut was chosen by measurement) | 1c0ff3a; 4704b94 (WARM) | `tiny_l2_rows_keep_their_adc_estimate_where_f16_is_coarser`: L2 1e-7 was 0.848 → 0.767, and is now 0.848 → 0.848. L2 1e-6 goes 0.848 → 0.975. WARM `warm_rerank_scores_rows_like_its_hot_source` was red at 1e-7 and at 1e5. |
| 5. K=100 `rerank_cost_ab` cell (optional) | **NOT DONE** | — | It needs a release-fast build on a quiet box. The shared box was at load ~10. |

The reviewer's proof file passes 2/2 with the fixes applied.

## Design notes
- **Item 1:** the mutable leg uses `k` for rerank depth and truncation (4k rows instead of 12k), on the yielding path and in the `search_filtered` HnswPostFilter arm. The graph legs keep their 3k oversample.
- **Item 2:** the limit is enforced in `parse_filter_string`, which every path goes through: inline and FILTER, single- and multi-shard, both runtimes. It answers the existing `ERR invalid FILTER expression`. It is an input limit, for parity with HybridFilter's limits and for bounded evaluation cost.
- **Item 3:** `retain_unseen_in_db(&mut impl ResultBuffer, …)` retains in place on `Vec` or `SmallVec`. The hybrid and sparse SESSION blocks filter and record `fused` directly, removing up to three copies per query.
- **Item 4:** an L2 row keeps its ADC estimate only when every component is subnormal and the row's RMS is under 3 subnormal steps (`Σ mantissa² < 9·dim`).
  - The check is integer-exact and early-exits, so an ordinary row costs one comparison.
  - The literal "subnormal norm" rule was measured and rejected, because it lost the rerank gain from 1e-6 to 1e-5.
  - Cosine is excluded because its rerank beats ADC at every scale.
  - WARM now shares the function, so it also picks up e4d31c4's non-finite rule.

## Gates (final tree)
- fmt, audit-unsafe and audit-unwrap: clean.
- clippy `--all-targets` and clippy tokio: clean.
- tokio `check --all-targets`: clean.
- Lib `vector` filter:
  - monoio: 1123 passed.
  - tokio: 941 passed, 1 failed. The failure is the wall-clock `bg_compact_tests::test_bg_compact_pool_parallelism` at load ~10; it passes alone. This branch touches no compaction code.
- `perf_fix_1238_inline_prefilter` passes on both runtimes.

## Cross-ownership edits
- `src/vector/persistence/warm_search.rs` and `warm_sub_signs_tests.rs` (4704b94): the WARM rerank uses the shared exact distance. `hot_then_warm` takes a `scale` argument, and existing callers pass 1.0.
- `src/command/vector_search/tests.rs` (a7097eb): four test-only `pub(super)` visibility changes.

## Risks for integration
1. **Intended result changes:**
   - HnswPostFilter FT.SEARCH reranks 4k mutable candidates, matching RANGE/SESSION.
   - 129 or more prefilter conditions answer the ERR.
   - Tiny L2 rows keep their ADC distance on HOT and WARM.
   - WARM keeps ADC for rows beyond the f16 range.
2. **Pre-existing, not fixed:** the mutable ADC Cosine estimate is not on the `2 − 2·cos` scale, so a Cosine row that keeps its ADC estimate is mis-ordered against reranked rows.
3. **Pre-existing, not fixed:** with immutable segments present, `search_mvcc` passes the bitmap into the graph (ACORN) while the yielding HnswPostFilter scan post-filters. Only the mutable leg is aligned here.
4. **Shared-target aliasing:** two integration builds picked up another worktree's artifacts, and every counted run was checked for provenance. Gate runs should check provenance too.

## CHANGELOG bullet
- **Vector / FT.SEARCH** (the PR #1242 review fixes):
  - Under a broad filter (HnswPostFilter), a plain FT.SEARCH returns the same documents as the same query with RANGE or SESSION.
  - A KNN prefilter accepts up to 128 conditions. More answers `ERR invalid FILTER expression`.
  - L2 vectors whose components are below f16 precision keep their quantized distance in the rerank, on HOT and WARM segments.
  - FT.SEARCH … SESSION on the hybrid and sparse paths filters in place.

(The FIX3B-VEC agent's report was committed by the orchestrator because the harness refused the agent's SUMMARY write.)
