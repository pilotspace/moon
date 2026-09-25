# WS17-vector-text-residuals SUMMARY

- **Branch:** `perf/ws17-vector-text-residuals`, base `ae21476` plus the plan commits up to `4660dce`. NOTES.md has the design reasoning.
- **Who committed this file:** the orchestrator. The harness refused the subagent's write, so under TEAM-RULES §6 this content is the agent's final report.

## Per-issue verdict

| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1226: `MOON_VECTOR_PAYLOAD_TEXT=off` made TextMatch match nothing | **FIXED** (a narrow case remains) | 6395586 | `tests/perf_ws17_payload_text_off.rs`, run with the setting off and on:<br>• red on `baseline-ae21476`: `inline TextMatch … must be an error, got "*1\r\n:0\r\n"`, and the FT.INFO field was missing;<br>• green 2/2.<br>`vector::filter::text_match_refusal` (3).<br>The refusal happens at parse time, so it covers the sync, yielding, cache and cross-shard paths. FT.INFO now reports `payload_text_index on\|off`, and the guide documents the setting. | Remaining case: schema-declared mode, payload text off, and TextMatch on a non-TEXT field still returns empty on the yielding path. The fix is one line in `capture_dense_knn_snapshot` (`ft_search/dispatch.rs`, WS8): return `None` when `idx.payload_index.text_match_refusal(f)` is `Some`. Apply it at integration. |
| moon#1226: prepared-state mismatch returned empty | **FIXED** | 984925b | `prepared_query_tests::prepared_state_without_the_sub_centroid_table_falls_back_to_a_local_lut` compares results bit for bit. Red: `"ADC LUT shorter than the segment's code layout"`, and a release build returns an empty result. | — |
| moon#1222: flaky `stream_effect` test | **FIXED** | 28aabe7 | Cause: `read_group_new` reads the clock once per stream, and outside a shard each read is a syscall.<br>• Red (stress test, same build): 944 of 100,000 runs diverged unpinned, 0 pinned.<br>• Green: 200/200 in-process runs across 8 threads. | — |
| moon#1228: declared-schema TEXT resolver | **FIXED** | 5c0a906 | `payload_filter::tests::bitmap_resolver_matches_the_scoring_resolver` gives results identical to HEAD. A/B: 30.4 → 15.9 ms. | — |
| moon#1228: HNSW prefetch line count | **FIXED**; aarch64 A/B **DEFERRED** | d359dac | `graph::tests::prefetch_hints_every_line_of_a_misaligned_row`. Red: `row of 20 B at line offset 45 — left 1 right 2`. | No aarch64 hardware available. |
| moon#1226: `PreparedTqQuery` allocated per query | **FIXED** | 2ff4284 | `a_single_graph_segment_query_builds_no_prepared_state`. Red: `left: 1, right: 0`. Saves 11–14 µs and a 64–128 KB allocation per single-segment query. | — |
| moon#1226: `results.clone()` on the no-session path | **PARTIAL** | 91b0a44 | New in-place API `retain_unseen_in_db`. Test: `retain_unseen_filters_in_place_without_copying`. | Switch the 3 call sites in `ft_search/dispatch.rs` (WS8) at integration. |
| moon#1226: EXACT far-query recall | **FIXED** (recall); CPU cost noted | 2205f86, e4d31c4, 379c6e3 | `mutable_leg_exact_rerank_recovers_far_query_recall` (3,000 × 384d):<br>• far-query R@10 0.703 → 0.990;<br>• in-distribution R@10 0.945 → 0.997;<br>• sync, MVCC and yielding results are bit-identical.<br>e4d31c4: rows beyond the f16 range keep their ADC estimate instead of an `inf` distance. This applies to immutable segments too. | CPU cost: see Measurements. A cheaper candidate selection is a follow-up. MiniLM validation is **DEFERRED**. |
| moon#1226: vector docs | **FIXED** | dacc1b2 | Updated the BUILD_MODE table, the insert/compaction/search steps and the memory table. | Re-measure post-compaction bytes per vector on Linux. |
| moon#1226: merged QJL mis-strided | **already fixed on base** | — | `merge.rs` keeps only a comment. | — |
| moon#1220 item 3: doc-id holes in `.tpost` | **FIXED** | d00bda0, b9e67df | `tpost_rewrite_compacts_doc_id_holes`.<br>• Red: `Invalid("doc ids too sparse")`, so the file was refused and the index rebuilt on every boot.<br>• Green: keys, score bits and tie order are identical, and the file re-encodes byte for byte. | `doc_terms` densification: not done. It is not counted in `estimated_bytes` (separate issue). |
| moon#1226: term-at-a-time scratch | **FIXED** | ec9782f, 9789b5a | Red: `scratch 480000 B for a 60,000-id window (bound: 32768 B)`. Time 16.99 → 16.54 ms. | — |
| moon#1226: `LeafTable::get` linear scan | **FIXED** | 1ed070d | Red: `2001000 comparisons for 2000 leaves`. | — |
| moon#1226: `TopK` heap-sorts when k ≥ n | **FIXED** | 20f92e5 | 25.3 → 7.06 ms. | — |
| moon#1226: `Chunks::insert` split capacity | **FIXED** | 2aa9597 | Red: 2.01× capacity to length. Green: ≤ 1.1×. | — |
| moon#1226: TEXT leaves always cloned | **FIXED** | d8d4963 | Red: `one-posting leaf must be borrowed`. | — |
| moon#1226: differential-oracle gaps | **FIXED** | 591de45 | TAG/NUMERIC answers now come from an independent model, and NUMERIC, TAG∧NUMERIC and 70K-doc shapes are added. A planted bug is caught: `"@num:[(9 (20]" 273 vs 264`. | The 70K test takes about 35 s in debug. |
| moon#1226: timing/RSS asserts in debug | **FIXED** | eb3590d, b874426 | Ratio tests now run only in optimised builds, a duplicate test is removed, and a vacuous bound is dropped. The position tests are serialised, and a doc-id-reuse golden is pinned. | — |
| moon#1226: BM25 order check | **FIXED** | 1de2740 | Checks score-descending order, doc-id tie order and in-order paging. | — |
| moon#1226: file sizes | **FIXED** | 0f0025f, 70e4050 | `posting.rs` 1754 → 1160 lines, `parse.rs` 1750 → 721 lines. `read.rs` is already 1488 on base. | — |
| moon#1226: root-container test | **FIXED** | d98b52b | The test skips when a mode-000 file still opens, which is the case for root. | — |

## Measurements
Method: release-fast lib-test build, in-binary A/B with alternating arms, 7 reps, median. The box is a 4-vCPU container, so the numbers are relative only.

**Mutable-segment rerank, MVCC scan (the path FT.SEARCH runs):**

| dim / n | cost at depth 4k | R@10 change | cost at depth 2k (R@10) |
|---|---|---|---|
| 384 / 1,000 | 112.0 → 190.8 µs (×1.70) | 0.829 → 0.995 | ×1.29 (0.968) |
| 384 / 5,000 | 225.4 → 358.1 µs (×1.59) | 0.808 → 0.979 | ×1.23 (0.935) |
| 768 / 5,000 | 665.3 → 1075.2 µs (×1.62) | 0.823 → 0.992 | ×1.24 (0.954) |

- The sync scan costs ×1.01–1.05.
- Most of the cost is the FastScan pre-filter pruning less against a deeper heap; the f16 work itself is small.

**Other A/Bs:**

| change | before → after |
|---|---|
| TAAT scoring | 16.99 → 16.54 ms |
| TopK | 25.32 → 7.06 ms |
| declared-TEXT resolver | 30.38 → 15.86 ms |
| prepared state | saves 11–14 µs per query |

## Cross-ownership edits
None outside the plan's list. The plan's test-only special cases were:
- `cold_index_rebuild_tests.rs`
- the `parse.rs` test move
- the `stream_effect` tests

## Risks for integration
1. **Mutable rerank cost:** ×1.6–1.7 on FT.SEARCH's mutable scan. The default compaction threshold of 1,000 keeps this to about +80 µs per query at 384d. Option: give the mutable leg a separate depth; depth 2 costs ×1.23–1.29 and reaches R@10 0.935–0.968.
2. **Results change by design:**
   - mutable-segment distances are now exact;
   - on tokio (no `text-index`), a full-text KNN filter now returns ERR;
   - FT.INFO has a new field;
   - rows beyond the f16 range keep their ADC estimate.
3. **`.tpost` bytes change** for indexes with doc-id holes. The v1 layout is the same, so older binaries still read the files.
4. **After WS8 lands:** apply the two small `ft_search/dispatch.rs` follow-ups (the TextMatch residual, and the 3 session call sites).
5. **Commit types:** besides perf/fix there are docs/test/refactor commits. Each carries an issue reference and the trailers.
6. **Shared-target aliasing:** seen once in an unpinned run. The pinned `MOON_BIN` passed 16/16.

## Gates (final tree bf73b12; last code commit e4d31c4)
- `fmt`, `audit-unsafe`, `audit-unwrap`: clean. No new `unsafe`.
- clippy on both legs and tokio `check --all-targets`: clean.
- Lib tests:

  | run | monoio | tokio |
  |---|---|---|
  | all WS17 modules | 1950/1950 | 1078/1078 |
  | vector modules, re-run at e4d31c4 | 1075/1075 | 914/914 |

- Every added or touched integration test is green by name on both runtimes.

## CHANGELOG bullets
- **Vector:** full-text KNN filters are refused when the payload text index is off (moon#1226). With `MOON_VECTOR_PAYLOAD_TEXT=off`, or in a build without `text-index`, an `@field:{multi word}` KNN prefilter used to match nothing silently. It now returns `ERR full-text KNN filter …`, and `FT.INFO` reports `payload_text_index on|off`.
- **Vector:** a prepared query no longer returns empty results when its collection lacks the sub-centroid table (moon#1226).
- **Vector:** the mutable segment is exact-reranked from its f16 rows (moon#1226).
  - Far-query R@10 goes from 0.70 to 0.99 on the test fixture.
  - Cost: 1.6–1.7× mutable-scan CPU.
  - Rows beyond the f16 range keep their ADC estimate.
- **Vector:** `PreparedTqQuery` is built only for queries that span two or more graph segments (moon#1226).
- **Vector:** SESSION filtering has an in-place API (moon#1226, partial).
- **Vector:** the declared-TEXT KNN filter resolves membership by bitmap, 1.9× faster (moon#1228).
- **Vector:** the HNSW prefetch covers misaligned code rows (moon#1228).
- **Docs:** the vector-search guide matches the post-QJL engine (moon#1226).
- **Text:** `.tpost` rewrites compact doc-id holes, so such files are no longer refused at boot, which forced a full index rebuild (moon#1220).
- **Text** (moon#1226):
  - term-at-a-time scoring uses a bounded scratch;
  - query leaves are found by binary search;
  - TopK sorts once when k covers every match;
  - single-posting leaves are borrowed rather than cloned;
  - posting runs no longer keep twice their memory.
- **Tests:**
  - the differential oracle is independent of the live indexes;
  - the shard-consistency suite checks BM25 order;
  - ratio asserts run only in optimised builds;
  - the root-container test skips where mode 000 cannot block reads;
  - the XREADGROUP replay test pins the clock (moon#1222);
  - the `posting.rs` and `parse.rs` tests moved out (moon#1226).

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.95 · Practicality 0.9 · Optimization 0.85 · Edge cases 0.9 · Self-evaluation 0.9.

- Optimization is below 0.9 because of the rerank's MVCC-scan cost.
- Completeness carries the two dispatch.rs follow-ups and the items blocked on hardware or data (aarch64, MiniLM).
