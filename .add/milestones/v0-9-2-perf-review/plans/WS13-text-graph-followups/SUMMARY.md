# WS13-text-graph-followups SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws13-text-graph-followups`, base `f32546c`, 8 commits.
> Personas: performance-engineer (lead), ci-test-integrity-engineer (moon#1219). Zero new
> `unsafe`. Binaries: `/home/user/wt/bin/ws13-base-f32546c`, `/home/user/wt/bin/ws13-final-630908f`
> (later commits change only tests, cfg attributes and notes).

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1219 ignored cross-shard FT consistency suites | **FIXED** | e926c1b (fixes), aff9809 | Each test spawns its own `--shards 1` and `--shards 4` servers (`MOON_BIN`, `ServerGuard` cleanup), own index name and prefix; seed checked via HSET added-field counts and `*` totals; a shard-spread check forces every answer of ≥4 keys onto ≥2 shards; every query must match between shards 1 and 4 AND an in-test oracle over an asymmetric fixture (13 TAG + 16 NUMERIC queries, upserts, exactly-once paging, identical errors). Stale assertions moved to the current contract (`{a\|b}` is a union; `numeric_filter_invalid`). 6 TAG + 5 NUMERIC tests pass, 0 ignored, on base, final and `baseline-935c555`. Proven able to fail: sabotaging `merge_text_results` (drop remote replies / drop remote totals / halve the page offset) turns 4/5 NUMERIC + 5/6 TAG, the same, and exactly the 2 paging tests red. | none |
| moon#1220 item 1: term-at-a-time scoring for wide prefix/fuzzy expansions | **FIXED** | 90d7ea2, beb1a15 | `oracle_tests::differential_term_at_a_time_matches_head` (forced TAAT vs forced cursors vs cost model, incl. DFS weights, `search_field_or`, AS_OF; ids, keys, order, total, score bits; asserts both paths ran); `score::tests::term_at_a_time_window_matches_cursor_scoring_bit_for_bit`; `tests/perf_ws13_text_prefix.rs` red on base (50-term prefix 8.7× → 3.0× one broad term, bound 6×). Server: `ka*` **132.8 → 29.6 ms (4.5×)**, `kalo*` 6.0× | MaxScore/WAND not needed after this |
| moon#1220 item 2: contiguous per-run positions (overlaps moon#884) | **PARTIAL** | e5c0c13 | `PosColumn` per run (end offsets + concatenated positions), runs bounded by 2048 positions and by entry count; `add_term_positions` inserts each (term, doc) once, removing moon#884's per-token `Vec`; `.tpost` byte-identical (golden hash pinned on the base layout). `perf_ws13_text_positions`: RSS per (term, doc) **107.6 → 48.4 B** (bound 75, red on base). 200K-doc RSS **683 → 433 MB (−37%)**; indexing CPU −15%. Oldest/newest upsert ratio 1.75 → 1.73 — NOT flattened | the upsert cost falls linearly with doc id on base too (not the Vec header); profile with `perf` on Linux |
| moon#1220 item 3: doc-id hole compaction, dense `doc_terms` | **DEFERRED** | — | PR #1221 added doc-id reuse, a free-id bitmap and a `.tpost` density guard to the same columns/decoder; renumbering on rewrite must build on that allocator, and with reuse holes stay bounded | after PR #1221, as its own issue |
| moon#1220 item 4: Cypher `RETURN … ORDER BY … LIMIT` projects only the top-k | **FIXED** | 28cddcf | New `executor/topk.rs`: only ORDER BY columns evaluated per row, bounded top-k buffer, only kept rows projected; streams scan → Filter/Unwind/single-hop Expand; declines on DISTINCT, aggregation, WITH, no LIMIT, LIMIT 0, non-total key orders (NaN, int > 2^53 next to float). `limit_sort_tests` +18 oracle queries and the fallbacks; `return_order_by_limit_takes_the_fused_top_k`; `perf_ws13_graph_topk` red on base (1.46× → 2.74× vs full evaluation, bound 2×). Server: **462 → 221 ms (2.1×)**; wide RETURN 3.0× | none |
| moon#1220 item 5: range IndexScan streams in chunks | **FIXED** | 630908f | `index_scan_keys` → visitor `index_scan_try_for_each` in `executor/index_scan.rs` (`read.rs` 1810 → 1468 lines); streamed-prefix and top-k paths stop at the LIMIT. 7 IndexScan oracle queries; `perf_ws13_graph_index_scan` (15K frozen + 15K live): **3.3× → 286.6×** faster than full evaluation (bound 15×; re-verified red after the restart at 3.3× and 4.0×). Server: **44 → 4.7 ms (9.4×)** | the index seed (bitmaps / mutable-tier range `Vec`) is still computed in full |

## Measurements
4-vCPU shared Linux x86_64 (relative only). `baseline-935c555`, `ws13-base-f32546c`,
`ws13-final-630908f`; raw-RESP Python client, one connection, identical data; `--appendonly no
--save "" --maxmemory 0 --disk-offload disable`; blocks interleaved across binaries.

FT.SEARCH `LIMIT 0 10`, 200K Zipf docs, shards 1, 3 blocks × median of 9 (ms); replies byte-identical:

| query | 935c555 | base | ws13 | base/ws13 |
|---|---|---|---|---|
| `ka*` (50 terms, 200K matches) | 3854 / 3509 / 3568 | 132.8 / 136.6 / 129.2 | 29.6 / 31.2 / 29.3 | 4.5× |
| `kalo*` (50 of 100 terms) | 1226 / 1380 / 1290 | 36.9 / 43.0 / 39.7 | 6.7 / 6.9 / 6.2 | 6.0× |
| `ka* @t:{red}` | 3200 / 3231 / 3315 | 61.9 / 64.0 / 60.6 | 27.7 / 28.0 / 31.9 | 2.2× |
| `%kalomi%` (control) | 19.0 / 19.8 / 19.2 | 1.36 / 1.26 / 1.33 | 1.31 / 1.30 / 1.33 | 1.0× |
| rank-0 term (control) | 578 / 601 / 581 | 8.1 / 8.1 / 7.7 | 7.8 / 7.9 / 7.4 | 1.0× |

Upsert at 200K docs (150 reps, median µs): 935c555 doc0 17,047 / docN 375 (45.5×); base 499 / 286
(1.75×); ws13 479 / 276 (1.73×). Indexing CPU for 100K docs: medians 5.19 → 4.42 s (0.85×, sign never
flips). RSS after 200K docs: 935c555 +665 MB, base +683, ws13 +433 (−37%); 100K tagged docs: +212 /
+132 / +114 MB.

GRAPH.RO_QUERY, 200K nodes, 3 blocks × median of 5 (ms), rows identical:

| query | 935c555 | base | ws13 | base/ws13 |
|---|---|---|---|---|
| `RETURN n.x, n.y ORDER BY n.x DESC, n.y LIMIT 10` | 925 / 591 / 944 | 468 / 470 / 455 | 221 / 213 / 230 | 2.1× |
| 4-column RETURN ordered by 2 keys | 1204 / 1299 / 1308 | 335 / 572 / 272 | 106 / 234 / 111 | 3.0× |
| `WHERE n.y >= 700 RETURN … LIMIT 10` (range IndexScan) | 315 / 334 / 358 | 42.9 / 44.2 / 46.1 | 4.3 / 7.5 / 4.7 | 9.4× |
| `WITH n ORDER BY … LIMIT 10` (control) | 1594 / 1393 / 1436 | 273 / 270 / 256 | 276 / 272 / 257 | 1.0× |

## Cross-ownership edits
None: `src/text/**`, `src/graph/**`, `tests/inverted_search_*_consistency.rs`, `tests/perf_ws13_*.rs`
and this plan dir only.

## Risks / things the orchestrator must re-check at integration
1. **Conflict with PR #1221** in `read.rs` `execute_with_slots`: keep WS13's `while` loop and call `apply_op(op, &mut st, &env, demand[i], 0)?`. Semantic (no textual conflict): `topk.rs`'s streamed-segment `apply_op` needs a 5th argument `0` (only single-hop Expand streams there, so the var-length cap does not apply).
2. `src/text/posting.rs` is 1702 lines (> 1500): its test module stays inline because PR #1221 inserts a test into it — split into `posting_tests.rs` as a pure move after integration.
3. Re-run `tests/perf_ws13_text_positions.rs` after merging PR #1221 (pins the `.tpost` golden hash `0xe4dc7f45658079e0`; expected to pass — encoding unchanged, the corpus never allocates after its deletions).
4. Also re-run after merging: `cargo check --lib --tests`, `text::query::eval::oracle_tests`, `graph::cypher::executor::limit_sort_tests`, both consistency suites with `MOON_BIN` = the merged build.
5. API: new `PostingStore::add_term_positions`; `PostingList.positions` is a `PosColumn` (public `positions_for`, `position_lists`, `add_term_occurrence`, `from_parts` unchanged); `index_scan_keys` moved to `executor/index_scan.rs`, re-exported from `read.rs`.
6. Keep moon#1220 open (item 3 and the upsert slope); commits say `refs`, not `fixes`.

## Gates (final tree `beb1a15`)
fmt 0; clippy `--all-targets` WS13 targets clean (the only failure is the pre-existing
`tests/perf_ws6_aof_record_alloc.rs`, fixed on PR #1221); clippy tokio+jemalloc+text-index 0; clippy
`--lib` tokio+jemalloc 0; tokio `check --all-targets --keep-going` fails only on the pre-existing
`benches/text_search.rs` (fixed on PR #1221). Lib filters `text::`, `command::vector_search`,
`graph::`, `command::graph`: 1120 passed; 21 default-feature integration suites green (`fts_*`,
`ft_*`, `graph_*`, `perf_ws5b_*` ×4, `perf_ws13_*` ×4, both consistency suites; `MOON_BIN` =
ws13-final); 16 tokio+graph+text-index suites green.

## Self-evaluation (0–1)
Completeness 0.82 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.92 ·
Self-evaluation 0.9 — item 3 deferred on purpose (PR #1221 owns that allocator), item 2's upsert
goal unmet because the measured cause predates this work and needs `perf` on Linux.
