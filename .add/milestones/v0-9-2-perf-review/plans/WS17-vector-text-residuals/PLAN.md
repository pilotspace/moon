# WS17-vector-text-residuals — PLAN (wave 2, part 3; runs alongside WS7/WS8/WS10)
personas: `.add/personas/performance-engineer.md` (lead) · `.add/personas/ci-test-integrity-engineer.md` (test hygiene items)

Base: main `ae21476` + plan commits. Baseline binary `/home/user/wt/bin/baseline-ae21476`. Ports 7340–7359.
Everything here is small and independent: one commit per checkbox (or per issue where the issue is small), `refs moon#1226` / `(moon#NNNN)`.
Read first: `plans/WS5a-vector-engine/NOTES.md`, `plans/WS11-vector-followups/NOTES.md`, `plans/WS5b-text-graph/NOTES.md`, `plans/WS13-text-graph-followups/NOTES.md`, and moon#1226 (body + comment), moon#1228 ("Vector (WS11) small items"), moon#1220 (comments say what remains), moon#1222.

## Issues (correctness first)
1. **moon#1226 vector correctness**
   - `MOON_VECTOR_PAYLOAD_TEXT=off` → every TextMatch KNN prefilter silently matches nothing (`payload_index.rs:57`, the `spsc_handler.rs:~4537` use is WS8's file — do not edit it; solve at the payload-index / FT.SEARCH parse layer): answer a clear `ERR` for TextMatch when disabled (preferred) and expose the setting in FT.INFO.
   - Prepared-state mismatch returns empty silently (`search.rs:507,528` `p.lut32().unwrap_or(&[])`): fall back to building the LUT locally; test that forces the mismatch.
2. **moon#1222 (test flake)** `a_read_is_one_forced_claim_per_stream_that_replays_exactly`: pin the thread-local clock (`tl_clock_set`) and audit the sibling tests in `stream_effect.rs` for the same hazard. Evidence: 200 consecutive runs green (`cargo test --lib <name> -- --test-threads 8` in a loop), and the reasoning why it could fail before.
3. **moon#1228 vector small items**
   - `MOON_VECTOR_PAYLOAD_SCHEMA=declared` BM25 TextMatch resolver scores every matching doc and allocates per hit (`src/command/vector_search/ft_search/payload_filter.rs:~77-83`): intersect postings into a bitmap; result-identity test vs the current resolver.
   - HNSW prefetch line count ignores row misalignment (`src/vector/hnsw/graph.rs:~310`: an unaligned 516 B row spans 10 lines, 9 hinted): compute from `(addr & 63) + bpc`; unit test of the arithmetic. aarch64 A/B is DEFERRED (no hardware) — say so.
4. **moon#1226 vector perf**
   - `PreparedTqQuery` allocates a 32–128 KB zeroed LUT + rotated query per query even for single-segment queries (`prepared.rs:206,218`, `holder.rs`): build only for ≥2 graph segments / pooled path, or thread-local scratch. Allocation-count test.
   - `results.clone()` on the no-session path (`session.rs:70`): filter in place.
   - EXACT far-query recall 0.831 → 0.728 (moon#1207/#1208 change): mutable-segment exact rerank using the `raw_f16`/`raw_f32` the segment already keeps (as immutable segments' `rerank_exact`); recall test on the existing fixture (report the number; MiniLM validation is DEFERRED — no real embeddings here); update `docs/vector-search-guide.md:61-72` (still describes EXACT with "QJL correction" and 8.6 s compaction).
   - (Merged-QJL stride item: already resolved on `ae21476` — `merge.rs` has no QJL code after WS11. Just confirm it in SUMMARY.)
5. **moon#1226 + #1220 text/graph**
   - moon#1220 item 3: compact doc-id holes when `.tpost` is rewritten (builds on the doc-id reuse allocator); `PostingStore::doc_terms` HashMap → dense structure if the profile/size numbers justify it.
   - Term-at-a-time scratch (`src/text/score.rs:~273-295`): cap the `len_norm`/`best` window (e.g. 4 × candidates) or fold in fixed id blocks with per-term resume points.
   - `LeafTable::get` linear scan (large ORs O(leaves²)); `TopK` heap-sorts when k ≥ n; `Chunks::insert` `split_off` leaves the left run at 2× capacity; TEXT leaves always cloned.
   - Differential oracle gaps: pure NUMERIC shape, TAG∧NUMERIC shape, a corpus > 65K docs (bitmap containers).
   - Timing / RSS-ratio asserts in debug on shared runners (`upsert_cost_is_flat_across_doc_position`, `limit_sort_tests` timings, `perf_ws5b_text_billing` 1.24 vs 1.5, `perf_ws4_multibulk_linear` ratio, `perf_ws13_graph_topk` bound that passes on base): `#[ignore]` with a reason or gate to release; remove the duplicate upsert test; `perf_ws13_text_positions` VmRSS test serialised from the golden-corpus test + a doc-id-reuse golden case.
   - Consistency suites compare key sets only — add an order check for BM25-scored queries where the merge order is deterministic.
6. **File-size rule (CLAUDE.md: ≤1500 lines)** — pure moves, no logic change, one commit each: `src/text/posting.rs` (1744), `src/protocol/parse.rs` (1750 — move tests to a `tests.rs` submodule). (`src/graph/cypher/executor/read.rs` is already 1488 on `ae21476` — confirm only.)
7. **Root-container test** `storage::tiered::cold_index_rebuild_tests::unreadable_file_is_counted_and_skipped_never_queued_for_unlink`: skip when euid is 0, with a note (it is the only lib failure in root containers). Test-only edit.

## Owned files
`src/vector/**`, `src/text/**`, `src/graph/**`, `src/command/vector_search/**` EXCEPT `ft_search/dispatch.rs` (WS8 edited it), `src/protocol/parse.rs` (test move only), `src/replication/**` stream_effect tests only (WS7 owns `state.rs`/`backlog.rs`), `src/storage/tiered/cold_index_rebuild_tests.rs` (item 7 only), `docs/vector-search-guide.md`, the named `tests/perf_ws4_*`, `tests/perf_ws5b_*`, `tests/perf_ws13_*` files, consistency-suite test files for text, tests `tests/perf_ws17_*.rs`.

## Not yours
`src/server/conn/**` (WS7), `src/shard/**` (WS8), `src/storage/**` other than item 7, collections/listpack (WS10), persistence (WS15/WS16), `src/pubsub/**`, `src/command/string/**`.
