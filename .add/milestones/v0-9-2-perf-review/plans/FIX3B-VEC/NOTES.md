# FIX3B-VEC — working notes (review fixes for PR #1242, vector + FT.SEARCH)

## Context loaded
CLAUDE.md, SECURITY.md (wording rule for item 2), personas `performance-engineer` (lead) and
`ci-test-integrity-engineer`, WS17 SUMMARY, the reviewer's proof tests
(`/home/user/wt/handoff/review3b_vec_rerank_proofs.rs`), and the files named in the brief.
Base `07d9850` (the #1242 head), branch `fix/3b-vec-review`. Ports 7600–7619.
Shared target `/home/user/wt/target`, `CARGO_INCREMENTAL=0`, `MOON_DISK_FREE_MIN_PCT=0`.

## Item 1 — SHOULD-1: filtered yielding scan reranks at the wrong depth

### Mechanism (verified in code, then red)
- `search_mvcc` (holder.rs ~L719): the mutable leg keeps the ADC top
  `exact_rerank_depth(k, mult)` = `4·k`, reranks from `raw_f16`, truncates to `k`. It applies the
  filter bitmap exactly inside the scan, whatever the strategy.
- `search_mvcc_yielding_with_pool` (holder.rs ~L1043): under `FilterStrategy::HnswPostFilter`
  `fetch_k = 3·k` (the GRAPH legs' oversample, needed because they traverse unfiltered and
  post-filter). The mutable leg reused `fetch_k` for BOTH `scan_k = exact_rerank_depth(fetch_k)` =
  `4·3k` and the rerank truncation (`3k`). The mutable scan passes `filter_ref`, so it is already
  exactly filtered: the extra `3×` buys nothing for the merge (only its top `k` can reach the global
  top `k`) and changes WHICH `k` survive the rerank — an item outside the ADC top `4k` but inside
  the ADC top `12k` can enter the exact top `k` on the yielding path only.
- `search_filtered` HnswPostFilter arm (holder.rs ~L588): same shape, `brute_force_search_reranked(
  oversample_k = 3k, filter_bitmap, mult)` → ADC top `12k`, reranked, `3k` kept. No production
  caller passes a filter to `search_filtered` (grep: FT.SEARCH / FT.RECOMMEND / hybrid use
  `search_mvcc` or the yielding scan; `search` passes `None`), but it is the documented sync twin.
- The strategy is chosen at capture by `select_strategy`: HnswPostFilter needs ≥ 20K matching
  vectors AND > 80 % selectivity. So the divergence needs a large index with a broad filter; the
  plain FT.SEARCH (yielding) and the same query with RANGE/SESSION (sync `search_mvcc`) then differ.
- Red (lib test, this worktree, base code): the existing identity test extended with a 90 % filter
  and the three filtered strategies forced into the snapshot:
  `1 filtered (strategy, query) pairs: search_mvcc != yielding; first: ("far", HnswPostFilter, …
  (1954, …)] vs […(994, 1071909176), (1038, …)])` — the yielding path's deeper rerank admitted
  doc 994. Reviewer's proof (128d random directions): 2 of 40.
- Red at a size where the capture itself picks HnswPostFilter (new test
  `post_filter_strategy_scans_agree_with_search_mvcc`, 20,500 × 64d COSINE TQ4 EXACT, 98 % filter,
  `select_strategy` asserted to return HnswPostFilter, 30 far queries):
  - `search_filtered` arm reverted (first version of the test, sync leg only): `19 of 30 queries:
    search_filtered != search_mvcc`;
  - `search_filtered` fixed, yielding reverted: `left: (0, 19)` (search_filtered, yielding).
  So at a realistic size the divergence is the common case, not a 1-in-40 edge.

### Design
- Mutable leg uses `k` for both the rerank depth (`exact_rerank_depth(k, mult)`) and the truncation,
  in the yielding scan and in `search_filtered`'s HnswPostFilter arm. The graph legs keep `fetch_k` /
  `oversample_k` (their post-filter needs it). One-line changes; holder.rs does not grow.
- Regression: the committed identity test (`mutable/rerank.rs`) now also runs a 90 % filter through
  `search_filtered`, `search_mvcc` and the yielding scan under BruteForceFiltered, HnswFiltered and
  HnswPostFilter; all must be bit-identical. Plus `post_filter_strategy_scans_agree_with_search_mvcc`
  for the natural HnswPostFilter size, which is the only test that reaches `search_filtered`'s arm.
- Green: both tests pass (`2 passed … 6613 filtered out`, 8.3 s debug, this worktree's build).

### Risks
- Result change on the yielding path under HnswPostFilter with a non-empty mutable segment: the
  mutable leg now reranks `4k` instead of `12k` candidates, so it can no longer find a true neighbour
  sitting between ADC rank `4k` and `12k` — exactly what `search_mvcc` and every other strategy
  already do. Parity over a marginal recall gain the other paths never had; RERANK_MULT still
  deepens all paths alike.
- CPU: strictly less work (heap of `4k` instead of `12k`, `4k` f16 rows instead of `12k`).

## Item 2 — KNN prefilter condition limit

### Mechanism (verified in code)
- `parse_filter_string` (`ft_search/parse.rs`) reads space-separated `@field:{…}` / `@field:[…]`
  conditions into a `Vec` and folds them into a left-deep `FilterExpr::And` chain. Nothing bounds the
  count. It is the one grammar behind both entry points: the inline `<prefilter>=>[KNN …]` prefix
  (`parse_inline_filter`) and the explicit `FILTER` clause (`parse_filter_clause`), at `--shards 1`
  (`ft_search/dispatch.rs:143`, `:567`) and at `--shards > 1` (`parse_ft_search_args`,
  `ft_search/response.rs:238`, called by both runtimes' handlers before the multi-shard FILTER
  refusal). So a limit enforced there answers identically on every path.
- Evaluation (`payload_index.rs` `evaluate_bitmap_with`): every condition resolves a bitmap over the
  index's payload and is intersected — per-query cost grows with the condition count.
- `HybridFilter` (FT.HYBRID's wire filter) already bounds its own input at parse time: depth ≤ 4,
  ≤ 16 leaves (`hybrid_filter.rs`). The KNN prefilter grammar had no counterpart.
- Existing prefilters in tests/scripts: 35 with one condition, 5 with two; none above two.

### Design
- `MAX_KNN_FILTER_CONDITIONS = 128`: an input limit for parity with HybridFilter's parse-time
  limits and for bounded evaluation cost. The grammar is a flat AND (no OR/NOT/nesting), so it gets
  more room than HybridFilter's 16 leaves: 8× that, and 64× the largest prefilter in the repo's own
  tests. A flat AND of more than 128 conditions has no use a narrower filter cannot express.
- Enforced inside the parse loop: when a 129th condition starts, return `None` before parsing it
  (bounded parse work too) → `FilterParse::Invalid(ERR_INVALID_FILTER)` → the existing
  `ERR invalid FILTER expression`. No new error text, so clients see the same reply as for any other
  unreadable filter.
- Tests: unit tests in `parse.rs` (exactly-at-limit parses to 128 leaves; limit + 1 is `None`, and
  `Invalid` through both entry points with the exact wire text); integration rows in
  `tests/perf_fix_1238_inline_prefilter.rs` (its helpers already compare every shard count against
  `--shards 1`): at-limit inline answers the filtered keys at shards 1/2/4, limit + 1 inline and
  explicit FILTER answer the ERR at shards 1/2/4.

### Risks
- Behaviour change: a prefilter of 129+ conditions that used to run now answers the ERR. No such
  filter exists in the repo; a client that builds one programmatically gets the documented error
  text instead of a result.
- TextMatch term count (`@f:{a b c …}`) is one condition with N terms; it is not counted by this
  limit (terms are bounded by the query's own length and resolve as one posting-list AND). Noted,
  not changed — the brief scopes the limit to conditions.

### Evidence
- Unit (lib, monoio, this worktree's binary verified by test-name marker): the limit constant
  present but the check disabled → `a_prefilter_over_the_condition_limit_is_an_invalid_filter`
  FAILED `129 conditions`; with the check → `2 passed`.
- Integration (`perf_fix_1238_inline_prefilter`, debug monoio bin built from this tree, copied out of
  the shared target, `MOON_BIN` pinned): check disabled → the new test FAILED — the 129-condition
  FILTER answered 4 rows at `--shards 1` (`*9 … d:1 d:4 d:7 d:10`) instead of the ERR; with the
  check → `6 passed` (all six tests of the file, shards 1/2/4).

## Item 3 — NIT-3: the in-place SESSION comment and its self-comparing test

### Mechanism (verified in code)
- `ft_search/dispatch.rs` hybrid (~L271) and sparse-only (~L336) SESSION blocks: `fused` is a
  `Vec<SearchResult>`, but `retain_unseen_in_db` took `&mut SmallVec<[_; 32]>`, so both did
  `fused.drain(..).collect()` into a SmallVec (a copy; a heap allocation past 32 results), filtered,
  then `into_vec()` (a second copy when inline). The comment said "filtered in place — no copy".
  Both then did `fused.into_iter().collect()` into another SmallVec for `record_session_results`
  (third copy), which only reads its input.
- `session_tests::retain_unseen_filters_in_place_without_copying` took its expectation from
  `filter_session_results_in_db`, which since WS17 is `clone()` + `retain_unseen_in_db` — the
  function compared with itself. Only its `len() == 44` check was independent.

### Design
- Make the claim true rather than weaken it: a tiny `session::ResultBuffer` trait (retain in
  place), implemented for `Vec<SearchResult>` and `SmallVec<[SearchResult; 32]>`;
  `retain_unseen_in_db(&mut impl ResultBuffer, …)`. The dense call site is unchanged; the hybrid and
  sparse call sites filter `fused` directly. `record_session_results` takes `&[SearchResult]`
  (every caller's `&SmallVec` deref-coerces), so those paths record by reference. Net: up to three copies
  per SESSION query removed on each of the two paths, no new allocation, nothing new in the
  hot-path modules.
- Test: expectation computed by a plain filter in the test (`(0..48).filter(!seen)`); the in-place
  pointer check now covers the `Vec` too; the borrowed form is checked against the same reference.
- New end-to-end guard `sparse_and_hybrid_session_searches_skip_seen_documents`: FT.SEARCH … SPARSE
  … SESSION (sparse-only and KNN + SPARSE) through `ft_search`: first search returns doc:1 and
  doc:2, the repeat returns none, another session returns both. These two call sites had no test.
  It holds `METRICS_LOCK` (made `pub(super)`, with the three hybrid fixtures in `tests.rs`) because
  `ft_search` bumps the global search counter another test asserts exactly.

### Evidence (guards shown able to fail)
- Mutation: the session lookup reads `key_hash + 1` (drops the wrong 4 of 48 — same count):
  the pre-fix test body (verbatim from 07d9850, run as a temporary test) PASSES; the new
  `retain_unseen_filters_in_place_without_copying` FAILS (`SmallVec: left [1001, 1004, …] right
  [1000, 1002, …]`); `in_db_filter_matches_snapshot_filter` and the new e2e test fail too.
- Mutation: the two dispatch.rs retain calls removed: only the new e2e test fails
  (`sparse: both already seen — left [doc:1, doc:2] right []`).
- Green: `session_tests` 5 passed.

### Risks
- `retain_unseen_in_db` is now generic (`impl ResultBuffer`); `record_session_results` takes a
  slice. Source-compatible for every in-tree caller (grep src/ tests/ fuzz/ benches/). No behaviour
  change: the same survivors in the same order.
- Remaining copies on these paths (not claimed in-place by any comment): the RANGE filter still
  `drain().collect()`s `fused` into a SmallVec because `apply_range_filter` takes a SmallVec.
  Out of scope; noted.

## Item 4 — NIT-2: recall loss on tiny L2 rows

### Mechanism (verified by measurement)
- `raw_f16` stores every row as f16. Below 2^-14 (6.1e-5) f16 is subnormal: a fixed absolute step
  of 2^-24 (5.96e-8), so a row whose components sit at ~1e-7 keeps one or two bits per component.
  The TQ ADC estimate quantizes the unit direction and keeps the norm, so its relative error does not
  depend on scale. For L2 the rerank therefore replaces a scale-invariant estimate by a coarse one.
- Temporary in-test sweep (debug lib test, mutable segment, TQ4 EXACT, 800 Gaussian rows,
  40 queries, R@10; "always" = today's rerank; "Tn" = keep ADC when every component is subnormal
  and the row's RMS is below n steps):

  | L2, 32d | ADC | always | T2 | T3 | T4 |
  |---|---|---|---|---|---|
  | 5e-8 | 0.848 | 0.618 | 0.848 | 0.848 | 0.848 |
  | 1e-7 | 0.848 | 0.767 | 0.848 | 0.848 | 0.848 |
  | 1.5e-7 | 0.848 | 0.845 | 0.833 | 0.848 | 0.848 |
  | 2e-7 | 0.848 | 0.880 | 0.880 | 0.853 | 0.848 |
  | 3e-7 | 0.848 | 0.917 | 0.917 | 0.917 | 0.907 |
  | 5e-7 … 1 | 0.848 | 0.963 … 1.000 | = always | = always | = always |

  128d: same shape (ADC 0.823; always 0.677 / 0.780 at 5e-8 / 1e-7, 0.853 at 1.5e-7; T3 keeps
  0.823 at ≤ 1.5e-7, 0.865 at 2e-7 vs 0.890, identical from 3e-7).
- The literal "keep ADC when the row's f16 norm is subnormal" (norm < 6.1e-5, i.e. every component
  subnormal and more) is the wrong cut: the sweep's all-subnormal variant answered 0.848 (ADC) at
  1e-6 … 1e-5 where the rerank answers 0.975 … 0.998. It would give back most of WS17's gain for
  any small-scale L2 corpus to fix a 1e-7 corner.
- The break-even follows from the error sizes: f16 rounding error is uniform in ±½ step, RMS
  step/√12 ≈ 1.7e-8 per component; TQ4's relative RMS error per coordinate is about 0.1. They meet at
  a component RMS of ≈ 1.7e-7 ≈ 3 steps — where the sweep crosses over (1.5e-7 → 2e-7).
- Cosine / IP: NOT affected. At the same scales the rerank stays far above ADC (32d: ADC 0.230,
  rerank 0.440–0.487 at every scale), and a fallback there HURTS badly in the transition band
  (T2 at 1.5e-7: 0.052; T3 at 2e-7: 0.028): the mutable ADC cosine estimate is not on the rerank's
  `2 − 2·cos` scale, so mixing the two within one candidate list mis-orders them. So the fallback is
  L2-only.

### Design
- `exact_f16_distance`, L2 branch only: `None` (the caller keeps its ADC estimate, as for a
  non-finite result) when `f16_row_below_rerank_precision(row)`: every component is subnormal
  (exponent bits 0) AND Σ mantissa² < 9·dim, i.e. RMS < 3 subnormal steps.
- Exact: integer arithmetic on the stored bits — no float rounding, identical on every SIMD tier.
  Cheap: the loop returns at the first component with a nonzero exponent, which for any row at a
  normal scale is component 0; only rows that are wholly subnormal pay a full integer pass (≤ `4k`
  rows per segment per query).
- Shared by the mutable and immutable reranks (both call `exact_f16_distance`).
- Tests (rerank.rs): the reviewer's shape as a committed red/green — L2 32d at 1e-7 must not lose
  recall vs ADC — plus a guard that at 1e-6 the rerank still gains ≥ 0.05 over ADC (fails for the
  over-broad all-subnormal cut), plus bit-level unit tests of the predicate in prepared.rs.

### Evidence
- Red (base code, new test `tiny_l2_rows_keep_their_adc_estimate_where_f16_is_coarser`):
  `L2 scale 1e-7 R@10: ADC 0.848 -> reranked 0.767` FAILED.
- Green: `L2 1e-7: 0.848 -> 0.848`, `L2 1e-6: 0.848 -> 0.975`, `Cosine 1e-7: 0.230 -> 0.470`;
  predicate unit tests 2/2; `rows_outside_the_f16_range_keep_their_adc_rank` and the far-query
  identity test still green (R@10 far 0.703 → 0.990, in 0.945 → 0.997 — unchanged).
- The guards can fail: predicate mutated to "every wholly-subnormal row" → FAILED at
  `L2 1e-6: 0.848 -> 0.848`; fallback applied to every metric → FAILED at `Cosine 1e-7: 0.230 ->
  0.230`.

### Risks
- A transition band (RMS ≈ 3–5 steps, components ≈ 2e-7–3e-7) forgoes up to 0.03 R@10 of rerank
  gain, never below ADC in the sweep. A threshold of 2 steps keeps that gain but dipped 0.015 below
  ADC at 1.5e-7 (32d); 3 is the conservative choice matching the error model.
- An all-zero L2 row now keeps its ADC estimate instead of the (exact) `‖q‖²`: an f16 zero row cannot
  be told apart from a row below half a step. The ADC estimate of a zero-norm row is `‖q‖²` too.
- WARM segments do not call `exact_f16_distance` (their own inline copy in
  `persistence/warm_search.rs`), so they neither keep ADC for non-finite rows (e4d31c4) nor for
  these rows. See the cross-ownership note below.

## Item 4b (cross-file, own commit) — WARM rerank on the shared distance

### Mechanism
- `WarmSearchSegment::rerank_exact` (`persistence/warm_search.rs`) documents that it "mirrors
  `ImmutableSegment::rerank_exact` exactly", but kept its own inline copy of the distance: it
  always overwrote the ADC estimate with the f16 distance. Since e4d31c4 (moon#1226) the HOT
  reranks keep ADC for non-finite rows, and since item 4 for L2 rows below f16 precision; WARM did
  neither, so the same row ranked differently once its segment aged HOT → WARM.
- Red (new `warm_rerank_scores_rows_like_its_hot_source`, 600 clustered 64d L2 rows with the f16
  sidecar, HOT searched, moved WARM through the store's own transition, searched again; data and
  queries × scale): `1e-7: WARM must rerank like HOT` FAILED; with the list cut to `[1e5, 1]` (the
  pre-existing non-finite case alone): `1e5: WARM must rerank like HOT` FAILED.

### Design
- WARM calls `hnsw::prepared::exact_f16_distance` like HOT (replaces 8 lines with 5). The test
  fixture `hot_then_warm` gains a `scale` argument (the two existing callers pass 1.0) so queries sit
  at the data's scale — with unit queries against 1e-7 rows every distance ties and the comparison
  measures tie-breaking, not the rerank.
- Green: `vector::persistence::warm*` 30 passed (control scale 1.0 identical as before).

### Risks
- Result change on WARM segments, only for rows the HOT rerank already treats this way (non-finite
  f16 rows, L2 rows below f16 precision). Everything else is bit-identical (control case).
- File outside the brief's named list (`src/vector/persistence/warm_search.rs` and
  `warm_sub_signs_tests.rs`); no other agent's branch touches them (checked WS16, WS19, FIX-MAINCI
  diffs against 07d9850).

## Instrument notes (shared target)
- Cargo handed this worktree another worktree's artifacts several times (its fingerprints are
  path-relative and mtime-based, so a newer foreign build of the same unit looks fresh): one lib-test
  run listed 6617 tests without this branch's new ones, and two integration builds produced a test
  exe + `moon` bin that FAILED the new tests with base behaviour (proof test 0.853 → 0.762).
  Every counted run below was verified: lib tests by this branch's test names in `--list`; integration
  runs by the symbol `f16_row_below_rerank_precision` in the copied `moon` bin (and test exe where it
  links the engine), rebuilding after `touch` when absent.

## Gates (final code tree; last code change 1bcf2ed is comment-only)
- `cargo fmt --check`: exit 0. `scripts/audit-unsafe.sh`: PASSED (0 missing SAFETY; no new unsafe).
  `scripts/audit-unwrap.sh`: PASSED (within baseline).
- `cargo clippy --all-targets -- -D warnings`: first run FAILED on this branch's own doc line
  (`doc_lazy_continuation`, a wrapped "> 80 %"), fixed in e920441; rerun exit 0.
- `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings`: exit 0.
- `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc`: exit 0.
- Lib tests, filter `vector` (every `vector::*` and `command::vector_search::*` incl. ft_search):
  - monoio: 1123 passed, 0 failed, 9 ignored.
  - tokio: 941 passed, 1 failed, 8 ignored — `vector::store::bg_compact_tests::
    test_bg_compact_pool_parallelism` (`single=15.2s, parallel(K=3)=30.2s`, a wall-clock ratio
    assert, box load ~10 on 4 vCPU). The same copied binary passes it alone (22 s). This branch
    touches no compaction code. Not called flaky: reported for a re-run under the gate's load.
- Integration, by name, pinned `MOON_BIN`, provenance-checked:
  - `perf_fix_1238_inline_prefilter` monoio: 6 passed (shards 1/2/4); tokio: 5 passed (the
    declared-schema test is `text-index`-gated, absent on the tokio leg).
  - reviewer's `review3b_vec_rerank_proofs.rs` (copied in uncommitted, removed after): 2 passed —
    `small-magnitude L2 R@10: ADC-only 0.853 -> f16-reranked 0.853`,
    `sync_and_yielding_scans_agree_under_hnsw_post_filter` ok.

## Item 5 (optional) — not done
The K=100 `rerank_cost_ab` cell needs a release-fast lib-test build and a quiet box; this run's
shared 4-vCPU box sat at load ~10 with 4.8 GB free, so a number from it would not be trustworthy.

## SUMMARY.md
The harness refused writing SUMMARY.md from this agent; its content is in the final report
(TEAM-RULES precedent: WS17).

## Self-scores (0–1): Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation
- Item 1: 0.95 · 0.95 · 0.95 · 0.95 · 0.9 · 0.9. Edge cases held at 0.9: only the mutable leg is
  aligned; `search_mvcc` still passes the bitmap to graph segments (ACORN) where the yielding scan
  post-filters them under HnswPostFilter — a separate, pre-existing strategy difference (risk for the
  orchestrator), not this finding.
- Item 2: 0.95 · 0.95 · 0.95 · 0.95 · 0.9 · 0.9. TextMatch terms inside one condition are not
  counted (one condition, bounded by the query length).
- Item 3: 0.95 · 0.9 · 0.95 · 0.9 · 0.9 · 0.9. The RANGE path's `drain().collect()` copies remain; no
  comment claims otherwise.
- Item 4: 0.9 · 0.9 · 0.9 · 0.9 · 0.9 · 0.9. A narrow transition band forgoes ≤ 0.03 R@10 of gain
  (never below ADC); the Cosine ADC-scale mismatch for rows that keep ADC is pre-existing and reported.
- Item 5: not done (see above).
