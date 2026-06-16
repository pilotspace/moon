# MILESTONE: FTS Hardening

goal: Moon's full-text search is trustworthy and competitive: every query combinator (term, AND, OR, TEXT+TAG, NUMERIC) returns correct results with true total-matched counts, and indexing plus high-DF queries run without the O(V)/O(M^2) cliffs the 2026-06-16 benchmark exposed.
rationale: new-major (split 1/3) — headline slice of the v3 "secondary-engine correctness & parity" theme opened from the 2026-06-16 deep review + 4-feature GCloud benchmark (FTS was the benchmark's biggest gap vs RediSearch and was previously un-benchmarked). No closed milestone (v1 shared-nothing, v2/v2-1 throughput) covers FTS correctness/parity. This slice owns FTS's O(M^2)/O(V) perf cliffs + the OR/combo/count correctness defects + a latent query-routing panic. Graph (v3-2) and Vector/KV (v3-3) are sibling slices.
stage: production · status: active · created: 2026-06-16

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:
- High-DF term query no longer O(M^2): replace the O(N) `.position(|id| id == doc_id)` TF lookup
  with a rank-based lookup (e.g. `RoaringBitmap::rank`). `src/text/store.rs:507,773`.
  (bench: term_hi 419 ms / 19 qps vs RediSearch 2 ms / 3,813 qps.)
- Bulk indexing no longer O(V) per doc: replace the per-doc posting upsert scan with an
  incremental update. `src/text/posting.rs:139`. (bench: 376 vs 18,052 docs/s, ~48×.)
- `OR` (`|`) returns the union of matched docs; `TEXT+TAG` combined query returns the intersection
  (non-empty when matches exist). (bench: OR total 10 vs 2,072; combo 0 vs 253.)
- FT.SEARCH reply reports the true total-matched count, not the returned-page count.
  (bench: TAG 10 vs 5,064 — an FT.SEARCH protocol deviation.)
- Query routing + robustness: `is_text_query()` recognizes `SPARSE` (no misroute of sparse-vector
  queries to the BM25 path) and the FT query path removes its 3× `expect()` panics.
  `src/command/vector_search/ft_text_search.rs:1004`, `src/text/store.rs:463`.

Out:
- BM25 ranking-quality tuning / new analyzers — this is correctness of result SETS + counts + speed,
  not relevance-score changes.
- Aggregation/GROUPBY speed (bench 6 vs 11 qps) and NUMERIC-range query speed (~3× slower) — noted,
  deferred perf, not correctness defects.
- Vector-search QPS/recall (v3-3 / a later effort) and graph (v3-2).

## Shared decisions & glossary deltas   (living — every task must honor these)
- FT.SEARCH count semantics = RediSearch's: the integer reply is the TOTAL matched; paging is
  separate. Every query task honors this once `fts-search-count-semantics` freezes it.
- TDD red/green: each correctness defect lands a FAILING test first (wrong union / zero combo /
  wrong count / SPARSE misroute), then the fix (CLAUDE.md Rule 3).
- New/changed parser/eval paths get a fuzz target (CLAUDE.md Fuzzing) and NEVER panic on malformed
  input — return `Frame::Error`, no `expect`/`unwrap` on the query path.
- No new hot-path allocations on the dispatch/query path; no new `unsafe` without approved SAFETY.

## Shared / risky contracts (freeze these first)
- FT.SEARCH total-count semantics (matched vs returned) — a wire-visible reply contract every query
  path shares and clients depend on. Freeze first.            -> owning task `fts-search-count-semantics`
- Posting-list TF lookup API (rank-based) — the shape the high-DF fix and the count/eval paths both
  call; wrong here re-does downstream query work.             -> owning task `fts-posting-rank-tf`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] fts-posting-rank-tf           depends-on: none                      — replace O(N) TF `.position()`
      with rank-based lookup; kills the high-DF O(M^2) cliff. DONE 2026-06-16 (gate PASS, commits
      45b3db8+a7e816d). Also fixed a latent BM25-misalignment bug; froze the rank-aligned PostingList
      TF contract (term_freqs/positions sorted-doc_id-aligned, tf()/positions_for() via rank).
- [ ] fts-upsert-incremental        depends-on: none                      — replace O(V) per-doc posting
      upsert scan with incremental update; fast bulk indexing.
- [ ] fts-query-combinators         depends-on: none                      — `OR` (`|`) unions and
      `TEXT+TAG` combo intersects; correct matched sets.
- [ ] fts-search-count-semantics    depends-on: fts-query-combinators     — FT.SEARCH reply = true
      total-matched count (RediSearch semantics).
- [ ] fts-query-routing-robustness  depends-on: none                      — `is_text_query()` recognizes
      `SPARSE`; remove 3× `expect()` on the FT query path.

## Exit criteria (observable; map each to the task that delivers it)
- [ ] A high-DF term (~5% of docs) returns without the O(M^2) cliff — a 100K-doc query-latency test
      holds well under the old 419 ms (rank-based, not linear scan).            (← fts-posting-rank-tf)
- [ ] Bulk indexing scales ~linearly (no O(V) per-doc scan) — an indexing-rate test shows the cliff
      gone vs the 376 docs/s baseline.                                          (← fts-upsert-incremental)
- [ ] `OR` returns |A ∪ B| matched docs and `TEXT+TAG` returns the non-empty intersection —
      correctness tests over a known corpus.                                    (← fts-query-combinators)
- [ ] FT.SEARCH's integer reply equals the true matched count (not the page size) — test asserts the
      total over a corpus larger than the returned page.                        (← fts-search-count-semantics)
- [ ] A `SPARSE @field …` query routes to the vector path (not BM25), and malformed FT input returns
      an error frame (no panic) — routing + fuzz/negative tests.                (← fts-query-routing-robustness)
