# FIX-1238 — working notes (ADD discipline, not a shared artifact)

## Context loaded
CLAUDE.md, UNSAFE_POLICY.md, MILESTONE.md, TEAM-RULES.md (§0 first), personas
`routing-dispatch-engineer` (lead: same answer on every shard layout, or refuse) and
`ci-test-integrity-engineer` (the guard must be shown red), moon#1238 (no comments),
moon#648 + its comment, PR #666 (the tri-state `FilterParse` and the "Invalid short-circuits"
rule), and the files named in the prompt. Base `1a635f1` (int/part3b). Ports 7520–7539.

## Mechanism (verified in the code, then on a binary)
- `--shards 1`: `ft_search` and `ft_search_capture` (`ft_search/dispatch.rs:143`, `:567`)
  resolve the prefilter as
  `parse_filter_clause(args).or_else(|| parse_inline_filter(&query_str)).into_option()`:
  an explicit `FILTER` wins, else the inline `<prefilter>=>[KNN …]` prefix; `Invalid` is an ERR.
- `--shards > 1`: both handlers (`handler_monoio/ft.rs:390`, `handler_sharded/ft.rs:273`) call
  `parse_ft_search_args` (`ft_search/response.rs:175`). It takes `k`/field/param from
  `parse_knn_query` and the filter from `parse_filter_clause(args)` ONLY. The inline prefix is never
  looked at, so it is neither parsed nor refused. The handlers then refuse `filter.is_some()`
  (`ERR FILTER not supported in multi-shard mode yet`) — which only an explicit FILTER can trigger —
  and scatter an UNFILTERED KNN: `scatter_vector_search_remote` has no filter parameter,
  `VectorSearchPayload` has no filter field, and both legs call `capture_knn` (filter `None`) with a
  `search_local_filtered(.., None, ..)` fallback.
- Second, latent hole on the same path: `merge_search_results` SKIPS a leg that answered
  `Frame::Error`. Once a filter is carried, a leg that refuses it (the declared-schema per-index
  `text_match_refusal`, decided in `capture_dense_knn_snapshot` / `execute.rs`, not at parse time)
  would be folded into a successful `[0]` — every leg refuses, the merge answers an empty page.
  That is the moon#648 class again one layer down, so it is part of this fix.
- Reproduced on `baseline-ae21476` (release) with a 12-doc fixture, `--shards 2`:
  `@lang:{fr}=>[KNN 5 …]` → d:0..d:4 (d:0, d:2, d:3 are not fr; shards 1: d:1 d:4 d:7 d:10);
  `@year:[(2003 2007]=>[KNN 10 …]` → 10 docs (shards 1: 4); `@body:{word7 common}` → 5 docs
  (shards 1: d:7 only); `@year:[abc def]=>[KNN …]` → 5 docs (shards 1: `ERR invalid FILTER
  expression`). Unfiltered KNN scores are byte-identical at 1 and 2 shards, so an exact-equality
  test is sound.

## Fix design
1. **One grammar.** `parse_ft_search_args` resolves the filter with the exact expression
   `--shards 1` uses (`parse_filter_clause(args).or_else(|| parse_inline_filter(&query_str))
   .into_option()?`). No second grammar. `Invalid` (unparseable, inverted range, parse-time
   TextMatch refusal) answers the same ERR string as `--shards 1`.
2. **Explicit FILTER stays refused at multi-shard** (the prompt: keep that refusal). The parser
   now returns a struct `FtSearchArgs` with `filter_is_clause`, so a handler can refuse an explicit
   clause without refusing the inline prefix. Both present → `--shards 1` resolves to the clause
   (FILTER wins), multi-shard refuses that clause: an error, never a silent drop. The tuple became a
   struct — every caller updated (2 handlers, 2 unit tests; grep of src/, tests/, fuzz/, benches/).
3. **Carry the filter to every leg.** `scatter_vector_search_remote(.., filter:
   Option<Arc<FilterExpr>>, ..)`; `VectorSearchPayload.filter: Option<Arc<FilterExpr>>` (one Arc
   per FILTERED query, a refcount bump per remote leg; zero cost for unfiltered queries);
   `capture_knn(.., filter)` → `capture_dense_knn_snapshot(.., filter, ..)` (the same capture
   `--shards 1` uses, which already evaluates filters incl. the BM25 route via `text_store`); the
   synchronous fallback becomes `search_local_filtered_with_text(.., filter, .., Some(text_store))`
   so a declared-TEXT TextMatch is answered on the fallback too (identical to
   `search_local_filtered` when unfiltered).
4. **Fail loudly on a leg.** When a filter is present, the first leg error (local first, then
   remote in ascending shard order — deterministic) is the reply instead of being skipped by the
   merge. Unfiltered queries keep the legacy fold (behaviour frozen; see Risks).

## Risks
- Behaviour change (intended, correctness): an inline prefilter at `--shards > 1` now filters, and
  an unreadable one is an ERR. Callers that relied on the silent widening get fewer rows / an ERR —
  same wording as moon#648's behaviour change.
- The explicit-FILTER refusal is kept; lifting it is now a one-line change in each handler
  (follow-up, not done: the prompt says keep it).
- `--shards 1` with FILTER AND an inline prefix honours FILTER and ignores the inline prefix without
  an error — the same class, but the prompt says match `--shards 1`; recorded as a follow-up.
- Out of scope, recorded: at `--shards > 1` the KNN scatter also ignores LIMIT (`_offset/_count`
  are dropped, merge uses `0, usize::MAX`) and a non-default `@field` (legs search the default
  field); unfiltered leg errors (`Unknown Index name`, dimension mismatch) still fold into `[0]`.
- Ownership: the fix necessarily touches `src/shard/{coordinator,vector_scatter,dispatch,
  spsc_handler}.rs` and both conn handlers (WS8 / WS7 files in earlier waves); the prompt names
  them, so they are in scope here, and each edit is confined to the vector-scatter path.

## What was built (matches the design above)
- `ft_search/response.rs`: `parse_ft_search_args` → `FtSearchArgs { index_name, query_blob, k,
  filter, filter_is_clause, offset, count }`; the filter is resolved with the `--shards 1`
  expression, BEFORE the PARAMS lookup (the same precedence `ft_search` has, so a bad prefilter
  with a missing vector answers the same ERR at every shard count).
- `shard/vector_scatter.rs`: `KnnLeg { Yield, Done }` + `plan_knn_leg` replace `capture_knn` and
  the two copies of the synchronous fallback (coordinator local leg, `spsc_handler` remote leg):
  capture and fallback now run in ONE shard-slice borrow, with the filter and `Some(text_store)`.
  `merge_knn_legs` = first leg error when filtered, else `merge_search_results` unchanged.
- `shard/dispatch.rs`: `VectorSearchPayload.filter: Option<Arc<FilterExpr>>`.
- `shard/coordinator.rs`: `scatter_vector_search_remote(.., filter, ..)`; local leg via
  `plan_knn_leg`; merge via `merge_knn_legs`. Net −3 lines (file is 4483 lines on base).
- `shard/spsc_handler.rs`: the `VectorSearch` arm destructures `filter` and uses `plan_knn_leg`.
  Net −9 lines (4893 on base).
- Both handlers: `Ok(parsed) if parsed.filter_is_clause` → the kept refusal; else scatter with
  `parsed.filter.map(Arc::new)`.
- FT.* is not on the three `command::dispatch` paths: both runtimes route FT.* through
  `handler_{monoio,sharded}/ft.rs::try_handle_ft_command` only (checked: no `FT.` arm in
  `command/mod.rs` or `server/conn/blocking.rs`). Both handlers are covered by the e2e test (monoio
  binary → `handler_monoio`, tokio binary → `handler_sharded`).

## Guards shown able to fail
- e2e red on `1a635f1` (monoio 4/5 red, tokio 3/4 red — the declared-schema test is
  `text-index`-only) and on `ae21476` (release baseline, 4/5 red). The 5th test is the kept-refusal
  guard (green on base by design).
- Mutation `if false && filtered` in `merge_knn_legs` (leg errors skipped again): only
  `declared_schema_text_match_is_answered_or_refused_alike_on_every_leg` goes red —
  `left: "*1\r\n:0\r\n"` vs `right: "-ERR full-text KNN filter … MOON_VECTOR_PAYLOAD_TEXT=off"`.
  The unit test `a_refusing_leg_is_the_reply_of_a_filtered_query` pins the same rule in-process.
- Script rows (extracted verbatim, run by a harness on port 7523/7524 that starts/stops its own
  servers with `shutdown nosave`): fixed binary PASS 11/11; base binary FAIL 6 (KNNFILT-01..05 at
  `--shards 4` and the 1/4/12 parity row). KNNFILT-01..04 (moon#648's rows) were never green at
  `--shards 4` before this fix — the prefilter never reached a multi-shard leg.

## Self-evaluation (0–1)
- Completeness 0.92 — parse, carry (both legs), fail-loud merge, both handlers, unit + e2e + script
  rows, red/green on both runtimes. Not done (by instruction or out of scope, recorded): lifting the
  explicit-FILTER refusal; LIMIT / non-default field / unfiltered leg errors at multi-shard.
- Clarity 0.92 — one grammar, one leg planner, one merge rule; comments name moon#1238/#648.
- Practicality 0.93 — no new grammar, no new unsafe, dual-runtime, net shrink of the two >1500-line
  shard files.
- Optimization 0.90 — unfiltered path: one extra O(|query|) `=>` scan and a `None` move per leg;
  filtered path: one `Arc` per query, refcount per remote leg (no deep clone). Not A/B-measured: a
  release build for a tens-of-ns delta on a millisecond search is below this box's noise.
- Edge cases 0.91 — exclusive bounds, NumEq, compound AND, empty match, TextMatch (payload and BM25
  routes), declared-schema refusal, unreadable/inverted/garbage prefixes, FILTER+prefix, invalid
  FILTER short-circuit, filter-vs-PARAMS error precedence.
- Self-evaluation 0.90 — every claim above has a log in the scratchpad and a line in SUMMARY.md.
