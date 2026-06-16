# TASK: Evaluate the query AST to matched sets + wire FT.SEARCH dispatch (2b)

slug: fts-query-eval-dispatch · created: 2026-06-16 · stage: production · risk: high · autonomy: conservative
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- risk: high — INHERITS fts-query-combinators §3 FROZEN @ v1 (no re-freeze) and REWIRES the
     FT.SEARCH text dispatch across runtime handlers (M5 regression surface). Verify is the COMBINED
     2a+2b human gate (per the 2a-gate decision). run.md unguarded_high_risk_auto guard applies. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Evaluate the parsed `QueryNode` AST (from 2a) to a matched doc-id set + wire it into the
FT.SEARCH text dispatch — replacing the old inline parser with `parse_query → eval_query →
build_text_response` so OR unions, multi-`@clause` intersections, and grouping return correct results.
Framings weighed: ONE centralized kernel `eval_query(idx,&QueryNode,dfs,top_k)->Vec<TextSearchResult>`
called by every handler text branch (chosen — removes the 4-way inline-parser duplication) · per-handler
kernel-internal splice keeping inline copies (rejected — leaves duplication) · single-shard-only first
cut (rejected — would silently leave multi-shard ranking on the buggy path)
INHERITED CONTRACT: this task does NOT re-freeze. It builds against fts-query-combinators §3 FROZEN @ v1
(grammar · QueryNode · eval_set set-semantics · 5 wire error codes). See that task's §3.
Must:
<must>
  - E1 — eval_set(node, idx) -> RoaringBitmap: And = ∩ (fold &=), Or = ∪ (fold |=), Empty = ∅; leaves
    reuse search_field/search_field_or (text) · search_tag per value OR-unioned · search_numeric_range.
    Shared doc-id space (ensure_doc_id) so set-ops compose directly.
  - E2 — eval_query wraps eval_set + best-effort scoring: TEXT leaves contribute BM25 (summed across OR
    branches); docs matched only by TAG/NUMERIC score 0.0. Final order = score DESC, doc_id ASC (stable).
    Pure single-field BM25 and pure-filter paths stay byte-identical to today (score + order).
  - E3 — DISPATCH: every FT.SEARCH text branch (handler_sharded/monoio/single + ft_text_search() +
    spsc) routes raw query bytes through `parse_query(bytes, QuerySchema::from_index(idx))` → on Ok
    `eval_query` → `build_text_response(results, offset, count)`; on Err return `Frame::Error` with the
    QueryError.code(). The old pre_parse_field_filter / parse_text_query / parse_field_targeted_query /
    tokenize_with_modifiers calls on the text path are retired.
  - E4 — UNTOUCHED: HYBRID / vector-KNN / SPARSE / SESSION / RANGE dispatch (caught before is_text_query)
    stay byte-identical. The off-event-loop Yield path (dense-KNN only) is not touched.
  - E5 — multi-shard DFS path (execute_text_search_with_global_idf, global_df/global_n) is preserved:
    eval_query accepts Option<&global_df>/Option<global_n> and forwards to the text leaves as today.
  - E6 — server NEVER panics on a malformed query: parse error → Frame::Error, next command served.
</must>
Reject:
<reject>
  - malformed query (any QueryError) -> Frame::Error coded per §3 (syntax_error | empty_query |
    unknown_field | numeric_filter_invalid | tag_filter_invalid). Index unchanged (read path).
  - query with no matches -> empty result reply (count 0), NOT an error.
</reject>
After:
<after>
  - OR returns unions (~2,072 not ~10); TEXT+TAG / TEXT+NUMERIC combos intersect (~253 not 0); grouping
    works; the duplicated inline parsers are gone; fts-search-count-semantics can count eval_set(root).
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 — M5 NO-REGRESSION across the full-replacement: routing all handler text branches through the
    new path must keep every already-working shape (single term, AND, fuzzy/prefix, single @field:term,
    bare @field:{tag}/@field:[num] fast-path) byte-identical. Lowest confidence because the change spans
    4 runtime handler files + the DFS path, and subtle reply differences (sort order, score, count) would
    regress silently. If wrong: a per-shape compat fix. → Guarded by the existing 144 text-unit + 15
    store-search tests + new e2e tests asserting result sets AND that HYBRID/KNN replies are unchanged.
  - [ ] A2 — best-effort scoring (text BM25 summed across OR; filters 0.0; order score desc / doc_id asc)
    is acceptable; exact union-score formula is NOT frozen (per the 2a freeze decision). (high)
  - [ ] A3 — the single branch point is is_text_query() (HYBRID/KNN/sparse caught before it), so only the
    text branch changes; verified in the dispatch trace. (high)
  - [ ] A4 — build_text_response is the one reply builder; feeding it the eval result Vec is byte-format
    identical to today. (high — verified ft_text_search.rs:1807)
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# These are the END-TO-END result-set scenarios (the parse-tree level is 2a's suite).
# E1/E2 — OR union via the wired path
Scenario: OR query returns the union end-to-end
  Given an index where "alpha" matches {a,b,c} and "beta" matches {c,d,e} in field body
  When FT.SEARCH "alpha | beta" runs through eval_query
  Then the matched key set is {a,b,c,d,e} (union), NOT {c} (the old AND bug)
  And each result carries a __bm25_score field (best-effort scoring)

# E1/E2 — TEXT+TAG combo intersect
Scenario: TEXT and TAG clauses intersect end-to-end
  Given docs where @body:foo matches {1,2,3,4} and @tag:{bar} matches {3,4,5}
  When FT.SEARCH "@body:foo @tag:{bar}" runs
  Then the matched set is {3,4}, NOT empty (the old 0-result bug)

# E1 — TEXT+NUMERIC combo intersect
Scenario: TEXT and NUMERIC clauses intersect
  Given docs where @body:phone matches {1,2,3} and @price in [10 20] matches {2,3,9}
  When FT.SEARCH "@body:phone @price:[10 20]" runs
  Then the matched set is {2,3}

# E1 — grouping
Scenario: grouping scopes a union under an AND end-to-end
  Given red={1,2,3} blue={3,4} car={2,3,4,5}
  When FT.SEARCH "car (red | blue)" runs
  Then the matched set is {2,3,4}

# E2 — best-effort scoring order
Scenario: OR results are ordered score desc, doc_id asc, deterministically
  When "alpha | beta" runs twice
  Then both runs return the SAME ordered key list

# E3/E6 — malformed query never panics, returns coded error
Scenario: a malformed query returns a coded error and the server stays up
  When FT.SEARCH "alpha | (beta" runs
  Then the reply is Frame::Error coded "syntax_error"
  And the very next FT.SEARCH command succeeds (server did not panic)

Scenario: unknown field returns unknown_field
  When FT.SEARCH "@nope:foo" runs against an index without field "nope"
  Then the reply is Frame::Error coded "unknown_field"

# E2/A1 — no regression on already-correct shapes
Scenario: single-term / AND / single @field / bare tag-filter are byte-identical to before
  Given the existing text corpus + the 144 text-unit and 15 store-search tests
  When "alpha", "alpha beta", "@body:alpha", "@tag:{bar}", "@price:[10 20]" run
  Then result sets, ordering, scores and total counts match the pre-change implementation
  And the full pre-existing FT text test suite stays green

# E4 — other dispatch paths untouched
Scenario: HYBRID / vector-KNN / SPARSE replies are unchanged
  When a KNN / HYBRID / SPARSE query runs
  Then its reply is byte-identical to before (only the is_text_query branch changed)
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
INHERITED CONTRACT — fts-query-combinators §3 FROZEN @ v1 (grammar · QueryNode · eval_set set-
semantics · wire error codes). 2b does NOT re-freeze; it IMPLEMENTS that contract. The only new
symbol 2b adds is the evaluator kernel (an internal API, not a frozen wire contract):

  // src/text/query/eval.rs
  fn eval_set(node: &QueryNode, idx: &TextIndex) -> RoaringBitmap          // the frozen set-semantics
  pub fn eval_query(
      idx: &TextIndex,
      node: &QueryNode,
      global_df: Option<&HashMap<String,u32>>,   // DFS path preserved (E5)
      global_n: Option<u32>,
      top_k: usize,
  ) -> Vec<TextSearchResult>                       // eval_set + best-effort BM25 scoring (E2)

DISPATCH (E3): each FT.SEARCH text branch ->
  match parse_query(raw, &QuerySchema::from_index(idx)) {
    Ok(node)  => build_text_response(&eval_query(idx,&node,gdf,gn,top_k), offset, count),
    Err(e)    => Frame::Error("<e.code()>"),       // 5 frozen codes, never panic (E6)
  }
WIRE REPLY: unchanged — build_text_response(results, offset, count) (ft_text_search.rs:1807):
  Array[ Integer(total), (BulkString(key), Array[BulkString("__bm25_score"), BulkString(score)])* ].
  Order: score DESC, doc_id ASC. Pure-filter docs score 0.0 (matches today). HYBRID/KNN/SPARSE: UNTOUCHED.
Code surface: + src/text/query/eval.rs · ~ ft_text_search.rs (retire old parser on text path; route to
  eval_query) · ~ handler_sharded/ft.rs · handler_monoio/ft.rs · handler_single.rs · spsc_handler.rs
  (text branch -> parse_query→eval_query). reuse: search_field/_or/_tag/_numeric_range, build_text_response.
```

Status: INHERITED — fts-query-combinators §3 FROZEN @ v1 (approved Tin Dang 2026-06-16). No re-freeze.
Least-sure flag surfaced at freeze: [spec] M5 full-replacement regression across 4 runtime handler
files + the DFS path (A1) — guarded by the existing 144+15 tests + new e2e; [contract] eval_set node
algebra IS the inherited frozen set-semantics — a bug here is a contract violation, caught by the
combinator e2e scenarios. Combined 2a+2b human verify gate per the 2a-gate decision.
<!-- Inherited frozen contract; 2b implements it. Changing the inherited contract = change request
     back to fts-query-combinators SPECIFY. -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every E-rule via an in-process e2e test (build TextIndex, index docs, run search,
assert key set + order + error). Mirror the existing harness (ft_text_search.rs:2683 build pattern +
extract_hits). Use DISTINCT corpora so union≠intersection is observable.
Plan (asserting result sets / errors, not internals):
<test_plan>
  - test_or_union_e2e: alpha={a,b,c} beta={c,d,e}; "alpha | beta" -> keys {a,b,c,d,e} (E1)
  - test_text_tag_intersect_e2e: @body:foo={1,2,3,4} @tag:{bar}={3,4,5}; "@body:foo @tag:{bar}" -> {3,4} (E1)
  - test_text_numeric_intersect_e2e: "@body:phone @price:[10 20]" -> {2,3} (E1)
  - test_grouping_e2e: "car (red | blue)" -> {2,3,4} (E1)
  - test_or_scoring_order_deterministic: "alpha | beta" twice -> identical ordered key list (E2)
  - test_malformed_returns_coded_error: "alpha | (beta" -> Frame::Error "syntax_error"; next search ok (E3/E6)
  - test_unknown_field_error: "@nope:foo" -> Frame::Error "unknown_field" (E3)
  - test_no_regression_single_and_filter: "alpha" / "alpha beta" / "@body:alpha" / "@tag:{bar}" /
    "@price:[10 20]" -> same sets+order+score as a captured pre-change baseline (E2/A1)
  - test_empty_result_not_error: "zzz" (absent) -> empty reply count 0, NOT an error
  - (regression net, not new files) the existing 144 text-unit + 15 store-search tests stay green; a
    KNN/HYBRID smoke asserts that path is unchanged (E4).
</test_plan>

Tests live in: `tests/fts_query_eval_e2e.rs` · MUST run red (eval_query missing) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): the dispatch path runs on untrusted query bytes — a parse Err MUST
become Frame::Error (never unwrap/expect/panic; E6). Only the is_text_query() branch may change;
HYBRID/KNN/SPARSE/SESSION/RANGE code stays byte-identical (E4). eval_set uses RoaringBitmap & / | for
the set algebra; leaf Vec<u32> -> bitmap collect. Preserve the DFS global_df/global_n forwarding (E5).
Build: + src/text/query/eval.rs (eval_set + eval_query) wired into src/text/query/mod.rs; then retire
the old text-path parser and route each handler text branch through parse_query → eval_query →
build_text_response (ft_text_search.rs, handler_sharded/ft.rs, handler_monoio/ft.rs, handler_single.rs,
spsc_handler.rs). Keep dead old-parser fns only if still used by a non-text path; else remove.
Code lives in: `src/text/query/`, `src/command/vector_search/`, `src/server/conn/`, `src/shard/`
Constraints: do NOT change any test or the inherited frozen contract; no new deps; ask if unclear.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — see EVIDENCE below (lib 3584/0; e2e 12/12; tokio FT integration 5/5;
      over-the-wire smoke + numeric x-shard probe all correct on shards 1 AND 4)
- [x] coverage did not decrease — +12 e2e tests (incl. 2 N-invariant guards); no test deleted. The
      one suite that did NOT run (numeric shard-consistency) is a pre-existing stale `#[ignore]`
      test — diagnosed, not regressed (FLAG-1).
- [x] no test or contract was altered during build — §3 INHERITED from 2a, untouched; the stale
      numeric test was NOT modified (its intent reproduced manually instead — FLAG-1).
- [x] concurrency / timing of the risky operation is safe — the cross-shard DFS scatter (the
      high-risk surface) is verified end-to-end: Phase-1 df-scatter N-invariant holds (one N per
      shard, guarded by test_df_field_terms_single_entry_invariant), Phase-2 raw-query eval is
      deterministic; 1-shard vs 4-shard returns IDENTICAL matched sets across 6 numeric/OR/combinator
      queries. No new locks, no `.await`-held locks, no new `unsafe`.
- [x] no exposed secrets, injection openings, or unexpected dependencies — query bytes flow as
      opaque `Bytes` re-parsed per shard via the frozen 2a parser (defensive: parse Err → coded
      Frame::Error, never panic); no new deps.
- [x] layering & dependencies follow CONVENTIONS.md — `eval.rs` lives in `src/text/` using only
      `TextIndex` methods (no upward command/ dep); dispatch wrappers in `command/vector_search/`.
- [ ] a person reviewed and approved the change — PENDING the combined 2a+2b human gate.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — every new symbol is referenced. `eval_query`/`eval_set` ← run_text_query_on_index
      (single-shard local + Phase-2 local + spsc_handler remote); `collect_df_field_terms` ← coordinator
      Phase-1 scatter; `collect_highlight_terms` ← coordinator + spsc_handler HIGHLIGHT; `run_text_query`
      ← handler_single / handler_sharded / handler_monoio. Confirmed: default clippy clean AND tokio
      (no-text-index) clippy clean — both feature sets compile with zero unused-symbol warnings.
- [x] DEAD-CODE (code) — one KNOWN orphan: `scatter_text_search_filter` (coordinator ~1834-1933)
      is now dead (the new raw-query scatter replaces it). Left in place ON PURPOSE: its removal
      cascades into the InvertedSearch wire-protocol enum = scope creep beyond 2b. Recorded as a
      deferred follow-up (FLAG-2), not silently shipped.
- [x] SEMANTIC (prose / non-code) — n/a (code change).

### EVIDENCE (2026-06-16, macOS dev host; production magnitudes N/A — correctness only)
- **default clippy**: clean (`cargo clippy -- -D warnings`).
- **tokio clippy** (no-text-index path): clean (`--no-default-features --features runtime-tokio,jemalloc`).
- **lib**: `test result: ok. 3584 passed; 0 failed; 0 ignored` (244.82s, default features).
- **release build**: clean, `Finished release in 6m01s`.
- **e2e `fts_query_eval_e2e`**: 12/12 pass (OR-union, text∩tag, text∩numeric, grouping, OR-scoring
  order, malformed→coded error, unknown-field, no-regression single AND-filter, empty-not-error,
  kernel-direct, + 2 N-invariant df guards).
- **tokio FT integration** (`runtime-tokio,text-index,graph`): `hybrid_filter_tag` 1/1,
  `ft_search_as_of_filter` 1/1, `txn_ft_search_snapshot` 3/3 — rewired tag-filter, as-of, and
  **sharded txn-snapshot scatter** paths all green.
- **over-the-wire redis-cli smoke, shards 1 AND 4** (the OR/combinator fix end-to-end):
  `alpha | beta`→3, `alpha beta`→1, `@body:alpha @tag:{bar}`→1, `@body:beta @price:[15 25]`→1,
  `alpha | (beta`→syntax_error. Identical matched SET + ordering on both shard counts (only BM25
  magnitudes differ — within the frozen "scoring best-effort" boundary).
- **numeric 1-shard vs 4-shard identity probe** (replaces stale numeric test — FLAG-1): 6/6
  ALL-IDENTICAL and semantically correct (`@score:[5 10]`, exclusive `[(5 (10]`, `[-inf 4]`,
  `[15 +inf]`, `@score+@status` combinator, `@status:{closed} | @score:[18 19]` OR-union = 8 keys ✓).

### FLAGS (carried to the combined gate)
- **FLAG-1 (resolved, non-blocking)** — `tests/inverted_search_numeric_shard_consistency.rs` is a
  pre-existing stale `#[ignore]` manual test: its `seed()` panics at line 73 because `hset_multiple`
  emits HMSET (reply `+OK`) but the test annotates `let _: i64`, so it fails parsing "OK" as int —
  in `seed()`, before any FT.SEARCH assertion. Independent of 2b (diff touches zero HSET code).
  Intent reproduced manually (numeric x-shard probe above, 6/6 pass). Follow-up: fix the annotation
  + per-test index isolation, then un-ignore.
- **FLAG-2 (deferred follow-up)** — dead `scatter_text_search_filter` in coordinator.rs; remove in a
  cleanup task once the InvertedSearch wire-protocol simplification is in scope.
- **FLAG-3 (approximation, by design)** — `collect_df_field_terms` returns at-most-one entry to keep
  the Phase-1 N-sentinel invariant (one N per shard); when text leaves span multiple fields the
  field_hint collapses to None and df uses the cross-field path. Correctness preserved (set/ordering
  identical x-shard, proven above); only BM25 IDF magnitude is approximate for multi-field OR.

### GATE RECORD
Proposed outcome: **PASS** — but GATE DEFERRED to the combined 2a+2b human gate (per Tin Dang's
decision "build 2b first, gate together"). All functional evidence is green; the sole non-pass
(numeric shard-consistency) is a diagnosed pre-existing stale test (FLAG-1), not a 2b regression,
and its intent is independently re-verified. No security findings. No frozen-contract or test
weakening. Lowest-confidence item for the human: the full-replacement of all three FT.SEARCH
dispatch paths + the cross-shard DFS payload change (raw `Bytes`) — does any already-working
FT.SEARCH shape regress? (Mitigated by: lib 3584/0, tokio FT 5/5, smoke + x-shard probe on both
shard counts.)
If RISK-ACCEPTED -> owner: <name> · ticket: <link> · expires: <date>   (never for a security gap)
Outcome: **PASS** (combined 2a+2b gate).
Reviewed by: Tin Dang (combined 2a+2b human gate, "PASS both") · date: 2026-06-16

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): OR/combinator result-set correctness — the e2e suite
(`tests/fts_query_eval_e2e.rs`) + the x-shard identity probe ARE the monitors. Re-run after any
FT.SEARCH dispatch or scatter change; a divergence between shards=1 and shards=4 matched sets is
the canary for a scatter-path regression.
Spec delta for the next loop: the cross-shard FT.SEARCH payload is now opaque `Bytes` re-parsed per
shard — `fts-search-count-semantics` (consumes `eval_set(root).len()`) and `fts-query-routing-robustness`
build on this; `fts-upsert-incremental` is independent of the query path.

### Competency deltas
- [TDD · folded] Test SELECTION must be validated to actually execute, not just compile: 4 of my chosen
  FT integration tests ran 0 cases (`#![cfg(feature="runtime-tokio")]`-gated under monoio default;
  one `#[ignore]`'d) and a green `cargo test --release` hid it. Evidence: first verify pass reported
  "0 passed; 0 failed" yet I nearly read it as PASS. Lesson: assert a nonzero ran-count per suite, or
  the suite is silent coverage. Mitigation already applied: ran them under the correct feature set.
- [TDD · folded] A pre-existing `#[ignore]`'d manual test (`inverted_search_numeric_shard_consistency`)
  is stale-broken at its seed (`let _: i64` vs HMSET `+OK`), so it cannot guard the scatter path it
  was written for. Evidence: panics at line 73 before any assertion. Follow-up task: fix annotation +
  per-test index isolation + un-ignore (a real cross-shard numeric guard is valuable post-2b).
- [SDD · folded] A flat dispatch payload `(field_idx, Vec<QueryTerm>)` silently constrained the wire to
  AND-only — the OR bug was unfixable at the leaf without a payload redesign. Evidence: OR returned
  AND on multi-shard until `TextSearchPayload` carried raw `Bytes`. Lesson: when a cross-shard payload
  can't REPRESENT the contract's algebra, that's a contract-blocking design smell to surface at freeze.
- [ADD · folded] Building 2b on 2a's frozen contract before gating either ("gate together") worked: it
  let the inherited eval_set algebra be validated by real end-to-end result sets rather than parser
  unit tests alone. Evidence: 12/12 e2e + x-shard identity. Keep "split parser/dispatch, gate the
  pair" as a pattern for contract-then-wire features.
- [ADD · folded] Deferred-cleanup honesty: `scatter_text_search_filter` left dead-but-flagged (FLAG-2)
  rather than removed-with-scope-creep into the InvertedSearch protocol. Folds at milestone close.
