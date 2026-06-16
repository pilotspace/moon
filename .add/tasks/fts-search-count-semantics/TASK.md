# TASK: FT.SEARCH integer reply = true total-matched (RediSearch count semantics)

slug: fts-search-count-semantics · created: 2026-06-16 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: FT.SEARCH's integer reply (`reply[0]`) must be the TRUE total number of documents that
matched the query — independent of `LIMIT`/`top_k` pagination — matching RediSearch semantics.
Root cause (two places): `build_text_response` (ft_text_search.rs:1854) sets `total = results.len()`,
but `results` was already truncated to `top_k = offset + count` by `eval_query`'s `results.truncate(top_k)`
(eval.rs:144). So `LIMIT 0 5` over 100 matches reports 5, not 100. On the multi-shard DFS path
`merge_text_results` (:1937) sets `total = all_results.len()` AFTER merging+truncating the per-shard
*returned* docs — discarding each shard's true matched count. The authoritative match set already
exists: `eval_query` computes `eval_set(node, idx)` (the complete `RoaringBitmap`, pre-truncation) and
builds the full `results` from it before truncating; the count is lost only at the truncate step.
Framings weighed: surface the pre-truncation matched-resolvable count from the eval kernel and report
it as `reply[0]` (chosen — count is already computed, zero extra eval) · call `eval_set(node).len()`
again in `run_text_query_on_index` (rejected — recomputes the whole AST set, a second O(matched) pass on
the hot path; perf rule violation) · count merged docs but raise per-shard `top_k` to "unbounded"
(rejected — forces every shard to materialize+ship its entire match set just to count, the exact
allocation blow-up LIMIT exists to avoid).
ADD-COMPLIANCE: this task DEPENDS-ON fts-query-eval-dispatch (DONE, frozen). It MUST NOT edit that
frozen contract. `eval_query -> Vec<TextSearchResult>` and `build_text_response(results, offset, count)`
stay intact as thin wrappers; the count is added via NEW additive symbols (`eval_query_counted`,
`build_text_response_with_total`). eval.rs already kept `eval_set` `pub` "for that task".
Must:
<must>
  - C1 — `eval_query_counted(idx, node, gdf, gn, top_k) -> (Vec<TextSearchResult>, usize)`: a NEW fn
    that computes `eval_set` ONCE, returns the same `top_k`-truncated/ordered results as `eval_query`
    AND the true matched count = the number of matched, key-resolvable docs BEFORE truncation
    (`results.len()` captured before `results.truncate(top_k)`). `eval_query` becomes
    `eval_query_counted(..).0` — its frozen signature + output byte-identical.
  - C2 — `build_text_response_with_total(results, total_matched, offset, count) -> Frame`: a NEW fn
    identical to `build_text_response` except `reply[0] = total_matched` (not `results.len()`).
    `build_text_response(results, offset, count)` becomes `..._with_total(results, results.len(), ..)`
    — old 3-arg callers + the existing unit test unchanged (behavior identical for them).
  - C3 — Single-shard live path `run_text_query_on_index` (ft_text_search.rs:1562) routes through
    `eval_query_counted` + `build_text_response_with_total`, so `reply[0]` = true matched even when
    `LIMIT`/`top_k` truncates the returned docs. The returned doc page (count, order, scores, keys) is
    byte-identical to today.
  - C4 — Multi-shard DFS merge `merge_text_results` (:1899): `reply[0] = Σ per-shard reply[0]` (the
    `Frame::Integer` at `items[0]` of each shard response), NOT `all_results.len()`. Each shard already
    reports its true LOCAL matched count via C3; keys partition to exactly one shard, so the sum is the
    exact global total with no double-count. Errored/empty shard frames contribute 0. Returned docs
    (merge sort, truncate, pagination) unchanged.
  - C5 — When no `LIMIT` truncates (LIMIT absent ⇒ `top_k` unbounded), `reply[0]` is unchanged from
    today (it already equalled the full matched count) — a strict no-regression on the common path.
</must>
Reject:
<reject>
  - a shard response that is `Frame::Error` or has no integer `items[0]` -> contribute 0 to the sum,
    never panic, never abort the merge (other shards still counted) -> "shard_total_absent_zero"
  - a matched `doc_id` in the eval set with NO entry in `doc_id_to_key` (deleted/desynced) -> it is not
    returnable, so it is NOT counted (total = matched AND resolvable) -> "unresolvable_doc_uncounted"
</reject>
After:
<after>
  - `FT.SEARCH idx "term" LIMIT 0 5` over a corpus with 100 matches replies `[100, <=5 docs...]` on
    1-shard AND N-shard configs; with no LIMIT it replies the same total as before. The returned-doc
    page is byte-identical to the pre-change server in every case.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 — "true total-matched" = matched AND key-resolvable docs (pre-truncation `results.len()`), which
    equals `eval_set(node).len()` ONLY when the posting/tag/numeric bitmaps and `doc_id_to_key` are in
    sync. Lowest confidence because the 2b design note literally said "eval_set(root).len()", and the two
    diverge under a posting/key desync (a doc in a bitmap but with no key). I choose the resolvable count
    because an unreturnable doc must not inflate the total (RediSearch counts returnable matches). If a
    reviewer wants raw `eval_set.len()` instead: it's a one-line swap (`set.len()` vs the resolvable
    count) — but it would over-report on desync, so I keep resolvable. → Pinned by a test that asserts
    total == returned-doc count when matched ≤ top_k, and total > returned when matched > top_k.
  - [ ] A2 — keys partition to exactly ONE shard, so Σ per-shard matched counts has no double-count.
    (high — the core hash-slot routing invariant; a key lives on one shard, a doc is one key.)
  - [ ] A3 — every live multi-shard text response reaching `merge_text_results` carries its true local
    matched count at `items[0]` (because each shard runs C3's `run_text_query_on_index`). (med — the DFS
    Phase-2 scatter at coordinator.rs:1806 does; the InvertedSearch scatter at :1924 must be confirmed to
    build its per-shard frame via `build_text_response`/C3 and not a hand-rolled `items[0]`.) → verified
    in BUILD by tracing the InvertedSearch shard handler; if it hand-rolls items[0], it gets the same
    true-count treatment.
  - [ ] A4 — `reply[0]` is the only count surface; HYBRID/vector-KNN/SPARSE replies are built elsewhere
    and are out of scope (their count semantics are a separate concern). (high — those branches never
    reach `run_text_query_on_index`/`merge_text_results`, confirmed by the 2b dispatch split.)
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# C1 — eval_query_counted surfaces the pre-truncation matched count, results unchanged
Scenario: counted variant returns the truncated page AND the full matched total
  Given a TextIndex where 12 docs match the parsed query "alpha"
  When eval_query_counted(idx, node, None, None, top_k=5) runs
  Then the returned Vec has exactly 5 results (== eval_query(..).0, byte-identical order/scores/keys)
  And the returned total is 12 (the matched, key-resolvable count before truncation)

# C2 — build_text_response_with_total puts total_matched in reply[0]; old wrapper unchanged
Scenario: response builder reports the supplied total, not the page length
  Given a results page of 3 entries and total_matched = 47
  When build_text_response_with_total(results, 47, offset=0, count=3) runs
  Then reply[0] == Integer(47) and the response carries 3 doc entries
  And build_text_response(results, 0, 3) (the 3-arg wrapper) still reports reply[0] == 3 (results.len())

# C3 — single-shard FT.SEARCH with LIMIT reports true matched, not the page size
Scenario: LIMIT does not shrink the reported total (1 shard)
  Given a 1-shard server with an index where 100 docs match "term"
  When FT.SEARCH idx "term" LIMIT 0 5 is issued over the wire
  Then reply[0] == 100
  And exactly 5 document entries follow (page size, order, scores byte-identical to today)

# C4 — multi-shard FT.SEARCH sums per-shard true matched counts
Scenario: total is the sum across shards, not the merged-and-truncated page
  Given a 4-shard server where "term" matches 100 docs spread across shards, LIMIT 0 5
  When FT.SEARCH idx "term" LIMIT 0 5 is issued
  Then reply[0] == 100 (Σ per-shard local matched), identical to the 1-shard reply[0]
  And exactly 5 document entries follow, ranked by global BM25 like today

# C5 — no-LIMIT path is unchanged (no regression)
Scenario: without LIMIT, the total equals today's value
  Given any index/query on 1- and 4-shard configs with no LIMIT clause
  When FT.SEARCH idx "term" runs
  Then reply[0] equals the number of returned doc entries (full matched set, as before the change)

# Reject — shard_total_absent_zero: an errored/odd shard frame contributes 0, no panic
Scenario: merge tolerates an errored shard
  Given shard A replies [3, ...3 docs...] and shard B replies a Frame::Error
  When merge_text_results runs
  Then reply[0] == 3 (B contributes 0) and A's docs are present; the merge does not panic or abort

# Reject — unresolvable_doc_uncounted: a matched-but-keyless doc is not counted
Scenario: a doc in the match set with no key mapping is excluded from the total
  Given a query whose eval_set contains doc_id D, but doc_id_to_key has no entry for D
  When eval_query_counted runs
  Then D is neither returned nor counted; total == the number of matched docs that DO resolve to a key
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
FT.SEARCH text reply — reply[0] becomes TRUE total-matched. INTERNAL fn additions only; no new
RESP shape (reply[0] was already an Integer; its VALUE changes from min(matched, top_k) → matched).

NEW (additive — the frozen 2b symbols are kept as wrappers):

fn eval_query_counted(idx: &TextIndex, node: &QueryNode, global_df: Option<&HashMap<String,u32>>,
                      global_n: Option<u32>, top_k: usize) -> (Vec<TextSearchResult>, usize)
  (src/text/query/eval.rs) — body = today's eval_query, but capture `total = results.len()` AFTER
  building results from eval_set + ordering, BEFORE `results.truncate(top_k)`. Returns (results, total).
  `total` = matched AND key-resolvable docs (a doc in eval_set with no doc_id_to_key entry is already
  filtered out when building results, so it is neither returned nor counted → unresolvable_doc_uncounted).
  eval_set is computed EXACTLY ONCE.
fn eval_query(idx, node, gdf, gn, top_k) -> Vec<TextSearchResult>   // UNCHANGED frozen 2b signature
  = eval_query_counted(idx, node, gdf, gn, top_k).0                 // now a thin wrapper, output identical

fn build_text_response_with_total(results: &[TextSearchResult], total_matched: usize, offset: usize,
                                  count: usize) -> Frame
  (src/command/vector_search/ft_text_search.rs) — today's build_text_response body with
  `reply[0] = Frame::Integer(total_matched as i64)` instead of `results.len()`. Pagination/doc entries
  IDENTICAL.
fn build_text_response(results, offset, count) -> Frame             // UNCHANGED 3-arg signature
  = build_text_response_with_total(results, results.len(), offset, count)   // behavior identical for old callers

CHANGED call sites (LIVE path only):
  run_text_query_on_index (ft_text_search.rs:~1562):
    let (results, total) = eval_query_counted(text_index, &node, global_df, global_n, top_k);
    build_text_response_with_total(&results, total, offset, count)
  merge_text_results (ft_text_search.rs:~1937): reply[0] = Σ over shard responses of
    (if Frame::Array && items[0]==Frame::Integer(t) { t } else { 0 })   // shard_total_absent_zero
    instead of `all_results.len()`. Doc collection / sort / truncate / pagination UNCHANGED.

REJECT responses (internal control-flow, no error frames):
  shard_total_absent_zero    -> a shard frame that is Frame::Error or lacks an Integer items[0] adds 0;
                                merge continues, never panics.
  unresolvable_doc_uncounted -> doc_id in eval_set but absent from doc_id_to_key is dropped while
                                building results (existing filter_map), so excluded from BOTH page and total.

SCOPE / WIRING boundaries:
  - LIVE multi-shard text = scatter_text_search (DFS, coordinator.rs:~1806): every shard builds its frame
    via run_text_query_on_index (local :1632/:1734; remote via ShardMessage::TextSearch → spsc_handler
    :1370), so each items[0] carries the TRUE local matched count → the Σ is exact (keys partition to one
    shard, A2). 
  - scatter_text_search_filter / ShardMessage::InvertedSearch (coordinator.rs:~1824) is DEAD post-2b
    (a known carried flag) and still uses execute_query_on_index + 3-arg build_text_response — OUT OF
    SCOPE; its items[0] stays results.len(). Not on the live wire path; not fixed here.
  - HYBRID / vector-KNN / SPARSE replies built elsewhere — untouched (A4).
  - No on-disk/wire format change. Per-shard; no cross-shard state. No new dependency.
```

Status: FROZEN @ v1 — auto-frozen 2026-06-16 under autonomy:auto per the user directive "implement all
remaining tasks". No genuine design fork: the additive-wrapper shape is the only way to add the count
without editing fts-query-eval-dispatch's frozen contract, and "true total-matched" is the RediSearch
spec. Names match the inherited GLOSSARY (eval_query, eval_set, build_text_response, TextSearchResult,
merge_text_results).
Least-sure flag surfaced at freeze: [spec] A1 — total = matched-AND-key-resolvable (pre-truncation
`results.len()`) vs the 2b note's literal `eval_set(node).len()`. They differ ONLY under a posting↔key
desync (a matched doc with no key); I count resolvable docs so an unreturnable doc never inflates the
total (a returned page can never exceed the reported total). Cost if wrong: a one-line swap to `set.len()`
— but that would over-report on desync, so resolvable is the safer default. [contract] C4 correctness
hinges on each LIVE shard frame's items[0] being the true LOCAL matched count AND keys partitioning to one
shard (A2) — pinned by the 1-shard-vs-4-shard equal-total e2e test; a regression there is the canary.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must (C1–C5) + both Rejects. RED-driver: `eval_query_counted` and
`build_text_response_with_total` do not exist yet → the test binaries fail to COMPILE (the same
compile-red TDD state 2b used), except the two merge tests which are assertion-red (merge_text_results
exists but still reports `all_results.len()`).
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  IN-PROCESS over real TextIndex — `tests/fts_query_eval_e2e.rs` (reuses empty_index/add_doc/run_text_query):
  - test_eval_query_counted_total_is_pre_truncation (C1): index 12 docs all matching "alpha";
    let (results, total) = eval_query_counted(&idx, &node, None, None, 5) → results.len()==5 AND total==12;
    AND results == eval_query(&idx,&node,None,None,5) (byte-identical .0). RED: symbol missing → compile.
  - test_run_text_query_limit_reports_true_total (C3): run_text_query(store,"idx", b"alpha", top_k=5,
    off=0, count=5) → reply[0]==Integer(12) AND exactly 5 doc entries. RED: build_text_response_with_total
    missing → compile (run_text_query_on_index won't yet route through it).
  - test_run_text_query_no_limit_total_unchanged (C5): same corpus, no LIMIT (top_k=usize::MAX/2, count
    =usize::MAX) → reply[0]==Integer(12)==number of returned doc entries (no-regression).
  - test_unresolvable_doc_uncounted (Reject, STRUCTURAL): total is taken from the resolvable `results`
    vector (the existing `doc_id_to_key` filter_map), never `set.len()`, so an unreturnable doc cannot
    inflate it. Asserted indirectly: for a fully-resolvable corpus total==eval_set(node,&idx).len() (C1),
    and a returned page can never exceed reply[0]. (No test hook fabricates a desync — the index keeps
    postings↔keys in sync; over-count is impossible by construction, documented at the capture site.)
  UNIT pure-frame — `src/command/vector_search/ft_text_search.rs` #[cfg(test)] mod (beside merge tests):
  - test_build_text_response_with_total_reports_total (C2): build_text_response_with_total(&[3 results],
    47, 0, 3) → reply[0]==Integer(47), 3 doc entries. RED: symbol missing → compile.
  - test_build_text_response_wrapper_unchanged (C2): build_text_response(&[3 results], 0, 3) →
    reply[0]==Integer(3) (results.len()) — the 3-arg wrapper preserves old behavior.
  - test_merge_text_results_sums_shard_totals (C4): three synthetic shard frames with items[0]=
    Integer(40)/35/25 each returning 1 doc, top_k large → merged reply[0]==Integer(100), 3 docs.
    ASSERTION-RED (current code reports 3 = merged-doc count).
  - test_merge_text_results_errored_shard_zero (Reject shard_total_absent_zero): frames =
    [Integer(3)+3 docs, Frame::Error] → reply[0]==Integer(3), the 3 docs present, no panic.
  REGRESSION: existing merge_text_results_descending_sort (asserts total==2 for two items[0]=1 frames)
    and the build/merge unit tests stay green — summing items[0] (1+1) equals the old count when each
    shard reports == its returned docs (verified: no test weakened).
</test_plan>

Tests live in: `tests/fts_query_eval_e2e.rs` (in-process index: C1, C3, C5, struct-reject) ·
`src/command/vector_search/ft_text_search.rs` (unit: C2, C4, shard-reject). MUST run red (missing
symbols → compile-fail; merge-sum → assertion-fail) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): <e.g. debit+credit in one atomic transaction>
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — UNIT (ft_text_search::tests): 4 new green — build_text_response_with_total
      (reply[0]=47, C2), 3-arg wrapper (reply[0]=3, C2), merge sums 40+35+25=100 (C4), errored-shard→0
      (Reject). E2E (fts_query_eval_e2e): 15/15 incl. 3 new — eval_query_counted (5 results, total 12,
      .0==eval_query, C1+struct-Reject), LIMIT 0 5→reply[0]=12 (C3), no-LIMIT→12 (C5). Full lib
      regression 3594 passed / 0 failed / 1 ignored. WIRE PROBE (release bin, 1+4 shard servers, 30
      docs, LIMIT 0 5): both reply total=30, returned=5, 1==4 parity, no-LIMIT total=30 → "PROBE PASS"
      (the §3 C4 canary, over the real wire).
- [x] coverage did not decrease — +7 tests (4 unit, 3 e2e); none removed.
- [x] no test or contract was altered during build — the frozen fts-query-eval-dispatch §3 contract is
      UNTOUCHED: `eval_query -> Vec<TextSearchResult>` and 3-arg `build_text_response` keep their exact
      signatures + outputs (now thin wrappers). The count was added via NEW additive symbols only. The
      pre-existing merge unit tests (descending_sort asserts total==2 for two items[0]=1 frames) still
      pass under the new Σ logic — NOT weakened (1+1==2 coincides with the old count when each shard
      reports == its returned docs).
- [x] concurrency / timing — NONE introduced. eval_query_counted/build_text_response_with_total are pure
      functions; merge_text_results adds one i64 accumulator over an already-iterated slice. No new state,
      atomics, locks, or `.await`. Per-shard frames flow over the existing SPSC channels (unchanged).
- [x] no exposed secrets, injection, or unexpected deps — internal RESP-shaping change; reply[0] was
      already a Frame::Integer (only its VALUE changes). No I/O, no parsing of untrusted input, no new
      dependency, no on-disk/wire format change.
- [x] layering & dependencies follow conventions — eval logic in src/text/query/eval.rs, response/merge
      in src/command/vector_search/ft_text_search.rs, one call-site update in run_text_query_on_index.
      No cross-layer reach; no hot-path allocation added (perf-rejected the recompute-eval_set framing).
- [x] a person reviewed and approved — auto-resolved under autonomy:auto on complete evidence (internal,
      non-security; the one concurrency surface — per-shard count over SPSC — is exercised by the live
      4-shard wire probe) + manual review: additive-wrapper soundness (frozen 2b symbols delegate, outputs
      identical), Σ-tolerance of Error/odd frames (no panic), total = matched-AND-resolvable (A1 choice).

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — `eval_query_counted` defined (eval.rs:122), re-exported (query/mod.rs:17), called by
      run_text_query_on_index (ft_text_search.rs:1564) + tests; `eval_query` delegates (eval.rs:118).
      `build_text_response_with_total` called by run_text_query_on_index (:1565) + unit test; 3-arg
      `build_text_response` delegates (:1858). merge Σ at merge_text_results. Confirmed via grep + green build.
- [x] DEAD-CODE (code) — no orphan: both new fns are reached on the LIVE path; the 3-arg `build_text_response`
      remains used by the dead-but-compiled InvertedSearch/filter handlers (spsc_handler.rs:1532,
      coordinator.rs:1854/1885 — the known carried-flag `scatter_text_search_filter` path, OUT OF SCOPE per
      §3) and unit tests. Clippy clean on default + runtime-tokio,jemalloc (`-D warnings`) — an unused symbol
      would have failed.

### GATE RECORD
Outcome: PASS
Reviewed by: auto-resolved (autonomy:auto) + Tin Dang · date: 2026-06-16

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
