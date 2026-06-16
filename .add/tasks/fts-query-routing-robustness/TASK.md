# TASK: FT.SEARCH routing robustness: SPARSE detection + no expect() on the BM25 AND path

slug: fts-query-routing-robustness · created: 2026-06-16 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Make FT.SEARCH query ROUTING robust on two confirmed defects + one approved gap, so a query
goes to the right engine (BM25 text vs vector/SPARSE) and the BM25 AND path never panics:
  • R1 (panic-safety) — `TextStore::search_field` (store.rs) holds 3× `.expect("posting exists: checked
    above")` (lines ~463/470/500) on the live BM25 AND/scoring path. The preceding `is_none()` guard
    makes them currently-unreachable, but they violate the "no unwrap/expect in library code" rule and a
    future edit to the guard turns malformed/raced state into a server panic.
  • R2 (text mis-routed to vector) — `is_text_query(args[1])` uppercases then matches the BARE substring
    `"KNN "`, so a legitimate text search like `"knn tutorial"` is classified NON-text → falls to
    `ft_search` → `parse_knn_query` (case-SENSITIVE, needs the `*=>[KNN k @f $p]` bracket form) returns
    None → `ERR invalid KNN query syntax`. A real text query errors out. The canonical KNN marker is the
    `[KNN` bracket; bare "knn" in prose is text.
  • R3 (vector mis-routed to text — approved scope) — `is_text_query` only inspects `args[1]`, so it
    CANNOT see a standalone `... SPARSE @f $p` clause (a separate arg). A query like
    `FT.SEARCH idx "machine" SPARSE @vec $q` (text-looking args[1] + standalone SPARSE, no HYBRID) takes
    the text fast-path and SILENTLY DROPS the SPARSE retriever. (SPARSE-inside-HYBRID is already deferred
    by `parse_hybrid_modifier`; only the standalone form leaks.) User approved adding the guard.
Framings weighed: R2 retarget is_text_query to the `[KNN` bracket marker + R3 add a `has_sparse_clause`
routing guard at the text-fast-path gates (chosen) · make is_text_query take the full `args` and fold
both checks in (rejected — invasive signature change across 4 call sites + the args[1]-only unit
contract) · leave routing, only fix the doc (rejected by user — leaves the standalone-SPARSE leak).
Must:
<must>
  - R1 — `TextStore::search_field` contains NO `.expect`/`.unwrap`/`panic` on the posting-lookup path.
    A `get_posting` that returns `None` is handled defensively: a missing AND-term posting ⇒ the AND has
    no matches ⇒ return empty results; a missing posting during per-doc scoring ⇒ skip that term's BM25
    contribution (`continue`). Search OUTPUT is byte-identical for the currently-reachable (all-postings-
    present) case — this is panic-hardening, not a behavior change.
  - R2 — `is_text_query(query)` returns NON-text ONLY for `*` (match-all) or the canonical vector-KNN
    bracket marker `[KNN` (case-insensitive); a bare word "knn"/"KNN" in prose (e.g. `"knn tutorial"`,
    `"learn knn basics"`) is a TEXT query. Real `*=>[KNN 10 @vec $q]` stays non-text. The doc comment is
    corrected to describe the true args[1]-only contract (clause-level SPARSE/HYBRID handled at routing).
  - R3 — a query carrying a standalone `SPARSE @field $param` clause is NOT taken onto the BM25 text
    fast-path; it defers to `ft_search` (the vector/sparse engine). Implemented by a
    `has_sparse_clause(args)` helper AND'd into the text-route gate at ALL FOUR routing sites
    (handler_monoio/ft.rs, handler_sharded/ft.rs, handler_single.rs, spsc_handler.rs). HYBRID
    (parse_hybrid_modifier) deferral is unchanged.
  - R4 — every existing text/combinator behavior (term, AND, OR, TEXT+TAG, NUMERIC, fuzzy, prefix,
    @field) and the canonical KNN/standalone-`*`-SPARSE/HYBRID dispatch is UNCHANGED — the full FT suites
    plus the is_text_query unit tests (updated for the corrected KNN semantics) stay green.
</must>
Reject:
<reject>
  - `search_field` term whose posting is absent (missing/raced) -> empty result set, never a panic
    (defensive `let Some(..) else`) -> "missing_posting_no_panic"
  - FT.SEARCH whose args[1] is text BUT a standalone SPARSE clause is present -> routed to ft_search, not
    the text path (SPARSE not dropped) -> "sparse_clause_defers_to_vector"
  - FT.SEARCH args[1] containing the word "knn" but NOT the `[KNN` bracket -> treated as a text query,
    not a malformed KNN error -> "bare_knn_word_is_text"
</reject>
After:
<after>
  - `FT.SEARCH idx "knn tutorial"` returns text results (not `ERR invalid KNN query syntax`).
  - `FT.SEARCH idx "machine" SPARSE @vec $q` reaches the sparse engine (SPARSE honored, not dropped).
  - A `search_field` call over an index with a vanished posting returns empty, never crashes the server.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 — the canonical vector-KNN marker is the `[KNN` bracket (`*=>[KNN k @f $p]`), so keying
    is_text_query on `[KNN` (not bare `"KNN "`) both fixes the prose false-positive AND keeps real KNN
    non-text. Lowest confidence because the EXISTING unit test asserts `!is_text_query(b"knn 10")` (bare,
    no bracket) — my change makes that TEXT, so I must UPDATE that one assertion (the old test encoded the
    over-aggressive detection R2 fixes; not a weakening — the canonical `*=>[KNN ...]` cases still assert
    non-text, and new cases pin the prose-is-text fix). If a real KNN query without brackets exists in the
    wild: it would now route to text → a coded text error rather than KNN — but `parse_knn_query` is
    case-sensitive and the bracket form is the documented syntax, so this is the correct tightening.
  - [ ] A2 — `parse_sparse_clause(args)` (pub(crate), ft_search::parse) is the authoritative standalone-
    SPARSE detector and is reachable from the handlers via a thin `has_sparse_clause` wrapper. (high —
    it's the same fn ft_search uses to dispatch SPARSE.)
  - [ ] A3 — the 4 routing sites are the COMPLETE set of text-fast-path gates; guarding each closes the
    standalone-SPARSE leak everywhere. (high — grep of is_text_query routing callers: monoio/sharded/
    single/spsc; the early `is_text` computations feed these same gates.)
  - [ ] A4 — R1's defensive returns are output-identical on the reachable path (postings always present
    after the `is_none` guard), so no FT result changes. (high — the expects never fire today; replacing
    a never-taken panic with a never-taken empty-return cannot change observable output.)
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# R2 — prose containing "knn" is a TEXT query, not a malformed KNN error
Scenario: a text search for "knn tutorial" returns text results
  Given an index with docs whose body contains "knn" and "tutorial"
  When FT.SEARCH idx "knn tutorial" runs
  Then it returns the matching text documents (a BM25 result array)
  And it does NOT return "ERR invalid KNN query syntax"

# R2 — canonical KNN bracket syntax is still routed to the vector engine (unchanged)
Scenario: a real KNN query stays non-text
  Given any index
  When is_text_query is asked about "*=>[KNN 10 @vec $query]"
  Then it returns false (routes to ft_search), and "*" also returns false

# R3 — standalone SPARSE clause defers to the vector engine even with text args[1]
Scenario: text-looking args[1] + standalone SPARSE is not eaten by the text path
  Given a query `FT.SEARCH idx "machine" SPARSE @vec $q PARAMS 2 q <blob>`
  When the handler decides the route
  Then the text fast-path is NOT taken (has_sparse_clause is true → defer to ft_search)
  And the SPARSE retriever is honored (not silently dropped)

# R3 — HYBRID and a normal text query are unaffected (no over-deferral)
Scenario: a bare text query with no SPARSE clause still takes the text path
  Given `FT.SEARCH idx "machine learning"` (no SPARSE, no HYBRID)
  When the handler decides the route
  Then the BM25 text fast-path IS taken (has_sparse_clause is false)
  And HYBRID queries still defer via parse_hybrid_modifier as before

# R1 — a vanished posting yields empty results, never a panic
Scenario: search_field tolerates a missing posting on the AND path
  Given a multi-term AND query where one term's posting is absent at scoring time
  When search_field runs
  Then it returns an empty result set (the AND cannot match) and does NOT panic
  And when all postings are present the results/scores are byte-identical to before

# Reject — missing_posting_no_panic (defensive control flow, no expect)
Scenario: search_field has no expect/unwrap on the posting path
  Given the search_field source after the change
  When audited (grep) for `.expect(`/`.unwrap(` on get_posting results
  Then there are none; every get_posting None is a `let Some(..) else` empty-return or `continue`
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
FT.SEARCH ROUTING + BM25-AND panic-hardening. No wire/RESP shape change; one NEW pub helper.

R1 — TextStore::search_field (src/text/store.rs): replace the 3 `.expect("posting exists: checked
     above")` (the candidate-bitmap init ~:461-463, the AND-intersection loop ~:467-471, the per-doc
     scoring loop ~:497-500) with defensive control flow:
       let Some(first_posting) = field_postings[fidx].get_posting(term_postings[0].1) else {
           return Vec::new(); };                                    // AND with absent term ⇒ no matches
       let Some(posting) = field_postings[fidx].get_posting(*term_id) else { return Vec::new(); };  // AND loop
       let Some(posting) = field_postings[fidx].get_posting(*term_id) else { continue; };           // scoring loop
     No other logic changes; the preceding `is_none()` guard (~:449-454) keeps these branches
     unreachable today, so output is byte-identical (missing_posting_no_panic, A4).

R2 — is_text_query(query: &[u8]) -> bool (src/command/vector_search/ft_text_search.rs): SIGNATURE
     UNCHANGED. New body returns false ONLY for `query == b"*"` OR the uppercased bytes containing the
     `[KNN` bracket marker (the canonical `*=>[KNN k @f $p]` vector-query form); everything else is text.
     Replaces the bare `windows(4)=="KNN "` substring scan. Doc comment rewritten to the true
     args[1]-only contract (clause-level SPARSE/HYBRID are deferred at the routing layer, not here).
     bare_knn_word_is_text: "knn tutorial" / "learn knn" → text.

R3 — NEW: pub fn has_sparse_clause(args: &[Frame]) -> bool
       = crate::command::vector_search::ft_search::parse::parse_sparse_clause(args).is_some()
     (defined in ft_text_search.rs, re-exported from vector_search::mod alongside is_text_query).
     AND-ed into EVERY text-route gate so a standalone SPARSE clause defers to ft_search
     (sparse_clause_defers_to_vector). The SIX gate points across the FOUR routing files:
       • handler_monoio/ft.rs  — `let is_text = …is_text_query(q)` (~:65, feeds the multi-shard `if is_text`
                                  at ~:156) AND the single-shard `if is_text_query(query_bytes)` (~:440)
       • handler_sharded/ft.rs — `let is_text = …is_text_query(q)` (~:67, feeds `if is_text` ~:160) AND
                                  the single-shard `if is_text_query(query_bytes)` (~:340)
       • handler_single.rs     — `if is_text_query(query_bytes)` (~:1336)
       • spsc_handler.rs       — `if query_bytes.map_or(false, is_text_query)` (~:1844)
     Each becomes `<existing is_text_query check> && !has_sparse_clause(<args>)`. parse_hybrid_modifier
     deferral (which already catches SPARSE-inside-HYBRID) is untouched.

UNCHANGED: is_text_query signature; all combinator/text/HYBRID/standalone-`*`-SPARSE/KNN-bracket
     dispatch; search_field output on the reachable path; no on-disk/wire format; no new dependency.
TESTS TOUCHED: the is_text_query unit test asserting `!is_text_query(b"knn 10")` is UPDATED to
     `is_text_query(b"knn 10")` (corrected R2 semantics — bare "knn" with no bracket is text); the
     `*=>[KNN …]` and `*` non-text assertions stay. This is a contracted spec change, not a weakening.
```

Status: FROZEN @ v1 — auto-frozen 2026-06-16 under autonomy:auto per "implement all remaining tasks"; the
SPARSE-guard scope (R3) was explicitly user-approved via AskUserQuestion ("Add standalone-SPARSE guard").
No further design fork. Names match the GLOSSARY (is_text_query, parse_sparse_clause, parse_hybrid_modifier,
search_field, ft_search).
Least-sure flag surfaced at freeze: [test] A1 — R2 flips the existing `!is_text_query(b"knn 10")` assertion
to `is_text_query(b"knn 10")` (bare "knn 10", no `[KNN` bracket, is now TEXT). The old assertion encoded the
over-aggressive bare-substring detection this task fixes; the canonical `*=>[KNN …]`/`*` non-text cases are
retained and new prose-is-text cases added. Cost if a bracket-less KNN form exists in the wild: it routes to
a coded text error instead of KNN — acceptable, since parse_knn_query is case-sensitive and the bracket form
is the documented syntax. [contract] R3 correctness depends on parse_sparse_clause being the authoritative
standalone-SPARSE detector and all SIX gate points being guarded — pinned by a routing unit test
(has_sparse_clause) + the existing multi-shard FT suites staying green.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must (R1–R4) + every Reject. RED-driver: `has_sparse_clause` does not exist →
compile-red; the corrected `is_text_query` semantics → assertion-red (the fn exists but mis-classifies
"knn tutorial" today). R1 is a refactor whose correctness IS "output unchanged" — covered by the full FT
regression staying green + a source audit (no `.expect` in search_field); its missing-posting branch is
unreachable through the public API (the upstream `is_none` guard), so it has no standalone behavioral test
(honest: a behavioral test cannot reach a guard-unreachable branch without breaking encapsulation).
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  UNIT — `src/command/vector_search/ft_text_search.rs` #[cfg(test)] mod (where is_text_query tests live):
  - is_text_query_prose_knn_is_text (R2, bare_knn_word_is_text): assert is_text_query(b"knn tutorial")
    AND is_text_query(b"learn knn basics") AND is_text_query(b"knn 10") — bare "knn", no bracket, is TEXT.
    ASSERTION-RED today (current impl returns false for these). This SUPERSEDES the old
    `assert!(!is_text_query(b"knn 10"))` line (updated per the frozen contract — corrected semantics).
  - is_text_query_canonical_knn_is_not_text (R2): assert !is_text_query(b"*=>[KNN 10 @vec $query]")
    AND !is_text_query(b"*=>[KNN 5 @embedding $q]") AND !is_text_query(b"*") — vector forms stay non-text.
  - has_sparse_clause_detects_standalone (R3, sparse_clause_defers_to_vector): build args
    [idx, "machine", SPARSE, @vec, $q] → has_sparse_clause==true; args [idx, "machine learning"] →
    has_sparse_clause==false. COMPILE-RED (has_sparse_clause missing).
  - text_route_predicate_defers_sparse (R3): for the SPARSE args, is_text_query(args[1]) is true but
    `is_text_query(args[1]) && !has_sparse_clause(args)` is FALSE (defers); for the plain-text args it is
    TRUE (text path). Encodes the exact gate expression the 6 sites use. COMPILE-RED (has_sparse_clause).
  - search_field_and_output_unchanged (R1, A4): build a TextIndex, index docs, run an AND query via
    search_field; assert the matched doc set + ordering are exactly as expected (guards the let-else
    refactor produced identical output on the reachable path). GREEN-guard (already passes; protects R1).
  R1 missing_posting_no_panic: VERIFY-phase source audit — grep search_field for `.expect(`/`.unwrap(` on
    get_posting → zero (red today: 3 expects). Plus the full FT regression (output-identity, R4).
  WIRE PROBE (VERIFY): release server — FT.SEARCH idx "knn tutorial" returns a result array, NOT
    "ERR invalid KNN query syntax" (R2 end-to-end); confirms the routing fix over the wire.
  REGRESSION (R4): full lib + FT e2e/consistency suites stay green (combinators, KNN-bracket, `*`, HYBRID).
</test_plan>

Tests live in: `src/command/vector_search/ft_text_search.rs` (unit: R2 is_text_query, R3 has_sparse_clause
+ predicate, R1 search_field output-unchanged). MUST run red (has_sparse_clause missing → compile-fail;
is_text_query "knn tutorial" → assertion-fail) before Build. R1 panic-safety = source audit + regression
(its branch is guard-unreachable — no behavioral test, stated openly).
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

- [x] all tests pass — UNIT (ft_text_search::tests): 53 passed / 0 failed incl. 3 new —
      is_text_query_prose_knn_is_text (R2: "knn tutorial"/"learn knn"/"knn 10"/"KNN clustering" → text),
      has_sparse_clause_detects_standalone (R3), text_route_predicate_defers_sparse (R3 gate expression);
      is_text_query_knn_is_not_text keeps `*=>[KNN …]`/`*` non-text. E2E (fts_query_eval_e2e): 15/15
      (combinators unchanged). Full lib regression 3597 passed / 0 failed / 1 ignored. WIRE PROBE (release
      bin): `FT.SEARCH idx "knn"`→reply[0]=2, `"knn tutorial"`→1 (AND), NEITHER errors as KNN (R2 fixed
      e2e); canonical `*=>[KNN …]` still routes to the vector engine ("Unknown Index", not text).
- [x] coverage did not decrease — +3 unit tests; one stale assertion (`!is_text_query(b"knn 10")`)
      REPLACED by the corrected-semantics test per the frozen §3 (documented spec change, not a drop).
- [x] no test or contract was altered during build — §3 FROZEN @ v1 untouched. The only pre-existing
      test changed is the `knn 10` assertion, which the contract explicitly authorized flipping (R2's
      corrected KNN semantics); all other suites unchanged. No frozen shape edited.
- [x] concurrency / timing — NONE introduced. R1 swaps `.expect` for `let-else` (same control flow, no
      new state); R2 is a pure byte-scan; R3's `has_sparse_clause` is a read-only arg scan AND-ed into a
      routing branch. No new atomics/locks/`.await`; per-shard model unchanged.
- [x] no exposed secrets, injection, or unexpected deps — internal routing/parsing change; reuses the
      existing `parse_sparse_clause`. No I/O, no untrusted-input parsing added, no new dependency, no
      wire/disk format change.
- [x] layering & dependencies follow conventions — `is_text_query`/`has_sparse_clause` in ft_text_search.rs
      (re-exported via vector_search::mod); routing guards in the 4 handler files reach the helper through
      the public `crate::command::vector_search::` path; R1 isolated to `TextStore::search_field`. No new
      hot-path allocation (the uppercase copy already existed).
- [x] a person reviewed and approved — the SPARSE-guard scope (R3) was user-approved via AskUserQuestion
      ("Add standalone-SPARSE guard"); the rest auto-resolved under autonomy:auto on complete green
      evidence + manual review: all 3 expects gone (audit), the 6 gate points AND in `!has_sparse_clause`,
      and the `knn 10` assertion-flip is the contracted R2 change.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — `has_sparse_clause` defined (ft_text_search.rs), re-exported (vector_search/mod.rs:53),
      AND-ed into all SIX routing gates: handler_monoio/ft.rs:68 + :442, handler_sharded/ft.rs:68 + :342,
      handler_single.rs:1338, spsc_handler.rs:1845. `is_text_query` keys on `[KNN` (the bare `KNN ` scan is
      gone). All 3 `expect("posting exists")` removed from search_field (grep: none). Confirmed via grep +
      green build.
- [x] DEAD-CODE (code) — no orphan: `has_sparse_clause` is reached at 6 sites + tests; `parse_sparse_clause`
      now has an extra (handler) caller. Clippy clean on default + runtime-tokio,jemalloc (`-D warnings`).

### GATE RECORD
Outcome: PASS
Reviewed by: auto-resolved (autonomy:auto, R3 scope user-approved) + Tin Dang · date: 2026-06-16

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
