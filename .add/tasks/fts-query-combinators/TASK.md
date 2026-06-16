# TASK: OR unions + TEXT+TAG intersects — correct combinator result sets

slug: fts-query-combinators · created: 2026-06-16 · stage: production · risk: high · autonomy: conservative
phase: build   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- risk: high — freezes the FT query GRAMMAR + matched-set semantics (the contract
     fts-search-count-semantics depends-on and counts over) AND changes wire-visible FT.SEARCH
     behavior. Verify must NOT auto-pass: human gate (run.md unguarded_high_risk_auto guard). -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: FT.SEARCH query-combinator parser + evaluator — a proper recursive-descent parser over
the RediSearch query subset (terms with modifiers · implicit-AND juxtaposition · OR `|` · grouping
`( )` · field clauses `@f:` · TAG `{a|b}` · NUMERIC `[min max]`) producing a small AST, evaluated to
correct matched doc-id SETS via RoaringBitmap union/intersection — replacing the current ad-hoc
string-slicing parser that drops `|` and mis-tokenizes second `@field:` clauses.
Framings weighed: full recursive-descent AST parser + RoaringBitmap evaluator reusing the existing
per-clause leaves (search_field / search_field_or / search_tag / numeric) (chosen) · thin clause-combiner
bolt-on over the old parser (rejected — won't compose to @a:x|@b:y, multi-tag, numeric combos) · two
point-fixes for just `t1|t2` and `@text:.. @tag:{..}` (rejected — re-opened on the next combo shape)
Must:
<must>
  - M1 — UNION: `a | b` (default field or within `@f:(a|b)`) returns the UNION of {docs matching a} ∪
    {docs matching b}, NOT their intersection. The `|` token is parsed as a boolean-OR node, never
    discarded. [fixes Defect 1: tokenize_with_modifiers drops `|` → AND, ft_text_search.rs:1095]
  - M2 — MULTI-CLAUSE INTERSECTION (generic): a query with ≥2 field clauses — any mix of TEXT
    `@t:words`, TAG `@g:{v}`, NUMERIC `@n:[lo hi]` — returns the INTERSECTION of each clause's matched
    doc set. Each TAG/NUMERIC clause is dispatched to its own evaluator (search_tag / numeric range),
    NEVER word-tokenized into text terms. [fixes Defect 2 generically: parse_field_targeted_query
    slices after first `:`, ft_text_search.rs:1273 — covers @text+@tag, @text+@num, @tag-first]
  - M3 — GROUPING: `( … )` groups a sub-expression; `@f:(a | b)` applies the union scoped to field f;
    groups nest and compose with AND/OR.
  - M4 — PRECEDENCE is explicit + documented, matching RediSearch DIALECT 2: ladder is AND
    (juxtaposition, highest) > term-modifiers (`%`fuzzy / `*`prefix) > OR `|` (lowest); parentheses
    override. So `a b | c d` ≡ `(a b) | (c d)`. [VERIFIED — redis.io query_syntax, see A1]
  - M5 — NO REGRESSION on paths that already work: single term, all-Exact space-AND, fuzzy `%t%` /
    prefix `t*`, and a single `@field:term(s)` clause return byte-identical matched sets + ordering to
    today. (The known-correct AND path and modifier handling are preserved, not rewritten away.)
  - M6 — Correct matched SET + a STABLE ordering is the contract. Combined BM25 scoring across OR
    branches is BEST-EFFORT (reuse existing per-term BM25; exact union-score formula is NOT frozen
    here). True total-matched COUNT is OUT — owned by `fts-search-count-semantics`, which depends-on
    and counts over this set contract.
  - M7 — NEVER PANIC on malformed/partial input: the parser returns `Frame::Error` with a named code;
    no `expect`/`unwrap`/`panic!` on the FT query path. (Aligns with `fts-query-routing-robustness`;
    a fuzz target covers the new parser per CLAUDE.md.)
</must>
Reject:
<reject>
  - unbalanced `(` `)` / `{` `}` / `[` `]`                         -> "syntax_error"   (Frame::Error, never panic)
  - empty query, or an empty group `()` with no terms              -> "empty_query"
  - `@name:` where name is not a schema field                     -> "unknown_field"
  - NUMERIC filter not two parseable numbers, or min > max         -> "numeric_filter_invalid"
  - TAG filter with no values `{}` / `{ }`                        -> "tag_filter_invalid"
  - a term that matches no document  -> NOT an error: contributes the empty set to its node (tf_absent
    semantics, consistent with [[project_v3_1_fts_hardening]] fts-posting-rank-tf). The whole query
    returning zero matches is a valid empty reply, not an error.
</reject>
After:
<after>
  - OR queries return unions (bench term-OR: ~2,072, not ~10); TEXT+TAG and TEXT+NUMERIC combos return
    intersections (bench combo: ~253, not 0); multi-`@clause` queries dispatch each clause correctly.
  - The query grammar + AST + matched-set semantics are the FROZEN contract `fts-search-count-semantics`
    counts over; the old string-slicing parser (pre_parse_field_filter / parse_field_targeted_query /
    the `|`-dropping tokenizer) is retired on the FT.SEARCH path.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 (now the lowest-confidence-but-DELIBERATE call) — SCOPE: a NEW recursive-descent parser+AST
    REPLACES the ad-hoc parser on the FT.SEARCH path (parse_text_query / parse_field_targeted_query /
    pre_parse_field_filter / the `|`-dropping tokenizer), reusing search_field / search_field_or /
    search_tag / numeric as evaluator leaves. Lowest confidence because the dispatch wiring in
    ft_text_search.rs is broad (~1400-1810) and some exotic already-working shape (e.g. quoted phrase,
    HYBRID-adjacent text) may still route through an old path; full replacement risks a silent
    regression there. If wrong: a compatibility shim / larger integration surface. → I assume full
    replacement on the pure-text path with M5 (byte-identical on working shapes) as the guard;
    HYBRID/vector/SPARSE dispatch is untouched. (Mitigated by M5 regression scenarios + the existing
    144 text unit + 15 store-search tests as the no-regression net.)
  - [x] A2 — PRECEDENCE: AND > term-modifiers > OR, `a b | c d` ≡ `(a b) | (c d)`, parens override.
    VERIFIED against RediSearch DIALECT 2 (redis.io query_syntax; the DIALECT-1→2 breaking change
    confirms `hello world | "goodbye" moon` now parses as the two-branch union). No longer an open risk.
  - [ ] A3 — doc-id space is SHARED across TEXT/TAG/NUMERIC (verified: `ensure_doc_id`, store.rs:242),
    so RoaringBitmap set-ops compose directly with no id/hash translation. (high — verified in trace)
  - [ ] A4 — one AST allocation PER FT.SEARCH COMMAND is acceptable (per-command, not per-key/per-doc;
    precedent: the authorized per-query `FtSearchPlan` box). No new per-key hot-path alloc. (high)
  - [ ] A5 — term modifiers (`%fuzzy%`, prefix `*`, exact) stay term-level leaves and compose under the
    new grammar unchanged; fuzzy/prefix still expand via search_field_or as today. (high)
  - [ ] A6 — NOT/negation (`-term`) and SLOP/INORDER/optional (`~`) are OUT of v1 scope (the benchmark
    did not exercise them; not a correctness defect today). If present in current code, left untouched;
    if absent, stays absent — noted, not silently dropped. (high)
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# ── M1 — OR is a union, not an AND ────────────────────────────────────────
Scenario: term OR returns the union, not the intersection
  Given docs where "alpha" matches {1,2,3} and "beta" matches {3,4,5} in the default field
  When FT.SEARCH runs query "alpha | beta"
  Then the matched doc set is {1,2,3,4,5}            # union (5), not the AND {3} (~the old ~10 bug)
  And every returned key actually contains alpha OR beta

# ── M2a — TEXT + TAG combo intersects (the 0-result bug) ──────────────────
Scenario: TEXT and TAG clauses intersect instead of returning zero
  Given docs where @body:"foo" matches {1,2,3,4} and @tag:{bar} matches {3,4,5}
  When FT.SEARCH runs query "@body:foo @tag:{bar}"
  Then the matched doc set is {3,4} (the intersection), NOT empty
  And the TAG clause is dispatched to the tag evaluator, not word-tokenized into body terms

# ── M2b — TEXT + NUMERIC combo intersects (same generic fix) ──────────────
Scenario: TEXT and NUMERIC clauses intersect
  Given docs where @body:"phone" matches {1,2,3} and @price in [10 20] matches {2,3,9}
  When FT.SEARCH runs query "@body:phone @price:[10 20]"
  Then the matched doc set is {2,3}
  And the NUMERIC filter is evaluated as a range, not tokenized into body terms

# ── M3 — grouping scopes a union under an AND ─────────────────────────────
Scenario: parentheses scope a union within an intersection
  Given docs where "red" matches {1,2,3}, "blue" matches {3,4}, "car" matches {2,3,4,5}
  When FT.SEARCH runs query "car (red | blue)"
  Then the matched doc set is {2,3,4}   # car ∩ (red ∪ blue) = {2,3,4,5} ∩ {1,2,3,4} = {2,3,4}
  And the result is identical to the explicit "car red | car blue" union

# ── M4 — precedence: AND binds tighter than OR (DIALECT 2) ────────────────
Scenario: unparenthesized mixed query groups AND tighter than OR
  Given docs where "a" matches {1,2}, "b" matches {2,3}, "c" matches {7,8}, "d" matches {8,9}
  When FT.SEARCH runs query "a b | c d"
  Then the parse is (a AND b) OR (c AND d) and the matched set is {2} ∪ {8} = {2,8}
  And it is NOT {2,3,7} (which the wrong "a AND (b|c) AND d" or "(a b|c) d" precedence would give)

# ── M5 — no regression on already-correct shapes ──────────────────────────
Scenario: existing single-term, AND, fuzzy/prefix, and single-field queries are unchanged
  Given the existing text-store corpus and the current 144 text-unit + 15 store-search tests
  When the new parser handles "alpha", "alpha beta", "%alpa%", "al*", and "@body:alpha"
  Then each returns a matched set + ordering byte-identical to the pre-change implementation
  And the full pre-existing FT text test suite stays green

# ── M6 — matched set + stable ordering is the contract (scoring best-effort)
Scenario: OR result set and ordering are deterministic and correct
  Given the M1 corpus
  When "alpha | beta" runs twice
  Then both runs return the SAME ordered key list (stable, deterministic)
  And BM25 scores are present and monotonic with relevance (exact union-score formula NOT asserted)

# ── M7 — malformed input never panics ─────────────────────────────────────
Scenario: a malformed query returns an error frame, the server stays up
  Given a running server
  When FT.SEARCH runs query "alpha | (beta"           # unbalanced paren
  Then the reply is an error frame coded "syntax_error"
  And the server process does not panic and serves the next command normally

# ── Reject — named error codes (each leaves state unchanged: read-only path) ──
Scenario: unbalanced bracket/brace/paren is a syntax error
  When FT.SEARCH runs "@tag:{bar"  (or "a [10 20"  or "a )")
  Then the reply is error "syntax_error"
  And no documents are returned and the index is unchanged

Scenario: empty query or empty group is rejected
  When FT.SEARCH runs ""  (or "()")
  Then the reply is error "empty_query"
  And the index is unchanged

Scenario: a field not in the schema is rejected
  Given an index whose schema has no field "nope"
  When FT.SEARCH runs "@nope:foo"
  Then the reply is error "unknown_field"
  And the index is unchanged

Scenario: an invalid numeric filter is rejected
  When FT.SEARCH runs "@price:[20 10]"  (min>max)  or "@price:[x y]"  (non-numeric)
  Then the reply is error "numeric_filter_invalid"
  And the index is unchanged

Scenario: an empty tag filter is rejected
  When FT.SEARCH runs "@tag:{}"  (or "@tag:{ }")
  Then the reply is error "tag_filter_invalid"
  And the index is unchanged

Scenario: a term matching nothing is a valid empty result, not an error
  Given a corpus where "zzz" appears in no document
  When FT.SEARCH runs "zzz"  and  "alpha zzz"  and  "alpha | zzz"
  Then "zzz" and "alpha zzz" return an empty result set (count 0), NO error
  And "alpha | zzz" returns exactly alpha's matched set (zzz contributes ∅ to the union)
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
COMMAND  FT.SEARCH <index> <query> [LIMIT off cnt] [other opts unchanged]
  query is parsed by a NEW recursive-descent parser over this FROZEN grammar (RediSearch subset,
  DIALECT-2 precedence). Bytes in, AST out, matched doc-id SET out.

GRAMMAR (EBNF — the frozen accepted subset; anything outside -> "syntax_error"):
  query      = union
  union      = intersect ( '|' intersect )*           # OR  — lowest precedence
  intersect  = factor ( WS+ factor )*                 # implicit AND — binds tighter than '|'
  factor     = group | field_clause | term
  group      = '(' union ')'
  field_clause = '@' field ':' ( tag_filter | numeric_filter | group | term+ )
  tag_filter   = '{' tag_value ( '|' tag_value )* '}'   # values OR-union within the tag field
  numeric_filter = ('['|'(') num WS+ num (']'|')')      # REUSE existing pre_parse_numeric_range:
                                                        # '(' = exclusive bound, +inf/-inf supported
  term       = WORD modifier?                          # WORD = analyzer token(s)
  modifier   = '*'(prefix) | '%'…'%'(fuzzy)            # existing term modifiers, term-level leaves
  # OUT of v1 (left exactly as today, never silently dropped): negation '-', optional '~',
  # phrase quotes "…", SLOP/INORDER. Reaching them -> unchanged-or-syntax_error (never a panic).

AST  (FROZEN node type — src/text/query/ast.rs; fts-search-count-semantics consumes THIS):
  enum QueryNode {
    Term   { field: Option<FieldIdx>, token: Bytes, modifier: TermModifier },
    And    (Vec<QueryNode>),          // intersect children
    Or     (Vec<QueryNode>),          // union children
    Tag    { field: FieldIdx, values: Vec<Bytes> },     // values OR-unioned (per-value search_tag)
    Numeric{ field: FieldIdx, min: f64, max: f64, min_excl: bool, max_excl: bool },  // reuse range eval
    Empty,                            // a node that matches ∅ (absent term) — NOT an error
  }
  fn parse_query(input: &[u8], schema: &IndexSchema) -> Result<QueryNode, QueryError>
  enum QueryError { Syntax, EmptyQuery, UnknownField(Bytes), NumericInvalid, TagInvalid }
     // maps 1:1 to the wire error codes below; carries NO panic path (M7).

EVALUATOR  (src/text/query/eval.rs — the SET contract):
  fn eval_set(node:&QueryNode, idx:&TextIndex, dfs:Option<&Dfs>) -> RoaringBitmap   // matched doc-ids
    Term/field-term -> reuse search_field / search_field_or leaf -> collect its result doc_ids
    Tag    -> reuse search_tag(field,value) per value (-> Vec<u32>) -> OR-union into a bitmap
    Numeric-> reuse search_numeric_range(field,min,max,min_excl,max_excl) (-> Vec<u32>) -> bitmap
    And(xs) -> fold ∩ (RoaringBitmap &=)   ·   Or(xs) -> fold ∪ (|=)   ·   Empty -> ∅
  # leaves already return doc-ids in the SHARED id space (ensure_doc_id); Vec<u32> -> RoaringBitmap
  # collect is the only adaptation. & / | compose directly. (A set-only fast path that skips BM25
  # scoring on pure-filter leaves is a permitted optimization, NOT required for v1 correctness.)

WIRE REPLY:
  success -> existing FT.SEARCH array reply over eval_set(root): docs ordered by BM25 score DESC,
             tie-break doc_id ASC (STABLE, deterministic). BM25 score across OR branches = best-effort
             (sum of matched-leaf contributions); the exact union-score formula is NOT frozen here.
  error   -> Frame::Error, code ∈ { "syntax_error" | "empty_query" | "unknown_field"
             | "numeric_filter_invalid" | "tag_filter_invalid" }.  Index unchanged (read-only path).

FROZEN BOUNDARY for fts-search-count-semantics (depends-on this task):
  the "total matched" it must report == eval_set(root).len()  (cardinality of the matched doc-id set,
  BEFORE LIMIT/paging). This task freezes eval_set; the count task reads its cardinality.

Schema / code surface (new module, additive):
  + src/text/query/{mod.rs, ast.rs, parse.rs, eval.rs}   (new recursive-descent parser + evaluator)
  ~ src/command/vector_search/ft_text_search.rs          (dispatch: route pure-text FT.SEARCH through
                                                           parse_query -> eval_set -> score/reply;
                                                           retire pipe-dropping tokenizer + first-colon slice)
  + fuzz/fuzz_targets/fts_query_parse.rs                  (parser fuzz target — never panics; CLAUDE.md)
  reuse (unchanged): TextIndex::search_field / search_field_or / search_tag, numeric range, RoaringBitmap.
  HYBRID / vector / SPARSE dispatch: UNTOUCHED.
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-06-16). Build SPLIT (freeze decision): this task =
**2a (parser + AST)** — `parse_query` → `QueryNode`/`QueryError` + grammar + parser fuzz target; the
new task **`fts-query-eval-dispatch` = 2b** (eval_set + ft_text_search dispatch + wire reply) depends-on
this one and INHERITS this frozen contract. The §3 shape below is the shared contract both halves honor.
Least-sure flag surfaced at freeze: [spec] FULL-REPLACEMENT SCOPE (§1 A1) — swapping the ad-hoc FT
text parser for a new recursive-descent parser+AST risks silently regressing an exotic already-working
shape that routes through an old path (e.g. quoted phrase, HYBRID-adjacent text); cost if wrong = a
compatibility shim + re-baseline. Mitigated by M5 byte-identical regression scenarios + the existing
144 text-unit / 15 store-search tests as the no-regression net, and by leaving HYBRID/vector/SPARSE
dispatch untouched. [contract] the SET semantics (eval_set cardinality) are what fts-search-count-
semantics inherits — if the union/intersect node algebra is wrong here, the count task re-opens this
contract. (Precedence A2 was the prior top risk; now VERIFIED against RediSearch DIALECT 2, so it
drops out.) [size] this is the largest v3-1 task: new module + dispatch rewrite + fuzz target — scoped
as ONE task per your "full query-AST parser" decision; decomposable if you'd rather split parser/eval.
<!-- Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen contract = change
     request back to SPECIFY. -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: 90% of parse_query branches (every grammar production + every QueryError code).
SCOPE = 2a PARSER ONLY — pure `parse_query(bytes, schema) -> Result<QueryNode, QueryError>` unit tests,
no index/server needed. The end-to-end matched-SET scenarios (M1/M2/M3 result sets, M5 no-regression,
M7 server-stays-up) are 2b's tests (`fts-query-eval-dispatch`); here we assert the PARSE TREE + errors.
Notation: per the frozen §3 AST there is NO separate Field node — a field scopes its terms by being
pushed onto each leaf `Term{field:Some(idx), ...}`. So `Field(body,[foo])` below is shorthand for
`Term{field:body, token:foo}`, and `@f:(a|b)` parses to `Or[Term{f,a}, Term{f,b}]` (field pushed into
the group). `@f:t1 t2` scopes BOTH terms to f (`term+`, Moon-compatible — keeps M5 safe; RediSearch's
stricter one-token binding is a deferred refinement, noted not silently adopted).
Plan (asserting the AST/error, not internals):
<test_plan>
  - test_or_parses_as_or_node: "alpha | beta" -> Or[Term(alpha), Term(beta)]   (M1 at parse level)
  - test_pipe_never_discarded: assert '|' produces an Or node, never dropped (regression vs the bug)
  - test_multiclause_parses_distinct_clauses: "@body:foo @tag:{bar}" -> And[Field(body,[foo]), Tag(tag,[bar])]
    — the tag clause is a Tag node, NOT word-tokens "tag","bar" in body (M2 at parse level)
  - test_text_numeric_clause: "@body:phone @price:[10 20]" -> And[Field(body,[phone]), Numeric(price,10,20,F,F)]
  - test_grouping_scopes_union: "car (red | blue)" -> And[Term(car), Or[Term(red),Term(blue)]]  (M3)
  - test_field_scoped_group: "@f:(a | b)" -> Field(f, Or[Term(a),Term(b)])
  - test_precedence_and_binds_tighter: "a b | c d" -> Or[And[a,b], And[c,d]]  (M4, DIALECT 2)
  - test_modifiers_preserved: "%alpa%" / "al*" -> Term with Fuzzy / Prefix modifier (M5 parse level)
  - test_numeric_exclusive_and_inf: "@price:[(10 +inf]" -> Numeric(price,10,+inf,min_excl=true,max_excl=false)
    (reuse pre_parse_numeric_range — must NOT regress existing capability)
  - test_multi_tag_values: "@tag:{a|b}" -> Tag(tag, [a, b])
  - REJECTS (each asserts the exact QueryError -> wire code, NO panic):
    test_unbalanced_paren_is_syntax: "alpha | (beta" -> Err(Syntax)
    test_unbalanced_brace_bracket: "@tag:{bar" / "@price:[10 20" -> Err(Syntax)
    test_empty_query: "" / "()" -> Err(EmptyQuery)
    test_unknown_field: "@nope:foo" (schema lacks "nope") -> Err(UnknownField)
    test_numeric_invalid: "@price:[20 10]" (min>max) / "@price:[x y]" -> Err(NumericInvalid)
    test_tag_invalid: "@tag:{}" / "@tag:{ }" -> Err(TagInvalid)
    test_absent_term_is_not_error: "zzz" parses OK to Term(zzz) (emptiness is an EVAL outcome, not a parse error)
  - test_parser_never_panics: fuzz-style table of malformed inputs -> all return Err, none panic (M7)
</test_plan>

Tests live in: `tests/fts_query_parse.rs` · MUST run red (missing implementation) before Build.
Also add fuzz target `fuzz/fuzz_targets/fts_query_parse.rs` (parse_query never panics; CLAUDE.md).
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): parse_query is on the malformed-input boundary — NO `unwrap`/`expect`/
`panic!`/indexing-that-can-panic; every error path returns a `QueryError`. The recursive-descent must be
depth-bounded (reject pathological nesting with `Syntax`) so a deep `(((…)))` cannot blow the stack.
SCOPE = 2a PARSER ONLY. Build: `src/text/query/{mod.rs, ast.rs, parse.rs}` — the `QueryNode`/`TermModifier`/
`QueryError` types + `parse_query(bytes, schema) -> Result<QueryNode, QueryError>` honoring the frozen §3
grammar/precedence + `fuzz/fuzz_targets/fts_query_parse.rs`. Do NOT wire dispatch or write `eval_set` here
— that is 2b (`fts-query-eval-dispatch`). Reuse the existing `pre_parse_numeric_range` helper for numeric
bounds rather than re-implementing it.
Code lives in: `src/text/query/`
Constraints: do NOT change any test or the frozen contract; reuse existing crates only (roaring, bytes —
no new deps); ask if unclear.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [ ] all tests pass
- [ ] coverage did not decrease
- [ ] no test or contract was altered during build
- [ ] concurrency / timing of the risky operation is safe
- [ ] no exposed secrets, injection openings, or unexpected dependencies
- [ ] layering & dependencies follow CONVENTIONS.md
- [ ] a person reviewed and approved the change

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [ ] WIRING (code) — every new symbol is referenced; record where / how confirmed
- [ ] DEAD-CODE (code) — no new unused or orphaned symbol introduced
- [ ] SEMANTIC (prose / non-code) — read in full, not skimmed: <what read · what confirmed>

### GATE RECORD
Outcome: <PASS | RISK-ACCEPTED | HARD-STOP>
If RISK-ACCEPTED -> owner: <name> · ticket: <link> · expires: <date>   (never for a security gap)
Reviewed by: <name> · date: <date>

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
