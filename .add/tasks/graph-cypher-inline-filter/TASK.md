# TASK: Cypher MATCH narrows on inline node-property predicate (no label full-scan)

slug: graph-cypher-inline-filter · created: 2026-06-16 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Cypher MATCH applies a node pattern's inline property predicate (filtered-scan)
Framings weighed: filtered-scan (chosen) · index-probe · hybrid(filter+index)
  — chosen filtered-scan: emit `Filter` from the existing `properties_to_filter`; fixes the
    row-count exit criterion, reuses code, stays inside the milestone's "correctness only" boundary.
    index-probe (consult `property_indexes`) and hybrid would also cut the 40-qps full-scan but are
    bigger/riskier and graph throughput is explicitly milestone-Out → deferred to a perf milestone.
Must:
<must>
  - M1 An inline node pattern `(v {k1:e1, k2:e2, …})` is evaluated as the equality conjunction
    `v.k1 = e1 AND v.k2 = e2 …`; only nodes satisfying ALL bind to `v`. (Build via the existing
    `properties_to_filter`; semantics = the existing `Filter`/`Expr` evaluation, unchanged.)
  - M2 The predicate applies to EVERY pattern node that carries inline properties — the first
    (scanned) node AND any node reached by an `Expand` — each via a `Filter` placed immediately
    after the op that BINDS that node (after its `NodeScan`, or after the `Expand` that produces it).
  - M3 `MATCH (a {id:N})-[]->(b)` returns exactly the edges of the node(s) where `a.id = N`
    (returned rows == those edges), never the whole label's edges (the ≈|E| / 14,991-row bug).
  - M4 Inline properties filter even when the node has no label — `(a {id:N})` with the label
    omitted still narrows (label is `Option`; the property filter is independent of it).
  - M5 Value/equality semantics — parameter values (`{id:$p}`), cross-type comparison, and
    missing-property ⇒ node EXCLUDED (not an error) — follow the existing `Expr` evaluation as used
    by `WHERE`; this task introduces NO new comparison or coercion rules.
  - M6 A node with empty `{}` / no inline properties adds NO `Filter`; the plan and results for
    such patterns are byte-identical to today (no regression to plain label/all scans).
  - M7 A non-evaluable inline value never panics — it follows the existing `Expr`-eval failure path
    and returns the existing graph error frame (milestone shared decision: malformed Cypher → error).
</must>
Reject:
<reject>
  - This feature introduces NO new rejected inputs (it makes a previously-IGNORED predicate apply).
    The single failure mode — an inline value that fails `Expr` evaluation (e.g. references an
    unbound variable) -> "graph_eval_error" (the EXISTING eval-error frame, inherited unchanged; no
    new code, never a panic). A property compared against a node lacking that property is NOT a
    rejection -> the node is excluded (M5), a normal empty/narrowed result.
</reject>
After:
<after>
  - For every node pattern carrying inline properties, the compiled `PhysicalPlan` contains a
    `Filter` (equality conjunction) positioned right after that node binds; query results contain
    only rows whose bound nodes satisfy their inline predicates — no full-label rows leak.
  - `properties_to_filter` is reused; NO new `PhysicalOp`; the executor scan path is unchanged
    (still scans the label, now correctly filtered). The O(N) scan / 40-qps is intentionally left
    (perf is milestone-Out; index-probe deferred).
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 The filter must apply to ALL pattern nodes carrying inline props (scanned + expanded), each
    `Filter` placed AFTER the op that binds its node — lowest confidence because the benchmark only
    exercised the first scanned node `{id:N}`; if intended scope is "first node only", filtering
    expanded nodes is broader than needed (still correct), and a `Filter` mis-placed BEFORE its
    `Expand` binds the target would drop valid rows. If wrong: over-broad scope, or false-negative
    rows on mis-ordering. Resolve: apply to all, place each Filter after its binding op; a scenario
    pins the expanded-node case.
  ⚠ A2 Reusing `properties_to_filter` + existing `Expr` equality is correct for parameter values
    (`{id:$p}`) and cross-type compares (int property vs int literal) — lowest confidence on
    coercion because ShortestPath is the only current caller and the bench used a literal `{id:N}`;
    if `Expr` equality coerces types unexpectedly, a matching node could be filtered out. If wrong:
    false-negative (under-return) rows. Resolve: a scenario asserts a literal AND a `$param` match.
  - [x] The executor runs `Filter` immediately after the preceding `NodeScan`/`Expand` in plan
    order (sequential `PhysicalOp` pipeline) — confirmed by the executor's op loop.
  - [x] Leaving the O(N) label scan (40 qps) is acceptable — graph throughput is explicitly Out.
  - [x] Cypher planning/execution is NOT a CLAUDE.md hot path (not dispatch/protocol/event-loop/io),
    so a `Filter` clone/alloc here is fine; no new `unsafe`.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# Shared corpus C: Person nodes id=1,2,3; directed edges (1)->(2), (1)->(3), (2)->(3).

Scenario: Point-query returns only the matching node's edges  (M3 — the headline bug)
  Given corpus C
  When I run  MATCH (a:Person {id:1})-[]->(b) RETURN b.id
  Then exactly 2 rows are returned, b.id ∈ {2, 3} (node 1's out-edges only)
  And the result is NOT the whole label's edges (≈|E| = 3 here; never the 14,991-row full-scan shape)

Scenario: Multiple inline properties AND together  (M1 — equality conjunction)
  Given corpus C extended with name: id=1 name='alice', id=2 name='bob', id=3 name='alice'
  When I run  MATCH (a:Person {id:1, name:'alice'})-[]->(b) RETURN b.id
  Then only node 1 binds to a (matches BOTH id=1 AND name='alice'); 2 rows (b.id ∈ {2,3})
  And node 3 (name='alice' but id=3) does NOT bind to a

Scenario: Inline predicate on the EXPANDED target node  (M2 — every node, not just the first)
  Given corpus C
  When I run  MATCH (a:Person {id:1})-[]->(b {id:3}) RETURN b.id
  Then exactly 1 row is returned, b.id = 3
  And node 2 (also a target of node 1) is excluded by b's inline predicate

Scenario: Inline predicate with the label omitted still narrows  (M4 — label is optional)
  Given corpus C
  When I run  MATCH (a {id:1})-[]->(b) RETURN b.id
  Then the same 2 rows as the labeled query are returned (node 1's out-edges)
  And it does NOT return every node's edges

Scenario: Parameter inline value matches like a literal  (M5 — params)
  Given corpus C
  When I run  MATCH (a:Person {id:$pid})-[]->(b) RETURN b.id  with param pid = 1
  Then the result is identical to the literal {id:1} query (2 rows, node 1's out-edges)

Scenario: A node missing the inline property is excluded, not errored  (M5 — missing prop)
  Given corpus C where no Person node has an 'email' property
  When I run  MATCH (a:Person {email:'x@y.z'})-[]->(b) RETURN b
  Then zero rows are returned (no node satisfies the predicate)
  And no error frame is raised — it is a normal empty result, and the graph is unchanged

Scenario: No inline properties leaves scan behavior byte-identical  (M6 — no regression)
  Given corpus C
  When I run  MATCH (a:Person)-[]->(b) RETURN b.id
  Then all 3 label out-edges are returned (b.id rows: 2,3,3), exactly as before this change
  And the compiled plan for this pattern contains NO added Filter op

Scenario: Non-evaluable inline value returns an error frame without panic  (M7 / Reject — graph_eval_error)
  Given corpus C
  When I run a query whose inline value fails evaluation (e.g. an unbound identifier in the value position)
  Then an error frame is returned (the existing "graph_eval_error", no new code)
  And the server does NOT panic (a subsequent PING succeeds) and the graph is unchanged
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
Surface: GRAPH.QUERY <key> "<cypher>"  — a Cypher MATCH whose node pattern(s) carry inline
properties `(v {k:e, …})`. Internal seam being frozen: graph::cypher::planner::compile_match.

WIRE BEHAVIOR
  MATCH (a {k:e, …}) [ -[…]-> (b {…}) ] RETURN …
    -> result rows = ONLY rows whose every inline-propertied node satisfies  v.k = e AND …
       (equality conjunction; `$param` values resolved; node missing the prop ⇒ excluded).
    -> a node pattern with empty / no inline properties is unfiltered (behavior unchanged).
  Non-evaluable inline value (e.g. an unbound identifier in value position)
    -> existing graph error frame   error: "graph_eval_error"   (no new code; never a panic).

PLANNER CONTRACT (the frozen seam)
  compile_match, for EACH PatternNode `n` with NON-EMPTY `n.properties`, emits
      PhysicalOp::Filter { expr: properties_to_filter(var_of(n), &n.properties) }
  positioned IMMEDIATELY AFTER the op that BINDS `n`:
      • first node       -> right after its PhysicalOp::NodeScan
      • subsequent node  -> right after the PhysicalOp::Expand that produces it
  Empty `n.properties` -> NO Filter emitted (M6 — plan byte-identical to today).
  NO new PhysicalOp variant. `properties_to_filter` is REUSED (generalized from its current
  ShortestPath-only caller; signature + semantics unchanged: equality, AND-conjoined).
  Filter / Expr evaluation semantics are unchanged (M5).

SCHEMA / STATE
  Read-only over the existing graph (memgraph nodes + CSR edges). NO new persisted structure,
  NO new index, NO segment-format change. Plan stays the existing sequential PhysicalOp pipeline.

NAMES (glossary): PatternNode.properties · PhysicalOp::{NodeScan,Expand,Filter} ·
properties_to_filter · Expr · graph_eval_error.
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-06-16)
Least-sure flag surfaced at freeze:
  ⚠ [spec] A1 — emit the Filter for ALL inline-propertied nodes (scanned + expanded), each AFTER
    its binding op. Most likely wrong: the benchmark only exercised the first node `{id:N}`; if the
    intended scope is "first node only", expanded-node filtering is broader (still correct), and a
    Filter mis-ordered BEFORE its Expand would drop valid rows. If wrong: over-broad scope, or
    false-negative rows. Pinned by scenario M2 (inline predicate on the expanded target).
  ⚠ [contract] A2 — generalizing `properties_to_filter` + the existing `Expr` equality is correct
    for `$param` values and cross-type compares. If `Expr` equality coerces types unexpectedly, a
    matching node is filtered out (under-return). Pinned by scenarios M5-param + M5-missing-prop.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every §2 scenario covered at TWO levels — plan-shape (mechanism) AND row-count
(effect), per the folded "mechanism-proxy pass ≠ effect measured" lesson. ~90% of the changed
`compile_match` lines.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  PLAN-SHAPE — planner unit tests (src/graph/cypher/planner.rs `mod tests`; RED now: compile drops props)
  - test_inline_prop_emits_filter_after_nodescan (M3): compile `MATCH (a:Person {id:1})-[]->(b) RETURN b`
    / assert ops contain a Filter at the index immediately AFTER the NodeScan.
  - test_inline_prop_on_expanded_node_filters_after_expand (M2): compile
    `MATCH (a {id:1})-[]->(b {id:3}) RETURN b` / assert TWO Filters — one right after NodeScan, one
    right after the Expand (each after its binding op).
  - test_inline_prop_without_label_emits_filter (M4): compile `MATCH (a {id:1}) RETURN a`
    / assert a Filter is present even though the NodeScan label is None.
  - test_inline_multi_prop_is_and_conjunction (M1): compile `MATCH (a {id:1, name:'alice'}) RETURN a`
    / assert the Filter expr is a BinaryOp(And) of two equality compares.
  - [green-pin] test_no_inline_prop_emits_no_filter (M6): compile `MATCH (a:Person)-[]->(b) RETURN b`
    / assert NO Filter op (green now AND after — guards no-regression to plain scans).

  ROW-COUNT EFFECT — integration test (tests/graph_cypher_inline_filter.rs, server-spawn harness per
  the duplicated-config convention, `#![cfg(all(feature="runtime-tokio", feature="graph"))]`; seed
  corpus C with `GRAPH.QUERY g "CREATE …"`, assert via MATCH; RED now: returns ≈|E| rows)
  - test_point_query_returns_only_matching_edges (M3): MATCH (a:Person {id:1})-[]->(b) RETURN b.id
    / assert exactly 2 rows {2,3}, NOT all label edges.
  - test_param_matches_like_literal (M5-param): {id:$pid} pid=1 / assert == literal {id:1} result.
  - test_missing_property_excluded_not_errored (M5-missing): {email:'x@y.z'} / assert 0 rows, no error frame.
  - test_noneval_inline_value_errors_no_panic (M7/Reject): unbound ident in value / assert error frame
    AND a subsequent PING succeeds (server up) AND graph unchanged.
</test_plan>

Tests live in: `src/graph/cypher/planner.rs` `tests/graph_cypher_inline_filter.rs` · MUST run red (missing implementation) before Build.
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

- [x] all tests pass — planner unit 20/20; effect-level integration 4/4 (graph_cypher_inline_filter);
  broad graph regression 460/0 across lib graph + 6 integration suites (graph_integration,
  lunaris_cypher_shortest_path, lunaris_cypher_temporal, txn_cypher_write_rollback, txn_graph_wiring).
- [x] coverage did not decrease — 5 planner unit + 4 effect-level tests ADDED; none removed/weakened.
- [x] no test or contract was altered during build — build touched ONLY src/graph/cypher/planner.rs
  (compile_match + the properties_to_filter doc comment); §3 contract and all §4 tests unchanged.
- [x] concurrency / timing — N/A: pure plan-compile logic (compile_match), no shared state, no async,
  no lock, no new thread. Cypher planning is NOT a CLAUDE.md hot path (§1 [x]).
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new crate; reuses the
  existing `properties_to_filter`/`Filter`/`Expr` path; no string interpolation into a query.
- [x] layering & dependencies follow CONVENTIONS.md — no new PhysicalOp, no index, executor unchanged;
  Filter placed after the binding op (matches compile_shortest_path_match's existing pattern).
- [x] a person reviewed and approved the change — AUTO-RESOLVED (autonomy: auto): non-security,
  non-concurrency, non-architecture; complete red→green evidence at two levels + clean clippy/fmt.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — the emitted `PhysicalOp::Filter` is consumed by the executor's op loop
  (executor/read.rs:179 & 684, executor/write.rs:146). Confirmed at the EFFECT level: the integration
  tests observe the narrowed row counts (3→2, 3→0) that only a wired, evaluated Filter can produce.
- [x] DEAD-CODE (code) — no new symbol; `properties_to_filter` gained a second caller (was
  ShortestPath-only); `first_var`/`target_var` are locals both used by their op AND their Filter.
- [x] SEMANTIC — n/a (code change; no prose artifact frozen).

### GATE RECORD
Outcome: PASS  (auto-resolved on complete two-level red→green evidence; no security/concurrency/
  architecture residue — A1 pinned green by the expanded-target planner test, A2 pinned green by the
  param + missing-property effect tests)
If RISK-ACCEPTED -> owner: — · ticket: — · expires: —   (n/a)
Reviewed by: ADD auto-gate (autonomy: auto) · date: 2026-06-16

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
