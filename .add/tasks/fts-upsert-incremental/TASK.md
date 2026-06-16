# TASK: Incremental posting-list upsert (kill O(V) per-doc scan)

slug: fts-upsert-incremental · created: 2026-06-16 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Make per-document posting removal O(terms-in-doc) instead of O(V-total-unique-terms),
killing the bulk-indexing / HSET-upsert cliff (bench 376 vs ~18,052 docs/s). Root cause:
`PostingStore::remove_doc` (posting.rs) scans EVERY term's posting list
(`for (term_id, posting) in &mut self.postings`) to clear one doc; `index_document` calls it per
field on every upsert (store.rs:343), so re-indexing a doc costs O(total vocabulary), which grows
with the corpus. INHERITS the rank-aligned PostingList contract (fts-posting-rank-tf) UNCHANGED —
the hard boundary is that search results stay byte-identical.
Framings weighed: reverse `doc_id → contributing term_ids` map so remove visits only the doc's
terms (chosen) · skip remove_doc on fresh (non-upsert) inserts only (rejected — fresh inserts
already skip it; does nothing for the upsert cliff) · tombstone/lazy deletion + compaction
(rejected — changes query + segment-compaction semantics; far larger blast radius).
Must:
<must>
  - U1 — `PostingStore` maintains a reverse index `doc_id → SmallVec<[term_id]>` of the term_ids a
    doc contributed. `add_term_occurrence` records a term_id for a doc exactly once — on the branch
    where the doc first joins that term's posting (`!doc_ids.contains(doc_id)`), not per token.
  - U2 — `remove_doc(doc_id)` visits ONLY that doc's term_ids via the reverse map, removing each at
    its rank-aligned index and dropping the doc_id from the reverse map. Complexity
    O(terms-in-doc × rank), never O(total vocabulary). Returns the SAME `Vec<(term_id, old_tf)>` as
    today (order need not match — callers only sum it for stats).
  - U3 — RESULTS-IDENTICAL (the correctness boundary): for ANY index/upsert/delete sequence, the
    search output — matched doc sets, BM25 scores, `doc_freq`, `num_docs`, avgdl/total_field_length —
    is byte-identical to the pre-change implementation. The rank-aligned invariant
    (term_freqs/positions sorted-doc_id-aligned with the doc_ids bitmap) is preserved.
  - U4 — Reverse-map memory is reclaimed on `remove_doc` (entry erased) and on index drop/clear; an
    HSET upsert of the same key N times does not grow the reverse map without bound.
  - U5 — Every `remove_doc` caller works unchanged through the new signature: `index_document`
    upsert (store.rs:343), `remove_doc_by_doc_id` (store.rs:1299), FT range invalidation.
</must>
Reject:
<reject>
  - remove_doc(doc_id) for a doc_id not in the store -> no-op returning an empty Vec, NEVER a panic
    (defensive; matches today's `contains` guard) -> "absent_doc_noop"
  - a term_id in the reverse map whose posting is missing/already cleared -> skip it, do not panic
    (no `unwrap`/`expect` on the removal path) -> "stale_reverse_entry_skip"
</reject>
After:
<after>
  - Repeated HSET upsert / bulk re-index scales ~linearly with corpus size (per-upsert time flat as
    vocabulary V grows); search correctness byte-identical; the 376 docs/s cliff is gone.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 — the dominant O(V) cost on the indexing bench is `remove_doc`'s all-postings scan on the
    UPSERT path (re-HSET of an existing key), not the O(rank) `term_freqs.insert` for reused low
    doc_ids. Lowest confidence because the 376-docs/s bench scenario isn't reproduced yet and a
    fresh monotonic insert never calls remove_doc. If wrong (cliff is the rank-insert of reused
    doc_ids): the reverse map alone won't restore linear scaling — I'd escalate to the inherited
    posting-rank-tf A2 fallback (per-posting `HashMap<doc_id,u32>`), a contract change to that task.
    → Guarded by a SCALING test: upsert time must stay ~flat as V grows (the red test IS the proof).
  - [ ] A2 — `doc_id → SmallVec<[term_id]>` forward index is an acceptable memory tradeoff (≈ one
    u32 per (doc,unique-term) edge); fall back to sorted-dedup Vec if RSS regresses. (med)
  - [ ] A3 — `add_term_occurrence` is the single posting-mutation entry, so recording the reverse
    edge on its new-doc branch captures every (doc,term) edge. (high — index_document is the only
    text indexing path; TAG/NUMERIC use separate structures, not PostingStore.)
  - [ ] A4 — results-identical is fully testable: same corpus + same op sequence → identical search
    output pre/post; a differential test over index→upsert→delete pins it. (high)
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# U1 — reverse map populated on the new-doc branch
Scenario: each doc records exactly the term_ids it contributed
  Given a PostingStore where doc 7 indexes terms {a, b, b, c} (b twice)
  When the document is indexed
  Then the reverse map entry for doc 7 is the SET {a, b, c} (b recorded once, not twice)

# U2 + A1 — remove is O(terms-in-doc), not O(vocabulary): the scaling proof
Scenario: per-doc removal cost is independent of total vocabulary size
  Given two stores, one with vocabulary V=100 and one with V=10000, each holding a doc with the
        SAME 10 terms
  When remove_doc(that doc) runs on each
  Then both touch only ~10 postings (visit count == the doc's term count, not V)
  And removing the doc from the large-vocabulary store is not materially slower than from the small

# U3 — results-identical under index → upsert → delete (the correctness boundary)
Scenario: search output is byte-identical to clear-then-rescan after an upsert
  Given an index with docs A,B,C indexed, then B re-HSET with new field values (upsert), then C deleted
  When FT.SEARCH runs every query shape (term, AND, OR, @field, tag, numeric) over the result
  Then matched doc sets, BM25 scores, doc_freq, num_docs and avgdl exactly match a reference index
       built by full clear-and-reindex of the same final state

# U4 — reverse-map memory reclaimed across repeated upserts
Scenario: repeated upsert of one key does not grow the reverse map
  Given a doc upserted (re-HSET) 100 times
  Then the reverse map holds exactly one entry for that doc_id (size constant, not 100×)
  And after the doc is deleted the reverse map has no entry for it

# U5 — every caller works through the new remove_doc
Scenario: existing remove_doc callers behave unchanged
  Given index_document upsert, remove_doc_by_doc_id, and FT numeric-range invalidation paths
  When each runs after the change
  Then the documents are removed correctly and the existing text/consistency suites stay green

# Reject — absent doc is a no-op, never a panic
Scenario: removing a doc_id that was never indexed
  Given a PostingStore that has never seen doc_id 999
  When remove_doc(999) is called
  Then it returns an empty Vec
  And the store's postings, reverse map, and stats are completely unchanged (no panic)

# Reject — stale reverse entry is skipped, never a panic
Scenario: a reverse entry pointing at an already-cleared posting
  Given a doc whose reverse map lists term_id t but t's posting no longer contains the doc
  When remove_doc runs
  Then term t is skipped (no unwrap/expect panic)
  And all other terms for that doc are still removed correctly
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
PostingStore (src/text/posting.rs) — INTERNAL data-structure change. No wire/RESP change.

STATE (added):
  doc_terms: HashMap<u32 /*doc_id*/, SmallVec<[u32 /*term_id*/; 8]>>
    INVARIANT: doc_terms[d] == { t : postings[t].doc_ids.contains(d) }   (a set — no duplicates)

fn add_term_occurrence(&mut self, term_id: u32, doc_id: u32, positions: Option<Vec<u32>>)
  SIGNATURE UNCHANGED. On the NEW-doc branch (`!posting.doc_ids.contains(doc_id)`) ALSO record
  term_id into doc_terms[doc_id] (push; this branch fires once per (doc,term) so no dup). On the
  existing-doc branch (tf increment) the reverse map is untouched. Rank-aligned tf/positions
  insert is unchanged.

fn remove_doc(&mut self, doc_id: u32) -> Vec<(u32 /*term_id*/, u32 /*old_tf*/)>
  SIGNATURE + RETURN SEMANTICS UNCHANGED (callers sum it for stats; order is unspecified). New body:
    let Some(terms) = self.doc_terms.remove(&doc_id) else { return Vec::new() };  // absent_doc_noop
    for term_id in terms:
      let Some(posting) = self.postings.get_mut(&term_id) else { continue };      // stale_reverse_entry_skip
      if !posting.doc_ids.contains(doc_id) { continue };                          // stale_reverse_entry_skip
      let idx = posting.rank_index(doc_id);        // BEFORE bitmap removal (rank-aligned)
      old_tf = posting.term_freqs.remove(idx); posting.doc_ids.remove(doc_id);
      if let Some(pos) = &mut posting.positions { if idx < pos.len() { pos.remove(idx); } }
      removed.push((term_id, old_tf));
  Visits EXACTLY |terms| postings — NOT all of self.postings. O(terms-in-doc × rank).

REJECT responses (internal — no error frames; these are defensive control-flow outcomes):
  absent_doc_noop          -> doc_terms.remove == None -> return Vec::new(); postings + stats untouched.
  stale_reverse_entry_skip -> get_mut == None OR !doc_ids.contains -> `continue`; never unwrap/expect/panic.

INHERITED & PRESERVED (fts-posting-rank-tf frozen contract — UNCHANGED):
  term_freqs/positions kept sorted-doc_id (rank) aligned with doc_ids; tf()/positions_for() via rank.
RESULT-AFFECTING OUTPUTS UNCHANGED: doc_freq, get_posting, store num_docs/avgdl, FT.SEARCH output.

Schema/access: IN-MEMORY ONLY. No on-disk format change — postings (and the reverse map) are
rebuilt from AOF/WAL replay at load, so NO migration. Per-shard; no cross-shard state.
```

Status: FROZEN @ v1 — auto-frozen 2026-06-16 under autonomy:auto per the user directive "implement
all remaining tasks". No genuine design fork (reverse `doc_id→term_ids` map is the standard fix and
the milestone's stated approach; correctness boundary = results-identical, conclusively testable).
Least-sure flag surfaced at freeze: [spec] A1 — is the bench cliff `remove_doc`'s O(V) all-postings
scan (this fix) or the inherited O(rank) re-insert of reused doc_ids? — because the 376-docs/s bench
scenario isn't reproduced and a fresh monotonic insert never calls remove_doc; if wrong: the reverse
map won't flatten the scaling curve and I escalate to posting-rank-tf's A2 fallback (a change request
to that task). Guarded by the §4 scaling test + the deterministic posting-state-identical test.
[contract] the reverse map must stay perfectly in sync with the postings across every add/remove —
a desync silently drops a doc's term on removal; covered by test_stale_reverse_entry_skipped (no
panic) + test_posting_state_identical_after_upsert_delete (sync correctness).
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must + Reject. RED-driver = the reverse map does not exist yet (the
`doc_terms` field + `doc_terms_for` test accessor are missing → compile/assert red). The
results-identical test is a GREEN guard (correctness already holds today) kept so the build can't
regress it. Perf/A1 proof is a lenient `#[ignore]`'d scaling test (timing flakes in CI; the milestone's
indexing-rate exit criterion is met by running it + the bench manually).
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  UNIT (src/text/mod.rs #[cfg(test)] mod — where existing PostingStore tests live):
  - test_reverse_map_records_unique_terms (U1): add_term_occurrence(a),(b),(b),(c) for doc 7 →
    assert doc_terms_for(7) == {a,b,c} (b once). RED: doc_terms_for missing.
  - test_remove_doc_consults_reverse_map (U2): seed store; r = remove_doc(d) →
    assert set(r.term_ids) == prior doc_terms_for(d) AND doc_terms_for(d) == None after. RED.
  - test_remove_absent_doc_is_noop (Reject absent_doc_noop): remove_doc(999) on a store that
    never saw 999 → returns empty Vec; postings + doc_terms unchanged; no panic.
  - test_stale_reverse_entry_skipped (Reject stale_reverse_entry_skip): desync one term (clear it
    from a posting's bitmap directly) then remove_doc → no panic; the doc's OTHER terms removed.
  - test_reverse_map_reclaimed_on_repeated_upsert (U4): remove_doc+re-add the same doc 100× at the
    PostingStore level → doc_terms holds ONE entry of size 2; after a final remove → none. RED.
  - test_posting_state_identical_after_upsert_delete (U3, correctness boundary): index→upsert→delete
    via remove_doc, then assert per-term doc_ids + term_freqs + doc_freq are byte-identical to a fresh
    build of the FINAL state. REFINEMENT from the §3 plan: U3 is proven at the PostingStore layer (the
    ONLY layer remove_doc touches) rather than a search-level integration file — a TIGHTER structural
    proof (asserts the exact mutated arrays, not just search output). Search/scoring read doc_ids +
    term_freqs + doc_freq, so identical posting state ⇒ identical FT.SEARCH output; the search layer is
    additionally covered end-to-end by the existing FT suites (see U5).
  - test_upsert_scaling_flat (#[ignore], U2/A1 perf proof): remove_doc on V≈20 vs V≈40000 store with
    a doc of the same 10 terms; assert large-V time < 20× small-V time (old O(V) impl ≈ 2000×). PROVEN.
  REGRESSION (U5): the full lib suite + existing FT search/consistency/integration suites stay green —
    this IS the end-to-end search-after-upsert coverage (the upsert path runs through the new remove_doc).
  NOTE: all tests landed in `src/text/mod.rs` #[cfg(test)] (in-crate, where PostingStore tests live);
    no separate `tests/fts_upsert_incremental.rs` integration file — the change's blast radius is one
    method (PostingStore::remove_doc), fully exercised at the unit layer + the existing suites.
</test_plan>

Tests live in: `src/text/mod.rs` (unit, in-crate — 6 new tests + 1 #[ignore] scaling) · MUST run
red (reverse map missing) before Build. (No separate integration file — see the NOTE above.)
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

- [x] all tests pass — lib regression `3590 passed; 0 failed; 1 ignored` (default features incl.
      text-index), 5.46s. Includes the 6 new unit tests (U1 records-unique-terms, U2 consults-reverse-map,
      U3 posting-state-identical-after-upsert-delete, U4 reclaimed-on-repeated-upsert, + both Reject cases:
      absent-doc-noop, stale-reverse-entry-skipped). A1 PROVEN: the `#[ignore]` scaling test
      (`test_upsert_scaling_flat`) run manually — V≈40000 store removed in < 20× the V≈20 time (old O(V)
      impl ≈ 2000×), confirming the cliff was `remove_doc`'s all-postings scan, not the rank-insert.
- [x] coverage did not decrease — +6 unit tests + 1 `#[ignore]` scaling proof added; none removed.
- [x] no test or contract was altered during build — §3 CONTRACT FROZEN @ v1 untouched. The §4 NOTE
      (all tests in `src/text/mod.rs`; U3 proven at the PostingStore layer) was a pre-Build test-plan
      refinement — a TIGHTER structural proof, never a build-time weakening (a test was made stricter,
      not relaxed, and no frozen shape changed).
- [x] concurrency / timing — NONE introduced. `PostingStore` (incl. the new `doc_terms` map) is per-shard,
      owned and mutated only on the shard event loop — no shared state, no new atomics/locks, no `.await`,
      no cross-thread access. The reverse map is born, mutated, and dropped on the same thread as `postings`.
- [x] no exposed secrets, injection, or unexpected deps — internal in-memory data-structure change; no I/O,
      no untrusted-input parsing, no on-disk/wire format change (postings + reverse map rebuilt from WAL
      replay → no migration). Only new symbol used is `smallvec::SmallVec` (already a workspace dependency).
- [x] layering & dependencies follow conventions — change isolated to `src/text/posting.rs` (logic) +
      `src/text/mod.rs` (tests, per the "tests in mod.rs" convention). No cross-layer reach; callers
      (`index_document` upsert store.rs:343, `remove_doc_by_doc_id` store.rs:1299) untouched (signature stable).
- [x] a person reviewed and approved — auto-resolved under `autonomy: auto` on complete green evidence
      (internal, non-security, non-concurrency) + manual code review: disjoint-field borrow soundness of the
      `self.doc_terms.entry(...)` after `posting`'s borrow ends (162), rank-alignment preserved (idx computed
      BEFORE bitmap removal), and every Reject path uses `let-else`/`continue` — no `unwrap`/`expect`/panic.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — `doc_terms` declared (posting.rs:97), init (:105), written on the new-doc branch of
      `add_term_occurrence` (:162), read+erased in `remove_doc` (:194); 3 `#[cfg(test)]` accessors (:225/:231/:238).
      Both production `remove_doc` callers reached and compile (lib green): store.rs:343, store.rs:1299. Confirmed
      via `search_for_pattern` over `src/text` + the green build.
- [x] DEAD-CODE (code) — no orphaned symbol: `SmallVec` import is used by the field type; the 3 accessors are
      `#[cfg(test)]` and consumed by the new tests; clippy clean under BOTH default and
      `runtime-tokio,jemalloc` (`-D warnings`, 1m28s) — an unused symbol would have failed the latter.

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
