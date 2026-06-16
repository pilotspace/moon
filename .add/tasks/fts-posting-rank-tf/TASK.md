# TASK: Rank-based posting TF lookup (kill the high-DF O(M^2) cliff)

slug: fts-posting-rank-tf · created: 2026-06-16 · stage: production · risk: high · autonomy: conservative
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- risk: high — freeze-first shared contract (the PostingList TF representation that
     fts-upsert-incremental inherits) AND it fixes a latent BM25-correctness bug, so verify
     must NOT auto-pass: human gate required (run.md unguarded_high_risk_auto guard). -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Rank-aligned posting-list TF lookup — correct AND sub-linear term-frequency reads for BM25.
Framings weighed: rank-aligned parallel arrays (term_freqs/positions kept in sorted-doc_id order; lookup via `RoaringBitmap::rank` → O(log N)) (chosen) · per-posting `HashMap<doc_id,u32>` tf (O(1), alignment-free, +mem) · sorted `Vec<(doc_id,tf)>` + binary_search (replaces the parallel arrays)
Must:
<must>
  - M1 — TF lookup for `(term_id, doc_id)` returns the document's TRUE term frequency, including
    AFTER a document is updated / re-indexed (HSET on an existing key). [fixes the latent
    insertion-order-vs-sorted-bitmap misalignment — see A1]
  - M2 — TF lookup is sub-linear in posting-list length: a high-DF term query over M candidate docs
    costs O(M·log N), not O(M·N) (no per-candidate `.position()` linear scan). [kills the O(M²) cliff]
  - M3 — both BM25 read sites use the lookup: `search_field` (store.rs:504) and the expanded-term
    `search_field_or` path (store.rs:770).
  - M4 — no regression on the already-correct path: for a corpus inserted in ascending doc_id order
    with no updates, BM25 scores AND result ordering are byte-identical to today.
  - M5 — `term_freqs` and the parallel `positions` (when `Some`) stay consistent with `doc_ids`
    under add / upsert / remove — the alignment invariant is explicit and maintained in ONE place
    (the `add_term_occurrence` / `remove_doc` data structure), not re-derived per read site.
</must>
Reject:
<reject>
  - doc_id absent from the term's posting list -> tf = 0 (defined default; BM25 treats the term as
    not occurring in that doc — current `.unwrap_or(0.0)`).                  -> "tf_absent" (= 0, not an error)
  - (No client-facing error codes: this is an internal index path; malformed query/input is rejected
    upstream. The sole "rejection" is the absent-doc default above.)
</reject>
After:
<after>
  - A high-DF term query (term in ~5% of 100K docs) no longer exhibits the O(M²) blow-up — latency
    drops from the measured ~419 ms toward RediSearch-class.
  - BM25 scores are correct even after document updates (the re-index misalignment bug is gone).
  - The PostingList TF representation is the FROZEN contract `fts-upsert-incremental` builds on.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 — This task is NOT perf-only. The same path carries a latent CORRECTNESS bug: `index_document`
    re-indexes an updated doc with its SAME doc_id (store.rs:343,355) via `remove_doc` + re-add, and
    the re-add does `doc_ids.insert()` (sorted position in the bitmap) but `term_freqs.push()` (END of
    the vec, posting.rs:96-97). So re-indexing a doc whose id is NOT the max in a shared posting list
    misaligns `term_freqs` against `doc_ids.iter()` (sorted) → wrong TF → corrupted BM25 for that whole
    term. Lowest confidence because the milestone scoped this as a perf fix, but a rank-based lookup is
    only correct if `term_freqs` is sorted-aligned — so the fix NECESSARILY corrects the bug. If wrong
    (ship "perf-only", keep push-to-end): `rank()` would index the wrong TF and corrupt scores worse
    than today, silently. → I assume scope INCLUDES the correctness fix.
  - [ ] A2 — Representation = rank-aligned parallel arrays (keep `doc_ids` RoaringBitmap + `term_freqs`
    /`positions` sorted-aligned; lookup via `rank`) over a per-posting `HashMap<doc_id,u32>`. It's the
    freeze-first contract `fts-upsert-incremental` inherits: arrays hold current memory + O(log N)
    lookup but make upsert insert O(N) (shift at rank); a HashMap is O(1) lookup+insert but adds a map
    per term. If wrong: the upsert task re-opens this contract.
  - [ ] A3 — `doc_ids` MUST remain a RoaringBitmap (query-eval AND/OR candidate set-ops depend on it);
    only the TF side changes. (high confidence)
  - [ ] A4 — `positions[i]` moves in lockstep with `term_freqs[i]` under the new alignment so phrase /
    HIGHLIGHT data stays correct. (high confidence)
  - [ ] A5 — for the already-correct ascending-insert path, `rank(doc_id) == old position`, so results
    are unchanged there. (high confidence)
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# ── M1 / A1 — the correctness fix (headline) ─────────────────────────────
Scenario: TF stays correct after a low-id document is updated
  Given term "alpha" is indexed into doc 3 (tf=1), then doc 5 (tf=1), then doc 8 (tf=1)
  When doc 3 is updated (re-HSET) so "alpha" now occurs 4 times in doc 3
  Then FT.SEARCH "alpha" scores doc 3 with TF=4 and docs 5 and 8 with TF=1 each
  And the term_freqs of docs 5 and 8 are NOT scrambled by doc 3's re-insertion   # no cross-doc corruption

# ── M2 — sub-linear lookup, no O(M^2) cliff ──────────────────────────────
Scenario: High-DF term query does not scan the whole posting per candidate
  Given a 100K-doc index where term "common" occurs in ~5000 docs
  When FT.SEARCH "common" runs under concurrency
  Then per-query latency is sub-linear in posting length — far under the ~419 ms O(M*N) baseline
  And the BM25 results are identical to the linear-scan reference   # speed gained, answers unchanged

# ── M3 — both read sites use the lookup ──────────────────────────────────
Scenario: Expanded-term (OR / fuzzy) path uses the same correct TF lookup
  Given the updated-doc-3 setup above, queried via the expanded-term path (search_field_or)
  When that query runs
  Then doc 3's TF is read as 4 — identical to the direct search_field path
  And no read site keeps an inline .iter().position() TF scan

# ── M4 — no regression on the already-correct path ───────────────────────
Scenario: Ascending-insert corpus produces identical BM25 to before
  Given a corpus inserted purely in ascending doc_id order with no document updates
  When FT.SEARCH runs for any term
  Then BM25 scores and result ordering are byte-identical to the pre-change implementation

# ── M5 — positions stay aligned with TF ──────────────────────────────────
Scenario: Position data stays aligned with TF after an update
  Given a position-tracked index where low-id doc 3 is updated (as above)
  When the positions for doc 3 are read
  Then they belong to doc 3 (consistent with its tf=4), not a neighbouring doc
  And positions[i] still corresponds to the i-th doc_id in sorted order

# ── Reject — tf_absent (defined default, not an error) ───────────────────
Scenario: A term absent from a document yields TF 0 without panic
  Given term "rare" does NOT occur in doc 7 (doc 7 not in "rare"'s posting)
  When BM25 scores doc 7 for "rare"
  Then the TF contribution is 0   # unwrap_or(0.0) default
  And the lookup does not panic and does not return a neighbouring doc's tf
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

Internal data-structure contract (not a wire endpoint). The frozen shape is the PostingList TF
representation + its lookup API in `src/text/posting.rs`, consumed by the BM25 read sites in
`src/text/store.rs`. Wire protocol (FT.SEARCH request/reply) is UNCHANGED.

```
INVARIANT (frozen) — rank-alignment of PostingList:
  term_freqs[i] and (when positions = Some) positions[i] correspond to the i-th doc_id of
  `doc_ids` in ASCENDING (sorted / rank) order. For a present doc_id d:
      idx(d) = doc_ids.rank(d) as usize - 1        // rank(d) = count of ids <= d  (roaring 0.11)
  term_freqs is RANK-aligned, NOT insertion-aligned. Maintained in exactly one place
  (PostingStore::add_term_occurrence / remove_doc) — never re-derived at a read site.

READ API (impl PostingList, src/text/posting.rs):
  fn tf(&self, doc_id: u32) -> u32
    doc_ids.contains(doc_id) -> term_freqs[idx(doc_id)]      // sub-linear (rank), no O(N) scan
    else                     -> 0                             // "tf_absent" default — never panics
  fn positions_for(&self, doc_id: u32) -> Option<&[u32]>
    Some(&positions[idx]) when tracked AND present, else None

WRITES preserve the invariant (impl PostingStore):
  add_term_occurrence(term_id, doc_id, positions):
    existing doc -> i = idx(doc_id); term_freqs[i] += 1; positions[i].extend(..)
    NEW doc      -> doc_ids.insert(doc_id); i = idx(doc_id);
                    term_freqs.insert(i, 1); positions.insert(i, ..)   // INSERT-at-rank, NOT push
  remove_doc(doc_id) -> i = idx(doc_id); term_freqs.remove(i); positions.remove(i); doc_ids.remove

READ SITES updated (src/text/store.rs):
  search_field (~:504) and search_field_or (~:770): the inline
    doc_ids.iter().position(|id| id==doc_id).map(|i| term_freqs[i]).unwrap_or(0.0)
  becomes posting.tf(doc_id) as f32. (contains() short-circuit at ~:765 may stay — O(1).)

OUT OF CONTRACT:
  - doc_ids stays a RoaringBitmap; query-eval AND/OR candidate set-ops unchanged.
  - No on-disk / persistence format change — PostingList is the in-memory mutable-segment struct.
  - BM25 formula, k1/b, analyzer pipeline: untouched.
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-06-16). Lowest-confidence flags surfaced + accepted at freeze.
Least-sure flag surfaced at freeze: [spec] fixing the misalignment CHANGES BM25 output for previously-corrupted post-update cases — that is the bug being corrected, NOT a regression, so M4's no-regression golden must come from an ascending-insert corpus (the path already correct); if wrong, a snapshot of the OLD buggy scores would mis-fail. [contract] rank-aligned arrays make `add_term_occurrence` O(rank) on insert (shift `term_freqs`/`positions`) — the write cost `fts-upsert-incremental` inherits; if high-churn indexing regresses, that task re-tunes the structure it inherits here.
<!-- bundle lowest-confidence flag (surfaced at freeze):
  ⚠ [spec] Fixing the misalignment CHANGES BM25 output for previously-corrupted (post-update) cases —
     that is the bug being corrected, not a regression. Any golden/snapshot asserting the OLD buggy
     score must be updated; M4's reference must come from an ASCENDING-INSERT corpus (the correct path).
  ⚠ [contract] rank-aligned arrays make add_term_occurrence O(rank) on insert (shift term_freqs) —
     the perf cost fts-upsert-incremental must address; if high-churn indexing regresses, that task
     re-tunes the structure (it inherits this contract). -->

<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: all 6 scenarios; new `posting.rs` `tf`/`positions_for`/insert-at-rank lines covered.
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - test_tf_correct_after_low_id_update (M1/A1): add_term_occurrence for "alpha" in order doc3,doc5,doc8;
    then remove_doc(3) + re-add doc3 ×4; assert tf(3)==4 AND tf(5)==1 AND tf(8)==1.
    [RED today: push-to-end makes tf(3) read a neighbour → assertion fails before the fix]
  - test_high_df_query_sublinear (M2): ~100K docs, term in ~5%; assert query latency far under the
    linear baseline (best-of-K guard, like perf_v0112) AND results identical to a linear-scan reference.
  - test_expanded_path_same_tf (M3): the doc3-update setup queried via search_field_or; assert its TF==4
    and equals the search_field path; assert no inline `.position()` TF scan remains at either read site.
  - test_ascending_insert_unchanged (M4): ascending corpus, no updates; assert BM25 scores + ordering
    equal a captured pre-change reference (golden from the CORRECT ascending path).
  - test_positions_aligned_after_update (M5): position-tracked; after doc3 update assert positions_for(3)
    belongs to doc3 (aligned with tf=4).
  - test_tf_absent_is_zero (reject "tf_absent"): tf(absent_doc)==0, no panic, no neighbour's tf returned.
</test_plan>

Tests live in: `tests/fts_posting_rank_tf.rs` · MUST run red (missing `tf()` / push-to-end bug) before Build.
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

- [x] all tests pass — 7/7 new (`tests/fts_posting_rank_tf.rs`); full lib suite 3584/0;
      `text::` 144/0; `text::store::tests` 15/0. Tokio+text-index parity `cargo check
      --no-default-features --features runtime-tokio,jemalloc,text-index,graph` → clean
      (change has zero runtime-specific code). `cargo fmt --check` clean; `cargo clippy
      -- -D warnings` clean (no posting.rs/store.rs warnings).
- [x] coverage did not decrease — net +7 tests; they cover the previously-UNTESTED
      update-misalignment path (distinct tfs after a low-id re-index), which no prior
      test exercised. M4 keeps the already-correct ascending path green.
- [x] no test or contract was altered during build — §3 CONTRACT FROZEN @ v1 untouched;
      red suite (tests phase) went green via `src/text/{posting,store}.rs` edits only.
- [x] concurrency / timing of the risky operation is safe — BM25 search
      (`search_field`/`search_field_or`, store.rs:506,767 reading `posting.tf`) runs
      SYNCHRONOUSLY on the owning shard's event loop under an exclusive `&mut TextStore`
      borrow: the off-event-loop `FtSearchPlan::Yield` path (PR #179) is dense-KNN-only on
      an owned `SearchSnapshot` and routes every TEXT shape to `Sync` (ft_search/dispatch.rs),
      so it never reads postings. Writes (`add_term_occurrence` via HSET auto-index) run on
      the SAME single thread under the SAME `&mut`. No concurrent reader/writer of
      `PostingStore`; the change preserves the prior mutation discipline (read indexes the
      same Vec, write mutates the same Vec — neither adds shared mutability). No residue.
- [x] no exposed secrets, injection openings, or unexpected dependencies — pure in-memory
      index re-indexing; no I/O, no new crate, no on-disk format change (postings rebuilt
      from HSET replay on reload). `tf()`/`positions_for()` are panic-free (`contains` guard
      + `.get().unwrap_or(0)`), so malformed/absent doc_ids degrade to tf 0, never a crash.
      No security finding.
- [x] layering & dependencies follow CONVENTIONS.md — change confined to `src/text/`:
      `posting.rs` owns the structure, `store.rs` the read sites. New methods hang off the
      existing public `PostingList`; `rank_index` is private. No new module, no cross-layer
      dependency, no hot-path allocation introduced (rank/insert on existing Vecs).
- [x] a person reviewed and approved the change   <!-- Tin Dang · 2026-06-16 · risk:high human gate · PASS -->


### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — `tf()` referenced at store.rs:506 and store.rs:767 (the ONLY two BM25
      read sites; confirmed by serena pattern sweep of `src/text` — store.rs:891 `.position()`
      is an unrelated TAG-separator byte scan, not a posting read). `rank_index()` (private) is
      called by `tf`, `positions_for`, `add_term_occurrence` (×2 branches) and `remove_doc`.
- [x] DEAD-CODE (code) — the old linear-scan blocks were REMOVED (not left behind); the stale
      "RESEARCH Pitfall 1" note was rewritten, not orphaned. clippy reports no dead code.
      ⚠ ONE flag: `positions_for()` is currently exercised only by its M5 conformance test —
      there is NO production reader of `positions` yet (positions are stored "from day one for
      future phrase queries"; verified the only `.positions` accesses are the write path +
      `estimated_bytes`). It is the frozen-contract forward accessor (the rank-aligned
      counterpart to `tf()`) that `fts-upsert-incremental` inherits and phrase/HIGHLIGHT will
      consume — intentional contract API, not accidental dead code. Surfaced, not buried.
- [ ] SEMANTIC (prose / non-code) — n/a (code change).

### GATE RECORD
Outcome: PASS  (human gate — risk: high · autonomy: conservative)
Lowest-confidence item surfaced + accepted at the gate: `positions_for()` is contract API
referenced only by its conformance test today (no production reader of positions until phrase
queries / fts-upsert-incremental land) — accepted as intentional frozen-contract forward API.
No security or concurrency residue; no test/contract weakened.
Reviewed by: Tin Dang · date: 2026-06-16

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): BM25 score correctness after document re-index (M1
scenario as a regression monitor) · high-DF term-query p99 latency vs the ~419 ms O(M²)
baseline (M2 scenario) · FT.SEARCH ranking stability under update-heavy workloads.
Spec delta for the next loop: a milestone-scoped "perf fix" hid a latent correctness bug —
the rank-based lookup is only sound if `term_freqs` is sorted-aligned, so perf and correctness
were inseparable. fts-upsert-incremental now inherits the FROZEN rank-aligned contract: its
known cost is O(rank) insert (shift `term_freqs`/`positions`); if high-churn indexing regresses,
that task re-tunes the representation (the A2 HashMap fallback) rather than re-opening this one.

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
- [SDD · open] A milestone scoped a task as "perf-only" but grounded code investigation in
  Specify found a latent correctness bug on the same path (insertion-order term_freqs vs
  sorted-bitmap read) — perf and correctness were inseparable. Lesson: re-derive task scope
  from the code during Specify, don't inherit the milestone's framing verbatim. (evidence:
  A1 flag; the misalignment was real and is fixed + tested.)
- [TDD · open] The bug survived the code's entire prior life because every existing test used
  EQUAL term frequencies, which mask alignment errors. Lesson: index/posting fixtures must use
  DISTINCT, asymmetric values so a positional misalignment changes an observable output.
  (evidence: test_tf_correct_after_low_id_update needs a=5,b=2,c=3 to catch it; equal tfs pass
  under the buggy code.)
- [ADD · open] risk:high + autonomy:conservative correctly forced a human gate on a change that
  looked mechanically green — the surfaced flag (positions_for is test-only-referenced forward
  API) was a real judgement call the auto-gate should not have silently swallowed. (evidence:
  unguarded_high_risk_auto guard held; gate presented + human-approved.)
