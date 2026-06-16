════════════════════════════════════════════════════════════════════════
 v3-1-fts-hardening · FTS Hardening
════════════════════════════════════════════════════════════════════════
 VERDICT   DONE
 TASKS     6/6 done           CRITERIA  5/5 met
 GATES     6 PASS             WAIVERS   none

 goal  Moon's full-text search is trustworthy and competitive: every
       query combinator (term, AND, OR, TEXT+TAG, NUMERIC) returns
       correct results with true total-matched counts, and indexing plus
       high-DF queries run without the O(V)/O(M^2) cliffs the 2026-06-16
       benchmark exposed.

 TASK                        PHASE     GATE TESTS PROGRESS
 ───────────────────────────────────────────────────────────────────────
 fts-posting-rank-tf         done      PASS 0     ●●●●●●●●
 fts-query-combinators       done      PASS 0     ●●●●●●●●
 fts-query-eval-dispatch     done      PASS 0     ●●●●●●●●
 fts-upsert-incremental      done      PASS 0     ●●●●●●●●
 fts-search-count-semantics  done      PASS 0     ●●●●●●●●
 fts-query-routing-robustne… done      PASS 0     ●●●●●●●●
 legend  ● reached  ◉ current  ○ pending   spec→…→done

 EXIT CRITERIA  ●●●●●●●●●● 5/5 met

 LEARNINGS (8 carried)
   • SDD · folded · A milestone scoped a task as "perf-only" but
     grounded code investigation in Specify found a latent correctness
     bug on the same path (insertion-order term_freqs vs sorted-bitmap
     read) — perf and correctness were inseparable. Lesson: re-derive
     task scope from the code during Specify, don't inherit the
     milestone's framing verbatim. (evidence: A1 flag; the misalignment
     was real and is fixed + tested.)
   • TDD · folded · The bug survived the code's entire prior life
     because every existing test used EQUAL term frequencies, which mask
     alignment errors. Lesson: index/posting fixtures must use DISTINCT,
     asymmetric values so a positional misalignment changes an
     observable output. (evidence: test_tf_correct_after_low_id_update
     needs a=5,b=2,c=3 to catch it; equal tfs pass under the buggy
     code.)
   • ADD · folded · risk:high + autonomy:conservative correctly forced a
     human gate on a change that looked mechanically green — the
     surfaced flag (positions_for is test-only-referenced forward API)
     was a real judgement call the auto-gate should not have silently
     swallowed. (evidence: unguarded_high_risk_auto guard held; gate
     presented + human-approved.)
   • TDD · folded · Test SELECTION must be validated to actually
     execute, not just compile: 4 of my chosen FT integration tests ran
     0 cases (`#![cfg(feature="runtime-tokio")]`-gated under monoio
     default; one `#[ignore]`'d) and a green `cargo test --release` hid
     it. Evidence: first verify pass reported "0 passed; 0 failed" yet I
     nearly read it as PASS. Lesson: assert a nonzero ran-count per
     suite, or the suite is silent coverage. Mitigation already applied:
     ran them under the correct feature set.
   • TDD · folded · A pre-existing `#[ignore]`'d manual test
     (`inverted_search_numeric_shard_consistency`) is stale-broken at
     its seed (`let _: i64` vs HMSET `+OK`), so it cannot guard the
     scatter path it was written for. Evidence: panics at line 73 before
     any assertion. Follow-up task: fix annotation + per-test index
     isolation + un-ignore (a real cross-shard numeric guard is valuable
     post-2b).
   • SDD · folded · A flat dispatch payload `(field_idx,
     Vec<QueryTerm>)` silently constrained the wire to AND-only — the OR
     bug was unfixable at the leaf without a payload redesign. Evidence:
     OR returned AND on multi-shard until `TextSearchPayload` carried
     raw `Bytes`. Lesson: when a cross-shard payload can't REPRESENT the
     contract's algebra, that's a contract-blocking design smell to
     surface at freeze.
   • ADD · folded · Building 2b on 2a's frozen contract before gating
     either ("gate together") worked: it let the inherited eval_set
     algebra be validated by real end-to-end result sets rather than
     parser unit tests alone. Evidence: 12/12 e2e + x-shard identity.
     Keep "split parser/dispatch, gate the pair" as a pattern for
     contract-then-wire features.
   • ADD · folded · Deferred-cleanup honesty:
     `scatter_text_search_filter` left dead-but-flagged (FLAG-2) rather
     than removed-with-scope-creep into the InvertedSearch protocol.
     Folds at milestone close.

 DECIDE NEXT  consolidate learnings + archive-milestone
              v3-1-fts-hardening
════════════════════════════════════════════════════════════════════════