════════════════════════════════════════════════════════════════════════
 v2-2-xshard-read-validation · V2 2 Xshard Read Validation
════════════════════════════════════════════════════════════════════════
 VERDICT   DONE
 TASKS     1/1 done           CRITERIA  4/4 met
 GATES     1 PASS             WAIVERS   none

 goal  the shipped cross-shard read fast-path is validated on a
       trustworthy absolute instrument (GCloud bare-metal), with a
       data-backed decision on whether coalescing / RCU acceleration is
       warranted

 TASK                        PHASE     GATE TESTS PROGRESS
 ───────────────────────────────────────────────────────────────────────
 xshard-read-gcloud-validat… done      PASS 0     ●●●●●●●●●
 legend  ● reached  ◉ current  ○ pending   spec→…→done

 EXIT CRITERIA  ●●●●●●●●●● 4/4 met

 LEARNINGS (4 carried)
   • SDD · open · a RELATIVE-only win (OrbStack +18→+22.9%) needs an
     ABSOLUTE bare-metal cross-check before a line is closed — the
     absolute residual (~10µs) is the number that actually decides
     coalesce/RCU, invisible to a same-run ratio (evidence: this whole
     task; the residual was unknowable from the relative table).
   • ADD · open · the engine `_scope_walk` md5's `target/` and hangs on
     large/slow trees — workaround was to stash all `target*` dirs to a
     sibling outside the walk root; `_SCOPE_EXCLUDE_DIRS` should exclude
     Rust target dirs (evidence: tests→build advance hung on 8GB+ target
     over virtiofs; regrew to 767M mid-task).
   • ADD · open · the §5 scope token-resolver is POSITION-sensitive in a
     surprising way — a project-ROOT bare file (`CHANGELOG.md`) only
     resolves to root when FIRST in the list; trailing after a `tmp/…`
     token it became `tmp/CHANGELOG.md`, mis-scoping the real
     `/CHANGELOG.md` edit (evidence: state.json scope.declared snapshot
     held `tmp/CHANGELOG.md`; workaround = declare root bare files
     first).
   • TDD · open · measure-only tasks split test surface cleanly: a
     deterministic green-pin (gate consts byte-identical) + a
     fail-closed harness self-test guard the MECHANISM, while the perf
     Musts (M2/M3) are EVIDENCE-gated in §6 — the
     behavior-preserving-perf split reused from xshard-read-fastpath
     worked again (evidence: §4 RED→GREEN both deterministic; M2/M3
     recorded as cells not asserts).

 DECIDE NEXT  consolidate learnings + archive-milestone
              v2-2-xshard-read-validation
════════════════════════════════════════════════════════════════════════