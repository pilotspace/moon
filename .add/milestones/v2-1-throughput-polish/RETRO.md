════════════════════════════════════════════════════════════════════════
 v2-1-throughput-polish · Throughput Polish (recover v2-deferred costs)
════════════════════════════════════════════════════════════════════════
 VERDICT   DONE
 TASKS     1/1 done           CRITERIA  4/4 met
 GATES     1 PASS             WAIVERS   none

 goal  On monoio, FT.SEARCH's off-event-loop yield reaps the io_uring CQ
       without timer-wheel latency, so the brute-force chunk shrinks
       back to its latency-optimal size and reclaims the ~22% throughput
       #179 deferred, with #179's co-located latency relief preserved.

 TASK                        PHASE     GATE TESTS PROGRESS
 ───────────────────────────────────────────────────────────────────────
 ft-yield-costfree-monoio    done      PASS 0     ●●●●●●●●
 legend  ● reached  ◉ current  ○ pending   spec→…→done

 EXIT CRITERIA  ●●●●●●●●●● 4/4 met

 LEARNINGS (3 carried)
   • TDD · folded · (foundation v4 → CONVENTIONS.md) A behavioral red
     test (`monoio_yield_overhead_is_microscopic`: 200 yields must
     finish <100ms) caught the cost-free property deterministically
     WITHOUT internal counters — measuring wall-time at the yield
     granularity beats exposing an introspection hook (evidence: red
     360ms → green <1ms, no new public surface).
   • SDD · folded · (foundation v4 → PROJECT.md §Spec)
     Spike-before-freeze (a ~40-line standalone monoio program) refuted
     the make-or-break ⚠#1 AND corrected the contract
     (`Pipe`→`UnixStream::pair()`: `Pipe` has no `AsyncReadRent`) BEFORE
     locking the mechanism — cheap de-risking that the frozen-contract
     model should reach for whenever the riskiest assumption is a
     library-internals question (evidence: the literal "NOP io_uring op"
     the request named was unreachable; the spike found the reachable
     equivalent).
   • ADD · folded · (foundation v4 → CONVENTIONS.md) Mechanism
     arithmetic UNDER-predicted the tuning parameter; the end-to-end A/B
     was worth running. The 0.317µs per-yield constant predicted <2%
     overhead at K=256, but the real FT.SEARCH A/B measured +4.98% there
     (and +2.74% at the shipped K=512) — a ~3pt gap from per-chunk loop
     bookkeeping the mechanism cost ignored. Lesson: a measured
     dominant-cost constant is necessary but NOT sufficient to freeze a
     tuning knee; pair it with a RELATIVE same-binary A/B (which cancels
     the absolute-RPS noise that made us defer in the first place)
     before committing the default. The relative A/B sidesteps the
     OrbStack absolute-noise problem entirely — control and treatment
     share the VM, so only the ratio matters.

 DECIDE NEXT  consolidate learnings + archive-milestone
              v2-1-throughput-polish
════════════════════════════════════════════════════════════════════════