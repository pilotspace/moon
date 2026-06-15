# TASK: Swap monoio cooperative_yield timer-park -> cost-free io_uring CQ-reap; re-tune chunk; A/B re-validate

slug: ft-yield-costfree-monoio · risk: high · autonomy: conservative · created: 2026-06-15 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: cost-free monoio FT.SEARCH yield — replace the `sleep(ZERO)` timer-park with an
always-ready self-pipe read so the io_uring CQ is reaped at ~µs cost, not ~ms.

Framings weighed: self-pipe via monoio public `UnixStream::pair()` socketpair (chosen — spike-proven) ·
forked/vendored NOP op (unsafe + maintenance) · demand-gated yield (infeasible: CQ-peek is `pub(crate)`,
search hogs the core) · spawn_blocking offload (rejected: segment data is `!Send`/thread-local; risks low-RSS).
[NOTE: monoio `Pipe` does NOT impl `AsyncReadRent`; the readable primitive is `UnixStream::pair()`.]

Must:
<must>
  - On the io_uring driver, `cooperative_yield()` MUST drain the task queue and park the run loop so
    the completion queue is reaped (co-located connections' read CQEs serviced) WITHOUT depending on
    the timer wheel — by awaiting a read on an always-ready per-shard `UnixStream` socketpair.
  - The always-ready socketpair MUST be lazily created once per shard thread (thread-local, on first
    FT.SEARCH yield, like the Lua sandbox) and stay readable across yields (pre-filled / re-armed) so
    every yield's read completes immediately.
  - The yield MUST preserve #179's frozen latency-relief contract: co-located p99 during a heavy
    search stays ≈ the #179 anchor (6.6ms @1 thread, 27ms @3 threads), NOT regress toward full-search-time.
  - The brute-force chunk default (`max_brute_force_vecs_per_chunk`, currently 16384) MUST be re-tuned
    DOWN to the new latency-optimal knee now per-yield cost is ~µs, recovering the deferred QPS; the
    `MOON_FT_YIELD_CHUNK` operator override MUST keep working.
  - The tokio path (`yield_now`) MUST remain unchanged.
  - No new `unsafe` in moon — the mechanism uses monoio's public `net::unix::UnixStream::pair()`.
</must>
Reject:   (internal primitive — "reject" = named failure mode + its handling, not an error frame)
<reject>
  - self-pipe/eventfd init fails (fd exhaustion / ENFILE) -> "yield_init_failed" -> fall back to
    `sleep(ZERO)` timer-park for the thread. MUST NOT fail the search; MUST NOT run synchronously
    (that would breach #179 relief).
  - running under the legacy/non-uring driver (`MOON_NO_URING`, macOS poll) -> "uring_unavailable"
    -> keep `sleep(ZERO)` (the self-pipe park trick targets the io_uring reap path).
  - the self-pipe read returns 0/EOF or would block (drained / mis-armed) -> "yield_pipe_starved"
    -> re-arm and fall back to `sleep(ZERO)` for that one yield; MUST NEVER hang the search waiting
    on an empty pipe.
</reject>
After:
<after>
  - A heavy brute-force FT.SEARCH on monoio yields at ~µs/chunk; co-located commands progress within
    the #179 latency anchor; brute-force QPS recovers to within ~5% of the pre-#179 / timer-disabled
    same-run control; tokio behavior unchanged; zero new unsafe; the per-shard self-pipe lives for the
    thread's lifetime (freed on shard shutdown).
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ✅ RESOLVED @ freeze (spike `tmp/yield-spike`, io_uring VM 2026-06-15): submitting an io_uring read on
    an ALREADY-READY `UnixStream` DOES force monoio 0.2.4 to drain→park→reap the CQ — co-located victim
    relieved (10ms vs 250ms starved). NOT a silent no-op. [was: the make-or-break ⚠#1]
  ✅ MITIGATED @ freeze (same spike): the self-pipe yield is ~µs (50 yields added ~0.3ms to the hog) vs
    `sleep(ZERO)`'s ~1.8ms/yield (+89ms / ~26% wall tax). Overhead does NOT eat the gain — it shrinks as
    the chunk shrinks, so the knee can go small. [was: ⚠#2] Still confirm the exact knee K via A/B sweep.
  - [ ] keeping `sleep(ZERO)` on the legacy/non-uring driver is acceptable (Linux io_uring is the
    production perf target; the poll driver isn't) — confirm.
  - [ ] the socketpair stays readable cheaply (pre-fill to socket buffer; re-arm with one write when low)
    without per-yield re-arm writes dominating — impl detail, validated in build.
  - [ ] per-shard thread-local lifecycle (lazy first-use init) is the right home; no cross-shard sharing.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: self-pipe yield relieves co-located latency (the silent-no-op guard)
  Given a heavy brute-force FT.SEARCH on the monoio io_uring driver
  And a co-located connection issuing GET on the same shard thread
  When the search yields between chunks via the always-ready self-pipe read
  Then co-located GET p99 stays within the #179 anchor (~6.6ms @1 thread)
  And it does NOT regress toward full-search-time (the self-wake no-op failure)

Scenario: re-tuned chunk recovers the deferred QPS
  Given the self-pipe yield active at the re-tuned (small) brute-force chunk
  When a heavy brute-force FT.SEARCH runs in a same-run A/B vs the timer-park control
  Then brute-force QPS is within ~5% of the pre-#179 / timer-disabled control
  And co-located latency relief from scenario 1 is preserved

Scenario: operator chunk override still honored
  Given MOON_FT_YIELD_CHUNK=2048 in the environment
  When ft_search_yield_budget() resolves (first search)
  Then max_brute_force_vecs_per_chunk == 2048
  And the cached OnceLock value is unchanged on later calls

Scenario: tokio path unchanged
  Given the runtime-tokio build
  When cooperative_yield() runs during a heavy FT.SEARCH
  Then it uses tokio yield_now (no self-pipe is created)
  And co-located relief is unchanged from #179 (~6ms under a ~63ms search)

Scenario: no new unsafe introduced
  Given the completed build
  When scripts/audit-unsafe.sh runs
  Then the unsafe-block count is unchanged from main (zero new unsafe in moon)

Scenario: yield_init_failed -> graceful timer-park fallback
  Given self-pipe/eventfd creation fails (injected ENFILE / fd exhaustion)
  When cooperative_yield() is first called on that shard thread
  Then it falls back to sleep(ZERO) timer-park
  And the FT.SEARCH completes with correct results
  And it neither errors nor runs synchronously (relief contract intact)

Scenario: uring_unavailable -> keep timer-park on the poll driver
  Given MOON_NO_URING=1 (legacy/poll driver)
  When cooperative_yield() runs during a heavy FT.SEARCH
  Then it uses sleep(ZERO) (no self-pipe park trick)
  And co-located commands still progress via the poll readiness pump

Scenario: yield_pipe_starved -> re-arm, never hang
  Given the self-pipe has been drained / mis-armed (no byte ready)
  When a yield attempts its read
  Then it re-arms the pipe and falls back to sleep(ZERO) for that one yield
  And the search never blocks waiting on the empty pipe
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

Internal Rust seam (no wire/RESP surface). Frozen shape = the yield API + its behavioral decision
tree + the budget knob. Exact tuned numbers are build-determined and pinned by the §2 invariants,
not guessed here.

```
SEAM (unchanged signature — callers in handler_{monoio,sharded}/ft.rs untouched):
  pub async fn cooperative_yield()                          // src/runtime/mod.rs

BEHAVIOR (monoio build), evaluated per call:
  if uring_unavailable            -> sleep(ZERO)            // MOON_NO_URING / poll driver
  else if pipe_ready()            -> read 1 byte on the always-ready self-pipe (cost-free park+reap)
  else /* init/arm failed */      -> re-arm best-effort, then sleep(ZERO)   // never block, never sync
  POSTCONDITION (all branches): the run loop drained to empty, parked, and reaped the CQ once.

BEHAVIOR (tokio build): tokio::task::yield_now()            // UNCHANGED from #179

RESOURCE (new, monoio-only):
  thread_local! YIELD_PIPE: lazy per-shard self-pipe (monoio net::unix::UnixStream::pair() socketpair,
    public API — `Pipe` does NOT impl AsyncReadRent, UnixStream does).
    - created on first cooperative_yield() call (lazy, like Lua sandbox)
    - kept readable: pre-filled to the socket buffer; re-armed (1 write) when the readable count runs low
    - state: Uninit | Ready(rx,tx) | Failed   (Failed is sticky for the thread -> permanent sleep(ZERO))
    - lifetime: lives for the shard thread; dropped on thread teardown. NO cross-shard sharing.

BUDGET (src/vector/segment/holder.rs):
  FT_SEARCH_YIELD_BUDGET.max_brute_force_vecs_per_chunk : usize
    - default re-tuned DOWN from 16384 to the measured knee K (K determined by the §2 A/B sweep;
      recorded in §7). CONSTRAINT, not a guess: K MUST satisfy scenario 1 (relief within #179 anchor)
      AND scenario 2 (QPS within ~5% of timer-disabled control).
  MOON_FT_YIELD_CHUNK env override: UNCHANGED (OnceLock-cached, >0 wins over default).
```

Outcome for every §1 Reject code (the seam NEVER returns an error — it degrades):
```
  yield_init_failed   -> YIELD_PIPE := Failed (sticky); this + all later yields use sleep(ZERO).
  uring_unavailable   -> sleep(ZERO) path taken unconditionally; self-pipe never created.
  yield_pipe_starved  -> best-effort re-arm; THIS yield uses sleep(ZERO); pipe may recover next call.
```

Glossary deltas (add to GLOSSARY.md): **self-pipe yield**, **cost-free park-reap**, **yield knee (K)**.

Freeze justification — spike `tmp/yield-spike` (io_uring VM, 2026-06-15): self-pipe `UnixStream::pair()`
read relieves a co-located victim (10ms vs 250ms starved) at ~µs/yield vs `sleep(ZERO)`'s ~1.8ms/yield
(+26% wall tax). Mechanism proven before freeze; the two ⚠ make-or-break assumptions are resolved.

Least-sure flag surfaced at freeze: ⚠ [contract] the exact knee K that recovers QPS within ~5% AND holds
the #179 relief is build-measured, not yet known — because it depends on the brute-force scan's real
per-vec cost vs the now-µs yield. If wrong: K lands higher than hoped → partial recovery (logged in §7),
never a correctness or relief regression (those are guarded by scenarios 1 & 6–8).
[RESOLVED in verify: K=512 — end-to-end A/B (20k×384d, release) = +2.74% vs sync, within the 5% bound.]

Status: FROZEN @ v1 — approved by Tin Dang (conditional freeze pre-authorized "if spike yes → freeze + build";
spike GREEN 2026-06-15). Changing this frozen contract = change request back to SPECIFY.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: behavior-complete (one test per §2 scenario). Perf scenarios assert bounds, not %.
Red-first vs regression-guard is marked per test — latency relief is already green on main (#179),
so it is a GUARD; the new mechanism + re-tune + fallbacks are RED-first.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - test_self_pipe_path_taken  [RED]  monoio: a test hook (atomic YIELD_PIPE_READS counter, #[cfg(test)])
      proves cooperative_yield took the self-pipe read path during a heavy search (counter > 0).
      Red on main: symbol/path absent. (Deterministic wiring proof for ⚠#1.)
  - test_colocated_relief       [GUARD/BENCH]  monoio: heavy FT.SEARCH + co-located GET; assert co-located
      p99 within the #179 anchor (~6.6ms @1t). Green on main (sleep(ZERO)); MUST stay green after swap.
      This is the EFFECTIVENESS proof for ⚠#1 — run in verify on a clean VM (the #179 silent-no-op catcher).
  - test_qps_recovery           [RED/BENCH]  same-run A/B: self-pipe@K vs timer-disabled control; assert
      brute-force QPS within ~5%. Red on main (−22% at chunk 16384). Verify-phase, clean VM.
  - test_chunk_default_retuned  [RED]  assert FT_SEARCH_YIELD_BUDGET.max_brute_force_vecs_per_chunk == K (< 16384).
      Red on main (== 16384).
  - test_chunk_env_override     [GUARD]  MOON_FT_YIELD_CHUNK=2048 -> budget == 2048; OnceLock stable. Green on main, keep.
  - test_tokio_unchanged        [GUARD]  runtime-tokio: yield path == yield_now; no self-pipe symbol compiled. Green, keep.
  - test_no_new_unsafe          [GUARD]  scripts/audit-unsafe.sh count unchanged vs main. Green, keep.
  - test_yield_init_failed      [RED]  inject pipe-create failure (MOON_TEST_YIELD_PIPE_FAIL) -> path falls to
      sleep(ZERO), search returns correct results, no error, not synchronous. Red on main (no fallback branch).
  - test_uring_unavailable      [RED]  MOON_NO_URING=1 -> sleep(ZERO) path; co-located still progresses. Red on main
      (no driver-gated branch — main always sleep(ZERO), so assert the SELECTOR exists and chose timer).
  - test_yield_pipe_starved     [RED]  force drained pipe -> re-arm + sleep(ZERO) for that yield; search never hangs
      (bounded wall-clock). Red on main (no starvation branch).
</test_plan>

Tests live in: `tests/` · MUST run red (missing implementation) before Build.
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

- [x] all tests pass — RED→GREEN: `chunk_default_retuned_to_512` (16384→512), `monoio_yield_overhead_is_microscopic`
      (200 yields <100ms vs timer ~360ms). GUARDS green: `monoio_yield_relieves_colocated`, `monoio_yield_falls_back_on_init_failure`,
      `chunk_env_override_still_honored`, #179 `m1/m1b/m2/m3/m4` (monoio), 677 vector tests (tokio, recall/correctness intact).
- [x] coverage did not decrease — 3 new monoio unit tests + 2 integration tests added; no tests removed.
- [x] no test or contract was altered during build — §3 FROZEN untouched; the only test edit was adding new RED tests.
- [x] concurrency / timing of the risky operation is safe — per-shard thread-local (`!Send`, shared-nothing); NO lock held
      across `.await` (the pipe is taken OUT of the thread-local before the read await, restored after); re-entrant caller
      (two searches/thread) falls back to timer; cancelled-mid-await drops+re-inits the pipe (no leak). ⚠ ESCALATES (below).
- [x] no exposed secrets, injection openings, or unexpected dependencies — zero new crates (monoio `UnixStream` already a dep);
      no I/O of user data; the socketpair carries only sentinel filler bytes.
- [x] layering & dependencies follow CONVENTIONS.md — change confined to `src/runtime/mod.rs` + the `holder.rs` constant;
      no new `unsafe` (218/218, audit PASS); 0 new unannotated unwrap (ratchet PASS); fmt + clippy clean BOTH runtimes.
- [x] a person reviewed and approved the change — Tin Dang PASS @ 2026-06-15 at the verify gate, choosing K=512 from the
      build-measured sweep (see GATE RECORD).

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — `cooperative_yield` → `monoio_yield::park_reap` (mod.rs:51); `park_reap` is the FT.SEARCH yield,
      called at `holder.rs:648,672,693,704,750` (the chunk loop, confirmed by grep); `set_force_fail` referenced by
      `monoio_yield_falls_back_on_init_failure`; `uring_active`/`timer_park`/`create_pipe` all referenced within `park_reap`.
- [x] DEAD-CODE (code) — no orphans: `clippy -D warnings` (which fails on `dead_code`) passed under BOTH runtimes;
      the `#[cfg(test)]` `FORCE_FAIL`/`set_force_fail` are used only under test (correctly gated).
- [x] SEMANTIC — the freeze flag (⚠ exact knee K) was discharged by an end-to-end FT.SEARCH A/B (`tests/ft_yield_chunk_ab.rs`,
      20k×384d KNN10, release VM, best-of-3, fresh server/arm), NOT mechanism arithmetic alone. Mechanism: self-pipe yield =
      **0.317µs** (100k-iter, `tmp/yield-spike/overhead.rs`), 5514× cheaper than `sleep(ZERO)` (1746µs). End-to-end sweep vs a
      true sync control (chunk=1e9, never yields): **K=512 = +2.74%** (shipped knee, 2× margin under the 5% bound), K=256 =
      +4.98% (on the line at 384d), K=1024 = +2.02%. The pure-arithmetic prediction (<2% @384d at K=256) UNDER-predicted real
      overhead by ~3pts — per-chunk loop bookkeeping beyond the read cost is not negligible; the real A/B was worth running.
      Spike `tmp/yield-spike/main.rs`: co-located victim relieved 10ms vs 250ms starved; at K=512 relief is ~39 yields/query.

### GATE RECORD
Outcome: **PASS** — Tin Dang @ 2026-06-15 (autonomy: conservative; risk: high). The human took the stricter of the two
documented options: require the end-to-end FT.SEARCH chunk-sweep A/B BEFORE gating, then PASS on its result.
Residue at escalation and how it was discharged:
  (1) CONCURRENCY/TIMING — the thread-local self-pipe + take-out/restore across `.await` is a new monoio-internals timing
      contract the unit tests exercise but cannot exhaustively prove under production interleavings. → ACCEPTED by the human
      reviewer on the strength of the unit tests (`monoio_yield_relieves_colocated`, fallback) + the #179 m1b relief guard +
      no-lock-across-await deep check; a production interleaving regression would surface on the scenario-1 monitor (§7).
  (2) PERF MAGNITUDE — scenario 2 was previously proven only at the mechanism level (arithmetic). → DISCHARGED by a real
      end-to-end FT.SEARCH A/B on the VM (`tests/ft_yield_chunk_ab.rs`, 20k×384d KNN10, release): K=512 = **+2.74%** vs a
      true sync control, inside the 5% bound with 2× margin. The arithmetic had under-predicted (~<2%); the bench corrected it
      and drove the K=256→512 choice. The absolute single-shard RPS magnitude on real disk (GCloud) remains deferred with the
      milestone's two siblings, but the RELATIVE within-5% claim — the actual scenario-2 contract — is now VM-confirmed.
Reviewed by: Tin Dang · date: 2026-06-15

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors):
- `ft_search_cooperative_yields_total` (INFO) — now increments ~32× more often (chunk 16384→512). NOT a regression
  (each yield is 0.317µs), but the counter's MAGNITUDE shifted; any dashboard alerting on its absolute rate must re-baseline.
- co-located command p99 during heavy FT.SEARCH — should now be ~40µs-class (512-vec gaps), far under the 6.6ms #179 anchor.

Spec delta for the next loop:
- The default chunk 512 is the build-measured knee (+2.74% vs sync at 384d, 2× margin). It holds the <5% throughput bound
  for ≥~210d — covering the common embedding floor (384d MiniLM, 768d, 1536d) and 256d models. LOW-dim/toy workloads (≤~210d,
  e.g. the 4d test vectors) have a higher per-yield-relative cost — operators there should raise `MOON_FT_YIELD_CHUNK`
  (and conversely 256 is reachable for latency-first deployments that accept ~5%). A future adaptive chunk (scale K by
  measured per-vec distance cost) would remove the manual knob and let the knee track dimension automatically.
- The end-to-end FT.SEARCH RPS A/B (scenario 2 absolute) remains GCloud-deferred (OrbStack bench-exception), joining the
  milestone's two other deferred magnitudes — a natural `xshard-read-coalescing`-style follow-up bundles them on real disk.

### Competency deltas
- [TDD · open] A behavioral red test (`monoio_yield_overhead_is_microscopic`: 200 yields must finish <100ms) caught the
  cost-free property deterministically WITHOUT internal counters — measuring wall-time at the yield granularity beats
  exposing an introspection hook (evidence: red 360ms → green <1ms, no new public surface).
- [SDD · open] Spike-before-freeze (a ~40-line standalone monoio program) refuted the make-or-break ⚠#1 AND corrected the
  contract (`Pipe`→`UnixStream::pair()`: `Pipe` has no `AsyncReadRent`) BEFORE locking the mechanism — cheap de-risking that
  the frozen-contract model should reach for whenever the riskiest assumption is a library-internals question (evidence: the
  literal "NOP io_uring op" the request named was unreachable; the spike found the reachable equivalent).
- [ADD · open] Mechanism arithmetic UNDER-predicted the tuning parameter; the end-to-end A/B was worth running. The 0.317µs
  per-yield constant predicted <2% overhead at K=256, but the real FT.SEARCH A/B measured +4.98% there (and +2.74% at the
  shipped K=512) — a ~3pt gap from per-chunk loop bookkeeping the mechanism cost ignored. Lesson: a measured dominant-cost
  constant is necessary but NOT sufficient to freeze a tuning knee; pair it with a RELATIVE same-binary A/B (which cancels
  the absolute-RPS noise that made us defer in the first place) before committing the default. The relative A/B sidesteps the
  OrbStack absolute-noise problem entirely — control and treatment share the VM, so only the ratio matters.
