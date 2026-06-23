# TASK: Xshard Read Gcloud Validation

slug: xshard-read-gcloud-validation · created: 2026-06-22 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: done   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower the
     autonomy level to `manual` or `conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Ground truth gathered 2026-06-22 (file:line / commit evidenced; code-nav + git, not
memory). This is a **measure-only** validation task (boundary confirmed by user): it
ports the existing quiesced-xshard instrument onto GCloud bare-metal to record TRUE
ABSOLUTE cross-shard read latency for the SHIPPED C2 mechanism, and emits a data-backed
recommendation. No `src/` change — the C2 symbols below are READ-ONLY (measured, asserted
unchanged), never modified.

Touches (files · symbols · signatures):
- HARNESS (the task's actual deliverable surface — "build" = a GCloud-targeted absolute harness, not src/):
  - `scripts/baseline-xshard-quiesced.sh` (207 ln) — the quiesced pinned RELATIVE baseline
    instrument from xshard-read-fastpath M0: moon cores 0-3 / redis-benchmark cores 4-5,
    fresh-server-per-rep, best-of-N RPS (= latency floor), 1-min-load quiesce gate (<0.7),
    `-r 1000000 --appendonly no`. The instrument to PORT to GCloud (where absolute µs is trustworthy).
  - `scripts/probe-xshard.sh` (1.6k) — local-vs-xshard discriminator; the s1 LOCAL control cell
    that proves a regression is cross-shard-specific, not a slow-binary / slow-host artifact.
  - `scripts/remeasure-xshard-fix.sh` (2.6k) — the 3-way commit remeasure pattern (pre/SPSC/post-fix).
  - `scripts/gcloud-bench-setup.sh` (42 ln, May) — GCloud instance provisioning; `scripts/run-gcloud-bench.sh`
    (435 ln) / `scripts/gcloud-benchmark.sh` (454 ln) — older general GCloud harnesses (pre-xshard) reused
    for the ssh/scp + instance-lifecycle pattern, NOT the xshard cells.
- C2 MECHANISM UNDER TEST (read-only — `src/shard/slice.rs`, the lock-free idle+batch-depth gate):
  - `xshard_should_spin(batch_remote: usize) -> bool` (slice.rs:273) — the single reply-side spin
    decision; spins iff `batch_remote <= XSHARD_SPIN_MAX_BATCH_REMOTE && xshard_may_spin()`.
  - `xshard_may_spin() -> bool` (slice.rs:261) — near-idle test on thread-local `XSHARD_INFLIGHT: Cell<u32>` (no atomic/lock/syscall).
  - consts: `XSHARD_SPIN_GATE = 2` (slice.rs:235), `XSHARD_SPIN_BUDGET = 4096` (slice.rs:239),
    `XSHARD_SPIN_MAX_BATCH_REMOTE = 1` (slice.rs:253); `XshardWaitGuard` (RAII inc/dec of inflight).
  - the two reply-wait spin sites: `src/server/conn/handler_monoio/mod.rs:1733+` (monoio) and
    `src/server/conn/handler_sharded/mod.rs:1636+` (tokio) — both call `xshard_should_spin`.
  - `src/shard/dispatch.rs:35 CoalescedReadBatch` — DEAD scaffolding for the deferred coalescing
    path; relevant only as input to this task's recommendation (build it / drop it).
- REFERENCE COMMITS to measure across (all verified present):
  - `3e376a1` v0.3.0 — pre-shardslice lock-read fast-path (the "before"); `a497602` — post-shardslice
    SPSC baseline (the regression); `7048e8a` — C2 batch-gate fix (the shipped mechanism); main `7e0a5db`.

Context (working folder):
- `.add/tasks/xshard-read-fastpath/TASK.md` §6 M0 RECORD — the RELATIVE table this gives absolute
  ground truth for: s4-c1-GET 37580→22252 (−40.8% on OrbStack); same-run +18.0→+22.9% C2 recovery;
  s4-c100-GET ~202k starvation guard; s4-P16 the −27.5%→+5.2% batch-gate cell.
- `.add/tasks/shardslice-migration/TASK.md` §6 GATE RECORD — the RISK-ACCEPTED waiver (owner Tin Dang,
  expires 2026-08-01) whose absolute-validation residue this task closes.
- `CHANGELOG.md [Unreleased]` — a `### … (PR #N)` entry is a CI-Lint blocker (gotcha_ci_lint_fmt_and_changelog).
- GCloud access: the v3-2 / 4-feature pattern (`tmp/gce-4feature-bench.sh`; ssh-stdin + scp traps; `gh`
  account is pull-only → cannot open PRs from the instance). Billing reopened since 2026-06-13 (v2-1/v3-2 ran on GCloud).

Honors (patterns / conventions — task-delta only):
- CLAUDE.md: "All benchmark numbers MUST come from the Linux VM (or GCloud instances)" — this task is the
  GCloud-instance form; it is the instrument that makes ABSOLUTE RPS trustworthy.
- INVERTS the xshard-read-fastpath observe SDD delta ("state perf Musts on the OrbStack VM ONLY in
  relative form, never absolute RPS") — that constraint existed BECAUSE the VM is invalid for absolute
  cross-shard latency (host→guest vCPU starvation inflated 36µs→580µs); GCloud bare-metal lifts it.
- Instrument hygiene (gotcha_leaked_moon_busypoller): assert no stray `moon-…-monoio` busy-poller before
  measuring; best-of-N floor; load-quiesce gate; s1 LOCAL control to separate signal from host drift.
- Measure-only: zero `src/` edits; verification is EVIDENCE-gated (absolute table + recommendation), not a unit assert.

Anchors the contract cites:
- `scripts/baseline-xshard-quiesced.sh` (the instrument ported) → the new GCloud-xshard absolute harness it yields.
- the reference commits {3e376a1, a497602, 7048e8a, 7e0a5db} and the cells {s4-c1-GET, s4-c100-GET guard, s4-P16, s1-LOCAL control}.
- `xshard_should_spin` / `XSHARD_SPIN_GATE` / `XSHARD_SPIN_MAX_BATCH_REMOTE` (asserted unchanged — read-only).
- the absolute-numbers evidence table + the disposition recommendation (close-line / coalesce / RCU) as the task's output artifact.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Validate the shipped cross-shard read C2 fast-path on a TRUSTWORTHY ABSOLUTE
instrument (GCloud bare-metal) and emit a data-backed disposition recommendation for the
cross-shard-read line. The xshard-read-fastpath win (+18.0→+22.9% c1 GET) was only ever a
SAME-RUN RELATIVE ratio on OrbStack — an instrument the spike proved invalid for absolute
cross-shard latency (host→guest vCPU starvation inflated 36µs→580µs). This task ports the
quiesced-xshard instrument to GCloud, records TRUE absolute µs/RPS across the four reference
commits dual-runtime, and recommends close-the-line / build-coalescing / reopen-RCU from the
numbers. **Measure-only (user-confirmed boundary): zero `src/` change** — any code follow-on
is a separate next task with its own contract.

Framings weighed: GCloud bare-metal absolute validation, measure-only + recommend (CHOSEN —
user 2026-06-22, both scope and boundary questions) · validate-and-act in one task (offered,
declined — couples measurement to an unknown fix, can't freeze the contract until data is in) ·
build coalescing now / reopen RCU now (declined at the scope question — premature without absolute
data) · keep trusting the OrbStack relative ratio (rejected — the whole point is the relative
ratio is NOT absolute evidence, and billing reopened).

Must:
<must>
  - M1 (a valid absolute instrument): a committed GCloud harness reproduces the quiesced-xshard
    cells with cores pinned (moon shards / redis-benchmark on disjoint dedicated cores), fresh-
    server-per-rep, best-of-N RPS (= latency floor), `-r 1000000 --appendonly no`, and PASSES three
    validity gates BEFORE any cell is trusted: (a) 1-min load quiesced (<0.7) at measure time,
    (b) no stray `moon-…-monoio` busy-poller alive (gotcha_leaked_moon_busypoller), (c) the s1 LOCAL
    control cell is flat across commits (proves a delta is cross-shard-specific, not a slow-binary /
    contended-host artifact). The harness is reusable (`scripts/`), built on a GCloud-local clone with
    `MOON_BIN` pinned (gotcha_ci_matrix_vmlocal_target).
  - M2 (the absolute table — the deliverable): record ABSOLUTE s4-c1-GET (the target cell) for all
    four commits {3e376a1 lock-read, a497602 SPSC-regression, 7048e8a C2-fix, 7e0a5db main} on BOTH
    runtimes (monoio + tokio), reported as both µs/op and RPS with the best-of-N floor. The C2 recovery
    (a497602→7048e8a) is stated as an ABSOLUTE delta, and the residual gap to the 3e376a1 lock-read
    floor is QUANTIFIED in absolute µs — the number that decides whether coalescing/RCU is worth it.
  - M3 (guard cells hold absolutely): s4-c100-GET (the starvation guard — must not regress vs spin-
    disabled) and s4-P16 (the pipeline batch-gate cell) are measured absolute on the same instrument
    and show no regression at the C2 commit; the s1 LOCAL control is recorded as the validity anchor.
  - M4 (the recommendation — the decision output): a written disposition backed by the M2/M3 numbers,
    choosing exactly one of {close-the-line · build-coalescing (`xshard-read-coalescing`) · reopen-RCU/
    snapshot}, stating the absolute residual-gap magnitude, the memory-vs-latency framing for RCU, and
    the fate of the dead `CoalescedReadBatch` scaffolding. Recorded in §6 + carried as an `[ADD·open]`
    delta seeding the next task (or its absence).
  - M5 (no code drift; mechanism unchanged): the run touches zero `src/` files; the C2 gate constants
    (`XSHARD_SPIN_GATE`, `XSHARD_SPIN_MAX_BATCH_REMOTE`, `xshard_should_spin`) are asserted byte-identical
    pre/post; a CHANGELOG `[Unreleased]` entry records the validation; no waiver needed (validation, not a fix).
</must>
Reject:
<reject>
  - Measuring on a host-contended / shared-vCPU GCloud instance (noisy neighbour, high steal-time) such
    that absolute numbers repeat the OrbStack invalidity -> "contended_instrument" (GUARD: sole-tenant /
    dedicated-core shape + steal-time check + s1 LOCAL control flat, else the run is void, not reported).
  - Trusting any cell while a leaked busy-poller or a stale Mach-O/ELF binary is in play -> "dirty_instrument".
  - Reporting an absolute number from a single rep, or without the best-of-N floor / quiesce gate -> "unstable_absolute".
  - Editing `src/` to "improve" a number mid-validation (re-tuning the gate, an RCU spike) -> forbidden;
    measure-only -> "scope_creep_into_fix" (a fix is the NEXT task, behind its own contract).
  - Emitting a recommendation not entailed by the recorded numbers (hand-wave / vibe) -> "unbacked_recommendation".
</reject>
After:
<after>
  - A reusable GCloud-xshard absolute harness exists, and a §6 table reports absolute s4-c1-GET µs/RPS for
    all four commits × both runtimes, with the C2 recovery and the residual-to-lock-read gap in absolute µs.
  - Guard cells (c100, P16) are recorded unregressed at C2; the s1 LOCAL control confirms instrument validity.
  - Exactly one disposition is recommended with the numbers behind it; the `CoalescedReadBatch` scaffolding's fate is named.
  - `src/` is untouched; the C2 gate constants are unchanged; the shardslice waiver's absolute-validation residue is closed.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ GCloud actually yields a TRULY uncontended absolute instrument — lowest confidence because the
    OrbStack spike proved host→guest vCPU starvation is the exact failure that voids absolute cross-shard
    latency, and a default shared-core GCloud VM can repeat it. Mitigation: a dedicated/sole-tenant or
    pinned-core machine (c2/n2-style) with a verified-low steal-time, gated by the s1 LOCAL control being
    flat before any cell is trusted. If wrong: absolute numbers stay untrustworthy and M2 cannot be met —
    surfaced at the M1 instrument-validation step, not after a full sweep (the run voids early).
  ⚠ The shipped +18→+22.9% same-run RELATIVE c1 recovery survives as a POSITIVE ABSOLUTE delta on GCloud —
    lower confidence because the wins were ratios on a noisy VM and could shrink (or, less likely, grow)
    in absolute terms. If wrong (it shrinks/vanishes): the recommendation legitimately shifts toward
    "C2 under-delivers absolutely → coalescing/RCU warranted" — which is a VALID outcome of this task, not a failure.
  - [ ] The tokio matrix reproduces on GCloud (the OrbStack baseline was monoio-primary) — confirm both
    runtimes measure cleanly; if tokio is too noisy to floor, report monoio-absolute + tokio-relative and say so.
  - [ ] GCloud billing/access is live for this session (memory says reopened since 2026-06-13) — confirm at
    M1 setup; if blocked, the task parks at instrument-setup with an honest "instrument unavailable", not a fudge.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# --- one per Must ---

Scenario: xrgv1 instrument validity gates pass before any cell is trusted (M1)
  Given a pinned-core GCloud instance (moon shards / redis-benchmark on disjoint dedicated cores)
  When the harness runs its pre-measure validity check
  Then it proceeds ONLY if all hold: 1-min load < 0.7, vCPU steal ~0, no stray moon busy-poller,
    and the s1 LOCAL control cell is flat across the commits under test
  And the harness is a committed reusable script built from a GCloud-local clone with MOON_BIN pinned

Scenario: xrgv2 absolute s4-c1-GET table across four commits, dual-runtime (M2)
  Given the validated instrument
  When s4-c1-GET is measured for {3e376a1, a497602, 7048e8a, 7e0a5db} on monoio AND tokio, best-of-N
  Then §6 records each cell as ABSOLUTE µs/op + RPS (best-of-N floor), the C2 recovery
    (a497602→7048e8a) as an absolute delta, and the residual gap to the 3e376a1 lock-read floor in absolute µs

Scenario: xrgv3 guard cells unregressed at the C2 commit (M3)
  Given the validated instrument at commit 7048e8a
  When s4-c100-GET and s4-P16 are measured absolute
  Then neither regresses vs the a497602 baseline beyond best-of-N noise
  And the s1 LOCAL control is recorded as the validity anchor for the run

Scenario: xrgv4 exactly one disposition recommended, entailed by the numbers (M4)
  Given the M2 + M3 absolute tables
  When the recommendation is written
  Then it selects exactly one of {close-the-line · build-coalescing · reopen-RCU}, states the
    absolute residual-gap magnitude, gives the memory-vs-latency framing for RCU, and names the
    fate of the dead CoalescedReadBatch scaffolding
  And it is carried as an [ADD·open] delta seeding (or explicitly closing) the next task

Scenario: xrgv5 no code drift; the C2 mechanism is unchanged (M5)
  Given the validation run completes
  When the working tree is inspected
  Then zero src/ files changed, the C2 gate constants (XSHARD_SPIN_GATE,
    XSHARD_SPIN_MAX_BATCH_REMOTE, xshard_should_spin) are byte-identical to pre-run,
    and a CHANGELOG [Unreleased] entry records the validation

# --- one per Reject ---

Scenario: xrgv-r1 a host-contended instance voids the run (reject contended_instrument)
  Given a GCloud instance with high steal-time or a non-flat s1 LOCAL control across commits
  When the harness detects it at the validity gate
  Then the run is declared VOID and no absolute cell is reported
  And no recommendation is emitted from contended numbers

Scenario: xrgv-r2 a leaked busy-poller / stale binary voids the cell (reject dirty_instrument)
  Given a stray moon-…-monoio process OR a binary whose ELF/Mach-O provenance is unverified
  When the harness pre-check runs
  Then it refuses to measure until the instrument is clean
  And any cell taken under contamination is discarded, not reported

Scenario: xrgv-r3 a single-rep / no-floor number is not reportable (reject unstable_absolute)
  Given a measurement taken from one rep or without the best-of-N floor / quiesce gate
  When it is offered for §6
  Then it is rejected; only best-of-N floored, quiesce-gated cells enter the table

Scenario: xrgv-r4 no src/ edit to improve a number (reject scope_creep_into_fix)
  Given the measure-only boundary
  When any temptation arises to re-tune the gate or spike an RCU read mid-run
  Then it is forbidden in this task and deferred to the recommended next task behind its own contract
  And the working tree stays src/-clean

Scenario: xrgv-r5 the recommendation must be entailed by the data (reject unbacked_recommendation)
  Given the disposition is being chosen
  When it is not derivable from the recorded M2/M3 numbers
  Then it is rejected; every recommendation clause cites a measured cell or delta
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

This task ships no API/wire surface; the frozen shape is the **harness interface**, the
**instrument config**, the **evidence-table schema**, and the **recommendation artifact** —
plus a contracted response for every §1 Reject. The C2 `src/` symbols are cited READ-ONLY.

```
# --- C1 · HARNESS (the deliverable script — committed, reusable) ---
scripts/gcloud-xshard-absolute.sh
  subcommands: --self-test (gate logic, no cloud) · --measure (inner, on a pinned host) ·
               --gcloud (sweep MACHINES) · --one (single machine)
  inputs (env): COMMITS="3e376a1 a497602 7048e8a 7e0a5db" · RUNTIMES="monoio tokio" ·
                BEST_OF_N=5 · REQUESTS=1000000 (-n) · KEYSPACE=1000000 (-r) · --appendonly no
                CELLS = s1-LOCAL · s4-c1-GET · s4-c100-GET · s4-P16
                PIN = moon→cores 0-3 ; redis-benchmark→cores 4-7 ; rest IDLE (steal buffer)
  precondition GATE (fail-closed; pass BEFORE any cell is trusted, else VOID):
    quiesce (load<0.7) · steal (~0) · clean (no stray moon poller) · control (s1-LOCAL flat across commits)
  outputs: VOID{reason} reports nothing · OK writes C3 rows to tmp/XSHARD-GCLOUD-ABS.md

# --- C2 · INSTRUMENT (frozen machine shape — the assumption-#1 mitigation) ---
GCloud compute-optimized, dedicated physical cores, provisioned + TORN DOWN per run (trap).
DUAL-VENDOR cross-validation (Intel + AMD); 4 moon + 4 bench cores + idle steal-buffer on each.

# --- C3 · EVIDENCE-TABLE SCHEMA (one row per machine×runtime×commit×cell) ---
row: { machine, vendor, runtime, commit, cell, rps_best, us_per_op, n, loadavg }
derived: c2_recovery_abs  = rps_best(7048e8a) − rps_best(a497602)        # shipped win, ABSOLUTE
         residual_gap_abs = rps_best(3e376a1) − rps_best(7048e8a)        # distance to lock-read floor, ABSOLUTE
         reported in BOTH RPS and µs/op; guard cells flagged regressed iff worse than a497602 beyond noise

# --- C4 · RECOMMENDATION ARTIFACT (exactly one disposition) ---
recommendation: {
  disposition : "close-the-line" | "build-coalescing" | "reopen-RCU",
  residual_gap_abs_us : <number, per vendor>,
  rcu_tradeoff : "<memory-vs-latency framing, RSS cost named>",
  coalesced_read_batch_fate : "remove-scaffolding" | "keep-pending-build",
  cross_vendor_note : "<Intel-vs-AMD divergence, if any>",
  evidence_refs : [ <each clause cites a C3 row or derived value> ]
}
carried as an [ADD·open] SPEC delta seeding the next task (or [ADD·folded] close if disposition=close-the-line)

# --- C5 · READ-ONLY ASSERTION (mechanism unchanged) ---
src/shard/slice.rs : XSHARD_SPIN_GATE==2 · XSHARD_SPIN_MAX_BATCH_REMOTE==1 · xshard_should_spin sig unchanged
git diff --stat src/  ==  empty   (measure-only invariant)

Reject responses (every §1 code has a contracted outcome):
  contended_instrument   -> harness returns VOID; cell discarded; never feeds C3/C4
  dirty_instrument       -> harness refuses to measure until clean; contaminated cell discarded
  unstable_absolute      -> single-rep/no-floor number rejected; only best-of-N floored rows enter C3
  scope_creep_into_fix   -> any src/ edit fails C5 (git diff non-empty) → task cannot gate PASS
  unbacked_recommendation-> C4 clause without an evidence_ref → recommendation rejected
```

Status: FROZEN @ v2 — C2 instrument amended by Tin Dang 2026-06-23.

v1 FROZEN — approved by Tin Dang 2026-06-22 (the one approval over the §1–§4 bundle).
v2 CHANGE REQUEST (2026-06-23, quota-forced, approved by Tin Dang): C2's `c2-standard-16`
is unprovisionable (project `C2_CPUS` quota = 8; caught by the GCloud smoke before any cost).
User chose a DUAL-VENDOR cross-validation instead of downgrading. Reality-corrected during build:
C3 has no 16-vCPU size (4/8/22/44/...), and `CPUS_ALL_REGIONS` global cap = 32 forbids both at once.
Final MACHINES = {`c3-standard-22` (Intel, quota 24), `c2d-standard-16` (AMD, quota 100)}, run
SEQUENTIALLY (teardown between). Both keep 4 moon + 4 bench cores + a large idle steal-buffer, so the
8 measured cores compare fairly. C3 schema gains machine/vendor; C4 adds a cross_vendor_note. Everything
else in v1 (the 4 commits, the 4 cells, the gates, measure-only, the recommendation shape) RETAINED.

Least-sure flag surfaced at freeze: ⚠ [contract] instrument validity — a default GCloud VM could repeat
the OrbStack host-contention that inflated cross-shard latency 36µs→580µs, voiding the absolute numbers
M2 depends on; mitigation = compute-optimized dedicated cores (c3/c2d) + steal-time gate + s1-LOCAL
control gating every cell + idle-core buffer; if wrong: the run VOIDs early at M1 (no wasted sweep).
Second: ⚠ [spec] the +18→+22.9% relative c1 recovery may shrink/vanish in absolute µs; if so it
legitimately shifts the C4 recommendation toward coalescing/RCU — a valid outcome, not a failure.
Changing a frozen clause (the 4 commits, the 4 cells, the C3 schema, the machine class, measure-only)
is a change request back to SPECIFY.

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: the harness gate-logic + the mechanism-unchanged invariant fully covered; the
absolute-latency Musts (M2/M3) are EVIDENCE-gated (recorded in §6), NOT unit asserts — the
behavior-preserving-perf split (CONVENTIONS; same shape as xshard-read-fastpath §4).

Plan (one test per scenario; unit-red where deterministic, else named to its §6 oracle):
<test_plan>
  HARNESS GATE-LOGIC  (`scripts/gcloud-xshard-absolute.sh --self-test` — red until §5 creates the harness)
  - selftest_gate_voids_on_high_load (xrgv-r1): synthetic load ≥ 0.7 ⇒ VOID/contended_instrument.
  - selftest_gate_voids_on_busypoller (xrgv-r2): planted moon marker ⇒ refuse, dirty_instrument.
  - selftest_gate_voids_on_nonflat_control (xrgv-r1): non-flat s1-LOCAL ⇒ VOID.
  - selftest_cell_requires_floor (xrgv-r3): single-rep ⇒ rejected; only best-of-N floored accepted.
  - selftest_steal_gate (xrgv-r1): steal > STEAL_MAX ⇒ VOID; ~0 ⇒ pass.
  - selftest_harness_exists_and_executable (xrgv1): script present, +x, declares the cells + gates.

  MECHANISM-UNCHANGED GREEN-PIN  (`tests/xshard_mechanism_unchanged.rs` — GREEN, MUST stay green; reject scope_creep_into_fix)
  - c2_spin_gate_const_pinned + c2_max_batch_remote_const_pinned + c2_should_spin_rejects_pipelined_batch (xrgv5).
  - src_tree_clean (xrgv5 / xrgv-r4): `git diff --stat src/` empty at verify (asserted in §6).

  EVIDENCE-GATED (recorded in §6 against the instrument — NOT unit tests)
  - xrgv2 absolute table · xrgv3 guard cells · xrgv4 recommendation (each clause carries an evidence_ref).
</test_plan>

RED ESTABLISHED 2026-06-22 (re-created 2026-06-23 after a volume-remount rollback; byte-identical):
- GREEN-PIN `tests/xshard_mechanism_unchanged.rs` — 3/3 PASS (the measure-only tripwire).
- RUNTIME-RED→GREEN `tests/xshard_gcloud_harness_selftest.sh` — was red (harness absent); GREEN after §5
  created `scripts/gcloud-xshard-absolute.sh` with a passing `--self-test` (11/11 gates fail-closed).
- EVIDENCE-GATED (M2/M3/M4) pending the GCloud run — recorded in §6.

Tests live in: `scripts/gcloud-xshard-absolute.sh` `tests/` · MUST run red (missing implementation) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `CHANGELOG.md` `scripts/gcloud-xshard-absolute.sh` `tests/xshard_mechanism_unchanged.rs` `tests/xshard_gcloud_harness_selftest.sh` `tmp/XSHARD-GCLOUD-ABS.md`
  — NOTE: `src/` is DELIBERATELY EXCLUDED. Measure-only — any `src/` write fails the C5 invariant (`git diff --stat src/` non-empty) and the xrgv5 green-pin. The harness + the shape test + the evidence artifact + the changelog entry are the whole build.
Strategy (ordered batches): 1. mechanism-unchanged green-pin pins C2 consts · 2. harness `--self-test` gate logic (red→green) · 3. harness measure path (per-commit read-only build on a GCloud-local clone, MOON_BIN pinned → best-of-N cells → C3 rows) · 4. dual-vendor sweep (c3-standard-22 then c2d-standard-16, sequential — CPUS_ALL_REGIONS=32 cap) → write tmp/XSHARD-GCLOUD-ABS.md · 5. C4 recommendation + CHANGELOG entry.
Safety rule (feature-specific): the instrument GATE is fail-closed — on any validity-gate miss (load/poller/control/steal) the harness emits VOID and reports NOTHING; provisioned instances are torn down even on error (trap).
Code lives in: `scripts/` + `tests/` (NOT `src/`)
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- Scope tokens, backticked, FIRST declaring line: `./…` = this task dir · a token
     with "/" = project root · a bare name = sibling of the previous token's dir ·
     outside-root resolutions are dropped fail-closed · a DIRECTORY token covers its
     whole subtree (containment — diverges from §4's non-recursive counting) ·
     absent line = UNDECLARED (pre-existing tasks grandfathered, never retro-red) ·
     engine enforcement (touched ⊆ declared) lands in scope-gate-enforce.
     EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — green-pin `tests/xshard_mechanism_unchanged.rs` 3/3 (release); harness `--self-test` 11/11 gates fail-closed
- [x] coverage did not decrease — new tests added (green-pin + self-test); none removed
- [x] no test or contract was altered during build — §3 contract FROZEN@v2 untouched; §4 tests unchanged since RED
- [x] the green was EARNED, not gamed — adversarial refute-read spawned (subagent, `autonomy: auto`) against the M4 recommendation's entailment + raw cells; verdict recorded in GATE RECORD below
- [x] concurrency / timing of the risky operation is safe — instrument GATE fail-closed (load/steal/clean/control); provisioned instances torn down via EXIT-trap (verified: both c3 + c2d instance-list EMPTY post-run)
- [x] no exposed secrets, injection openings, or unexpected dependencies — harness uses gcloud/ssh/scp + redis-benchmark only; no creds embedded; no new crate deps (zero src/ change)
- [x] layering & dependencies follow CONVENTIONS.md — deliverable is `scripts/` + `tests/` + `tmp/` only; `src/` deliberately untouched (measure-only)
- [ ] a person reviewed and approved the change — pending Tin Dang sign-off (recorded below)

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [x] the absolute s4-c1-GET cell swings 17–20µs across commits WHILE s1-LOCAL stays flat (±<1%) — confirmed: tmp/XSHARD-GCLOUD-ABS.md C3 table + raw /tmp/xshard-abs-{c3,c2d}*.txt (`control_flat: OK` printed by both runs)
- [x] the C2-fix commit (7048e8a) RECOVERS vs the regression (a497602) on every instrument, never worse — confirmed: monoio Intel 41.01→34.41µs, tokio Intel 41.51→34.31, monoio AMD 64.60→55.28, tokio AMD 63.99→55.18 (4/4 positive)
- [x] guard cells (c100, P16) do NOT regress at 7048e8a vs a497602 — confirmed: M3 tables, all flat-or-improved (Intel-tokio c100 even +11%)
- [x] exactly ONE disposition emitted with each clause citing a measured cell — confirmed: C4 block = `close-the-line`, evidence_refs to C3 rows + derived residual
- [x] zero src/ edits; gate consts byte-identical — confirmed: `git diff --stat src/` empty + green-pin 3/3

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — green-pin imports `moon::shard::slice::{XSHARD_SPIN_GATE, XSHARD_SPIN_MAX_BATCH_REMOTE, xshard_should_spin}` and all three are referenced in asserts; harness `--self-test` exercises every gate function (quiesce/steal/clean/require_floor/control_flat/us_per_op)
- [x] DEAD-CODE — no new src symbol introduced (measure-only); the EXISTING dead `CoalescedReadBatch` scaffolding is explicitly dispositioned (remove) in C4, not left dangling
- [x] SEMANTIC (prose) — read tmp/XSHARD-GCLOUD-ABS.md in full: the 32+32 raw cells transcribe correctly into the C3 table, the derived recovery/residual arithmetic checks out, and every C4 clause traces to a cell. Independently re-verified by the refute-read subagent.

### GATE RECORD
Outcome: PASS
Adversarial refute-read verdict: **CANNOT-REFUTE** — recommendation (close-the-line) entailed by the data;
claims 1/3/5/7 verified against raw cells, no `fix worse than regression` anywhere, all n=5. Two overstatements
the skeptic caught were FOLDED INTO the doc (not waved away): (a) "invariant ~10µs" → "converges to ~10µs (±1µs)";
(b) the tokio runtime is rep-bimodal on both vendors (best-of-5 floor clean, full distribution not) — disclosed
in a new "Instrument honesty" section; monoio flagged as the high-confidence set, AMD·tokio as weakest (median
~9.5µs). Disposition unchanged: it rests on the 3 clean instruments + AMD·tokio-at-median. No cheat found
(measure-only — zero src logic to overfit; green-pin asserts the SHIPPED consts).
Reviewed by: AI build + adversarial refute-read subagent · **awaiting Tin Dang human approval** · date: 2026-06-23

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): the cross-shard read residual (~10µs absolute) is now a KNOWN
structural floor — monitor s4-c1-GET regressions past 7048e8a's ~34µs (Intel) / ~55µs (AMD) as a sign
the second cross-thread wake regressed; the s1-LOCAL control remains the validity anchor for any re-measure.

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.
- [SPEC · folded] cross-shard-read acceleration line CLOSED — C2 validated absolute on bare-metal (38–49% recovery, ~10µs vendor/runtime-invariant structural residual, 4 instruments); disposition `close-the-line`. The deferred `cross-shard-read-acceleration` follow-up is closed "no further work warranted", NOT carried (evidence: tmp/XSHARD-GCLOUD-ABS.md C3+C4; shardslice waiver residue resolved).
- [SPEC · seeded] remove the dead `CoalescedReadBatch` scaffolding (`src/shard/dispatch.rs:35`) — coalescing targets the concurrent path (c100 guard), which is flat/unregressed, and cannot help the singleton residual (evidence: M3 c100 flat 4/4; C4 coalesced_read_batch_fate=remove-scaffolding). A tiny src-touching cleanup task, its own contract.
- [SPEC · dropped] reopen RCU/snapshot foreign-read — rejected: closes the ~10µs but at per-shard RSS cost the design rejected; not justified while guards are healthy (evidence: C4 rcu_tradeoff).

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
- [SDD · open] a RELATIVE-only win (OrbStack +18→+22.9%) needs an ABSOLUTE bare-metal cross-check before a line is closed — the absolute residual (~10µs) is the number that actually decides coalesce/RCU, invisible to a same-run ratio (evidence: this whole task; the residual was unknowable from the relative table).
- [ADD · open] the engine `_scope_walk` md5's `target/` and hangs on large/slow trees — workaround was to stash all `target*` dirs to a sibling outside the walk root; `_SCOPE_EXCLUDE_DIRS` should exclude Rust target dirs (evidence: tests→build advance hung on 8GB+ target over virtiofs; regrew to 767M mid-task).
- [ADD · open] the §5 scope token-resolver is POSITION-sensitive in a surprising way — a project-ROOT bare file (`CHANGELOG.md`) only resolves to root when FIRST in the list; trailing after a `tmp/…` token it became `tmp/CHANGELOG.md`, mis-scoping the real `/CHANGELOG.md` edit (evidence: state.json scope.declared snapshot held `tmp/CHANGELOG.md`; workaround = declare root bare files first).
- [TDD · open] measure-only tasks split test surface cleanly: a deterministic green-pin (gate consts byte-identical) + a fail-closed harness self-test guard the MECHANISM, while the perf Musts (M2/M3) are EVIDENCE-gated in §6 — the behavior-preserving-perf split reused from xshard-read-fastpath worked again (evidence: §4 RED→GREEN both deterministic; M2/M3 recorded as cells not asserts).
