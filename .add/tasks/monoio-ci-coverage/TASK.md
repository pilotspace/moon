# TASK: CI runs the test suite on a monoio build (and clippy --all-targets)

slug: monoio-ci-coverage · created: 2026-08-09 · stage: production
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

Touches (files · symbols · signatures):
- `.github/workflows/ci.yml` — 8 jobs. **Every job that EXECUTES tests does so under
  `--no-default-features --features runtime-tokio,…`**: `check:127,133,136` (nextest + doctests +
  graph), `check-macos:166`, `check-windows:220`. The single mention of monoio anywhere in CI is
  `check-console:261`, and it is `cargo clippy`, never a test run.
- `check:` job — `runs-on: [self-hosted, moon-dev]`, the Linux VM. This is the ONLY place monoio's
  io_uring path could actually execute; macOS would exercise kqueue, Windows cannot run monoio.
- `client-compat:322` — the one partial mitigation, and it is black-box only: it runs
  `cargo build --release` (default features ⇒ **monoio**) and drives that binary over a real socket.
  So the monoio *RESP surface* is checked; no monoio Rust test ever runs. It also pins `MOON_BIN`
  explicitly, which is the correct pattern (contrast the script default — issue #461).

Measured exposure (2026-08-10, `main` @ `4c9bd2c5`):
- **26 integration test files** under `tests/` reference `runtime-monoio`
- **30 `src/` files** carry `feature = "runtime-monoio"` cfg, incl. 3 `cfg(all(test, …monoio))`
  unit-test modules — CI-invisible by construction
- **0 CI jobs** execute any of them

Cost side (last green run, warm self-hosted cache): `Check` ≈ 2m, `Memory steady-state` ≈ 2m,
`client-compat` < 1m. Five jobs across `ci.yml` + `crash-matrix.yml` already contend for the single
`moon-dev` runner, so a new job's queue cost is the real constraint, not its wall-clock.

Context (working folder): `.github/workflows/ci.yml` · `.config/nextest.toml` (the `ci` profile) ·
`Cargo.toml` feature table (`default = [runtime-monoio, …]`).

Honors (patterns / conventions): CLAUDE.md "Feature Gates" (all runtime code must compile under both
runtimes) and "Local CI Parity" (the documented `orb run` matrix already runs BOTH suites — CI is
strictly weaker than the documented local gate, which is the anomaly this task closes) · the
self-hosted-runner conventions in `check:` (CARGO_TARGET_DIR isolation per job, explicit timeouts).

Anchors the contract cites: `ci.yml::check` · a new `ci.yml::check-monoio` · `MOON_NO_URING` ·
`CARGO_TARGET_DIR` · `cargo nextest run --profile ci`.

**Why this is the milestone's structural task.** The defect class is not hypothetical and not rare:
- the v0.8.6 **inline-GET ACL bypass** (#457) — monoio-only, shipped unobserved
- `resp3-type-fidelity` (#463) — both enqueue sites had to be verified by hand locally, because CI
  cannot see one of them
- [[gotcha_monoio_intercept_order_ci_blind]] and [[project_o4_o5_o1_perf_wave]] both record
  monoio-only unit tests being invisible to every CI test job

Moon ships **monoio by default on Linux**. CI tests the fallback runtime and not the shipped one.

### ⚠ Assumption discharged BEFORE the contract — "is the monoio suite even green on Linux?"

This decides the task's size: "add a CI job" (hours) vs "add a job **and** fix N rotted tests"
(days). It was measured, not assumed — full monoio suite on the `moon-dev` Linux VM (aarch64,
kernel 6.17, io_uring available), 2026-08-10:

| run | config | result |
| --- | --- | --- |
| 1 | `cargo test --tests --no-fail-fast` (no retries) | 194 binaries · **5138 passed** · 0 compile errors · 2 failing targets |
| 2 | `cargo nextest run --profile ci` (**the proposed job's actual config**) | see GATE evidence |

**Verdict: the suite is healthy.** It compiles clean under default features and 5138 tests pass. This
task is "add a job", not a rescue mission.

The two failing targets are both load-sensitive, not rotted:
- `dbsize_offload_logical` — already filed as #459 (pre-existing, proven by same-load A/B where the
  pre-change binary failed MORE often).
- `blocking_peer_eof::silent_blocked_client_stays_blocked` — new to Linux (macOS never showed it);
  **passes 3/3 in isolation**, so contention, not a defect.

**The finding that shapes the contract:** the suite carries load-sensitive flakes, and a CI job that
goes red intermittently gets ignored or disabled — which is worse than no job, because it looks like
coverage. The existing `[profile.ci]` in `.config/nextest.toml` already anticipates exactly this
class (`retries = 2`, commented "fixed-port listeners, kill-9 timing under full-suite parallel
load"). Run 1 used plain `cargo test` and got no retries; run 2 exists to confirm the CI profile
absorbs both. **The contract must specify the job runs `nextest --profile ci`, never bare
`cargo test`.**

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: CI executes the monoio test suite — the runtime Moon actually ships — on every PR.

Framings weighed:
- **A dedicated `check-monoio` job on the self-hosted Linux runner** (chosen) — default features,
  `cargo nextest run --profile ci`, own `CARGO_TARGET_DIR`. Measured at 80s of test time for all
  5145 tests, so the objection this framing had to survive (cost) does not hold. It is the only
  option that exercises io_uring, because that is the only Linux runner available.
- *Add monoio as a matrix leg of the existing `check` job* (rejected) — `check` also runs clippy,
  doctests and a graph-filtered leg, all of which are runtime-agnostic; a matrix would duplicate
  them for no coverage and roughly double the job's wall-clock.
- *Flip the existing test jobs from tokio to monoio* (rejected) — trades one blind spot for
  another. tokio is a supported runtime (CI portability, Windows) and must stay covered.
- *Nightly only* (rejected by measurement) — defensible when a suite is expensive; 80s is not
  expensive, and nightly still lets a monoio defect land on `main` first, which is close to the
  present failure mode.

Must:
<must>
  - A new `check-monoio` job runs on every PR and every push that the existing `check` job runs on.
  - It builds and tests with the DEFAULT feature set (runtime-monoio), never
    `--no-default-features --features runtime-tokio`.
  - It runs on `[self-hosted, moon-dev]` — the only Linux runner, and therefore the only place
    monoio's io_uring driver executes at all.
  - It invokes `cargo nextest run --profile ci`, so the repo's existing flake policy (retries = 2,
    slow-timeout, per-test overrides) applies. A bare `cargo test` is forbidden: it has no retries,
    and the suite has a known load-sensitive flake class.
  - A test that only passes on retry is still reported as FLAKY in the job log — the signal is kept,
    not swallowed.
  - It uses its own `CARGO_TARGET_DIR`, so it never shares artifacts with the tokio `check` job.
  - The job is REQUIRED: a monoio-only failure must be able to block a merge, or the job is theatre.
  - CLAUDE.md's "Local CI Parity" command and the CI matrix agree afterwards — today the documented
    local gate is strictly stronger than CI, which is the anomaly being closed.
</must>
Reject:
<reject>
  - a monoio test failure -> the job FAILS (never `continue-on-error`)
  - the self-hosted runner being offline -> the job QUEUES and the PR is not green; it must never
    silently skip, which would restore the exact blind spot this task removes
    (see [[gotcha_selfhosted_runner_offline_service_active]])
  - a flake that exhausts its 2 retries -> a real failure, reported as such
</reject>
After:
<after>
  - Every `src/` path behind `#[cfg(feature = "runtime-monoio")]` — 30 files, including 3
    test-only modules — is reachable by CI.
  - All 26 monoio-referencing integration test files execute in CI.
  - A defect of the v0.8.6 inline-GET class (monoio-only, ACL bypass) fails a PR instead of shipping.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ DISCHARGED before contract — "the monoio suite is green on Linux today". Lowest confidence
    because nothing had run it in CI, ever, so months of rot were plausible and would have changed
    this from a config task into a rescue. MEASURED 2026-08-10 on `moon-dev`:
    `cargo nextest run --profile ci` -> **5145 passed, 1 flaky, 244 skipped, exit 0, 80.3s**.
    Cost if it had been wrong: the whole framing above (a REQUIRED per-PR job) would have been
    unlandable, and the honest option would have been a non-blocking job plus a rot-triage task.
  - [x] The `ci` nextest profile's retries cover the observed flake class — confirmed: the one
    FLAKY (`dbsize_offload_logical`, #459) passed 2/3 and the run still exited 0.
  - [x] io_uring is actually available on the runner — confirmed: `moon-dev` is kernel 6.17 and the
    suite ran without `MOON_NO_URING`, unlike every existing CI test job.
  - [ ] Build time on a COLD cache is acceptable for a required job. Test time is 80s, but the
    default feature set has never been built in CI, so the first run pays a full cold build and
    there is no measurement for it yet. Confirm from the first real run; if it is punitive, the
    remedy is cache warming, not dropping the job.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: a monoio-only defect blocks the merge   # the whole point of the task
  Given a PR whose change is wrong ONLY on the monoio dispatch path
    And the tokio `check` job passes, because it never executes that path
  When CI runs on the PR
  Then `check-monoio` fails and the PR is not mergeable
  And this is exactly the v0.8.6 inline-GET ACL bypass (#457), which shipped green

Scenario: the job tests the shipped runtime, not the fallback
  Given the `check-monoio` job
  When it builds and runs tests
  Then it uses the DEFAULT feature set (runtime-monoio)
  And its command contains no `--no-default-features --features runtime-tokio`
  And `MOON_NO_URING` is unset, so the io_uring driver is the one exercised

Scenario: monoio-gated code is reachable at all
  Given 30 `src/` files behind `#[cfg(feature = "runtime-monoio")]`, 3 of them test-only modules
    And 26 integration test files referencing runtime-monoio
  When `check-monoio` runs
  Then those tests execute and are counted in its summary
  And the tokio `check` job's own test count is unchanged

Scenario: a known load-sensitive flake does not redden the job
  Given `dbsize_offload_logical` fails under full-suite parallel load roughly 1 run in 3 (#459)
  When `check-monoio` runs it
  Then `--profile ci` retries it (retries = 2) and the job exits 0
  And the log still reports it as FLAKY, so the signal is not swallowed

Scenario: a genuine failure is never masked by retries
  Given a test that fails deterministically on monoio
  When `check-monoio` retries it twice
  Then all 3 attempts fail and the job FAILS
  And the job is REQUIRED, so the PR cannot merge

Scenario: the runner being offline does not fake a pass   # rejection
  Given the self-hosted `moon-dev` runner is offline or its service is flapping
  When a PR opens
  Then `check-monoio` QUEUES and the PR is not green
  And it never reports success or skips — a silent skip would restore the exact blind
      spot this task exists to remove (see gotcha_selfhosted_runner_offline_service_active)

Scenario: artifacts never collide with the tokio job   # rejection
  Given `check` and `check-monoio` may run concurrently on the same self-hosted runner
    And they build INCOMPATIBLE feature sets from one checkout
  When both run
  Then each uses its own CARGO_TARGET_DIR
  And neither invalidates the other's cache, and neither can execute the other's binaries
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```yaml
# .github/workflows/ci.yml  — NEW job, sibling of `check`
check-monoio:
  name: Check (monoio — the shipped runtime)
  runs-on: [self-hosted, moon-dev]          # only Linux runner => only io_uring
  env:
    CARGO_TARGET_DIR: /home/tindang/ci-target/check-monoio   # isolated from `check`
    MOON_DISK_FREE_MIN_PCT: "0"
    CARGO_BUILD_JOBS: "6"
    # MOON_NO_URING deliberately UNSET — the tokio jobs set it; this job must not.
  steps:
    - uses: actions/checkout@v7
    - uses: dtolnay/rust-toolchain@1.94.1
    - uses: taiki-e/install-action@nextest
    - name: Test (default features = runtime-monoio)
      run: cargo nextest run --profile ci      # NOT `cargo test` — no retries there
      timeout-minutes: 30
```

Scope — in:  `.github/workflows/ci.yml` (one new job) · `CLAUDE.md` CI section (document it)
Scope — out: `.config/nextest.toml` (the `ci` profile already covers this flake class — proven,
             5145 pass / 1 flaky / exit 0) · fixing #459 or the `blocking_peer_eof` flake ·
             any `src/` change · the `MOON_NO_URING` epoll leg (a later task if wanted)

Target: a monoio-only defect fails a PR. Verified by the negative control in §4 — deliberately
        break a monoio-only path, confirm `check-monoio` fails while `check` still passes.

Invariants:
  - the tokio `check` job is untouched: same features, same steps, same test count
  - no `continue-on-error`, no `if: always()`, nothing that lets the job pass while not running
  - the job is REQUIRED in branch protection (otherwise it is advisory and will be ignored)

Status: **FROZEN @ v1** — approved by Tin Dang, 2026-08-10

Lowest-confidence flags surfaced AT the freeze (both accepted by the approver):
1. **[contract] "REQUIRED" is not something this task can deliver alone.** Adding the job to
   `ci.yml` makes it RUN; only branch protection makes it BLOCK. That is a repo admin setting, not
   a workflow field, so the Must "the job is REQUIRED" is half-satisfiable by code. If it is never
   added to branch protection the job is advisory, and an advisory red job gets ignored — which
   would leave the blind spot open while *looking* closed, the worst outcome. **Approver accepted
   this and owns the branch-protection change.** Cost if forgotten: the task reports done while
   delivering nothing that can stop a merge.
2. **[contract] Cold-cache build time is unmeasured.** Test time is a measured 80.3s, but the
   default (monoio) feature set has never been built in CI, so the first run pays a full cold build
   with no data behind it. If it proves punitive the remedy is cache warming or a prebuilt target
   dir — never dropping the job, since 80s of test time is not the cost driver.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Suite: `tests/ci_covers_monoio.rs` — a repo-CONFIG guard, deliberately not a runtime test. Nothing
observable at runtime can tell you which runtime CI exercised, and the failure mode of CI coverage
is silent: a job that stops running, or is switched to the wrong feature set, looks exactly like a
green build. These tests make that loud.

| test | covers | scenario |
|---|---|---|
| `ci_has_a_job_that_tests_the_default_monoio_runtime` | Must 1,2 | "tests the shipped runtime" |
| `the_monoio_job_uses_the_ci_profile_so_known_flakes_retry` | Must 4,5 | "a known flake does not redden the job" |
| `the_monoio_job_cannot_pass_without_running` | Must 3,7 · Reject 1 | "a genuine failure is never masked" |
| `the_monoio_job_does_not_share_artifacts_with_the_tokio_job` | Must 6 · Reject 3 | "artifacts never collide" |
| `the_tokio_job_is_still_covered` | invariant | (green before AND after — see below) |

### RED RUN — recorded 2026-08-10, `cargo test --profile release-fast --test ci_covers_monoio`

```
test result: FAILED. 1 passed; 4 failed
  FAILED ci_has_a_job_that_tests_the_default_monoio_runtime  -> "ci.yml has no `check-monoio` job"
  FAILED the_monoio_job_uses_the_ci_profile_so_known_flakes_retry
  FAILED the_monoio_job_cannot_pass_without_running
  FAILED the_monoio_job_does_not_share_artifacts_with_the_tokio_job
  ok     the_tokio_job_is_still_covered
```

**4 red / 1 green is the intended split, not an accident.** All four reds fail with "no
`check-monoio` job" — i.e. red for the RIGHT reason (feature absent), not from a broken assertion.
The fifth was written to be GREEN before the build: it pins that tokio coverage survives, so if the
build ever "adds monoio" by *converting* the existing job rather than adding one, that shows up as a
newly-broken invariant instead of a silent trade of one blind spot for another. A 100%-red suite
cannot make that distinction. (Same technique as `r3f11` in [[resp3-type-fidelity]].)

### Negative control — planned, runs at VERIFY

The config guard proves the job is DECLARED correctly; it cannot prove the job CATCHES anything.
So the gate also requires a one-time negative control: introduce a deliberate defect on a
monoio-only path, then confirm
  - the tokio suite still PASSES  (demonstrating the blind spot is real, not theoretical), and
  - the monoio suite FAILS        (demonstrating the new job closes it),
then revert. Evidence goes in §6. Without this, "CI now covers monoio" is an unverified claim about
a YAML file.

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `./src/`   <fill before the §3 freeze — every file the build may write>
Strategy (ordered batches): <1. … 2. … — the planned build order; guidance, not enforced>
Safety rule (feature-specific): <e.g. debit+credit in one atomic transaction>
Code lives in: `./src/`
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

- [x] all tests pass — `ci_covers_monoio` 5/5 under BOTH feature sets (it is runtime-agnostic, so
      it must hold in each); `multi_queues_inline_get` 6/6 after the negative control was reverted.
- [x] coverage did not decrease — +5 tests, +1 CI job. Nothing removed, and
      `the_tokio_job_is_still_covered` exists specifically to prove tokio coverage was not traded away.
- [x] no test or contract was altered during build — the frozen §3 was implemented verbatim
      (runner, target dir, `nextest --profile ci`, no `continue-on-error`, `MOON_NO_URING` unset).
- [x] the green was EARNED — see the NEGATIVE CONTROL below. The config guard alone proves only that
      a YAML file says the right words; the control proves the job CATCHES a real defect.
- [x] concurrency / timing — the job takes its own `CARGO_TARGET_DIR`, so it cannot race `check`'s
      artifacts on the shared self-hosted runner. `--profile ci` supplies the retries that keep a
      known load-sensitive flake class from reddening a required job.
- [x] no exposed secrets / unexpected dependencies — no new crate; the guard test reads a repo file
      with `std::fs` and parses it with string ops (no YAML dependency added).
- [x] layering — CI config lives in `.github/workflows/`; the guard is a normal integration test.
- [ ] a person reviewed and approved the change

### NEGATIVE CONTROL — the evidence that matters

A CI-config change can be green and still be worthless. So the claim "CI now covers monoio" was
tested directly: a deliberate defect was injected on `try_inline_dispatch` (which is
`cfg(feature = "runtime-monoio")`, so a tokio build cannot reach it), making inline GET answer
`$6\r\nBROKEN\r\n`, and both suites were run against it.

| leg | `acl_inline_read_enforcement` | `multi_queues_inline_get` | meaning |
|---|---|---|---|
| **tokio** — what CI ran before this task | 4 passed | **6 passed** | the blind spot is REAL: this defect ships green today |
| **monoio** — what `check-monoio` runs | 4 passed | 3 passed, **3 FAILED** | the new job CATCHES it |

An incidental confirmation arrived first: the tokio leg *compiled* while monoio did not, because
`try_inline_dispatch` is cfg'd out entirely under tokio — the isolation is structural, not incidental.

Reverted immediately via `git checkout --`; verified `0` residual `NEGATIVE CONTROL` / `BROKEN`
markers in `src/`, a clean `git status` for `src/`, and `multi_queues_inline_get` back to 6/6.

### Gates

| gate | result |
| --- | --- |
| `ci_covers_monoio` (default/monoio) | 5 / 5 |
| `ci_covers_monoio` (tokio) | 5 / 5 |
| RED run before build | 4 failed / 1 passed — all four "no `check-monoio` job" |
| negative control | tokio PASSES the defect · monoio FAILS it |
| YAML structural parse | 9 jobs, `check-monoio` well-formed |
| `cargo clippy --all-targets -D warnings` (both feature sets) | exit 0 |
| `cargo fmt --check` | exit 0 |
| pre-landing suite measurement (`moon-dev`, kernel 6.17) | **5145 passed, 1 flaky, 244 skipped, exit 0, 80.3s** |

### GATE RECORD
Outcome: **PASS**
Reviewed by: Tin Dang · date: 2026-08-10

Approved on the negative control, not on the config guard: the same deliberate monoio-only defect
PASSES the tokio suite (6/6) and FAILS the new job (3/6). That is the difference between "a YAML
file says the right words" and "the job catches defects".

OPEN, and owned by the approver: `check-monoio` must be added to branch protection. Until then it
RUNS but does not BLOCK — flagged at the freeze (§3 flag 1) and accepted there.

<!-- Held open: §6 line 8 is the reviewer's. Note the §3 freeze flag — "REQUIRED" needs a branch
     protection change this task cannot make from a workflow file; the approver owns it. Until then
     the job RUNS but does not BLOCK. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
