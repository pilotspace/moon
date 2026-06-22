# Phase 6 — Verify (evidence + non-functional review)

Goal: establish trust and record an outcome. Passing tests are necessary, not
sufficient. Fill **§6** in TASK.md including the GATE RECORD.

> **Who resolves this gate depends on the `autonomy:` header (see `run.md`).**
> Under `autonomy: auto` (the default) a run auto-PASSes once the evidence is
> complete — every test green, the convergence loops dry, and **no residue**
> (security · concurrency · architecture) — recording it as *auto-resolved* with
> the named run as accountable owner: an explicit PASS, not a skip. **Security is
> always a HARD-STOP and is never auto-passed.** Under `autonomy: conservative`,
> or whenever residue is found, this phase is **human-led** and the checks below
> are the human's.

## Before you build — declare the build expectations

Fill the §6 **Build expectations** block BEFORE you start Build: the OBSERVABLE outcomes a
correct build must produce, derived from §2 SCENARIOS + §3 CONTRACT. At this gate, confirm each
one against real evidence (the `confirmed by` column) — so verify proves the build is *correct*,
not merely that the suite is green. An expectation with no evidence is not yet verified; passing
tests on inputs you thought of never substitute for the outcome you promised.

## Part one — confirm the evidence

- [ ] All tests pass.
- [ ] Coverage did not decrease.
- [ ] No test or contract was altered during build.
- [ ] Every §6 Build expectation is confirmed by real evidence (not just a green test).

If any is false, stop and return to Build — there is nothing to verify yet.

## Part two — check what tests miss

- **Concurrency/timing** — is it correct when two run at once? (Tests run serially
  and miss races.) This is usually the single most important check.
- **Security** — exposed secrets, injection openings, unexpected/invented
  dependencies. A security finding is always `HARD-STOP`, never a waiver.
  Writing ANY note on this line means the gate escalates to the human — and
  start it with `NOTE` or `⚠` so `add.py audit` can see it: a marked security
  note reviewed by the auto-gate is an audit finding (`unescalated_security_note`).
- **Architecture** — does it respect layering/dependency rules in CONVENTIONS.md?

## Part three — the deep check (do not skim)

Green tests prove behavior on the inputs you thought of. They do not prove the change
is *wired in*, nor that you did not leave a dead end behind — and for a non-coding change
they prove nothing about whether you actually *read* the thing you signed off. So one more
requirement, every gate:

Deep check — do not skim. If the task produced code, record that every new symbol is
referenced (wiring) and that no new dead/unused code was introduced. If it produced prose
or non-code, record a semantic read — what you read in full and what it confirmed. Which
path applies is the resolver's judgement; the engine never classifies.

Record it in the §6 **Deep checks** block — where each new symbol is called (a reference
search), the dead-code scan result, or the prose you read in full and what it confirmed.
An unfilled Deep checks block is a **shallow verify**, not a PASS.

## Part four — was the green earned?

A green suite proves the tests pass — not that the build EARNED them. Three judgment cheats
pass the unchanged suite without earning it: src overfit to the test fixtures (special-cased
to the literal inputs, not the general behavior §1 asked for), vacuous asserts (tautological —
green even against an empty implementation), and real logic stubbed away (the function returns
a constant the tests happen to accept). These cheats are invisible to the mechanical tamper
tripwire, which only sees edited files. Score them with an adversarial refute-read: an
independent reviewer — a subagent under `autonomy: auto` is recommended, the engine never
spawns one — prompted to argue the green was NOT earned from outside the build context. This
is the verify-gate, whole-suite specialization of run.md's adversarial verify (see run.md), not
a new discipline. A confirmed earned-green failure is HARD-STOP-class: never auto-passed, never
RISK-ACCEPTED — but a first cheat is a chance to redo: a confirmed cheat (mechanical tamper or a
reported earned-green failure) enters the bounded self-heal loop — it returns to build for an honest
redo, and only after the loop's cap does it HARD-STOP to the human (the loop lives in run.md).

## Record exactly one outcome (no silent pass)

When you present this gate to the human, open with the ARC (goal · done · plan) per
`report-template.md`, render the gate DECISION as a guided choice (the recommended pick + described alternatives), and reconcile its FLAGS with `add.py report --decide`'s open-item count
before the ask — per that file's reconcile rule (verify is where a flag-vs-digest mismatch bites).

| Outcome | When |
|---------|------|
| `PASS` | all checks met |
| `RISK-ACCEPTED` | a **non-security** gap, with signed owner + ticket + expiry |
| `HARD-STOP` | any failing test or any security finding |

## Exit gate / Next

<exit_gate>
- [ ] Evidence confirmed, non-functional risks checked, outcome recorded — a person approved, or
  (under `autonomy: auto` with no residue) the run auto-resolved as the accountable owner.
</exit_gate>

> **Advisor · Confidence** — the earned-green refute-read is the canonical adversarial spawn (advisor.md); score the verdict before you record the gate (confidence.md).

```bash
python3 .add/tooling/add.py gate PASS          # marks the task done
# or: add.py gate RISK-ACCEPTED   |   add.py gate HARD-STOP (return to Build)
```
Then read `phases/7-observe.md`. Book: `docs/08-step-6-verify.md`.
