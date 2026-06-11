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

## Part one — confirm the evidence

- [ ] All tests pass.
- [ ] Coverage did not decrease.
- [ ] No test or contract was altered during build.

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

## Record exactly one outcome (no silent pass)

When you present this gate to the human, open with the ARC (goal · done · plan) per
`report-template.md`, and reconcile its FLAGS with `add.py report --decide`'s open-item count
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

```bash
python3 .add/tooling/add.py gate PASS          # marks the task done
# or: add.py gate RISK-ACCEPTED   |   add.py gate HARD-STOP (return to Build)
```
Then read `phases/7-observe.md`. Book: `docs/08-step-6-verify.md`.
