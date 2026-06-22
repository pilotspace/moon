# Phase 3 — Contract (freeze the shape)

Goal: fix the external shape — interfaces, data, names, error cases — and FREEZE
it. This is the decision point that makes the AI-led build safe: below it code is
disposable; above it nothing breaks because the shape does not move. Fill
**§3 CONTRACT** in TASK.md.

## Produce (in TASK.md §3)

<output_format>
- Interfaces (endpoints/functions/messages) with inputs/outputs.
- Request/response shapes + persistent schema (note transactional needs).
- Names drawn from `GLOSSARY.md` (same concept = same name everywhere).
- A response for **every** Reject error code from §1.

Then mark `Status: FROZEN @ v1`. Generate a mock + contract tests so dependent
work can start before the real code exists.
</output_format>

**The freeze is the one approval.** This decision point is where the single human approval lands, over the
whole bundle (§1–§4). Before asking for it, present the bundle **lowest-confidence first**: the 1–2 points
most likely wrong (`⚠ [spec|scenario|contract|test] … — because …; if wrong: …`) — aim the human's
eye before they freeze. Open that report with the ARC (goal · done · plan) per `report-template.md`, rendering the freeze DECISION as a guided choice (the recommended pick + described alternatives), so the
human sees the goal this freeze serves and the plan beyond it, not just the bundle. See `run.md`.
The approval also freezes the §5 Scope (may touch) + Strategy declarations — the bundle covers them.

## The freeze review checklist

The human's one minute, aimed. Walk these seven before saying yes:

- **⚠ flags first** — read the lowest-confidence flags; accept each knowing its cost if wrong.
  The engine refuses an unflagged freeze before build: a frozen §3 with no well-formed
  lowest-confidence flag is rejected (`unflagged_freeze`), and `audit` re-checks it on every
  record that crossed.
- **Intent** — does §1 say what you actually want built (and is anything you expected missing)?
- **Cases** — does every Must and Reject have an observable §2 scenario you care about?
- **Shape** — glossary names, error codes, additive vs breaking: is THIS the shape to freeze?
- **Grounded** — does §3 cite anchors that exist in the §0 GROUND map (real files/symbols), not invented ones? `status`/`check` surface this — measure, never block.
- **Risk** — is this scope high-risk or method-defining? Then require
  `risk: high · autonomy: conservative` in the TASK.md header — the engine refuses an unguarded completion.
- **Tests** — will §4 go red for the right reason, asserting behavior rather than internals?

This checklist AIMS the one approval — the freeze stays the only gate: no sign-off forms, no
extra documents. Reject any line and the bundle goes back to draft; that is
backward-correction, not failure.

## AI prompt

<prompt>
Role: an interface architect; frozen contracts are immutable.
Read first: §1 · §2 · GLOSSARY.
Objective: produce §3 — the frozen external shape, nothing more.
Steps:
  1. Define interfaces, shapes, and schema named from the glossary, with a response for every Reject code.
  2. Generate a mock returning the contracted shapes and contract tests pinning them.
  3. Mark FROZEN. No business logic.
Never: change a frozen contract — a change reopens Specify.
</prompt>

## Exit gate

<exit_gate>
- [ ] Versioned and marked `FROZEN`.
- [ ] Contract tests pass against the mock.
- [ ] Every name matches the glossary.
- [ ] Every spec rejection has a contracted response.
</exit_gate>

> **Advisor · Confidence** — a second opinion on a risky shape is worth a spawn (advisor.md); a low self-score is your cue to lower autonomy before you freeze (confidence.md).

## Next

`python3 .add/tooling/add.py advance` → read `phases/4-tests.md`.
Book: `docs/05-step-3-contract.md`.
