# Scope drafting — turn a classified request into a versioned MILESTONE.md

This is the **second half of intake**. `intake.md` CLASSIFIES a request into a bucket; scope
drafting turns that classified request into a confirmed, well-formed, versioned `MILESTONE.md`
through discussion. The MILESTONE.md template is the SHAPE; this rubric is HOW to fill it well.
You (the AI) **propose**; the human **confirms before anything is created**.

## What to do per intake outcome

scope drafting honors intake's classification — it never re-sizes a request:

| intake outcome | scope-loop action | creates (after confirm) |
|----------------|-------------------|-------------------------|
| `new-major` / `sub-milestone` | draft ONE MILESTONE.md (fill the template via discussion) | 1 milestone |
| `task` | route to `add.py new-task <slug>` (it fits the active milestone) | 0 milestones |
| `change-request` | route to SPECIFY/CONTRACT of the affected task | 0 milestones |
| `split_required` | draft ALL N items as a batch in ONE pass | N milestones/tasks |

**Confirm before create is the invariant.** It holds in the one-pass split case too: "one pass"
means one drafting pass, NOT auto-creation. Nothing is written to disk — single draft or the
whole batch — until the human confirms. You propose; you wait.

## Brainstorm before you draft — co-specify at milestone level

Don't draft a MILESTONE.md from thin input. Run the same three-move co-specify as a
task's §1 (`phases/1-specify.md`) — Diverge (framings + open questions) → Converge
(draft + rank) → Validate (show flags first) — raised to milestone scope. Ask only
what moves the goal, the In/Out line, or the task list; skip what PROJECT.md settles.
Draft the WHOLE milestone before showing; nothing hits disk until the human confirms.

Diverge seeds (pick the live ones):
- **Outcome** — done means a user can do *what* they can't today? (goal sentence)
- **Edge of scope** — nearest thing assumed IN that you want OUT? (Out list)
- **Riskiest decision point** — which contract, if wrong, costs the most rework? (freeze-first)
- **Done-looks-like** — how do we SEE each outcome without reading code? (exit criteria)
- **First slice** — which task unblocks the rest? (breadth-first order)

Rank assumptions lowest-confidence first; the top 1–2 get the flag the human reads at confirm:
`⚠ <assumption> — lowest confidence because <why>; if wrong: <cost>`. Present the draft via
`report-template.md` — open with the ARC (goal · done · plan): the goal this milestone serves,
what is already covered, and the plan its task list lays out.

## Drafting a good MILESTONE.md (section by section)

- **goal** — ONE sentence, an outcome not an output ("a user can size any request", not "write
  intake.md"). If it needs an "and", it is probably two milestones.
- **Scope In/Out** — the explicit anti-creep deferral list. Naming what is OUT is as important
  as what is IN; an empty Out list usually means the scope is not yet thought through.
- **Shared decisions & glossary deltas** — cross-cutting rules every task must honor, named from
  the glossary. New terms get a glossary entry (the living documentation stays honest).
- **Shared / risky contracts to freeze first** — the decision points between tasks; name the owning task.
- **Tasks (breadth-first)** — `slug · depends-on · one line` each. Decompose by deliverable, not
  by phase; keep each task one-file-sized. Order by dependency, not by guesswork.
- **Exit criteria** — observable, and **every exit criterion maps to a declared task slug**
  (no dangling criterion). Each line answers "which task delivers this, and how would we see it?"

## Reject codes (emit `{ reject, rationale }`, create nothing)

<reject_codes>
- `not_classified` — the request has not been through intake yet. Classify it first; you cannot
  draft scope for an unclassified request.
- `dangling_criterion` — a drafted MILESTONE.md has an exit criterion that maps to no declared
  task slug. FIX the draft (add the task or drop the criterion) before proposing — never propose
  a malformed milestone. With no engine lint, you are the first check and the human is the backstop.
- `no_milestone` — intake routed the request to `task` or `change-request`; scope drafting
  creates NO milestone. Honor the classification; do not invent milestone-sized scope.
</reject_codes>

## Worked example (from this repo's own history)

Request: *"open the Interface & Intake milestone"* → intake classified it `sub-milestone` of the
live v4 self-driving theme → scope drafting produced **`.add/milestones/v4-1/MILESTONE.md`**:

- **goal**: make ADD harness-drivable and self-scoping — machine-readable state plus an
  AI-facilitated request→versioned-milestone intake loop (the real v4-1 goal, one outcome sentence).
- **tasks** (breadth-first): `machine-state-json` · `versioning-policy` · `scope-loop`.
- **exit criteria** — each maps to its task slug: `--json` emits owner+stop (← machine-state-json),
  the AI proposes a bucket with rationale (← versioning-policy), the AI drafts a versioned
  MILESTONE.md via discussion (← scope-loop). Every criterion names the task that delivers it —
  which is exactly the well-formedness rule above, checkable against the real file.
