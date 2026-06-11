# Stage graduation — propose the move to production as a roadmap, never a flip

A project does not become "production" because someone typed a new label. It graduates when
the MVP is genuinely covered AND a human-confirmed roadmap of production work exists. This guide
is the **4th scope level** — after setup (`phases/0-setup.md`), intake (`intake.md` / `scope.md`),
and the milestone loop (`loop.md`). It turns the bare `add.py stage` flip into the **final step** of
an analytics-driven, interview-led orchestration.

You (the AI) **gather and propose**; the **human confirms and judges**; the engine only counts
tallies and enforces the floor. The engine never decides that the project is "ready" — that is
judgment, and it belongs to the interview.

## The cue (what starts this)

When every milestone is `done` AND the human's stage-goal-criteria in `PROJECT.md` are all `[x]`,
`add.py status` prints:

```
  → MVP covered → propose graduation
```

That line is the trigger. Before both tallies complete, status is silent and nothing here applies
(a project with no stage-goal-criteria block behaves exactly as today — grandfathered, zero change).

## The flow

1. **Gather the analytics** — run `add.py graduation-report` (add `--json` to branch on it). It
   clusters the whole MVP loop's evidence into five labeled record-sets: open deltas by competency ·
   open RISK-ACCEPTED waivers by expiry · RETRO records · verify residue · observe-loop coverage gaps.
   It **gathers, never judges** — there is no readiness verdict to read; the records are what you
   reason from.
2. **Co-specify interview** — synthesize *"what production means HERE"* WITH the human, using the
   gathered records as the agenda (the residue to harden, the coverage gaps to monitor, the open
   deltas to consolidate). This synthesis is the judgment the engine refuses to make. Interview to real confidence —
   do not guess what "production-ready" means for this project.
3. **Draft the roadmap** — for each production outcome the interview surfaces, draft a production
   milestone with the EXISTING command and goal-gate criteria:
   `add.py new-milestone <slug> --stage production --goal "…"`, then write its exit criteria. The
   roadmap is **≥1** milestone — the hardening work itself (SLOs, rollback tests, incident runbooks)
   is what these milestones *contain*; this guide proposes them, it does not do them.
4. **Human confirms** — present the roadmap via `report-template.md`, opening with the ARC
   (goal · done · plan): the stage-graduation goal, the MVP coverage that earns the move, and the
   plan the production milestones lay out. The human accepts, edits, or declines each drafted
   milestone. No milestone is created without this; nothing advances on a draft the human has not confirmed.
5. **Flip — the final step** — only now run `add.py stage production`. Because ≥1 production milestone
   now exists, the guard passes and the transition is recorded. This is the orchestration's last act.

## The floor (what the engine enforces)

`add.py stage production` is **guarded**: it refuses with `stage_no_roadmap` (non-zero exit, state
byte-unchanged) when zero milestones have `stage: production`. The check is a **tally** — "does a
production-roadmap record exist?" — never a readiness judgment (gather-not-judge at stage level;
it mirrors the milestone goal-gate's `milestone_goal_unmet`). `--force` overrides it, preserving human
authority for grandfathered/edge cases; use it deliberately, not as the normal path.

Scope: the guard is on the `→production` **transition** only. Flips to prototype/poc/mvp are the
existing bare flip, unchanged. `add.py init --stage production` is an explicit at-creation declaration
(the same authority as `--force`), not a transition — it is out of scope of the guard by design.

## Invariants (never break these)

- **The flip is the final step**, never called outside this confirmed-roadmap path. A bare flip with
  no roadmap is the symptom this scope level removes.
- **The engine never auto-flips.** Every step here is human-confirmed; the engine gathers, counts, and
  enforces the floor — it does not advance the stage on its own.
- **The flow is continuous, not cue-reentrant.** The moment you draft the first production milestone,
  `status` stops printing the cue (the "every milestone done" tally breaks). That is expected — do NOT
  re-await the cue after drafting; carry the flow straight through to confirm and flip.

## Depth and reuse

The same orchestration serves prototype→poc and poc→mvp; **mvp→production** is the rigorous proof
case (every step at full depth + the observe loop). At lower stages, run it light — the shape is the
same, the depth is less.
