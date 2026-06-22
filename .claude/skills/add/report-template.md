# Chat reports — the decision-point template (for the AI, not for add.py)

The engine renders artifacts (`report`, `report --decide`, `status`); this file
governs the CHAT MESSAGE you wrap around them. The digest is the artifact BEHIND
your presentation, never a replacement for it — and your prose is never a
replacement for the digest.

Use it every time you report at or near a decision point: an intake proposal, a
bundle approval, a verify gate, a task completion, a milestone close.

## The decision arc — rendered first, above the report blocks

Every report at a human gate opens with the **ARC** — three labelled lines that
place the decision in the work's whole arc, so the human confirms with sight of
where this is going, not just the step in front of them. Render it first, then a
separator, then the report blocks below:

```
ARC  goal: <the milestone / project goal this decision serves>
     done: <proven progress — tasks done · exit-criteria met · what this gate proves>
     plan: <this gate → the next step → the goal>
```

- **goal** — the milestone or project goal the decision serves, read from the
  `m-goal` line in `add.py status`; never re-typed from memory.
- **done** — proven progress only: exit-criteria met/total and tasks done from
  the rollup, plus what this gate proves. An honest fact, never a hope.
- **plan** — this gate → the next step → the goal, mirroring the rollup's
  `DECIDE NEXT` line.

The arc is required at every human gate: **baseline-lock · contract-freeze ·
verify · intake · scope · milestone-close · graduation**. The three labels stay
constant; their content adapts to the gate. The arc is presentation only — it
adds no gate and changes no PASS / RISK-ACCEPTED / HARD-STOP / freeze outcome.

Its facts are engine-sourced, exactly like EVIDENCE below: goal = `m-goal` ·
done = exit-criteria met/total + tasks done · plan = `DECIDE NEXT`. If your arc
and `add.py` output disagree, the engine wins — fix the arc, not the engine.

### Per-gate examples — one shape, gate-specific content

- **verify** — `goal:` ship the decision arc · `done:` report-arc tests 6/6
  green, gate ready · `plan:` PASS this gate → wire the arc into every gate → goal.
- **contract-freeze** — `goal:` … · `done:` bundle drafted, lowest-confidence
  flag surfaced · `plan:` freeze §3 → build → goal.
- **milestone-close** — `goal:` … · `done:` exit-criteria 3/3 met, all tasks
  done · `plan:` close → archive → the next milestone.
- **intake** — `goal:` the sized request · `done:` classified new-major,
  rationale stated · `plan:` create the milestone → first contract → goal.

## The report blocks, in order

The blocks below are the CORE set, rendered in order at every decision-point
report. Render every one (write "none" rather than dropping a block); add MORE
blocks when a specific report needs them (see "Beyond the core blocks" below).

```
SUMMARY   one line: intent + target + where we are + what we done
DECISION  what you need from the human (or "none — FYI") — exactly one
FLAGS     lowest-confidence first: why + cost-if-wrong
DECIDED   highest-confidence first: the autonomous calls you made + why each was safe
EVIDENCE  small table: tests · gates · parity · check — engine-sourced
NEXT      the recommended next actions, ranked (top ▶ highlighted, bolded) + what each unlocks
```

1. **SUMMARY** — one line carrying intent + target + position, e.g.
   "v13 task 2/3 — tests-declared-fallback is green, gate PASS." The reader
   knows where they are before they read anything else.
2. **DECISION** — the question the human must answer, presented as a **guided
   decision**: lead with the one **recommended pick** marked `▶ … (recommended)`,
   then its **1–3** described alternatives, every option carrying a one-line
   description (see "guided choice" below). Exactly one decision per report, or an
   explicit "none — FYI". If a decision exists, ask it after everything below has
   been shown (show-before-ask).
3. **FLAGS** — lowest-confidence first, each with *why* confidence is lowest and the
   *cost if wrong*. Where TASK.md markers exist (`⚠` / `- [~]` / `- [ ]`),
   quote them verbatim and keep their document order — extraction ≠ judgment.
4. **DECIDED** — the counterpart to FLAGS: the high-confidence calls you settled
   autonomously WITHOUT a gate, highest-confidence first, each with *why* it was
   safe (the change-type or evidence that earned the autonomy). FLAGS surfaces the
   lowest-confidence calls so the human can challenge them; DECIDED surfaces the
   highest-confidence ones so the human can see what you closed without asking. "none" when you made no
   autonomous calls. NEVER list a security / residue / lowered-autonomy call here —
   those always escalate in DECISION (see the Hard rules), never auto-decided.
5. **EVIDENCE** — engine-sourced facts pasted from `add.py` output, never
   re-typed from memory. If your prose and the engine disagree, the engine
   wins: fix the engine or the data, not the sentence.
6. **NEXT** — the recommended next actions as a ranked list, the top one marked
   `▶` and each with what it unlocks. NEXT is **informational, not a second gate**:
   it shows the menu of what's next and never adds a decision — the one decision
   stays the DECISION block. Mirror the rollup's DECIDE NEXT line for the top
   action when it is right; overrule it only with a stated reason (e.g. planned
   tasks the state file cannot see yet).

### Beyond the core blocks — add more when the decision needs it

The six blocks above are the CORE set. When a specific report needs more than they
carry — a `RISK` ledger, a `DIFF`, a `SCOPE` map, a `COST` estimate — add an extra
block in the same shape (SCREAMING-CASE label · one-line intent · engine-sourced
where it can be), placed AFTER EVIDENCE and BEFORE NEXT so NEXT always closes the
report. Add a block only when it carries what the core six don't; never pad to look
thorough — and never drop a core block; render "none" instead.

### The DECISION block as a guided choice

When the human must choose, render block 2 as a **guided decision** — never a bare next step:

```
DECISION  <the question>

  ▶ <recommended option>  (recommended)
      <one-line description — what it means · what it unlocks or costs>
    <alternative option>
      <one-line description>
    <alternative option>
      <one-line description>
```

- **Recommended pick** — exactly one option carries the `▶ … (recommended)` marker; never zero,
  never two. Your `confidence.md` self-score informs which to recommend; the human overrides freely
  (a recommendation, not a default that auto-proceeds).
- **1–3 described alternatives** — only real, takeable options (no strawmen). If there is genuinely
  one path, show the single recommended step + its description — never invent filler to reach three.
- **Every option is described** — the pick and each alternative carry a one-line description (what
  it means + what it unlocks or costs); ≤1 line, no bare labels.
- **Human gates only** — render the guided choice at `[human gate]` decision points; at a
  `[you drive]` autonomous step there is no human to choose, so render none. Show-before-ask still
  holds — the described choice is the ASK, rendered after EVIDENCE.

**The ask itself** — when block 2's decision becomes a literal question component (an
`AskUserQuestion` picker or a numbered menu), compose it as the guided choice above: the
**recommended option goes first, with a `(Recommended)` suffix**, and each option's `description`
carries its one-line description. On a tool without `AskUserQuestion`, render the same shape as a
numbered/`▶` menu in chat — the convention is tool-agnostic. The question text stays a summary —
intent + what "yes" means + the flag count — pointing at the report above.

## Hard rules

<constraints>
- **Summary-first.** Never bury the decision under a task list or a diff.
- **Show before ask.** Render the artifact (digest · diff · report) before any
  approval question; the human decides on what they can see.
- **Guided decision.** At a `[human gate]`, present block 2 as a guided choice — one highlighted
  **recommended pick** (`▶ … (recommended)`, exactly one) + its 1–3 described alternatives, every
  option carrying a one-line description; never a bare next step. `confidence.md` informs the pick;
  the human overrides freely. Not at `[you drive]` autonomous steps.
- **Reconcile the count.** Before the ask, your FLAGS must reconcile with
  `add.py report --decide`'s open-item count. If your prose calls an item
  resolved while the digest still counts it open, the engine wins — fix the data
  (the TASK.md markers the digest reads), not the sentence. A report whose flag
  count disagrees with the engine is the un-transparent gate the ARC exists to close.
- **Never pre-stamp a human decision point.** Freeze / gate / lock fields stay DRAFT or
  blank until the answer returns: show → ask → stamp → advance. An artifact
  must never claim an approval that has not happened.
- **One report per decision point.** After an approval, point at the frozen artifact —
  do not re-render the whole bundle.
- **Honest scope.** "Done" means the request, not the last task: report
  "task 2/3", never "done" while approved scope remains.
- **The question is a summary, never the artifact.** Every approval ask carries
  two layers: a compact SUMMARY · DECISION · FLAGS block sits in chat
  immediately before the ask (positional), and the question text itself is a
  summary of two lines at most — intent + what "yes" means + the flag count —
  pointing at the report above (compositional). The full bundle, diff, or
  artifact lives only in the chat report; a question that re-carries it buries
  the decision.
- **NEXT is not a second gate.** NEXT lists recommended next actions (ranked, top
  `▶`); the single decision stays the DECISION block — NEXT never carries an approval ask.
- **DECIDED never holds a gate-class call.** A security / residue / lowered-autonomy
  call is escalated in DECISION, never reported as already auto-decided in DECIDED.
</constraints>
