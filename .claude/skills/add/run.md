# The dynamic run — executing a locked scope

Once a task's CONTRACT is frozen (phase 3), the scope is *locked*: the external shape will not move.
That lock is ADD's autonomy decision point — below it code is disposable; above it nothing breaks. This rubric
covers what runs on the far side of the decision point: the **build->verify half, executed as a dynamic,
self-improving run** instead of a manual, sequential build. The human-led **specification bundle** (Specify · Scenarios
· Contract) still owns *direction*, but v7 compresses it to a **single human approval at the decision point**
(see "The specification bundle" below) — the AI drafts the whole bundle, a human approves it once.

> **Self-improving = within-run convergence + emit v5 deltas** — same definition as v5: tracked,
> evidence-backed, never autonomous training. The run converges in-turn AND feeds the human-gated
> consolidation loop (`deltas.md` · `fold.md`). The engine stays judgment-free: this is a rubric, not `add.py`.

## The specification bundle (v7)

The specification bundle used to be three separate approvals — Specify, then Scenarios, then the Contract
freeze. v7 compresses it to **one**. From the user's input the AI **drafts the whole specification bundle in one pass** — the Spec, the Scenarios, the Contract, and the failing Tests — and presents it together. The
human gives **one approval, at the frozen contract** (the decision point). That single approval is the green light
for the self-driving run.

Why one approval and not zero: the contract freeze is the autonomy decision point, and the decision point **stays human**.
The AI *drafts* the contract but never *freezes its own* — a person approves the frozen shape before any
auto-run touches code. This is exactly what keeps "never self-gate a human-led gate" true under an auto
default: the one gate that remains is human. Drop it to zero and the AI would freeze the interface it
then builds against and self-gate the result — the circular trust v6's dogfood warned against.

What the human is actually approving in that one gate: that the drafted Spec captures the real intent,
that the Scenarios cover the cases that matter, and that the Contract shape is the one to freeze. Reject
any part and the bundle goes back to draft — that is backward-correction (principle 4), not failure.
Approve, and the run begins. The decision-point guide (`phases/3-contract.md`) carries the
**freeze review checklist** — seven lines that walk the human through exactly this, ⚠-first.

**The lowest-confidence flag — aiming the one approval.** A single approval over a whole bundle is easy to
grant without reading. So the AI presents the bundle **lowest-confidence first**: of everything it is asking the human
to freeze, it names the **1–2 points most likely to be wrong**, tagged by part
(`⚠ [spec|scenario|contract|test] … — because …; if wrong: …`), each with *why* it is uncertain and
*what it costs if wrong*. The §1 assumptions feed it, but a flag may equally point at an uncovered
scenario or the contract shape. If nothing is materially uncertain, the AI still names the single
biggest risk, however small — never a blank "none". Honest about its limit: the flag records that the
human approved with the soft spots **in front of them**, eyes open; it makes a real review cheap and a
lazy one visibly negligent, but it cannot *force* engagement — and the AI never asserts that the human
engaged when it cannot know (a self-asserted gate would just move the unread approval one level up). Closing
that enforcement gap is the job of a CI checker, not of prose.

## When the run begins — the scope-lock trigger

The trigger is the **frozen contract**, nothing else. A run may start only when:

- §3 CONTRACT is marked `FROZEN @ vN` (the shape is fixed), AND
- §4 TESTS exist and are RED for the right reason (the target the run drives to green).

No frozen contract -> no run: you are still inside the specification bundle, and starting early is the
forward-skip the flow forbids. The lock is what makes autonomous execution *safe* — the AI cannot
drift the interface, because the interface is frozen above it.

## The change scope — what the run may and may not touch

<constraints>
A locked run has a hard boundary. It MAY:

- write and rewrite **code** (`src/`) — code is disposable below the decision point;
- drive the **tests** to green WITHOUT weakening them (a weakened test is a method violation);
- gather **evidence** for the verify gate (test output, non-functional review).

It MUST NOT:

- change the **frozen contract** or the **locked scope** — a discovered gap is backward-correction:
  the run STOPS and hands back to a human to reopen Specify (principle 4). The run never re-locks
  scope on its own.
- weaken, delete, or skip a **test** to make the build pass (that inverts the method).
- touch the **specification-bundle artifacts** (§1–§3) except to halt and escalate.
</constraints>

Crossing the boundary is not a fast run; it is an unverified one. When the run hits something only the
specification bundle can resolve, it stops — and that stop is the loop working, not failing.

## The dynamic run — fan-out and in-run convergence

Once it starts, the run does not crawl the build in one linear pass. It **fans out** the independent
work — several build attempts, several test-fix loops, several checks at once — and then **converges**
on a trustworthy result with three loops:

- **loop-until-dry** — keep hunting failures and gaps until N consecutive passes find nothing new.
  Stopping at the first green is how defects survive; the run stops only when the well runs dry.
- **adversarial verify** — for every "done" claim, an independent skeptic tries to REFUTE it. The
  claim survives only if it withstands refutation, not because one pass looked plausible.
- **completeness-critic** — a final pass that asks "what did we NOT cover — a scenario, a non-functional risk,
  an unstated assumption?" Whatever it finds re-enters the run.

The run ends only when the loops go dry AND the auto-gate's evidence is satisfied. This is the run
**self-improving within the turn** — the same convergence the foundation loop runs across milestones,
compressed into one task.

## The automated quality gate

<constraints>
The verify gate may be resolved by **evidence** rather than by a person — when the evidence is
sufficient and the result is recorded (principle 7, reframed: an automated, recorded pass is an
explicit pass, not a skip).

- **Auto-PASS requires ALL of:** every test green; coverage not decreased; no test weakened and no
  contract edited; the convergence loops dry; the completeness-critic found nothing open; and the
  deep check below recorded.
- **The deep check (every gate, no skim).** Deep check — do not skim. If the task produced code, record
  that every new symbol is referenced (wiring) and that no new dead/unused code was introduced. If it
  produced prose or non-code, record a semantic read — what you read in full and what it confirmed.
  Which path applies is the resolver's judgement; the engine never classifies. An unfilled deep check is
  a **shallow verify**, not an auto-PASS — evidence the work is wired, not merely plausible.
- **Always escalates to a human (never auto-passed):** any **security** finding (HARD-STOP, always);
  a **concurrency**/timing risk the tests cannot exercise; an **architecture**/layering violation; and
  any failing test. These are the residue principle 2 names — automation cannot judge them.
- **Records exactly one outcome** (no silent skip): `PASS` (evidence + the named run as accountable
  owner) · `RISK-ACCEPTED` (non-security, signed) · `HARD-STOP`. The record states it was
  auto-resolved, names the run, and lists the residue checks performed.

The auto-gate NEVER writes a human signature it did not get. An auto-PASS is logged as *auto-resolved*,
honestly — the line between a pass and a skip is the recorded outcome, not a forged name.
</constraints>

## The bounded self-heal loop — a confirmed cheat returns to build

The auto-gate trusts evidence; but evidence can be **gamed**. A build can make the unchanged red suite
pass without EARNING it — a test or the frozen contract edited after the red run, src **overfit** to the
fixtures, **vacuous** asserts, or real logic **stubbed away**. That is a **confirmed cheat**, and a cheat
is **HARD-STOP-class**: never auto-passed, never RISK-ACCEPTED-waived (like a security finding). But a
first cheat is not yet a stop — it is a chance to redo honestly.

So a confirmed cheat enters a **bounded self-heal loop**: the engine returns the task to **build** for an
honest redo, **counts** the attempt, and **caps** it. After **3** honest re-build attempts a fourth
confirmed cheat forces a **HARD-STOP that escalates to the human** — never an auto-PASS, never an unbounded
loop. The engine COUNTS, CAPS, and ESCALATES; the **agent** does the honest re-build (the engine never
auto-fixes). The counter is **monotonic** — it never auto-resets, so the cap cannot be cleared by
re-crossing a phase; only an honest build (no cheat) escapes the loop, and an honest build PASSes even at
the third attempt (the cap bites a *continued* cheat, never a recovery).

Two findings enter the loop:
- **mechanical** (enforced) — the tamper tripwire (`tamper-tripwire`): at the gate the engine re-hashes the
  red test files + the frozen §3 against the `tests→build` snapshot; any divergence is a cheat, routed to
  the loop before any completing outcome is recorded.
- **semantic** (honor-system, necessary-not-sufficient) — the **adversarial refute-read** (`6-verify.md`):
  an independent reviewer argues "the green was NOT earned" and, on a confirmed overfit/vacuous/stub, the
  agent reports it with `add.py heal <slug> --reason "<finding>"`. The engine cannot SEE a judgment cheat,
  so this entry is the agent's honest report — the human verify gate stays the real backstop.

The mechanical entry returns-to-build automatically at the gate; the `heal` verb is how a *reported* cheat
enters the same bounded loop. Either way: ≤3 honest redos, then escalate. A gamed green never ships.

## Emitting deltas — feeding the foundation back

The completeness-critic does not discard what it finds. Every gap, surprise, or convention that helped
or hurt becomes an **`open` lesson learned** in the task's OBSERVE block, in the `deltas.md` grammar,
tagged by competency:

- a finding the run FIXED but that taught the foundation something (a missing scenario -> `TDD`);
- a finding the run could NOT fix — a residue escalation -> a delta AND the escalation to a human.

These `open` deltas feed v5's human-gated consolidation (`fold.md`) at milestone close: the run emits `open`;
the human consolidates. That is the loop closing — **v6 run -> v5 foundation** — so a dynamic run sharpens the
five competencies instead of letting its findings evaporate at end-of-run.

## The autonomy level

<constraints>
How much a run may auto-gate is a **per-scope setting**, not a global switch (principle 5: trust is
earned per scope). A task declares its level in its `TASK.md` header:

```
autonomy: manual | conservative | auto
```

An ordered ladder — `manual < conservative < auto` — declared once in the header and reviewed at the freeze:

- **auto (the seeded default)** — the run may auto-PASS when the evidence + residue checks above are
  satisfied. Security still always escalates. This is the default starting point: a frozen contract
  flips the task into a self-driving run that converges and auto-gates on evidence.
- **conservative** — the deliberate *lowering*: the run does all the work and converges, but STOPS at
  the verify gate for a human. Auto-PASS is disabled. Choose it wherever evidence is thin or risk is high.
- **manual** — the strict floor: the human owns the verify gate and the engine never auto-resolves
  (behaviourally the conservative floor with the explicit "I drive this decision; the AI proposes only"
  name). Choose it for the highest-stakes scope; like `conservative`, it satisfies the high-risk guard.

> **v7 reversal (recorded, not hidden).** Earlier the default was `conservative` and `auto` was the
> earned exception; v7 flips this — `auto` is the default, `conservative` is the deliberate lowering.
> What did **not** change is principle 5: the autonomy level is still **per-scope**, and it still lives in the
> `TASK.md` header, and you still lower it anywhere risk demands. Only the starting point moved.

**The high-risk guard — `auto` is refused where it matters most.** The autonomy level is not a blank cheque. On a
**high-risk or method-defining scope** — anything where a wrong-but-plausible result is expensive or
hard to reverse (auth, money, data-loss paths, the method/trust-layer itself) — `auto` must be lowered
to a stricter rung — `conservative` or `manual`; leaving it at `auto` there is the reject code
**`unguarded_high_risk_auto`**. This
closes the v6 dogfood gap, where the whole milestone ran at `auto` on the riskiest possible
scope (defining the method) with no friction. The default is `auto` *for ordinary, well-tested scope*;
high risk still earns a human gate.

Judging *what* is high-risk stays human — the scope declares **`risk: high`** in the same `TASK.md`
header where the autonomy level lives, reviewed at the freeze like every header line (the engine never
classifies scope). **Since v14 the guard is mechanical for the declared case:**
the engine refuses the declared combination — `add.py gate` will not complete (`PASS`/`RISK-ACCEPTED`) a task whose header
carries `risk: high` without a lowered level — `conservative` or `manual` (error `unguarded_high_risk_auto`; `HARD-STOP`
always records — stopping is never blocked), and `add.py audit` flags the same code on a finished
record whose header was tampered or whose GATE RECORD reviewer is the auto-gate — which CI enforces
(audit-ci). The honest limit mirrors the audit's: an **undeclared** high-risk scope passes; declaring
is the human decision point, the engine enforces what was declared.

**Autonomy is earned by goal-clarity — the auto-ready goal.** The level decides *who* resolves Verify;
an **auto-ready goal** decides whether a self-verifying run is even *meaningful*. A milestone goal is
auto-ready when **every exit criterion cites a verifier** — `(verify: <test | command | metric>)` — so the
run can check its own result against the goal without human judgment. `add.py check` raises a
`goal_not_auto_ready` WARN (never red, the active milestone only) while criteria are uncited, and `status`
prints a `goal-ready:` line every session. It **measures, never blocks** — it changes neither the freeze
gate nor the autonomy level. The lint forces a citation slot per criterion (raising the floor) but cannot
prove the citation is honest (`(verify: it works)` passes) — that judgment stays the human's.
</constraints>
