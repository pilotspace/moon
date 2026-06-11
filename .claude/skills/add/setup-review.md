# Setup review — the one page the human signs

Autonomous setup ends at a single human gate: the **baseline approval** (`add.py lock`). Before that
signature is honest, the human needs to see *what you drafted and how sure you were* — not re-derive
it. `SETUP-REVIEW.md` is that page: every decision you made while drafting the foundation, first-scope,
and the first contract, **ordered lowest-confidence-first** so the riskiest guesses meet their eye first.

This is the setup-level analog of presenting a task's specification bundle lowest-confidence-first at the contract freeze.
The engine never reads this file — `add.py lock` is judgment-free, the signature *is* the gate (see
`setup-lock-state`). The human **reading** this page is the review; your job is to make the reading honest.

## Where it lives

Write **one** artifact at `.add/SETUP-REVIEW.md`. **Never clobber a human-edited one** — if it already
exists with hand edits, append/update, don't overwrite (the same non-clobber rule `init` applies to
living docs). It is a per-onboarding, setup-level artifact; it sits beside `PROJECT.md`, not under a task.

## The template

```markdown
# SETUP REVIEW — <project>

<stage> · <brownfield | greenfield> · drafted by <model> @ <date>

| # | Decision | Lands in | Tag | Why / Evidence |
|---|----------|----------|-----|----------------|
| 1 | <the drafted decision> | PROJECT.md \| scope \| first-contract | `guessed` | <the inference + why you had to guess> |
| 2 | <…> | <…> | `evidence-grounded` | <cite the source file/line you read it from> |

Sign: confirm in chat → the agent runs `add.py lock --by "<name>"` (typing it yourself works too)
```

Rows are numbered for reference at the gate ("row 1 is where my confidence is lowest").

## The two rules that make it honest

<constraints>
1. **Lowest-confidence-first.** Order rows by confidence **ascending**. A `guessed` row always floats above an
   `evidence-grounded` one. The point is not completeness theatre — it is to spend the human's attention
   where it changes outcomes: the top of the table is the part they actually need to challenge.

2. **Every row is tagged — `guessed` or `evidence-grounded`.**
   - `evidence-grounded` — you read it from the code/repo. **Cite the file** (e.g. `pyproject.toml`,
     `src/orders/models.py`). Brownfield onboarding (see `adopt.md`) is mostly these.
   - `guessed` — the repo was silent, so you inferred it. **State the inference and why.** Thin-greenfield
     onboarding (a near-empty repo, only the 4-lens answers) produces these. These are what the human
     must check; that is why they sit on top.

   The tag vocabulary is shared with `adopt.md` — the brownfield map tags each filled living-doc decision
   `guessed`/`evidence-grounded`, and those tags flow straight into this table.
</constraints>

## Where it ends

`SETUP-REVIEW.md` is **read-only context** for the baseline approval. You do not ask the human to approve it
field-by-field; you present it, lowest-confidence-first; they confirm in conversation, and you run the lock
with their name:

```bash
python3 .add/tooling/add.py lock --by "<name>"
```

`lock` records the lock layers and opens the build — it does **not** parse or validate this file (the
engine stays judgment-free). The review lives in the human's reading of the page, not in the tool. Make
the top of the table the truth they most need, and the one signature is informed.
