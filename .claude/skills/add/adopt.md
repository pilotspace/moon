# Adopt — map an existing repo into the foundation (silent)

When ADD is pointed at a repo that already has code, onboarding is **silent**: the code
answers the questions a greenfield interview would ask, so you read it rather than ask.
This is the **brownfield path** of setup (the greenfield path keeps the 4-lens interview —
see `phases/0-setup.md`). You fill the living-documentation files from evidence, then stop at the one
human gate: the **baseline approval** (`add.py lock`).

## The signal — and arming the gate

Enter a brownfield repo with `--await-lock`:

```bash
python3 .add/tooling/add.py init --await-lock
```

`--await-lock` does two things. It seeds an **unlocked** setup, which *arms the baseline-approval gate*
— the engine then refuses a second task, crossing into build, and recording a gate until you
`lock`. And init, being brownfield-aware, prints a line that begins:

```
brownfield: existing code detected — the `add` skill maps it into your foundation …
```

That line is your cue to run this guide. **Always use `--await-lock` for brownfield onboarding**:
a plain `init` writes no setup and is grandfathered-locked, so its gate never arms *and* the
closing `lock` below would refuse with `already_locked`. The engine only *detects* the existing
code (a mechanical fact); it never reads or fills it — interpreting it is your job.

## The silent mapping

Fill each living-doc file in `.add/` from what the code actually shows — **ask nothing**:

| Living doc | Read it from |
|----------|--------------|
| `PROJECT.md` (foundation) | the domain nouns, entry points, the README, the first milestone the code implies |
| `CONVENTIONS.md` | the languages, folder layout, naming, lint config, error style already in the tree |
| `GLOSSARY.md` | the recurring names in modules, models, and public APIs (one name per concept) |
| `MODEL_REGISTRY.md` | leave the active model record; note any AI-authored code you can detect |
| `dependencies.allowlist` | the manifests already in the repo (package.json, pyproject, go.mod, …) |

Two rules that never bend:

<constraints>
1. **Never clobber a living doc.** `init` already skips any living-doc file that exists; if a human
   already wrote `PROJECT.md`, you READ it, you do not overwrite it. Add, never replace.
2. **Tag every drafted decision `evidence-grounded` vs `guessed`.** A line you read from the
   code is *evidence-grounded* (cite the file). A line you inferred because the code was silent
   is *guessed*. The human's single baseline approval is only honest if they can see which is which —
   the guesses are what they actually need to check. (The tags feed `SETUP-REVIEW.md`.)
</constraints>

## Where it ends — the baseline approval

Brownfield onboarding draws no per-step approvals. You map the foundation, then draft the
first milestone's scope and the first task's candidate specification bundle exactly as greenfield does, and
present it all at **one** human gate. The human reviews the decisions (lowest-confidence / `guessed`
first) and confirms in conversation; you run the lock with their name:

```bash
python3 .add/tooling/add.py lock --by "<name>"
```

`lock` freezes the foundation + scope + first contract in one atomic write and opens the build.
Until it is run, the engine refuses a second task, crossing into build, and recording a gate —
so nothing is built on an unreviewed map. That gate is the only thing brownfield onboarding asks
of a human; everything before it, you did from the code.
