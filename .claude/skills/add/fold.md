# Consolidating lessons — how the foundation self-improves

This **closes the loop**. `deltas.md` lets a task EMIT lessons (`open` lessons learned in its
OBSERVE phase); the retrospective consolidation gathers the confirmed ones and writes them into a
**versioned foundation**, so `DDD · SDD · UDD · TDD · ADD` sharpen across milestones instead of drifting.

Since v3 this is mechanized by a command — `add.py fold` — but the command stays **judgment-free**:
it only ever TRANSCRIBES each lesson's OWN captured text into its routed home and bumps the version.
It NEVER composes or merges prose, and it never **self-approves** a consolidation — running the
command records the human's confirmation, it does not replace their judgment. Deciding WHICH lessons
to keep, and later polishing the raw transcribed bullets into lean one-screen prose, remain the
human's work — the latter via the **compaction door** (`compact-foundation.md`).

## When to consolidate

At **milestone close** (the natural "version bump to the foundation"), or **on demand** when open
lessons have piled up. One run of `add.py fold` is ONE consolidation session: it bumps
`foundation-version` exactly once and stamps every lesson it resolves with that one new version.

## The ritual

1. **Gather** — `add.py deltas` reads every task's OBSERVE block for lessons still `open` (by the machine heading).
2. **Confirm** — decide which to keep; a lesson you do NOT want is marked `rejected` and left in place
   (see below). Running `add.py fold` over the rest IS your confirmation — the command needs no separate flag.
3. **Write** — `add.py fold [--task <slug>] [--comp <TAG>]` performs the mechanical write atomically:
   - flips each selected `open` lesson to `folded` and stamps it `[folded foundation-version N]`;
   - transcribes each lesson VERBATIM as one bullet at the TOP (newest-first) of its routed section (below);
   - prepends one row to §Key Decisions (date · what · why · outcome);
   - bumps `foundation-version` by one.
   Validate-all-then-write: if any precondition fails the command writes NOTHING (the tree stays byte-identical).
4. **Propose & polish** — the transcribed bullets are RAW; afterward you consolidate/merge them into
   lean prose (append-only, newest-first) via the compaction door. The engine never does this editorial step.

## Consolidation routing (every competency has a home)

| competency | consolidates into | how |
|------------|-----------|-----|
| `DDD` | `PROJECT.md` §Domain | a transcribed bullet at the top |
| `SDD` | `PROJECT.md` §Spec | a transcribed bullet at the top |
| `UDD` | `PROJECT.md` §Users | a transcribed bullet at the top |
| `TDD` | `CONVENTIONS.md` §Method learnings | a transcribed bullet (no PROJECT.md section — it is the engine) |
| `ADD` | `CONVENTIONS.md` §Method learnings | a transcribed bullet (likewise the engine) |

**Every** consolidation — whatever the competency — ALSO prepends one row at the TOP of `PROJECT.md`
**§Key Decisions** (newest-first) (date · decision · why · outcome): the universal, auditable trail of
what the foundation learned.

## Status transitions & version

- on **confirm**: the lesson moves `open` → `folded` (its text transcribed at the top of the routed target, newest-first).
- on **decline**: the lesson moves `open` → `rejected` and is **left in place** — never deleted —
  so "we considered this and chose not to act" stays auditable.
- a consolidation is **append-only (newest-first)**: it PREPENDS new bullets/rows at the top and never
  silently rewrites existing foundation text — EXCEPT via the recorded **compaction door** (`compact-foundation.md`):
  eligible (shipped + zero open residues) stable entries collapse upward into a rolled-up settled line at the tail.
- each `add.py fold` run **bumps** the `foundation-version:` marker in `PROJECT.md` by one (monotonic int).

## Reject codes (the command is fail-closed; validate-all-then-write)

<reject_codes>
- `no_open_deltas` — nothing is `open` in the selected scope. The session is a no-op; the version is NOT bumped.
- `missing_route_section` — a lesson routes to a foundation section that does not exist (e.g. a `UDD`
  lesson with no `## Users` section). Fail-closed: add the section header, then re-run. Nothing is written.
- `no_foundation_version` — `PROJECT.md` carries no parseable `foundation-version:` marker to bump.
</reject_codes>

The convention-era codes `unconfirmed_fold` and `unroutable_delta` are **retired** (bridged here, not
silently dropped): invoking `add.py fold` IS the confirmation, so `unconfirmed_fold` no longer applies;
and routing is total over the five competencies, so a missing destination surfaces as the more precise
`missing_route_section` rather than `unroutable_delta`.

## Worked example (from this repo's own history)

The `competency-deltas` task closed its OBSERVE with two lessons — the homeless ones, `TDD`/`ADD`,
which have no PROJECT.md section:

```
- [ADD · open] dogfood .add/tooling template can silently diverge from canonical (evidence: md5 mismatch this build)
- [TDD · open] structural tests guard canonical artifacts but not their dogfood twins (evidence: scope-loop note + this build)
```

At the next consolidation the human keeps both and runs `add.py fold`. Routing transcribes each into
`CONVENTIONS.md` §Method learnings, prepends a §Key Decisions row, flips them to `folded` with a
`[folded foundation-version N]` stamp, and bumps `foundation-version` 1 → 2 — all in one atomic write.
The two competencies the foundation never tracked before now have a home — which is exactly why v5
routes `TDD`/`ADD` to `CONVENTIONS.md`.
