# Foundation compaction — collapse the stable tail

This is the **retrospective shrink**. `fold.md` PREPENDS new learnings (newest-first); foundation
compaction later COLLAPSES the stable tail of each foundation spec into a rolled-up settled line — so
PROJECT.md, CONVENTIONS.md, GLOSSARY.md, and MODEL_REGISTRY.md keep to one screen as the project grows.

You (the AI) **gather and propose**; the **human confirms**; you then write the settled line. This is a
**convention**, not a command — there is no `add.py compact-foundation` (the engine stays judgment-free).
It is DISTINCT from the engine `add.py compact <slug>` (the archive recovery-bundle move).

## When

At **milestone close** (after `fold.md`) or **on demand** when a spec has grown past one screen.
Compaction is a SEPARATE step from `fold.md` — never merged into the retrospective consolidation.

## Eligibility (one shared test)

An entry is compaction-eligible **IFF** its milestone is **shipped** (done/archived) **AND** it carries
**zero open residues**/deltas. An unshipped or open-residue entry is NOT eligible — leave it live
(`open-residue-version`).

## The ritual

1. **Gather** — collect the stable, shipped, zero-residue tail of the target spec (its oldest entries).
2. **Propose** — draft the per-spec rolled-up settled line (see the shapes below) and show the human.
3. **Confirm** — the human accepts or declines. No write happens without confirmation.
4. **Write** — replace the collapsed tail with ONE settled line at the BOTTOM (newest-first: live records
   stay on top, the settled line anchors at the tail), carrying a git/archive pointer.

## Per-spec rolled-line shapes (from the frozen compaction-contract)

- **PROJECT.md §Spec** — a run of stable `[folded fv N..M]` bullets → `settled fvN–fvM — <theme> (see git)`.
- **PROJECT.md §Key-Decisions** — matching shipped rows → one `| settled <dateA>–<dateB> | <N> decisions rolled | … | see git |` row at the tail.
- **CONVENTIONS.md** — a run of stable `(TAG)` learnings → `- settled conventions <range> — <N> rules (see git)`.
- **GLOSSARY.md** — a verbose, stable definition → its terse canonical line + `(rationale: see git)`. *(forward-looking — small today.)*
- **MODEL_REGISTRY.md** — superseded model rows → `Prior models: <list> (see git)`. *(forward-looking — nothing to compact yet.)*

## Preservation (every collapse)

- **Never delete** — summarize and point; a settled line is lossy on prose, lossless on traceability.
- A surviving **git**/archive pointer is mandatory (`trail-loss` if dropped).
- **OPEN residues stay** live; the audit trail is summarized-not-deleted.

## Reject codes (judgment-checked — the AI proposes, the human confirms)

- `open-residue-version` — the entry is unshipped or has ≥1 open delta/residue; leave it live.
- `trail-loss` — the collapse would drop the git/archive pointer or the audit summary.
- `wrong-order` — a record is not newest-first, or the settled line is not at the tail.

## Distinct from `add.py compact`

Foundation compaction ≠ engine `add.py compact <slug>`. It mirrors `fold.md`'s "AI proposes, human
confirms" voice and stays convention-guided — no engine command, no `add.py check` enforcement.
