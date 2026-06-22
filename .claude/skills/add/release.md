# Release — cut a versioned ship, never an unwatched flip

A project does not "release" because someone bumped a number. It releases when one or more
**closed milestones** are bundled into a versioned, user-facing cut whose notes are evidence-backed,
whose risk is disclosed, and whose behaviour is then watched. This guide is the **5th scope level** —
after setup (`phases/0-setup.md`), intake (`intake.md` / `scope.md`), the milestone loop (`loop.md`),
and stage graduation (`graduate.md`). It is a different verb from each of them:

- a **milestone** is *feature-complete and consolidated*; a **release** is *shipped + watched*.
- **graduation** changes the project's *rigor* (mvp→production); a **release** ships a *version*.
  The axes are orthogonal — you cut releases at every stage (prototype preview · mvp beta · prod GA).

So a release is its own scope level: it bundles **≥1** closed milestone, and it may bundle several.
You (the AI) **gather and propose**; the **human confirms and judges**; the engine records the cut
and enforces a floor — it **never tags, publishes, or deploys** (the same stance as "the engine never
renders" in `design.md` and "never spawns" in `advisor.md`). The outward act is the human's.

## The cue (what starts this)

When ≥1 milestone is `done` AND archived AND not yet attributed to a release, `add.py status` prints:

```
  → releasable: N milestone(s) closed since last release
```

That line is the trigger. It is a **tally** over unreleased-but-archived milestones — never a
readiness judgment. It is silent until the first such milestone exists (a project that has never
released, or has released everything, sees nothing here — grandfathered, zero change).

## The flow

One arc, seven steps: **cue → gather → draft notes → readiness floor → human confirms → cut → watch.**

1. **Gather the release inventory** — run `add.py release-report` (add `--json` to branch on it). It
   clusters the cut's evidence into labeled record-sets: the closed milestones since the last release ·
   their **consolidated deltas** (the "what changed" record) · open RISK-ACCEPTED waivers riding into
   the release · any open **security HARD-STOP** (a blocker) · the §2 scenarios to take live as monitors.
   It **gathers, never judges** — there is no readiness verdict to read; the records are what you reason from.
2. **Draft the release notes** — write a [Keep a Changelog](https://keepachangelog.com/) entry **from the
   consolidated deltas + each milestone's goal** (reuse — those consolidated deltas ARE the changelog source;
   you are not inventing the story, you are surfacing what the foundation already recorded). Group Added / Changed /
   Fixed; name the headline capabilities concretely. Propose the **semver bump** — breaking→MAJOR,
   feature→MINOR, fix-only→PATCH — and let the human confirm it (the version is a decision, not a default).
3. **Meet the readiness floor** — before the cut the engine enforces a floor (see below): the suite is
   green, **zero** open security HARD-STOP, and every RISK-ACCEPTED waiver shipping in this release is
   signed AND disclosed in the notes. A security finding is a HARD-STOP here exactly as in verify —
   never auto-passed, never shipped silent.
4. **Human confirms the cut** — present via `report-template.md`, opening with the ARC (goal · done · plan):
   the version you are shipping, the milestones + evidence that earn it, and the rollout + watch plan that
   follows. Render the cut as a guided choice — the recommended version + its described alternatives
   (per `report-template.md`). Show the drafted notes, the version, and the waivers being shipped. The human approves once
   (the decision point) — never pre-stamped; you surface a summary to decide on, not the artifact itself.
5. **Cut — record the marker** — only now run `add.py release <version> --notes <file>`. The engine
   **records**: it prepends the CHANGELOG entry, stamps one append-only row (newest-first) in
   `RELEASES.md` (date · version · milestones · waivers shipped · evidence), and attributes the bundled
   milestones to this version (so the cue stops firing for them).
6. **Ship — the human's outward act** — the engine has recorded the cut; the **human runs the tag /
   publish / deploy** (`git tag`, `npm publish`, the deploy pipeline — tool-agnostic, whatever this project
   ships through). The tag is the human-gated trigger, exactly the dogfooded recipe today. The engine never
   performs it: design-for-failure lives in the pipeline the human owns, not in the method tool.
7. **Watch — re-enter observe at the release scope level** — the §2 scenarios become live monitors for the
   *released* version; live-registry / deploy confirmation is post-cut **evidence**, not a unit test. A
   regression found in the wild re-enters at Specify as a **change request** → a narrowed **PATCH hotfix
   release** (this same flow, scoped to the fix). Release is not the finish line — it is where the most
   reliable information appears (`phases/7-observe.md`).

## The floor (what the engine enforces)

`add.py release <version>` is **guarded** — it refuses (non-zero exit, state byte-unchanged) on:

<reject_codes>
- `release_security_open` — an open security HARD-STOP exists. The non-negotiable; a security finding is
  never shipped. Resolve it (a change request back to Specify) before the cut. `--force` does NOT override this.
- `release_tests_red` — the suite is not green. Evidence, not a plausible diff, is what a release ships on.
- `release_no_closed_milestone` — nothing new since the last release. The cut is a no-op; do **not** bump.
- `release_undisclosed_waiver` — a RISK-ACCEPTED waiver rides into the release but is absent from the notes.
  Disclosure is the floor: a shipped risk the user can't read about is a hidden risk. Add it to the notes.
</reject_codes>

`--force` preserves human authority for grandfathered / edge cases (e.g. a first release of a brownfield
adopt), mirroring `stage --force` — but it never overrides `release_security_open`. Use it deliberately,
not as the normal path.

## Invariants (never break these)

- **The engine records; the human ships.** `add.py release` writes the CHANGELOG + ledger + attribution;
  it never tags, publishes, or deploys. The outward act stays human-owned and tool-agnostic.
- **Security is a HARD-STOP at the cut**, not just at verify. No `--force`, no waiver, no exception.
- **Notes draw from consolidated deltas** — release after `fold.md` has run, so the changelog surfaces
  consolidated learnings, not raw open lessons. The lifecycle order is one line:
  `milestone-done → fold → compact → archive → (repeat ≥1×) → release → watch`.
- **The ledger is append-only (newest-first)** — like §Key Decisions, a release row is never rewritten;
  a superseded or yanked version is recorded with a new row, never edited away.
- **A release bundles, it does not equal.** One version may attribute several milestones; never force a
  release per milestone.

## Depth and reuse

The shape is constant; the depth follows the stage (read it from `add.py status`):

- **prototype / poc** — a one-line preview note + a tag; no deploy ceremony. The point is feedback, not GA.
- **mvp** — full notes + tag + a guarded publish; watch the headline scenarios.
- **production** — every step at full rigor: notes + tag + deploy behind a rollback-tested pipeline +
  live scenario monitors + error-budget watch. The hotfix path (step 7) is first-class here.

## Worked example (this method's own 1.5.0)

The repo already runs this by hand. The `udd-design-loop` milestone closed (4/4) and consolidated into
`foundation-version 33`; the human then drafted the `## [1.5.0]` CHANGELOG entry from those deltas,
bumped the three version sources in lockstep, and the forward-pinned `test_release_1_5_0.py` asserted
in-repo readiness (versions agree · changelog lineage survives · feature anchors named · engine
untouched). The cut itself — the `git tag` that triggers the npm/PyPI publish — stayed human-gated, and
the live-registry confirmation was gathered *after* the tag as verify evidence, never a unit test. This
guide makes that ritual first-class: `release-report` gathers it, the floor enforces it, `add.py release`
records it, and the human still owns the tag.
