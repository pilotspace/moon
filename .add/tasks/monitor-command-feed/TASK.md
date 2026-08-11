# TASK: MONITOR command feed — stream executed commands, redacted, zero-cost when unattached

slug: monitor-command-feed · created: 2026-08-11 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: ground   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower the
     autonomy level to `manual` or `conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Split out of `client-identity-introspection` at its freeze (2026-08-11). That task measured the
whole identity surface against `redis-server` 8.6.1; MONITOR was carved off because it is a
different animal from the rest. Carrying the measured facts forward so this task does not re-probe:

Measured on `main` @ec0c4650: `MONITOR` -> `-ERR unknown command 'MONITOR', with args beginning
with:` — absent from dispatch AND from the `COMMAND_META` registry (`src/command/metadata.rs`,
271 entries). Redis 8.6.1 replies `+OK` and then streams the command feed.

There is NO existing infrastructure to build on: `grep -rn "monitor_feed\|MonitorFeed\|monitors"
src/` returns only `src/cluster/gossip.rs`, which is unrelated (gossip peer monitoring).

Why it was split rather than kept (the sizing argument, recorded so it is not re-litigated):
- It is a STREAM, not a reply — every other item in that task was a cold introspection response.
- It is the only one touching the per-command hot path, on every shard.
- It is the only one with a security surface: a monitor observes every other client's traffic,
  including credential-bearing arguments. Redis redacts AUTH; so must this.

Touches (files · symbols · signatures): <fill — the per-command execution points on all
production paths, the ACL gate, and wherever a server-global subscriber set would live>

Anchors the contract will cite: <fill>

### Design constraints inherited from the split decision
- The feed MUST be gated by a Relaxed atomic subscriber count checked BEFORE any formatting or
  allocation, so a server with no monitor attached pays strictly one atomic load per command.
  Anything more taxes every command for a feature almost nobody runs.
- Credential-bearing arguments (`AUTH`, `HELLO … AUTH`) must be redacted AT THE POINT OF
  FORMATTING — never emitted into a buffer and filtered afterwards.
- `MONITOR` must require the `admin` ACL category; without it, any user could read every other
  user's traffic.
- Redis's line format: `<unix.micros> [<db> <addr>] "CMD" "arg" …`.
- The three-dispatch-paths rule applies (CLAUDE.md): `handler_monoio`, `handler_sharded`, and the
  inline fast path, or the command is CI-invisible.

Consequence accepted at the split: `redis-cli monitor` fails against Moon, and the
v0-9-client-compat goal "a monitoring agent works unmodified" is NOT met, until this task lands.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: <name>
Framings weighed: <chosen> (chosen) · <alternative> · <alternative>
Must:
<must>
  - <required behavior>
</must>
Reject:
<reject>
  - <bad input / situation> -> "<error_code>"
</reject>
After:
<after>
  - <state that is true once it succeeds>
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ <the one assumption most likely to be wrong> — lowest confidence because <why>; if wrong: <cost>
  - [ ] <next assumption, ranked> — confirm or deny; never carry an open one forward
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: <short name>
  Given <starting situation>
  When <action>
  Then <expected result>
  And <what must remain unchanged>   # required for every rejection
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
<METHOD> <path>   body: { <fields> }
  200 -> { <success fields> }
  4xx -> { error: "<code>" | "<code>" }
Schema: <tables/fields touched, and access pattern>
```

Status: DRAFT
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: <e.g. 90%>
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - test_<scenario>: arrange <Given> / act <When> / assert <Then> + assert <unchanged>
</test_plan>

Tests live in: `./tests/` · MUST run red (missing implementation) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `./src/`   <fill before the §3 freeze — every file the build may write>
Strategy (ordered batches): <1. … 2. … — the planned build order; guidance, not enforced>
Safety rule (feature-specific): <e.g. debit+credit in one atomic transaction>
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- Scope tokens, backticked, FIRST declaring line: `./…` = this task dir · a token
     with "/" = project root · a bare name = sibling of the previous token's dir ·
     outside-root resolutions are dropped fail-closed · a DIRECTORY token covers its
     whole subtree (containment — diverges from §4's non-recursive counting) ·
     absent line = UNDECLARED (pre-existing tasks grandfathered, never retro-red) ·
     engine enforcement (touched ⊆ declared) lands in scope-gate-enforce.
     EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [ ] all tests pass
- [ ] coverage did not decrease
- [ ] no test or contract was altered during build
- [ ] the green was EARNED, not gamed — no overfit to fixtures, vacuous asserts, or stubbed-away logic (score with an adversarial refute-read — a subagent recommended under `autonomy: auto`; a confirmed cheat is HARD-STOP)
- [ ] concurrency / timing of the risky operation is safe
- [ ] no exposed secrets, injection openings, or unexpected dependencies
- [ ] layering & dependencies follow CONVENTIONS.md
- [ ] a person reviewed and approved the change

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [ ] <observable outcome a correct build must produce> — confirmed by <how / where>
- [ ] <another observable outcome> — confirmed by <evidence seen>

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [ ] WIRING (code) — every new symbol is referenced; record where / how confirmed
- [ ] DEAD-CODE (code) — no new unused or orphaned symbol introduced
- [ ] SEMANTIC (prose / non-code) — read in full, not skimmed: <what read · what confirmed>

### GATE RECORD
Outcome: <PASS | RISK-ACCEPTED | HARD-STOP>
If RISK-ACCEPTED -> owner: <name> · ticket: <link> · expires: <date>   (never for a security gap)
Reviewed by: <name> · date: <date>

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
