# TASK: First-party Rust/Python SDK wire forms + MQ/WS registry entries

slug: sdk-wire-form-fixes · created: 2026-08-09 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: done   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower the
     autonomy level to `manual` or `conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Touches (files · symbols · signatures):
  - `sdk/rust/src/mq.rs:MqClient::push_partitioned` / `::pop_partitioned` — send `MQ.PUSH` /
    `MQ.POP` as COMMAND NAMES. Neither exists.
  - `sdk/rust/src/vector.rs:VectorCommands::upsert` — sends `FT.UPSERT`. Does not exist.
  - `sdk/rust/src/client.rs:MoonClient::txn_begin` / `::txn_commit` / `::txn_abort` (~:1296) —
    send `TXN BEGIN|COMMIT|ABORT`. These WORK (see the §0 correction below); they are in scope
    only because the registry cannot see the command they use.
  - `src/command/transaction.rs:is_txn_begin` / `is_txn_commit` / `is_txn_abort` — the intercept
    that serves `TXN`, keyed on the SUBCOMMAND and run before registry lookup.
  - `src/command/metadata.rs` — the `phf` registry `COMMAND INFO` / `COMMAND COUNT` read.
    `FT.AGGREGATE` and `TXN` both dispatch but have no entry, so introspection reports them
    absent and a bare `TXN` answers `unknown command` instead of a wrong-arity error.
  - `sdk/rust/tests/integration.rs` — 12 live tests, ALL `#[ignore = "requires live server"]`, so
    they never run in CI. Running them was what exposed the `TXN` misdiagnosis. They also carry a
    self-inflicted flake: all 12 share ONE server and run in parallel, and `test_set_get_del:39`
    calls `flushdb()`. When that lands between `test_expire_ttl`'s `expire` and its `ttl`, TTL
    answers -2 and the assert fails — reproduced 1-in-3 in parallel, 0-in-N with
    `--test-threads=1`. A test-suite bug, NOT a server or SDK defect; measured, because an
    unexplained red would otherwise have been recorded as one.
  - `src/command/expire.rs` (TTL rounding) — measured while chasing that flake and NOT a defect
    in scope here, but worth the note: after `EXPIRE key 100`, Moon answers `TTL` = 99
    (PTTL 99993, truncating) where redis-server 8.6.1 answers 100 (it rounds to nearest,
    `(pttl+500)/1000`). Filed as a spec delta rather than folded in.
  - `sdk/python/moondb/__init__.py:27` — `__version__ = "0.1.0"`, a literal that already drifted
    from `sdk/python/pyproject.toml:7` (`0.1.1`).
  - `sdk/rust/Cargo.toml:3` — `version = "0.2.1"`; removing public helpers is a minor bump.

Context (working folder): live `moon` binary as the oracle — every claim below was measured by
  sending the command, not by reading the registry (`COMMAND INFO` is NOT authoritative here: it
  answers empty for `FT.AGGREGATE`, which dispatches fine).

MEASURED 2026-08-15, `--shards 1`, one fresh `redis-cli` per probe:

| command sent            | server answer                                | verdict |
|-------------------------|----------------------------------------------|---------|
| `MQ.PUSH topic 0 hello` | `ERR unknown command 'MQ.PUSH'`              | dead    |
| `MQ.POP g topic 0 …`    | `ERR unknown command 'MQ.POP'`               | dead    |
| `FT.UPSERT a b c d`     | `ERR unknown FT.* command`                   | dead    |
| `TXN` (no args)         | `ERR unknown command 'TXN'`                  | **live, see below** |
| `TXN BEGIN`             | `OK`                                         | live    |
| `TXN ABORT`             | `ERR not in a cross-store transaction`       | live    |
| `FT.AGGREGATE`          | `ERR wrong number of arguments` (dispatches) | live    |
| `COMMAND INFO FT.AGGREGATE` | *(empty)*                                | GAP     |
| `COMMAND INFO TXN`      | *(empty)*                                    | GAP     |
| `MQ CREATE q1` / `MQ PUSH q1 body hi` | `OK` / `1786747683478-0`       | live    |

### CORRECTION — `TXN` is NOT dead (caught before any code was written)

An earlier pass recorded `TXN` as a fourth dead wire form. That was a MEASUREMENT ERROR: the probe
ran `redis-cli -p PORT $a` with `a="TXN BEGIN"`, and **zsh does not word-split an unquoted
parameter expansion**, so the server received one argument literally named `TXN BEGIN` and
answered `unknown command 'TXN BEGIN'` — which reads exactly like the real thing.

What exposed it was running the SDK's own `#[ignore]`d `sdk/rust/tests/integration.rs` against a
live server: `test_moon_txn` PASSED, which is impossible if `txn_begin()` errors. Re-probed with
real argument splitting: `TXN BEGIN` -> `OK`, `TXN ABORT` -> `ERR not in a cross-store
transaction`. `TXN` is implemented in `src/command/transaction.rs` as an intercept keyed on the
SUBCOMMAND (`is_txn_begin` / `is_txn_commit` / `is_txn_abort`), which runs BEFORE the registry.

So `TXN` is the same shape as `FT.AGGREGATE`: dispatched by an intercept, absent from the
registry. A bare `TXN` matches none of the three predicates, falls through to a registry that has
no entry, and answers `unknown command` — which is why the no-arg probe lied.

**This invalidates the sweep design.** "Send the bare name; `unknown command` means dead" produces
a FALSE POSITIVE for every intercept-before-registry command. It is fixed by making the invariant
true rather than by special-casing the test.

MECHANISM, read rather than assumed (`src/server/conn/shared.rs:1241-1258`): a registry-driven
pre-gate looks the command up in the `phf` table and returns `unknown command` on a miss, else an
arity error if `args+1` violates `meta.arity`. The command intercepts (`TXN …`, `FT.*`) run
BEFORE that gate — which is why `TXN BEGIN` answers `OK` today despite `TXN` being absent from the
registry, and why bare `TXN` reaches the gate and gets `unknown command`. Adding a `TXN` entry
therefore changes ONLY the unmatched case: bare `TXN` starts answering a wrong-arity error, while
`TXN BEGIN|COMMIT|ABORT` never reach the gate at all. `swf3b` pins exactly that.

Note the two arity errors are NOT from the same place: `FT.AGGREGATE`'s
`ERR wrong number of arguments for 'FT.AGGREGATE' command` keeps the name's original case, whereas
the gate lowercases (`meta.name.to_lowercase()`) — so that one comes from the FT dispatcher, and
`FT.AGGREGATE` is genuinely absent from `metadata.rs` (11 `FT.*` entries there, not including it).
Adding it is a pure `COMMAND INFO` / `COMMAND COUNT` fix with no routing effect, since the FT
intercept already runs first.

Three helpers are dead, not four — `txn_begin` / `txn_commit` / `txn_abort` WORK and must be kept.

So the real MQ wire form is `MQ <SUB> <key> …` returning a stream entry ID — there is no
`(topic, partition)` model server-side and no numeric offset to return. All 123 other command
names the Rust SDK sends were swept the same way and every one of them dispatches.

The Python SDK was swept identically: every command name it sends (`FT.*`, `GRAPH.*`, core KV)
dispatches. Its defects are the version literal above and a surface gap — `sdk/rust/src/lib.rs`
exports `mq`, `temporal`, `workspace`; `sdk/python/moondb/` has no counterpart and
`client.py` exposes only `vector` / `graph` / `session` / `cache`.

Honors (patterns / conventions): measured-not-derived (CLAUDE.md, and the reason the `COMMAND
  INFO` gap was caught rather than trusted); new commands are registered in the `phf` table with
  an ACL category (CLAUDE.md "New Commands"); no CI workflow references `sdk/` at all, which is
  the root cause of the dead wire forms shipping — the guard has to be a test in the main repo's
  suite, not a new SDK-only job, and `#[ignore]` on the SDK's own live tests is the same
  invisibility in a smaller box.

**The title's "+ MQ/WS registry entries" is already DONE** — `src/command/metadata.rs:456-457`
carries `"WS"` and `"MQ"` with `flags: W`, and `metadata.rs:1091-1100` pins `is_write(b"MQ")` /
`is_write(b"WS")` in both cases. Verified rather than re-done; the remaining registry work is
`FT.AGGREGATE` and `TXN`, which the title predates.

Anchors the contract cites: `MqClient`, `VectorCommands::upsert`, `MoonClient::txn_*`,
`src/command/metadata.rs` registry, `moondb.__version__`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: every public first-party SDK helper works against a live Moon — right command name AND
right argument shape — with a test in the main repo that keeps it that way

**The helpers are not typos.** `sdk/rust/src` carries 11 "Lunaris-shaped" doc comments; the
memory record for the v0.1.1 SDK publish names `push_partitioned`, `pop_partitioned`, `upsert`
and `search_raw` as additions made FOR a Lunaris integration. They target a different server's
command set and were published in Moon's own crate (`moondb`, now 0.2.1). So the question is not
"fix a typo" but "does Moon's SDK ship API that Moon cannot answer" — decided: it does not.

Framings weighed:
  - **delete the three dead helpers, bump 0.2.1 -> 0.3.0, and round-trip every remaining helper
    live** (chosen) — a method whose first round trip is always `ERR unknown command` reads as a
    supported feature and is worse than its absence. Every removal is named in the CHANGELOG
    together with the working replacement. `txn_begin`/`txn_commit`/`txn_abort` are NOT removed:
    they work (see the §0 correction).
  - gate them behind a `lunaris` feature, off by default — keeps a downstream API alive, but
    ships code that provably cannot work against the server this crate is named for, and the
    sweep would have to skip exactly the code most likely to be wrong.
  - leave them and allow-list the four names in the sweep — smallest blast radius, but it makes
    the guard encode the defect, and the dead API keeps shipping.

Must:
<must>
  - Every command NAME either SDK sends is one the server dispatches. Not "is in `COMMAND INFO`" —
    dispatches, because those two sets provably differ (`FT.AGGREGATE` and `TXN` are both
    dispatched and both absent from the registry).
  - A command that is dispatched by an intercept is still discoverable: `COMMAND INFO` answers for
    it, and calling it with no arguments gives a wrong-arity error, never `unknown command`.
  - Every public helper in the Rust SDK, called with plausible arguments against a live server,
    completes without a protocol-level error. A name sweep alone is not enough: the remaining
    Lunaris-shaped helpers (`graph::query_with_params`, `graph::query_raw`, `vector::search_raw`,
    `temporal::snapshot_at_packed`) send REAL command names and could still carry a wrong arity
    or argument order.
  - A dead wire form is REMOVED, not left in place with a doc-comment warning.
  - Each removal is named in the CHANGELOG with a working replacement.
  - `FT.AGGREGATE` and `TXN` are introspectable: `COMMAND INFO` answers for each, and
    `COMMAND COUNT` includes both.
  - `moondb.__version__` cannot drift from `pyproject.toml` — it is derived, not typed twice.
  - Both guards live in the MAIN repo's suite, because no CI workflow references `sdk/` at all.
</must>

Reject:
<reject>
  - a command name in an SDK source that the server answers with `unknown command` -> the sweep
    FAILS naming the file, the command, and the server's exact reply
  - an `FT.*` name the server answers with `unknown FT.* command` -> same failure (the `FT.`
    dispatcher swallows the name before the top-level registry sees it, so a registry lookup
    would MISS this one)
  - a helper that returns `ERR wrong number of arguments` / `ERR syntax error` when called with
    plausible arguments -> the round-trip test FAILS naming the helper
  - `moondb.__version__` disagreeing with the installed distribution metadata -> a Python test
    fails
</reject>

After:
<after>
  - `cargo build` of `sdk/rust` exposes no method that cannot work.
  - The next SDK helper written against a command that does not exist, or against the right
    command with the wrong argument shape, is caught by CI on the PR that adds it — not by a
    manual sweep a milestone later.
</after>

Assumptions — lowest-confidence first:
<assumptions>
  ⚠ **No working caller depends on the removed helpers.** Lowest confidence because `moondb` is
    published and removal is a breaking change I cannot verify downstream — and, unlike a typo,
    these were added ON PURPOSE for a named integration. What makes removal still right is that
    each helper's FIRST server round trip against Moon is `ERR unknown command`: a caller can
    depend on the code COMPILING, never on its behaviour. Cost if wrong: a downstream build
    break, made loud by the minor bump and by naming every removed method in the CHANGELOG.
    Decision confirmed with the maintainer before any code was written.
  - [x] `COMMAND INFO` is a reliable "does this command exist" oracle — DENIED by measurement:
    `FT.AGGREGATE` dispatches and answers `COMMAND INFO` with nothing. The sweep must SEND.
  - [x] The Python SDK has the same dead-wire-form problem — DENIED: every command name it sends
    dispatches. Its defects are the version literal and missing surfaces.
  - [x] A command-name sweep is sufficient — DENIED twice over: it is blind to argument shape
    (where the surviving Lunaris-shaped helpers are most likely wrong), AND a bare-name probe
    false-positives on every intercept-before-registry command, which is how `TXN` was briefly
    misrecorded as dead. Hence the second guard, and the registry entries that make the first
    guard's rule actually true.
  - [x] `redis-cli -p P $var` splits `$var` into arguments — DENIED: zsh does not word-split
    unquoted parameter expansions. Every shell-loop probe over multi-word commands in this task
    was re-run with literal arguments before anything was believed.
  - [ ] Python surface parity (`mq`, `temporal`, `workspace` modules) belongs in THIS task —
    NO. The milestone goal is that a STOCK client works against Moon without special-casing;
    first-party Python feature parity is a separate feature, not a wire-form defect. Filed as a
    spec delta with its evidence rather than silently widened into here.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first. -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: every command an SDK sends is one the server dispatches
  Given a running moon server
  When the sweep collects every command-name literal from sdk/rust/src and sdk/python/moondb
  And sends each one to the server
  Then no reply is "unknown command" or "unknown FT.* command"
  And a name that IS unknown is reported with its source file and the server's exact reply

Scenario: the sweep would have caught the shipped defect
  Given the sweep test
  When it is run against the SDK sources as they were before this task
  Then it fails naming MQ.PUSH, MQ.POP, FT.UPSERT and TXN
  And it does NOT flag FT.AGGREGATE, which dispatches despite being absent from COMMAND INFO

Scenario: every surviving helper round-trips against a live server
  Given a running moon server with an index, a graph and a queue already created
  When each public Rust SDK helper is called with plausible arguments
  Then none returns a protocol-level error (unknown command, wrong arity, syntax error)
  And a helper that does is reported by name

Scenario: a helper with the right command but the wrong argument shape is caught
  Given the round-trip test
  When a helper's argument order or arity is altered to something the server rejects
  Then the test fails naming that helper
  And the name sweep alone stays green — proving the two guards cover different defects

Scenario: FT.AGGREGATE is introspectable
  Given a running moon server
  When a client sends COMMAND INFO FT.AGGREGATE
  Then it gets a command entry back
  And COMMAND COUNT is one higher than before this task

Scenario: the Python version cannot drift from the package metadata
  Given the moondb package
  When a test compares moondb.__version__ to the distribution version
  Then they are equal
  And editing pyproject.toml alone keeps them equal
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
REMOVED from sdk/rust (moondb 0.2.1 -> 0.3.0), each dead on arrival against Moon:
  MqClient::push_partitioned   (sent `MQ.PUSH`)
  MqClient::pop_partitioned    (sent `MQ.POP`)
  VectorCommands::upsert       (sent `FT.UPSERT`)
Working replacement, named in the CHANGELOG and required for each removal:
  queues        -> MqClient::push / ::pop        (`MQ PUSH|POP <key> …`)
  vector upsert -> FT.CREATE + HSET on the index's own vector field

KEPT — measured working, do NOT remove:
  MoonClient::txn_begin | txn_commit | txn_abort   (`TXN BEGIN|COMMIT|ABORT`, intercept-dispatched)

ADDED to src/command/metadata.rs (both dispatch today but are invisible to COMMAND INFO):
  "FT.AGGREGATE" => CommandMeta { arity: -2, flags: R, first_key: 1, last_key: 1, step: 1, … }
  "TXN"          => CommandMeta { arity: -2, flags: W, first_key: 0, last_key: 0, step: 0, … }
  Registering TXN also turns a bare `TXN` from `unknown command` into a wrong-arity error, which
  is what makes GUARD 1's rule true rather than special-cased.

sdk/python/moondb/__init__.py:
  __version__ derived from importlib.metadata.version("moondb"); pyproject.toml is the single
  source of truth.

GUARD 1 — name sweep (new test, main repo):
  for each command-name literal L in sdk/rust/src/**.rs and sdk/python/moondb/**.py:
      send L with no arguments to a live server
      assert the reply does NOT start with "ERR unknown command" / "ERR unknown FT.* command"
  An arity error is a PASS — it proves dispatch, which is the whole question.

GUARD 2 — live round trip (new test, main repo):
  drive a live server through every public Rust SDK helper with plausible arguments
  assert none returns unknown-command / wrong-arity / syntax-error
  a helper whose result is legitimately empty or Nil still PASSES — the assertion is on the
  protocol, not on the data
```

Status: FROZEN @ v1 — approved by Tin Dang
Status: AMENDED @ v2 — two further removals, found by GUARD 2 during build

### AMENDMENT v2 — what the guard found that the freeze could not

v1 froze THREE removals. The build found two more. They are recorded here rather than edited into
v1, because the difference between "we decided this" and "the guard proved this" is the whole
result of the task:

```
ALSO REMOVED (moondb 0.3.0), neither known at freeze time:
  TemporalClient::snapshot_at_packed   sent `TEMPORAL.SNAPSHOT_AT <packed_hlc>`
                                       -> ERR wrong number of arguments (server takes NO argument)
  TemporalClient::release_snapshot     sent bare `TEMPORAL.INVALIDATE`
                                       -> ERR wrong number of arguments (that command is the
                                          3-arg entity form: <entity_id> <NODE|EDGE> <graph>)
Replacement:
  snapshot_at_packed -> TemporalClient::snapshot_at   (the server captures the timestamp itself)
  release_snapshot   -> NOTHING. Delete the call; the premise was false — see below.
```

Both name a command Moon really has, which is exactly why GUARD 1 could not see them and why the
task's whole shape (two guards, not one) was right. `release_snapshot` was found by GUARD 2 on its
FIRST live run, **after** a by-hand read of the same file had already cleared it — the single
strongest piece of evidence produced by this task.

`release_snapshot` has no replacement because its documentation described behaviour the server
never had: `TEMPORAL.SNAPSHOT_AT` does not pin the connection to a snapshot view, it records a
shard-global `wall_ms -> LSN` binding that `AS_OF` resolves later (`src/server/conn/shared.rs:168`
is the only reader). No pin is taken, so none can be released. `snapshot_at`'s doc comment, which
asserted the imaginary pin, is corrected as part of the removal.

Scope consequence: this is an ADDITION to §3's removal list, made under §5's explicit build
instruction *"fix whatever it finds"*. No frozen clause was weakened or deleted, and no test was
altered to accommodate it.

Least-sure flag surfaced at freeze: **[contract] registering a command that has no
registry-dispatched handler.** `TXN` is served by an intercept in `src/command/transaction.rs`
that runs before lookup. Adding a registry entry must make it DISCOVERABLE without routing a bare
`TXN` into a missing-handler path — verify at build time that `TXN` with no args answers a
wrong-arity error and that `TXN BEGIN` is unchanged. Cost if wrong: a working command starts
answering nonsense, which is worse than the invisibility being fixed.

Second flag: **[contract] removing published API that was added on
purpose.** These are not typos — they were written for a Lunaris integration and shipped in a
published crate. The reason removal is still right is that each one's first server round trip
against Moon has always been `ERR unknown command`, so no caller can depend on behaviour, only on
compilation. Cost if wrong: a downstream build break. Mitigated by the minor bump and by naming
every removed method plus its working replacement rather than deleting quietly. Confirmed with
the maintainer before any code was written, precisely because "deliberate API for another
product" and "dead wire form" are the same diff but not the same decision.

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject above has a test.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - swf1_every_sdk_command_dispatches: arrange a live server / act send each command-name literal
    scraped from both SDK trees / assert none answers "unknown command"; on failure print file,
    command, reply (RED before the fixes: names MQ.PUSH, MQ.POP, FT.UPSERT — and TXN until the
    registry entry lands, which is the point of adding it)
  - swf2_a_dispatchable_command_absent_from_command_info_is_not_flagged: assert FT.AGGREGATE
    passes the sweep — pins that the sweep tests DISPATCH, not the registry, and would not have
    been satisfied by a registry lookup
  - swf3_intercept_dispatched_commands_are_introspectable: assert COMMAND INFO answers for
    FT.AGGREGATE and for TXN (RED before the registry entries)
  - swf3b_txn_still_works_after_registration: assert `TXN BEGIN` -> OK, `TXN ABORT` -> the
    not-in-a-transaction error, and bare `TXN` -> a wrong-arity error rather than unknown-command.
    Pins the risk named at the freeze: registering an intercept-dispatched command must not
    reroute it.
  - swf4_every_public_helper_round_trips: drive every public Rust SDK helper against a live
    server with plausible arguments; assert no protocol-level error. Lives in the SDK's own test
    tree (it needs the crate) and is invoked from the main suite so it cannot be forgotten
  - swf4b_round_trip_covers_every_public_helper (ADDED during build): scrape `pub async fn` from
    `sdk/rust/src/**`, qualify by owning impl type, diff against what swf4 drove. Without this,
    swf4 degrades silently as helpers are added — and "the surface shrank while the suite stayed
    green" is precisely how the two temporal helpers survived. RED at 43/168 when first run.
  - swf5 (Python): assert moondb.__version__ == the version in pyproject.toml (RED before:
    0.1.0 vs 0.1.1)
  - swf5b (ADDED during build): assert `__version__` is not assigned a string LITERAL at all.
    Equality alone only catches drift after it happens and is repaired by hand-editing the same
    literal that caused it; this asserts the derivation, so the class is closed rather than the
    instance. RED before.
  - swf5c (ADDED during build): assert the resolved value still looks like a release string —
    guards the uninstalled-source-tree fallback, which could otherwise satisfy swf5 and swf5b
    while handing callers `""`.
</test_plan>

Build note — a PRE-EXISTING test asserted the DEFECT: `tests/test_client.py::TestVersionExported`
pinned `__version__ == "0.1.0"`, a second copy of the stale literal. That is why the drift was
invisible for two releases: the suite restated the wrong number instead of deriving it. It was
repointed at `pyproject.toml`. This is not a test weakened to make a build pass — the assertion
was factually false about the published package, and the replacement is strictly stronger.

Tests live in: `tests/sdk_wire_forms.rs` · `sdk/rust/tests/round_trip.rs` ·
`sdk/python/tests/test_version.py`
MUST run red (missing implementation) before Build.

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `sdk/rust/src/mq.rs` `vector.rs` `client.rs` `graph.rs` `temporal.rs`
`sdk/rust/Cargo.toml` `sdk/rust/tests/` `sdk/python/moondb/__init__.py` `sdk/python/tests/`
`src/command/metadata.rs` `tests/sdk_wire_forms.rs` `CHANGELOG.md` `.github/workflows/ci.yml`

Strategy (ordered batches):
  1. Write the name sweep; confirm it goes red naming exactly the four dead names.
  2. Add the `FT.AGGREGATE` and `TXN` registry entries; confirm swf3 + swf3b green and swf2 still
     green. swf3b is the guard that registering an intercept-dispatched command did not reroute it.
  3. Remove the three dead methods (NOT the `txn_*` trio); bump `moondb` to 0.3.0; build the SDK.
  4. Write the round-trip test over every remaining public helper; fix whatever it finds — the
     four surviving Lunaris-shaped helpers are the prime suspects.
  5. Derive `moondb.__version__`; add the Python test.
  6. CHANGELOG: name every removed method and its working replacement.

Safety rule (feature-specific): the name sweep must treat an ARITY error as a pass — a sweep that
demanded success would require calling all ~125 commands correctly and would rot immediately.
The round-trip test is where arity is actually checked, per helper, with real arguments.

Code lives in: `sdk/`, `src/command/metadata.rs`, `tests/`
Constraints: do NOT change any test or the contract; do not add a server feature to satisfy an
SDK helper — remove the helper.

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [ ] all tests pass
- [ ] coverage did not decrease
- [ ] no test or contract was altered during build
- [ ] the green was EARNED, not gamed
- [ ] concurrency / timing of the risky operation is safe
- [ ] no exposed secrets, injection openings, or unexpected dependencies
- [ ] layering & dependencies follow CONVENTIONS.md
- [ ] a person reviewed and approved the change

### Build expectations — what "correct" looks like
- [x] the sweep, run against the PRE-fix SDK sources, names exactly MQ.PUSH, MQ.POP, FT.UPSERT,
      TXN — confirmed by running it before the removals
- [x] `COMMAND COUNT` rises by exactly **2**, not 1 — the expectation as drafted was wrong: §3
      adds TWO registry entries (`TXN` and `FT.AGGREGATE`), so +2 is what correct looks like.
      MEASURED: table entries 265 (HEAD) -> 267 (worktree), diff shows exactly those two names,
      and a live server reports `COMMAND COUNT` = 267.
- [x] `cargo build -p moondb` succeeds with no reference to a removed method remaining —
      grep across `*.rs` / `*.md` / `*.toml` finds the five names only in the comment blocks that
      explain their removal and in this record
- [x] the CHANGELOG names all **five** removed methods AND a working replacement for each (two are
      "nothing — delete the call", with the reason), and states explicitly that TXN was NOT removed
- [x] the round-trip test covers EVERY public helper on the Rust SDK — MEASURED, not eyeballed:
      `swf4b` scrapes `pub async fn` from `sdk/rust/src/**`, qualifies each by its owning `impl`
      type, and diffs against what the suite drove. **168 of 168.** Non-vacuity proved by dropping
      one call and observing `1 of 168 ... VectorClient::compact`.
      Coverage is keyed by `Type::fn`, NOT bare name — five sub-clients declare a `search` and
      three a `create`, so a name-keyed check would count a future `NewClient::search` as covered
      because `VectorClient::search` happens to be driven. Caught and fixed before the gate.
- [x] the two guards are shown to catch DIFFERENT defects — MEASURED by reordering
      `MqClient::create`'s arguments (`MQ CREATE <key>` -> `MQ <key> CREATE`): the round trip goes
      RED (`mq.create -> unknown MQ subcommand`) while the name sweep stays 4/4 green.
      This experiment also found a hole in the round trip's OWN predicate: it matched the literal
      phrases "unknown command"/"unknown subcommand", and Moon names the family in between
      (`ERR unknown MQ subcommand`), so the first run of the mutant PASSED. Widened to two loose
      tokens; the mutant then failed as it should. The guard was only trustworthy after being
      attacked.

### Deep checks
- [x] WIRING (code) — GUARD 1 lives in the main test tree and already runs in `check` /
      `check-monoio`. GUARDS 2 and 3 had NO home: `sdk/` is outside the cargo workspace and had no
      CI job of any kind, which is the root cause of all five defects. Both are now steps in
      `client-compat` (`.github/workflows/ci.yml`) — the job that already builds Moon and proves a
      real client works against it. The CI invocation form (`--manifest-path sdk/rust/Cargo.toml`)
      was rehearsed locally, including against a FRESH server, so it has no hidden state
      dependency. The step arms its kill-trap BEFORE the readiness wait and hard-fails if the
      server never answers, so a green-because-it-never-ran result is not reachable.
- [x] DEAD-CODE — `parse_mq_messages` is still used by `MqClient::pop`; no orphan left by any of
      the five removals. `cargo clippy` reports no dead-code warning on either feature leg.
- [x] FEATURE-LEG CORRECTNESS — the sweep initially FAILED the tokio leg (`--no-default-features
      --features runtime-tokio,jemalloc`): `graph` and `text-index` are DEFAULT features that leg
      drops, so 13 `GRAPH.*` names and `FT.AGGREGATE` are legitimately absent there. The sweep now
      consults `cfg!(feature = …)` and skips only those, ANNOUNCING what it skipped rather than
      shrinking silently. The default build skips nothing. Had this not been caught locally it
      would have turned CI red on merge.

### GATE RECORD
Outcome: PASS
Reviewed by: Tin Dang · date: 2026-08-15

Evidence:
  main repo, monoio (default):  lib 4641 passed / 0 failed; sdk_wire_forms 4/4;
                                batch_protocol_version 7/7
  main repo, tokio+jemalloc:    lib 3807 passed / 0 failed; sdk_wire_forms 3 passed + 1 correctly
                                ignored (FT.AGGREGATE is behind `text-index`);
                                batch_protocol_version 7/7
  sdk/rust:                     round_trip 2/2 against a live server — 168/168 helpers driven,
                                zero protocol-level rejections
  sdk/python:                   test_version 3/3; full offline suite 208 passed / 7 failed, where
                                all 7 are pre-existing `test_text.py` async failures that fail
                                identically at HEAD (no `pytest-asyncio` in this environment)
  fmt + clippy:                 clean on both feature legs, main repo

Flakes observed and characterised, NOT attributed to this change:
  - `persistence::manifest::tests::test_overflow_compaction_bounds_growth` failed once under full
    parallel suite load; 3/3 clean re-run.
  - `parked_idle_parity::resumed_connection_keeps_registry_identity` failed once in the same
    loaded run; 3/3 clean re-run. Both are timing-sensitive suites in areas this change does not
    touch.

Known, deliberately NOT fixed here (would widen scope):
  - `sdk/rust` has 3 pre-existing `clippy::too_many_arguments` errors (`cache.rs:20`, `text.rs:152`,
    `vector.rs:221`). Count is 3 at HEAD and 3 now — verified by stashing the change and re-running.
    SDK clippy is not in CI; filed as a spec delta rather than silently expanded into this task.

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): the two guards are the monitors — a name that stops
dispatching or a helper whose arguments stop being accepted fails CI on the PR that does it. The
thing NOT covered, and worth watching by hand, is a command that still dispatches but changes its
REPLY shape; only the round trip's typed deserialization would catch that, and only where the
helper returns a concrete type rather than `redis::Value`.

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.

  - [SPEC · open] `TTL` truncates where Redis rounds to nearest: after `EXPIRE key 100`, Moon
    answers 99 (PTTL 99993) and redis-server 8.6.1 answers 100 — Redis computes `(pttl+500)/1000`
    (evidence: measured side by side 2026-08-15 while chasing an unrelated SDK test flake). A
    client that asserts on a read-back TTL sees an off-by-one.
  - [SPEC · open] `sdk/rust/tests/integration.rs` runs 12 tests in parallel against ONE shared
    server while `test_set_get_del` calls `flushdb()` — a self-inflicted 1-in-3 flake, invisible
    because every test is `#[ignore]`d (evidence: reproduced above, clean at `--test-threads=1`)
  - [SPEC · open] Python surface parity — `sdk/rust/src/lib.rs` exports `mq`, `temporal`,
    `workspace`; `sdk/python/moondb/` has no counterpart (evidence: §0). Scoped OUT of this task
    deliberately: it is a feature, not a wire-form defect.
  - [SPEC · open] the Python SDK has no round-trip guard — GUARD 2 covers the Rust surface only,
    and `sdk/python` is the tree where the version drift lived (evidence: this task fixed the
    Python version defect but could only guard it structurally, not by round trip). The five
    defects found here were all Rust-side because that is the only side with a guard.
  - [SPEC · open] `sdk/rust` carries 3 `clippy::too_many_arguments` errors and no clippy CI
    (evidence: 3 at HEAD, 3 now, verified by stash). Either wire SDK clippy into `client-compat`
    and fix them, or record an explicit allow with a reason.
  - [SPEC · open] `TemporalClient` documents a snapshot-pin model the server does not implement —
    `snapshot_at`'s doc was corrected here, but the underlying question ("should a connection be
    pinnable to a temporal view at all, or is `AS_OF` the whole story?") is a product decision, not
    a doc fix (evidence: `src/server/conn/shared.rs:168` is the registry's only reader).

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence.

  - [TDD · open] A guard must be attacked before it is trusted. The mutation experiment was
    written to prove the two guards catch different defects; it instead proved the round trip's
    predicate was too narrow to catch the mutant at all (phrase-matching "unknown subcommand" when
    Moon says "unknown MQ subcommand"). Both guards were green and one was partly blind. Budget a
    deliberate mutation for every new guard, not as a nicety but as the thing that makes the
    green mean something (evidence: mutant survived the first predicate, failed the widened one).
  - [TDD · open] Coverage keyed by the wrong identity is coverage theatre. `swf4b` first keyed on
    bare fn name; five sub-clients declare `search`, so a whole future client could inherit
    "covered" status from an unrelated namesake. Key a coverage assertion by the identity that can
    actually collide (evidence: fixed to `Type::fn` before the gate).
  - [TDD · open] "It's an ordinary command, it's surely fine" is the exact reasoning that ships
    dead code. `release_snapshot` was cleared by a by-hand read of the file and then failed on the
    guard's FIRST live run. The round trip initially drove 43 of 168 helpers because the other 125
    "looked like plain Redis"; expanding to 168 is what turned the suite from a spot-check into a
    guarantee (evidence: §6 measured 168/168).
  - [ADD · open] A frozen contract can be under-specified without being wrong. §3 froze three
    removals; the build found five. The right move was an AMENDMENT recorded beside v1 — not a
    silent edit of the frozen list, and not refusing the extra removals on a technicality. The
    freeze bounds the DECISION, not the discovery (evidence: §3 AMENDMENT v2).
  - [ADD · open] A tree with no CI accumulates defects at exactly the rate you would predict.
    All five dead helpers, plus the version drift, lived in `sdk/` — the one tree with no job of
    any kind. The durable fix was wiring, not the five deletions (evidence: §6 WIRING).
  - [TDD · open] Default features are part of what a test asserts against. The sweep passed the
    default leg and failed the tokio leg because `graph`/`text-index` are default-on and CI's
    portability leg drops them. A cross-feature test must consult `cfg!` and announce its skips
    (evidence: 26 names skipped, printed, on the tokio leg). See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
