# TASK: MULTI queues every command, and EXEC answers correctly on every shard count

slug: multi-exec-queue-semantics · created: 2026-08-09 · stage: production
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
- The MULTI queueing step in each handler — `handler_monoio/mod.rs` (~1826, `if conn.in_multi {`),
  `handler_sharded/mod.rs`, `handler_single.rs` (~1752). Today it pushes EVERY frame onto
  `conn.command_queue` and answers `+QUEUED` without looking at the command at all. The only
  exception already there is an `FT.*` rejection.
- `src/command/metadata.rs:COMMAND_META` — `lookup()` + `arity`, which is the information the
  queue step needs and does not consult.
- `ConnectionState.command_queue` / `in_multi` (`src/server/conn/core.rs`), and the EXEC replay in
  `handler_monoio/write.rs` + `src/shard/spsc_handler.rs` (the two per-command txn loops).

Context (working folder): `tests/multi_queues_inline_get.rs` (guards that commands QUEUE at all —
the #457 regression), `tests/watch_cas_transactions.rs` (WATCH/CAS), `scripts/client-compat/
manifest.yaml`.

Honors: `Frame::Error(Bytes)` for errors; per-shard locks; no allocation on the dispatch path.

Anchors the contract cites: the `in_multi` queue step, `COMMAND_META::lookup`, `arity`.

### Measured against redis-server 8.6.1 (Moon single-shard, identical bytes, one command per write)

Probe: `scratchpad/probe_ground.py` plus stepwise follow-ups. **Cases are FLUSHALL-isolated** —
without that, one real finding manufactured two false ones (see below).

| sequence | redis-server 8.6.1 | Moon | verdict |
|---|---|---|---|
| `MULTI` / `NOSUCHCMD a b` / `EXEC` | `+OK` · `-ERR unknown command …'a' 'b'` · `-EXECABORT Transaction discarded because of previous errors.` | `+OK` · **`+QUEUED`** · `*1[-ERR unknown command]` | diverges |
| `MULTI` / `GET` (bad arity) / `EXEC` | `+OK` · `-ERR wrong number of arguments for 'get' command` · `-EXECABORT` | `+OK` · **`+QUEUED`** · `*1[-ERR wrong number…]` | diverges |
| `MULTI` / `NOSUCHCMD` / `SET k v` / `EXEC` | whole transaction DISCARDED — `SET` never runs | **`*2` — the `SET` IS APPLIED** | diverges |
| `MULTI` / `SUBSCRIBE ch` / `EXEC` | `+QUEUED`, then EXEC runs it | **executes SUBSCRIBE immediately**, then EXEC errors | diverges |
| `MULTI` / `*-9` (bad frame) / `EXEC` | bad frame ignored; `EXEC` → `*0` | **connection CLOSED / RST** | diverges |
| `MULTI` / `BLPOP nokey 0` / `EXEC` | `*1` `*-1` (Null **Array**) | `*1` `$-1` (Null **Bulk**) | diverges |
| nested `MULTI` | `-ERR MULTI calls can not be nested`, `EXEC` → `*0` | identical | matches |
| `EXEC` / `DISCARD` without MULTI | `-ERR EXEC without MULTI` / `-ERR DISCARD without MULTI` | identical | matches |
| `MULTI` / `SET k v` / `DISCARD` / `GET k` | `$-1` — discarded | identical | matches |
| `MULTI` / `SELECT 3` / `EXEC` | `+OK` inside `*1` | identical | matches |
| empty `MULTI` / `EXEC` | `*0` | identical | matches |
| `MULTI` / `SET k v` / `RESET` / `EXEC` | `+RESET` then `-ERR EXEC without MULTI` | identical | matches |
| WRONGTYPE inside MULTI | `*1[-WRONGTYPE …]` | identical | matches |

**The headline is atomicity, not the error text.** Moon accepts anything into the queue, so a
transaction containing ONE unrunnable command still commits every OTHER command in it. Redis
refuses the command at queue time and then refuses the whole transaction. A client that typos one
command gets a partial write from Moon and nothing from Redis — and it cannot tell, because Moon
answered `+QUEUED` to the typo.

**Method note (cost paid, recorded so it is not paid twice):** the first probe pass shared server
state across cases. The missing EXECABORT applied `SET k v` on Moon only, which then made `DISCARD`
look broken (`GET k` → `v` instead of `$-1`) and `DEL` look wrong (`:1` vs `:0`). Both were FALSE —
verified against a fresh server, DISCARD and DEL are correct. One real bug manufactured two
phantom ones. A second pass under host load showed empty replies that were really 2s-timeout
truncation, not silence. Every row above was re-confirmed stepwise on an isolated server.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: a transaction is all-or-nothing at QUEUE time, the way Redis makes it. A command that
could never have run — unknown name, impossible arity — poisons the transaction when it is QUEUED,
so EXEC refuses the whole block instead of applying the half that happened to be valid.

Framings weighed:
- **Validate at queue time, latch the failure, refuse at EXEC (chosen).** Redis's actual design:
  `MULTI` opens the queue; each queued command is checked for existence and arity BEFORE it is
  stored; a failure replies an error immediately AND sets a dirty flag; `EXEC` sees the flag and
  answers `-EXECABORT`. Cheap (the check is a `COMMAND_META` lookup Moon already performs on every
  dispatch) and it is the semantics every client library is written against.
- *Validate at EXEC time by walking the queue.* Rejected: it gives the right EXEC answer but the
  WRONG queue answer — the client is told `+QUEUED` for a command that cannot exist, so a driver
  that surfaces per-command errors (node-redis, lettuce) reports success for the typo and only
  fails later, at a call site that has no idea which command was bad.
- *Do nothing; document the divergence.* Rejected: this is not cosmetic. Measured today,
  `MULTI / NOSUCHCMD / SET k v / EXEC` leaves `k` SET on Moon and unset on Redis. A transaction
  that partially applies is a data-correctness bug, not a compatibility nit.

Must:
<must>
  - A command queued inside MULTI is checked for (a) existence and (b) arity against the same
    `COMMAND_META` table dispatch uses. Failure replies the error immediately, does NOT queue it,
    and marks the transaction dirty.
  - `EXEC` on a dirty transaction replies
    `-EXECABORT Transaction discarded because of previous errors.`, executes NOTHING, and leaves
    the connection in the normal (non-MULTI) state.
  - `MULTI / NOSUCHCMD / SET k v / EXEC` leaves `k` UNSET. This is the measured data-loss case.
  - `DISCARD` clears the dirty flag along with the queue: a poisoned transaction can be abandoned
    and a fresh `MULTI` on the same connection starts clean.
  - A command that is merely *unqueueable*, not invalid — `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`,
    `PUNSUBSCRIBE`, `WATCH` — is REJECTED at queue time with its Redis error instead of executing
    immediately. Moon runs `SUBSCRIBE` for real inside MULTI today.
  - A malformed frame arriving inside a MULTI follows the protocol-error-lifetime contract (error
    on the wire, then close) instead of Moon's current bare close/RST.
  - A blocking command inside MULTI does not block: it takes its zero-timeout path. `BLPOP` on a
    missing key must reply Null **Array** (`*-1` RESP2), not Null Bulk (`$-1`) — Moon's current
    reply mistypes the value and confuses statically-typed clients.
  - Behavior already matching Redis stays matching: nested `MULTI`, `EXEC`/`DISCARD` without
    `MULTI`, `DISCARD` inside `MULTI`, `SELECT` inside `MULTI`, empty `MULTI`/`EXEC`, `RESET`
    inside `MULTI`, and a `WRONGTYPE` at execution time (a runtime error does NOT abort the block).
</must>
Reject:
<reject>
  - unknown command queued -> "ERR unknown command '<name>', with args beginning with: ..."
  - wrong arity queued -> "ERR wrong number of arguments for '<name>' command"
  - SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE queued -> "ERR <NAME> is not allowed in transactions"
  - WATCH queued -> "ERR WATCH inside MULTI is not allowed"
  - EXEC after any of the above -> "EXECABORT Transaction discarded because of previous errors."
</reject>
After:
<after>
  - A transaction either applies wholly or not at all with respect to queue-time faults. No
    interleaving of "the valid half ran".
  - The client learns WHICH command was bad, at the moment it sent it.
  - The dirty flag never outlives its transaction: DISCARD, EXEC, and RESET all clear it.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ That every command Moon dispatches is present in `COMMAND_META` with a correct arity — lowest
    confidence because Moon has THREE dispatch paths (`dispatch`, `dispatch_read`, inline) and a
    command reachable through one of them but absent from the table would now be REJECTED inside
    MULTI while still working outside it. That would be a regression strictly worse than the bug.
    Mitigation: a test that walks the full `COMMAND_META` table AND a test that queues a
    representative command from each dispatch path; if wrong, the cost is a working command
    becoming unusable in transactions.
  - [x] The dirty flag has somewhere to live — CONFIRMED: the per-connection MULTI state already
    holds the queue, so the flag sits beside it with no new allocation and no new lock.
  - [x] Runtime errors must NOT abort — CONFIRMED by measurement: `WRONGTYPE` inside a block
    returns the error as one element of the EXEC array on BOTH servers, and the other commands run.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: an unknown command poisons the transaction  # Must 1,2,3 · Reject 1,5
  Given a connection that has sent MULTI
  When it queues NOSUCHCMD, then SET k v, then EXEC
  Then the NOSUCHCMD reply is "ERR unknown command 'NOSUCHCMD', with args beginning with: "
  And the SET reply is "+QUEUED"
  And the EXEC reply is "EXECABORT Transaction discarded because of previous errors."
  And k does not exist   # the measured data-loss case: Moon SETs it today

Scenario: wrong arity poisons the transaction  # Must 1,2 · Reject 2,5
  Given a connection that has sent MULTI
  When it queues GET with no arguments, then EXEC
  Then the GET reply is "ERR wrong number of arguments for 'get' command"
  And the EXEC reply begins "EXECABORT"

Scenario: DISCARD clears the poison  # Must 4
  Given a connection whose MULTI has been poisoned by an unknown command
  When it sends DISCARD, then MULTI, then SET k v, then EXEC
  Then the EXEC reply is an array of one "+OK"
  And k equals v   # the dirty flag did not leak into the next transaction

Scenario: SUBSCRIBE is refused at queue time, not executed  # Must 5 · Reject 3
  Given a connection that has sent MULTI
  When it queues SUBSCRIBE ch
  Then the reply is "ERR SUBSCRIBE is not allowed in transactions"
  And the connection is NOT in subscriber mode   # Moon subscribes for real today
  And a following EXEC replies "EXECABORT ..."

Scenario: WATCH is refused inside MULTI  # Reject 4
  Given a connection that has sent MULTI
  When it queues WATCH k
  Then the reply is "ERR WATCH inside MULTI is not allowed"

Scenario: a malformed frame inside MULTI names itself before the close  # Must 6
  Given a connection that has sent MULTI
  When it sends a bulk header with a non-numeric length
  Then the server replies "ERR Protocol error: invalid bulk length"
  And only then closes   # Moon closes bare today

Scenario: a blocking command inside MULTI returns the right NULL TYPE  # Must 7
  Given an empty keyspace
  When a connection runs MULTI / BLPOP nokey 0 / EXEC
  Then EXEC replies an array whose single element is a Null Array ("*-1" in RESP2)
  And it is not a Null Bulk ("$-1")   # Moon mistypes it today

Scenario: a runtime error does not abort the block  # Must 8
  Given k holds a list
  When a connection runs MULTI / GET k / SET other v / EXEC
  Then EXEC replies a 2-element array whose first element is a WRONGTYPE error
  And other equals v   # a runtime error is data, not a queue-time fault

Scenario: already-matching behavior stays matching  # Must 8
  Given a fresh connection
  When it exercises nested MULTI, EXEC without MULTI, DISCARD without MULTI,
       DISCARD inside MULTI, SELECT inside MULTI, empty MULTI/EXEC, and RESET inside MULTI
  Then every reply is byte-identical to redis-server 8.6.1
  And no connection is closed
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
The contract is the RESP wire, not an HTTP route.

MULTI                     -> +OK                (already correct)
<cmd> ... while queueing  -> +QUEUED            when COMMAND_META has <cmd> AND arity matches
                          -> -ERR unknown command '<cmd>', with args beginning with: <args>
                          -> -ERR wrong number of arguments for '<lower>' command
                          -> -ERR <NAME> is not allowed in transactions        (pubsub verbs)
                          -> -ERR WATCH inside MULTI is not allowed
                             ... and any of the four errors sets dirty = true; nothing is queued.
EXEC   dirty              -> -EXECABORT Transaction discarded because of previous errors.
                             queue cleared · dirty cleared · connection leaves MULTI state
       clean              -> *<n> of per-command replies         (already correct)
       no MULTI           -> -ERR EXEC without MULTI             (already correct)
DISCARD                   -> +OK, queue cleared, dirty cleared   (dirty-clear is NEW)
RESET                     -> +RESET, queue cleared, dirty cleared

State: one added `bool` on the existing per-connection MULTI state. No new lock, no new
allocation, no shared/global state — the flag is connection-local by construction, so shard count
cannot affect it.

Arity source of truth: `COMMAND_META::lookup(name)` + its `arity` field — the SAME table
`dispatch` consults, so a command cannot be queueable-but-undispatchable or the reverse.

Null-type fix: the zero-timeout path of the blocking family returns `Frame::Array(vec![])`-null
(RESP2 `*-1`), not `Frame::Null` bulk (`$-1`).
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-08-12)

Least-sure flag surfaced at freeze:
- [contract] That every command Moon dispatches is present in COMMAND_META with a correct arity.
  Why it might be wrong: Moon has THREE dispatch paths, and a command reachable through one but
  absent from the table would be REJECTED inside MULTI while still working outside it — a
  regression strictly worse than the bug. Cost if wrong: a working command becomes unusable in
  transactions.
  OUTCOME: **this flag was correct.** COMMAND_META (263 entries) omits TS.*, JSON.*, TXN, FT and
  bare GRAPH. Nothing broke only because TXN is intercepted above the queue gate, FT.* is rejected
  above it, and TS./JSON. do not exist — four accidents, not a safety argument. Resolved with a
  dotted-name carve-out in `queue_time_rejection`, pinned by me10b.
- [scenario] me7 (BLPOP null TYPE) turned out to be unfixable in scope: `Frame` has no null-array
  variant, so RESP2 `*-1` is inexpressible anywhere in Moon. Filed; test #[ignore]d with its reason.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject above has one test; suite runs on BOTH runtimes
and at shards=1 AND shards=4 (the §1 ⚠ is about dispatch-path coverage, which shard count changes).

Plan (one test per scenario, asserting wire behavior — never internals):
<test_plan>
  - me1_unknown_command_aborts_and_applies_nothing: MULTI / NOSUCHCMD / SET k v / EXEC;
    assert the unknown-command error, "+QUEUED" for SET, "EXECABORT" for EXEC, and GET k is nil
  - me2_wrong_arity_aborts: MULTI / GET / EXEC; assert arity error then EXECABORT
  - me3_discard_clears_dirty: poison, DISCARD, then a clean MULTI/SET/EXEC applies
  - me4_subscribe_refused_not_executed: assert the error AND that a following PING still answers
    "+PONG" (a real SUBSCRIBE would have put the connection in subscriber mode)
  - me5_watch_inside_multi_refused: assert the exact Redis text
  - me6_bad_frame_inside_multi_names_itself: assert the protocol error arrives BEFORE the close
  - me7_blpop_in_multi_is_null_array: assert the RESP2 bytes are "*-1", not "$-1"
  - me8_wrongtype_does_not_abort: assert the 2-element array and that the sibling SET applied
  - me9_matching_behavior_unchanged: the seven already-correct cases, byte-compared
  - me10_every_command_meta_entry_is_queueable: walk COMMAND_META; for each non-exempt entry,
    queue it with its minimum valid arity and assert "+QUEUED" — this is the §1 ⚠ mitigation,
    the test that catches a command becoming unusable in transactions
  - me11_one_command_per_dispatch_path_queues: a write (dispatch), a read (dispatch_read), and
    an inline-fast-path command each queue successfully at shards=1 and shards=4
</test_plan>

Tests live in: `tests/multi_exec_queue_semantics.rs` · MUST run red before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/server/conn/` `src/command/mod.rs` `src/command/list/` `tests/multi_exec_queue_semantics.rs` `scripts/client-compat/manifest.yaml`
Strategy (ordered batches):
  1. Add the dirty flag + queue-time validation helper next to the existing MULTI queue state.
  2. Wire it into all three handlers' queueing step (the ONE place each of them queues).
  3. EXEC: check dirty first; DISCARD/RESET: clear it.
  4. Refuse the pubsub verbs + WATCH at queue time.
  5. Fix the blocking-family zero-timeout null type.
  6. Retire the corresponding compat-harness waivers.
Safety rule (feature-specific): the dirty flag MUST be cleared on every exit from MULTI state
(EXEC, DISCARD, RESET, and connection reuse) — a leaked flag silently aborts an innocent later
transaction, which is a worse bug than the one being fixed. me3 is the test that proves it.
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
- [x] The data-loss case is gone: `MULTI / NOSUCHCMD / SET k v / EXEC` leaves `k` UNSET.
      Confirmed by reading the KEY afterwards, not by reading the EXEC reply — the reply could
      say EXECABORT while the write had already landed, and that is the bug being closed.
- [x] The client is told WHICH command was bad, at the moment it sent it: the typo gets its
      error inline instead of `+QUEUED`, so a driver that surfaces per-command errors reports
      the fault at the call site that caused it — confirmed by asserting the queue-time reply,
      which is the half the "validate at EXEC" framing would have got wrong.
- [x] The dirty flag never outlives its transaction. DISCARD, EXEC and RESET all clear it, so a
      poisoned transaction followed by a fresh MULTI on the SAME connection runs normally —
      confirmed by same-connection sequence tests, the only shape that can catch a leaked flag
      silently aborting an innocent later block.
- [x] Runtime errors still do NOT abort: a `WRONGTYPE` inside a block is returned as one element
      of the EXEC array and the other commands run — confirmed against measured Redis behavior.
      The queue-time/run-time distinction is the whole contract; collapsing it would "fix" this
      task by breaking working transactions.
- [x] `SUBSCRIBE` inside MULTI is refused at queue time rather than EXECUTED — confirmed by
      checking the connection is NOT left in subscriber mode afterwards, which is what Moon did
      before and what a reply-only assertion would miss.
- [ ] `BLPOP` on a missing key inside MULTI replies Null **Array** — NOT MET. See the gate
      record: `Frame` has no null-array variant, the divergence reproduces outside MULTI too,
      and `me7` is ignored (not weakened) pending #482.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — the validation helper is worthless in a handler that still queues blindly,
      and Moon queues in three separate handlers. All three call it; confirmed by running the
      suite against each rather than by grepping, since a missed handler compiles fine and only
      diverges at runtime.
- [x] DEAD-CODE (code) — no orphaned symbol. The dirty flag sits beside the existing MULTI queue
      state (no new allocation, no new lock) and every clear site is exercised.
- [x] SEMANTIC — the ⚠ assumption was DISCHARGED by test, not by argument: a command present in
      one of Moon's three dispatch paths but missing from `COMMAND_META` would now be rejected
      inside MULTI while still working outside it — a regression strictly worse than the bug.
      A test walks the full `COMMAND_META` table and another queues a representative command
      from each dispatch path.

### GATE RECORD
Outcome: RISK-ACCEPTED
owner: Tin Dang · ticket: https://github.com/pilotspace/moon/issues/482 · expires: 2026-10-31

Not a PASS, because Must #7 is not met. `BLPOP` on a missing key inside MULTI still replies a
Null Bulk (`$-1`) where Redis replies a Null Array (`*-1`), and `me7` is `#[ignore]`d rather than
passing. The remaining seven Musts and all five Rejects are met.

Why the risk is acceptable rather than a blocker: the divergence is NOT introduced by this task
and is NOT specific to transactions — `Frame` has no null-array variant at all, so a plain
`BLPOP key 1` that times out on a normal connection mistypes its reply too. Fixing it means
threading a `Frame::NullArray` through every `Frame::Null` arm in `serialize.rs` / `resp3.rs`,
which touches every reply path in the server and can flip the type of replies that are currently
correct. That deserves its own contract and review, which is #482. RESP2-only; RESP3 spells both
nulls `_\r\n`.

`me7` is ignored, NOT weakened — its assertion is already the right one and the ignore reason
names the missing capability, so the task that adds the variant un-ignores it as its own proof.

Evidence: PR #472 (squash `292269ac`). Re-verified on merged `main` @ `3f842d9f`:
`tests/multi_exec_queue_semantics.rs` 12 passed / 1 ignored under BOTH `runtime-monoio` (shipped)
and `runtime-tokio,jemalloc`.
Reviewed by: Tin Dang · date: 2026-08-14

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): an EXECABORT on a transaction whose commands were all valid
— that is the signature of a dirty flag leaking across transactions on a reused connection, and
it would abort innocent work rather than merely reporting it.

### Spec delta
- [SPEC · seeded] `Frame` has no null-array variant, so every command whose empty answer is an
  array replies `$-1` instead of `*-1` in RESP2 (evidence: measured `*1\r\n$-1\r\n` vs Redis
  `*1\r\n*-1\r\n`; reproduces outside MULTI; seeded as #482, gate RISK-ACCEPTED against it).

### Competency deltas
- [TDD · open] Assert the STATE, not the reply, when the bug is that state changed anyway. The
  data-loss case here is `k` being set while EXEC says EXECABORT — a reply-only assertion passes
  on a server that reports abort and writes regardless (evidence: the test reads the key back).
- [ADD · open] Discharge a ⚠ assumption by test, not by argument. "Every dispatched command is in
  `COMMAND_META`" was plausible and its failure mode was a regression worse than the bug being
  fixed, so it became two tests rather than a paragraph (evidence: §1 assumption 1, and Moon's
  three dispatch paths).
- [ADD · open] An unmet Must is a RISK-ACCEPTED with a ticket, never a quiet PASS with a deleted
  test. `me7` stays in the suite, ignored with a reason that names the missing capability, so the
  gap is visible to the next reader instead of disappearing with the assertion (evidence: this
  gate record; #482).
