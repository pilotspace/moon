# TASK: Protocol errors reply and close cleanly, never stall or eat the valid prefix

slug: protocol-error-lifetime · created: 2026-08-09 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: tests   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower the
     autonomy level to `manual` or `conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Touches (files · symbols · signatures):
- `src/protocol/mod.rs:parse_frame_zerocopy` — returns `Frame::Null` on ANY parse failure by
  standing policy (CLAUDE.md: malformed client input must never crash the server). That policy is
  why every malformed frame below is INDISTINGUISHABLE from a valid null at the call site: the
  parser discards the reason, so the handler cannot report one.
- `src/server/conn/handler_monoio/mod.rs` / `handler_sharded/mod.rs` — the read loops that decide
  what to do when a frame does not parse: today they break the loop, which closes the connection.
- `src/server/codec.rs:RespCodec` — inline-command length cap (the 70KB case below).
- `src/command/mod.rs` — the `unknown command` error text.

Context (working folder): `scripts/client-compat/manifest.yaml` (this surface has NO entries
today), `tests/` (no protocol-error test file exists).

Honors: `Frame::Error(Bytes)` for all command errors, never `Result` in dispatch; no `unwrap()` in
parsing; zero-copy `Bytes::slice`.

Anchors the contract cites: `parse_frame_zerocopy`, `RespCodec`, the two handler read loops.

### Measured against redis-server 8.6.1 (Moon single-shard, both on loopback, identical bytes)

Probe: `scratchpad/probe_ground.py` — raw sockets, because `redis-cli` cannot send a malformed
frame, two commands in one write, or a half-finished bulk. Each case records three things, since
"what it replied" hides the interesting half: the reply, whether the connection SURVIVED, and
whether a following `PING` still works.

| bytes sent | redis-server 8.6.1 | Moon | verdict |
|---|---|---|---|
| `*-9\r\n` | ignored; connection alive, next command works | **closes, NO error reply** | diverges |
| `*100000000\r\n` | waits for more data (alive) | **closes, NO error reply** | diverges |
| `$abc` / `$-5` / `$999999999` bulk len | `-ERR Protocol error: invalid bulk length` **then** closes | closes with **no error at all** | diverges |
| `GET "unclosed` | `-ERR Protocol error: unbalanced quotes in request` + close | replies `$-1` — **accepts it as a key** | diverges |
| `GET ` + 70 000 bytes inline | serves it (`$-1`), alive | **closes, no error** | diverges |
| `PING\r\n` + `*-9\r\n` (ONE write) | `+PONG` for the valid prefix, then handles the bad frame | **no reply at all — the valid prefix is EATEN** | diverges |
| `@bogus\r\nPING\r\n` (ONE write) | error + `+PONG`, alive | error + `+PONG`, then **STALLS** on the next command | diverges |
| `*1\r\n$4\r\nPING` (no trailing CRLF) | waits, alive | waits, alive | matches |
| `\r\n` (empty inline) | ignored, alive | ignored, alive | matches |
| `*2 $3 GET $1 kkkk` (len < data) | `$-1` then `-ERR unknown command 'k'` | same shape | matches |

Two failure MODES, both in the task title, both confirmed:
- **Eats the valid prefix** — `PING` before a bad frame in the same write gets no answer at all.
- **Stalls** — after `@bogus`, the connection accepts writes and never answers again (not closed,
  so the client waits on a socket that will never speak).

And a third the title did not predict: **Moon closes silently.** Redis names the fault
(`-ERR Protocol error: …`) before hanging up; Moon just disappears, so a driver reports a network
error where Redis reports a protocol bug in the driver.

Separate, smaller: the unknown-command text. Redis emits
`-ERR unknown command 'X', with args beginning with: 'a' 'b' ` and OMITS the suffix when there are
no args; Moon always emits the suffix and the arg list is ALWAYS empty.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: a malformed frame ends the connection the way Redis ends it — the valid prefix is
answered first, the fault is NAMED on the wire, and only then does the socket close. Never a silent
close, never a stall.

Framings weighed:
- **Name the fault at the boundary (chosen).** `ParseError` already carries a reason; it is
  DISCARDED by `Err(_) => break` in the handler read loops. Give `ParseError` a typed `kind`, map
  that kind to Redis's fixed wire string, and have the read loops answer the valid prefix, emit the
  error, then close. The parser keeps its detailed internal message for logs and fuzzing.
- *Rewrite every parser message to Redis's wording.* Rejected: it throws away the diagnostic detail
  the fuzz targets rely on ("invalid bulk string length: -5" localises a bug; "invalid bulk length"
  does not), to save a mapping function.
- *Reply a single generic `-ERR Protocol error`.* Rejected by the user at freeze: a driver author
  reading "invalid bulk length" fixes their encoder; one reading "Protocol error" files a bug
  against Moon.

Must:
<must>
  - A frame that fails to parse produces `-ERR Protocol error: <reason>` on the wire BEFORE the
    connection closes, using Redis 8.6.1's wording for the reason.
  - Every VALID frame that arrived before the malformed one in the same read is executed and
    answered first. `PING\r\n*-9\r\n` in one write answers `+PONG`.
  - After a command that produced an error but left the stream well-formed (`@bogus\r\nPING\r\n`),
    the connection keeps serving: the NEXT command still gets a reply.
  - An inline request over the cap replies `-ERR Protocol error: too big inline request` and closes.
    Moon already builds this exact string; it never reaches the client today.
  - `*-9\r\n` (negative multibulk count) is IGNORED, connection alive — measured Redis behavior.
  - An unterminated inline quote replies `-ERR Protocol error: unbalanced quotes in request` and
    closes. Moon currently ACCEPTS `GET "unclosed` and answers `$-1`, treating the quote as part of
    the key.
  - The three connection handlers agree, and so does the inline fast path.
</must>
Reject:
<reject>
  - malformed bulk length (`$abc`, `$-5`, `$999999999`) -> "ERR Protocol error: invalid bulk length"
  - malformed multibulk count (non-numeric) -> "ERR Protocol error: invalid multibulk length"
  - a non-`$` byte where an array element must start -> "ERR Protocol error: expected '$', got '<c>'"
  - inline line over `max_inline_size` -> "ERR Protocol error: too big inline request"
  - unbalanced quote in an inline request -> "ERR Protocol error: unbalanced quotes in request"
</reject>
After:
<after>
  - The client can always distinguish "my encoder is wrong" from "the network dropped": a protocol
    fault arrives as an error frame, never as a bare FIN/RST.
  - No byte a client sent before a fault is silently discarded.
  - No connection is left open-but-mute.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ That answering the valid prefix BEFORE closing is safe for every command in it — lowest
    confidence because the prefix may contain a write, and we are about to drop the connection, so
    a client that never reads the reply cannot tell whether the write landed. Redis has the same
    property, so parity is the tie-breaker; if wrong, the cost is a write acknowledged into a
    socket nobody reads (which is already true of any close-after-write).
  - [x] The reason is available at the point of failure — CONFIRMED: `ParseError::Invalid`
    carries `message` + `offset`; only the defensive zero-copy SECOND pass returns `Frame::Null`,
    and that pass runs only after validation already succeeded.
  - [x] Moon's inline cap is near Redis's — CONFIRMED by measurement: identical up to 65 530 B;
    Moon closes at 70 000 B where Redis still serves; both RST at 200 000 B.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: the valid prefix is answered before the fault  # Must 2
  Given a fresh connection
  When it writes "PING\r\n*-9\r\n" in ONE write
  Then it receives "+PONG"
  And the PING is not swallowed   # Moon answers nothing at all today

Scenario: a malformed bulk length names itself  # Must 1 · Reject 1
  Given a fresh connection
  When it sends a GET whose bulk header is "$abc"
  Then it receives "ERR Protocol error: invalid bulk length"
  And only then is the connection closed   # Moon closes with no error today

Scenario: a negative multibulk count is ignored, not fatal  # Must 5
  Given a fresh connection
  When it sends "*-9\r\n"
  Then no error is returned
  And the connection is still alive: a following PING answers "+PONG"

Scenario: an oversized inline request names itself  # Must 4 · Reject 4
  Given a fresh connection
  When it sends an inline command longer than the inline cap
  Then it receives "ERR Protocol error: too big inline request"
  And only then is the connection closed
  # Moon already BUILDS this exact string in src/protocol/inline.rs — it never reaches the client

Scenario: an unbalanced quote is rejected, not silently accepted  # Must 6 · Reject 5
  Given a fresh connection
  When it sends 'GET "unclosed\r\n'
  Then it receives "ERR Protocol error: unbalanced quotes in request"
  And no key lookup is performed   # Moon answers "$-1" today, treating the quote as key bytes

Scenario: an error does not stall the connection  # Must 3
  Given a fresh connection
  When it writes "@bogus\r\nPING\r\n" in one write
  Then it receives an error followed by "+PONG"
  And a further PING still answers   # Moon answers both, then STALLS on the next command

Scenario: every handler agrees  # Must 7
  Given the same malformed bytes
  When they are sent to a monoio server, a sharded-tokio server, and the inline fast path
  Then all three produce the same wire bytes and the same connection outcome
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
The contract is the RESP wire and the socket lifecycle.

ParseError gains a typed kind (no new allocation on the error path beyond the existing message):

  enum ProtoFault { BulkLen, MultibulkLen, ExpectedDollar(u8), InlineTooBig, UnbalancedQuotes,
                    MbulkCountTooBig, UnknownType(u8) }

  ParseError::Invalid { kind: ProtoFault, message: String, offset: usize }
                                ^^^^ NEW      ^^^^ kept: detailed, for logs + fuzz triage

ProtoFault::wire_text() -> &'static str, Redis 8.6.1 verbatim:
  BulkLen           -> "Protocol error: invalid bulk length"
  MultibulkLen      -> "Protocol error: invalid multibulk length"
  ExpectedDollar(c) -> "Protocol error: expected '$', got '<c>'"        (one formatted case)
  InlineTooBig      -> "Protocol error: too big inline request"
  UnbalancedQuotes  -> "Protocol error: unbalanced quotes in request"
  MbulkCountTooBig  -> "Protocol error: too big mbulk count string"
  UnknownType(c)    -> "Protocol error: expected '$', got '<c>'"        (Redis's own conflation)

Read-loop contract, IDENTICAL in all three handlers — replacing today's
`Err(_) => { break_outer = true; break; }` which discards BOTH the reason and `batch`:

  Err(ParseError::Incomplete) => break            (unchanged: wait for more bytes)
  Err(ParseError::Io)         => close, no reply  (unchanged: the socket is already gone)
  Err(ParseError::Invalid{kind, ..}) =>
        1. execute and flush `batch` — every valid frame parsed before the fault
        2. write "-ERR <kind.wire_text()>\r\n"
        3. flush, then close

Special case: a negative multibulk count is NOT a fault. `*-9\r\n` consumes its bytes and
yields no frame, matching Redis, so it never enters the Invalid arm at all.

No new state. No allocation added to the SUCCESS path — `wire_text()` returns &'static str and is
reached only when the connection is already terminating.
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-08-12)
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject has one test. Tests speak the raw socket — no
redis-rs — because the whole subject is bytes-and-close behavior that a client library hides.
Each asserts THREE things: the reply bytes, whether the connection closed, and whether a
following command still answers.

Plan:
<test_plan>
  - pe1_valid_prefix_answered_before_fault: "PING\r\n*-9\r\n" in one write -> "+PONG"
  - pe2_bad_bulk_len_names_itself: "$abc", "$-5", "$999999999" -> "invalid bulk length" then close
  - pe3_negative_multibulk_is_ignored: "*-9\r\n" -> no reply, connection alive
  - pe4_oversized_inline_names_itself: -> "too big inline request" then close
  - pe5_unbalanced_quote_rejected: 'GET "unclosed' -> "unbalanced quotes in request", NOT "$-1"
  - pe6_error_does_not_stall: "@bogus\r\nPING\r\n" -> error + "+PONG", then ANOTHER PING answers
  - pe7_handlers_agree: the same six payloads against monoio and sharded-tokio servers,
    asserting byte-identical replies and identical close behavior
  - pe8_wire_text_is_redis_verbatim: a pure unit test over ProtoFault::wire_text(), so a typo in
    an error string fails without needing a live server
</test_plan>

Tests live in: `tests/protocol_error_lifetime.rs` · MUST run red before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/protocol/` `src/server/conn/` `tests/protocol_error_lifetime.rs` `scripts/client-compat/manifest.yaml`
Strategy (ordered batches):
  1. Add ProtoFault + wire_text(); tag every existing ParseError::Invalid construction site with
     its kind, keeping the detailed message untouched.
  2. Make "*-9" consume-and-yield-nothing instead of an error.
  3. Fix the inline parser's unbalanced-quote acceptance.
  4. Replace the three handlers' `Err(_) => break` with the contracted execute-prefix / reply /
     close sequence.
  5. Chase the "@bogus" stall to its cause and fix it (cause unknown at freeze — see the flag).
  6. Retire the corresponding compat-harness waivers.
Safety rule (feature-specific): NOTHING here may add an unwrap, an expect, or a panic to a parse
path. Malformed client input must never crash the server — that invariant outranks every parity
goal in this task, and the fuzz targets are the standing proof.
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
