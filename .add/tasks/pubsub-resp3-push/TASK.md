# TASK: Pub/sub Push frames, subscriber-mode rules, and sharded pub/sub

slug: pubsub-resp3-push · created: 2026-08-09 · stage: production
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

Touches (files · symbols · signatures):
- `src/pubsub/mod.rs:468-501` — `subscribe_response` / `unsubscribe_response` /
  `psubscribe_response` / `punsubscribe_response`. All four hardcode `Frame::Array` and take
  **no protocol argument**, so they cannot answer differently under RESP3. This is the single
  cause of divergence A below.
- `src/pubsub/mod.rs:437-534` — `message_frame_push` / `pmessage_frame_push` and the lazy
  pre-serialisation pair. The DELIVERY path already does RESP3 correctly; only the
  CONFIRMATION path does not. Whatever fix lands must not disturb the pre-serialisation
  (one serialise per PUBLISH, not per subscriber).
- `src/pubsub/subscriber.rs:12` — the per-subscriber `resp3` flag, already threaded from
  `HELLO 3`. The information the confirmation builders need already exists; it is simply not
  passed to them.
- The subscriber-mode allow-list, duplicated in THREE handlers with TWO different texts:
  `handler_monoio/mod.rs:785` · `handler_sharded/pubsub.rs:240` (both
  `"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT"`) and `handler_single.rs:437`
  (the same list plus `/ HELLO`). Three copies, two texts, and neither matches Redis.
- `src/command/mod.rs` — the `phf` dispatch table: no `SSUBSCRIBE`, `SUNSUBSCRIBE` or
  `SPUBLISH` entry exists, and `PUBSUB` has no `SHARDCHANNELS`/`SHARDNUMSUB` subcommand.

Context (working folder): `scripts/client-compat/manifest.yaml` (the parity verifier) ·
  `tests/` (no pub/sub protocol-level test file exists) · probes in
  `scratchpad/probe_pubsub.py` + `probe_pubsub2.py`.

Honors: `Frame::Error(Bytes)` for errors, never `Result` in dispatch; pub/sub envelopes are
  pre-serialised at most once per PUBLISH (`src/pubsub/mod.rs:422`); new commands need a `phf`
  entry + ACL category + `test-consistency.sh` / `test-commands.sh` rows.

Anchors the contract cites: the four `*_response` builders, `Subscriber::resp3`, the
  subscriber-mode allow-list, `COMMAND_META`.

### Measured against redis-server 8.6.1 (Moon @ `cfb83a92`, `--shards 1`, raw sockets)

Raw sockets, not a client library — the entire question is which BYTE leads the frame (`>` vs
`*`), and a client library normalises exactly that away before a test could see it.

| case | redis-server 8.6.1 | Moon | verdict |
|---|---|---|---|
| RESP3 `message` delivery | `>3 message ch hi` | identical | **matches** |
| RESP3 `pmessage` delivery | `>4 pmessage …` | identical | **matches** |
| RESP2 `message` delivery | `*3 message ch hi` | identical | **matches** |
| RESP3 SUBSCRIBE confirmation | `>3 subscribe ch :1` | **`*3 …`** | diverges |
| RESP3 PSUBSCRIBE confirmation | `>3 psubscribe q.* :1` | **`*3 …`** | diverges |
| RESP3 UNSUBSCRIBE confirmation | `>3 unsubscribe ch :0` | **`*3 …`** | diverges |
| RESP3 PUNSUBSCRIBE confirmation | `>3 punsubscribe p.* :0` | **`*3 …`** | diverges |
| RESP3 + subscribed: `GET k` | `_` (allowed) | **rejected** | diverges |
| RESP3 + subscribed: `SET k v` | `+OK` (allowed) | **rejected** | diverges |
| RESP3 + subscribed: `PING` | `+PONG` | **`*2 pong ""`** | diverges |
| RESP2 + subscribed: `PING` | `*2 pong ""` | identical | matches |
| RESP2 + subscribed: `GET k` text | `…only (P\|S)SUBSCRIBE / (P\|S)UNSUBSCRIBE / PING / QUIT / RESET…` | `…only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT…` | diverges |
| RESP2 + subscribed: `RESET` | `+RESET` | **rejected** | diverges |
| RESP2 + subscribed: `SSUBSCRIBE` | `*3 ssubscribe sch :1` | **rejected** | diverges |
| RESP2 + subscribed: `HELLO 3` | rejected (not on the allow-list) | rejected | matches |
| `SSUBSCRIBE` | `*3 ssubscribe sch :1` | **unknown command** | diverges |
| `SUNSUBSCRIBE` | `*3 sunsubscribe sch :0` | **unknown command** | diverges |
| `SPUBLISH` + delivery | `:1` then `*3 smessage sch hi` | **unknown command** | diverges |
| `PUBSUB SHARDCHANNELS` / `SHARDNUMSUB` | `*1 sch` / `*2 sch :1` | **unknown subcommand** | diverges |
| `PUBSUB NUMPAT`, 2 subs on ONE pattern | `:1` | **`:2`** | diverges (#480) |
| `PUBSUB NUMPAT`, after one leaves | `:1` | `:1` | matches — and this MASKS the bug |
| `PUBSUB CHANNELS news.*` | `*2 news.biz news.tech` | identical | matches |
| `SPUBLISH` reaching a plain `SUBSCRIBE` | not delivered | not delivered | matches |
| `UNSUBSCRIBE` (no args), not subscribed | `*3 unsubscribe $-1 :0` | **`*3 unsubscribe $0"" :0`** | diverges |

**The headline is A, the confirmations.** Delivery is already correct, which is what makes this
dangerous rather than merely incomplete: a RESP3 client tells an out-of-band push from a command
reply by the leading byte. Moon's `message` arrives as `>` (out-of-band, correct), but its
`subscribe` confirmation arrives as `*` — so a client that dispatches on frame type reads the
confirmation as the reply to whatever command it sends NEXT, and every subsequent reply on that
connection is off by one. A half-correct RESP3 implementation desynchronises clients that a
fully-absent one would not.

**B is a silent capability loss.** RESP3 exists partly to let one connection both subscribe and
issue commands; Moon keeps the RESP2 jail in RESP3, so a client that legitimately multiplexes
gets an error where Redis works.

**Two method notes, recorded so the cost is not paid twice:**
- `PUBSUB NUMPAT` is right in the obvious test and wrong in the real one. With two subscribers on
  one pattern Moon answers `:2`; after one leaves it answers `:1` — the same value Redis gives —
  so a test that subscribes, unsubscribes and checks would PASS against the bug. Only the
  concurrent-two-subscribers shape exposes it.
- The `UNSUBSCRIBE`-with-no-args ORDER differs (Redis emitted `b` then `a`, Moon `a` then `b`).
  That is Redis's internal dict iteration order and is **not contractual** — no test may pin it.
  What is contractual is one message per channel and the count descending to 0.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: pub/sub speaks RESP3 the whole way — confirmations are Push frames like the deliveries
already are, subscriber mode obeys the protocol's own rules, and the sharded channel namespace
exists.

Framings weighed:
- **Make the protocol version an argument to the four confirmation builders (chosen).** The
  `resp3` flag already lives on the subscriber; the builders simply never receive it. Passing it
  is a signature change to four small functions with one call site each per handler, and it puts
  the RESP2/RESP3 decision in exactly one place per message kind — the same place the delivery
  path already makes it.
- *Rewrite the frame at the write boundary — serialise Array, then patch `*`→`>` for RESP3
  connections.* Rejected: it makes the wire form depend on a byte-level rewrite far from where
  the semantics live, and it would silently convert any OTHER array reply that happens to pass
  through the same path. The bug being fixed is exactly a frame that means one thing and is
  typed as another; curing it with a blind retype invites the next one.
- *Leave confirmations as Array and document it.* Rejected: this is not a cosmetic gap. A client
  that dispatches on frame type reads the Array confirmation as the reply to its next command and
  every later reply on that connection is off by one — worse than not supporting RESP3 at all.

Must:
<must>
  - Under RESP3, the `subscribe`, `unsubscribe`, `psubscribe` and `punsubscribe` confirmations are
    Push frames (`>`), matching the `message`/`pmessage` deliveries that already are.
  - Under RESP2 every one of those confirmations stays an Array (`*`). Existing clients see no
    change whatsoever.
  - Under RESP3 a subscribed connection may run ANY command. `GET`, `SET` and the rest answer
    normally instead of being refused.
  - Under RESP2 a subscribed connection stays restricted, and the allow-list is Redis's:
    `SUBSCRIBE` · `UNSUBSCRIBE` · `PSUBSCRIBE` · `PUNSUBSCRIBE` · `SSUBSCRIBE` · `SUNSUBSCRIBE` ·
    `PING` · `QUIT` · `RESET`.
  - `RESET` works while subscribed (`+RESET`) and returns the connection to its normal state.
  - Under RESP2 a subscribed `PING` answers `*2 pong ""`; under RESP3 it answers `+PONG`.
    Moon gives the RESP2 shape in both today.
  - All THREE handlers and the inline fast path give the SAME answer to every rule above. The
    allow-list is stated once and consulted from each, not copied.
  - `SSUBSCRIBE` / `SUNSUBSCRIBE` / `SPUBLISH` exist, with `smessage` as the delivery event name
    and confirmations that follow the same RESP2-Array / RESP3-Push rule as their unsharded
    counterparts.
  - The sharded namespace is SEPARATE: `SPUBLISH ch` never reaches a plain `SUBSCRIBE ch`, and
    `PUBLISH ch` never reaches an `SSUBSCRIBE ch`.
  - `PUBSUB SHARDCHANNELS [pattern]` and `PUBSUB SHARDNUMSUB [ch …]` answer for the sharded
    namespace only.
  - `PUBSUB NUMPAT` counts DISTINCT patterns, not subscribers: two connections on `p.*` is `:1`.
  - `UNSUBSCRIBE` with no arguments on a connection subscribed to nothing replies a Null channel
    name (`$-1`), not an empty string.
</must>
Reject:
<reject>
  - any command outside the allow-list while subscribed under RESP2 -> "ERR Can't execute
    '<cmd>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this
    context" (verbatim, measured — `<cmd>` lowercased)
  - `HELLO` while subscribed under RESP2 -> the same error. It is NOT on the allow-list, so a
    RESP2 subscriber cannot upgrade mid-subscription (measured; `handler_single.rs` currently
    advertises HELLO as allowed in its error text while refusing it)
  - `SSUBSCRIBE` / `SUNSUBSCRIBE` with no arguments -> "ERR wrong number of arguments for
    '<cmd>' command"
  - `SPUBLISH` with wrong arity -> "ERR wrong number of arguments for 'spublish' command"
</reject>
After:
<after>
  - A RESP3 client can tell an out-of-band push from a command reply by the leading byte alone,
    for every pub/sub frame Moon emits — no frame means one thing and is typed as another.
  - One connection can subscribe and issue commands under RESP3, which is a reason RESP3 exists.
  - The three handlers cannot drift apart again: there is one allow-list, not three.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ That lifting the subscriber-mode restriction under RESP3 does not break Moon's own reply
    ordering — lowest confidence because a subscribed RESP3 connection may now have a command
    reply and a delivery in flight at once, and Moon's handlers were written assuming a subscribed
    connection only ever WRITES pushes. If the two can interleave mid-frame the connection emits
    corrupt bytes, which is strictly worse than the refusal it replaces. Mitigation: a test that
    publishes to the connection's own channel and then issues a command on it, asserting both
    frames arrive intact and in a legal order (measured Redis: the push, then the reply).
    Cost if wrong: a torn frame on a multiplexed connection — the loudest possible failure, which
    is at least why the test can catch it.
  ⚠ That sharded pub/sub can be implemented with standalone semantics now and made
    cluster-correct later — lowest confidence after the above because in a real cluster
    `SSUBSCRIBE` is served by the slot's owner and clients rely on that routing, which is
    `cluster-client-bootstrap`'s territory, not this task's. The probe measured a STANDALONE
    redis-server, so standalone semantics are what parity means here. Cost if wrong: the routing
    layer lands later and the channel registry needs a slot key it was not built with.
  - [x] The `resp3` flag reaches the confirmation site — CONFIRMED: `Subscriber` already carries
    it (`src/pubsub/subscriber.rs:12`) and the delivery path already branches on it.
  - [x] `PING`'s subscriber-mode shape is protocol-dependent, not mode-dependent — CONFIRMED by
    measurement: RESP2 subscribed gives `*2 pong ""`, RESP3 subscribed gives `+PONG`.
  - [x] The pre-serialise-once optimisation is not in the way — CONFIRMED: it covers
    `message`/`pmessage` deliveries (one serialise per PUBLISH), not the per-connection
    confirmations, which are built once per subscribed channel anyway.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: a RESP3 subscribe confirmation is a Push frame   # Must 1
  Given a connection that has sent HELLO 3
  When it sends SUBSCRIBE ch
  Then the confirmation's leading byte is ">" not "*"
  And its contents are unchanged: subscribe, ch, 1

Scenario: RESP2 confirmations are untouched   # Must 2
  Given a connection that has NOT sent HELLO 3
  When it subscribes and then unsubscribes
  Then every confirmation is an Array ("*"), byte for byte as today

Scenario: RESP3 lifts the subscriber jail   # Must 3
  Given a RESP3 connection subscribed to ch
  When it sends SET k v and then GET k
  Then it receives +OK and then the value
  And it is STILL subscribed: a publish to ch still reaches it

Scenario: a reply and a delivery on one RESP3 connection do not tear   # Must 3 (⚠ flag)
  Given a RESP3 connection subscribed to ch
  When a message is published to ch and the connection then sends PING
  Then both frames arrive whole and parse cleanly
  And neither is truncated or interleaved into the other

Scenario: RESP2 stays jailed, with Redis's wording   # Must 4 · Reject 1
  Given a RESP2 connection subscribed to ch
  When it sends GET k
  Then it receives the verbatim Redis error naming (P|S)SUBSCRIBE … / RESET
  And the connection is still alive and still subscribed

Scenario: RESET escapes subscriber mode   # Must 5
  Given a RESP2 connection subscribed to ch
  When it sends RESET
  Then it receives +RESET
  And a following GET k is answered normally — it is no longer subscribed

Scenario: PING's shape follows the protocol, not the mode   # Must 6
  Given two subscribed connections, one RESP2 and one RESP3
  When each sends PING
  Then the RESP2 one receives *2 pong ""
  And the RESP3 one receives +PONG

Scenario: every handler agrees   # Must 7
  Given the same bytes sent to a 1-shard and a 4-shard server, on both runtimes
  When each rule above is exercised
  Then the wire bytes are identical

Scenario: sharded pub/sub delivers   # Must 8
  Given a connection that has sent SSUBSCRIBE sch
  When another connection sends SPUBLISH sch hi
  Then the subscriber receives smessage, sch, hi
  And SPUBLISH answered :1

Scenario: the two namespaces do not leak   # Must 9
  Given one connection on SUBSCRIBE dual and another on SSUBSCRIBE dual
  When SPUBLISH dual hi is sent
  Then only the SSUBSCRIBE connection receives it
  And the plain subscriber receives nothing at all

Scenario: PUBSUB reports the sharded namespace separately   # Must 10
  Given a connection subscribed with SSUBSCRIBE sch
  When PUBSUB SHARDCHANNELS and PUBSUB SHARDNUMSUB sch are issued
  Then they report sch and a count of 1
  And PUBSUB CHANNELS does NOT list sch

Scenario: NUMPAT counts patterns, not subscribers   # Must 11
  Given TWO connections each subscribed to the same pattern p.*
  When PUBSUB NUMPAT is issued
  Then it answers 1
  # Both must be subscribed AT ONCE: after one leaves, the buggy and correct
  # answers coincide at 1 and the test would pass against the bug.

Scenario: unsubscribing from nothing names a Null channel   # Must 12
  Given a connection subscribed to nothing
  When it sends UNSUBSCRIBE with no arguments
  Then the channel name in the reply is Null ($-1), not an empty string
  And the count is 0

Scenario: sharded verbs are allowed while subscribed under RESP2   # Must 4
  Given a RESP2 connection subscribed to ch
  When it sends SSUBSCRIBE sch
  Then it is accepted, not refused
  And the connection is subscribed to both namespaces

Scenario: HELLO cannot be used to escape subscriber mode   # Reject 2
  Given a RESP2 connection subscribed to ch
  When it sends HELLO 3
  Then it receives the allow-list error
  And it is still a RESP2 connection: confirmations stay Arrays

Scenario: the sharded verbs check their arity   # Reject 3 · Reject 4
  Given a fresh connection
  When it sends SSUBSCRIBE with no arguments, then SPUBLISH with one
  Then each receives "wrong number of arguments" naming that command
  And the connection stays open
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

The contract here is WIRE BYTES, not a signature — every clause below is a frame a client
receives, and each was measured against redis-server 8.6.1 rather than read from a spec.

```
Confirmations — src/pubsub/mod.rs
  subscribe_response(channel, count, resp3: bool)   -> resp3 ? Push : Array
  unsubscribe_response(channel: Option<&Bytes>, count, resp3) -> resp3 ? Push : Array
       channel None  => Frame::Null  ($-1)   [UNSUBSCRIBE with no args, nothing subscribed]
  psubscribe_response(pattern, count, resp3)        -> resp3 ? Push : Array
  punsubscribe_response(pattern: Option<&Bytes>, count, resp3) -> resp3 ? Push : Array
  ssubscribe_response / sunsubscribe_response       -> same rule, verbs "ssubscribe"/"sunsubscribe"
  Deliveries (unchanged): message · pmessage · NEW smessage — Push under RESP3, Array under RESP2

Subscriber mode — stated ONCE, consulted by all three handlers + the inline path
  allowed_in_subscriber_mode(cmd) -> bool
      SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE SSUBSCRIBE SUNSUBSCRIBE PING QUIT RESET
  gate: RESP2 && subscribed && !allowed  =>
      "ERR Can't execute '<cmd-lowercased>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING /
       QUIT / RESET are allowed in this context"
  RESP3 && subscribed  => NO gate at all; every command dispatches normally
  PING while subscribed: RESP2 -> Array["pong", ""] · RESP3 -> SimpleString "PONG"

Commands (phf dispatch + COMMAND_META + ACL category @pubsub)
  SSUBSCRIBE   ch [ch ...]    arity -2   -> one confirmation per channel
  SUNSUBSCRIBE [ch ...]       arity -1   -> one per channel; none subscribed => Null name, :0
  SPUBLISH     ch message     arity  3   -> Integer: receivers in the SHARDED namespace only
  PUBSUB SHARDCHANNELS [pat]             -> Array of sharded channel names
  PUBSUB SHARDNUMSUB [ch ...]            -> flat Array of ch, count pairs
  PUBSUB NUMPAT                          -> Integer: DISTINCT patterns (not subscribers)

Registry: the sharded namespace is a SEPARATE channel map from the plain one. No key is shared,
so no publish can cross between them. Per-shard locks only; no global lock on the publish path.
```

Not in scope, stated so the boundary is explicit: cluster slot ROUTING for `SSUBSCRIBE` (a real
cluster serves it from the slot's owner). The probe measured a standalone redis-server, so
standalone semantics are what parity means here; routing belongs to `cluster-client-bootstrap`.

Status: FROZEN @ v1 — approved by Tin Dang, 2026-08-14.
Approved with both flags surfaced: all three halves ship as ONE PR, and if the RESP3 jail-lifting
turns out to risk torn frames the answer is to fix the write path properly (serialise the two
writers) rather than to ship the framing fix alone. Recorded here because it converts flag 1 from
"we may fall back" into "there is no fallback" — the tear test is now a blocker, not a probe.

Lowest-confidence flags, surfaced for the freeze:
1. `[spec]` **Lifting the RESP2 jail under RESP3 is the risky half, not the Push framing.** A
   subscribed RESP3 connection can now have a command reply and a delivery in flight at once, and
   Moon's handlers were written assuming a subscribed connection only ever writes pushes. If those
   two writers can interleave mid-frame the connection emits corrupt bytes — strictly worse than
   the refusal being removed. A scenario is dedicated to it; if it cannot be made safe, the honest
   fallback is to ship the Push framing and keep the jail, and say so.
2. `[contract]` **Sharded pub/sub is contracted standalone-only.** `SSUBSCRIBE` in a real cluster
   is answered by the slot owner and clients depend on that. Building the sharded registry without
   a slot key now may mean re-keying it when cluster routing lands.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject has a test; no Must is represented only by prose.

Tests read RAW SOCKET BYTES, never a client library. The entire contract is which byte leads the
frame, and every Rust/Python Redis client normalises `>` and `*` to the same value before a test
could see the difference — a library-based suite would pass against the bug it exists to catch.

<test_plan>
  - ps1_resp3_subscribe_confirmation_is_push — HELLO 3 / SUBSCRIBE ch; assert leading `>` AND the
    three elements unchanged. (Must 1)
  - ps2_resp3_all_four_confirmations_are_push — subscribe · unsubscribe · psubscribe ·
    punsubscribe in one connection; all four lead with `>`. (Must 1)
  - ps3_resp2_confirmations_stay_array — the same four without HELLO 3; all lead with `*`. The
    regression guard for existing clients. (Must 2)
  - ps4_resp3_subscribed_connection_runs_commands — SET then GET while subscribed; both answered,
    and a publish still arrives afterwards so the subscription survived. (Must 3)
  - ps5_resp3_reply_and_delivery_do_not_tear — publish to the connection's OWN channel, then PING;
    parse the buffer frame by frame and assert both are whole. The ⚠ flag's test. (Must 3)
  - ps6_resp2_jail_error_is_verbatim — GET while subscribed; byte-exact error, connection alive
    and still subscribed. (Must 4 · Reject 1)
  - ps7_resp2_allow_list_admits_sharded_verbs — SSUBSCRIBE while subscribed is accepted. (Must 4)
  - ps8_reset_escapes_subscriber_mode — RESET gives +RESET and a following GET is answered.
    (Must 5)
  - ps9_ping_shape_follows_protocol — RESP2 subscribed `*2 pong ""`, RESP3 subscribed `+PONG`.
    (Must 6)
  - ps10_hello_cannot_escape_the_jail — HELLO 3 while subscribed is refused AND confirmations stay
    Arrays afterwards. (Reject 2)
  - ps11_sharded_publish_delivers_smessage — SSUBSCRIBE / SPUBLISH; `smessage`, and `:1`.
    (Must 8)
  - ps12_namespaces_do_not_leak — SPUBLISH reaches only the SSUBSCRIBE side; PUBLISH only the
    plain side. Asserts the NEGATIVE on both, which is the half a happy-path test omits. (Must 9)
  - ps13_pubsub_shard_introspection — SHARDCHANNELS / SHARDNUMSUB report the sharded namespace,
    and PUBSUB CHANNELS does NOT list a sharded channel. (Must 10)
  - ps14_numpat_counts_distinct_patterns — TWO connections subscribed to `p.*` AT ONCE. Both must
    be live: after one leaves, the buggy and correct answers coincide at 1. (Must 11)
  - ps15_unsubscribe_from_nothing_names_null_channel — `$-1`, not `$0`. (Must 12)
  - ps16_sharded_verbs_check_arity — SSUBSCRIBE with no args, SPUBLISH with one; each names its
    own command and the connection stays open. (Reject 3 · Reject 4)
  - ps17_all_handlers_agree — the confirmation-framing and jail rules re-run at `--shards 4`, so
    a fix landing in one handler and not the others fails here. (Must 7)
</test_plan>

Runtime note, learned the expensive way on `info-observability`: this suite MUST run under
`runtime-monoio` (the shipped default) as well as `runtime-tokio`. PR CI runs tokio, and a
handler-order bug that exists only in the monoio path is invisible to it — that is exactly how
#477 survived.

Harness: `scripts/client-compat/manifest.yaml` gains live parity entries for the RESP3
confirmation framing and the subscriber-mode allow-list, so the oracle keeps them honest.

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
