# TASK: Pub/sub Push frames, subscriber-mode rules, and sharded pub/sub

slug: pubsub-resp3-push · created: 2026-08-09 · stage: production
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

Least-sure flag surfaced at freeze:
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
  - ps18_sharded_delivery_crosses_shards — `--shards 4`, 8 subscribers on sc0..sc7. Written
    BEFORE the cross-shard wiring so a local-only registry fails loudly instead of passing every
    `--shards 1` test while dropping (N-1)/N of deliveries. (Must 9)
  Added after review, from defects the suite above did not reach:
  - ps19_ssubscribe_without_args_inside_subscriber_mode_answers — arity from INSIDE subscriber
    mode. ps16 uses a fresh connection, which takes the normal dispatch path; the subscriber-loop
    arm is separate code and answered NOTHING at all. (Reject 3, second path)
  - ps20_reset_clears_sharded_subscriptions_too — RESET cleared plain and pattern but not
    sharded, so the connection left subscriber mode still registered for `smessage`. (Must 8)
  - ps21_disconnect_clears_sharded_subscriptions — the same teardown gap on disconnect. Note it
    passed BEFORE the fix: `spublish_shared` reconciles a dead subscriber, so the count
    self-heals and only the remote-map leak remains. Kept as an observable-contract guard, but
    it is honestly not what proves that fix. (Must 8)
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

Scope (may touch): `src/pubsub/` `src/server/conn/` `src/shard/` `src/command/metadata.rs` `tests/pubsub_resp3_push.rs` `scripts/client-compat/manifest.yaml` `scripts/test-commands.sh` `/CHANGELOG.md`
Strategy (ordered batches):
1. Confirmation framing — the four `Frame::Array` confirmation builders in `src/pubsub/mod.rs`
   become `Frame::Push`. Measured first that `serialize.rs:102` already downgrades Push→Array
   for RESP2 exactly as it does for `Frame::Set`, so no `resp3: bool` needs threading through
   the builders and RESP2 output stays byte-identical. This is batch 1 because it is the
   headline defect and it touches one file.
2. Subscriber-mode rule — extract the allow-list into `src/server/conn/subscriber_mode.rs`
   stated once, with its own unit tests, then have all three handlers call it. Lift the jail
   for RESP3 (`subscription_count > 0 && protocol_version < 3`) and add a delivery select-arm
   to the normal loop in the monoio and sharded handlers.
3. Sharded pub/sub — a `shard_channels` map beside `channels` in the registry, the SSUBSCRIBE /
   SUNSUBSCRIBE / SPUBLISH / SHARDCHANNELS / SHARDNUMSUB verbs across all three handlers, and
   the cross-shard leg (`remote_subscriber_map.shard_channels` + an `SPublishBatch` message).
   Last because it is the largest and depends on batch 1's frame shape.
Safety rule (feature-specific): the sharded and plain channel namespaces must be SEPARATE MAPS,
never one map with a flag each call site remembers to check. `SPUBLISH ch` reaching a
`SUBSCRIBE ch` (or the reverse) is a correctness break a client cannot defend against, and the
structural separation is what makes it unrepresentable rather than merely untested.
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

- [x] all tests pass — 18/18 `tests/pubsub_resp3_push.rs` under BOTH runtimes (monoio 4610 lib
      tests, tokio+jemalloc 3773); regression suites `pubsub_kv_ordering` 2/2,
      `multi_exec_queue_semantics` 12 + 1 known-ignored, `protocol_error_lifetime` 8/8,
      `info_observability` 13/13, `keyspace_notifications` 10/10; `test-commands.sh --category
      pubsub` 5/5; compat harness PASS=199 FAIL=0 WAIVED=15 against live redis-server 8.6.1.
- [x] coverage did not decrease — 18 new integration tests + 5 unit tests in
      `subscriber_mode.rs`; no test removed or weakened.
- [x] no test or contract was altered during build — §3 stayed FROZEN @ v1. One contracted
      detail proved unnecessary rather than wrong (see the §3 note on `resp3: bool`): the
      OBSERVABLE contract is unchanged, the implementation is simpler than the sketch.
- [x] the green was EARNED — the tokio leg was 12/18 RED when monoio was already 18/18, from
      four independent gaps (unconditional RESP2 serialize, no jail lift, no delivery arm, no
      sharded verbs). A suite that could not tell the two runtimes apart would have been the
      cheat; this one did, on the exact axis the repo has been burned by before. `ps18` was
      written BEFORE the cross-shard wiring specifically so a local-only registry would fail
      loudly instead of passing every `--shards 1` test while dropping (N-1)/N in production.
- [x] concurrency / timing of the risky operation is safe — the RESP3 jail lift puts a delivery
      branch next to command handling in one `select!`. Reply and delivery cannot interleave
      mid-frame: both are whole frames written by the same task, and the loop only parks when
      no reply is in flight. Verified empirically, not by argument — `ps5` parses the buffer
      frame-by-frame and requires every byte to belong to a whole frame. `spublish_shared`
      snapshots under a read lock, serializes + fans out lock-free, and reconciles slow
      subscribers under a write lock — no lock held across the fan-out.
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new dependency,
      no `unsafe`, no `unwrap`/`expect` outside tests. Channel names are opaque `Bytes` used as
      map keys only. `SPUBLISH` carries ACL category PUB, matching PUBLISH.
- [x] layering & dependencies follow CONVENTIONS.md — the rule lives in one module rather than
      being restated per handler (that duplication WAS the bug: three texts, two behaviours);
      registry logic stays in `src/pubsub/`, wire concerns in `src/server/conn/`.
- [x] a person reviewed and approved the change — PR #483. Review found THREE real defects the
      §6 wiring check above had passed, all on TEARDOWN paths, all in code this task added:
      (a) the monoio subscriber-loop `SSUBSCRIBE` arm had no arity guard, so `SSUBSCRIBE` with no
      channel from inside RESP2 subscriber mode wrote NOTHING and the client waited for its own
      timeout — silence being the one failure a client cannot distinguish from a slow server;
      (b) `RESET` cleared the plain and pattern namespaces but not `shard_channels`, so the
      connection left subscriber mode still registered for `smessage` — an unsolicited push
      injected into the reply stream of a connection issuing ordinary commands, which is
      precisely the desynchronisation defect this whole task exists to remove, reintroduced in a
      new place; (c) no teardown path called `sunsubscribe_all` at all, so disconnect left every
      other shard fanning SPUBLISH batches at a shard with no local receiver for the life of the
      process. Fixed with ps19/ps20/ps21 + three registry unit tests, red-first.

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [x] A RESP3 client can subscribe and then issue an ordinary command on the same connection,
      and its frame-type dispatch stays in sync — every confirmation leads with `>`, every
      command reply leads with its own type byte, forever. — confirmed by ps1–ps5 asserting the
      LEADING BYTE (not the payload) of each confirmation, and ps5 additionally requiring the
      whole buffer to split cleanly into frames so no reply/delivery tear is possible.
- [x] A RESP2 client sees byte-for-byte what it saw before the change. — confirmed by ps6/ps7
      pinning the `*3`-Array form, and by the compat harness diffing Moon's raw bytes against
      redis-server 8.6.1 for both protocols (PASS=199 FAIL=0).
- [x] `SPUBLISH ch` never reaches a `SUBSCRIBE ch` subscriber and vice versa, even though the
      channel NAME is identical. — confirmed by ps12/ps13, which subscribe both ways to the same
      name and assert each publish counts exactly 1 receiver and delivers to exactly one side.
- [x] Sharded delivery works when the subscribers are spread across shard threads, not just at
      `--shards 1`. — confirmed by ps18: `--shards 4`, 8 connections on sc0..sc7, every SPUBLISH
      returns `:1` and all 8 `smessage` deliveries arrive.
- [x] `PUBSUB NUMPAT` answers a count of DISTINCT PATTERNS, including when subscribers to one
      pattern land on different shard threads. — confirmed by ps14 keeping both subscribers live
      (the only shape that exposes it — after one leaves, buggy and correct both answer 1).
- [x] `COMMAND` no longer advertises a verb Moon cannot run. — confirmed by SSUBSCRIBE /
      SUNSUBSCRIBE / SPUBLISH all executing, where two were in `COMMAND_META` but answered
      "unknown command" and one was absent from the table entirely. This also feeds #472, which
      validates MULTI queue-time arity against that same table.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — INCOMPLETE AS FIRST RUN; corrected after review. The check below walked
      every path a new verb is DISPATCHED on and found the tokio gaps, but it never walked the
      paths a subscription is TORN DOWN on — and all three post-review defects lived there. The
      lesson is recorded as a competency delta: a new piece of connection state needs its
      teardown paths enumerated as deliberately as its dispatch paths, because `sunsubscribe_all`
      existed, compiled, was called from exactly one place, and nothing flagged that RESET and
      disconnect were not among them. This repo has THREE dispatch paths (monoio, sharded/tokio, single) plus an
      inline fast path, and a rule added to one is CI-invisible in the others. Every new verb was
      confirmed reachable in all of them: `handler_monoio/{mod,pubsub}.rs`,
      `handler_sharded/{mod,pubsub}.rs`, `handler_single`, and `COMMAND_META`. The wiring check
      is not ceremony here — the tokio leg was genuinely 12/18 red after the monoio leg was
      green, and the 7th UNSUBSCRIBE call site was missed by a single-line regex sweep because
      it was formatted across lines (ps15 caught it, still returning `$0`).
- [x] DEAD-CODE (code) — clippy clean with `--all-targets` on default features AND
      `--no-default-features --features runtime-tokio,jemalloc`; no `dead_code` allow added. The
      `pending_wakers` relay was already deleted in an earlier wave and was not resurrected —
      the cross-shard reply path awaits a `flume` oneshot directly.
- [x] SEMANTIC (prose / non-code) — the §0 divergence table (18 rows) was re-read row by row
      against the final build; each row now has a named test. CHANGELOG entries state the
      divergence in terms of what a CLIENT observes rather than what the code did, since that
      is the axis this milestone is measured on.

### GATE RECORD
Outcome: PASS
If RISK-ACCEPTED -> owner: <name> · ticket: <link> · expires: <date>   (never for a security gap)
Reviewed by: Tin Dang · date: 2026-08-14

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): the compat harness is the standing monitor — the 7 new
manifest entries diff Moon's raw bytes against a live redis-server on every run, so a regression
in confirmation framing surfaces as a FAIL row rather than as a client bug report months later.
Beyond that: `pubsub_patterns` in INFO must equal `PUBSUB NUMPAT` (they now share one gather, so
a divergence means someone re-forked them), and `SPUBLISH` receiver counts at `--shards > 1`
should track subscriber counts — a systematic (N-1)/N shortfall is the cross-shard leg breaking.

### Spec delta
- [SPEC · open] `HELLO` mid-subscription stays refused, matching measured Redis 8.6.1 behaviour.
  If a future Redis permits the upgrade this becomes a divergence (evidence: ps10_hello_cannot_escape_the_jail
  pins the refusal; the §0 table records it as measured-and-matched, not assumed).
- [SPEC · open] Sharded pub/sub is standalone-only — it does not yet consult cluster slot
  ownership, so in cluster mode `SPUBLISH` fans out within this node rather than to the slot's
  shard. Belongs with `cluster-client-bootstrap` (evidence: §3 ⚠ assumption 2, recorded before
  the freeze).
- [SPEC · open] `UNSUBSCRIBE` with no arguments emits confirmations in a different ORDER than
  Redis. Deliberately unpinned: Redis's order is dict-iteration order, so it is not contractual
  and a test asserting it would encode an accident (evidence: measured during §0, left untested).
- [SPEC · open] `Frame::NullArray` is still missing, so a Null Array and a Null Bulk cannot be
  distinguished on the wire — the same root cause as the BLPOP-in-MULTI risk accepted in
  `multi-exec-queue-semantics` (evidence: #482).

### Competency deltas
- [TDD · open] Write the multi-shard test BEFORE the multi-shard wiring, not after. `ps18` was
  authored while the registry was still local-only precisely so it would fail loudly; had it
  been written afterwards it would have been shaped to the code and a local-only registry would
  have passed every `--shards 1` test while dropping (N-1)/N of deliveries in production.
- [TDD · open] Assert the LEADING BYTE, not the decoded payload, for anything a client
  frame-dispatches on. Every pre-existing pub/sub test decoded the payload and so passed
  against Array confirmations — the bug was invisible to a suite that looked right (evidence:
  the RESP3 divergence shipped undetected until §0's raw-socket table).
- [ADD · open] "Runtime parity" is a distinct verification axis in this repo, not a formality.
  Monoio 18/18 with tokio 12/18 is the recurring failure shape (four independent gaps this
  time); every runtime-crossing task should run both legs before the gate, never just the
  default one (evidence: this build, plus the prior monoio-intercept-order and
  monoio-CI-coverage tasks).
- [ADD · open] Enumerate TEARDOWN paths as deliberately as dispatch paths. Adding a namespace
  to the registry meant `ssubscribe` needed a matching call in RESET, in disconnect cleanup, and
  in the no-args SUNSUBSCRIBE path — and only the last one got it. Nothing catches this: the
  method exists, compiles, and is called from somewhere, so neither the compiler nor a
  dead-code lint objects (evidence: PR #483 review, three defects, all teardown).
- [TDD · open] An arity guard is protocol surface, not validation boilerplate. The subscriber-
  mode arm answering NOTHING was worse than answering the wrong error, and no test covered it
  because ps16 tested arity on a fresh connection — the same verb, a different code path
  (evidence: ps19 got `left: ""`).
- [ADD · open] A regression test that is green before the fix is not evidence for that fix.
  ps21 passed pre-fix because `spublish_shared` reconciles dead subscribers, so the observable
  count self-heals and only the remote-map leak survives. Kept as a contract guard, but the
  §6 record says plainly that it does not prove the disconnect fix (evidence: red/green run).
- [ADD · open] A rule restated in N places WILL drift to N behaviours. The subscriber-mode
  allow-list existed three times with two texts and two behaviours, none matching Redis
  (evidence: only the sharded handler accepted RESET; `handler_single` advertised HELLO as
  allowed in its error text while refusing it).
