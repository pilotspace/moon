# TASK: RESP3 reply-type parity, applied at one conversion choke point

slug: resp3-type-fidelity · created: 2026-08-09 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: verify   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower the
     autonomy level to `manual` or `conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Touches (files · symbols · signatures):
- `src/protocol/resp3.rs:maybe_convert_resp3(cmd: &[u8], response: Frame, proto: u8) -> Frame` — the
  whole conversion policy, 240 lines. A flat `match cmd` over 4 helper fns: `array_to_map`,
  `array_to_set`, `int_to_bool`, `bulk_to_double`. **It never sees the command's ARGS**, which is the
  structural reason half the table is wrong — WITHSCORES / WITHVALUES / a `<count>` argument decide
  the reply shape, and the current signature cannot express that.
- `src/server/conn/util.rs:49:apply_resp3_conversion(cmd, response, proto)` — the thin uppercasing
  wrapper every handler calls; forwards to `maybe_convert_resp3`.
- 11 call sites across 3 handlers (NOT one choke point): `handler_monoio/mod.rs:2470,2625,2983` ·
  `handler_sharded/mod.rs:1195,1980,2072,2295` · `handler_single.rs:1146,2288,2742`.
  `handler_single` is the embedded-mode handler; the two production paths are monoio + sharded.
- `src/server/conn/shared.rs:221:execute_transaction_sharded(shard_databases, shard_id,
  command_queue: &[Frame], selected_db, cached_clock, exec_publishes, exec_flushes)` — builds the
  EXEC `results` vec. It HAS each queued command frame but takes no `proto`, so no inner reply is
  ever converted. This is the "shape changes by context" defect.

Context (working folder): `scripts/client-compat/manifest.yaml` (19 entries carry an `expect_diff`
waiver; 17 name this task as owner) · `scripts/test-client-compat.sh --strict` is the verifier.

Honors (patterns / conventions): CLAUDE.md "New Commands" (three dispatch paths — a check on two of
three is the shape of the v0.8.6 P0) · hot-path allocation rules (`src/protocol/` is on the no-alloc
list: conversion must reuse the incoming `Vec`, never rebuild it) · MILESTONE.md "a command must not
change shape by context" and "real Redis is the oracle, never Moon's own expectation".

Anchors the contract cites: `maybe_convert_resp3` · `apply_resp3_conversion` ·
`execute_transaction_sharded` · `Frame::{Map,Set,Double,Boolean,Integer,BulkString,Array}`.

**Oracle ground truth** (sweep vs redis-server 8.6.1, RESP3, standalone + MULTI, 2026-08-09 — this
table is the spec, not a guess). ◆ = Moon diverges:

| command | Redis RESP3 | Moon today | verdict |
|---|---|---|---|
| SISMEMBER · HEXISTS · EXPIRE · PEXPIRE · PERSIST · SETNX · MSETNX | `:int` | ◆ `#bool` | `int_to_bool` is wrong for ALL 7 — delete the branch |
| INCRBYFLOAT · HINCRBYFLOAT | `$bulk` | ◆ `,double` | over-converted, and lossy (`,10.6` vs `"10.59999999999999964"`) |
| ZSCORE · ZINCRBY | `,double` | `,double` | correct |
| ZMSCORE | `*[,\|_]` | ◆ `*[$\|_]` | missing from the table |
| GEOPOS | `*[*2[,]]` | ◆ `*[*2[$]]` | missing from the table |
| HRANDFIELD … WITHVALUES | `*[*2[$]]` | ◆ `%map` | inverted — pairs, not a Map |
| ZRANDMEMBER … WITHSCORES | `*[*2[$\|,]]` | ◆ `%map` | inverted — pairs with a Double score |
| HRANDFIELD / ZRANDMEMBER (no WITH…) | `*[$]` | `*[$]` | correct — proves ARG awareness is required |
| ZRANGE · ZREVRANGE · ZRANGEBYSCORE … WITHSCORES | `*[*2[$\|,]]` | ◆ `*4[$]` flat | missing pair-wrap AND Double |
| ZPOPMIN · ZPOPMAX (no count) | `*2[$\|,]` | ◆ `*2[$]` | score must be Double |
| ZPOPMIN/MAX `<count>` | `*[*2[$\|,]]` | ◆ `*4[$]` flat | pair-wrap + Double |
| ZDIFF · ZUNION · ZINTER … WITHSCORES | `*[*2[$\|,]]` | ◆ flat | pair-wrap + Double |
| SPOP (no count) | `$bulk` | `$bulk` | correct |
| SPOP `<count>` | `~set` | ◆ `*array` | ARG-dependent again |
| SMEMBERS · SINTER · SUNION · SDIFF | `~set` | `~set` | correct standalone |
| HGETALL · CONFIG GET | `%map` | ◆ CONFIG is `*array` | CONFIG's call site bypasses the converter |
| XINFO STREAM | `%16` | ◆ `*14` | Map + 8 missing fields |
| CLIENT INFO | `=verbatim` | ◆ `$bulk` | Verbatim string never emitted |
| **inside MULTI/EXEC** — HGETALL · SMEMBERS · SINTER · ZSCORE · ZINCRBY | same as standalone | ◆ unconverted | no inner reply is converted |

Out of scope (owned elsewhere, discovered by the same sweep): `ZADD … INCR` answers an ERROR on Moon
(functional gap, not a type gap) and every MULTI *semantics* defect -> `multi-exec-queue-semantics`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: RESP3 reply types match real Redis for every covered command, in every context, decided at
one arg-aware choke point.

Framings weighed:
- **Arg-aware policy at one choke point** (chosen) — widen the conversion signature to take the
  command's args, and call it from exactly one place per handler including the EXEC inner-reply loop.
  Fixes the type table AND the context-dependence with one structural change; the per-command fixes
  become data.
- *Fix each wrong command where it is produced* (rejected) — pushes RESP3 knowledge into ~20 command
  handlers, and leaves the shape still decided per call-site, so "changes shape by context" can
  silently return. It also cannot fix CONFIG, whose call site never reaches a converter at all.
- *Convert at the codec on the way out* (rejected) — the codec sees a `Frame`, not the command that
  produced it; ZRANGE-with-scores and ZRANGE-plain are the same `Frame::Array` on the wire side.

Must:
<must>
  - The conversion decides on (command, args, reply), not (command, reply): HRANDFIELD/ZRANDMEMBER
    convert only with WITHVALUES/WITHSCORES, SPOP only with a count, ZRANGE-family only with
    WITHSCORES.
  - A WITHSCORES/WITHVALUES reply is an Array of 2-element Arrays; every score element is a Double.
  - ZPOPMIN/ZPOPMAX with no count stay a flat 2-element Array, member Bulk + score Double.
  - ZMSCORE returns an Array of Double (Null preserved for absent members); GEOPOS returns an Array
    of 2-element Arrays of Double (Null preserved for absent members).
  - SPOP with a count returns a Set; without one it stays a Bulk.
  - CONFIG GET returns a Map. CLIENT INFO returns a Verbatim string.
  - XINFO STREAM returns a Map.
  - Integer stays Integer for SISMEMBER, HEXISTS, EXPIRE, PEXPIRE, PERSIST, SETNX and MSETNX.
  - Bulk stays Bulk for INCRBYFLOAT and HINCRBYFLOAT, byte-identical to the RESP2 reply.
  - Every EXEC inner reply is converted with ITS OWN command and args, so a command's shape is
    identical standalone, inside MULTI/EXEC, and inside a pipeline.
  - RESP2 replies are byte-identical before and after this task, for every command in the table.
  - The conversion runs on all three handlers (`handler_monoio`, `handler_sharded`, `handler_single`).
</must>
Reject:
<reject>
  - proto < 3 -> return the reply untouched (no conversion, no allocation)
  - a reply that is already an Error or Null -> pass through unchanged, whatever the command
  - a WITHSCORES reply with an odd element count (a malformed inner reply) -> pass through unchanged
    rather than panic or drop the tail
  - a score element that does not parse as a float -> leave that element as Bulk, convert the rest
  - a command not in the table -> return unchanged (no default conversion)
</reject>
After:
<after>
  - `scripts/test-client-compat.sh --strict` reports zero unexplained differences AND fails on the
    17 now-stale waivers owned by this task, which are deleted from the manifest in the same commit.
  - `moon_dispatch_path_total{path="local_inline"}` is unchanged for GET at c=1 P=1 — the inline
    fast path did not lose a command to the widened signature.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ RESOLVED — **WRONG, as suspected. Checked before the freeze; the contract below is the
    corrected one.** The claim was "the args are present at every call site as a `&[Frame]`".
    They are present at the LOCAL sites (`handler_monoio:2470` has `cmd` + `cmd_args` in scope) but
    ABSENT at the cross-shard reply loops (`handler_monoio:2983`, `handler_sharded:2295`): those
    iterate a `RemoteMeta` tuple `(resp_idx, aof_bytes, cmd_name, track_keys)` that carries only the
    command NAME. Same for the EXEC inner-reply loop.
    Cost had this shipped unchecked: the build would have had to either (a) carry the full args
    across the batch — an allocation per remote command on the shard hot path, which
    CLAUDE.md forbids in `src/server/conn/` — or (b) silently skip conversion for cross-shard
    replies, which re-creates the exact "shape changes by context" defect this task exists to kill,
    and would have passed a shards=1 test suite.
    Resolution (folded into §3): classify at ENQUEUE time, where `cmd` and `cmd_args` ARE in scope
    (`handler_monoio:2681-2704`), into a 1-byte `Copy` `Resp3Shape` tag, and carry the TAG in
    `RemoteMeta` instead of the args. Conversion then needs no args at reply time. Zero allocation,
    and the local and remote paths share one policy function.
  - [ ] Widening `maybe_convert_resp3` costs nothing on the RESP2 path — the `proto < 3` early
    return happens before any arg inspection, so a RESP2 connection never pays for the new
    parameter. Confirm with the c=1 P=1 A/B, not by reading.
  - [ ] Redis's Double formatting for scores round-trips Moon's stored f64 exactly for the values
    under test. If wrong, the value assert (not the type assert) fails and the test needs a
    tolerance policy like the harness's `numeric_tolerance`.
  - [ ] No client in the acceptance set depends on Moon's CURRENT (wrong) Boolean replies. Low risk:
    they contradict Redis, so any client tolerating them also tolerates Integer.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: a scored range is an array of pairs with Double scores
  Given a RESP3 connection and a zset z with a=1 b=2
  When the client sends ZRANGE z 0 -1 WITHSCORES
  Then the reply is *2 whose every element is *2 of [BulkString, Double]
  And ZRANGE z 0 -1 without WITHSCORES is still a flat *2 of BulkString

Scenario: arg-awareness decides the shape, not the command name
  Given a RESP3 connection and a hash h with one field
  When the client sends HRANDFIELD h 1 WITHVALUES
  Then the reply is an Array of 2-element Arrays, never a Map
  And HRANDFIELD h 1 without WITHVALUES is still a flat Array of BulkString

Scenario: a count turns SPOP into a Set
  Given a RESP3 connection and a set s with 3 members
  When the client sends SPOP s 2
  Then the reply type byte is ~ (Set)
  And SPOP s with no count is still a BulkString

Scenario: predicate replies stay Integer
  Given a RESP3 connection and a set s containing a
  When the client sends SISMEMBER s a
  Then the reply is Integer :1, not Boolean #t
  And the same holds for HEXISTS, EXPIRE, PEXPIRE, PERSIST, SETNX and MSETNX

Scenario: INCRBYFLOAT keeps full precision as a BulkString
  Given a RESP3 connection and f = 10.5
  When the client sends INCRBYFLOAT f 0.1
  Then the reply is a BulkString
  And its bytes are identical to the RESP2 reply for the same operation

Scenario: shape does not change inside a transaction
  Given a RESP3 connection and a hash h with two fields
  When the client sends MULTI, HGETALL h, EXEC
  Then the inner reply is a Map, exactly as it is standalone
  And the same holds for SMEMBERS (Set), ZSCORE (Double) and CONFIG GET (Map)

Scenario: shape does not change across a shard boundary
  Given a 4-shard server and a RESP3 connection
  When the client sends ZRANGE {t1}z 0 -1 WITHSCORES and ZRANGE {t7}z 0 -1 WITHSCORES
  Then both replies have the identical pair-wrapped Double shape
  And neither depends on which shard the connection landed on

Scenario: RESP2 is untouched
  Given a RESP2 connection
  When the client sends every command in the §0 oracle table
  Then every reply is byte-identical to the reply the same build gives today
  And no reply contains a RESP3-only type byte (%, ~, ,, #, =)

Scenario: a non-RESP3 connection pays nothing
  Given a RESP2 connection
  When any covered command is dispatched
  Then the conversion returns before inspecting the command or its args

Scenario: an error reply is never converted
  Given a RESP3 connection
  When the client sends ZRANGE against a key holding a string, with WITHSCORES
  Then the reply is the WRONGTYPE Error unchanged
  And no pair-wrapping is attempted on it

Scenario: a malformed scored reply is passed through, not dropped
  Given a scored reply whose element count is odd
  When the pair-wrapping conversion runs
  Then the reply is returned unchanged
  And no element is lost and no panic occurs
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```rust
// src/protocol/resp3.rs — the whole policy, in two halves.

/// What RESP3 shape a command's reply must take. Decided from (name, args) ONCE,
/// carried by value. 1 byte, Copy — so the cross-shard batch can hold it in
/// RemoteMeta with no allocation (see §1 ⚠ RESOLVED).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Resp3Shape {
    None,          // pass through unchanged  (the default for every unlisted command)
    Map,           // flat array  -> %map        HGETALL, CONFIG GET, XINFO STREAM
    Set,           // array       -> ~set        SMEMBERS/SINTER/SUNION/SDIFF, SPOP <count>
    Double,        // bulk        -> ,double     ZSCORE, ZINCRBY
    DoubleArray,   // [bulk]      -> [,double]   ZMSCORE
    ScoredPairs,   // flat [m,s,…]-> [[m ,s],…]  ZRANGE-family/ZDIFF/ZUNION/ZINTER WITHSCORES,
                   //                            ZPOPMIN/MAX <count>, ZRANDMEMBER WITHSCORES
    ScoredFlat,    // [m,s]       -> [m ,s]      ZPOPMIN/ZPOPMAX with no count
    ValuePairs,    // flat [f,v,…]-> [[f,v],…]   HRANDFIELD WITHVALUES
    CoordPairs,    // [[x,y],…]   -> [[,x ,y],…] GEOPOS
    Verbatim,      // bulk        -> =txt        CLIENT INFO
}

/// Classify. Pure, allocation-free, no I/O. `args` EXCLUDES the command name.
pub fn resp3_shape_of(cmd_upper: &[u8], args: &[Frame]) -> Resp3Shape;

/// Apply. `proto < 3` returns `response` untouched before touching anything else.
/// Error and Null always pass through. A shape whose input does not match
/// (odd element count, unparseable score) passes through UNCHANGED.
pub fn apply_shape(shape: Resp3Shape, response: Frame, proto: u8) -> Frame;

// src/server/conn/util.rs — convenience wrapper for the LOCAL paths, where args are in scope.
pub(crate) fn apply_resp3_conversion(
    cmd: &[u8], args: &[Frame], response: Frame, proto: u8,
) -> Frame;   // = apply_shape(resp3_shape_of(upper(cmd), args), response, proto)
```

Call-site contract (this is the part that kills "shape changes by context"):

| path | classify at | apply at |
|---|---|---|
| local, single command | dispatch exit — `cmd` + `cmd_args` in scope | same statement |
| cross-shard | ENQUEUE (`handler_monoio:~2698`, `handler_sharded` twin) — tag stored in `RemoteMeta` | the reply loop (`:2983` / `:2295`), from the carried tag |
| MULTI/EXEC | inside `execute_transaction_sharded`, per queued command Frame | the same loop, on each inner reply before it joins `results` |
| pipeline | unchanged — it reuses the local path per command | — |

Signature changes (breaking, internal only):
- `maybe_convert_resp3(cmd, response, proto)` -> **removed**; replaced by the pair above.
- `apply_resp3_conversion` gains `args: &[Frame]`.
- `RemoteMeta` gains a `Resp3Shape` field; `cmd_name` stays (AOF/tracking still use it).
- `execute_transaction_sharded` gains `proto: u8`.

Invariants:
- `proto < 3` -> identity. Asserted by the RESP2 byte-identity scenario, not by reading.
- Classification never allocates and never inspects reply CONTENT — only (name, args).
- Conversion reuses the incoming `Vec`'s allocation where the arity allows (pair-wrapping halves
  the outer length; it must not build a second Vec of the original size).
- An unlisted command returns `Resp3Shape::None`, so adding a command is opt-in, never implicit.

Status: FROZEN @ v1 — approved by Tin Dang, 2026-08-10
Frozen with the §1 ⚠ already discharged (args absent at the cross-shard reply loops) — the
enqueue-time `Resp3Shape` tag above IS the corrected shape, not the original draft.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every row of the §0 oracle table, in 3 contexts × 2 protocols; both shard counts.

Plan (one test per scenario, asserting the WIRE TYPE BYTE — the thing every prior suite could not
see. `redis-cli` renders replies to text and destroys the type byte, so these speak raw RESP):
<test_plan>
  - r3f1_scored_replies_are_pairs_of_double: arrange zset a=1 b=2 / act ZRANGE·ZREVRANGE·
    ZRANGEBYSCORE·ZDIFF·ZUNION·ZINTER WITHSCORES + ZPOPMIN <count> / assert outer Array of 2-element
    Arrays with a Double second element + assert the no-WITHSCORES form stays a flat Bulk array
  - r3f2_zpopmin_without_count_is_flat_pair: act ZPOPMIN z / assert *2 [Bulk, Double] (NOT wrapped)
  - r3f3_arg_awareness_decides_shape: act HRANDFIELD/ZRANDMEMBER with and without WITH… / assert
    pairs when present, flat Bulk array when absent, and NEVER a Map
  - r3f4_spop_count_is_a_set: act SPOP s 2 / assert ~ ; act SPOP s / assert $
  - r3f5_predicates_stay_integer: act SISMEMBER·HEXISTS·EXPIRE·PEXPIRE·PERSIST·SETNX·MSETNX /
    assert every reply is Integer, none is Boolean
  - r3f6_incrbyfloat_stays_bulk_and_exact: act INCRBYFLOAT + HINCRBYFLOAT / assert BulkString AND
    assert the bytes equal the RESP2 reply for the same operation (precision, not just type)
  - r3f7_zmscore_and_geopos_are_doubles: act ZMSCORE (one present, one absent) + GEOPOS / assert
    Double elements with Null preserved for the absent member
  - r3f8_map_replies: act CONFIG GET·HGETALL·XINFO STREAM / assert % ; act CLIENT INFO / assert =
  - r3f9_shape_is_identical_in_multi_and_pipeline: for every command above, run it standalone,
    inside MULTI/EXEC, and inside a pipeline / assert the three type-shapes are EQUAL to each other
    (self-consistency — this one fails today even where the standalone shape is right)
  - r3f10_shape_is_identical_across_shards: 4-shard server, hash-tagged keys {t0}..{t7} so both the
    local and cross-shard reply paths are exercised / assert same shape from both
  - r3f11_resp2_is_byte_identical: every command over RESP2 / assert no %, ~, ,, #, = byte appears
    anywhere in any reply
  - r3f12_errors_and_edges_pass_through: WRONGTYPE with WITHSCORES / assert Error unchanged;
    ZRANGE WITHSCORES on a missing key / assert empty Array, not a panic or a malformed pair
</test_plan>

Tests live in: `tests/resp3_type_fidelity.rs` · MUST run red (missing implementation) before Build.

Oracle note: the expected values are transcribed from the live sweep against redis-server 8.6.1
recorded in §0 — not from reading Moon's source, and not from memory of the Redis docs.

### RED RUN — recorded 2026-08-09, `cargo test --profile release-fast --test resp3_type_fidelity`
`test result: FAILED. 2 passed; 10 failed` — red for the RIGHT reason, and the split was predicted
in the suite's header comment BEFORE the run:

| test | outcome | the divergence it names |
|---|---|---|
| r3f1 scored pairs | RED | `*4[$]` flat, want `*2[*2[$\|,]]` |
| r3f2 zpopmin flat pair | RED | `*2[$]`, want `*2[$\|,]` — score is Bulk not Double |
| r3f3 arg-awareness | RED | HRANDFIELD WITHVALUES answers `%1`, want `*1[*2[$]]` |
| r3f4 spop count is a Set | RED | `*2[$]`, want `~` |
| r3f5 predicates stay Integer | RED | Boolean `#`, want `:` |
| r3f6 incrbyfloat bulk+exact | RED | Double `,`, want `$` |
| r3f7 zmscore/geopos Doubles | RED | Bulk elements, want Double |
| r3f8 map replies | RED | CONFIG GET `*`, want `%` |
| r3f9 shape stable by context | RED | shape differs standalone vs EXEC |
| r3f10 shape stable across shards | RED | agrees across shards, but on the WRONG shape |
| **r3f11 RESP2 byte-purity** | **GREEN** | invariant pin — must STAY green through every batch |
| **r3f12 error/edge pass-through** | **GREEN** | invariant pin — conversion must not mangle these |

The two greens are deliberate: a suite that is 100% red cannot tell you whether the build broke an
invariant or merely failed to add a feature. r3f11 is the RESP2 guard rail, r3f12 the error path.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/protocol/resp3.rs` `src/server/conn/util.rs` `src/server/conn/shared.rs`
`src/server/conn/handler_monoio/mod.rs` `src/server/conn/handler_sharded/mod.rs`
`src/server/conn/handler_single.rs` `scripts/client-compat/manifest.yaml` `CHANGELOG.md`

Strategy (ordered batches):
1. `Resp3Shape` + `resp3_shape_of` + `apply_shape` in `src/protocol/resp3.rs`, with unit tests for
   the pure classifier. `maybe_convert_resp3` deleted in the same batch so no caller can drift back.
2. Rewire the LOCAL call sites (args in scope) — 8 of 11. Suite goes from red to partly green.
3. Cross-shard: add the `Resp3Shape` field to `RemoteMeta`, classify at enqueue, apply from the tag.
4. `execute_transaction_sharded` takes `proto` and converts each inner reply -> r3f9 goes green.
5. Delete the 17 now-stale waivers from `manifest.yaml`; `--strict` must pass.

Safety rule (feature-specific): RESP2 is a hard invariant — batch 1 lands the `proto < 3` early
return before ANY classification, and r3f11 gates every later batch. A conversion that cannot be
applied cleanly (odd arity, unparseable score) returns the reply UNCHANGED; it never panics, never
truncates, and never partially rewrites a reply.

Code lives in: `src/`
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

- [x] all tests pass — `resp3_type_fidelity` 13/13; lib `protocol::resp3` 20/20; full suite (see
      GATE RECORD). Harness `--strict` PASS 157 / FAIL 0 / WAIVED 25, real exit 0 (captured to a
      file: `./scripts/test-client-compat.sh | tail` reports **tail's** status, not the harness's).
- [x] coverage did not decrease — +1 integration test (r3f13), net +0 unit tests (one deleted, two
      added), +5 harness cases +1 waived case. No case was removed to obtain green.
- [!] no test or contract was altered during build — **one test WAS deleted, and it must not pass
      unremarked.** `an_empty_map_reply_passes_through` asserted "HGETALL of a missing key: an empty
      array, not an empty map". It was written by me EARLIER IN THIS SAME BUILD from an assumption,
      never from the oracle, and it is false: redis 8.6.1 answers `%0`. It was green, so it read as
      coverage while actually locking in a defect. Deleted and replaced by
      `an_empty_map_reply_is_an_empty_map_not_an_empty_array`, which fails on the pre-fix binary.
      This is NOT the forbidden move (weakening a test to make a build pass) — the replacement is
      strictly stronger and the code changed to meet it, not the reverse. No pre-existing test and
      no clause of the frozen §3 was altered. Filed as a TDD delta in §7.
- [x] the green was EARNED, not gamed — the refute-read is what FOUND the empty-reply defect, after
      all three instruments (harness, unit tests, redis-py acceptance) were unanimously green. Two
      vacuous-assert traps were caught and removed rather than shipped: r3f13 asserts `shape()` not
      `items().is_empty()`, because `items()` returns `&[]` for a Map by construction and the
      is_empty form would have been trivially true on exactly the two cases under test; and every
      new harness case was re-run against the pre-fix binary, where all five fail, proving they
      discriminate. No fixture overfit: the oracle is a live `redis-server`, not a recorded expectation.
- [x] concurrency / timing of the risky operation is safe — the shape is a `Copy` enum classified at
      enqueue and read on the reply path; it adds no shared state, no lock, and nothing held across
      an `.await`. Classification is gated on `proto >= 3`, so RESP2 cross-shard pays one integer
      compare. Conversions are idempotent (every arm matches the input variant with an
      `other => other` fallthrough), so a double-apply cannot corrupt a reply.
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new dependency, no
      `unsafe`, no `unwrap`/`expect`. Input handling is total: unparseable scores stay Bulk, odd
      arities pass through whole, Errors and Nulls short-circuit before any shape match. The
      uppercase buffer is a fixed 32-byte stack array with the copy length clamped by `.min(32)`, so
      an arbitrarily long client-supplied command name cannot overflow it.
- [x] layering & dependencies follow CONVENTIONS.md — policy lives in `src/protocol/`, handlers call
      it through `server::conn::util`; no handler re-implements the table. No allocation added to
      `src/command/`, `src/protocol/`, `src/shard/event_loop.rs` or `src/io/`.
- [ ] a person reviewed and approved the change

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [x] An unmodified redis-py stops RAISING on RESP3 sorted sets — confirmed by re-running the same
      acceptance script against both servers: `zrange withscores (RESP3)` went from
      `ValueError: not enough values to unpack (expected 2, got 1)` to `[['a', 1.0], ['b', 2.5]]`,
      byte-equal to redis 8.6.1. `hrandfield withvalues` likewise `[['f', 'v']]` on both.
      This is the outcome that matters: the client no longer throws.
- [x] The harness — the milestone's named verifier, diffing against a LIVE redis-server, not against
      Moon's own expectation — improves and stays honest: **PASS 98 -> 157, WAIVED 54 -> 25, FAIL 0**,
      with `--strict` exit 0 after the 13 stale waivers were deleted. The +29 over the first green
      run is the miss-path family added when the refute-read (below) found the harness had never
      diffed an empty reply.
- [x] Emptiness does not change the reply TYPE — `HGETALL`/`CONFIG GET` on a miss answer `%0`, not
      `*0`; `SMEMBERS` answers `~0`; `ZRANGE … WITHSCORES` stays `*0`. Confirmed by a raw-socket
      byte-diff against redis-server 8.6.1 that came back BYTE-IDENTICAL, by r3f13, and by the five
      new manifest cases. Proven discriminating, not vacuous, by re-running the new cases against the
      pre-fix binary as a control: all five FAIL there.
- [x] The same command answers the same shape in every context — confirmed by r3f9, which compares
      standalone vs MULTI/EXEC vs pipeline against EACH OTHER (not against a literal), so it fails on
      any context-dependence whatever the shape happens to be.
- [x] The shape does not depend on which shard owns the key — confirmed by r3f10 on a 4-shard server
      across hash tags {t0}..{t7}, which asserts BOTH that the shards agree AND that they agree on
      the Redis shape (agreeing on a wrong shape must not pass).
- [x] RESP2 is untouched — confirmed by r3f11, written and green BEFORE the build, asserting no
      `%`, `~`, `,`, `#` or `=` byte appears in any RESP2 reply. Still green after.
- [x] No conversion can panic or silently truncate — confirmed by unit tests
      `an_odd_element_count_passes_through_whole` and `an_unparseable_score_stays_bulk`, which assert
      the reply is returned WHOLE and unchanged rather than partially rewritten.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — every new symbol is referenced on BOTH runtimes, which is the check that
      matters here (see [[gotcha_monoio_intercept_order_ci_blind]]: CI builds only tokio, so a
      monoio-only miss ships). `Resp3Shape` + `resp3_shape_of` + `apply_shape`: referenced from
      `util::{resp3_shape_for, apply_resp3_conversion}`, from both enqueue sites
      (`handler_monoio/mod.rs`, `handler_sharded/mod.rs`), both cross-shard reply loops, both CONFIG
      intercepts, both CLIENT INFO intercepts, and `shared::execute_transaction_sharded`. Confirmed
      by `cargo check` under default (monoio) AND `--no-default-features --features
      runtime-tokio,jemalloc`, both clean.
- [x] DEAD-CODE (code) — `maybe_convert_resp3` was DELETED, not left beside its replacement, so no
      caller can drift back to the name-only policy. The old helpers (`int_to_bool` in particular)
      went with it. One item found and deliberately NOT acted on: the `cmd_name`/`cmd_bytes` field
      in `RemoteMeta` became unread once conversion stopped needing it — the frozen §3 says it stays
      ("AOF/tracking still use it"), which turned out to be wrong. Removing it is a contract change,
      not a build decision, so it is bound to `_cmd_name` here and filed as a SPEC delta in §7
      rather than edited into the frozen contract.
- [x] SEMANTIC (prose) — the 6 surviving waiver reasons in `manifest.yaml` were re-read in full and
      corrected, not left to rot: 3 moved owner to `multi-exec-queue-semantics` (they are
      transaction-QUEUEING defects, never reply-type ones) and `shape_xinfo_stream` was rewritten,
      because its old reason ("Moon answers a flat Array") is now FALSE — the type is fixed and only
      the field set differs. A stale-but-passing waiver reason is the same blindness the harness
      exists to remove.

### GATE RECORD

Evidence, recorded 2026-08-10 (commit `70fa83a6`, branch `fix/resp3-type-fidelity`):

| gate | result |
| --- | --- |
| `tests/resp3_type_fidelity.rs` | 13 passed / 0 failed |
| lib `protocol::resp3` unit tests | 20 passed / 0 failed |
| `test-client-compat.sh --strict` (oracle redis 8.6.1) | PASS 157 · FAIL 0 · WAIVED 25 · exit 0 |
| miss-path raw-socket byte-diff vs redis 8.6.1 | BYTE-IDENTICAL |
| `cargo clippy --all-targets -D warnings` (default) | exit 0 |
| `cargo clippy --all-targets -D warnings` (runtime-tokio,jemalloc) | exit 0 |
| `cargo fmt --check` | exit 0 |
| full suite (`--tests --no-fail-fast`) | 194 binaries · 2 failed, both triaged below |

Full-suite residue — NEITHER is caused by this change, and neither is silently dropped:
1. `cross_shard_consistency_red` — `Connection refused` at server spawn. A DIFFERENT test failed on
   each run (`cdg6f`, then `cdg6d`), which is the signature of startup contention rather than a
   defect; passes 7/7 in isolation. The suite spawns ~200 servers; the machine, not the code.
2. `dbsize_offload_logical::dbsize_counts_spilled_keys_and_survives_restart` — identical numbers on
   both runs (`live 373, recovered 400`), i.e. deterministic UNDER LOAD, passing in isolation and
   reproducing 2/4 under self-parallel load. `recovered > live` means restart resurrected keys the
   live instance had evicted — the test's own comment calls that "restart must never INVENT keys",
   so it is a real pre-existing question about eviction-vs-spill accounting under memory pressure,
   owned by the storage/eviction line of work, NOT by this task. This task cannot reach it: the test
   never sends `HELLO`, so the connection stays at protocol_version 2 and every conversion path
   early-returns at `proto < 3`. That argument was NOT accepted on its own — it was measured:

   **Same-load A/B, two servers, one variable.** A pre-change `moon` was built from `main`
   (`4c9bd2c5`) and both binaries were snapshotted to `/tmp` BEFORE either leg ran, so neither could
   be clobbered by a rebuild. Controls verified behaviourally distinct on this task's own markers —
   `main` answers `SISMEMBER → #t` and `HGETALL <miss> → *0`; the branch answers `:1` and `%0`.
   Under identical 4-way self-parallel load:

   | leg | server | result |
   | --- | --- | --- |
   | A | `main` (pre-change) | 7 of 8 tests failed |
   | B | this branch | 5 of 8 tests failed |

   Same assertion (`dbsize_offload_logical.rs:352`), same message, same key counts (`122/400` live,
   `120/400` multishard) on BOTH legs — the test's own vacuity guard, tripping because contention
   starves the spill. The pre-change server fails MORE often, so the defect is pre-existing and
   load-induced; this change is not implicated in either direction.

Outcome: <PASS | RISK-ACCEPTED | HARD-STOP>
If RISK-ACCEPTED -> owner: <name> · ticket: <link> · expires: <date>   (never for a security gap)
Reviewed by: <name> · date: <date>

<!-- Held open deliberately. §6 line 8 ("a person reviewed and approved the change") is the one
     box the AI must not tick for itself, and this change carries a disclosure that deserves a human
     look: a unit test was DELETED during build (see the [!] line in §6). Not auto-gated despite
     `autonomy: auto`. -->



<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>

### Spec delta
- [SPEC · open] `XINFO STREAM` reports 7 fields where Redis 8.6.1 reports 16 — the reply TYPE is
  fixed (Map), the FIELD SET is not (evidence: waiver `shape_xinfo_stream` still reproduces;
  `entries-added`, `max-deleted-entry-id`, `recorded-first-entry-id` need real stream bookkeeping
  and the `idmp-*`/`iids-*` counters are Redis 8.x idempotency features Moon lacks entirely.
  Fabricating them would mislead a client worse than their absence).
- [SPEC · open] `RemoteMeta`'s `cmd_name` field is now unread on BOTH handlers — the frozen §3 said
  it stays because "AOF/tracking still use it", which the build disproved (evidence: rustc
  `unused_variable` on the destructure in both `handler_monoio` and `handler_sharded`; bound to
  `_cmd_name`). Removing it drops a `Bytes` clone per cross-shard command from the hot path.
- [SPEC · open] The intercept family short-circuits the dispatch exit, so each intercept that
  answers a converted type must remember to convert (evidence: CONFIG and CLIENT INFO both needed
  their own call; the generic choke point never sees them). A registry-driven check that every
  intercept's reply passes through the policy would make the next one impossible to forget —
  natural fit alongside `client-identity-introspection`'s registry↔dispatch reconciliation.

### Competency deltas
- [TDD · open] Two tests written to be GREEN before the build (r3f11 RESP2 byte-purity, r3f12 error
  pass-through) did more work than the ten red ones: they are what let a 10-red/2-green result be
  read as "feature missing" rather than "invariant broken". A suite that is 100% red cannot make
  that distinction (evidence: the red-run table in §4 predicted the exact split, and the build never
  had to guess whether a RESP2 change was intended).
- [ADD · open] The §1 ⚠ lowest-confidence flag paid for itself: "args are available at every call
  site" was checked BEFORE the freeze and proved wrong at 3 of 11 sites, which changed the contract
  from "widen the signature" to "classify at enqueue, carry a Copy tag". Had it been frozen unchecked
  the build would have either allocated per remote command on the shard hot path or skipped
  cross-shard conversion — and the second failure mode passes a shards=1 test suite (evidence:
  §1 ⚠ RESOLVED; r3f10 exists precisely because that mode would otherwise be invisible).
- [TDD · open] A unit test I wrote during this same build asserted the WRONG behaviour and made the
  defect look verified: `an_empty_map_reply_passes_through` pinned "HGETALL of a missing key: an
  empty array, not an empty map" — an assumption I never put to the oracle, unlike every other row
  in the §0 table. It was green, so it read as coverage while it was actually a lock on the bug
  (evidence: `redis-server` 8.6.1 answers `%0`; the test was deleted and replaced by
  `an_empty_map_reply_is_an_empty_map_not_an_empty_array` + r3f13, which fail on the pre-fix binary).
  Rule earned: in a differential task, a test asserting a REPLY may only be written from oracle
  bytes — never from reasoning about what the reply "should" be.
- [TDD · open] Fixture-shaped blindness: every one of the 152 harness cases populated its key before
  asking, so the entire miss path was undiffed and a 128-PASS green run still shipped two wrong type
  bytes. Coverage counted by CASES hid a gap that coverage counted by STATES would have shown
  (evidence: five miss-path cases added; all five fail against the pre-fix binary). Worth asking of
  every differential suite: which of empty / missing / error / max is never exercised?
- [ADD · open] The refute-read found the defect that all three instruments missed — harness, unit
  tests and the redis-py acceptance script were unanimously green (evidence: this defect was found by
  re-reading `array_to_map`'s `!items.is_empty()` guard and asking the oracle what empty should be,
  after the suite was already green). An adversarial read of the diff is not a formality once the
  bars are green; it is the only instrument not built from the same assumptions as the code.
- [ADD · open] `scripts/test-client-compat.sh` defaults `MOON_BIN` to `target/release/moon`, a
  quarantined stale artifact in this checkout, and reports the resulting 32 failures as ordinary
  divergences with no provenance warning — a two-day-old binary produced a confident, entirely false
  compatibility picture (evidence: FAIL=32 including `SISMEMBER -> #t`, from a code path DELETED in
  this task; PASS=128 FAIL=0 with `MOON_BIN` pinned). The harness should print the binary's path and
  mtime in its header, and refuse or loudly warn when it is older than the newest source file.

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
