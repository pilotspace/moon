# TASK: Response batch must be encoded in the protocol in effect when each reply was produced

slug: batch-protocol-version-fidelity · created: 2026-08-12 · stage: production
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
  - `src/server/conn/core.rs:ConnectionState` — the per-connection state both shipped handlers own;
    already carries `protocol_version: u8`, which is the value read at dispatch time.
  - `src/server/conn/shared.rs` — the handler-agnostic helpers module; already the home of the
    code both `handler_monoio` and `handler_sharded` must agree on.
  - `src/server/conn/handler_monoio/dispatch.rs` — two HELLO sites that mutate
    `conn.protocol_version` mid-batch (`check_auth_gate` ~:103, `try_handle_hello` ~:417).
  - `src/server/conn/handler_monoio/mod.rs` — two flush sites (~:1922, ~:3539) that drain the
    accumulated `responses: Vec<Frame>` into `write_buf` under ONE version.
  - `src/server/conn/handler_sharded/mod.rs` — the same four sites (HELLO ~:684/~:822, flush
    ~:1435/~:2716). This handler bypasses `RespCodec` entirely and writes buffers directly.
  - `src/server/codec.rs:RespCodec::set_protocol_version` — the wire codec used by the monoio
    single-frame encode path only; NOT reachable from `handler_sharded`.
  - `src/protocol::serialize` / `serialize_resp3` — the two encoders the fix must choose between
    per reply rather than per batch.
  - `src/command/config.rs:config_get` — read only `args[0]`; found while writing the pipelined
    probe, since `CONFIG GET` is this suite's natural RESP3-shaped reply.
Context (working folder): `tests/batch_protocol_version.rs` (new) · `tests/common/` harness
  (`spawn_listening`, `find_moon_binary`, `sigkill`) · live `redis-server 8.6.1` as the oracle.
Honors (patterns / conventions): no allocation on the dispatch/flush hot path (CLAUDE.md
  "Allocations on Hot Paths" — `src/server/conn/` sits on it), so the switch record is a
  `SmallVec` that stays empty and untouched for every batch without a HELLO; three-dispatch-path
  parity (memory `gotcha_moon_three_dispatch_paths`); measured-not-derived parity claims.
Anchors the contract cites: `ConnectionState`, `serialize`, `serialize_resp3`, `config_get`.

---

## 0 · GROUND — measured, not recalled

**Found while verifying `client-identity-introspection` (2026-08-12).** Filed rather than folded
into that PR because the reproducer below touches none of that task's code.

Moon accumulates the replies for every command in one read batch and serializes them at FLUSH time,
using whatever protocol version is in effect at the end of the batch. Any command later in the same
batch that changes the protocol therefore RETRO-ENCODES the earlier replies. Redis writes each
reply as it is produced, so it does not have this failure mode.

Measured against this branch's binaries, single-shard, both handlers (monoio default and the tokio
`handler_single` path) — identical results on both:

| bytes sent in ONE write        | first bytes of the HELLO 3 reply | correct? |
|--------------------------------|----------------------------------|----------|
| `HELLO 3\r\n` (alone)          | `%7`                             | yes      |
| `HELLO 3\r\nPING\r\n`          | `%7`                             | yes      |
| `HELLO 3\r\nHELLO 2\r\n`       | `*14`                            | **no**   |
| `HELLO 3\r\nRESET\r\n`         | `*14`                            | **no**   |

`*14` is the RESP2 flattening of the 7-entry map. A client that pipelines `HELLO 3` with any later
protocol-changing command reads the handshake reply in the wrong protocol and misparses it.

`HELLO 3` + `HELLO 2` is the ownership proof: it predates and is independent of
`client-identity-introspection` (no `RESET`, no `ROLE`, no `COMMAND`, no `identity.rs`). RESET,
which reverts the protocol to RESP2 by contract, simply adds a second trigger for the same
pre-existing defect.

Reproducer script: `/tmp/prepipe.sh` (raw `/dev/tcp` writes — `redis-cli` cannot express "two
commands in one write", which is why this survived).

Shape of the fix (NOT yet decided — this is §0, not a contract): either flush the accumulated
responses BEFORE applying a protocol change, or tag each queued response with the protocol version
current when it was produced. The first is smaller; the second is harder to regress.


## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: per-reply protocol fidelity within a pipelined batch (and `CONFIG GET` multi-parameter)

Framings weighed:
  - **tag each reply with the version in effect when it was produced** (chosen) — the batch stays
    one write, and a future third protocol-changing command inherits the fix for free.
  - flush the accumulated responses BEFORE applying a protocol change — smaller, but it converts
    one pipelined write into N, silently degrading pipeline throughput for any client that sends
    HELLO inside a batch, and it re-breaks the moment a new switch site is added without the flush.
  - re-encode at dispatch time (serialize straight into `write_buf` instead of buffering `Frame`s)
    — correct by construction, but a rewrite of both handlers' reply paths and of the cross-shard
    reply plumbing; out of proportion to the defect.

Must:
<must>
  - Every reply is encoded with the protocol version that was in effect at the moment that reply
    was produced — for a HELLO, that is the version the HELLO itself established.
  - A `HELLO 2` later in a batch does not retro-downgrade replies produced earlier under RESP3.
  - A `HELLO 3` later in a batch does not retro-upgrade replies produced earlier under RESP2.
  - Several protocol switches in one batch each take effect from their OWN index onward.
  - A batch with no protocol switch — essentially every batch — is encoded exactly as before,
    with no added allocation and one branch.
  - Both shipped dispatch paths (`handler_monoio`, `handler_sharded`) behave identically, at
    `--shards 1` and `--shards 4`.
  - `CONFIG GET` answers every pattern it is given, not only the first.
</must>

Reject:
<reject>
  - `CONFIG GET` with a non-string argument -> `"ERR invalid argument"` (unchanged)
  - `CONFIG GET` with an unknown pattern -> that pattern contributes nothing; it is NOT an error
    (measured on redis-server 8.6.1; all-unknown yields an empty array)
  - overlapping patterns (`maxmemory` + `maxmemory*`) -> each parameter reported ONCE
</reject>

After:
<after>
  - A client may pipeline HELLO with anything and parse every reply with the protocol that reply
    declares — the handshake is no longer order-of-write sensitive.
  - `redis-py`'s `config_get(*params)` and monitoring agents that read several settings in one
    call get all of them.
</after>

Assumptions — lowest-confidence first:
<assumptions>
  ⚠ **The frame SHAPE is already fixed correctly at dispatch, and only the SERIALIZER version at
    flush is wrong.** Lowest confidence because it is the whole reason the fix is small: it says
    `apply_resp3_conversion` (which reads `conn.protocol_version` at dispatch) needs no change at
    all. If wrong, the downgrade direction would still misreport after the serializer is fixed,
    and the fix would have to move into dispatch. CONFIRMED by measurement: the RESP3-produced
    `Frame::Map` re-serialized under RESP2 flattens to `*` (visible), while a RESP2-flattened
    `Frame::Array` re-serialized under RESP3 still emits `*` (invisible) — which is exactly why
    only the downgrade direction was ever observed to break.
  - [x] `handler_single` also needs the fix — DENIED. `main.rs` drives `run_sharded` at both call
    sites, so the binary never reaches `handler_single`; it is a library/embedding path that
    deliberately omits TXN. Fixing the two shipped handlers is fixing every shipped path.
  - [x] The switch record can live on `RespCodec` — DENIED. `handler_sharded` bypasses the codec
    (direct buffer I/O, no `RespCodec` in scope). The state must live on `ConnectionState`.
  - [x] Redis's `CONFIG GET` returns parameters in the CALLER's pattern order — DENIED by
    measurement: it is the server's own table order, deduplicated, union over patterns.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first. -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: a reply produced before HELLO 2 keeps its RESP3 encoding
  Given a connection that has completed HELLO 3
  When it sends CONFIG GET maxmemory / HELLO 2 / CONFIG GET maxmemory in ONE write
  Then the first reply is a RESP3 map (%)
  And the HELLO reply is the RESP2 array (*) that HELLO 2 itself establishes
  And the third reply is a RESP2 array (*)

Scenario: a reply produced before HELLO 3 keeps its RESP2 encoding
  Given a connection still on RESP2
  When it sends CONFIG GET maxmemory / HELLO 3 / CONFIG GET maxmemory in ONE write
  Then the first reply is a RESP2 array (*)
  And the HELLO reply is a RESP3 map (%)
  And the third reply is a RESP3 map (%)

Scenario: two HELLOs in one batch each take effect from their own index
  Given a connection still on RESP2
  When it sends CONFIG GET / HELLO 3 / CONFIG GET / HELLO 2 / CONFIG GET in ONE write
  Then the type bytes are, in order: * % % * *
  And no reply is encoded under a version that took effect after it was produced

Scenario: a batch without HELLO is encoded entirely in one protocol
  Given a connection that has completed HELLO 3
  When it sends five CONFIG GETs in ONE write
  Then every reply is a RESP3 map (%)
  And the no-switch fast path is what produced them (nothing else changed)

Scenario: CONFIG GET honours every parameter, not just the first
  Given any connection
  When it sends CONFIG GET maxmemory appendonly
  Then the reply names both maxmemory and appendonly
  And an unknown pattern in the same call is silently skipped, not an error

Scenario: CONFIG GET deduplicates overlapping patterns
  Given any connection
  When it sends CONFIG GET maxmemory maxmemory*
  Then maxmemory is reported exactly once
  And the surviving entries keep the server's own table order
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
Wire contract (RESP, not HTTP) — per CONNECTION, per BATCH:

  For a batch of replies R[0..n) flushed in one write, let V(i) be the connection's
  protocol_version at the moment R[i] was PRODUCED. Then R[i] is serialized with
  serialize_resp3 iff V(i) >= 3, else with serialize.

  V(0) = the version in effect when the batch began.
  A protocol-changing command at index k sets V(i) = new for all i >= k  (INCLUSIVE of k:
  a HELLO's own reply is encoded in the protocol that HELLO establishes — measured on
  redis-server 8.6.1).

Internal shape (frozen):
  ConnectionState {
    proto_switches:    SmallVec<[(usize, u8); 2]>,   // (reply index, version) in batch order
    proto_batch_start: u8,                            // version V(0) for the pending batch
  }
  shared::note_protocol_switch(conn, at: usize, version: u8)
      -> MUST be called BEFORE `conn.protocol_version` is reassigned; it reads the old value to
         learn what the batch STARTED in. Called with `at = responses.len()`.
  shared::encode_response_batch(conn, responses: &[Frame], buf: &mut BytesMut)
      -> encodes per the rule above and CLEARS proto_switches. With proto_switches empty it is
         the previous single-version loop: one branch, no allocation.

  CONFIG GET <pattern...>
    -> flat array, the UNION over patterns, deduplicated, in the server's own table order;
       unknown patterns contribute nothing (all-unknown -> empty array)
    -> non-string argument: -ERR invalid argument
```

Status: FROZEN @ v1 — approved by Tin Dang (auto, `autonomy: auto`)

Least-sure flag surfaced at freeze: **[contract] the INCLUSIVE boundary** — whether a HELLO's
own reply belongs to the old protocol or the new one. Cost if wrong: the handshake reply itself
is misparsed, which is worse than the bug being fixed. Resolved by measurement against
redis-server 8.6.1 rather than by reasoning: `HELLO 3` on a RESP2 connection answers `%7`, so the
switch is inclusive of its own index. `note_protocol_switch(conn, responses.len(), …)` is called
BEFORE the HELLO reply is pushed, which is what makes the recorded index inclusive.

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject above has a test; both runtimes; shards 1 and 4.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - bpv1_a_reply_produced_before_hello_2_keeps_its_resp3_encoding: arrange HELLO 3 / act one
    pipelined write CONFIG GET, HELLO 2, CONFIG GET / assert type bytes "%**"  (RED before fix:
    observed "***")
  - bpv2_a_reply_produced_before_hello_3_keeps_its_resp2_encoding: assert "*%%" — the direction
    that already passes, PINNED so the fix cannot trade one direction for the other
  - bpv3_two_hellos_in_one_batch_each_take_effect_from_their_own_index: assert "*%%**"
    (RED before fix: observed "*****")
  - bpv4_a_batch_without_hello_is_encoded_entirely_in_one_protocol: assert "%%%%%" — pins the
    no-switch fast path against regression
  - bpv5_config_get_honours_every_parameter_not_just_the_first: assert both names present, an
    unknown pattern skipped rather than erroring (RED before fix: second name absent)
  - bpv6_config_get_deduplicates_overlapping_patterns: assert maxmemory appears exactly once
  - bpv7_reset_is_a_protocol_switch_and_does_not_reach_backwards: assert "%+*" — RESET is the
    SECOND protocol-moving command and fails through a different code path
    (`shared::try_handle_reset`), so a HELLO-only fix leaves it red (RED at the time it was added,
    AFTER the HELLO sites were already green: observed "*+*")
  - shared.rs::proto_walk_tests (5 unit tests): a switch never reaches backwards; a switch applies
    at its OWN index; every switch is honoured in order; no switches = one version throughout; a
    switch beyond the last reply is inert
</test_plan>

Tests live in: `tests/batch_protocol_version.rs` · `src/server/conn/shared.rs`
MUST run red (missing implementation) before Build — confirmed: bpv1, bpv3, bpv5 red; bpv2, bpv4,
bpv6 green from the start (they are pins, not proofs).

Every test body runs against a server on `--shards 1` AND `--shards 4` (`on_each_shard_count`),
because this repo's recurring defect class is a behaviour present on some dispatch paths only.

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/server/conn/core.rs` `shared.rs` `src/server/conn/handler_monoio/`
`src/server/conn/handler_sharded/` `src/command/config.rs` `tests/batch_protocol_version.rs`

Strategy (ordered batches):
  1. Write the red suite against the CURRENT binary; record which tests are red and which are pins.
  2. Add `proto_switches` + `proto_batch_start` to `ConnectionState` and the two helpers
     (`note_protocol_switch`, `encode_response_batch`) + `ProtoWalk` unit tests in `shared.rs`.
  3. Wire the four monoio sites (2 HELLO, 2 flush), then the four sharded sites.
  4. Fix `config_get` to iterate every pattern.
  5. Prove non-vacuity by reverting the walk to a single version and confirming exactly bpv1 and
     bpv3 fail.

Safety rule (feature-specific): `note_protocol_switch` MUST run before `conn.protocol_version` is
reassigned. Reversed, it records the NEW version as the batch start and the fix silently becomes a
no-op for the first switch — this exact ordering bug occurred during build and was caught by bpv1.

Code lives in: `src/server/conn/`, `src/command/config.rs`
Constraints: do NOT change any test or the contract; no allocation added to a switch-free batch.

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — monoio 6/6 + tokio 6/6 (`batch_protocol_version`); `proto_walk_tests` 5/5;
      lib 4640 (monoio) / 3806 (tokio); integration 108; `resp3_hello` 1, `resp3_type_fidelity` 13,
      `pubsub_resp3_push` 21, `protocol_error_lifetime` 8 — all green
- [x] coverage did not decrease — 11 tests added, none removed or weakened
- [x] no test or contract was altered during build
- [x] the green was EARNED — revert probe: with `ProtoWalk::new(conn.protocol_version, &[])`
      (i.e. the old single-version behaviour) EXACTLY bpv1 and bpv3 fail and nothing else does.
      The suite is therefore neither vacuous nor overfit: it fails for the defect and only for it.
- [x] concurrency / timing — the new state is per-connection and touched only on the connection's
      own task; no lock, no shared mutation, no `.await` held across it
- [x] no exposed secrets, injection openings, or unexpected dependencies — `smallvec` was already
      a direct dependency; nothing else added
- [x] layering & dependencies follow CONVENTIONS.md — the shared logic lives in
      `server/conn/shared.rs`, which is exactly the module both handlers already share; no
      handler-to-handler dependency introduced
- [x] a person reviewed and approved the change — Tin Dang, standing approval for this milestone

### Build expectations — what "correct" looks like
- [x] A RESP3 connection pipelining `CONFIG GET / HELLO 2 / CONFIG GET` reads `%`, `*`, `*` —
      confirmed on the wire by `bpv1` and by hand against live redis-server 8.6.1, which answers
      byte-for-byte the same TYPES for all four probe cases (A up, B down, C many, D dbl).
- [x] A batch containing no HELLO allocates nothing new — confirmed by reading
      `encode_response_batch`: the `proto_switches.is_empty()` arm returns before `ProtoWalk` is
      constructed, and `SmallVec::new()` is inline-capacity, never heap, until a switch is pushed.
- [x] Both shipped handlers agree — confirmed by running the whole suite at `--shards 1` and
      `--shards 4` on both runtime legs (4 combinations, 24 test runs).
- [x] `CONFIG GET maxmemory appendonly` names both — confirmed by `bpv5`; ordering/dedup semantics
      measured against redis-server 8.6.1 rather than assumed.

### Deep checks
- [x] WIRING (code) — `note_protocol_switch` referenced at 4 sites (monoio dispatch.rs ×2,
      handler_sharded/mod.rs ×2); `encode_response_batch` referenced at 4 flush sites
      (handler_monoio/mod.rs ×2, handler_sharded/mod.rs ×2); `proto_switches` /
      `proto_batch_start` read only through those two helpers. Confirmed by grep + by the revert
      probe failing (dead code cannot fail a test).
- [x] DEAD-CODE — no new unused symbol; `cargo clippy --all-targets -- -D warnings` clean on BOTH
      feature sets (default/monoio and `runtime-tokio,jemalloc`), which is what caught the one
      genuinely unused import (`BytesMut`) during the gate.

### CORRECTION to §0
§0 recorded the second reproducing path as "the tokio `handler_single` path". That is imprecise:
`main.rs` drives `run_sharded` at both call sites, so the binary never reaches `handler_single`.
The two SHIPPED paths are `handler_monoio` and `handler_sharded`, and both are fixed. The §0
measurement itself stands — the defect reproduced on both runtime legs, as the tokio leg of the
suite still confirms.

### SECOND SWITCH FOUND AT THE GATE — `RESET`
The first green suite covered only `HELLO`. A wiring sweep for every writer of
`conn.protocol_version` (`grep -rn '\.protocol_version = '`, 6 sites) surfaced a third:
`shared::try_handle_reset`, which restores the connection's default state — RESP2 included. §0 had
already MEASURED `HELLO 3` + `RESET` producing `*14`, so this was a gap between the recorded
evidence and the fix, not a new discovery. `bpv7` was written against the already-fixed binary,
observed red (`*+*`, want `%+*`), and went green once `try_handle_reset` records its switch. The
lesson generalises: "which command changes the protocol" is a set, not a special case, and the
sweep that finds all of them is over the ASSIGNMENTS, not over the command names.

### KNOWN LIMITATION — `handler_single`
`handler_single` (the library/embedding path, `server::run`; `main.rs` drives `run_sharded` at both
call sites, so no shipped binary reaches it) flushes through `Framed::send`, encoding each frame
with the codec's version at send time, and therefore still retro-encodes a batch containing a
protocol switch. It is NOT fixed here: its two flush sites include `flush_with_aof_ack`, which
takes a bare sink and no `ConnectionState`, so threading the switch walk through it is a change of
a different size on a path with no shipped caller. What IS done is bounding it — the handler clears
`proto_switches` at each batch boundary, because it shares `try_handle_reset` and would otherwise
accumulate switch records forever on a connection that RESETs repeatedly. Filed as a spec delta.

### GATE RECORD
Outcome: PASS
Reviewed by: Tin Dang · date: 2026-08-15

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): the four bpv wire assertions run on every CI leg and at
shards 1 and 4 — a regression shows up as a type-byte mismatch, not as a latency number. The
no-switch fast path is the thing most likely to be quietly undone by a future refactor, which is
why bpv4 pins it even though it has never been red.

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.

  - [SPEC · open] any FUTURE command that mutates `conn.protocol_version` must call
    `note_protocol_switch` before the assignment; nothing enforces this today (evidence: the
    ordering bug occurred during this very build and was caught only by bpv1, not by the compiler)
  - [SPEC · open] `handler_single` still retro-encodes a batch containing a protocol switch; it is
    bounded (switch record cleared per batch) but not fixed, because `flush_with_aof_ack` takes a
    sink with no `ConnectionState` (evidence: the KNOWN LIMITATION section above; no shipped binary
    reaches this handler, which is why it was scoped out rather than rushed)
  - [SPEC · open] `CONFIG GET` reports a hand-maintained table of ~23 parameters; anything Moon
    accepts via `CONFIG SET` but omits here is invisible to a client that reads its own config
    back (evidence: found while measuring dedup/order semantics against redis-server 8.6.1)

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->

  - [TDD · open] a test that passes BEFORE the fix is still worth writing, but must be labelled a
    pin rather than counted as red — bpv2/bpv4/bpv6 were green from the start, and calling the
    suite "red" without that distinction would have overstated the evidence (evidence: only
    bpv1/bpv3/bpv5 were genuinely red; the revert probe confirms exactly bpv1+bpv3 own the fix)
  - [ADD · open] a defect reachable only by a single `write()` of two commands is invisible to
    every `redis-cli`-driven and client-library-driven test in this repo; wire-level suites need a
    raw-socket harness, not a client (evidence: this bug survived 13 prior milestone tasks)
  - [TDD · open] when a defect has a KNOWN second trigger recorded in §0, write its test even after
    the first one is green — the RESET case was measured in §0, survived the whole build, and was
    caught only by a gate-time sweep over the state assignment rather than over the command names
    (evidence: bpv7 red against an otherwise-green binary)
