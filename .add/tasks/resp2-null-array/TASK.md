# TASK: RESP2 null array: Frame::NullArray so BLPOP, EXEC-abort and friends reply *-1

slug: resp2-null-array · created: 2026-08-17 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: done   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Touches (files · symbols · signatures):
- `src/protocol/frame.rs:119` — `pub enum Frame`; `Frame::Null` at :137 documented as "`$-1\r\n` (RESP2) or `_\r\n` (RESP3)". One variant, two wire meanings — this is the defect's root.
- `src/protocol/serialize.rs:87` — `serialize()` RESP2 arm: `Frame::Null => buf.put_slice(b"$-1\r\n")`. **This match has ZERO wildcard arms**, so adding a variant is a compile error here, not a silent fallthrough. That is the safety net for the whole task.
- `src/protocol/serialize.rs:206` — `serialize_resp3()` arm: `Frame::Null => buf.put_slice(b"_\r\n")`.
- `src/protocol/parse.rs:101` — `parse_count!` macro: `if count == -1 { return Frame::Null }`. Shared by `*`, `%`, `~`, `>`. Moon parses *replies* here too (replication, peers), so `*-1` currently round-trips to `$-1`.
- `src/protocol/parse.rs:204` — `b'_'` arm → `Frame::Null` (RESP3 null; must stay `Null`).
- `src/server/conn/shared.rs:268` — `execute_transaction` (`#[cfg(feature = "runtime-tokio")]`), WATCH conflict → `Frame::Null`.
- `src/server/conn/shared.rs:380` — the sharded/CAS-gate executor, same abort → `Frame::Null`.
- `src/server/conn/blocking.rs:488,493,513,853,858,877` — blocking-command timeout paths → `Some(Frame::Null)`; `:308,348,1289,1509` are the `BRPOPLPUSH` arms.
- `src/command/list/list_write.rs:177` — `LPOP`/`RPOP` on an absent key: `if args.len() == 2 { Frame::Array(framevec![]) } else { Frame::Null }` → the count form emits `*0`, not a null at all.
- `src/command/list/list_write.rs:882` — `LMPOP` falls through to `Frame::Null`.
- `src/command/sorted_set/sorted_set_write.rs:848` — `ZMPOP` falls through to `Frame::Null`.
- `src/command/stream/stream_read.rs:267,787` — `XREAD` no-data → `Frame::Null`.
- `src/command/geo/geo_cmd.rs:162` — `geopos` absent member → `Frame::Null` (nested inside the outer array); `:766` `geopos_readonly` is the mirror.
- `src/command/geo/geo_cmd.rs:247,258,260,891,901` — **`geohash`**, whose absent member is `$-1` in Redis. Same file, opposite null. Do not touch.

Context (working folder): oracle probe scripts (scratch, uncommitted) —
`null_oracle2.py` (RESP2 A/B table), `null_oracle3.py` (RESP3 + geo cases).
Measured 2026-08-17 against `redis-server 8.6.1` and Moon `--shards 1`.
Full RESP2 table posted to moon#482 (`#issuecomment-5311292918`).

Honors (patterns / conventions):
- CLAUDE.md **Parser defensiveness** — `parse_frame_zerocopy` returns `Frame::Null` on ANY parse failure and must never panic. The new variant must not become a second failure sentinel.
- CLAUDE.md **Allocations on hot paths** — `src/protocol/` and `src/command/` are on the no-alloc list; a new unit variant allocates nothing, and no arm added here may allocate.
- `gotcha_moon_three_dispatch_paths` — dispatch / dispatch_read / inline are separate; a reply changed in one is invisible in the others.
- `gotcha_default_features_dropped_on_tokio_leg` + `project_464_monoio_ci_coverage` — monoio is the shipped runtime; prove both legs.

Anchors the contract cites: `Frame::NullArray`, `serialize`, `serialize_resp3`,
`parse_count!`, `execute_transaction`, `geopos`, `geohash`.

**Wildcard-arm census** (the sites the compiler will NOT catch, so they are hand-audited):
`src/protocol/resp3.rs` 4 · `src/scripting/types.rs` 9 · `src/admin/http_server.rs` 6 ·
`src/admin/console_gateway.rs` 3 · `src/protocol/frame.rs` 1 · `src/admin/ws_bridge.rs` 0.
154 files reference `Frame::` in total.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: A distinct null-aggregate frame, so that a reply whose value is a
missing *array* decodes as a missing array in a statically-typed client instead
of as a missing string.

Framings weighed:
- **A new `Frame::NullArray` variant (chosen)** — the compiler enumerates every
  serializer for us (`serialize.rs` has no wildcard arms), the variant nests
  naturally inside `Frame::Array` for GEOPOS, and each call site states which
  null it means at the point where that is actually known.
- *A flag on `Frame::Null`* (`Null(NullKind)`) — rejected: it changes the shape
  of ~250 existing `Frame::Null` constructions and pattern matches, i.e. it
  makes the blast radius the whole codebase to avoid adding one variant.
- *Fix it in the serializer by tracking the command* — rejected: the serializer
  does not know which command produced the frame, and threading that through
  would put per-reply state on the hot path.

Must:
<must>
  - `Frame::NullArray` serialises to `*-1\r\n` under RESP2 and `_\r\n` under RESP3 (both measured on redis 8.6.1).
  - `Frame::NullArray` nests: as an element of `Frame::Array` it emits `*-1` inline, so `GEOPOS live-key absent-member` is `*1\r\n*-1\r\n`.
  - The eight blocking commands reply `NullArray` on timeout: `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`.
  - `EXEC` aborted by a broken `WATCH` replies `NullArray`, on BOTH transaction executors (`shared.rs:268` and `shared.rs:380`) and under BOTH runtimes.
  - `LPOP key <count>` and `RPOP key <count>` on an absent key reply `NullArray` — today they reply `*0`, so this is a change of a non-`Frame::Null` site.
  - `LMPOP` and `ZMPOP` finding no non-empty key reply `NullArray`.
  - `XREAD` reply `NullArray` when no stream has data, for both an absent stream and a live stream with nothing past the given ID.
  - `GEOPOS` replies `NullArray` for an absent member.
  - `parse_frame_zerocopy` maps the byte sequence `*-1\r\n` to `Frame::NullArray`, so a reply parsed from a peer re-serialises unchanged.
  - `Frame::NullArray != Frame::Null` under `PartialEq`.
</must>
Reject:
<reject>
  - A `Frame::Null` site NOT named in the contract table is changed -> the task is wrong; the ~19 measured `$-1` sites (GET, HGET, LPOP-no-count, LMOVE, LPOS, LINDEX, ZSCORE, ZRANK-no-WITHSCORE, ZRANDMEMBER/SPOP/SRANDMEMBER/HRANDFIELD-no-count, GETEX, GETDEL, SET..GET, OBJECT ENCODING, GEODIST, **GEOHASH**, XADD NOMKSTREAM, ACL GETUSER, CLIENT GETNAME) must still emit `$-1`.
  - An empty-aggregate site is turned into a null aggregate -> the ~12 measured `*0` sites (ZPOPMIN, SPOP/SRANDMEMBER/ZRANDMEMBER/HRANDFIELD with count, XRANGE, SMEMBERS, HGETALL, SORT_RO, GEOSEARCH, LPOS..COUNT, KEYS, CONFIG GET) must still emit `*0`.
  - `parse_frame_zerocopy` panics, or returns `NullArray` as a parse-FAILURE sentinel -> malformed input must still yield `Frame::Null`; only a well-formed `*-1` yields `NullArray`.
  - A wildcard `_ =>` arm silently absorbs `NullArray` in a Lua / JSON / console / metrics converter -> each of the 23 censused wildcard arms is inspected and handled explicitly.
</reject>
After:
<after>
  - Re-running the oracle A/B over the probe set shows zero reply-type divergences for the 16 rows in scope, and the previously-matching rows still match.
  - A statically-typed client (`redis-py`, `go-redis`) that calls `BLPOP` on an empty key inside `MULTI` decodes the reply instead of raising.
  - Both runtimes and both protocol versions produce the contract's bytes.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ **Parsing `*-1` as `NullArray` is safe on the COMMAND path, not just the reply path.** `parse_frame_zerocopy` parses inbound client commands *and* inbound peer replies with the same function; a client that sends a bare `*-1\r\n` currently yields `Frame::Null` and is ignored. Lowest confidence because the two callers are not separated by type, so I am changing the command path to prove the reply path. If wrong: a malformed or hostile client frame takes a different branch in the connection handler than it does today — a protocol-parsing regression, which is the one area CLAUDE.md singles out as never-crash. Mitigation: a test asserts a bare `*-1` command is still refused, not dispatched.
  - [ ] **The 23 wildcard arms all want `NullArray` to behave exactly like `Null`.** Lua converts both nils to `false`; JSON/console render both as `null`. Believed true because RESP3 itself collapses them (measured: both are `_\r\n`), so any consumer above the wire cannot already be distinguishing them. Confirm by reading each arm — never by assuming.
  - [ ] **`BRPOPLPUSH`/`BLMOVE` timeout is `*-1`, which contradicts the intuition that a single-element reply is a bulk.** Measured, not assumed: both are `*-1` on redis 8.6.1 even though a successful reply is a bulk string. Confirmed by probe; recorded because it looks like a bug in the fix if a reviewer checks it against intuition.
  - [x] **No persisted format embeds a serialized `Frame::Null`** such that a new variant changes bytes at rest. **CONFIRMED 2026-08-17 before freeze**: `src/persistence/` stores COMMANDS (`Frame::Array` of args — `migrate_aof.rs:613,694`, `recovery.rs:413`), and nothing outside `src/protocol/` calls `serialize_frame`/`serialize_resp3_frame`. The one `Frame::Null` in `src/replication/apply.rs:534` is an internal "ignored this record" result, never written to a wire or a file. No format changes.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first. -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: a blocking pop that times out is a missing ARRAY
  Given a RESP2 client and a key that does not exist
  When it sends BLPOP <key> 0.05 and the timeout expires
  Then the reply bytes are exactly "*-1\r\n"
  And GET on that same key still replies "$-1\r\n"

Scenario: the same reply under RESP3 collapses to the single null
  Given a client that has sent HELLO 3
  When it sends BLPOP <key> 0.05 and the timeout expires
  Then the reply bytes are exactly "_\r\n"

Scenario: a transaction aborted by a broken WATCH is a missing ARRAY
  Given client A has WATCHed a key, opened MULTI and queued a command
  And client B has since written that key
  When client A sends EXEC
  Then the reply bytes are exactly "*-1\r\n"
  And the queued command did not execute

Scenario: popping N from an absent key is a missing array, not an empty one
  Given a RESP2 client and a key that does not exist
  When it sends LPOP <key> 2
  Then the reply bytes are exactly "*-1\r\n"
  And ZPOPMIN on an absent key still replies "*0\r\n"

Scenario: a null array nests inside an enclosing array
  Given a geo key holding one member
  When the client sends GEOPOS <key> <absent-member>
  Then the reply bytes are exactly "*1\r\n*-1\r\n"
  And GEOHASH <key> <absent-member> still replies "*1\r\n$-1\r\n"

Scenario: a parsed *-1 re-serialises as *-1
  Given a buffer containing "*-1\r\n"
  When parse_frame_zerocopy reads it and the result is serialised back to RESP2
  Then the output bytes equal the input bytes
  And a buffer containing "_\r\n" still parses to Frame::Null

Scenario: a malformed frame is still the Null sentinel, never NullArray
  Given a buffer containing a truncated or negative-length aggregate ("*-7\r\n")
  When parse_frame_zerocopy reads it
  Then the result is Frame::Null
  And no panic occurs

Scenario: a bare *-1 sent as a COMMAND is refused, not dispatched
  Given a connected RESP2 client
  When it sends the bytes "*-1\r\n" where a command array is expected
  Then the server does not execute a command
  And the connection is still usable for a subsequent PING

Scenario: the sites that were already correct do not move
  Given a RESP2 client
  When the full oracle probe set is replayed against Moon and against redis-server
  Then every probe's reply type is identical between the two
```

</scenarios>

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
Frame::NullArray                      -- new unit variant of `pub enum Frame`

serialize(&Frame::NullArray, buf)      -> b"*-1\r\n"      (RESP2)
serialize_resp3(&Frame::NullArray, buf)-> b"_\r\n"        (RESP3)
parse_frame_zerocopy(b"*-1\r\n")       -> Frame::NullArray
parse_frame_zerocopy(b"_\r\n")         -> Frame::Null      (unchanged)
parse_frame_zerocopy(<malformed>)      -> Frame::Null      (unchanged — failure sentinel)
Frame::NullArray == Frame::NullArray, Frame::NullArray != Frame::Null

REPLY TABLE — the 16 sites that change (measured, redis 8.6.1 vs moon --shards 1)

  site                                        anchor                                   before -> after
  BLPOP/BRPOP timeout                         blocking.rs:488,493,853,858              $-1  -> *-1
  BLMOVE timeout                              blocking.rs:513,877                      $-1  -> *-1
  BRPOPLPUSH timeout                          blocking.rs:348,1289,1509                $-1  -> *-1
  BLMPOP timeout                              blocking.rs (blocking mpop arm)          $-1  -> *-1
  BZPOPMIN/BZPOPMAX timeout                   blocking.rs (zset arms)                  $-1  -> *-1
  BZMPOP timeout                              blocking.rs (zset mpop arm)              $-1  -> *-1
  LPOP key <count>, absent key                list_write.rs:177                        *0   -> *-1
  RPOP key <count>, absent key                list_write.rs (rpop mirror of :177)      *0   -> *-1
  LMPOP, no non-empty key                     list_write.rs:882                        $-1  -> *-1
  ZMPOP, no non-empty key                     sorted_set_write.rs:848                  $-1  -> *-1
  XREAD, absent stream                        stream_read.rs:267                       $-1  -> *-1
  XREAD, live stream past the ID              stream_read.rs:787                       $-1  -> *-1
  GEOPOS, absent member (NESTED)              geo_cmd.rs:162 + :766 mirror             $-1  -> *-1
  EXEC aborted by WATCH (embedded executor)   shared.rs:268                            $-1  -> *-1
  EXEC aborted by WATCH (CAS-gate executor)   shared.rs:380                            $-1  -> *-1
  parse of a peer reply "*-1"                 parse.rs:107 (parse_count!)              Null -> NullArray

REGRESSION FENCE — must NOT change
  still $-1 : GET · HGET · LPOP/RPOP no-count · LMOVE · LPOS · LINDEX · ZSCORE ·
              ZRANK no-WITHSCORE · ZRANDMEMBER/SPOP/SRANDMEMBER/HRANDFIELD no-count ·
              GETEX · GETDEL · SET..GET · OBJECT ENCODING · GEODIST · GEOHASH ·
              XADD NOMKSTREAM · ACL GETUSER · CLIENT GETNAME
  still *0  : ZPOPMIN · SPOP/SRANDMEMBER/ZRANDMEMBER/HRANDFIELD with count · XRANGE ·
              SMEMBERS · HGETALL · SORT_RO · GEOSEARCH · LPOS..COUNT · KEYS · CONFIG GET
  still _   : every one of the above under RESP3 that is a null at all

Schema: no persisted format changes. WAL/AOF/RDB record COMMANDS, not replies;
        confirm before build (§1 assumption 4).
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-08-17)

<!-- Lowest-confidence flags to surface at the freeze:
  [spec]     ⚠ parsing `*-1` as NullArray also changes the inbound COMMAND path, which shares
             parse_frame_zerocopy with the reply path. Cost: a protocol-parsing regression in the
             one area CLAUDE.md marks never-crash. Covered by the "bare *-1 as a command" scenario.
  [contract] the 23 censused wildcard `_ =>` arms are the only places the compiler will NOT force a
             decision. Cost: a converter silently renders NullArray as something wrong (e.g. Lua
             gets `nil` where it should get `false`). Covered by the wiring deep-check, not a test.
-->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject has a test; no Must proven only by a unit test where the defect is on the wire.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - rna1_blocking_timeouts_are_null_array: spawn moon; for each of BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP on an absent key, assert the reply bytes are exactly "*-1\r\n"; assert GET on the same key is still "$-1\r\n"
  - rna2_resp3_collapses_to_underscore: after HELLO 3, BLPOP timeout replies exactly "_\r\n"
  - rna3_exec_aborted_by_watch_is_null_array: two connections; A WATCH+MULTI+queue, B writes, A EXEC -> "*-1\r\n"; assert the queued command did not run
  - rna4_pop_with_count_on_absent_key: LPOP k 2 and RPOP k 2 -> "*-1\r\n"; ZPOPMIN k -> "*0\r\n" in the same test (the fence and the fix in one assertion block)
  - rna5_geopos_nests_null_array: GEOPOS live absent-member -> "*1\r\n*-1\r\n"; GEOHASH live absent-member -> "*1\r\n$-1\r\n"
  - rna6_lmpop_zmpop_xread_are_null_array: LMPOP/ZMPOP/XREAD(absent) and XREAD(live, past ID) -> "*-1\r\n"
  - rna7_regression_fence: replay the measured $-1 set and *0 set; assert each still has its recorded type (this is the test that fails if the audit over-reaches)
  - test_null_array_round_trips (unit, parse.rs): parse("*-1\r\n") == NullArray and re-serialises to "*-1\r\n"; parse("_\r\n") == Null
  - test_malformed_aggregate_is_null_not_null_array (unit, parse.rs): "*-7\r\n" and a truncated aggregate -> Frame::Null, no panic
  - test_bare_null_array_command_is_not_dispatched (integration): send "*-1\r\n" as a command; assert no execution and a following PING still answers +PONG
  - test_null_array_ne_null (unit, frame.rs): PartialEq distinguishes them
</test_plan>

Tests live in: `tests/resp2_null_array.rs` · unit arms in `src/protocol/parse.rs`, `src/protocol/serialize.rs`, `src/protocol/frame.rs`.
MUST run red (missing implementation) before Build.

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/protocol/frame.rs` `src/protocol/serialize.rs` `src/protocol/parse.rs`
`src/protocol/resp3.rs` `src/scripting/types.rs` `src/admin/http_server.rs`
`src/admin/ws_bridge.rs` `src/admin/console_gateway.rs` `src/server/conn/shared.rs`
`src/server/conn/blocking.rs` `src/command/list/list_write.rs`
`src/command/sorted_set/sorted_set_write.rs` `src/command/stream/stream_read.rs`
`src/command/geo/geo_cmd.rs` `tests/resp2_null_array.rs` `scripts/test-consistency.sh`
`scripts/test-commands.sh` `CHANGELOG.md`

Strategy (ordered batches):
1. Add the variant + both serializer arms + `PartialEq` + the parse arm. Compile — the
   error list from `serialize.rs` and every other wildcard-free match IS the audit worklist.
2. Walk the 23 censused wildcard arms by hand; make each explicit. Nothing behavioural yet.
3. Change the 15 reply sites in the contract table, one command family per commit-sized batch.
4. Regression fence + oracle re-run; then the shell suites and CHANGELOG.

Safety rule (feature-specific): the parse-failure sentinel stays `Frame::Null`. `NullArray` is
only ever produced by a WELL-FORMED `*-1`, never by a bail-out path. Every `return Frame::Null`
inside `parse.rs`'s defensive guards stays exactly as it is.

Code lives in: `src/`
Constraints: do NOT change any test or the contract; no new `unsafe`; no allocation added to
`src/protocol/` or `src/command/` paths; ask if unclear.

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — see the two-runtime record below
- [x] coverage did not decrease — 8 new integration tests + 9 new unit tests, none removed
- [x] no test or contract was altered during build — one exception, declared and justified below (`test_parse_null_array` pinned the defect)
- [x] the green was EARNED, not gamed — proven three independent ways (mutation, pre-fix binary A/B, no-server vacuity check)
- [x] concurrency / timing of the risky operation is safe — no new shared state; the change is a pure enum variant plus its serialization. No lock, no `await`, no allocation added on any hot path.
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new deps; parser change is length-bounded and returns the existing `Frame::Null` failure sentinel on anything malformed
- [x] layering & dependencies follow CONVENTIONS.md — protocol change stays in `src/protocol/`; consumers only pattern-match
- [ ] a person reviewed and approved the change

### Two-runtime test record (measured, not inferred)

| leg | invocation | passed | failed | `rna*` seen |
|---|---|---|---|---|
| monoio (SHIPPED runtime, default features) | `cargo test --profile release-fast --no-fail-fast` | 5416 | 2 (both load flakes) | 9 |
| tokio (CI portability leg) | `MOON_NO_URING=1 cargo test --profile release-fast --no-default-features --features runtime-tokio,jemalloc --no-fail-fast` | 4646 | 2 (both load flakes) | 9 |

All four failures are **server-spawning suites failing on connection setup under build load**, all four are
**green when run isolated**, and the two sets are **disjoint between the legs** — which is what a flake looks
like and what a real regression does not. None of them touches a reply type:

- monoio: `cb12_readonly_serves_replica_reads_but_never_replica_writes` (`connect to :25533 kept failing`) → isolated 20/20 ok · `parked_connection_serves_all_traffic_after_wake` (`Connection reset by peer`) → isolated 7/7 ok
- tokio: `persistence::manifest::tests::test_overflow_compaction_bounds_growth` (`injected persist failure (test)` — a fault-injection toggle bleeding across parallel tests) → isolated ok · `killed_node_is_flagged_by_survivors` (`BrokenPipe` writing to the node the test itself kills) → isolated 3/3 ok

Lint legs, both green with `--all-targets` (stricter than CI): `cargo clippy --all-targets -- -D warnings`
and `cargo clippy --no-default-features --features runtime-tokio,jemalloc --all-targets -- -D warnings`.
`cargo fmt --all --check` clean.

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
- [x] The oracle A/B run against the BUILT binary returns zero type divergences vs `redis-server 8.6.1` — **CONFIRMED**. RESP2 at `--shards 1` and `--shards 4`: 51 probes, **3 divergences, all out of scope and separately filed** (#520 RPOPLPUSH unimplemented, #521 ZRANK/ZREVRANK WITHSCORE). All 16 null-type rows now match. RESP3: every row matches including `GEOPOS`→`*1\r\n_\r\n`, `GEOHASH`→`*1\r\n_\r\n`, `EXEC`-abort→`_`; the only RESP3 miss is `GET`, which is #522.
- [x] The regression fence fires — **CONFIRMED by mutation**, restored from a `/tmp` copy (never `git checkout`). Mutating GEOHASH's 3 absent-member arms to `NullArray` turned `rna5` red with its intended message. Separately, the shell block was run against the PRE-FIX binary: **11 FAIL / 9 pass**, the 11 being exactly the parity set and the 9 being exactly the fence — so neither half is vacuous.
- [x] `cargo test --no-fail-fast` on BOTH runtimes shows `resp2_null_array` actually ran — **CONFIRMED by grepping `^test rna[0-9]`, not by reading a green total: 9 lines on each leg** (8 running + the committed `#[ignore]`d #522 fence). The count is asserted because a suite that fails to link, or a file cargo never picked up, still leaves a green total behind.
- [x] No new wildcard arm was introduced to make the compiler quiet — **CONFIRMED**: every arm added is an explicit `Frame::NullArray` (or `Frame::Null | Frame::NullArray`) pattern; `git diff` shows no new `_ =>`.

### Corrections to my own §0/§4 — recorded rather than quietly fixed
- **The compiler-driven audit was weaker than §0 claimed.** §0 said `serialize.rs`'s wildcard-free match makes the variant a compile error and "that is the safety net for the whole task". Under the DEFAULT feature set the build went clean, because every other consumer has a catch-all. The real worklist only appeared under `--features console`: 3 exhaustive matches (`http_server.rs:396`, `console_gateway.rs:187`, `ws_bridge.rs:214`) that the default build never compiles. Feature-gated code hides the compiler's help.
- **My wildcard census (23 arms) was built with the wrong instrument.** I grepped `^\s+_ =>`, which misses a named catch-all (`other =>`). A clippy-driven census (`-W clippy::wildcard_enum_match_arm`) returned **1227** sites codebase-wide — too broad to be the worklist, since most match command ARGUMENTS where the variant cannot appear. The audit that actually mattered was the ~6 reply CONSUMERS, hand-walked.
- **§5 scope did not list `src/command/graph/graph_read.rs`**, which I touched (`hash_frame_bytes`, a result-cache key). One file outside the declared scope; the change is 3 lines and additive.
- **One existing test was changed**: `protocol::parse::tests::test_parse_null_array` asserted `*-1 -> Frame::Null`, i.e. it pinned the defect. The frozen contract requires the opposite, so its expectation moved with the contract. Recorded here because "do not change a test to make the build pass" is a standing rule and this is the one exception, taken deliberately.

### The one contract item NOT delivered — and why
The issue asked to un-ignore `me7_blpop_in_multi_returns_null_array_not_null_bulk`.
I did, and it still failed. Inside `MULTI` the queued command is rewritten
`BLPOP k t` -> `LPOP k`, so the reply is LPOP's — and LPOP's null correctly IS
`$-1`. Measuring the HIT path against redis 8.6.1 showed the rewrite is wrong
in SHAPE, not just in null type:

    Redis  MULTI; BLPOP q 0; EXEC -> [["q", "v1"]]   (key AND value)
    Moon   MULTI; BLPOP q 0; EXEC -> ["v1"]          (key dropped)

`BLPOP` answers `[key, value]` precisely because it takes many keys. Patching
only the null here would have turned `me7` green over a command that still
answers the wrong shape — the letter of the request against its purpose. Filed
as **#524** with the hit-path assertion named as its acceptance criterion, and
`me7`'s ignore REASON moved to point there rather than being restated. The
assertion itself was left unweakened.

### Findings split out rather than folded in (each with its own issue)
- **#522** RESP3 `GET` miss replies `$-1`; per-KEY at `--shards 4` (4/8 wrong, splitting on shard ownership) because the inline path writes hardcoded bytes. Kept as a committed `#[ignore]`d test.
- **#523** A timed-out `BLPOP`/`BLMOVE` on a missing key leaves a phantom empty list (`EXISTS`→1, `TYPE`→list, `DBSIZE`→1). **A/B-verified pre-existing**: identical on `origin/main` @ `8f784777` and on this branch, two separately built binaries (md5 `3b3d168c…` vs `aa25a09d…`).
- **#520** `RPOPLPUSH` unimplemented · **#521** `ZRANK/ZREVRANK ... WITHSCORE` unsupported.
- Pre-existing and NOT fixed here: `cargo clippy --all-targets --features console` fails on `approx_constant` in `console_gateway.rs` test code. Confirmed identical on `origin/main`. CI's console job does not pass `--all-targets`, and that exact invocation is green.

### Deep checks — do not skim
- [x] WIRING (code) — enumerated, not eyeballed. **20 reply-construction sites**, not the 15 §3 estimated
  (`blocking.rs` 10 · `shared.rs` 2 · `list_write.rs` 3 · `stream_read.rs` 2 · `geo_cmd.rs` 2 ·
  `sorted_set_write.rs` 1), plus **1 parser site** (`parse.rs:169`, the `*-1` arm). The count came in
  above the estimate because `blocking.rs` carries the same wait shape four times (tokio and monoio ×
  single-key and multi-key) and each has both a deadline and a no-deadline exit. Consumed by both
  serializers (`serialize` → `*-1`, `serialize_resp3` → `_`), and passed through by `resp3.rs`'s shape
  guard, `scripting/types.rs` (→ Lua `false`), and the three console matches.
- [x] DEAD-CODE (code) — no helper was added, so none was orphaned. `Frame::Null` remains in use as
  BOTH the missing-string reply and the parse-failure sentinel; the variant did not replace it anywhere
  it was correct. Verified by the regression fence (`rna7`), which asserts the already-correct `$-1`
  sites — GET, LPOP without count, GEOHASH, BRPOPLPUSH's success shape — did not move.
- [x] SEMANTIC (prose) — n/a

### GATE RECORD
Outcome: PASS
Reviewed by: Tin Dang (pending human sign-off on the PR) · date: 2026-08-17
Target hit: yes — every measured RESP2 null-type divergence vs `redis-server 8.6.1` is gone at both
`--shards 1` and `--shards 4`; the residue is four separately-filed issues (#520, #521, #522, #524),
each with a named acceptance criterion, none of them a null-type divergence this contract covers.

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch: rate of client-side decode errors on blocking commands and on EXEC.

### Spec delta
- [SPEC · seeded → moon#522] RESP3 `GET` on a missing key replies `$-1`, not `_`, because the inline fast path writes hardcoded bytes (`blocking.rs:1831`) and never reaches the version-aware serializer. Per-KEY at `--shards 4` (4/8 keys wrong — local keys wrong, remote keys right), so a client cannot even cache "this server is correct". Evidence: `rna2b_resp3_get_miss_is_underscore` (committed `#[ignore]`d; un-ignoring it is #522's acceptance criterion).
- [SPEC · dropped] "RESP3 `SMEMBERS` on an absent key returns `*0` instead of `~0`" — WRONG, withdrawn. I read that row off the *Redis* RESP3 run and attributed it to Moon. Re-measured against the built Moon binary: `~0`, matching Redis. No defect.

### Competency deltas
- [TDD · open] The A/B oracle found in one run what the whole `v0-9-client-compat` milestone's suites missed, because those suites assert Moon against Moon's own expectations. Diffing against a live reference server should be a standing verifier, not a one-off — evidence: 16 wrong reply types plus 2 missing commands (#520, #521) surfaced on first run.
- [ADD · open] The first oracle script produced 30 confident false findings because one probe mutated the key the next probe read. The instrument needed its own correctness check before its output was evidence — evidence: `WRONGTYPE` cascade in the v1 run.
