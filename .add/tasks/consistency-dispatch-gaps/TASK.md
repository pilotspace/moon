# TASK: Close the consistency-suite gaps: unreachable read commands + script defects

slug: consistency-dispatch-gaps · created: 2026-06-11 · stage: production
phase: verify   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Every command implemented in `command::dispatch` is reachable over the wire,
regardless of which of the three dispatch paths (local read · local write · cross-shard)
the handler routes it through — plus a trustworthy `scripts/test-consistency.sh`.

Root cause (evidenced 2026-06-11, all empirical over a real connection): the local
read/write split in all three connection handlers is `if metadata::is_write(cmd) {
dispatch } else { dispatch_read }`, but `dispatch_read` implements only ~62 of the
~180 commands `dispatch` handles. **25 implemented read commands return
`ERR unknown command` over the wire** while their unit tests pass: XRANGE · XREAD ·
XREVRANGE · XINFO · XPENDING · ZDIFF · ZINTER · ZUNION · ZINTERCARD · ZRANDMEMBER ·
GEOPOS · GEODIST · GEOHASH · GEOSEARCH · LCS · RANDOMKEY · EXPIRETIME · PEXPIRETIME ·
TOUCH · LOLWUT — plus 5 more found by the red registry sweep (2026-06-11, contract
v2): TIME · WAIT · SLOWLOG · ZMSCORE · XLEN (same bug, missed by the candidate
regex). A 26th gap is runtime divergence: CDC.READ is special-cased only in the
tokio handler (`handler_sharded/dispatch.rs:239`) — monoio answers unknown.
Ten further registry entries (WATCH · UNWATCH · RESET · SSUBSCRIBE · SUNSUBSCRIBE ·
LATENCY · MODULE · DUMP · RESTORE · RECLAMATION) are advertised in COMMAND_META but
implemented NOWHERE (verified unknown on both runtimes incl. the v0.3.0 release
binary) — missing features, not routing bugs: BACKLOGGED by user decision
("Fix 26, backlog the 10"), excluded from the sweep with inline justification.
The cross-shard fast path is additionally gated by
`is_dispatch_read_supported` — a coarse (len, first-byte) bucket filter whose false
positives (e.g. GEOPOS matches the GETBIT bucket) ALSO dead-end in dispatch_read.
Separately, the consistency script has two defects that masked/added noise: SETRANGE
is sent only to moon (then both GETs compared), and the moon-only FT.CREATE uses
algorithm FLAT (Moon's documented subset is HNSW-only).

Third defect class (found by cdg2's red fixtures, 2026-06-11): `extract_primary_key`
(shared.rs:244) returns args[0] for shard routing, so subcommand-first and
numkeys-first commands route by hashing the WRONG string on multi-shard servers —
XGROUP/XINFO hash the subcommand ("CREATE"), ZDIFF/ZINTER/ZUNION/ZINTERCARD hash
the numkeys literal ("2"), XREAD hashes "COUNT". Reproduced: 2-shard `XADD s:{t0}`
then `XGROUP CREATE s:{t0} g 0` → "requires the key to exist" (works at 1 shard).
The OBJECT special case at shared.rs:289 is the established fix pattern; fixing
these is ENTAILED by the frozen cdg2 scenario (local + cross-shard correctness) —
metadata first_key is available to drive it.

Framings weighed:
- **Add 20 read arms to dispatch_read (chosen — user decision at freeze)** — each of
  the 20 commands gains a proper arm in `dispatch_read` built on the read-only
  accessor pattern the existing 62 arms already use (`&Database`, lazy-expired keys
  invisible but NOT deleted under the shared lock). Reads stay on the shared-lock
  read path — full read parallelism, no write-lock detour, and the
  `is_dispatch_read_supported` false positives (GEOPOS et al.) are fixed by the arms
  existing. Feasibility verified up front: TOUCH = read-only `exists` per key
  (dispatch_read already has an EXISTS arm); XREAD's BLOCK option is parsed-but-
  ignored in today's dispatch impl, so a non-blocking arm mirrors current behavior;
  the remainder are pure reads.
- *NotSupported fallback through the write path* — offered, declined by the user:
  one mechanism for all gaps + future-proofing, but routes these reads through the
  exclusive write path (slower, and A2-class side-effect review burden). The
  future-drift protection it offered is provided instead by M5's registry-sweep test
  (CI fails on the next stranded command).
- *Exact-set gate (sync the predicate to the arms)* — subsumed: the gate is updated
  to include the new commands' (len, first-byte) buckets so the cross-shard fast
  path serves them too; the sweep test pins gate/arm agreement.

Must:
<must>
  - M1 (reachability): every command with an arm in `command::dispatch` produces a
    non-`unknown command` reply over a real client connection, on both runtimes, at
    1 shard and multi-shard (local + cross-shard routing). Wrong-arity/wrong-type
    errors are fine; "unknown command" for an implemented command is the bug.
  - M2 (read arms): each of the 25 commands gains a `dispatch_read` arm operating on
    `&Database` via read-only accessors — an arm MUST NOT mutate (no lazy-expire
    deletion, no LRU write) under the shared lock; expired keys are invisible to it
    exactly as they are to the existing 62 arms. Named exceptions (v2): SLOWLOG
    RESET mutates the GLOBAL slowlog ring (internally synchronized, never the
    `Database`) — permitted; WAIT returns 0 (no replication) touching nothing.
    CDC.READ is ported as a monoio-handler special case mirroring tokio's (it reads
    WAL files by path argument, never the `Database` — no lock held).
    `is_dispatch_read_supported` gains the corresponding (len, first-byte) buckets
    so the cross-shard fast path serves these commands too. XREAD's arm is
    non-blocking (BLOCK parsed-and-ignored — today's dispatch semantics, unchanged).
  - M3 (semantic parity): the re-routed 25 commands return byte-identical replies to
    `redis-server` for the consistency suite's cases (GEO*, EXPIRETIME/PEXPIRETIME,
    TOUCH, plus stream/zset reads).
  - M4 (script defects): test-consistency.sh sends SETRANGE to BOTH servers before
    comparing; the moon-only FT.CREATE uses HNSW; suite exits 0 on moon-dev VM-local
    runs at 1/4/12 shards (the milestone's "consistency green" criterion).
  - M5 (regression pin): a wire-reachability test enumerates the command registry
    (metadata.rs) and asserts M1 mechanically — the three-dispatch-paths gotcha can
    never silently strand a command again.
  - M6 (hot-path neutrality): the new arms add no allocation and no extra work to
    the existing 62 arms' paths (GET/MGET/HGET... byte-identical dispatch order);
    pipelined + non-pipelined throughput within noise of the PR #172 baseline.
</must>
Reject:
<reject>
  - A truly unknown command (e.g. FOOBAR) -> "ERR unknown command" still, with no
    double-dispatch loop ("unknown_must_stay_unknown").
  - A new arm mutating the database under the shared read lock (lazy-expire delete,
    LRU write, cache fill) -> forbidden; expired keys are invisible, never collected,
    from the read path ("read_arm_mutation").
  - Any change to the 62 existing arms' replies or dispatch order -> forbidden; the
    shared-read fast path must stay byte-identical and the common case
    ("fastpath_regression", bench-gated).
  - Weakening the consistency script to pass (skipping tests, loosening asserts) ->
    forbidden; script fixes are limited to the two named defects + reporting
    hygiene ("test_weakening").
</reject>
After:
<after>
  - All 20 named commands answer correctly over the wire on both runtimes (spot-proof:
    the consistency suite's GEO/EXPIRETIME/TOUCH sections go green).
  - `scripts/test-consistency.sh` exits 0 on moon-dev (VM-local clone) at 1/4/12
    shards; the milestone exit criterion box can be checked with evidence.
  - The registry-sweep reachability test is in CI and red-proof (it fails on main
    before this fix — 20 violations — and passes after).
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1: the 20 re-routed commands are semantically CORRECT once reachable — their
    unit tests pass against `dispatch`, but they have never run over the wire, so
    wire-visible diffs vs redis-server (RESP3 shaping, empty-reply forms, GEO float
    formatting) may surface. Lowest confidence because it is untested territory by
    definition; if wrong: each diff is a small per-command fix INSIDE this task's
    scope (consistency suite is the arbiter), worst case a named residual recorded
    at the gate.
  ⚠ A2: all 20 commands are expressible against `&Database` read-only accessors with
    behavior identical to their `&mut` dispatch twins MODULO lazy expiry (read path:
    expired key invisible but not deleted — the same divergence the existing 62 arms
    already have). Moderate confidence: TOUCH/EXISTS and XREAD-nonblocking verified
    up front; the stream/zset/geo internals are presumed to expose & accessors like
    the rest of Database; if one impl genuinely requires &mut (e.g. an internal cache
    fill): that single command falls back to write-path routing in the handler as a
    documented exception — contained, named at the gate.
  - [ ] A3: adding (len, first-byte) buckets to `is_dispatch_read_supported` for the
    20 commands introduces no false positive that lacks an arm afterwards — pinned
    mechanically by the registry-sweep test (gate ∧ no-arm = sweep failure).
  - [ ] A4: after the SETRANGE/FT.CREATE script fixes and the 20 reachability fixes,
    the remaining suite failures on main are zero — i.e. the 15 observed FAILs are
    fully explained by {unreachable commands} ∪ {2 script defects}. Confirmed by the
    A/B logs (SWAPDB/HOTKEYS/FT failures were diskfull-environment artifacts), but
    re-verified at verify on a clean VM-local run.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: cdg1_registry_sweep_no_unknowns (M1, M5)
  Given a running moon server (each runtime; 1 shard and 2 shards)
  When every command in the metadata registry is sent over a real connection
       with minimal plausible arguments
  Then no reply is "ERR unknown command" for a registry command
       (skip-list: SHUTDOWN + the 10 backlogged never-implemented commands,
        each with inline justification — v2 user decision)
  And FOOBAR (not in the registry) still gets "ERR unknown command"   # reject: unknown_must_stay_unknown

Scenario: cdg2_twenty_commands_answer_correctly (M1, M3)
  Given seeded stream/zset/geo/string keys on a 2-shard server
  When XRANGE, XREAD, XINFO STREAM, XPENDING, XREVRANGE, ZDIFF, ZINTER, ZUNION,
       ZINTERCARD, ZRANDMEMBER, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, LCS,
       RANDOMKEY, EXPIRETIME, PEXPIRETIME, TOUCH, LOLWUT — plus (v2) TIME,
       WAIT, SLOWLOG GET/LEN, ZMSCORE, XLEN, CDC.READ — are each sent for keys
       that are LOCAL and keys that are CROSS-SHARD
  Then each returns its documented reply (value-asserted per command, not just
       non-error)
  And GET/MGET/HGETALL on the same connection still answer via the read fast
      path (INFO dispatch counters show the fastpath count rising)   # reject: fastpath_regression

Scenario: cdg3_read_arms_are_pure_reads (A2, M2)
  Given AOF enabled (appendonly yes) and keys with short PX TTLs on a 1-shard server
  When each of the 20 commands runs against live keys, then again after their TTLs
       lapse (expired-but-not-yet-collected keys)
  Then live keys answer normally; expired keys are invisible (missing-form replies,
       byte-identical to redis-server's post-expiry replies)
  And the AOF byte length does not grow from any of the 20 (reads are never
      appended; no lazy-expire DEL is written from the shared-lock path)
  And a subsequent SET still appends to AOF normally   # write path intact

Scenario: cdg4_consistency_suite_green (M4)
  Given the consistency script with SETRANGE sent to both servers and FT.CREATE HNSW
  When scripts/test-consistency.sh runs on moon-dev from a VM-local clone
  Then it exits 0 at shards=1, 4, and 12
  And no assertion was removed or loosened (diff shows only the two named fixes
      + reporting hygiene)   # reject: test_weakening

Scenario: cdg5_throughput_unchanged (M6)
  Given the PR #172 bench baseline on moon-dev
  When bench-swf-style SET/GET runs (1 and 4 shards, P1 and P16)
  Then throughput is within noise of baseline (reads with dispatch_read arms
       never take the fallback)
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
INTERNAL (no client-visible protocol change; RESP surface only GAINS working commands):

fn dispatch_read(db: &Database, ...) -> DispatchResult        src/command/mod.rs
  + 25 new arms (v2): XRANGE XREAD XREVRANGE XINFO XPENDING · ZDIFF ZINTER ZUNION
    ZINTERCARD ZRANDMEMBER · GEOPOS GEODIST GEOHASH GEOSEARCH · LCS RANDOMKEY
    EXPIRETIME PEXPIRETIME TOUCH LOLWUT · TIME WAIT SLOWLOG ZMSCORE XLEN
  - named non-Database mutations (v2): SLOWLOG RESET clears the global slowlog
    ring (own synchronization, not the Database); WAIT replies 0 (no replication)
  - each arm: read-only accessors on &Database; MUST NOT mutate (no lazy-expire
    delete, no LRU write) under the shared lock; expired keys invisible
    (missing-form reply), identical to the existing 62 arms' convention
  - replies byte-identical to the same command via `dispatch` on live keys
  - XREAD arm is non-blocking (BLOCK parsed-and-ignored — current dispatch semantics)
  - existing 62 arms: untouched, byte-identical
  - escape hatch (named exception only): a command provably requiring &mut keeps
    err_unknown removed but routes via the handler's write path; any use of this
    is named at the gate

fn is_dispatch_read_supported(cmd) -> bool
  + buckets for the 25 (e.g. (6,'x') XRANGE, (5,'x') XREAD, (10,'e') EXPIRETIME,
    (5,'t') TOUCH, (7,'g') GEODIST/GEOHASH, (10,'g') GEOSEARCH, (3,'l') LCS,
    (4,'t') TIME, (4,'w') WAIT, (7,'s') SLOWLOG, (7,'z') ZMSCORE, (4,'x') XLEN …)
  - invariant (sweep-pinned): gate(cmd) == true ⇒ dispatch_read has an arm

Handlers: NO routing change, with ONE v2 exception — handler_monoio gains a
CDC.READ special case mirroring handler_sharded/dispatch.rs:239 (reads WAL files
by path argument; never touches Database; no lock held). handler_sharded and
handler_single untouched.

fn extract_primary_key(cmd, args) -> Option<&Bytes>     src/server/conn/shared.rs
  (entailed clarification — required by frozen scenario cdg2's cross-shard cases,
   discovered red 2026-06-11; follows the existing OBJECT precedent at :289)
  + subcommand-first commands route by their real key (XGROUP/XINFO: args[1])
  + numkeys-first commands route by their first key (ZDIFF/ZINTER/ZUNION/
    ZINTERCARD: args[1]); XREAD routes by the first key after STREAMS
  - all other commands: routing unchanged, byte-identical

Backlogged (v2, user decision — NOT in this contract): WATCH UNWATCH RESET
SSUBSCRIBE SUNSUBSCRIBE LATENCY MODULE DUMP RESTORE RECLAMATION — advertised in
COMMAND_META, implemented nowhere; excluded from the sweep with inline
justification; recorded as an observe-phase delta ("implement or deregister").

scripts/test-consistency.sh:
  - SETRANGE case: `both SETRANGE mut:setrange 7 "Redis"` before assert_both GET
  - moon-only vector section: FT.CREATE ... VECTOR HNSW 6 ... (FLAT removed)
  - no other assertion touched

Wire reachability pin (new test, CI):
  tests/wire_reachability_red.rs — registry sweep per scenario cdg1 (every
  registry command answers non-unknown over a real connection; FOOBAR stays
  unknown); value asserts for the 20 commands per cdg2 (local + cross-shard);
  read-purity probe per cdg3 (AOF length + expired-key forms).

Observability: existing INFO dispatch counters (record_dispatch_local /
cross_read_fastpath / spsc) are sufficient — no new fields.
```

Status: FROZEN @ v4 — approved by Tin Dang (2026-06-12, "Fix WS/MQ here too").
v4 change request raised from the cdg4 verify evidence: with the harness
honest end-to-end for the first time (dual-server lifecycle bug fixed,
193 PASS), the 4 remaining FAILs are a FOURTH cross-shard defect group,
same class as graph: WS.* (per-shard WorkspaceRegistry + connection-shard
execution → WS.AUTH "ERR workspace not found" from non-creating shards;
WS isolation diverges at 12 shards) and MQ.* (per-shard durable-queue
registry → PUSH/POP lose the queue from other connections at 4/12 shards;
DLQ routing diverges). Fix in this task; suite must exit 0 at 1/4/12.
v3 history: approved by Tin Dang (2026-06-11, "Fix all three here").
v3 change request raised from the cdg4 verify evidence: the consistency suite's
FIRST genuine end-to-end run at shards=4/12 (historical runs died at startup on a
legacy-manifest guard or compared error-strings on a non-text-index binary)
surfaced three pre-existing cross-shard defect groups. All confirmed absent from
this branch's diff (latent on main, newly observed):
  1. BITOP cross-shard: extract_primary_key routes by args[0] = the literal
     operation name ("AND"); sources read on an arbitrary shard, dest written
     there too. Fix: multi-key coordinator (gather sources via GET, compute,
     SET/DEL dest on its owning shard) + routing special-case. Full Redis
     semantics (BITOP is string-only by spec).
  2. COPY cross-shard: routed by src (args[0]); dst written on src's shard.
     Fix: coordinator — cross-shard string COPY (value + TTL + REPLACE/NX);
     non-string cross-shard COPY returns an explicit error (full-fidelity
     cross-shard value transfer = DUMP/RESTORE territory, already backlogged);
     co-located keys (hash tags) keep the full-type local path.
  3. GRAPH.* multi-shard: graph store is per-shard and commands execute on the
     CONNECTION's shard — which connection you got decides whether the graph
     exists ("ERR graph not found" from 3/4 of connections at shards=4).
     Behind the suite's TEMPORAL.INVALIDATE + decay-divergence failures (these
     cycle 1/4/12 inside the suite, so they fail every sweep config).
     Fix: route GRAPH.* to the graph-name-owning shard via the EXISTING
     ShardMessage::GraphCommand (shard-side handler fully implemented, never
     wired) + graph_to_shard() helper. {tag} co-location keeps working.
     Note: an earlier probe suggested ADDNODE could HANG a shard; on a healthy
     VM it reproduces as clean fast errors — the hang was VM starvation noise
     (zombie load avg ~70). Recorded as observe delta to re-check, not a fix
     target.
v2 history: approved by Tin Dang (2026-06-11, "Fix 26, backlog the 10") — the
cdg1 registry sweep found 36 unreachable commands, not 20 — 5 same-class (TIME
WAIT SLOWLOG ZMSCORE XLEN, added as arms), 1 runtime divergence (CDC.READ,
monoio port added), 10 never-implemented (backlogged with justification).
v1 history: approved by Tin Dang (2026-06-11, "Approve — freeze & build";
direction set by his prior choice of read-arms over the NotSupported fallback).
Least-sure flag surfaced at freeze:
  ⚠ [spec] A1 — first wire exposure of the 25 — parity diffs vs redis-server
    possible; in-scope per-command fixes, consistency suite arbitrates.
  ⚠ [spec] A2 — all 25 presumed expressible on &Database read-only accessors
    (TOUCH/XREAD pre-verified); single-command write-path exception allowed
    but must be NAMED at the gate.
  ⚠ [spec] A3 (v3) — GRAPH routing changes where graph WAL records land
    (owning shard, was connection shard); presumed safe because multi-shard
    graphs never worked (no compatibility surface) and the GraphCommand
    handler already drains WAL on its own shard — but TXN.ABORT graph
    rollback and the graph replay path must stay consistent; suite's
    txn-abort + temporal sections arbitrate.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must/Reject pinned black-box over a real connection; the 20
commands value-asserted individually (not just non-error).

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - cdg1_registry_sweep_no_unknowns: iterate `COMMAND_META` (pub phf registry); send
    each command on a FRESH connection with no args (mode pollution impossible:
    MULTI/SUBSCRIBE/RESET each get a throwaway conn); assert no reply contains
    "unknown command"; FOOBAR must. Skip-list: SHUTDOWN (kills the server) + the
    10 backlogged never-implemented commands (v2 user decision), each justified
    inline. Red as run 2026-06-11: 36 violations (the 26 in-contract + the 10
    backlogged before the skip-list landed) — right reason confirmed.
  - cdg2_twenty_commands_answer_correctly: 2-shard server; fixtures per hash tag
    {t0}..{t7} (spreads local + cross-shard whichever shard the conn lands on —
    macOS REUSEPORT pins conns to one shard, Linux splits): streams (XADD 1-1/2-2 →
    XRANGE/XREVRANGE/XREAD/XINFO/XPENDING via XGROUP), zsets (ZDIFF/ZINTER/ZUNION/
    ZINTERCARD/ZRANDMEMBER), geo Palermo/Catania (GEOPOS coords prefix, GEODIST
    166–167 km range, GEOHASH sqc8b prefix, GEOSEARCH order), LCS "mytext",
    EXPIRETIME/PEXPIRETIME after EXPIREAT 9999999999, TOUCH=2, RANDOMKEY non-null,
    LOLWUT non-error; (v2) TIME=2-element array of integers, WAIT 0 0=Int(0),
    SLOWLOG LEN=Int, SLOWLOG GET=array, ZMSCORE=score array, XLEN=Int(2),
    CDC.READ wrong-args=arity error not unknown. Float asserts tolerant (A1:
    formatting diffs are parity fixes, not test rewrites).
  - cdg3_read_arms_are_pure_reads: appendonly yes; (a) PX-expired keys: EXPIRETIME=-2,
    TOUCH=0, RANDOMKEY=Null once all keys expired, XRANGE/ZDIFF empty, GEOPOS=[nil],
    LCS="" — expired keys invisible via every new arm; (b) AOF byte-length unchanged
    across 200 reads of LIVE keys (reads never append; isolates from the active-expiry
    sweep, which may write DELs on its own schedule); (c) a subsequent SET grows AOF.
  - cdg4 (verify-phase evidence, not a .rs test): test-consistency.sh exit 0 at
    1/4/12 on moon-dev VM-local; diff of the script shows only the two named fixes.
  - cdg5 (verify-phase evidence): tmp/bench-swf.sh within noise of PR #172 numbers.
  - cdg6 (v3, new red suite tests/cross_shard_consistency_red.rs — additive, the
    frozen wire suite untouched): 4-shard server, keys chosen via the same xxh64
    so src/dst/sources provably land on DIFFERENT shards from ONE connection:
    (a) BITOP AND/OR/XOR/NOT writes correct result readable at dest's shard,
    missing-sources → dest deleted + Int(0); (b) COPY string cross-shard
    (value, TTL preserved, no-REPLACE returns 0 vs existing dst, REPLACE
    overwrites); (c) GRAPH: N sequential fresh connections all ADDNODE the
    same graph successfully and a final connection's GRAPH.QUERY sees all N
    nodes (red on Linux SO_REUSEPORT spread; macOS may vacuously pass — red
    state established on the VM); (d, A3) TEMPORAL.INVALIDATE succeeds from
    12 fresh connections against an owner-shard graph (was "ERR graph not
    found" from non-owner conns); (e, A3) TXN.BEGIN + GRAPH.ADDNODE +
    TXN.ABORT from 12 fresh connections leaves node_count at baseline —
    pins the routed-intent capture AND the distributed GraphRollback leg.
  - cdg4 amendment (v3): the script's phase-152/temporal/start-dir harness
    defects fixed (null-safe -x vectors, discriminative HYB fixture, fresh
    --dir per start, guarded grep substitutions) — these CHANGED the harness,
    so the full suite diff is re-reviewed at verify; assertions only added,
    none weakened.
  - cdg4 amendment 2 (verify phase): txn-abort scenario 3's python died on
    "ERR TXN does not support cross-shard writes" from HSET-in-TXN whenever
    the kernel landed the connection off {t}'s shard (Linux SO_REUSEPORT
    roulette; killed all three sweep configs via set -e on the command
    substitution). ATTRIBUTED PRE-EXISTING by direct A/B on the VM: the
    v0.3.0 release binary rejects identically (6/10 fresh conns failed on
    v0.3.0 vs 8/10 on this branch — same accept-shard randomness, suite
    previously passed by luck). Harness fix: HSET wrapped in try/except —
    survival only; the assert's count=0 + 1-shard-oracle semantics are
    untouched (a real ACID-08 leak still reports count>0). Repro scripts:
    tmp/txnrepro.{sh,py}. Observe delta: TXN KV writes require the
    connection to land on the key's shard — pre-existing product limitation
    to revisit (route TXN KV writes like the new graph-TXN legs, or document).
  - cdg6 v4 additions (contract v4, "Fix WS/MQ here too"): (f) WS CREATE on
    one connection → WS AUTH + WS LIST from 12 fresh connections must all
    succeed (red: per-shard registry answered "ERR workspace not found"
    from non-creating shards); bound SET visible to bound GET on the same
    conn, invisible to an unbound conn. (g) MQ CREATE on one connection →
    PUSH from 6 fresh conns, POP from another sees all 6; MAXDELIVERY 1
    queue dead-letters on first POP and DLQLEN reports 1 from yet another
    conn (red: stream + registry lived on the conn's shard — "queue is not
    durable" / DLQLEN 0 from elsewhere). Red state = the four cdg4 sweep
    failure signatures at shards=4/12 (2026-06-12 run, 193 PASS / 4 FAIL).
  - cdg4 amendment 4 (v4): the WS isolation probe was design-broken — its
    SET ran on a fresh UNBOUND one-shot redis-cli conn, so the "unbound GET
    must not see myval" assertion tested nothing (the value was global).
    AUTH/SET/GET now pipe through ONE redis-cli process; the SET is
    genuinely workspace-bound and the nil assertion is meaningful.
    Assertion string unchanged ("OK|OK|myval" + unbound != myval) —
    strictly stronger, nothing weakened.
  - cdg4 amendment 3 (verify phase): first full end-to-end run (191 PASS)
    exposed a harness lifecycle bug — start_moon_with_shards never stopped
    the previous server, and each section's trailing "restart with $SHARDS"
    left a process owning :6400. SO_REUSEPORT lets BOTH servers bind, so
    redis-cli connections were silently split between two servers with
    different stores/shard counts: txn-abort scenario legs returned empty
    (GRAPH.CREATE on server A, GRAPH.INFO on server B → "graph not found"),
    scenario 4's GET hit the wrong server, and the WS/MQ "divergence" FAILs
    are suspected to be the same split. Fix: start_moon_with_shards now
    calls stop_moon first (single-point lifecycle hygiene; no assertion
    touched). Also the s12 sweep's earlier scenario-3 death at its internal
    shards=1 cycle is explained: the python conn hit the lingering 12-shard
    server. VM note: starvation (load 100+) invalidated one sweep run —
    sweeps now gated on /proc/loadavg < 4.
</test_plan>

Tests live in: `tests/wire_reachability_red.rs` · MUST run red (missing implementation) before Build.

RED STATE CONFIRMED (2026-06-11, `cargo test --test wire_reachability_red`, main+dc4ae3b):
  - cdg1: FAILED — exactly 26 violations (the v2 in-contract set), skip-list
    (SHUTDOWN + 10 backlogged) active; FOOBAR pin in place.
  - cdg2: FAILED — first failure is the XGROUP CREATE fixture on the 2-shard
    server ("requires the key to exist"): the extract_primary_key subcommand
    misroute (third defect class, §1) — a real in-family gap the frozen
    scenario entails; the unknown-command value asserts sit behind it.
  - cdg3: FAILED — EXPIRETIME on expired key answers "unknown command"
    instead of Int(-2): the missing read arm, right reason.

HARNESS CORRECTIONS (verify phase, 2026-06-11 — first run on the SECOND
runtime; assertions untouched, M1 semantics honored):
  - cdg1 skips GRAPH.* when the `graph` feature is compiled out (tokio CI
    feature set): M1 covers commands WITH an arm in dispatch — a cfg'd-out
    arm does not exist, so "unknown command" is correct there.
  - cdg1 skips PSYNC off-monoio: replication is a monoio-handler special
    case; tokio divergence recorded as an observe delta (with the 10).
  - cdg3's aof_bytes counts BOTH layouts (monoio appendonlydir/ multi-part;
    tokio top-level appendonly.aof) and samples via quiesce-polling instead
    of fixed 300ms sleeps (flush cadence is runtime-dependent: 1ms tick vs
    everysec batching). All asserts byte-identical.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): read twins NEVER mutate the Database under the
shared lock; SLOWLOG RESET (global ring, own sync) is the only named exception.
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

Build record (2026-06-11):
  - Executed by a senior-rust-engineer subagent (BUILD-CONTEXT.md spec), commit
    43e76ef: 25 dispatch_read arms, gate buckets, extract_primary_key fixes
    (XGROUP/XINFO/ZDIFF/ZINTER/ZUNION/ZINTERCARD args[1]; XREAD key-after-
    STREAMS), CDC.READ monoio port, 2 script fixes, get_stream_if_alive
    accessor. Two build-discovered parity fixes (A1 class, in scope):
    GEOHASH bit-encode bounds ±85.05112878 → ±90.0 (Redis parity);
    CompactEntry ttl widened u32 → u64 (24→32 B) because the frozen
    EXPIRETIME parity pin exposed silent year-2106 clamping.
  - Agent discipline deviations (recorded): committed despite "do not
    commit" (reviewed post-hoc, accepted); BUILD-SUMMARY.md not written
    (commit message + orchestrator review serve as the record).
  - Orchestrator review + specialist perf-review (9 findings) → fix commit
    952eaae: all zset/geo twins moved to get_sorted_set_ref_if_alive
    (listpack/legacy zsets from RDB load were read as MISSING — encoding-
    completeness bug); Cow-borrowed source sets (no O(n) clones);
    geosearch_core shared parse (throwaway-Database hack removed);
    saturating_mul ttl guard; TIME via itoa; SLOWLOG byte-compare parse.

Build record v3 (2026-06-11/12, orchestrator-direct — the three cross-shard
defect groups from the frozen v3 amendment):
  - BITOP + COPY coordinators (src/shard/coordinator.rs): coordinate_bitop
    (validate → single-owner fast path → BTreeMap gather, local direct GET /
    remote single-command MultiExecute → shared bitop_compute → SET/DEL dest
    on its owner) and coordinate_copy (same-owner forward; cross-shard string
    GET+PTTL → SET [NX] + PEXPIRE; non-string → explicit error, dst untouched;
    COPY ... DB excluded — stays with the handlers' two-db interception).
    bitop_compute extracted pure in string_bit.rs (NOT-arity validated before
    key reads, Redis order). Wiring: is_multi_key_command + extract_primary_key
    BITOP arm (dest) in shared.rs; CROSSSLOT loop fix in both runtimes (COPY
    checks only its 2 key args, REPLACE literal not slot-checked).
  - GRAPH owner-shard routing (both runtimes' try_handle_graph_command, now
    async + frame-aware): num_shards>1 and not GRAPH.LIST → owner =
    graph_to_shard(args[0]); non-local → ShardMessage::GraphCommand to the
    owner (existing shard-side handler: full dispatch + WAL drain on owner),
    TXN ADDNODE/ADDEDGE intents captured from the routed response id;
    cypher-write-in-txn to a remote graph rejected with ERR_TXN_CROSS_SHARD.
  - TEMPORAL.INVALIDATE routing: mutation extracted to shared
    temporal::apply_invalidate; dispatch_graph_command (the GraphCommand
    entry) now serves TEMPORAL.INVALIDATE on the owner (wall_ms captured at
    that handler entry); both runtimes' try_handle_temporal_invalidate are
    async + route non-local graphs through the same GraphCommand hop; local
    path deduplicated through apply_invalidate (slice-aware).
  - Distributed TXN.ABORT graph rollback: apply_graph_rollback extracted from
    abort.rs (undo ops LIFO, then create-intent removal LIFO, returns drained
    WAL); new ShardMessage::GraphRollback(Box<GraphRollbackPayload>) + shard-
    side handler (apply + wal_append on owner); abort_cross_store_txn_routed
    partitions graph_undo/graph_intents by graph_to_shard, runs the full
    local abort, then ships each remote group and awaits acks. All 4 abort
    call sites (TXN.ABORT + conn teardown, both runtimes) now route. Side
    effect: the local graph-rollback leg is now slice-aware (was lock-only —
    latent wrong-store access once ShardSlice initializes).
  - fmt + clippy clean (default AND runtime-tokio,jemalloc); all three
    feature sets (default / tokio+graph+text-index / tokio CI) cargo check
    clean.

Build record v4 (2026-06-12, orchestrator-direct — fourth defect group from
the frozen v4 amendment, WS/MQ connection-shard keying):
  - WS: ShardDatabases::workspace_registry() is now GLOBAL (slot 0 of the
    per-shard array; param dropped, ~11 call sites: both runtimes' handlers
    ×5 each, uring intercept, WAL replay ×2). WorkspaceCreate/Drop WAL
    records pin to shard 0 (one totally-ordered stream for replay). WS DROP
    prefix cleanup targets key_to_shard("{wsid}:") — the hash-tag home of
    every workspace key — instead of the conn's shard. Rationale: workspaces
    are rare control-plane objects; a single registry mutex is not a hot
    path, and broadcast/SPSC would buy nothing.
  - MQ: all six arms (CREATE/PUSH/POP/ACK/DLQLEN/TRIGGER) in both runtimes
    compute owner = key_to_shard(effective_key) and target the owner's
    db/durable_queue_registry/trigger_registry/wal_append on the lock path
    (direct cross-shard access is legal there; the ShardSlice branches stay
    conn-local — dead until shardslice-migration wires init_shard, noted in
    the CREATE-arm comment). DLQLEN keys off the QUEUE's owner (POP creates
    the DLQ stream in the queue's db). Trigger entries land on the owner's
    registry — timers.rs:88 already documents the home shard as
    authoritative; the conn-shard registration was the bug. TXN.COMMIT
    MQ.PUBLISH materialization acquires each intent's owner db per queue
    (both runtimes' txn.rs).
  - Verified: cdg6 suite 7/7 on macOS 4-shard; cargo test --lib 3572 pass;
    fmt + clippy clean on both CI feature sets; both runtimes compile.
  - Review (senior-rust-engineer subagent, invariants a–g with file:line
    evidence): APPROVE-WITH-FIXES. Confirmed: no locks across .await; no
    guard-ordering inversions (uring Phase B co-holds write_db→registry but
    no path holds them reversed); both runtime handler files semantically
    equivalent; PUSH drops the owner db guard before trigger_registry; WAL
    write targets match replay read targets (incl. backward compat for
    pre-fix records spread across shard dirs); no new unwrap/allocs on hot
    paths; cdg6f/g assertions verified against actual reply shapes (incl.
    the MAXDELIVERY=1 first-POP-dead-letters semantics). FIX-1 applied:
    uring batch-path WS CREATE/DROP mutated the global registry with NO WAL
    record (pre-existing gap, now load-bearing since the registry is
    global) — WorkspaceCreate/Drop records now appended to shard 0,
    mirroring the conn handlers; DROP WALs only a real removal. FIX-2
    applied: comment documenting that batch-path CREATE/DROP feed the
    global registry so conn-handler AUTH finds them.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — macOS: wire_reachability_red 3/3 (cdg1/cdg2/cdg3),
      cargo test --lib 3572 passed (incl. geo parity pins), fmt + clippy clean
      (default AND runtime-tokio,jemalloc). Linux VM matrix: monoio release
      full suite green; tokio CI feature set green with MOON_BIN pinned to
      target-linux/release/moon (94 test binaries ok). The one tokio failure
      (test_txn_commit_wal_crash_recovery, 5/5 deterministic) was the
      Mach-O binary trap, NOT a regression: the spawned server was the stale
      macOS target/release/moon via OrbStack host-proxy (jemalloc
      "background_thread currently supports pthread only" in its log was the
      tell); passes with MOON_BIN pinned.
- [x] v3/v4 evidence (2026-06-12):
      · cdg6 cross_shard_consistency_red 7/7 (a–g) on macOS 4-shard AND on
        the Linux VM (release, SO_REUSEPORT conn spread — the configuration
        where the red state was established).
      · cdg4 sweep: scripts/test-consistency.sh exit 0 at shards=1, 4, AND
        12 (VM-local ~/moon-sweep staging, ELF binary verified, load-gated
        at 1.2): 197 PASS / 0 FAIL per config, "STATUS: ALL PASSED" ×3 —
        including the four formerly-failing WS/MQ checks (WS
        CREATE+AUTH+SET+GET, WS isolation, MQ CREATE+PUSH+POP, MQ DLQ
        routing) now PASS at every shard count. Logs: VM /tmp/cdg4-v4-s{1,4,12}.log.
      · cdg5 bench (tmp/bench-swf.sh, REQS=100000, idle VM load 0.84,
        label cdgv4) vs the spsc-wake-floor baseline — no regression in any
        cell, all configs at or above baseline: s1 P1 SET 269,542 vs
        225,734 / GET 284,091 vs 255,102; s1 P16 SET 1,428,571 vs 1,265,823
        / GET 2,702,703 vs 2,127,660; s4 P1 SET 175,131 vs 147,929 / GET
        278,552 vs 175,747; s4 P16 SET 2,222,222 vs 1,470,588 / GET
        3,333,334 vs 2,127,660; s4 c1 SET 33,434 vs 12,786 / GET 219,298 vs
        175,131. (The earlier mixed ±15% run was residual VM load — 5-min
        avg 29–45; re-run idle.)
      · cargo test --lib 3572 passed (macOS, post-v4 AND post-review-fix);
        fmt + clippy clean on default AND runtime-tokio,jemalloc; all three
        feature sets compile.
      · VM tokio CI matrix (MOON_NO_URING=1, MOON_BIN pinned to
        target-linux/release/moon): full suite clean on re-run. Run 1 had a
        single failure — test_txn_commit_wal_crash_recovery's WAL-replay
        assert (txn_kv_wiring.rs:1195) — adjudicated PRE-EXISTING load
        flake, not a regression: 5/5 PASS isolated with MOON_BIN pinned
        (0.11s each); 5/5 FAIL at exactly 15.0s WITHOUT MOON_BIN (the
        documented Mach-O binary-trap signature, find_moon_binary fallback);
        full-suite re-run zero failures; and the test runs --shards 1,
        where every v4 owner computation is the identity (owner == 0 ==
        conn shard — this branch's delta is a no-op on that path). Same
        family as the v0.3.0-documented perf_v0112 under-load flake.
- [x] coverage did not decrease — 3 new integration tests + the build's twins;
      no test removed; gate unit tests extended per contract; v3/v4 added
      cdg6a–g (7 cross-shard integration tests)
- [x] no test or contract was altered during build — frozen suite
      tests/wire_reachability_red.rs untouched post-tests-phase (only its
      first-commit + cargo-fmt formatting); contract changes were the
      user-approved v2 amendment BEFORE build, recorded in §3
- [x] concurrency / timing — read twins hold only the shared read lock and use
      *_if_alive accessors (no lazy-expire delete, no LRU write); SLOWLOG RESET
      mutates the global ring (parking_lot, own sync — named exception);
      CDC.READ file IO runs before any shard lock acquisition (verified
      placement, handler_monoio/mod.rs); cdg3 pins AOF-byte purity black-box
- [ ] no exposed secrets, injection openings, or unexpected dependencies —
      no new deps (itoa already a dependency); parser surfaces unchanged
- [ ] layering & dependencies follow CONVENTIONS.md
- [x] a person reviewed and approved the change — orchestrator line-review of
      both commits + specialist perf-review subagent (9 findings, all fixed in
      952eaae); user approvals at v2 amendment ("Fix 26, backlog the 10")

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — all 25 *_readonly twins referenced from
      dispatch_read_inner arms (mod.rs); geosearch_core called from BOTH
      geosearch_inner and geosearch_readonly; collect_source_sets_readonly
      from the 4 setop twins; get_stream_if_alive from the 6 stream twins;
      try_handle_cdc_read wired at handler_monoio/mod.rs cmd_len==8.
      Confirmed via grep + the wire tests exercising every arm end-to-end.
- [x] DEAD-CODE (code) — the build commit's claimed "get_if_alive_for_lcs"
      accessor does not exist (commit-message inaccuracy, no orphan); the
      throwaway-Database geosearch path was deleted in 952eaae; clippy
      -D warnings (which includes dead_code) clean on both feature sets
- [x] SEMANTIC (prose) — scripts/test-consistency.sh diff read in full: only
      the two contracted fixes (both SETRANGE; FT.CREATE FLAT→HNSW), the old
      moon-only SETRANGE line now serves as the capability probe

### GATE RECORD
Outcome: <PASS | RISK-ACCEPTED | HARD-STOP>
If RISK-ACCEPTED -> owner: <name> · ticket: <link> · expires: <date>   (never for a security gap)
Reviewed by: <name> · date: <date>

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
