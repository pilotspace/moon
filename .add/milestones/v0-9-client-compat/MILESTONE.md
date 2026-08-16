# MILESTONE: Client & SDK Compatibility

goal: a stock Redis client, driver, and monitoring agent works against Moon without special-casing — RESP3 reply types, feature negotiation, pub/sub push frames, cluster bootstrap, and INFO telemetry all match Redis 7.4 semantics
rationale: capability-gap. A client/SDK deep review of v0.8.5 (2026-08-08) found ~22 compatibility defects against real Redis 8.6.1. Two were P0 and shipped separately as the v0.8.6 hotfix (PR #457, ACL bypass + silent CLIENT TRACKING failure on the inline GET path). The remaining surface is what stops an unmodified redis-py / go-redis / ioredis / monitoring agent from treating Moon as a drop-in — the class of defect that is invisible to Moon's own tests because both sides of every assertion are Moon.
stage: production · status: active · created: 2026-08-09

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:  reply-shape and reply-type parity with Redis 7.4/8.x on the covered command
     set (RESP2 and RESP3); HELLO/COMMAND/ROLE/RESET feature negotiation; pub/sub
     push frames and subscriber-mode rules; cluster client bootstrap (CLUSTER
     SHARDS, READONLY/READWRITE, honest `cluster_state`); INFO fields the standard
     monitoring stack reads; keyspace notifications; protocol-error replies; the
     first-party SDK wire forms; and the CI coverage that keeps all of it from
     regressing.

Out: new data structures or commands not required for client compatibility;
     RediSearch/Cypher surface beyond what a client library negotiates;
     performance work (this milestone must be perf-neutral, not perf-positive);
     MIGRATE-based live resharding if it proves to need a new cluster subsystem —
     in that case the exit is an explicit, documented non-goal plus a supported
     resharding path, not a stub.

## Shared decisions & glossary deltas   (living — every task must honor these)
- **Real Redis is the oracle, never Moon's own expectation.** Every parity claim
  is proven by a byte-level diff against a running `redis-server`, not by an
  assertion written from reading Moon's source. The review that opened this
  milestone found defects precisely where Moon tested Moon.
- **RESP2 and RESP3 are both first-class.** A fix that changes a RESP3 shape must
  leave the RESP2 shape untouched, and vice versa. Every reply-shape task asserts
  both protocols.
- **A command must not change shape by context.** The same command answers the
  same shape standalone, inside `MULTI/EXEC`, and inside a pipeline. Conversions
  belong on one choke point (`apply_resp3_conversion`), not per-call-site.
- **Registered ⇒ reachable.** A command visible to `COMMAND`/ACL must be
  dispatchable. The registry and the three dispatch paths cannot diverge silently
  (see [[gotcha_moon_three_dispatch_paths]]) — this divergence is what made ACL and
  COMMAND lie about the surface.
- **Every dispatch path, not the default one.** `handler_monoio`, `handler_sharded`,
  and `handler_single` each need the behavior. A check that exists on two of three
  is the exact shape of the v0.8.6 P0.
- **Perf-neutral, measured.** Any hot-path touch is A/B'd on the Linux VM with one
  server alive at a time, and fast-path retention is proven by
  `moon_dispatch_path_total{path="local_inline"}`, not inferred from latency.

## Shared / risky contracts (freeze these first)
- `scripts/test-client-compat.sh` — the raw-RESP diff harness and its command
  manifest. Every other task cites it as its verifier, so its output format and
  invocation must be frozen before the parity tasks start
  -> owning task `client-compat-harness`
- `apply_resp3_conversion` — the single RESP2→RESP3 conversion choke point. Both
  `resp3-type-fidelity` and `pubsub-resp3-push` widen it; they must agree on where
  it is applied (dispatch exit, including EXEC/pipeline inner replies)
  -> owning task `resp3-type-fidelity`
- command registry ↔ dispatch reconciliation (`metadata.rs` vs the three dispatch
  tables) — `sdk-wire-form-fixes` registers MQ/WS into the same table
  -> owning task `client-identity-introspection`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] client-compat-harness        depends-on: none  — raw-RESP diff harness (Moon vs real redis-server) asserting reply TYPE and shape per command in RESP2 and RESP3, plus a CI job that FAILS (never skips) when no server is reachable. Every other task's verifier.
- [x] monoio-ci-coverage           depends-on: none  — a CI test job that builds monoio. All current CI test jobs build tokio, which is the structural reason a security bypass on the default runtime shipped in two releases. Also re-enables `cargo clippy --all-targets` (currently omitted, so test-code lints never gate).
- [x] client-identity-introspection depends-on: client-compat-harness — HELLO `version` → REDIS_COMPAT_VERSION (redis-py refuses client-side caching against the Moon version string); COMMAND COUNT/INFO/GETKEYS/DOCS served from `metadata.rs`; ROLE, RESET, CLIENT SETINFO with lib-name/lib-ver surfaced in CLIENT LIST/INFO; reconcile the registered-but-unreachable set (DUMP, RESTORE, LATENCY, MODULE, RECLAMATION) — implement or deregister — plus a test that the registry and dispatch cannot diverge again. Also closes the INVERSE divergence: MONITOR, MQ and WS are reachable-or-wanted but absent from `metadata.rs`, so they are invisible to COMMAND and uncategorised for ACL — the reconciliation test must sweep both directions, and MONITOR itself (unimplemented, and not on the known-gap allowlist) is implemented or explicitly declared a non-goal here.
- [x] watch-cas-transactions      depends-on: client-compat-harness — WATCH/UNWATCH answer "unknown command" on both production dispatch paths (`handler_monoio`, `handler_sharded`); the implementation exists only in `handler_single.rs`, the embedded-mode handler, while `metadata.rs` registers both so COMMAND and ACL claim they are available. Optimistic-locking CAS is the one missing primitive with no client-side workaround. Includes the key-touch invalidation path (EXEC must abort when a watched key changed, cross-shard included) and removal of WATCH/UNWATCH from the `BACKLOGGED_UNIMPLEMENTED` allowlist in `tests/wire_reachability_red.rs`.
- [x] protocol-error-lifetime     depends-on: client-compat-harness — a malformed frame must produce `-ERR Protocol error: <detail>` and then close, after the already-valid pipelined prefix has been answered. Today Moon sends NOTHING for `PING` + a bad multibulk header (the valid PONG dies with the connection), and a bulk header followed by inline garbage produces no reply AND no close, so the client blocks to its own socket timeout. This is a connection-lifetime defect, not the reply-formatting item originally folded into `info-observability` — that scope moves here.
- [x] resp3-type-fidelity          depends-on: client-compat-harness — fix Map-vs-pairs inversions (ZRANDMEMBER/HRANDFIELD must be pair arrays); add pair-wrapping + Double for ZRANGE/ZPOPMIN/ZDIFF/ZUNION WITHSCORES; stop over-converting SISMEMBER/EXPIRE (Integer) and INCRBYFLOAT (Bulk); route CONFIG and EXEC/pipeline inner replies through `apply_resp3_conversion`; XINFO STREAM → Map with the missing fields; SPOP `<count>` → Set. Oracle sweep 2026-08-09 widened this: the whole `int_to_bool` branch is wrong (Redis answers Integer for all 7 of SISMEMBER/HEXISTS/EXPIRE/PEXPIRE/PERSIST/SETNX/MSETNX), `bulk_to_double` is wrong for INCRBYFLOAT/HINCRBYFLOAT and missing for ZMSCORE/GEOPOS, and the conversion needs ARG awareness (WITHSCORES/WITHVALUES/count decide the shape) — which the current `maybe_convert_resp3(cmd, response, proto)` signature cannot express. Transaction *semantics* moved OUT to `multi-exec-queue-semantics`.
- [x] multi-exec-queue-semantics   depends-on: client-compat-harness — two defect classes, both proven by a shards=1 vs shards=2 control (2026-08-09). (A) shard-independent: CONFIG, CLIENT and INFO are EXECUTED at queue time instead of queued — they answer their result where Redis answers `+QUEUED`, and EXEC then returns a short array; MOVE inside EXEC answers `-ERR MOVE requires handler-level dispatch`. Same root as the v0.8.6 inline-GET P0: an intercept runs before the MULTI-queue check. (B) shards>=2 only: multi-key commands inside MULTI SILENTLY return empty or nil — ZINTER `*[]`, ZINTERCARD `:0`, LMPOP `$nil` with the pop not applied — all correct at shards=1; RENAMENX/MSETNX at least fail loudly with `-CROSSSLOT`. Also owns the missing `-EXECABORT` on queue-time errors (Moon queues a bad command and surfaces its error from inside EXEC, so a poisoned transaction looks partially applied).
- [x] pubsub-resp3-push            depends-on: client-compat-harness — subscribe/unsubscribe confirmations as Push frames under RESP3; lift the subscriber-mode command restriction when `protocol_version >= 3`; add RESET and (S)SUBSCRIBE to the RESP2 subscriber allowlist; implement SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH and PUBSUB SHARD*.
- [x] cluster-client-bootstrap     depends-on: client-compat-harness — CLUSTER SHARDS; READONLY/READWRITE; `cluster_state` must report `fail` while slots are uncovered (today it answers `ok` with zero slots assigned, so clients believe an unusable topology is healthy); `# Cluster` section in INFO; MIGRATE or an explicit documented non-goal plus a supported resharding path.
- [x] info-observability           depends-on: client-compat-harness — dedupe the two `# Replication` sections; honor `INFO <section>` (today the argument is ignored); fill `# Commandstats` and add `# Errorstats`/`# Latencystats`; add the missing counters (33 of the 51 pinned monitoring fields are absent: keyspace_hits/misses, evicted_keys, expired_keys, maxmemory*, instantaneous_ops_per_sec, total_net_*, rejected_connections, uptime_in_seconds, run_id, tcp_port, redis_mode, cluster_enabled…); keyspace notifications (notify-keyspace-events + `__keyspace@`/`__keyevent@`). The protocol-error item moved OUT of this task to `protocol-error-lifetime` — the observed defect is a connection-lifetime bug, wider than the reply-formatting fix this task had scoped.
- [x] sdk-wire-form-fixes          depends-on: client-identity-introspection — moondb Rust `mq.rs` sends `MQ.PUSH`/`MQ.POP`, a wire form the server rejects (the container form is `MQ PUSH`/`MQ POP`); moondb Python version disagrees with pyproject (0.1.0 vs 0.1.1) and ships no mq/temporal/workspace modules; register MQ/WS in `metadata.rs` so ACL and COMMAND can see them.

## Exit criteria (observable; map each to the task that delivers it)
- [x] A raw-RESP diff of Moon against a real `redis-server` reports zero unexplained differences in reply type or shape across the covered command manifest, in both RESP2 and RESP3 (verify: `scripts/test-client-compat.sh --strict`) (← client-compat-harness)
- [x] An acceptance suite driven by an unmodified **redis-py** — its own connection pools, `pipeline()`, `pubsub()`, `scan_iter()`, `Lock`, `from_url`, not raw sockets — runs green against a live Moon in CI, alongside the raw-RESP differ, on a job that fails rather than skips when no server is reachable (verify: steps `redis-py acceptance suite (unmodified client, live server)` and `Compatibility diff across contexts` in CI job `client-compat` in `.github/workflows/ci.yml`) (← client-compat-harness)
  > AMENDED 2026-08-15. Originally named redis-py + go-redis + ioredis. Narrowed to redis-py plus the raw-RESP differ: the self-hosted runner has no Go or Node toolchain and cannot get one (PEP 668 blocks pip; `python3.14-venv` is absent — redis-py comes from the distro package `python3-redis`). redis-py is the most-used client, and the differ already covers wire shape for all three. Known gaps the suite found (#507, #508, and `CLIENT INFO cmd=NULL`) are pinned inside it as amplified probes — twenty distinct keys each, because both multi-shard defects fire for only ~50% of keys — so a fix breaks the run rather than leaving a stale skip.
- [x] A monoio build runs the test suite in CI, so a defect on the default runtime cannot ship unobserved (verify: CI job `check-monoio` green on a PR, and red when the v0.8.6 P0 fix is reverted) (← monoio-ci-coverage)
- [x] redis-py negotiates client-side caching against Moon without a version override, and `COMMAND DOCS`/`COMMAND GETKEYS` answer for every registered command (verify: `cargo test --test client_identity_introspection`) (← client-identity-introspection)
- [x] No command registered in `metadata.rs` answers "unknown command" on any of the three dispatch paths (verify: `cargo test --test wire_reachability_red cdg1_registry_sweep_no_unknowns`, which enumerates the registry and dispatches every entry) (← client-identity-introspection)
- [x] Every command in the manifest returns the same reply shape standalone, inside MULTI/EXEC, and inside a pipeline, under both protocols (verify: `scripts/test-client-compat.sh --contexts standalone,multi,pipeline`) (← resp3-type-fidelity)
- [x] A RESP3 subscriber receives subscribe/unsubscribe confirmations as Push frames and may issue non-pub/sub commands while subscribed, matching Redis (verify: `cargo test --test pubsub_resp3_push`) (← pubsub-resp3-push)
- [x] A stock cluster-aware client bootstraps against a Moon cluster via CLUSTER SHARDS, and reports the topology unhealthy while slots are uncovered instead of accepting it (verify: `cargo test --test cluster_client_bootstrap`) (← cluster-client-bootstrap)
- [x] `INFO` exposes every field the pinned monitoring-stack manifest reads, with no duplicate sections, and `INFO <section>` returns only that section (verify: `scripts/test-client-compat.sh --info-manifest`) (← info-observability)
- [x] Every command answers `+QUEUED` inside MULTI and its real reply from EXEC, with identical values at 1, 2 and 4 shards — no command executes at queue time and no multi-key command silently returns empty (verify: `cargo test --test multi_exec_queue_semantics`) (← multi-exec-queue-semantics)
- [x] A stock client completes a WATCH/MULTI/EXEC compare-and-set against Moon, and EXEC returns nil when a watched key changed under it — on every dispatch path, single-shard and cross-shard (verify: `cargo test --test watch_cas_transactions`) (← watch-cas-transactions)
- [x] A malformed frame is answered with `-ERR Protocol error: …` after the valid pipelined prefix has been replied, then the connection closes — no stalled connection and no discarded valid reply (verify: `cargo test --test protocol_error_lifetime`) (← protocol-error-lifetime)
- [x] The first-party Rust and Python SDKs execute their full documented surface against a live Moon with no rejected wire forms (verify: `cargo test --test sdk_wire_forms` in the root crate, plus CI steps `Rust SDK round trip (every public helper, live)` and `Python SDK version derivation` in job `sdk` of `.github/workflows/ci.yml`) (← sdk-wire-form-fixes)
  > AMENDED 2026-08-16. The original citation named `cargo test -p moondb --test sdk_wire_forms` and `pytest sdk/python/tests`; NEITHER resolves. There is no `moondb` workspace member (`sdk/rust` is a separate, non-member crate — `cargo test -p moondb` errors with "package ID specification `moondb` did not match any packages"), the wire-form sweep lives in the ROOT crate at `tests/sdk_wire_forms.rs`, and the runner has no pytest and cannot get one (PEP 668; no `python3.14-venv`). Corrected to the three checks that actually run. Verified 2026-08-16 on merged `main` @ `a3043a84`: `sdk_wire_forms` 4/4.
- [x] The milestone is perf-neutral on the hot path (verify: Linux VM A/B, one server alive at a time, alternating order, medians of 5 — no regression beyond noise at c=1 P=1 and c=8 P=16, with fast-path retention confirmed via `moon_dispatch_path_total{path="local_inline"}`) (← all tasks)

## Close — ship review   (AI fills when every task is done — the evidence behind the engine gate, read before the boxes are checked)
> Whole-milestone, cross-task review the AI fills in. It is the evidence behind the EXISTING engine
> gate (milestone-done / checking the Exit-criteria boxes) — NOT a new approval. Tool-agnostic.

### Ship by domain   (what changed, per bounded context)
- protocol      : RESP3 type fidelity across the conversion table (`int_to_bool` was wrong for all 7
                  of SISMEMBER/HEXISTS/EXPIRE/PEXPIRE/PERSIST/SETNX/MSETNX; `bulk_to_double` wrong for
                  INCRBYFLOAT/HINCRBYFLOAT, missing for ZMSCORE/GEOPOS), arg-aware conversion
                  (WITHSCORES/count decide the shape), Push frames for pub/sub confirmations, and the
                  per-connection protocol version honoured in batch contexts.
- connection    : WATCH/UNWATCH/EXEC CAS on ALL THREE dispatch paths (it existed only in
                  `handler_single`, the embedded handler, while `metadata.rs` advertised it);
                  protocol-error connection lifetime — a malformed frame is now answered
                  `-ERR Protocol error:` AFTER the valid pipelined prefix has been replied, then closed.
- registry      : COMMAND COUNT/INFO/GETKEYS/DOCS, ROLE, RESET, CLIENT SETINFO with lib-name/lib-ver;
                  five advertised-but-unreachable commands deregistered (267 -> 262); MQ/WS/MONITOR
                  registered. The registry sweep now runs with NO waiver list at all.
- pubsub        : sharded pub/sub (SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH, PUBSUB SHARD*), RESP3
                  subscriber-mode parity.
- cluster       : CLUSTER SHARDS, READONLY/READWRITE, `cluster_state:fail` while slots are uncovered
                  (it previously answered `ok` on a topology no client could use), `# Cluster` in INFO.
- observability : `INFO <section>` honoured, duplicate `# Replication` removed, Commandstats /
                  Errorstats / Latencystats, keyspace notifications, the pinned monitoring fields;
                  MONITOR command feed, redacted and zero-cost when unattached.
- transactions  : queue-time interception fixed (CONFIG/CLIENT/INFO were EXECUTED where Redis answers
                  `+QUEUED`), `-EXECABORT` on queue-time errors, multi-key commands inside MULTI at
                  shards>=2.
- SDK           : moondb Rust `MQ.PUSH`/`MQ.POP`/`FT.UPSERT` were wire forms the server rejects;
                  Python version derivation reconciled.
- CI            : `check-monoio` — the SHIPPED runtime now runs the suite (its absence is the
                  structural reason a security bypass shipped on the default runtime in two releases);
                  the raw-RESP differ vs a real redis-server; a redis-py acceptance suite driven by
                  the unmodified client; an INFO-manifest coverage gate with self-invalidating waivers.

### Cross-task evidence   (one row per task)
- client-compat-harness      : gate=PASS · differ + a CI job that FAILS (never skips) when no server
                               is reachable; 44 manifest entries, 7 baseline waivers each owned by a
                               named wave-2 task · residue=none
- monoio-ci-coverage         : gate=PASS · approved on a NEGATIVE CONTROL, not a config guard: the
                               same deliberate monoio-only defect passes the tokio suite 6/6 and
                               fails the new job 3/6 · residue=`check-monoio` RUNS but does not BLOCK
                               until added to branch protection (flagged at freeze, owned by approver)
- client-identity-introspection: gate=PASS · #471 · residue=none
- resp3-type-fidelity        : gate=PASS · residue=a unit test was DELETED during build; disclosed and
                               explicitly asked about BEFORE the gate was recorded, not after.
                               Follow-ups filed (#459) rather than folded in
- pubsub-resp3-push          : gate=PASS · #483 · residue=none
- cluster-client-bootstrap   : gate=PASS · every §3 clause tested against redis-server 8.6.1; the
                               refute-read's one P0 fixed and re-verified BEFORE the gate ·
                               residue=two contracted divergences (MYSHARDID across failover,
                               lone-node bootstrap) + Windows failure detection (#494, tests gated
                               with evidence rather than silently un-run) + a port flake (#505,
                               pre-existing, proven by a 12-run A/B: branch 4/12 vs base 3/12)
- info-observability         : gate=PASS · PR #481 (`73c6597d`); 10/10 keyspace_notifications, 13/13
                               info_observability under BOTH runtimes; lib 4605 / 3768 green; full
                               matrix run `31725319305` 9/9 · residue=four INFO fields waived with
                               recorded reasons; `used_memory_lua` WITHDRAWN rather than shipped
                               wrong (#506 — monoio reports it two orders of magnitude off)
- multi-exec-queue-semantics : gate=**RISK-ACCEPTED** · owner Tin Dang · ticket #482 · expires
                               2026-10-31 · Must #7 unmet: `BLPOP` on a missing key inside MULTI
                               replies Null Bulk (`$-1`) where Redis replies Null Array (`*-1`), and
                               `me7` is `#[ignore]`d rather than passing. 7 of 8 Musts and all 5
                               Rejects met. Accepted because `Frame` has NO null-array variant at
                               all — plain `BLPOP key 1` mistypes its reply outside MULTI too, so the
                               fix threads `Frame::NullArray` through every `Frame::Null` arm in
                               `serialize.rs`/`resp3.rs` and can flip currently-correct reply types.
                               RESP2-only. **This is the milestone's one unmet Must.**
- watch-cas-transactions     : gate=PASS on §6's test/bench/matrix evidence plus an explicit git file
                               list · residue=the scope walk at this gate compared the post-merge tree
                               against itself and therefore proved nothing; recorded rather than
                               glossed. Method delta filed: a task gated after its merge inherits an
                               unfalsifiable scope check
- protocol-error-lifetime    : gate=PASS · PR #472 (`292269ac`), re-verified on merged `main` @
                               `3f842d9f` rather than on the PR branch; 8/8 under both runtimes ·
                               residue=none
- monitor-command-feed       : gate=PASS · #484 · all 14 Musts and 4 Rejects tested under both
                               runtimes · residue=none. External review round 2 found five real
                               defects my own §6 did not
- batch-protocol-version-fidelity: gate=PASS · four wire assertions on every CI leg at shards 1 and 4 ·
                               residue=none
- sdk-wire-form-fixes        : gate=PASS · sdk/rust round_trip 168/168 helpers driven live, zero
                               protocol-level rejections; sdk_wire_forms 4/4 (monoio) and 3 + 1
                               correctly ignored (tokio — FT.AGGREGATE is behind `text-index`) ·
                               residue=7 pre-existing `test_text.py` async failures that fail
                               identically at HEAD (no pytest-asyncio in this environment)

Milestone-level evidence, gathered on the integrated tree rather than per-PR:
- Full `workflow_dispatch` matrix on the final integrated branch: CI 9/9 (including `Check (Windows)`,
  `Check (macOS)`, `Check (console feature)` and `Check (monoio — the shipped runtime)`, none of which
  PR path-filters run), Console Integration, Crash Matrix (Cross-Plane), Supply Chain — all green.
- EC14 perf A/B (below) FAILED on first measurement and was fixed before this gate, not waived.

### Goal met?   (map the evidence back to this milestone's Exit criteria — read before the Exit-criteria boxes are checked)
- [x] each Exit criterion above is satisfied by a Cross-task evidence row or a Ship-by-domain change (cite which)
- goal: *a stock Redis client, driver, and monitoring agent works against Moon without special-casing.*

  The single line that proves it: **an UNMODIFIED redis-py — its own connection pools, `pipeline()`,
  `pubsub()`, `scan_iter()`/`hscan_iter()`, `redis.lock.Lock`, `from_url`, RESP2 and RESP3
  handshakes — runs green against a live Moon on every CI run**, on a job that fails rather than skips
  when no server is reachable. Not a harness written to agree with us: a client we did not write,
  exercising its own idioms.

  Read honestly, that suite is also what QUALIFIES the claim. It found three defects on its first run
  and each is PINNED inside it rather than skipped, so the milestone ships with them visible:
  **#507** — an `MGET` in the same pipeline batch as the `SET`s that wrote its keys returns nulls at
  `--shards >= 2` despite those `SET`s acking `+OK` earlier in the batch (a read-your-own-writes
  violation, and the most serious thing open against this goal); **#508** — `EVALSHA` of a SINGLE-KEY
  script is rejected `CROSSSLOT`, which breaks `redis.lock.Lock.release()`; and `CLIENT INFO`
  reporting a literal `cmd=NULL`. Both multi-shard defects fire for ~50% of keys, decided by which
  shard owns the key relative to the connection's, so each pin drives twenty distinct keys — a
  single-trial pin was tried first and made CI flaky, which is how the rate was found.

  So: the goal is MET for a stock client at `--shards 1`, and met for identity, negotiation, pub/sub,
  cluster bootstrap and INFO telemetry at any shard count. It is NOT fully met for multi-key pipelined
  reads at `--shards >= 2`. Stated here rather than deferred to a follow-up, because a milestone whose
  own acceptance suite pins two open data-correctness defects should say so at its gate.

## Release steps   (AI-DEFINED — fill the ordered steps to ship this milestone; engine records, human gate)
> The AI writes the release steps for THIS milestone here (hints, not engine commands). MERGE is one
> small step among them. These feed the release scope (release.md) when the cut is bundled.
- [x] land `client-compat-harness` and `monoio-ci-coverage` first and merge them on their own — they are the verifiers every later task cites, and a verifier merged alongside the fix it verifies proves nothing
- [x] land each parity task as its own PR, red-first, each citing the harness output before and after
- [x] run the full local CI matrix in `moon-dev` (fmt · clippy ×2 · monoio release suite · tokio suite) before each push — PR CI skips Windows/macOS/console on all PRs
- [x] run the Linux A/B for the perf-neutrality exit criterion once, on the fully integrated branch, not per-PR
  > Run 2026-08-16 on the integrated tree and it **FAILED**: SET at c=8 P=16 fell 1,360,544 ->
  > 584,795 ops/s (-57.0%, noise floor 7.1%) and at c=1 P=1 159,236 -> 134,048 (-15.8%, floor 11.8%),
  > with GET neutral in both — the shape of a cost paid only by writers. `git bisect` over the
  > benchmark named `527def22` (#478): the maxmemory real-footprint correction was computed INSIDE
  > `evict_to_budget`, i.e. on the write path, costing three `/proc/self/statm` syscalls plus an
  > instance-wide accounting sum (which takes a read lock on the GLOBAL replication state) per SET.
  > Its own noise-floor guard could not skip it — the guard sat inside the callee with the expensive
  > reader passed as its argument, and Rust evaluates arguments eagerly. Fixed in PR #510
  > (`a3043a84`) by sampling once a second in the shard-0 chore that was already reading statm for
  > the RSS gauge; the write path now reads one relaxed atomic. Re-run: all four configs neutral,
  > set_c8_P16 back to 1,307,190 (-1.3% against a 3.4% floor), fast-path retention 1.000 everywhere.
  > Recorded because the criterion was earned by a fix, not by a first-try pass.
- [ ] refresh `docs/redis-compat.md` from the harness manifest so the published compatibility table is generated evidence, not prose
  > **NOT DONE, and the step rests on a premise that does not hold.** Checked 2026-08-16:
  > `scripts/client-compat/manifest.yaml` has 44 entries covering 30 distinct commands, and it is a
  > DEFECT-REPRODUCTION manifest (hard cases first, then one entry per known divergence), not a
  > coverage manifest. Generating a compatibility table from it would publish a 30-of-262-command
  > table that LOOKS authoritative — worse than the current prose, not better. No generator exists
  > either (the last `docs(compat): regenerate` commit, `24ccea60`, left none behind).
  > Meanwhile the published doc is stale: last touched 2026-07-10 in #262, which predates every
  > parity fix in this milestone.
  > The right source for a generated matrix is `metadata.rs` (the 262-command registry) crossed with
  > the reachability sweep — a different artifact than this step names, and its own task. Left
  > unchecked rather than satisfied with a misleading table or a hand-edit.
- [ ] open the milestone PR from the Close ship-review above; the human reviews + merges
- [ ] tag / publish  (human-run, per release.md)
