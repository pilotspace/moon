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
- [ ] client-compat-harness        depends-on: none  — raw-RESP diff harness (Moon vs real redis-server) asserting reply TYPE and shape per command in RESP2 and RESP3, plus a CI job that FAILS (never skips) when no server is reachable. Every other task's verifier.
- [ ] monoio-ci-coverage           depends-on: none  — a CI test job that builds monoio. All current CI test jobs build tokio, which is the structural reason a security bypass on the default runtime shipped in two releases. Also re-enables `cargo clippy --all-targets` (currently omitted, so test-code lints never gate).
- [ ] client-identity-introspection depends-on: client-compat-harness — HELLO `version` → REDIS_COMPAT_VERSION (redis-py refuses client-side caching against the Moon version string); COMMAND COUNT/INFO/GETKEYS/DOCS served from `metadata.rs`; ROLE, RESET, CLIENT SETINFO with lib-name/lib-ver surfaced in CLIENT LIST/INFO; reconcile the registered-but-unreachable set (DUMP, RESTORE, LATENCY, MODULE, RECLAMATION) — implement or deregister — plus a test that the registry and dispatch cannot diverge again. Also closes the INVERSE divergence: MONITOR, MQ and WS are reachable-or-wanted but absent from `metadata.rs`, so they are invisible to COMMAND and uncategorised for ACL — the reconciliation test must sweep both directions, and MONITOR itself (unimplemented, and not on the known-gap allowlist) is implemented or explicitly declared a non-goal here.
- [ ] watch-cas-transactions      depends-on: client-compat-harness — WATCH/UNWATCH answer "unknown command" on both production dispatch paths (`handler_monoio`, `handler_sharded`); the implementation exists only in `handler_single.rs`, the embedded-mode handler, while `metadata.rs` registers both so COMMAND and ACL claim they are available. Optimistic-locking CAS is the one missing primitive with no client-side workaround. Includes the key-touch invalidation path (EXEC must abort when a watched key changed, cross-shard included) and removal of WATCH/UNWATCH from the `BACKLOGGED_UNIMPLEMENTED` allowlist in `tests/wire_reachability_red.rs`.
- [ ] protocol-error-lifetime     depends-on: client-compat-harness — a malformed frame must produce `-ERR Protocol error: <detail>` and then close, after the already-valid pipelined prefix has been answered. Today Moon sends NOTHING for `PING` + a bad multibulk header (the valid PONG dies with the connection), and a bulk header followed by inline garbage produces no reply AND no close, so the client blocks to its own socket timeout. This is a connection-lifetime defect, not the reply-formatting item originally folded into `info-observability` — that scope moves here.
- [ ] resp3-type-fidelity          depends-on: client-compat-harness — fix Map-vs-pairs inversions (ZRANDMEMBER/HRANDFIELD must be pair arrays); add pair-wrapping + Double for ZRANGE/ZPOPMIN/ZDIFF/ZUNION WITHSCORES; stop over-converting SISMEMBER/EXPIRE (Integer) and INCRBYFLOAT (Bulk); route CONFIG and EXEC/pipeline inner replies through `apply_resp3_conversion`; XINFO STREAM → Map with the missing fields; SPOP `<count>` → Set. Oracle sweep 2026-08-09 widened this: the whole `int_to_bool` branch is wrong (Redis answers Integer for all 7 of SISMEMBER/HEXISTS/EXPIRE/PEXPIRE/PERSIST/SETNX/MSETNX), `bulk_to_double` is wrong for INCRBYFLOAT/HINCRBYFLOAT and missing for ZMSCORE/GEOPOS, and the conversion needs ARG awareness (WITHSCORES/WITHVALUES/count decide the shape) — which the current `maybe_convert_resp3(cmd, response, proto)` signature cannot express. Transaction *semantics* moved OUT to `multi-exec-queue-semantics`.
- [ ] multi-exec-queue-semantics   depends-on: client-compat-harness — two defect classes, both proven by a shards=1 vs shards=2 control (2026-08-09). (A) shard-independent: CONFIG, CLIENT and INFO are EXECUTED at queue time instead of queued — they answer their result where Redis answers `+QUEUED`, and EXEC then returns a short array; MOVE inside EXEC answers `-ERR MOVE requires handler-level dispatch`. Same root as the v0.8.6 inline-GET P0: an intercept runs before the MULTI-queue check. (B) shards>=2 only: multi-key commands inside MULTI SILENTLY return empty or nil — ZINTER `*[]`, ZINTERCARD `:0`, LMPOP `$nil` with the pop not applied — all correct at shards=1; RENAMENX/MSETNX at least fail loudly with `-CROSSSLOT`. Also owns the missing `-EXECABORT` on queue-time errors (Moon queues a bad command and surfaces its error from inside EXEC, so a poisoned transaction looks partially applied).
- [ ] pubsub-resp3-push            depends-on: client-compat-harness — subscribe/unsubscribe confirmations as Push frames under RESP3; lift the subscriber-mode command restriction when `protocol_version >= 3`; add RESET and (S)SUBSCRIBE to the RESP2 subscriber allowlist; implement SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH and PUBSUB SHARD*.
- [ ] cluster-client-bootstrap     depends-on: client-compat-harness — CLUSTER SHARDS; READONLY/READWRITE; `cluster_state` must report `fail` while slots are uncovered (today it answers `ok` with zero slots assigned, so clients believe an unusable topology is healthy); `# Cluster` section in INFO; MIGRATE or an explicit documented non-goal plus a supported resharding path.
- [ ] info-observability           depends-on: client-compat-harness — dedupe the two `# Replication` sections; honor `INFO <section>` (today the argument is ignored); fill `# Commandstats` and add `# Errorstats`/`# Latencystats`; add the missing counters (33 of the 51 pinned monitoring fields are absent: keyspace_hits/misses, evicted_keys, expired_keys, maxmemory*, instantaneous_ops_per_sec, total_net_*, rejected_connections, uptime_in_seconds, run_id, tcp_port, redis_mode, cluster_enabled…); keyspace notifications (notify-keyspace-events + `__keyspace@`/`__keyevent@`). The protocol-error item moved OUT of this task to `protocol-error-lifetime` — the observed defect is a connection-lifetime bug, wider than the reply-formatting fix this task had scoped.
- [ ] sdk-wire-form-fixes          depends-on: client-identity-introspection — moondb Rust `mq.rs` sends `MQ.PUSH`/`MQ.POP`, a wire form the server rejects (the container form is `MQ PUSH`/`MQ POP`); moondb Python version disagrees with pyproject (0.1.0 vs 0.1.1) and ships no mq/temporal/workspace modules; register MQ/WS in `metadata.rs` so ACL and COMMAND can see them.

## Exit criteria (observable; map each to the task that delivers it)
- [ ] A raw-RESP diff of Moon against a real `redis-server` reports zero unexplained differences in reply type or shape across the covered command manifest, in both RESP2 and RESP3 (verify: `scripts/test-client-compat.sh --strict`) (← client-compat-harness)
- [ ] An acceptance suite driven by an unmodified **redis-py** — its own connection pools, `pipeline()`, `pubsub()`, `scan_iter()`, `Lock`, `from_url`, not raw sockets — runs green against a live Moon in CI, alongside the raw-RESP differ, on a job that fails rather than skips when no server is reachable (verify: steps `redis-py acceptance suite (unmodified client, live server)` and `Compatibility diff across contexts` in CI job `client-compat` in `.github/workflows/ci.yml`) (← client-compat-harness)
  > AMENDED 2026-08-15. Originally named redis-py + go-redis + ioredis. Narrowed to redis-py plus the raw-RESP differ: the self-hosted runner has no Go or Node toolchain and cannot get one (PEP 668 blocks pip; `python3.14-venv` is absent — redis-py comes from the distro package `python3-redis`). redis-py is the most-used client, and the differ already covers wire shape for all three. Known gaps the suite found (#507, #508, and `CLIENT INFO cmd=NULL`) are pinned inside it as amplified probes — twenty distinct keys each, because both multi-shard defects fire for only ~50% of keys — so a fix breaks the run rather than leaving a stale skip.
- [ ] A monoio build runs the test suite in CI, so a defect on the default runtime cannot ship unobserved (verify: CI job `check-monoio` green on a PR, and red when the v0.8.6 P0 fix is reverted) (← monoio-ci-coverage)
- [ ] redis-py negotiates client-side caching against Moon without a version override, and `COMMAND DOCS`/`COMMAND GETKEYS` answer for every registered command (verify: `cargo test --test client_identity_introspection`) (← client-identity-introspection)
- [ ] No command registered in `metadata.rs` answers "unknown command" on any of the three dispatch paths (verify: `cargo test --test wire_reachability_red cdg1_registry_sweep_no_unknowns`, which enumerates the registry and dispatches every entry) (← client-identity-introspection)
- [ ] Every command in the manifest returns the same reply shape standalone, inside MULTI/EXEC, and inside a pipeline, under both protocols (verify: `scripts/test-client-compat.sh --contexts standalone,multi,pipeline`) (← resp3-type-fidelity)
- [ ] A RESP3 subscriber receives subscribe/unsubscribe confirmations as Push frames and may issue non-pub/sub commands while subscribed, matching Redis (verify: `cargo test --test pubsub_resp3_push`) (← pubsub-resp3-push)
- [ ] A stock cluster-aware client bootstraps against a Moon cluster via CLUSTER SHARDS, and reports the topology unhealthy while slots are uncovered instead of accepting it (verify: `cargo test --test cluster_client_bootstrap`) (← cluster-client-bootstrap)
- [ ] `INFO` exposes every field the pinned monitoring-stack manifest reads, with no duplicate sections, and `INFO <section>` returns only that section (verify: `scripts/test-client-compat.sh --info-manifest`) (← info-observability)
- [ ] Every command answers `+QUEUED` inside MULTI and its real reply from EXEC, with identical values at 1, 2 and 4 shards — no command executes at queue time and no multi-key command silently returns empty (verify: `cargo test --test multi_exec_queue_semantics`) (← multi-exec-queue-semantics)
- [ ] A stock client completes a WATCH/MULTI/EXEC compare-and-set against Moon, and EXEC returns nil when a watched key changed under it — on every dispatch path, single-shard and cross-shard (verify: `cargo test --test watch_cas_transactions`) (← watch-cas-transactions)
- [ ] A malformed frame is answered with `-ERR Protocol error: …` after the valid pipelined prefix has been replied, then the connection closes — no stalled connection and no discarded valid reply (verify: `cargo test --test protocol_error_lifetime`) (← protocol-error-lifetime)
- [ ] The first-party Rust and Python SDKs execute their full documented surface against a live Moon with no rejected wire forms (verify: `cargo test -p moondb --test sdk_wire_forms` and `pytest sdk/python/tests`) (← sdk-wire-form-fixes)
- [ ] The milestone is perf-neutral on the hot path (verify: Linux VM A/B, one server alive at a time, alternating order, medians of 5 — no regression beyond noise at c=1 P=1 and c=8 P=16, with fast-path retention confirmed via `moon_dispatch_path_total{path="local_inline"}`) (← all tasks)

## Close — ship review   (AI fills when every task is done — the evidence behind the engine gate, read before the boxes are checked)
> Whole-milestone, cross-task review the AI fills in. It is the evidence behind the EXISTING engine
> gate (milestone-done / checking the Exit-criteria boxes) — NOT a new approval. Tool-agnostic.

### Ship by domain   (what changed, per bounded context)
- tooling : <add.py / state.json / templates — what shipped, or "untouched">
- skill   : <SKILL.md / phases/* / guides — what shipped, or "untouched">
- book    : <docs/* — what shipped, or "untouched">

### Cross-task evidence   (one row per task)
- <slug> : gate=<PASS|RISK-ACCEPTED> · tests=<n green> · residue=<none|note>

### Goal met?   (map the evidence back to this milestone's Exit criteria — read before the Exit-criteria boxes are checked)
- [ ] each Exit criterion above is satisfied by a Cross-task evidence row or a Ship-by-domain change (cite which)
- goal: <restate the milestone goal — and the one evidence line that proves the ship meets it>

## Release steps   (AI-DEFINED — fill the ordered steps to ship this milestone; engine records, human gate)
> The AI writes the release steps for THIS milestone here (hints, not engine commands). MERGE is one
> small step among them. These feed the release scope (release.md) when the cut is bundled.
- [ ] land `client-compat-harness` and `monoio-ci-coverage` first and merge them on their own — they are the verifiers every later task cites, and a verifier merged alongside the fix it verifies proves nothing
- [ ] land each parity task as its own PR, red-first, each citing the harness output before and after
- [ ] run the full local CI matrix in `moon-dev` (fmt · clippy ×2 · monoio release suite · tokio suite) before each push — PR CI skips Windows/macOS/console on all PRs
- [ ] run the Linux A/B for the perf-neutrality exit criterion once, on the fully integrated branch, not per-PR
- [ ] refresh `docs/redis-compat.md` from the harness manifest so the published compatibility table is generated evidence, not prose
- [ ] open the milestone PR from the Close ship-review above; the human reviews + merges
- [ ] tag / publish  (human-run, per release.md)
