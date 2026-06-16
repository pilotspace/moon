# PROJECT — living documentation (cross-milestone context)

> The durable foundation that outlives every milestone and feeds context into each
> TDD⇄ADD loop. Read this FIRST in any session. Keep it lean — one screen, not a
> manual. Map to the AIDD diagram: Domain = DDD · Spec = SDD (living document) ·
> UI/UX = UDD. When a loop reveals a gap here, come back and update this file —
> that is the re-entrant arrow from the engine down to the foundation.

slug: moon · stage: production · updated: 2026-06-16 · foundation-version: 5
goal: a Redis-compatible server whose thread-per-core architecture measurably out-scales Redis on multi-core hardware — without sacrificing protocol compatibility or durability semantics

---

## Domain (DDD) — the language and the boundaries
<!-- evidence-grounded: README.md, CLAUDE.md, src/ module tree -->
- Core concepts: Shard (thread-per-core owner of a keyspace slice) · Frame (RESP protocol unit) · DashTable (per-shard hash storage) · CompactKey/CompactValue (SSO entry types) · SPSC mesh (cross-shard dispatch channels) · WAL/AOF (per-shard durability log) · Segment (vector index lifecycle unit: mutable → immutable)
- Bounded contexts / modules: `protocol/` (RESP parse/serialize) · `shard/` (event loop, dispatch, coordinator) · `storage/` (dashtable, eviction, tiered) · `persistence/` (wal_v3, aof, page_cache) · `vector/` + `command/vector_search/` (FT.* engine) · `command/` (dispatch tables) · `io/` (uring driver) · `runtime/` (monoio/tokio abstraction) · cross-cutting: `acl/ pubsub/ replication/ cluster/ blocking/ transaction/ scripting/ tracking/`
- Invariants that must always hold:
  - Shared-nothing per shard: no global locks on the command write path (CLAUDE.md "Lock Handling") — **currently violated in ≥8 places (2026-06 architecture review)**
  - Malformed client input never crashes the server (`parse_frame_zerocopy` returns `Frame::Null`)
  - No new `unsafe` without approval + `// SAFETY:` comment (UNSAFE_POLICY.md)
  - No alloc in command dispatch / protocol parse / shard event loop / io drivers
  - All runtime-specific code compiles under both `runtime-monoio` and `runtime-tokio`
  - [foundation v1, 2026-06-13] The shared-nothing write-path lock invariant above is RESTORED and machine-checked as of milestone v1 (`tests/shardslice_shape.rs` forbids `is_initialized()` dual-branches + cross-shard read accessors). Living-doc claims about hot-path locking are NOT self-evident — back them with a CI grep: v1 found CLAUDE.md asserted the client registry was off the hot path, but it was written per batch (finding 1.3 / QW8). The lock-inventory audit grep should become a CI check.

## Spec / Living Document (SDD) — what we are building, now
- Active milestone → `.add/milestones/v1-shared-nothing/MILESTONE.md` (see `add.py status`)
- Frozen contracts (living docs): RESP2/RESP3 wire compatibility with Redis (external, immutable); CI matrix (fmt, clippy ×2, tests ×2, MSRV 1.94, unsafe/unwrap audits, fuzz)
- Settled vs still open: settled — thread-per-core + SPSC mesh architecture, monoio default on Linux. Open — sub-linear multi-shard scaling (root causes mapped in 2026-06 review: leaky shared-nothing + 1ms monoio wake floor)
- [foundation v2, 2026-06-15] A contract invariant that quantifies over "all N implementations" must be verified against EACH one, not assumed uniform: group commit's `CommitOutcome.write_failed ⇒ write_error latch` held in 3 of the 4 AOF writer loops, but the tokio-TopLevel loop never carried the latch (a pre-existing gap the new contract made explicit — Finding 2, wal-group-commit). When a spec says "both writers / all loops", enumerate and check each.
- [foundation v2, 2026-06-15] Keep REJECTED-risk flags IN the frozen §3 contract, not just in discussion: a pre-named, pre-reasoned risk (xshard synchronous-spin serializing pipelined reads) was the exact failure that materialized at verify — naming it at freeze turned a surprise −27.5% P16 regression into a targeted batch-depth-gate fix instead of a redesign (xshard-read-fastpath).
- [foundation v4, 2026-06-15] When the riskiest frozen assumption is a library-internals question, SPIKE it before freezing the contract: a ~40-line standalone program both refuted the make-or-break ⚠ (an io_uring read on an already-ready fd DOES force monoio to drain→park→reap the CQ — not a silent no-op) AND corrected the contract's named primitive (`Pipe` has no `AsyncReadRent`; `UnixStream::pair()` does) before the mechanism was locked. The literal "NOP io_uring op" the request named was unreachable; the spike found the reachable equivalent (ft-yield-costfree-monoio).
- [foundation v5, 2026-06-16] A task framed "perf-only" can hide a CORRECTNESS bug on the same path — re-derive scope from the code during Specify, don't inherit the milestone's framing verbatim: fts-posting-rank-tf's rank work also fixed a latent insertion-order-tf-vs-sorted-bitmap-read misalignment, and fts-query-routing-robustness's real defects (an `expect()` panic, a `[KNN`-bracket misroute, a dropped standalone SPARSE) were nothing like its originally-framed "is_text_query can't see SPARSE".
- [foundation v5, 2026-06-16] When a cross-shard payload CANNOT REPRESENT the contract's algebra, that's a contract-blocking design smell to surface at freeze: a flat `(field_idx, Vec<QueryTerm>)` dispatch payload silently constrained the wire to AND-only — `OR` was unfixable at the leaf until `TextSearchPayload` carried raw `Bytes` (fts-query-eval-dispatch).

## Users (UDD) — UI/UX: design before code
- No UI — surface is the **Redis wire protocol** (RESP2/RESP3) plus CLI flags (`--port --shards --appendonly --dir …`) and INFO/Prometheus metrics.
- Primary users & jobs: backend engineers replacing Redis for higher throughput/lower memory per node; benchmark parity tooling (`scripts/bench-*.sh`, `redis-benchmark`).
- Core flows: connect → AUTH/HELLO → command pipeline → responses; ops flows: persistence reload, replication, FT.* vector search.
- UI states: error surface = RESP error frames with Redis-compatible messages (`-ERR …`); never a crash, never a hang.
- Design source of truth → README.md (architecture diagram + command reference).

## Key Decisions (append-only)
| date | decision | why | outcome |
|------|----------|-----|---------|
| pre-ADD | thread-per-core, monoio/io_uring default on Linux | syscall/wakeup cost dominates at high QPS | shipped v0.3.0 |
| pre-ADD | per-shard WAL, no global append lock | Redis single-AOF serializes at depth | AOF advantage grows with pipeline depth |
| pre-ADD | SSO CompactKey(23B)/CompactValue(12B) | per-key memory vs naive Arc<String> | lower RSS than Redis per key |
| 2026-06-11 | adopt ADD; v1 milestone = restore shared-nothing integrity + remove cross-shard latency floor (review priorities 1·2·5) | 2026-06 architecture review: these two themes explain sub-linear multi-shard scaling | pending |
| 2026-06-11 | FT.SEARCH off-event-loop + WAL group commit deferred to v2 | different themes (event-loop blocking; durability); keep v1 one outcome | recorded in v1 Out list |
| 2026-06-13 | CLOSE v1-shared-nothing: shared-nothing restored (locks deleted, shape-enforced), 1ms monoio wake floor gone (cross-shard p99 0.071ms), consistency 197/197 @1/4/12; s4 routed parity-or-better (+12% P16 GET) vs v0.3.0 | exit criteria met to the agreed "no-regression + honest measurement" bar | done; default-config cross-shard read regression (−85% c1 GET) RISK-ACCEPTED → follow-up: lock-free cross-shard read acceleration (waiver → next perf milestone) |
| 2026-06-13 | fold v1 deltas → foundation-version 1 | close the ADD loop so learnings outlive the milestone | DDD: lock-inventory grep → CI (PROJECT §Domain); TDD: red-suite split pattern + ADD: §3 freeze flag-line requirement (CONVENTIONS) |
| 2026-06-15 | fold v2 deltas → foundation-version 2 (9 deltas from xshard-read-fastpath + wal-group-commit) | close the loop after the first 2 v2-performance tasks; perf-measurement + cross-cutting-deletion lessons recur | SDD: "verify each impl of an all-N invariant" + "keep rejected-risk flags in the freeze" (PROJECT §Spec); TDD: whole-repo symbol-removal grep, MOON_BIN-pinned VM integration, pipelined+control+best-of-7 perf anchor, frozen-red-test-may-be-wrong (CONVENTIONS); ADD: confirm instrument validity before a perf Must, full-dual-runtime gate for deletions, at-BUILD unsafe/unwrap audit (CONVENTIONS) |
| 2026-06-15 | CLOSE v2-performance (3/3 PASS) + fold deltas → foundation-version 3 (3 deltas from ft-search-off-eventloop) | all three v1-deferred bottlenecks delivered (xshard read latency, FT.SEARCH stalls, WAL group commit); a default-runtime perf no-op slipped past green tests until the effectiveness bench | TDD: mechanism-proxy pass ≠ effect measured (CONVENTIONS); ADD: per-runtime EFFECTIVENESS validation for `#[cfg]`-split primitives + make the instrument work before deferring a defect-hiding measurement (CONVENTIONS). v2 absolute magnitudes (xshard µs, WAL throughput) GATE-DEFERRED to GCloud per the milestone's sanctioned VM bench-exception |
| 2026-06-15 | CLOSE v2-1-throughput-polish (1/1 PASS, PR #189) + fold deltas → foundation-version 4 (3 deltas from ft-yield-costfree-monoio) | cost-free monoio self-pipe yield (`UnixStream::pair` park-reap, 0.317µs vs 1746µs) reclaims #179's deferred ~22% QPS; the build-measured A/B caught a knee (256, +4.98% on the 5% line) that mechanism arithmetic had cleared → shipped 512 (+2.74%) | SDD: spike library-internals risk before freeze (PROJECT §Spec); TDD: behavioral wall-time red test over an introspection hook; ADD: relative same-binary A/B beats mechanism arithmetic for a tuning knee (CONVENTIONS). Absolute single-shard RPS on real disk GCloud-deferred with the v2 siblings |
| 2026-06-16 | CLOSE v3-1-fts-hardening (6/6 PASS, PR #192) + fold deltas → foundation-version 5 (9 deltas from fts-posting-rank-tf + fts-query-eval-dispatch + ft-yield-costfree-monoio) | FTS correctness+parity restored: `OR` unions / `TEXT+TAG` intersects, FT.SEARCH reply = true total-matched, the O(V) upsert + O(M²) high-DF cliffs gone, routing handles prose-knn / standalone-SPARSE / no-panic; all 5 exit criteria met | SDD: re-derive scope from code (perf framing hid a correctness bug) + payload-can't-represent-the-algebra is a freeze smell (PROJECT §Spec); TDD: distinct-asymmetric fixtures, assert nonzero per-suite ran-count, a stale `#[ignore]`'d test guards nothing (CONVENTIONS); ADD: a tuning knee must A/B per-arch (x86 breached the aarch64-green 5% bound), risk:high conservative gate is load-bearing, split-parser/dispatch gate-the-pair, deferred-cleanup honesty (CONVENTIONS). Carried follow-ups: un-ignore the stale numeric test, remove `scatter_text_search_filter`, multi-field-OR IDF magnitude |
