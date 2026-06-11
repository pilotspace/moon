# PROJECT — living documentation (cross-milestone context)

> The durable foundation that outlives every milestone and feeds context into each
> TDD⇄ADD loop. Read this FIRST in any session. Keep it lean — one screen, not a
> manual. Map to the AIDD diagram: Domain = DDD · Spec = SDD (living document) ·
> UI/UX = UDD. When a loop reveals a gap here, come back and update this file —
> that is the re-entrant arrow from the engine down to the foundation.

slug: moon · stage: production · updated: 2026-06-11
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

## Spec / Living Document (SDD) — what we are building, now
- Active milestone → `.add/milestones/v1-shared-nothing/MILESTONE.md` (see `add.py status`)
- Frozen contracts (living docs): RESP2/RESP3 wire compatibility with Redis (external, immutable); CI matrix (fmt, clippy ×2, tests ×2, MSRV 1.94, unsafe/unwrap audits, fuzz)
- Settled vs still open: settled — thread-per-core + SPSC mesh architecture, monoio default on Linux. Open — sub-linear multi-shard scaling (root causes mapped in 2026-06 review: leaky shared-nothing + 1ms monoio wake floor)

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
