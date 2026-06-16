# GLOSSARY  (one name per concept — used everywhere: specs, contracts, code)
# Moon domain terms — evidence-grounded: src/ module tree, CLAUDE.md, README.md

shard: a thread-per-core unit owning one keyspace slice, its event loop, DashTable, WAL writer, and blocking/pubsub registries; the unit of shared-nothing isolation (src/shard/).
SPSC mesh: the grid of single-producer/single-consumer ring channels (256 entries) carrying ShardMessage between shards; the ONLY sanctioned cross-shard mutation path (src/shard/mesh.rs).
shard event loop: the per-shard select! loop (monoio or tokio) draining I/O, SPSC messages, and the 1ms tick; nothing on it may block (src/shard/event_loop.rs).
ShardSlice: the thread-local, lock-free per-shard state access path (`with_shard`) meant to replace the locked Arc<ShardDatabases>; migration in progress, gated on `is_initialized()` (src/shard/slice.rs).
ShardDatabases: the legacy Arc-shared, RwLock-per-DB store every shard can reach — the shared-nothing leak the ShardSlice migration removes (src/shard/shared_databases.rs).
Frame: the RESP protocol value enum (SimpleString, BulkString, Array, Error, …); command errors are Frame::Error, never Result (src/protocol/frame.rs).
DashTable: the per-shard extendible-hashing table (segments × 60 slots) holding CompactKey→Entry (src/storage/dashtable/).
CompactKey / CompactValue: SSO entry types — keys ≤23B and values ≤12B inline, heap beyond via tagged pointer (src/storage/compact_value.rs).
WAL (wal_v3): the per-shard write-ahead log; 1ms buffered flush, 1s fdatasync under everysec (src/persistence/wal_v3/).
AOF: append-only-file persistence layered on the WAL with rewrite/manifest machinery (src/persistence/aof/).
segment (vector): the FT.* index lifecycle unit — mutable (brute-force) → compact → immutable (HNSW + quantized codes); merged in the background (src/vector/segment/).
cross-shard latency floor: the ~1ms minimum cross-shard reply latency on monoio caused by the pending_wakers 1ms-tick relay (event_loop.rs:1049) — v1 milestone target.
hot path: command dispatch, protocol parsing, shard event loop, io drivers — the zones where allocation and locks are forbidden (CLAUDE.md "Allocations on Hot Paths").
2026-06 architecture review: the principal-level performance review (this session) cataloguing shared-nothing violations, dispatch-floor causes, and event-loop blocking work; source of v1+ milestone priorities.

# ADD method vocabulary (domain-standard names; bridges to legacy terms)
GOAL: the one durable outcome a project (and each milestone) runs toward — the loop's orientation anchor, declared as the lowercase `goal:` line in PROJECT.md / MILESTONE.md and surfaced by status/guide every session; distinct from a task's §1 Must (a single required behavior, not the whole-project outcome).
deep verify: the deepened Verify evidence (v20) required beyond passing tests — for a task that produced code, that every new symbol is referenced (wiring) and no new dead/unused code exists; for prose/non-code, a recorded no-skim semantic read; which path applies is resolver-judged and the engine never classifies (a rubric, not add.py).
onboarding: the install -> first-milestone path (formerly "on-ramp").
primary flow: the solid forward path of the flow diagram — a phase starts only when its input exists (formerly "forward spine").
cross-cutting concern: a concern running through every step rather than being one step — security, testing, observability, cost (formerly "spine / continuous concern").
working state: everything an agent loads each session — skill router, active phase, PROJECT/MILESTONE/TASK, state.json (formerly "state surface").
audit trail: the reference record read by people, never auto-loaded into agent context (formerly "story surface").
method rationale: the why behind every rule — the AIDD book, loaded on demand, never duplicated (formerly "trust layer").
failing-first suite: the test suite written before code, confirmed red for the right reason — a missing implementation (formerly "red safety net").
non-functional review: the deliberate post-evidence check of what tests rarely catch — concurrency, security, architecture (formerly "blind-spot checks").
change scope: the files a locked run may and may not touch (formerly "touch-boundary"; the <touch_boundary> XML prompt tag keeps its name).
automated quality gate: the evidence-based Verify resolver under autonomy auto — may auto-PASS on complete evidence; security always escalates (formerly "evidence auto-gate").
autonomy level: the per-task Verify resolver setting — auto (default) or conservative; declared in the TASK.md header, human-reviewed at the freeze (formerly "autonomy dial").
living documentation: the durable project artifacts — conventions, glossary, frozen contracts — that outlive any particular code (formerly "survivor layer").
scope level: the granularity a decision lives at — intake level (request -> versioned scope), milestone level, setup/foundation level, task level (formerly "altitude").
baseline approval: the one human gate that freezes the AI-drafted foundation, first scope, and first contract together — runs as `add.py lock` (formerly "the lock-down").
lesson learned: a single learning a loop produces, tagged by the competency it improves — the `- [DDD · open]` grammar and deltas.md/`add.py deltas` machine names stay (formerly "competency delta").
lowest-confidence flag: the AI's ranked declaration of the 1–2 points most likely to be wrong in what a human is asked to approve — each with why + cost-if-wrong; the ⚠ glyph keeps its name as the machine marker (formerly "least-sure flag").
decision point: a stop for human judgment — the contract-freeze approval, an escalated verify gate, intake confirmation, milestone close; the machine names seam (--json owner enum, decide key) and seam-audit (CI job) keep their names (formerly "seam").
retrospective consolidation: gathering confirmed lessons learned at milestone close and writing them append-only into the versioned foundation — human-confirmed, never self-approved; the machine names fold.md, the folded status, and add.py deltas keep their names (formerly "the fold / fold ritual").
specification bundle: a task's spec, scenarios, contract, and failing tests drafted as one piece and approved by a person once at the contract freeze (formerly "the one-approval front").
self-pipe yield: the monoio cost-free cooperative yield (ft-yield-costfree-monoio) — a read of one byte from an always-ready per-shard `UnixStream` socketpair, which forces the io_uring run loop to drain→park→reap the completion queue (servicing co-located connections) at ~µs instead of the timer wheel's ~1.8ms (`sleep(ZERO)`); falls back to `sleep(ZERO)` off io_uring or on init failure.
cost-free park-reap: the property a self-pipe yield achieves — one CQ reap per yield with no timer-wheel latency (measured 0.317µs/yield vs 1746µs for `sleep(ZERO)`, 5514× cheaper), so the brute-force chunk no longer has to be coarse to amortize the yield.
yield knee (K): the brute-force chunk size (`max_brute_force_vecs_per_chunk`) that trades co-located latency (smaller = finer relief) against search throughput (smaller = more yields); with the cost-free yield it returns to the cross-arch build-measured 1024 — an end-to-end FT.SEARCH A/B (20k×384d, release) holds it within the 5% throughput bound on BOTH targets (x86_64 Sapphire Rapids +2–3.5%, aarch64 Neoverse-N1 +2–3.4%) while still yielding ~20×/query. The knee is ARCH-dependent: a finer 512 holds on aarch64 (+4%) but breaches the bound on x86 (+6–8%, faster AVX-512 scan shrinks chunk wall-time) — found by a GCloud cross-arch bench, so the default is the conservative cross-arch value. Overridable per deployment via `MOON_FT_YIELD_CHUNK`.
