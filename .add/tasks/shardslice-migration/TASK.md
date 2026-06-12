# TASK: Wire ShardSlice: thread-local shared-nothing storage becomes the live path

slug: shardslice-migration · created: 2026-06-12 · stage: production · risk: high · autonomy: conservative
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: ShardSlice Phase 4 — `init_shard` wired at shard startup on BOTH runtimes,
thread-local shared-nothing storage becomes the live path, and the RwLock/Mutex
wrappers in ShardDatabases are deleted. This is the milestone's "without
per-command global locks" goal made real: today `slice::init_shard` has ZERO
production call sites, `is_initialized()` is always false, and every one of the
~130 dual-branch decision points takes the lock-based `else` branch.

Ground truth (3 parallel scouts, 2026-06-12, file:line evidenced):
- BOTH runtimes already pin connection tasks to dedicated per-shard OS threads:
  monoio = one runtime per `shard-N` thread (main.rs:1303–1347,
  monoio_impl.rs:60–66); tokio = `new_current_thread` + `LocalSet` +
  `spawn_local` (tokio_impl.rs:57–65, conn_accept.rs:263). NO work-stealing
  anywhere — `!Send` ShardSlice fits both runtimes natively.
- Structural gap at startup: databases are moved into `ShardDatabases::new`
  (main.rs:1226–1230, embedded.rs:278) BEFORE shard threads spawn; `Shard::run`
  has nothing to hand `init_shard`. Boot ownership must be restructured so each
  shard thread receives its `Box<[Database]>` + stores and calls `init_shard`
  at the top of `run()`, before the first accept.
- Slice-vs-lock branch divergences (must be fixed, not collapsed blindly):
  WS.DROP key cleanup (handler_sharded/mod.rs:116-area: slice deletes on conn's
  shard, lock routes to {wsid} owner), ALL SIX MQ arms in both runtimes'
  write.rs (slice conn-local, lock owner-routed — comments name this task),
  TXN.COMMIT MQ materialize in both txn.rs (same). Everything else (~125
  branch points across 16 files) is semantically equivalent and collapses.
- WAL appends are channel SENDS (`MpscSender<Bytes>`, wal_append_txs) — Send by
  design; cross-shard `wal_append(owner, …)` stays legal under slice with NO
  message variant. The senders array stays in the residual shared struct.
- Cross-thread direct-LOCK consumers that block wrapper deletion (the real
  Phase 4 work beyond wiring): (1) AOF rewrite fold — `aof-writer-N` threads
  take a foreign shard's write locks (aof/rewrite.rs:497–502 carries an
  explicit "Phase 4 MUST revisit" warning); (2) Prometheus memory publisher
  (metrics_setup.rs:1396–1429, admin tokio thread locks read_db + vector +
  graph across ALL shards every 15s); (3) MEMORY DOCTOR
  (command/server_admin.rs:385–416, same pattern on demand). All other
  background work is own-shard on the shard thread and already Phase-2d-gated.
- SPSC gaps needing new ShardMessage variants (after the WAL-sender and
  global-WS simplifications): an MQ domain hop (GraphCommand precedent — one
  variant serving CREATE/PUSH/POP/ACK/DLQLEN/TRIGGER on the owner, since
  Execute cannot express durable-flag/DLQ logic), a TXN MQ-intent materialize
  hop, and a WS.DROP prefix-cleanup hop.
- ~30 test/bench `ShardDatabases::new` construct sites break when its shape
  changes; Phase-3 leftovers (aggregate_memory → read_memory_sum at
  persistence_tick.rs:344,499; vector/graph memory atomics for metrics) become
  prerequisites for deleting the wrappers.

Framings weighed: Full Phase 4, both runtimes (CHOSEN — user decision
2026-06-12: wire unconditionally, delete wrappers, collapse all dual branches
in this task) · flag-gated cutover with wrapper deletion deferred (offered,
declined) · monoio-only with tokio keeping locks (offered, declined — moot
anyway: tokio is already thread-pinned).

Must:
<must>
  - M1 (wiring): `init_shard(ShardSlice::new(…))` runs at the top of
    `Shard::run()` on every shard thread, both runtimes, both entry points
    (main.rs AND embedded.rs), before the first connection is accepted.
    `is_initialized()` is true on every shard thread in production. Boot
    ownership restructured so the slice receives the real `Box<[Database]>`,
    vector/text/graph stores, intents, registries, and the
    `memory_publisher(shard_id)` atomic — not copies.
  - M2 (owner-routing the divergent branches): the five divergence groups
    (WS.DROP cleanup; MQ CREATE/PUSH/POP/ACK/DLQLEN/TRIGGER ×2 runtimes;
    TXN.COMMIT MQ materialize ×2) reach the OWNING shard under slice via new
    SPSC domain hops (MqCommand-style message + MQ-intent materialize + WS
    prefix-cleanup), mirroring the GraphCommand precedent. The consistency
    suite must hold 197/197 at shards=1/4/12 with slice live — same bar as
    the consistency-dispatch-gaps task.
  - M3 (workspace registry): the slot-0 Mutex trick is replaced by ONE
    process-global `Mutex<WorkspaceRegistry>` owned OUTSIDE the slices
    (control-plane state, rare ops — sharding it buys nothing); the per-shard
    `workspace_registry` field leaves ShardSlice/ShardSliceInit; WS WAL
    records keep their shard-0 stream via the (still-shared) WAL senders.
  - M4 (cross-thread consumers): AOF rewrite no longer takes foreign shard
    locks — the fold is redesigned as a shard-thread-cooperative snapshot
    handoff (message the shard, it produces the frozen view); the Prometheus
    publisher and MEMORY DOCTOR read published per-shard atomics (extend the
    memory_per_shard pattern to vector/text/graph memory) instead of locking
    all shards; the two Phase-3 leftover aggregate_memory call sites switch
    to read_memory_sum.
  - M5 (wrapper deletion): the RwLock/Mutex wrappers around per-shard state
    are DELETED from ShardDatabases; ~130 dual branches collapse to the slice
    path; ShardDatabases shrinks to genuinely-shared state only (SPSC mesh
    handles, WAL senders, published atomics, the global workspace registry,
    cluster/pubsub handles). The ~30 test/bench construct sites are updated
    to the new shape. uring_handler (MOON_URING=1 bridge, runs synchronously
    ON the shard thread) switches from write_db locks to with_shard.
  - M6 (verification bar): cargo test --lib + all integration suites green on
    both runtimes (macOS + Linux VM, MOON_BIN pinned); consistency sweep exit
    0 at 1/4/12; loom models still pass; fuzz targets unaffected; fmt/clippy
    clean on both CI feature sets.
  - M7 (performance): no bench-swf cell regresses vs current main (same
    REQS, idle VM, wakefloor-style comparison); the measured multi-shard
    delta is recorded as the milestone's "measured scaling improvement"
    evidence — improvement is recorded, not gated (user decision: that
    no-regression + honest measurement is the bar).
</must>
Reject:
<reject>
  - A shard thread serving commands without init_shard having run -> fail
    fast at STARTUP (panic/abort with shard id), never a per-command
    `with_shard` panic at runtime ("uninitialized_shard").
  - Nested `with_shard`/`with_shard_db` (RefCell double-borrow) introduced by
    the migration -> forbidden; closures stay flat, cross-shard work exits
    the closure before hopping ("slice_reentrancy").
  - Any `unsafe`, `Send`/`Sync` impl, or pointer trick to share a ShardSlice
    across threads -> forbidden; the `!Send` compile error IS the design
    ("slice_send_violation").
  - Weakening any test, the consistency suite, or protocol semantics to make
    the cutover pass -> forbidden ("test_weakening").
  - A background/admin path quietly re-acquiring a foreign-shard lock through
    a retained wrapper -> forbidden once M5 lands; the wrappers must be GONE,
    not bypassed ("wrapper_resurrection").
  - Holding a `with_shard` borrow across an `.await` -> forbidden (same rule
    as locks across await; the borrow is synchronous by construction)
    ("borrow_across_await").
</reject>
After:
<after>
  - Production servers (monoio AND tokio, main AND embedded) run with
    thread-local ShardSlices live on every shard thread; per-command access
    to shard-local state takes zero lock operations.
  - ShardDatabases contains no RwLock<Database>/Mutex<store> wrappers; the
    type still exists but only as the shared-infrastructure handle.
  - WS/MQ/TXN-MQ behave identically from every connection at every shard
    count (the cdg suite proves it), now via SPSC hops instead of cross-shard
    locks.
  - AOF rewrite, metrics, and MEMORY DOCTOR work without touching another
    thread's state directly.
  - bench-swf shows no regression; the multi-shard delta is recorded in §6
    as milestone evidence.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ The AOF-rewrite redesign is containable inside this task — lowest
    confidence because it is a durability path crossing thread ownership
    (aof/rewrite.rs:497's own comment defers it to "a future Phase 4"), and
    a cooperative-snapshot protocol (freeze + handoff) touches WAL/AOF
    correctness under concurrent writes; if wrong: data-loss-grade bug or
    the task splits, leaving wrapper deletion (M5) blocked.
  ⚠ The three scouts' inventory is COMPLETE — i.e. no un-inventoried
    cross-thread Database/store access hides outside the ~130 gated sites +
    3 named consumers (candidates: blocking-command wakeups, Lua, response
    slots, connection migration, replication) — lower confidence because
    213 call sites were migrated across Phases 2a–2f by different waves;
    if wrong: runtime panic (thread-local miss) or silent wrong-shard data
    under slice, surfacing only under load.
  - [ ] Tokio's LocalSet pinning makes !Send slices safe under tokio with no
        runtime changes (scout-verified: current_thread + spawn_local,
        conn_accept.rs:263) — confirm with a tokio slice smoke test early.
  - [ ] WAL cross-shard appends via MpscSender stay correct under slice
        (senders are Send; the drain side is the owner's event loop).
  - [ ] An MqCommand SPSC hop can carry all six MQ subcommands' semantics
        (durable flag, DLQ routing, trigger debounce) — the GraphCommand
        precedent suggests yes.
  - [ ] No-regression (M7) is achievable: the slice path strictly removes
        uncontended parking_lot acquisitions from the hot path.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: ssm1 slice live on every shard thread at startup (M1)
  Given a 4-shard server started fresh (each runtime; main and embedded entry)
  When the server begins accepting connections
  Then every shard thread logs ShardSlice initialization exactly once before
       its first accept, and PING/SET/GET succeed from fresh connections
  And a probe command confirms the slice path is serving (no lock-path
      counters increment / the lock wrappers no longer exist to be counted)

Scenario: ssm2 WS/MQ owner-routing survives the cutover (M2)
  Given a 4-shard server with slice live
  When the cdg6 cross-connection tests (a–g) and the full consistency suite run
  Then cdg6 passes 7/7 and the suite reports 197/197 at shards=1, 4, and 12
  And MQ CREATE on one connection is PUSH/POP/ACK/DLQLEN-able from fresh
      connections landing on every other shard (now via the MQ SPSC hop)

Scenario: ssm3 workspace registry is process-global (M3)
  Given a 4-shard server with slice live and a workspace created
  When WS AUTH / WS LIST run from 12 fresh connections, then the server
       restarts with appendonly yes
  Then every connection sees the workspace before AND after restart
       (WAL replay lands in the global registry)
  And ShardSlice no longer carries a workspace_registry field

Scenario: ssm4a AOF rewrite without foreign locks (M4)
  Given a 4-shard server with slice live, appendonly yes, and 10k keys written
  When BGREWRITEAOF runs while writes continue, then the server restarts
  Then the rewrite completes, no aof-writer thread touches another thread's
       Database directly (code-shape pin: the rewrite fold takes a
       shard-produced frozen snapshot), and all keys survive the restart

Scenario: ssm4b metrics and MEMORY DOCTOR are lock-free observers (M4)
  Given a 4-shard server with slice live under active write traffic
  When the Prometheus endpoint is scraped and MEMORY DOCTOR runs repeatedly
  Then both return plausible non-zero memory figures sourced from published
       per-shard atomics
  And neither path contains a read_db/vector_store/graph_store lock acquisition
      (code-shape pin)

Scenario: ssm5 the wrappers are gone (M5)
  Given the migrated codebase
  When grepping ShardDatabases for RwLock<Database> / Mutex<VectorStore> /
       Mutex<TextStore> / RwLock<GraphStore> / per-shard registry Mutexes
       and for `slice::is_initialized()`
  Then zero wrapper fields remain, zero dual-branch gates remain in
       production code (the gate fn may survive only for tests), and all
       ~30 test construct sites compile against the new shape
  And uring_handler accesses shard state via with_shard, not write_db

Scenario: ssm6 full matrix green (M6)
  Given the migrated codebase
  When the full verification matrix runs (lib tests both runtimes macOS+VM,
       integration suites with MOON_BIN pinned, loom, fmt, clippy both CI
       feature sets, consistency sweep)
  Then everything passes with zero skipped/weakened tests

Scenario: ssm7 perf bar (M7)
  Given bench-swf on an idle Linux VM (load < 4), same REQS as the baseline
  When slice-live HEAD is compared cell-by-cell against current main
  Then no cell regresses beyond run-to-run noise, and the s1/s4 P1/P16
       deltas are recorded in §6 as the milestone scaling evidence

Scenario: reject uninitialized_shard
  Given a shard event loop entered without init_shard (constructible only in
        a test harness after M1)
  When the loop would begin serving
  Then the process fails fast at startup with the shard id in the error
  And no command is ever answered from an uninitialized thread
      (no per-command with_shard panic reachable over the wire)

Scenario: reject slice_reentrancy
  Given the migrated handlers
  When with_shard is entered while another with_shard borrow is live
       (the slice.rs unit pin)
  Then it panics with the named recursive-borrow message in tests
  And no production call path constructs that nesting (review + the cross-
      shard hops all exit the closure before sending)

Scenario: reject slice_send_violation
  Given ShardSlice's _not_send marker
  When any code attempts to move/share a slice across threads
  Then compilation fails (the marker stays; no Send/Sync impl, no unsafe
       added anywhere in the migration)

Scenario: reject test_weakening
  Given the frozen suites (wire_reachability_red, cross_shard_consistency_red,
        scripts/test-consistency.sh assertions)
  When the migration lands
  Then their assertions are byte-identical to pre-task state (diff review)

Scenario: reject wrapper_resurrection
  Given M5 is complete
  When any later commit in this task adds a lock around per-shard state back
       into ShardDatabases or a helper reintroducing cross-thread access
  Then the code-shape pin (ssm5 grep test) fails CI

Scenario: reject borrow_across_await
  Given the migrated handlers
  When reviewing every with_shard/with_shard_db call site
  Then no closure spans an .await (structurally impossible — the closure is
       sync — but verified: no async block captures the guard-like borrow)
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

No wire-protocol change: every RESP command behaves byte-identically (the cdg
suite + scripts/test-consistency.sh assertions are the wire contract and stay
byte-identical — reject test_weakening). The frozen shape here is the
*internal* surface: boot handoff, new SPSC messages, the residual shared
struct, and the AOF cooperative-snapshot protocol.

### C1 — Boot handoff (M1)

```rust
// src/shard/shared_databases.rs — construction now RETURNS the per-shard
// slice packages instead of swallowing the per-shard state behind locks.
impl ShardDatabases {
    /// Same argument list as today. WAL/AOF recovery replays into the
    /// Databases BEFORE this call (single-threaded boot — no locks needed).
    pub fn new(/* unchanged args */) -> (ShardDatabases, Vec<ShardSliceInit>);
}
```

- `main.rs` and `embedded.rs` destructure; each shard thread's spawn closure
  receives its `ShardSliceInit` BY MOVE; the FIRST statement on the shard
  thread (both runtimes, before runtime block-on / first accept) is
  `crate::shard::slice::init_shard(init)`.
- The event loop asserts `slice::is_initialized()` once before entering its
  accept/drain loop and aborts the process with the shard id otherwise
  ("uninitialized_shard" → startup abort, never a per-command panic).
- The ~30 test/bench construct sites destructure the new tuple; test harnesses
  that need lock-style access are migrated to drive a real shard thread or use
  the slice directly on the test thread.

### C2 — New ShardMessage variants (src/shard/dispatch.rs)

```rust
/// MQ domain hop (GraphCommand precedent): conn handler computes
/// owner = key_to_shard(effective_queue_key); owner executes the FULL arm
/// (durable flag, DLQ stream creation, trigger debounce) against its slice.
MqCommand(Box<MqCommandPayload>),
pub struct MqCommandPayload {
    pub db_index: usize,
    /// Workspace prefix ("{wsid}:" or empty) — owner derives effective keys
    /// exactly as the inline arm does today (incl. DLQ sibling keys).
    pub key_prefix: bytes::Bytes,
    /// Full original MQ.* frame (CREATE/PUSH/POP/ACK/DLQLEN/TRIGGER).
    pub command: std::sync::Arc<Frame>,
    pub reply_tx: channel::OneshotSender<Frame>,
}

/// TXN.COMMIT MQ-intent materialize hop. Sender groups txn.mq_intents by
/// owner shard; owner applies get_stream_mut → durable check → add, exactly
/// the txn.rs:160–167 fold. MqIntent is the existing type
/// (src/transaction/mod.rs:84 — queue_key + fields, Clone).
MqTxnMaterialize {
    db_index: usize,
    intents: Vec<crate::transaction::MqIntent>,
    reply_tx: channel::OneshotSender<()>,
},

/// WS.DROP best-effort key cleanup. prefix = "{<wsid-hex>}:";
/// owner = key_to_shard(prefix); db pinned to 0 (current behavior, both
/// branches). Reply = deleted count (logged, not surfaced to the client).
WsDropCleanup {
    prefix: bytes::Bytes,
    reply_tx: channel::OneshotSender<u64>,
},

/// AOF rewrite cooperative snapshot (C4). Shard builds the expired-filtered
/// fold of ALL its dbs and replies; the writer thread never touches shard
/// state.
AofFold {
    reply_tx: channel::OneshotSender<AofFoldSnapshot>,
},
/// (entries, base_timestamp) per db index — same fold shape rewrite.rs
/// builds today (feeds rdb::save_snapshot_to_bytes unchanged).
pub struct AofFoldSnapshot {
    pub dbs: Vec<(Vec<(CompactKey, Entry)>, u32)>,
}
```

Senders: conn handlers use the existing mesh producers (`ctx.dispatch_tx`);
the aof-writer threads get dedicated external mesh producers — the SAME
mechanism replication's master already uses (`shard_producers`,
replication/master.rs:123–129). Enum-size discipline: boxed where the inline
payload would breach the 256-byte cap (MqCommand); the others fit inline.

### C3 — Global workspace registry (M3)

```rust
// src/shard/shared_databases.rs — ONE process-global registry replaces the
// per-shard Box<[Mutex<…>]> array (slot-0 trick retired).
pub workspace_registry: parking_lot::Mutex<Option<Box<WorkspaceRegistry>>>,
/// Accessor keeps PR #173's no-param signature — call sites unchanged.
pub fn workspace_registry(&self) -> MutexGuard<'_, Option<Box<WorkspaceRegistry>>>;
```

- `workspace_registry` field LEAVES `ShardSlice` + `ShardSliceInit`.
- WS WAL records keep the shard-0 stream via `wal_append(0, …)` (senders are
  Send; unchanged). WAL replay populates the global registry at boot.

### C4 — AOF cooperative-snapshot protocol (M4, replaces fold phases 2–5)

1. Writer phase-1 drain → fsync → ack (unchanged).
2. Writer pushes `AofFold` into its shard's external mesh producer, then
   BLOCKS on the oneshot reply (aof-writer-N is a plain OS thread).
3. Shard event loop processes `AofFold` atomically BETWEEN commands: builds
   the snapshot, replies. The single-threaded loop replaces RwLock mutual
   exclusion.
4. **Exactly-once invariant transfer** (the load-bearing line): every command
   the shard processed before the fold enqueued its AOF append
   (`try_send_append`) before the fold reply was sent — happens-before — so
   the writer's post-reply mid-drain captures ALL pre-snapshot appends into
   the OLD incr; post-snapshot appends land in the NEW incr. The in-guard
   append invariant (rewrite.rs:480–502) becomes the in-event-loop-order
   invariant; that doc comment is rewritten to state this.
5. Phases 6–8 (base write, manifest advance, outcome barrier, ShardDoneGuard)
   unchanged.
- Failure: mesh push fails or the oneshot drops (shard shutdown) →
  `AofError::RewriteFailed` → existing abort path keeps the old generation.
- Known limitation (channel saturation during phase 6) is pre-existing and
  carries over unchanged — explicitly NOT widened by this redesign.

### C5 — Published store-memory atomics (M4)

```rust
// src/shard/shared_databases.rs
pub struct ShardStoreMemory {
    pub vector: AtomicUsize,
    pub text: AtomicUsize,
    pub graph: AtomicUsize,
}
pub store_memory_per_shard: Box<[Arc<ShardStoreMemory>]>,
```

- `ShardSliceInit`/`ShardSlice` gain `store_memory: Arc<ShardStoreMemory>`;
  the shard refreshes it on the existing 100ms persistence/elastic tick.
- Prometheus publisher (metrics_setup.rs:1396–1429), MEMORY DOCTOR
  (server_admin.rs:385–416), and the two persistence_tick.rs leftovers
  (344, 499 → read_memory_sum) read ONLY published atomics — zero lock
  acquisitions (ssm4b code-shape pin). Figures may lag ≤1 tick (observability
  paths; acceptable, documented at the readers).
- `memory_per_shard` (KV) and `elastic_budgets` keep their existing shapes.

### C6 — Residual ShardDatabases (M5)

KEPT: `wal_append_txs`, `num_shards`, `db_count`, `memory_per_shard`,
`elastic_budgets`, `workspace_registry` (C3, global), `store_memory_per_shard`
(C5). DELETED fields: `shards`, `vector_stores`, `text_stores`,
`graph_stores`, `temporal_registries`, `temporal_kv_indexes`,
`workspace_registries`, `durable_queue_registries`, `trigger_registries`,
`kv_write_intents`, `deferred_hnsw_inserts`. DELETED methods: `write_db`,
`read_db`, `all_shard_dbs`, per-shard store/registry accessors,
`aggregate_memory`. `SwapDb` handling moves onto the slice. ~130
`is_initialized()` dual branches collapse to the slice arm; the gate fn may
survive only for tests (ssm5 grep pin).

### Contracted responses for §1 rejects

| Reject | Contracted response |
|---|---|
| uninitialized_shard | startup abort with shard id (C1 assert) before first accept; no wire-reachable with_shard panic |
| slice_reentrancy | RefCell double-borrow panic (existing slice.rs unit pin); hops exit the closure before sending — review + ssm-reject test |
| slice_send_violation | compile error — `PhantomData<Rc<()>>` stays; zero new `unsafe`/`Send` impls (UNSAFE_POLICY) |
| test_weakening | §6 diff gate: frozen-suite assertions byte-identical pre/post |
| wrapper_resurrection | code-shape test (`tests/shardslice_shape.rs` grep pins from ssm5) fails on any reintroduced wrapper/gate |
| borrow_across_await | closures are sync by type; grep pin: no `with_shard` call inside an async block capturing the borrow |

Names follow existing glossary: ShardMessage, ShardSliceInit, init_shard,
with_shard, key_to_shard. New names introduced here: MqCommandPayload,
MqTxnMaterialize, WsDropCleanup, AofFold, AofFoldSnapshot, ShardStoreMemory,
store_memory_per_shard.

Least-sure flag surfaced at freeze:
⚠ [contract] C4's AOF cooperative-snapshot redesign is containable in this
  task — because it moves a durability fold across thread ownership and the
  exactly-once proof rests on a new ordering argument (append enqueued before
  fold-reply ⇒ mid-drain captures it); if wrong: data-loss-grade bug or M4
  splits out, blocking M5 wrapper deletion.
⚠ [spec] The three scouts' cross-thread-consumer inventory is complete
  (~130 gated sites + 3 named consumers, nothing hidden) — because 213 call
  sites were migrated across six earlier waves by different hands; if wrong:
  runtime thread-local panic or silent wrong-shard data surfacing only under
  load.

Status: FROZEN @ v1 — approved by Tin Dang 2026-06-12 (both ⚠ flags accepted:
AOF cooperative-snapshot containable in-task; scout inventory complete).
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every scenario ssm1–ssm7 + 6 rejects has exactly one test; the
frozen suites (cdg, consistency sweep) are reused untouched as ssm2's oracle.
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - test_ssm1_slice_live_at_startup (`tests/shardslice_live.rs`, NEW): spawn a
    real 4-shard server (MOON_BIN pinned, fresh --dir), assert one
    "ShardSlice initialized" log line per shard BEFORE the port accepts, then
    PING/SET/GET from fresh conns. Runs under both runtimes (feature matrix).
  - test_ssm2_owner_routing: REUSED oracles — cdg6 a–g
    (tests/cross_shard_consistency_red.rs, byte-identical) + consistency
    sweep 197/197 at 1/4/12. Red today in the sense that they pass via the
    LOCK path; post-cutover they must pass via the slice path — the
    cutover-specific red signal is ssm5's shape test.
  - test_ssm3_ws_global_registry (`tests/shardslice_live.rs`): WS CREATE →
    AUTH/LIST from 12 fresh conns + bound/unbound key visibility (GREEN
    oracle). Restart half split into its own RED test (below). Shape half
    (no workspace_registry field in slice) lives in the shape test.
  - test_ssm3_ws_registry_survives_restart (`tests/shardslice_live.rs`, RED):
    WS CREATE with appendonly yes → restart same dir → WS LIST must still
    show it. RED for a CONFIRMED pre-existing bug found while writing the
    suite: replay_workspace_wal (shared_databases.rs:289) scans shard-{id}/
    but WAL v3 segments live in shard-{id}/wal-v3/ (recovery.rs:361) — zero
    workspace records ever replay. Build fixes it under frozen C3 ("WAL
    replay populates the global registry at boot").
  - test_ssm4a_aof_fold_cooperative (`tests/shardslice_live.rs`): appendonly
    yes, 2000 keys, BGREWRITEAOF under a concurrent INCR writer, exactly-once
    (ctr == recorded successful-INCR count), restart survival. Two variants
    of one helper: shards=1 (default-gated path) and shards=4 +
    --experimental-per-shard-rewrite (test_ssm4a_fold_4shard_experimental —
    drives do_rewrite_per_shard, the EXACT fold C4 redesigns). Both GREEN
    oracles that must stay green through the cutover.
  - test_ssm4b_observer_lockfree (`tests/shardslice_shape.rs`): grep pins —
    metrics_setup/server_admin/persistence_tick contain no
    read_db/write_db/all_shard_dbs/store-lock calls; runtime probe: scrape +
    MEMORY DOCTOR return non-zero figures under load.
  - test_ssm5_wrappers_gone (`tests/shardslice_shape.rs`, NEW — the PRIMARY
    RED TEST): asserts ShardDatabases source has zero
    RwLock<Database>/Mutex<VectorStore>/Mutex<TextStore>/RwLock<GraphStore>/
    per-shard registry Mutex fields, zero `is_initialized()` gates in
    src/server+src/command+src/shard production paths, and uring_handler
    uses with_shard not write_db. RED today on every assertion.
  - test_ssm6_full_matrix: not a test file — the §6 verify checklist (lib
    tests both runtimes macOS+VM, integration MOON_BIN pinned, loom, fmt,
    clippy both feature sets, sweep ×3).
  - test_ssm7_perf_bar: not a test file — bench-swf cell-by-cell vs main on
    idle VM, recorded in §6.
  - test_reject_uninitialized_shard (`tests/shardslice_live.rs` + unit):
    event-loop entry without init aborts at startup naming the shard id
    (unit-level: the assert fn; the wire-level absence is covered by ssm1).
  - test_reject_slice_reentrancy: existing slice.rs double-borrow unit pin
    stays byte-identical + grep pin: no nested with_shard in src.
  - test_reject_slice_send_violation: compile-fail doc-test on ShardSlice
    (already in slice.rs — kept) + audit-unsafe.sh unchanged (zero new
    unsafe).
  - test_reject_test_weakening: §6 diff gate — git diff of frozen suites
    empty.
  - test_reject_wrapper_resurrection: same shape test as ssm5 (it IS the
    permanent guard).
  - test_reject_borrow_across_await (`tests/shardslice_shape.rs`): grep pin —
    no `.await` inside any with_shard/with_shard_db closure body.
</test_plan>

Tests live in: `tests/shardslice_shape.rs` · `tests/shardslice_live.rs` ·
reused frozen oracles in `tests/cross_shard_consistency_red.rs` +
`scripts/test-consistency.sh` (byte-identical). MUST run red
(shape test fails on every wrapper-field assertion; live tests fail on the
missing init log) before Build.

RED-SUITE RECORD (verified 2026-06-12, macOS, MOON_BIN=target/debug/moon):
- shape: 4 RED / 1 GREEN — test_ssm5_wrappers_gone +
  test_reject_wrapper_resurrection (RwLock<Database> at
  shared_databases.rs:24,747 etc.), test_ssm4b_observer_lockfree
  (metrics_setup.rs:1411, server_admin.rs:398, persistence_tick.rs ×7),
  test_ssm3_shape_no_ws_field_in_slice (slice.rs:87,128,154);
  test_reject_borrow_across_await GREEN.
- live: 2 RED / 4 GREEN — test_ssm1_slice_live_at_startup (no "ShardSlice
  initialized" marker), test_ssm3_ws_registry_survives_restart (wal-v3
  replay-dir bug); GREEN: ssm3 cross-conn oracle, ssm4a ×2 (1-shard +
  4-shard experimental), reject wire guard. 3.2s wall.
- BUILD CONSTRAINT pinned in the shape test: C1's startup assert must be
  `slice::assert_initialized(shard_id)` (helper INSIDE slice.rs) — a raw
  `is_initialized()` call at the event loop would trip the ssm5 pin, which
  stays strict (zero call sites outside slice.rs).
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): AOF fold exactly-once — every acked write
survives restart exactly once; no append may straddle the snapshot boundary
into the wrong incr generation.
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

### BUILD RECORD (2026-06-12, branch feat/shardslice-migration)

Waves (parallel Sonnet subagents, orchestrator-reviewed; commits in order):
- f31f6cd bundle · 1e9dc8d A2 WS-WAL replay fix · 498fc19 A1 dispatch
  variants · 7d2f271 B owner-routing · 4de32a8 E1 gate collapse ·
  ee7da49 fold bounded-drain · 6f32f90 E2 structural cutover (C1+C6) ·
  8b2d87c C4 exact fold boundary + H1 fsync barrier.
- NOTE: seven foreign HYBRID FILTER commits (4e9c03c..e969cf0) from a
  concurrent session are interleaved on this branch + their uncommitted
  edits in tree; left untouched (not this task's work).

Build-phase findings fixed beyond plan (each caught by frozen oracles or
orchestrator review, never by weakening a test):
1. Slice re-entrancy (frozen reject hit live): drain_spsc_shared was
   wrapped in with_shard → BorrowMutError; arms now take flat borrows.
2. AOF fold deadlock: fold channels existed but were never wired in
   main.rs; unwired pools now abort rewrites cleanly.
3. ±1 double-apply (ssm4a flake): cross-shard PipelineBatch[Slotted]
   AOF appends were handler-deferred → escaped pending_aof_count →
   replayed onto a base already containing them. Appends moved into the
   SPSC arms (before response-slot fill).
4. H1 regression from (3): appendfsync=always lost its fsync ack for
   cross-shard writes → new AofWriterPool::fsync_barrier (zero-length
   AppendSync rendezvous, F2-bounded), one per target shard per batch.
5. P0 in (4) caught in orchestrator review: a len=0 framed record would
   be read as corruption by replay_incr_framed and brick boot. Writers +
   rewrite mid-drain now skip the disk write for barriers (fsync+ack
   only); reader skips len=0 defensively. Regression tests added.
6. Pre-existing crash-matrix harness flake (unique_port()+1 SO_REUSEPORT
   collision between the two parallel tests): +1 dropped.
7. Throttle a fix-iteration added to the frozen aof_fold_exactly_once
   writer loop was REVERTED (test-weakening); tight loop passes.
8. E2's test migration missed 29 call sites in 28 tokio-gated integration
   tests (never compiled under default monoio features → invisible to all
   macOS runs). All migrated to the tuple API + slice_init run() arg;
   compile-verified on both feature sets.
9. Slice re-entrancy #2, found by the first-ever post-cutover run of the
   tokio integration suites: the cross-store TXN undo-capture re-entered
   with_shard from inside the write closure (handler_sharded via
   with_shard_db; handler_monoio nested inside its own with_shard) →
   BorrowMutError panic on any TXN write. Fixed with disjoint NLL field
   borrows (s.kv_write_intents directly); handler_sharded's do_write now
   takes the whole ShardSlice. txn_kv_wiring 12/12 under tokio
   (MOON_BIN pinned to the Linux binary per the Mach-O host-proxy trap).

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — EVIDENCE (2026-06-12, eb5d664):
  - frozen oracles: shape 5/5 · live 6/6 serial (ssm1 marker+ids, ssm3 ×2,
    ssm4a 1-shard + 4-shard experimental, wire guard) · ssm4a 10/10 serial
    repeat at full INCR rate · cdg 7/7
  - lib: 3594 (monoio, macOS) · 2955+ (tokio; full VM integration sweep
    all targets green, MOON_BIN=target-linux/debug/moon)
  - crash_matrix_per_shard_bgrewriteaof 2/2 ×5 parallel + ×3 serial
  - loom_response_slot 4/4
  - clippy -D warnings: macOS ×2 featuresets + VM tokio (incl. the
    never-before-compiled cfg(linux+tokio) uring_handler) · fmt clean
  - consistency sweep: 197/197 PASSED @ shards=1/4/12 (VM-local clone of
    eb5d664, release build, 2026-06-12 23:49 — incl. TXN/GRAPH/FT/WS/MQ
    cross-shard groups)
- [x] coverage did not decrease — red suite all flipped green; +4 new
  regression tests (len-0 barrier reader/drain, H1-BARRIER ×3 in pool.rs)
- [x] no test or contract was altered during build — §3 untouched since
  freeze; one frozen-test VIOLATION (agent throttle in aof_fold_exactly_once)
  was caught and REVERTED, tight loop green; crash-matrix port fix is
  harness isolation (strengthens), not weakening
- [x] concurrency / timing of the risky operation is safe — C4 exactly-once
  verified 10/10 serial under saturating INCR load; slice re-entrancy guard
  caught both nesting defects (event-loop drain, TXN intent capture), both
  fixed with flat/disjoint borrows; no lock held across .await
  (shape test test_reject_borrow_across_await green)
- [x] no exposed secrets, injection openings, or unexpected dependencies —
  no new deps; no new unsafe (PhantomData<Rc<()>> design holds);
  parser paths untouched except replay_incr_framed len=0 skip (defensive,
  fuzz targets unaffected — no new decode surface)
- [x] layering & dependencies follow CONVENTIONS.md — slice access only via
  with_shard/with_shard_db; is_initialized confined to slice.rs (shape pin)
- [x] a person reviewed and approved the change — Tin Dang, 2026-06-13
  (gate: RISK-ACCEPTED + follow-up; PR shape: clean cherry-picked branch;
  PR creation approved)

### M7 bench-swf evidence (2026-06-13, VM idle load<1, REQS=200k, fresh
### server per rep ×3, eb5d664 vs main 3e376a1, both VM-local release)
- s1/P1: SET 305k vs 294k · GET 316k vs 306k — parity (slice ≥ main)
- s1/P16: SET 1.48M vs 1.61M (−5..8%, borderline vs noise) · GET 2.64M
  vs 2.64M — parity
- s4 vs main ROUTED (--cross-shard-fast-path off; architecture-fair):
  P1 SET 196k vs 187k · P1 GET 187k vs 186k · P16 SET 1.77M vs 1.81M ·
  P16 GET 1.99M vs 1.78M (+12%) · c1 SET 33.5k vs 33.7k — parity or
  slice ahead in every routed cell
- s4 vs main DEFAULT (fast path ON — user-visible): read cells regress
  by mechanism: c1 GET ~190k → ~26-34k (−85%; 4µs lock-read → ~47µs SPSC
  round trip) · P1 GET ~224k → ~187k (−16%). The cross-thread RwLock
  read_db fast path is definitionally incompatible with shared-nothing
  (deleting those locks IS this task). This materializes the ⚠ flag
  accepted at freeze; strictly exceeds the ssm7 "no cell beyond noise"
  bar → escalated to the human gate rather than auto-passed.
  Mitigations: {hash-tag} co-location (zero hops), --shards 1 guidance
  (already CLAUDE.md), follow-up task candidate: slice-native cross-shard
  read acceleration (read batching / snapshot reads) without locks.
- Methodology note: the first back-to-back run (both binaries, all cells
  sequentially against long-lived servers) showed phantom −20..−40% on
  s4 pipelined cells; fresh-server-per-rep ×3 showed parity. Recorded so
  future benches use the per-rep protocol.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — fsync_barrier: called from both handlers' cross-shard
  batch paths (handler_sharded/mod.rs:1667, handler_monoio/mod.rs:1828),
  policy-gated in pool.rs:313; fold channels: create_aof_fold_channels (mesh) → main.rs
  wiring → pool.set_fold_channels → spsc AofFold arm → rewrite recv;
  slice_init: main.rs/embedded.rs destructure → Shard::run → init_shard
  (28 test harnesses migrated to same shape)
- [x] DEAD-CODE (code) — uring_handler shard_id underscored w/ comment;
  E2 removed Cold variant + wrapper methods wholesale; clippy -D warnings
  clean on all three featureset×OS combos confirms no orphans
- [x] SEMANTIC (prose / non-code) — fsync_barrier doc corrected during
  review (claimed "reader skips len=0" before the writer-side skip existed;
  doc now matches the implemented mechanism)

### GATE RECORD
Outcome: RISK-ACCEPTED
Risk: cross-shard read fast-path cells regress vs main's default config
(single-client cross-shard GET 4µs → ~47µs / −85%; P1 GET −16%) — the
cross-thread RwLock read_db path is definitionally incompatible with
shared-nothing. All routed-vs-routed cells parity or better; s1 parity.
Non-security. Materializes the ⚠ flag accepted at the §3 freeze.
If RISK-ACCEPTED -> owner: Tin Dang · ticket: follow-up task
"cross-shard read acceleration (lock-free)" to be opened at observe ·
expires: next perf milestone (M7 successor)
Reviewed by: Tin Dang (gate decision via session, "RISK-ACCEPTED +
follow-up") · date: 2026-06-13

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
