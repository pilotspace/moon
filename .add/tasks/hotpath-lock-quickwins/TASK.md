# TASK: Eliminate per-command global locks & syscall-level quick wins

slug: hotpath-lock-quickwins · created: 2026-06-11 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: hot-path lock & syscall quick-wins (2026-06 architecture review, priority 5 batch — 7 independent mechanical fixes, each behavior-preserving)
Framings weighed: one batch task with per-item tests (chosen — items are tiny, share one bench baseline, and one PR keeps review cost low) · seven separate tasks (rejected: ceremony exceeds work) · fold into shardslice-migration (rejected: these are independent of the migration and de-noise its measurements)
Must:
<must>
  - QW1 TCP_NODELAY: every accepted client socket (tokio accept path AND uring register path) has TCP_NODELAY set; a connection-level test observes nodelay on the accepted fd.
  - QW2 WAL sender lock-free: `ShardDatabases::wal_append` / `try_wal_append_required` reach the per-shard sender without acquiring a Mutex (OnceLock or equivalent set-before-accept init); WAL durability semantics of `try_wal_append_required` unchanged (src/shard/shared_databases.rs:159).
  - QW3 replication offsets lock-free: per-write offset increment (`increment_shard_offset` / `master_repl_offset`) is reachable without read-locking `RwLock<ReplicationState>` — offsets distributed as Arc'd atomics at startup (src/shard/spsc_handler.rs:3089, src/replication/state.rs:138).
  - QW4 per-shard command counters: `TOTAL_COMMANDS` (and the per-batch `CONNECTED_CLIENTS` touch) become per-shard/per-thread counters aggregated on read; INFO `total_commands_processed` remains exact (src/admin/metrics_setup.rs:401).
  - QW5 backlog bulk append: `ReplicationBacklog::append` uses bulk copy (extend/copy_from_slice with wraparound), not a per-byte loop; eviction-at-capacity and offset bookkeeping byte-identical to today (src/replication/backlog.rs:35).
  - QW6 inflight sends O(1): the uring per-connection `inflight_sends` reclaims completed sends with O(1) pop_front (VecDeque or ring) preserving completion order (src/shard/uring_handler.rs:346).
  - QW7 vector key-map pruning: deleting an indexed vector removes its entries from `key_hash_to_key` AND `key_hash_to_global_id`; map size tracks live-key count, not historical insert count (src/vector/store.rs:192).
  - QW8 client registry off the batch path: the per-pipeline-batch `client_registry::update` global write-lock acquisition is removed from the steady-state loop (per-connection atomics or update-on-change); CLIENT LIST / CLIENT KILL behavior unchanged (src/client_registry.rs:80, handler_sharded/mod.rs:1868).
</must>
Reject:
<reject>
  - any RESP response byte-change, INFO field removal, or test-consistency diff -> "behavior_regression" (the batch is perf-only; parity is frozen)
  - a fix that adds a new cross-thread lock, new hot-path allocation, or new unsafe block -> "violates_conventions"
  - QW2: WAL append silently succeeding when persistence is configured but channel rejects -> "durability_contract_broken"
</reject>
After:
<after>
  - Zero global lock acquisitions remain on the per-command write path for: WAL sender, replication offsets, command counters, client-registry batch update (auditable by grep + the new tests).
  - A recorded moon-dev bench baseline (bench-compare, 1 + 4 shards) exists from BEFORE the batch, and the after-run shows no 1-shard regression.
  - Both runtimes compile and pass: default (monoio) and `--no-default-features --features runtime-tokio,jemalloc`.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ QW4 assumes no external consumer scrapes `moon_commands_total` at sub-second freshness — per-shard counters aggregate on INFO/scrape read, so instantaneous cross-shard sums may lag by one read. Lowest confidence because the Prometheus exporter path (`counter!` macro) may not support per-shard registration cleanly; if wrong: keep the `metrics` crate counter as-is and only fix the bespoke `TOTAL_COMMANDS` atomic (smaller win, ~zero risk).
  ⚠ QW8 assumes CLIENT LIST tolerates last-command metadata updated on-change rather than per-batch — lowest confidence because redis-cli tooling may read `cmd`/`age` fields expecting freshness; if wrong: per-connection atomics (still lock-free) instead of dropping the update.
  - [ ] QW1: monoio TcpStream exposes set_nodelay (or we set it via socket2 on the raw fd) on both runtimes — confirm at build.
  - [ ] QW6: uring send completions arrive in submission order per connection (current Vec::remove(0) already assumes this) — confirm with existing pipeline tests.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first. -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: nodelay set on accepted connection (QW1)
  Given a moon server listening on an ephemeral port (each runtime)
  When a client connects
  Then the accepted server-side socket reports TCP_NODELAY = true
  And the connection serves PING/GET/SET normally

Scenario: WAL append works lock-free after init (QW2)
  Given persistence configured and the WAL sender initialized before accept
  When 10 000 SET commands run across 4 threads
  Then every append is delivered to the WAL channel (or backpressure reported via try_wal_append_required = false)
  And no Mutex is acquired in wal_append (type-level: the field is OnceLock/immutable after init)

Scenario: WAL required-append still gates mutation (QW2 reject: durability_contract_broken)
  Given persistence configured and the WAL channel full
  When try_wal_append_required is called
  Then it returns false
  And the caller-visible contract (skip mutation) is unchanged

Scenario: replication offsets advance without ReplicationState lock (QW3)
  Given a replica attached and writes flowing on every shard
  When 1 000 writes execute per shard concurrently with a REPLICAOF state read
  Then per-shard offsets and master_repl_offset equal the exact byte totals
  And no write path blocks on the ReplicationState RwLock (offsets reachable via Arc'd atomics)

Scenario: INFO command count exact under concurrency (QW4)
  Given a fresh server
  When exactly N commands execute spread across 4 shards
  Then INFO total_commands_processed reports ≥ N for executed commands with no double-count (exact for the bespoke counter)
  And the counter increment touches only shard-local state on the hot path

Scenario: backlog bulk append byte-identical (QW5)
  Given a ReplicationBacklog of capacity C
  When appends of sizes [1, C/2, C, C+7, 3C] are applied in sequence
  Then buffer contents, start_offset, and end_offset equal the old per-byte implementation for every step (golden test)
  And get_range results match for all valid offsets

Scenario: inflight send reclaim preserves order (QW6)
  Given a connection with 1 024 pipelined responses in flight
  When SendComplete CQEs arrive in submission order
  Then buffers are reclaimed FIFO with O(1) pop (VecDeque)
  And all 1 024 responses reach the client intact (existing pipeline integration test stays green)

Scenario: vector key maps shrink on delete (QW7)
  Given an FT index with 100 auto-indexed HSET vectors
  When 60 of the keys are deleted (DEL/HDEL)
  Then key_hash_to_key and key_hash_to_global_id contain exactly 40 entries
  And FT.SEARCH still returns original key names for the surviving 40

Scenario: client registry not written per batch (QW8)
  Given a connection executing 10 000 pipelined PINGs
  When the steady-state loop runs
  Then the global REGISTRY write lock is not acquired per batch (update is on-change/atomic)
  And CLIENT LIST still shows the connection with correct db and name; CLIENT KILL still terminates it

Scenario: behavior parity frozen (Reject: behavior_regression)
  Given the full batch applied
  When scripts/test-consistency.sh runs at 1/4/12 shards and CI matrix runs both runtimes
  Then 132/132 consistency tests pass and CI is green
  And bench-compare at 1 shard shows no regression beyond run noise
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
No new wire surface — the contract is invariants over existing surfaces:

WIRE (frozen, byte-parity): all RESP responses · INFO fields (total_commands_processed,
  connected_clients, master_repl_offset) · CLIENT LIST/KILL semantics · FT.SEARCH key names.

INTERNAL SHAPES (the change):
  ShardDatabases.wal_append_txs : Vec<Mutex<Option<Sender>>>  ->  Vec<OnceLock<Sender>>
      wal_append(shard, bytes)            : no lock; no-op when unset
      try_wal_append_required(...) -> bool: false ONLY when set && channel-full   (unchanged)
  ReplicationState offsets  ->  Arc<ShardOffsets { per_shard: [AtomicU64], master: AtomicU64 }>
      handed to each shard at startup; RwLock<ReplicationState> no longer on the write path
  metrics::record_command*  ->  shard-local counter cell; INFO aggregates with one pass
  ReplicationBacklog::append(&[u8])       : bulk copy; observable state machine unchanged (golden)
  uring inflight_sends      : Vec -> VecDeque, FIFO reclaim
  VectorStore delete path   : also removes key_hash_to_key + key_hash_to_global_id entries
  client_registry::update   : leaves the per-batch loop; kill_flag stays Arc<AtomicBool>

ERROR CODES (test-visible): behavior_regression · violates_conventions · durability_contract_broken

Schema: no storage format, WAL record, or persistence layout change. Touched modules:
  shard/shared_databases.rs · replication/{state,backlog}.rs · admin/metrics_setup.rs ·
  shard/uring_handler.rs · vector/store.rs · client_registry.rs · server/conn/* accept paths.
Access pattern: all hot-path reads lock-free post-init; init strictly before first accept.
```

Status: FROZEN @ v1 — approved by Tin Dang via baseline lock, 2026-06-11

Least-sure flag surfaced at freeze:
- ⚠ [spec] QW4: the Prometheus `counter!` macro path may resist per-shard registration — because the `metrics` crate recorder owns the counter handles globally; if wrong: the win shrinks to the bespoke `TOTAL_COMMANDS` atomic only (smaller but safe fallback).
- ⚠ [contract] QW8: CLIENT LIST may be expected to show per-batch-fresh `cmd`/`age` metadata — because no test pins its freshness; if wrong: fall back to per-connection atomics (still lock-free), not dropping the update.

<!-- Changing this frozen contract = change request back to SPECIFY. -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must covered by a red test or a declared pin; QW5 golden model; lock-free internals verified at §6 by code audit (tests assert behavior, not internals).
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - tests/quickwins_red.rs (compiles + runs today — confirmed 4 passed / 1 failed):
    - qw7_vector_key_maps_pruned_on_delete — RED (fails: 100 entries vs 40 live; pruning missing)
    - qw5_backlog_append_golden — PIN green (reference per-byte model vs real, offsets + contents + range probes)
    - qw2_try_wal_append_required_false_on_full — PIN green (bounded(1) channel; false-on-full gate)
    - qw3_replication_offsets_exact — PIN green (4 threads × 1000 writes; exact totals)
    - qw4_total_commands_exact — PIN green (4 threads × 5000 record_command; exact delta)
  - tests/quickwins_red_api.rs (COMPILE-RED today — confirmed E0433/E0599, missing symbols):
    - qw1_accepted_socket_has_nodelay — needs `server::socket_opts::apply_client_socket_opts`
    - qw3_offset_handle_advances_without_state_lock — needs `ReplicationState::offset_handle()`
  - QW6 (inflight FIFO) + QW8 (CLIENT LIST/KILL parity): covered by existing pipeline integration tests + scripts/test-consistency.sh (declared pins); "no lock per batch / O(1) pop" are internals → §6 audit.
  - Parity backstop: scripts/test-consistency.sh 1/4/12 shards + both-runtime CI matrix at verify.
</test_plan>

Tests live in: `tests/` `quickwins_red.rs` `quickwins_red_api.rs` · red confirmed 2026-06-11 BEFORE build (qw7 runtime-red; api file compile-red).

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): QW2's `try_wal_append_required` false-on-full contract must hold at every commit — a dropped WAL gate is silent data loss.
Code lives in: `src/` (modules listed in §3)
Constraints: do NOT change any test or the contract; allow-list packages only; no new unsafe; both runtimes green; ask if unclear.

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass (both runtimes) — quickwins suites 7/7 · lib 3566 · tokio+jemalloc matrix 2945, all green 2026-06-11
- [x] coverage did not decrease — 7 new tests + 2 new module test sets added; none removed
- [x] no test or contract was altered during build — tests/quickwins_red*.rs committed (71fd6d3) before impl (ddcb6bc); only `cargo fmt` reformatting touched the test file after
- [x] concurrency / timing safe — QW3: OffsetHandle = same Arc'd atomics, pre-existing two-fetch_add skew documented in issue_lsn docs; QW4: relaxed per-slot monotonic counters, exact sum (no cross-atomic invariant → no state machine → loom not required per CLAUDE.md rule); QW8: independent relaxed atomics, no ordering dependency; QW2: std::sync::OnceLock (set-before-accept, get-only after). Exactness exercised under 4-thread concurrency by qw3/qw4 tests.
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new deps; no new unsafe (the QW1 BorrowedFd block is the pre-existing keepalive SAFETY block, extended)
- [x] layering & dependencies follow CONVENTIONS.md — parking_lot untouched where present; no lock added; no hot-path alloc added
- [ ] a person reviewed and approved the change
- [x] bench before/after recorded on moon-dev (tmp/bench-quickwins-results.txt): 4-shard P=16 SET +22% (930k→1136k), GET +40% (1905k→2667k); 1-shard within run noise (3-run recheck: ranges overlap, quickwins beat baseline in run 2)

### Deep checks — do not skim
- [x] WIRING — apply_client_socket_opts referenced in conn_accept/event_loop/uring_handler/tests; offset_handle in event_loop startup + state.rs + tests; ClientLiveState (touch/is_killed) in both handlers + dispatch + registry formatting; bump_total_commands/total_commands_sum at all 4 increment + 2 read sites (grep-confirmed)
- [x] DEAD-CODE — grep: zero references to old `TOTAL_COMMANDS` static, old `set_tcp_keepalive` call sites, `wal_append_txs[..].lock()`, or `rs.read()` offset path

### GATE RECORD
Outcome: PASS (auto-resolved per autonomy:auto; evidence above; security: none; residue: monoio end-to-end behavior covered by VM consistency run recorded below)
Reviewed by: auto-gate (run: hotpath-lock-quickwins build 2026-06-11) · date: 2026-06-11

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): 4-shard bench delta · INFO counter accuracy in prod runs · WAL backpressure rate
Spec delta for the next loop: the wins concentrate exactly where contention was predicted — 4-shard pipelined (+22% SET / +40% GET) while 1-shard is flat. This confirms the review's model: remaining 4-shard non-pipelined flatness is the SPSC/wake floor, not lock contention → spsc-wake-floor is correctly the next task. Consistency 132-suite ALL PASSED on VM (exit 0) post-batch.

### Competency deltas
- [TDD · folded] For behavior-preserving perf batches, the red suite naturally splits runtime-red + compile-red(api file) + green pins — worth encoding as a pattern in CONVENTIONS (evidence: quickwins_red.rs / quickwins_red_api.rs both did their job). → folded into CONVENTIONS.md (foundation v1, 2026-06-13).
- [ADD · folded] The §3 freeze needs the literal "Least-sure flag surfaced at freeze:" unit or the engine refuses build — template comment alone is not a declaration (evidence: unflagged_freeze error at advance). → folded into CONVENTIONS.md (foundation v1, 2026-06-13).
- [DDD · folded] CLAUDE.md says "registry not on the command hot path" but it WAS written per batch — living docs drift from code; the lock-inventory audit grep should become a CI check (evidence: finding 1.3, fixed by QW8). → folded into PROJECT.md §Domain (foundation v1, 2026-06-13).
