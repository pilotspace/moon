# MILESTONE: Shared-Nothing Integrity & Cross-Shard Latency

goal: a 4-shard Moon serves cross-shard traffic without per-command global locks and without the 1ms monoio reply floor, with measured multi-shard scaling improvement over the v0.3.0 baseline
rationale: intake bucket `new-major` — the user asked to "enhance Moon architecture following the 2026-06 architecture review priorities"; no milestone exists yet, and the review's top-leverage themes (priorities 1 · 2 · 5: ShardSlice completion, monoio wake floor, lock quick-wins) form one coherent outcome: restore the shared-nothing model the architecture claims.
stage: production · status: active · created: 2026-06-11

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:  review priority 5 (lock/syscall quick-wins batch) · priority 2 (eventfd cross-thread wake + drain-until-empty, removing the 1ms cross-shard reply floor) · priority 1 (complete the ShardSlice migration; delete the Arc<ShardDatabases> cross-shard read path and the dual `is_initialized()` branches)
Out: FT.SEARCH off-event-loop execution (review priority 3 → v2) · WAL group commit / the 11× appendfsync=always collapse (priority 4 → v2) · I/O-path copy elimination & PBUF_RING (review theme 3 → v2) · correctness-flavored debt (AOF rewrite drop window, MVCC delete_lsn, 16-bit LRU clock → tracked separately) · any new command or protocol surface · SortedSet/CompactValue data-structure redesign (theme 6)

## Shared decisions & glossary deltas   (living — every task must honor these)
- "shared-nothing per shard" (GLOSSARY: shard, SPSC mesh): cross-shard state changes travel as ShardMessage only; a task may not add a new cross-thread lock to fix an old one.
- "hot path" rule holds throughout: no new allocation or lock on command dispatch / parse / event loop / io drivers.
- Behavior parity is frozen: RESP responses, INFO fields, and `scripts/test-consistency.sh` (132 tests × 1/4/12 shards) must be byte-identical before/after every task.
- Benchmark evidence comes from the OrbStack `moon-dev` Linux VM only (CLAUDE.md rule); each task records before/after numbers from `scripts/bench-compare.sh`.
- Dual-runtime: every change compiles and passes tests under `runtime-monoio` AND `runtime-tokio,jemalloc`.

## Shared / risky contracts (freeze these first)
- `ShardSlice` as the ONLY per-shard state access path (the post-migration shape: no `Arc<ShardDatabases>` reads cross-shard, no dual branches) -> owning task `shardslice-migration`
- eventfd wake protocol between shards (who signals, when, idempotency under coalescing) -> owning task `spsc-wake-floor`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] hotpath-lock-quickwins   depends-on: none                    — remove per-command global locks/atomics (OnceLock WAL sender, lock-free repl offsets, per-shard metrics counters, backlog bulk-append, VecDeque inflight sends, key_hash prune) + TCP_NODELAY on accept; establishes the measurement baseline
- [x] spsc-wake-floor          depends-on: hotpath-lock-quickwins  — eventfd-based cross-thread wake for monoio replies + SPSC drain-until-empty; removes the ~1ms cross-shard latency floor (gate PASS 2026-06-11; p99 4.071ms → 0.071ms)
- [ ] shardslice-migration     depends-on: hotpath-lock-quickwins  — complete the ShardSlice migration: route remaining cross-shard reads through SPSC, delete Arc<ShardDatabases> foreign-thread access and every `is_initialized()` dual branch

## Exit criteria (observable; map each to the task that delivers it)
- [x] Lock inventory: zero global lock acquisitions on the per-command write path for metrics, replication offsets, WAL sender, client registry batch-update — verified by a recorded audit + tests (TASK.md §6, 2026-06-11)        (← hotpath-lock-quickwins)
- [x] `scripts/bench-compare.sh` on moon-dev shows no regression at 1 shard and measured improvement at 4 shards vs the recorded v0.3.0 baseline        (← hotpath-lock-quickwins · spsc-wake-floor · shardslice-migration)
      MET 2026-06-13 (slice eb5d664 vs v0.3.0 3e376a1, VM idle, REQS=200k, fresh-server-per-rep ×3): s1 parity (SET/GET ≥ main); s4 ROUTED parity-or-better every cell, P16 GET +12%. The default-config cross-shard read regression (c1 GET −85%, P1 GET −16%) is the signed RISK-ACCEPTED tradeoff (shardslice gate, Tin Dang 2026-06-13) — deleting the cross-thread RwLock IS the task; follow-up ticket "lock-free cross-shard read acceleration". (shardslice-migration §6 M7 evidence)
- [x] Cross-shard single-key SET/GET p99 on monoio drops below 1ms (the old floor) in a recorded latency run — 0.071 ms, 57× under the bar (.add/tasks/spsc-wake-floor/bench-results.txt, 2026-06-11)        (← spsc-wake-floor)
- [x] `grep -r "is_initialized()" src/shard/` returns zero dual-path branches; `ShardDatabases` no longer exposes cross-shard read access        (← shardslice-migration)
      MET 2026-06-13: `is_initialized()` confined to slice.rs (definition + single guarded helper + tests); zero production dual-branch gates. Machine-enforced by `tests/shardslice_shape.rs::assert_wrappers_gone`. No public cross-shard read accessor on ShardDatabases (C6 residual contract).
- [x] `scripts/test-consistency.sh` passes 132/132 on 1/4/12 shards and full CI matrix is green after each task        (← all three)
      MET 2026-06-13: suite grew to 197 via consistency-dispatch-gaps (the dedicated fix task for the 15 MAIN-baseline failures noted below); 197/197 PASSED @ shards=1/4/12 (VM-local release, shardslice eb5d664). CI matrix green per task: clippy -D warnings ×3 featureset×OS + fmt clean.
      ⚠ Gap discovered 2026-06-11: MAIN itself fails 15 of these (dispatch_read gaps for
      GEO*/EXPIRETIME/PEXPIRETIME/TOUCH + two script bugs + a Phase-152 early-exit), so this
      criterion needs a dedicated fix task; tasks 1–2 verified zero NEW failures vs main
      instead (identical branch-vs-main A/B). CI matrix green per task: holding so far.
