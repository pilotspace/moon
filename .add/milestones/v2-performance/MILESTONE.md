# MILESTONE: Multi-Core Throughput Hardening (v1-deferred perf wins)

goal: a 4-shard Moon measurably improves the three throughput bottlenecks v1 deferred — cross-shard read latency, FT.SEARCH event-loop stalls, and appendfsync=always write collapse — without re-introducing cross-thread locks or growing per-key memory
rationale: intake bucket `new-major` (confirmed Tin Dang 2026-06-13) — v1-shared-nothing is closed, so no active milestone covers these; deliberately BUNDLED (user choice) the three perf themes v1's Out list deferred to "v2" into one coherent outcome: realize the remaining measurable scaling wins. The cross-shard-read theme is scoped to the SAFE, memory-preserving wins (spin-then-park + coalescing + dead-flag cleanup) — the aggressive RCU/ArcSwap snapshot path was weighed and declined to protect moon's low-RSS differentiator (investigation 2026-06-13: snapshot reads = ~2× working-set RAM + O(keys) clone/publish). Discharges the shardslice-migration RISK-ACCEPTED waiver (expires 2026-08-01).
stage: production · status: active · created: 2026-06-13

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:  cross-shard read acceleration WITHOUT locks or memory growth (adaptive spin-then-park on the SPSC reply to cut the round-trip constant; read coalescing for the multi-client P1 case; DELETE the orphaned `--cross-shard-fast-path` flag + `moon_cross_shard_lock_contention_total` metric + stale docstrings, all dead since the shardslice cutover) · FT.SEARCH off-event-loop execution (a heavy vector/text query must not stall the shard's 1ms tick / co-located commands) · WAL group commit (batch concurrent pending writes into one fsync under appendfsync=always to collapse the ~11× write penalty)
Out: RCU/ArcSwap snapshot reads & client-side MOVED/CLUSTER-SLOTS routing (declined this milestone — memory + protocol cost; revisit only if the safe cross-shard wins miss target) · I/O-path copy elimination & PBUF_RING (review theme 3 — pullable if appetite remains) · SortedSet/CompactValue data-structure redesign (theme 6) · correctness-flavored debt (AOF rewrite drop window, MVCC delete_lsn, 16-bit LRU clock — tracked separately) · any new command or protocol surface

## Shared decisions & glossary deltas   (living — every task must honor these)
- Shared-nothing carries forward (GLOSSARY: shard, SPSC mesh): no new cross-thread lock on the command path; cross-shard state travels as ShardMessage only — the v1 invariant, now machine-enforced by `tests/shardslice_shape.rs`. A task may not re-add a lock or a `Send`/`Sync`/`unsafe` slice escape to buy latency.
- LOW-RSS is a first-class constraint this milestone: no task may grow steady-state per-key memory to buy latency. This is the explicit reason snapshot reads are Out — it protects moon's "lower RSS per key than Redis" differentiator (PROJECT.md Key Decisions).
- "hot path" rule holds throughout: no new allocation or lock on command dispatch / parse / event loop / io drivers (CLAUDE.md).
- Behavior parity frozen: RESP responses + `scripts/test-consistency.sh` 197/197 @ shards=1/4/12 byte-identical before/after every task.
- Bench evidence from the OrbStack `moon-dev` Linux VM only, fresh-server-per-rep ×3, per-runtime (the v1 phantom-regression lesson — shardslice §6 methodology note: long-lived servers showed phantom −20..−40% that vanished under per-rep protocol).
  - EXCEPTION (discovered by xshard-read-fastpath spike, 2026-06-13): the ABSOLUTE single-client cross-shard read latency is NOT reliably measurable on the OrbStack VM — host→guest vCPU starvation floors the 1-min load ~1.5 with shards parked, inflating cross-shard latency 36µs→580µs while the eventfd wake count stays correct (INFO `spsc_notify_wakes` ratio 1.000). RESOLUTION (2026-06-13): GCloud bare-metal was BLOCKED (no open billing account on the GCloud org), so cross-shard latency wins are gated on a RELATIVE target instead — measured as a best-of-N RPS ratio on the SAME quiesced, core-pinned VM before AND after the change (harness scripts/baseline-xshard-quiesced.sh). Contention only ADDS latency, so best-of-N is the floor and fixed VM/virtiofs overhead cancels in the pre/post ratio; the absolute µs stays untrusted. GCloud bare-metal absolute validation is deferred/optional (revisit only if billing reopens). Throughput-oriented VM numbers (the v1 cells) remain valid as before.
- Dual-runtime: every change compiles and passes under `runtime-monoio` AND `runtime-tokio,jemalloc`.

## Shared / risky contracts (freeze these first)
- WAL group-commit durability contract: the fsync-batching boundary + the exactly-once-under-crash invariant — must NOT weaken the shardslice C4 fold / H1-BARRIER machinery it builds on -> owning task `wal-group-commit`
- FT.SEARCH off-event-loop execution model: how heavy query work yields/offloads WITHOUT cross-thread access to the `!Send` slice and without a `with_shard` borrow held across `.await` -> owning task `ft-search-off-eventloop`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [ ] xshard-read-fastpath     depends-on: none — re-baseline cross-shard read latency per-runtime; recover it the lock-free-safe way via adaptive spin-then-park on the SPSC reply + read coalescing for the P1 multi-client case; delete the dead `--cross-shard-fast-path` flag/metric/docstrings. Target: at least HALVE the c1 GET regression with zero memory growth.
- [ ] ft-search-off-eventloop  depends-on: none — keep a pathological FT.SEARCH (large K / deep HNSW) from stalling concurrent simple commands on the same shard; cooperative-yield or snapshot-handoff execution that respects the !Send slice.
- [ ] wal-group-commit         depends-on: none — batch concurrent pending writes into one fsync under appendfsync=always; close a meaningful fraction of the ~11× throughput penalty with zero data-loss regression.

## Exit criteria (observable; map each to the task that delivers it)
- [ ] Cross-shard c1 GET regression at least HALVED vs v0.3.0 (tag 3e376a1, pre-shared-nothing), measured per-runtime as a best-of-N RPS ratio on the SAME quiesced, core-pinned moon-dev instrument before/after (relative anchor; absolute µs untrusted). M0 anchor ESTABLISHED 2026-06-14 (monoio, clean VM): c1-GET 37580→22252 = −40.8%; target = recover ≥ half the 15.3k-RPS gap → c1-GET ≥ ~30k. `grep` confirms the `--cross-shard-fast-path` flag + `moon_cross_shard_lock_contention_total` metric are GONE; consistency 197/197 @1/4/12 unchanged; RSS not regressed; s4-c100-GET guard within noise of ~202k        (← xshard-read-fastpath)
- [ ] During a heavy FT.SEARCH on a shard, p99 of a simple command (PING/GET) on that same shard stays under a recorded bound; FT.SEARCH recall/correctness unchanged vs current        (← ft-search-off-eventloop)
- [ ] appendfsync=always write throughput improves measurably at pipeline depth >1 with N concurrent writers (recorded before/after); crash-matrix green + exactly-once preserved (no data-loss regression)        (← wal-group-commit)
- [ ] Cross-cutting per task: dual-runtime green, `clippy -D warnings` ×2 featuresets + `fmt` clean, zero new `unsafe`, zero new cross-thread lock        (← all three)
