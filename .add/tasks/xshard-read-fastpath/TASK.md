# TASK: Recover cross-shard read latency lock-free: adaptive spin-then-park + coalescing + dead-flag cleanup

slug: xshard-read-fastpath · created: 2026-06-13 · stage: production · risk: high · autonomy: conservative
phase: tests   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Recover the cross-shard single-key READ latency the shardslice-migration
gave up, WITHOUT re-introducing a cross-thread lock or growing per-key memory.
Discharges the shardslice RISK-ACCEPTED waiver (default-config c1 GET −85%, P1
GET −16%; expires 2026-08-01). Three deliverables in one task: (D1) an adaptive,
IDLE-GATED reply-side spin-then-park on the cross-shard read reply — spin ONLY
when the requesting shard has no other ready connections (this gate is the
mechanism that wins at c1 without the c100 starvation the spike measured); the
spin removes the REPLY-side cross-thread wake (one of the round-trip's two
wakes → ceiling ≈ HALVE the gap); (D2) cross-connection read coalescing — combine
multiple in-flight foreign reads to the same target shard into fewer SPSC hops;
(D3) hard-delete the
orphaned `--cross-shard-fast-path` flag + its dead metric/histograms/functions
(zero production callers since the wave-e2 cutover — `cross_read_fast_dispatches`
is hardcoded 0, handler_sharded/mod.rs:384; `cross_shard_fast_path_enabled`
config.rs:760 has no consumers; the lock-read path it gated is shape-test-forbidden).

Ground truth (investigation 2026-06-13, file:line evidenced): the foreign GET is
SPSC `Execute`/`PipelineBatch[Slotted]` → cross-thread wake → owner drains +
`with_shard` execute → reply (oneshot / ResponseSlot `AtomicWaker`) → wake back
(coordinator.rs:960+, spsc_handler.rs:284, response_slot.rs:83, handler_sharded:1636
/ handler_monoio:1748). The per-shard storage (DashTable/Database) is a plain
`&mut self` structure with NO interior concurrency — single-thread `!Send` ownership
IS the safety; a foreign lock-free read is storage-impossible in place, so SPSC is
the only door and we make IT cheaper, not bypass it. PipelineBatchSlotted already
coalesces a single connection's pipeline; D2's new ground is CROSS-connection
batching on the originating shard. The requesting connection task runs ON the
originating shard's single event-loop thread — so the "spin" cannot be an unbounded
busy-loop (it would starve co-located connections + SPSC drain on that thread).

Framings weighed: ADAPTIVE idle-gated spin + cross-connection coalescing + cleanup
(CHOSEN — user decision 2026-06-13 after the spike) · fixed bounded spin (TRIED in
the spike, REJECTED — unconditional spin=4096 cut c100 throughput −48%, materializing
`shard_starvation`) · "diagnose a wake bug" (INVESTIGATED, REFUTED — INFO
`spsc_notify_wakes` ratio 1.000: the eventfd wake fires every op; the latency is
architectural cross-thread wake+reschedule cost, not a lost-wake bug) · RCU/ArcSwap
snapshot reads (the only path to ~4µs, rejected at milestone scope — memory vs
low-RSS) · hard-remove the dead flag (CHOSEN — breaking, cleanest) vs deprecate
(declined). The spike proved the OrbStack VM is an invalid instrument for ABSOLUTE
cross-shard latency under host vCPU contention; GCloud bare-metal was then BLOCKED
(no open billing account, 2026-06-13) → the "halve" target is measured as a RELATIVE
ratio on the SAME quiesced, core-pinned VM (best-of-N = latency floor; fixed VM
overhead cancels in the pre/post ratio). GCloud absolute validation is deferred/optional.

Must:
<must>
  - M0 (baseline, quiesced-VM RELATIVE anchor): re-establish the "before" numbers
    on moon-dev with the SAME pinned instrument that will measure the M1 result —
    moon shards pinned to cores 0-3, redis-benchmark to 4-5, fresh-server-per-rep,
    best-of-N RPS (= latency floor) counted only when the 1-min load is quiesced
    (<0.7). The absolute µs is NOT trusted (the spike proved host→guest vCPU
    starvation inflates it 36µs→580µs); the RATIO is, because contention only ADDS
    latency and any fixed VM/virtiofs overhead cancels pre/post. Measures BOTH
    commits — 3e376a1 (pre-regression, had the lock read fast-path) vs current main
    (post-shardslice SPSC) — per-runtime. "Halve the gap" is anchored to THIS relative
    baseline (harness: scripts/baseline-xshard-quiesced.sh). GCloud bare-metal absolute
    validation is deferred/optional (revisit only if billing reopens).
    ESTABLISHED 2026-06-14 (monoio, clean-VM best-of-5; full table in §6 M0 RECORD):
    s4-c1-GET 37580 (3e376a1) → 22252 (HEAD a497602) = −40.8% ← the gap to halve.
    Controls: s1 LOCAL c1-GET ~40k on BOTH (regression is cross-shard-specific, not a
    VM/binary artifact); s4-c100-GET 286k→202k (the starvation-guard baseline);
    s4-c1-SET 564→21720 (v0.3.0 cross-shard WRITES were the slow side — shared-nothing
    improved writes 38×; confirms this task is reads-only). NOTE the live regression is
    −40.8%, NOT the historical −85% (current main already recovered post-§6 via PR #173).
  - M1 (c1 latency, the primary win): a single-client (c=1, P=1) cross-shard GET
    recovers at least HALF the 15.3k-RPS gap on the same pinned instrument —
    best-of-N c1-GET ≥ ~30k RPS (from 22.3k), i.e. regression-vs-3e376a1 ≤ ~20%,
    achieved by an ADAPTIVE IDLE-GATED reply-side poll — the requesting connection
    polls the reply (skipping the reply-side cross-thread wake) ONLY when its shard
    has no other ready connection/SPSC work to serve; when the shard is busy it
    parks immediately (no poll). The gate is checked from cheap shard-local state
    (ready-queue/connection count), never a lock. Ceiling is ~half because only the
    reply-side wake is removed; the target-side wake is irreducible.
  - M2 (P1 multi-client): under c>1, P=1 cross-shard read load, multiple in-flight
    foreign reads from different connections to the SAME target shard are coalesced
    into fewer SPSC messages; the s4 P1 GET cell recovers measurably toward parity
    vs the M0 baseline. Coalescing preserves per-connection command order and
    read-your-writes exactly as the lock-free path does today.
  - M3 (cleanup, hard-remove): `--cross-shard-fast-path` (the `cross_shard_fast_path`
    field + `CrossShardFastPath` enum + `cross_shard_fast_path_enabled`), the
    `moon_cross_shard_lock_contention_total` metric, the
    `moon_dispatch_cross_read_fastpath_latency_us` histogram, the
    `record_dispatch_cross_read_fastpath_*` fns, and the hardcoded
    `cross_read_fast_dispatches` are DELETED; a project-wide grep returns zero
    production references; their stale docstrings (config.rs:603–613,
    handler_sharded:60-area "Arc<ShardDatabases>… RwLock" comment) are removed or
    corrected. The breaking removal is recorded in CHANGELOG/release notes.
  - M4 (no-regression guardrail): every other bench cell — s1 P1/P16, s4 routed,
    s4 P16, and the ENTIRE tokio matrix — shows no regression beyond run-to-run
    noise; `scripts/test-consistency.sh` holds 197/197 at shards=1/4/12; both CI
    feature sets `clippy -D warnings` + `fmt` clean; zero new `unsafe`; zero new
    cross-thread lock; RSS not regressed vs baseline.
</must>
Reject:
<reject>
  - A new cross-thread lock, `Send`/`Sync` impl, or `unsafe` slice escape added to
    accelerate reads -> forbidden; the SPSC hop stays the only cross-shard door
    ("lock_reintroduced").
  - A `with_shard`/`with_shard_db` borrow held across an `.await` introduced by the
    spin/park or coalescing logic -> forbidden (same rule as locks across await)
    ("borrow_across_await").
  - The poll runs when the requesting shard has OTHER ready work, stalling
    co-located connections / SPSC drain -> forbidden ("shard_starvation"). GUARD:
    s4 c100 GET must show NO throughput regression vs spin-disabled (the spike's
    fixed spin=4096 cut it −48%; the idle-gate exists precisely to prevent this —
    a high-concurrency shard is never idle, so it never polls).
  - Coalescing reorders or batches such that a cross-shard read returns staler data
    than the current lock-free path returns for the same client sequence (esp. a
    read after a same-connection acknowledged write to that key) -> forbidden
    ("read_your_writes_regression").
  - Steady-state RSS grows to buy latency (a per-key snapshot, a retained buffer
    that scales with keyspace) -> forbidden; the low-RSS invariant is the milestone
    constraint ("memory_regression").
  - Any residual reference to the removed flag/enum/metric/histogram/fns remains in
    production code after M3 -> the cleanup is complete, not partial ("removed_flag_referenced").
</reject>
After:
<after>
  - Cross-shard c1 GET median+p99 latency on monoio is at least halved vs the M0
    baseline; the s4 P1 multi-client GET cell recovers toward parity; every other
    cell (s1, routed, P16, all tokio) is unregressed.
  - The orphaned cross-shard-fast-path flag/enum/metric/histogram/functions are
    gone; `--cross-shard-fast-path` is no longer a valid argument (clap rejects it;
    documented breaking change).
  - No new lock, no new unsafe, no RSS growth; consistency 197/197 @1/4/12 holds;
    dual-runtime green.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ The idle-gate cleanly separates the two regimes — i.e. (a) "this shard has no
    other ready work" is checkable from cheap shard-local state on the hot path, and
    (b) gating on it actually threads the needle (c1 polls and ~halves; c100 never
    polls and does not regress). Lowest confidence because the spike proved the
    extremes (fixed spin halves nothing reliably AND starves c100 −48%), but the
    GATED middle is untested, and "shard has no other ready work" is a subtle signal
    in the monoio/tokio loops (a connection may be parked-on-reply yet another is
    runnable). If wrong: either the gate mis-fires and starvation returns, or it's
    too conservative and c1 sees no win — surfaced at the §3 freeze. Proven on the
    same quiesced-VM pinned instrument as the baseline (relative ratio); GCloud
    absolute deferred.
  ⚠ Cross-connection read coalescing preserves the read-your-writes / ordering
    guarantees the current lock-free path provides — lower confidence because it
    changes dispatch granularity on a path the 197-suite exercises at 1/4/12; if
    wrong: a subtle cross-shard consistency regression visible only under concurrent
    load.
  - [ ] On the quiesced, core-pinned VM, the 3e376a1→main best-of-N RPS gap is
        REPRODUCIBLE across reps (the latency floor is stable, not a fluke) so the
        relative anchor is trustworthy even though the absolute µs is not. If wrong
        (the floor itself is noisy run-to-run): raise N / widen the quiesce gate, and
        if still unstable fall back to coalescing + documented mitigation. (The spike
        characterized the cells: s1 c1 GET 146k/7µs healthy; s4 c1 warm 36µs.)
  - [ ] Hard-removing `--cross-shard-fast-path` breaks only internal bench scripts
        (it was Phase-0 migration scaffolding); no production deployment relies on
        it. If wrong: a user conf/script errors at startup — mitigated by the
        CHANGELOG note. (Grep bench scripts for the flag during build.)
  - [ ] No new allocation on the read hot path: the bounded poll allocates nothing;
        coalescing reuses pre-allocated batch buffers (the MultiExecute /
        ResponseSlotPool precedent), not a per-read Vec.
</assumptions>

### SPIKE FINDINGS (2026-06-13, pre-freeze — retire/sharpen the ⚠ flags)
Goal: retire ⚠1 (spin viability) before freezing a §3 mechanism. Ran on moon-dev
(monoio release, VM-local binary, fresh-server-per-rep). Verdict: **⚠1 NOT retired —
the fixed-spin premise is undercut, and the regression is architectural, not a
fixable wake bug.** Evidence:
- BASELINE health: s1 c1 GET = 146k RPS / 7µs (local floor fine). s4 c1 GET
  *warm* = 26k / 36µs (matches shardslice §6) but *sustained* collapses to
  ~1.7k / ~580µs.
- NEIGHBOUR STARVATION CONFIRMED (clean relative signal): s4 c100 GET with a
  fixed spin=4096 = 44k vs spin=0 = 87k (−48%). An unconditional spin trades
  multi-client throughput for c1 latency → materializes `shard_starvation`. Any
  spin MUST be idle-shard-gated/adaptive, not a fixed budget (harder than §1's M1).
- WAKE-LOSS HYPOTHESIS REFUTED (load-independent): INFO `spsc_notify_wakes`
  delta = 149999 for ~150k cross-shard ops at c1 → ratio **1.000**. The eventfd
  cross-thread wake fires for EVERY op; there is NO tick-floor/lost-wake bug.
  Latency = 2×(cross-thread wake + vCPU reschedule) + processing; the reschedule
  term scales with VM contention (warm 36µs → load1.5 188µs → load3 580µs) while
  the wake COUNT stays correct. The "collapse"/variance was VM vCPU scheduling,
  not code.
- INSTRUMENT INVALID: the OrbStack VM cannot measure c1 cross-shard latency under
  host contention (1-min load floored ~1.5 with shards parked at ~0% CPU, 1
  runnable thread = host→guest vCPU starvation). Real numbers need GCloud
  bare-metal / dedicated cores (scripts/gcloud-bench-setup.sh).
- IMPLICATION: the −85% is ARCHITECTURAL — shared-nothing replaced a 4µs
  same-thread lock-read with a cross-thread round-trip that inherently wakes
  another thread twice. No wake fix exists. Safe levers that survive: adaptive
  (idle-gated) reply-side spin removes ONE of the two wakes (ceiling ~halve);
  read coalescing (multi-client only); the dead-flag cleanup (D3, unaffected).
  Recovering to ~4µs needs the rejected snapshot/RCU or client-routing paths.
- Spike scaffolding: `scripts/spike-xshard-read.sh` (kept); the env-gated
  `MOON_XSHARD_SPIN` prototype in handler_monoio was REVERTED (throwaway).
- DIRECTION (user, 2026-06-13): pivoted to "diagnose wake degradation first" →
  diagnosis complete (no bug). RESOLVED (user, 2026-06-13): "adaptive spin + validate
  on GCloud" → §1 Must re-cut to the adaptive idle-gate (C1/C2) + coalescing (C3).
  GCloud bare-metal baseline was then BLOCKED (no open billing account); user chose
  to pivot the M0 anchor to the quiesced-VM RELATIVE ratio (same pinned instrument
  pre/post; best-of-N floor) — GCloud absolute validation deferred/optional.

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: xrf0 quiesced-VM relative baseline is recorded (M0)
  Given moon-dev quiesced (1-min load < 0.7) with moon pinned to cores 0-3 and
        redis-benchmark to 4-5 — the SAME instrument used to measure the M1 result
  When the M0 harness runs per-runtime (monoio + tokio), fresh-server-per-rep,
       best-of-N RPS, for BOTH commits 3e376a1 (pre-regression) and current main
       (s4 c1 GET, s4 c100 GET, s4 c1 SET)
  Then a baseline file records each cell's reps + best-of-N + the 3e376a1→main
       RPS-regression %, and that relative gap is reproducible run-to-run
  And the recorded RELATIVE gap — not any absolute VM µs — is the anchor for "halve"

Scenario: xrf1 idle-gated spin halves the c1 cross-shard read gap (M1)
  Given idle-gated reply-side spin enabled, measured on the same quiesced pinned VM
  When a single client (c=1, P=1) runs cross-shard GET (random keys, ~75% foreign)
  Then its best-of-N RPS recovers at least HALF the M0 3e376a1→main gap for that cell
  And on the same binary, s4 c100 P1 GET throughput is within run-to-run noise of
      spin-disabled (the gate must not move the busy-shard case)

Scenario: xrf2 cross-connection coalescing recovers the P1 multi-client cell (M2)
  Given idle-gated spin + read coalescing live, multiple clients (c>1, P=1)
  When their cross-shard reads target the same owner shard concurrently
  Then the owner serves them in fewer SPSC messages than reads (coalescing observable
       via an INFO/stat counter) and s4 P1 GET recovers measurably toward the M0 parity cell
  And each connection still observes its own prior writes (read-your-writes; xrf-ryw)

Scenario: xrf3 the dead fast-path surface is gone (M3)
  Given the migrated codebase
  When grepping production src for `cross_shard_fast_path`, `CrossShardFastPath`,
       `moon_cross_shard_lock_contention_total`,
       `moon_dispatch_cross_read_fastpath_latency_us`,
       `record_dispatch_cross_read_fastpath_*`, `cross_read_fast_dispatches`
  Then zero matches remain, and the stale docstrings are gone/corrected
  And starting moon with `--cross-shard-fast-path on` exits non-zero with a clap
      unknown-argument error (documented breaking change in CHANGELOG)

Scenario: xrf4 full matrix green, nothing else regresses (M4)
  Given the implemented change
  When the full matrix runs (bench s1/routed/P16 + entire tokio matrix; consistency
       sweep; clippy ×2 featuresets; fmt; unsafe audit; RSS under load)
  Then no non-c1 cell regresses beyond noise, consistency is 197/197 @ shards=1/4/12,
       clippy/fmt clean, zero new unsafe, and steady-state RSS is within noise of baseline

Scenario: reject lock_reintroduced
  Given the implementation
  When auditing for any new cross-thread lock, Send/Sync impl, or unsafe slice escape
       added to accelerate reads
  Then none exists — the SPSC hop remains the only cross-shard door
  And `tests/shardslice_shape.rs` + `scripts/audit-unsafe.sh` stay green

Scenario: reject borrow_across_await
  Given the spin/coalescing code paths
  When reviewing every `with_shard`/`with_shard_db` closure introduced or touched
  Then no closure holds its borrow across an `.await`
  And the shape grep-pin (no `.await` inside a with_shard closure body) passes

Scenario: reject shard_starvation
  Given idle-gated spin
  When a busy shard (c100 P1 cross-shard GET) serves traffic
  Then the poll never runs while other connections/SPSC work are ready, and s4 c100
       GET throughput shows NO regression vs spin-disabled
  And the spike's fixed-spin −48% does NOT reproduce

Scenario: reject read_your_writes_regression
  Given coalescing live at shards=4
  When one connection issues a cross-shard write (e.g. SET) and then a GET of the
       same key
  Then the GET reflects the just-written value, identical to the pre-task lock-free path
  And `scripts/test-consistency.sh` is byte-identical-green 197/197 @ 1/4/12

Scenario: reject memory_regression
  Given the implementation under active read+write load
  When steady-state RSS is sampled (fresh server, redis-benchmark -r over a large keyspace)
  Then RSS is within run-to-run noise of the M0 baseline
  And no structure was added that scales with keyspace (no per-key snapshot / retained buffer)

Scenario: reject removed_flag_referenced
  Given M3 is complete
  When any later commit reintroduces a reference to the removed flag/enum/metric/fns
  Then the M3 grep-pin (a shape test) fails CI
  And the failure names the resurrected symbol
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

No wire-protocol change: every RESP command stays byte-identical (the 197-test
consistency suite is the wire contract and must stay byte-identical — reject
test_weakening/read_your_writes_regression). The frozen shape is the *internal*
mechanism: the idle-gate, the reply-side poll integration, the coalescing message,
and the exact cleanup surface.

### C1 — Idle-gate signal (M1)
```rust
// src/shard/slice.rs — sibling to the existing `thread_local! { static SHARD }`.
// The shard thread is single-threaded, so a Cell needs NO atomic and NO lock.
thread_local! {
    /// Connections on THIS shard thread currently blocked in a cross-shard
    /// reply-wait. Incremented on entry, decremented on exit (RAII guard).
    static XSHARD_INFLIGHT: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}
/// May the caller POLL its reply instead of parking? True when it is (near-)alone
/// on its shard, so polling steals no co-located connection's turn.
pub fn xshard_may_spin() -> bool;            // XSHARD_INFLIGHT.get() <= XSHARD_SPIN_GATE
const XSHARD_SPIN_GATE: u32 = 2;             // tuned on bare metal; NOT a CLI flag (no new surface)
const XSHARD_SPIN_BUDGET: u32;               // bounded poll iterations before park
/// RAII: increments on construct, decrements on drop — wraps each reply-wait.
pub struct XshardWaitGuard;
```

### C2 — Reply-side adaptive poll (M1)
At each cross-shard reply-wait site — monoio `handler_monoio` `Err(Empty)` branch
(mod.rs:~1755) and tokio `handler_sharded` `response_pool.future_for(target).await`
(mod.rs:~1636) — wrap the wait in an `XshardWaitGuard`; if `xshard_may_spin()`, poll
the reply (`try_recv()` / `ResponseSlot` state) up to `XSHARD_SPIN_BUDGET` with
`core::hint::spin_loop()`; on hit, return WITHOUT parking; else fall through to the
EXISTING park path (race2+sleep / ResponseSlotFuture) UNCHANGED. The poll is
synchronous — it holds no borrow across `.await`. When the gate is false, the path
is byte-identical to today (immediate park).

### C3 — Cross-connection read coalescing (M2) — most-novel surface
```rust
// src/shard/dispatch.rs — one batched message carrying N independent foreign
// reads from DIFFERENT connections on the origin shard to ONE owner shard, each
// result routed back to its own connection's response slot.
CoalescedReadBatch {
    db_index: usize,
    reads: smallvec::SmallVec<[(std::sync::Arc<Frame>, ResponseSlotPtr); 8]>,
}
```
- The origin shard accumulates outbound single-key foreign READs per target within
  one event-loop turn and flushes as ONE message (reuses the MultiExecuteSlotted
  batching precedent, extended to per-read slots). Flush bound: the existing
  connection-batch boundary OR a small cap (no unbounded buffer → no memory_regression).
- INVARIANT (load-bearing): coalescing groups reads ACROSS connections only; within
  a connection, submission order and read-your-writes are preserved exactly as the
  current lock-free path (a read is never reordered before that connection's own
  acked write). The 197-suite + xrf-ryw are the oracle.

### C4 — Cleanup surface (M3, hard-remove — exact symbols)
- `src/config.rs`: `cross_shard_fast_path` field + `--cross-shard-fast-path` arg
  (615-619), `CrossShardFastPath` enum, `cross_shard_fast_path_enabled()` (760),
  the docstring (603-613), the 6 unit tests (1725-1768).
- `src/admin/metrics_setup.rs`: `moon_cross_shard_lock_contention_total`,
  `moon_dispatch_cross_read_fastpath_latency_us`, `record_dispatch_cross_read_fastpath_*`
  (824-863).
- `src/server/conn/handler_sharded/mod.rs`: `cross_read_fast_dispatches` (384) + its
  `record_…_batch` call (1573) + the stale `Arc<ShardDatabases>…RwLock` doc (60-area).
- `src/command/connection.rs`: drop any fast-path INFO field.
- CHANGELOG: breaking-change note. RESULT: clap rejects `--cross-shard-fast-path`.

### Contracted responses for §1 rejects
| Reject | Contracted response |
|---|---|
| lock_reintroduced | idle-gate is a thread-local `Cell<u32>` — no lock/atomic/Send; `shardslice_shape.rs` + `audit-unsafe.sh` green |
| borrow_across_await | poll is synchronous, before the `.await`; grep-pin: no `with_shard` across `.await` |
| shard_starvation | `xshard_may_spin()` gates on `XSHARD_INFLIGHT <= XSHARD_SPIN_GATE`; c100-GET guard bench shows no regression |
| read_your_writes_regression | C3 groups cross-connection reads only; per-connection order preserved; 197-suite @1/4/12 + xrf-ryw byte-identical-green |
| memory_regression | gate = one `Cell<u32>`/shard; coalescing buffer bounded+reused (SmallVec), not keyspace-scaled; RSS-under-load guard |
| removed_flag_referenced | M3 grep-pin in `shardslice_shape.rs` (or a new shape test) fails CI on any resurrected symbol |

Names from GLOSSARY: ShardSlice, ShardMessage, ResponseSlotPtr, MultiExecuteSlotted,
with_shard, key_to_shard. New names introduced here: XSHARD_INFLIGHT, xshard_may_spin,
XshardWaitGuard, XSHARD_SPIN_GATE, XSHARD_SPIN_BUDGET, CoalescedReadBatch.

Least-sure flag surfaced at freeze:
⚠ [contract] The idle-gate (`XSHARD_INFLIGHT <= gate`) is a sufficient proxy for "no
  other ready work" — it counts cross-shard WAITERS, not all runnable tasks, so a
  shard busy with local pipelines/accepts but only 1 cross-shard waiter would still
  spin and steal cycles. If wrong: starvation returns in MIXED workloads the c100-GET
  guard doesn't cover. Mitigation in reserve: also gate on the inbound SPSC being empty.
⚠ [contract] C3's per-result reply routing (one batched message → N different
  connection slots) is the most novel surface and the read-your-writes proof rests on
  "only cross-connection reads are grouped"; if wrong: a concurrent-load consistency
  regression (197-suite is the oracle).

Status: FROZEN @ v1 — approved by Tin Dang 2026-06-14. Both freeze gates satisfied:
(1) the quiesced-VM RELATIVE M0 baseline is established (clean-VM best-of-5; c1-GET
37580→22252 = −40.8%; full table in §6 M0 RECORD) and (2) the one human approval over
the §1–§4 bundle was given "Freeze as-is", lowest-confidence flag surfaced first (the
~30k halve target sits at the mechanism's ~one-wake-removed ceiling). Changing any C1–C4
clause or the M1 ≥~30k line now is a change request back to SPECIFY.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: <e.g. 90%>
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - test_<scenario>: arrange <Given> / act <When> / assert <Then> + assert <unchanged>
</test_plan>

Tests live in: `./tests/` · MUST run red (missing implementation) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): <e.g. debit+credit in one atomic transaction>
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

### M0 BASELINE RECORD (the "before" — M1 verify compares against this)
Measured 2026-06-14 on moon-dev, monoio, FRESH-BOOT clean VM (load 0.00 at start),
moon pinned cores 0-3 / redis-benchmark cores 4-5, fresh-server-per-rep, best-of-5,
-r 1000000, --appendonly no. Harness: `scripts/baseline-xshard-quiesced.sh`.
Both binaries built from a VM-local clone (`scripts/probe-xshard.sh` = the local-vs-xshard discriminator).

| cell | 3e376a1 (v0.3.0, lock read-path) best/med | a497602 (HEAD, SPSC) best/med | Δ(best) |
|------|------------------------------------------|-------------------------------|---------|
| s4-c1-GET   (TARGET) | 37580 / 36860 | 22252 / 22163 | **−40.8%** |
| s4-c100-GET (guard)  | 286533 / 280112 | 202429 / 200401 | −29.4% |
| s4-c1-SET   (control)| 564 / 555 | 21720 / 21552 | +3751% |
| s1-c1-GET   (LOCAL control, probe) | ~41k | ~40k | ~flat |

Reads of the table:
- **Anchor**: c1-GET XSHARD 37580 → 22252 = a 15328-RPS gap. M1 must recover ≥ half → ≥ ~30k.
- **LOCAL control flat at ~40k on both** ⇒ the regression is cross-shard-specific (not a slow
  binary / slow VM). This is the proof the relative anchor is valid on this instrument.
- **c100 guard baseline = ~202k** (a497602): the idle-gate must leave this within run-to-run noise.
- **c1-SET**: v0.3.0 cross-shard WRITES were the slow path (564 RPS ≈ 1.8ms); shared-nothing
  improved writes 38× → confirms reads-only scope. NOT a regression to chase.
- Live regression is **−40.8%**, not the historical −85% (shardslice §6, eb5d664-era); current
  main already recovered part of it post-§6. The task halves the LIVE gap.
- ⚠ INSTRUMENT HYGIENE (hard-won): a leaked `moon-<hash>-monoio` busy-poller (4 shard threads)
  from an earlier run survived `pkill -f "moon --port"` (name is `…monoio --port`, no match) and
  floored c1 at 1–6k for hours. Cross-shard c1 latency is ONLY valid on a verified-clean VM —
  assert `pgrep -af moon-baseline-bins` is empty before measuring.

- [ ] all tests pass
- [ ] coverage did not decrease
- [ ] no test or contract was altered during build
- [ ] concurrency / timing of the risky operation is safe
- [ ] no exposed secrets, injection openings, or unexpected dependencies
- [ ] layering & dependencies follow CONVENTIONS.md
- [ ] a person reviewed and approved the change

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [ ] WIRING (code) — every new symbol is referenced; record where / how confirmed
- [ ] DEAD-CODE (code) — no new unused or orphaned symbol introduced
- [ ] SEMANTIC (prose / non-code) — read in full, not skimmed: <what read · what confirmed>

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
