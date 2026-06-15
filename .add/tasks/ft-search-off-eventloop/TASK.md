# TASK: FT.SEARCH off-event-loop: a heavy vector/text query must not stall the shard's 1ms tick or co-located commands

slug: ft-search-off-eventloop · created: 2026-06-15 · stage: production · risk: high · autonomy: conservative
phase: contract   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: FT.SEARCH (and the heavy FT.* read path) must not monopolize a shard's event loop.
moon ALREADY scatter-gathers across shards concurrently (each shard searches its local slice, the
coordinator merges + reranks — `src/shard/scatter_hybrid.rs`, `merge_search_results`). The defect is
INTRA-shard: each shard runs its local slice (`search_local_*` → `idx.segments.search_mvcc` — brute-force
mutable scan + per-immutable-segment HNSW traversal at ef_search 200–1000 + TQ/SQ8 decode) FULLY
SYNCHRONOUSLY on its event-loop task (`ft_search/execute.rs:38/208`, no `.await`/`spawn_blocking`). While
that runs, the shard cannot fire its 1ms tick, cannot `drain_spsc_shared` (256 msgs/cycle), and co-located
PING/GET/SET pile up in the SPSC ring until the search returns. **Fix = make the per-shard local slice
yield cooperatively between bounded chunks** (per-segment / per-N-graph-nodes) so the shard interleaves its
queued commands + the 1ms tick — single-threaded, NO new cross-thread lock, NO snapshot, NO RSS growth. The
cross-shard scatter-gather, the merge/rerank/filter, and FT.SEARCH result semantics are UNCHANGED.

Ground truth (investigation 2026-06-15, file:line): FT.SEARCH dispatch `spsc_handler.rs:1820`
(`dispatch_vector_command`) → `ft_search/dispatch.rs:50` → `ft_search/execute.rs:38 search_local_raw` /
`:208 search_local_filtered` → `vector/segment/holder.rs:126 search_mvcc` — all synchronous, no yield. The
shard event loop `shard/event_loop.rs:987` runs ONE task; the 1ms `periodic_interval` (`:692`, fires `:1204`)
drives `cached_clock.update` + `drain_spsc_shared` + WAL/snapshot tick — none can run while a search blocks.
Concurrency model: the vector engine is single-threaded-by-construction — `VectorStore::indexes`,
`VectorIndex::{key_hash_to_key, payload_index, scratch}` are PLAIN unsynchronized HashMaps/structs (no Arc /
RwLock); only `segments: ArcSwap<SegmentList>` (`holder.rs:51`) is concurrency-safe. A write (HSET
auto-index, compaction install via `segments.swap`) and a read never overlap today because both run on the
shard thread. A YIELD breaks that "never overlap" assumption: between two chunks of a paused search, a
co-located HSET/compaction-install can run on the same thread (intra-thread re-entrancy) and mutate
`key_hash_to_key` / `payload_index` / install a new segment list — so the yield's correctness rests on the
search reading a STABLE snapshot across yields (the `ArcSwap` `Guard` held for the whole search) and never
re-reading mutated metadata mid-result.

Framings weighed: cooperative yield, single-thread (CHOSEN — user 2026-06-15: the only path that honors the
milestone's no-new-cross-thread-lock + no-RSS-growth constraints; snapshot/offload were declined milestone-wide
for the same low-RSS reason) · worker-thread offload below the shards (REJECTED — frees the loop + uses idle
cores, but the worker reads unsynchronized per-index metadata ⇒ needs Arc/RwLock or per-query snapshot ⇒
the locks/RAM the milestone forbids) · hybrid bounded-work-per-tick (declined — same low-RSS profile but needs
the synchronous HNSW traversal refactored into a resumable state machine; more invasive than natural yields).
Scope: FT.SEARCH + FT.* vector/text read path (the heavy queries); FT.AGGREGATE/hybrid reuse the same local
slice. OUT: the cross-shard scatter mechanism, the merge/rerank, write-path indexing, compaction.
Must:
<must>
  - M0 (baseline anchor — the stall, per-runtime relative): on moon-dev, fresh-server, drive a HEAVY FT.SEARCH
    on a shard while issuing a stream of co-located simple commands (PING/GET) to the SAME shard; record p99
    (and max) of those co-located commands — the "before" stall — for monoio and tokio, best-of-N. Record
    FT.SEARCH recall + top-k ordering as the unchanged-control. State the win as a RELATIVE before/after on
    the same instrument (milestone bench rule); confirm the instrument can resolve co-located p99 before
    anchoring (the wal-group-commit instrument-validity lesson — CONVENTIONS foundation v2).
  - M1 (the win): during a heavy FT.SEARCH, the shard's local slice yields between bounded chunks so the 1ms
    tick fires and `drain_spsc_shared` runs while the search is in flight ⇒ co-located PING/GET p99 on that
    shard stays under a recorded bound, measurably below M0. The yield granularity is BOUNDED (a cap on
    work per chunk — segments and/or graph-node visits) so neither the co-located p99 (too-coarse) nor
    per-query overhead (too-fine) is unbounded.
  - M2 (result-identity invariant — freeze-first): the FT.SEARCH response (the returned doc set, score
    ordering, top-k/LIMIT, payload/numeric filter, num_docs, key resolution via `key_hash_to_key`) is
    BYTE-IDENTICAL to the pre-change synchronous path for the same data + query. Cooperative yielding changes
    WHEN the work runs, never WHAT it returns.
  - M3 (MVCC / re-entrancy safety): a write that runs on the shard thread DURING a yield of an in-flight
    search (HSET auto-index, compaction installing a new `SegmentList`, a delete/`mark_deleted`) is INVISIBLE
    to that search — it observes the stable `ArcSwap` `SegmentList` `Guard` it captured at start, and never
    reads post-yield-mutated `key_hash_to_key` / `payload_index` for a result it already gathered. Existing
    MVCC/temporal isolation tests (`tests/ft_search_concurrent_readers.rs`, `ft_search_as_of_*`) stay GREEN,
    and a NEW test asserts a concurrent HSET issued mid-search is not reflected in that search's result.
  - M4 (no-regression guardrail): single-query end-to-end latency is not materially worse than the
    synchronous path (yield overhead bounded, no per-chunk heap alloc on the event-loop hot path); the
    cross-shard scatter-gather + merge/rerank are unchanged; `scripts/test-consistency.sh` 197/197 @1/4/12;
    dual-runtime green; clippy ×2 + `fmt`; zero new `unsafe`; **zero new cross-thread lock; steady-state RSS
    not grown** (the milestone's hard constraints); no new allocation on command dispatch / event loop.
</must>
Reject:
<reject>
  - a yield path that changes the search result (recall, ordering, top-k, filter, num_docs, key mapping)
    vs the synchronous path -> "result_not_identical"
  - a write committed on the shard thread during a yield becoming visible to the in-flight search that
    captured its snapshot before that write -> "snapshot_straddle" (MVCC violation)
  - a co-located command (HSET/compaction/delete) running during a yield corrupting `SearchScratch` or the
    per-index metadata the paused search is mid-iteration on -> "reentrancy_corruption"
  - a "yield" that does not actually relinquish to the event loop (co-located p99 still stalls for the full
    search) -> "no_yield_progress" (the win is absent — a bound, asserted by M1)
  - a per-chunk heap allocation / lock acquisition on the event-loop hot path introduced by the yield
    machinery -> "hotpath_alloc_or_lock" (violates the milestone hot-path + zero-lock rule)
</reject>
After:
<after>
  - A heavy FT.SEARCH runs as a sequence of bounded, cooperatively-yielding chunks on its shard's event loop;
    co-located PING/GET p99 on that shard stays under the recorded bound (measurably below M0) while the
    search is in flight; the search RESULT, MVCC isolation, cross-shard scatter-gather, RSS, and the
    zero-cross-thread-lock invariant are all unchanged; consistency 197/197 and the FT.* suites green on both
    runtimes.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A cooperative yield inserted into the synchronous local search preserves MVCC/result identity EXACTLY —
    lowest confidence because the search reads `scratch` + `key_hash_to_key` + `payload_index` that are NOT
    snapshotted (only `segments` is, via `ArcSwap`), so a yield that lets a co-located HSET/compaction/delete
    run mid-search could change metadata the search still depends on; getting the snapshot boundary exactly
    right (capture the `Guard` once, resolve keys only against captured state, never re-read mutated maps) is
    the freeze-first risk — if wrong: silently wrong/inconsistent FT.SEARCH results or a crash under
    concurrent write load (the milestone's correctness-parity bar broken).
  ⚠ Yielding relieves co-located p99 WITHOUT making single-query latency materially worse — the shard still
    does all the search work, just interleaved; if chunk granularity is too coarse the p99 isn't relieved,
    too fine the yield overhead dominates a query — lowest-confidence on the tuning, not the direction; if
    wrong: the win is marginal or trades query latency for it (an effectiveness miss, re-tune the cap).
  - [ ] the synchronous HNSW traversal (`ImmutableSegment::search`) + the brute-force mutable scan can be
        chunked at NATURAL boundaries (per-segment, and per-N-node within a segment) without a full async
        rewrite of the vector engine — confirm the search loop exposes a yield point; if wrong, scope grows
        to a resumable-iterator refactor (the rejected hybrid framing).
  - [ ] DiskANN cold-tier search (NVMe beam reads) and warm mmap page-faults are coarse enough that
        per-segment yields suffice (they already do blocking I/O) — confirm; if wrong, those tiers need their
        own yield cadence (likely already I/O-yielding).
  - [ ] the OrbStack VM can resolve co-located-p99-under-FT.SEARCH as a stable RELATIVE signal (a latency
        metric, jitter-sensitive) — confirm instrument validity BEFORE anchoring M1 (CONVENTIONS foundation
        v2: a perf Must can be un-measurable on the only instrument); if wrong, anchor on a deterministic
        proxy (tick-fired-count / SPSC-drained-count during a search) instead of wall-clock p99.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# ---- one per Must ----

Scenario: M0 — baseline co-located stall is measurable (the instrument resolves the metric)
  Given a fresh moon-dev server with an FT index large enough that one FT.SEARCH takes >> 1ms of CPU
    (many immutable HNSW segments at ef_search >= 200 over the pre-yield SYNCHRONOUS local slice)
  When a heavy FT.SEARCH runs on a shard while a separate connection streams simple PING/GET to the SAME shard
  Then the co-located PING/GET p99 (and max) on that shard is recorded, best-of-N, for monoio and tokio,
    AND a deterministic in-process proxy is recorded too — the count of 1ms ticks that fired and the count of
    drain_spsc_shared cycles that ran DURING the search window — so M1 has a non-jitter anchor if VM p99 is noisy
  And the FT.SEARCH recall + top-k ordering for this dataset is captured as the unchanged-result control

Scenario: M1 — the yield relieves the stall (tick fires mid-search, co-located p99 bounded below M0)
  Given the heavy-FT.SEARCH + co-located-PING/GET workload from M0 on the cooperatively-yielding build
  When the FT.SEARCH runs as a sequence of bounded chunks (per-segment / per-N-graph-node) that yield to the loop
  Then the 1ms tick fires >= K times and drain_spsc_shared runs during the single search window (vs ~0 at M0),
    AND co-located PING/GET p99 on that shard stays under a recorded bound that is measurably below M0's p99
  And the yield granularity is bounded by an explicit cap (work-per-chunk), not unbounded in either direction

Scenario: M2 — result is byte-identical to the synchronous path
  Given the same index data and the same FT.SEARCH query (vector KNN, with and without payload/numeric filter,
    with LIMIT/top-k, across mutable + multiple immutable segments)
  When the query is served by the yielding path and, separately, by the pre-change synchronous path
  Then the two RESP responses are byte-identical — returned doc set, score ordering, top-k/LIMIT slice,
    filtered-out docs, num_docs, and key resolution via key_hash_to_key all match exactly
  And no result row resolves to a synthetic vec:<id> that the synchronous path resolved to a real key

Scenario: M3 — a write during a mid-search yield is invisible to that search (MVCC isolation)
  Given a heavy FT.SEARCH in flight on a shard, paused at a yield point, having captured its ArcSwap Guard at start
  When a co-located HSET auto-index (or a compaction installing a new SegmentList, or a delete/mark_deleted)
    commits on the SAME shard thread between two chunks of the paused search
  Then that search's result reflects ONLY the SegmentList + metadata snapshot it captured at start —
    the mid-search write is NOT in its result set, and existing as-of / concurrent-reader tests stay green
  And the post-write segment state IS visible to the NEXT FT.SEARCH (the write is not lost, only isolated)

Scenario: M4 — no regression (latency, correctness, invariants)
  Given the cooperatively-yielding build
  When the full guardrail suite runs: a single (un-contended) FT.SEARCH end-to-end latency vs the sync path,
    scripts/test-consistency.sh @1/4/12, the FT.* suites, clippy x2 + fmt, and an RSS + lock-count check
  Then single-query latency is not materially worse than sync (bounded yield overhead, no per-chunk heap alloc),
    consistency is 197/197, both runtimes are green, there are zero new unsafe blocks and zero new cross-thread
    locks, and steady-state RSS is not grown

# ---- one per Reject (each asserts what stays unchanged) ----

Scenario: reject result_not_identical
  Given any index + query for which the synchronous path returns result R
  When the yielding path returns a result that differs from R in recall, ordering, top-k, filter, num_docs,
    or key mapping
  Then the change is REJECTED as "result_not_identical"
  And the synchronous path's result R for that query is unchanged (the oracle is the pre-change behavior)

Scenario: reject snapshot_straddle
  Given an in-flight search that captured its SegmentList snapshot before a co-located write
  When that later write becomes visible inside the same search's result (the search straddled the snapshot)
  Then the change is REJECTED as "snapshot_straddle"
  And the write itself still commits and is durable — only its leakage INTO the prior search is forbidden

Scenario: reject reentrancy_corruption
  Given a search mid-iteration over SearchScratch / key_hash_to_key / payload_index, paused at a yield
  When a co-located HSET/compaction/delete on the same thread mutates that structure and the resumed search
    reads corrupted/torn metadata (panic, wrong key, or out-of-bounds)
  Then the change is REJECTED as "reentrancy_corruption"
  And the co-located write completes correctly and the index remains internally consistent for later queries

Scenario: reject no_yield_progress
  Given the heavy-FT.SEARCH + co-located-PING/GET workload
  When the "yield" does not actually relinquish to the event loop — co-located p99 still stalls for the full
    search duration and the 1ms tick fires ~0 times during the search
  Then the change is REJECTED as "no_yield_progress" (the win asserted by M1 is absent)
  And FT.SEARCH still returns the correct result (a non-yielding build is wrong on the WIN, not on correctness)

Scenario: reject hotpath_alloc_or_lock
  Given the yield machinery on the event-loop hot path
  When resuming a chunk performs a heap allocation (Box/Vec/format!) or acquires a lock per chunk on the
    command-dispatch / event-loop path
  Then the change is REJECTED as "hotpath_alloc_or_lock"
  And the no-alloc-on-hot-path + per-shard-lock-only invariants (CONVENTIONS) remain intact
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

This is an INTERNAL Rust seam, not a wire API — the frozen shape is the
yielding search seam + its capture-before-yield invariant. The FT.SEARCH RESP
surface is UNCHANGED (frozen elsewhere, Redis-compatible). Names from GLOSSARY:
shard event loop · segment (vector) · hot path.

```
# ---- C1 · the captured snapshot (the consistency anchor) ----
# Taken at search entry WHILE the &mut VectorIndex borrow is held, BEFORE the
# first yield. After capture the search holds NO borrow into VectorStore/Index.
struct SearchSnapshot {
    segments:        Arc<SegmentList>,             // idx.segments.load_full() — O(1) Arc refcount bump, NOT a data copy (no RSS growth)
    key_hash_to_key: Arc<HashMap<u64, Bytes>>,     // captured at START (not after the loop) so a mid-search delete cannot drop a needed entry
    filter_bitmap:   Option<RoaringBitmap>,        // materialized from payload_index BEFORE the loop (already is, execute.rs:113)
    committed:       Arc<TxnTreemap> | owned clone, // MVCC committed set, captured with the segments
    query_f32:       Vec<f32>,                      // owned (already is)
    scratch:         SearchScratch,                 // OWNED by the query for its duration — NOT a &mut into idx.scratch held across a yield
    k: usize, ef_search: usize, snapshot_lsn: u64,
}
# Invariant CAP: SearchSnapshot is captured atomically under one &mut idx borrow;
# that borrow is dropped before the first cooperative_yield().await.

# ---- C2 · the yielding seam ----
async fn search_mvcc_yielding(
    snap:   &mut SearchSnapshot,
    budget: YieldBudget,
) -> SmallVec<[SearchResult; 32]>
#   walks the SAME logical steps as the sync search_mvcc, in the SAME order
#   (brute-force mutable → per-immutable HNSW → warm → cold → ivf → merge:
#    all.sort_unstable(); all.truncate(k)), against `snap` ONLY;
#   awaits cooperative_yield() between bounded chunks (per-segment, and within a
#   segment every budget.max_graph_nodes_per_chunk node-visits).
#   Holds only owned `snap` state across every yield — zero borrow into idx/store.

# ---- C3 · the explicit bounded cap ----
struct YieldBudget {                               // the M1 "bounded granularity" knob
    max_segments_per_chunk:        usize,          // default 1 (yield between segments)
    max_graph_nodes_per_chunk:     usize,          // default const — yield inside a large HNSW segment
    max_brute_force_vecs_per_chunk:usize,          // default const — yield inside a large mutable scan
}
const FT_SEARCH_YIELD_BUDGET: YieldBudget = …;     // named defaults; tuned at M1

# ---- C4 · runtime-abstracted cooperative yield (both runtimes) ----
async fn cooperative_yield();   // monoio: monoio::time::yield equiv · tokio: tokio::task::yield_now
#   relinquishes to the shard event loop so the 1ms tick + drain_spsc_shared run, then resumes.

# ---- C5 · M0/M1 deterministic proxy counters (non-jitter anchor) ----
# observable per-shard: ticks fired and drain_spsc_shared cycles that ran during a search window.
# (reuse existing tick/drain counters sampled around the search; exposed for the M1 assert.)
```

Guarantees (each maps to a Must / a Reject response):
- G-IDENTITY (M2 / `result_not_identical`): `search_mvcc_yielding(snap)` is byte-identical to the sync `search_mvcc` for the same `snap` — same per-segment order, same `sort_unstable`+`truncate(k)`, same `key_hash_to_key` resolution. Pinned by a differential test (oracle = sync path; the yielding path with an ∞ budget MUST equal it).
- G-ISOLATION (M3 / `snapshot_straddle`): results derive solely from `snap` captured at entry; a write committed after capture (segment `swap`, `key_hash_to_key` insert/delete, `mark_deleted`) is invisible to this search, visible to the NEXT. Key resolution uses the START-captured `key_hash_to_key`, never an end-of-loop re-read.
- G-NOBORROW (M3 / `reentrancy_corruption`): the yielding seam holds NO `&mut VectorStore`/`&mut VectorIndex`/`&mut idx.scratch` borrow across any `cooperative_yield().await` — it owns `snap` (incl. its own `SearchScratch`). A co-located write runs on the released borrow without aliasing the search's owned state.
- G-PROGRESS (M1 / `no_yield_progress`): the seam awaits `cooperative_yield()` at least once per `YieldBudget` of work; during one heavy search the C5 proxy shows ticks-fired > 0 and drain cycles > 0 (vs ~0 on the sync path).
- G-HOTPATH (M4 / `hotpath_alloc_or_lock`): the per-chunk resume path performs NO heap alloc (`Box`/`Vec`/`format!`) and acquires NO lock; the snapshot is captured ONCE at entry (one `load_full` + one `key_hash_to_key` Arc/clone); no per-chunk allocation, no new cross-thread lock, steady-state RSS not grown.
- G-UNCHANGED (M4): cross-shard scatter (`scatter_hybrid.rs`), `merge_search_results`, the FT.SEARCH RESP response builder (`response.rs`), write-path indexing, and compaction are UNTOUCHED.

SAFETY-NET clause (pre-authorized build fallback — approved at freeze v1, Tin Dang): if owning /
re-installing the per-index `idx.scratch` across a yield is NOT cleanly expressible, the build MAY
allocate the yielding search's `SearchScratch` ONCE per query at capture time (a single allocation at
entry, BEFORE the first yield) instead of moving `idx.scratch`. This per-QUERY allocation does NOT
violate G-HOTPATH (which forbids per-CHUNK alloc on the resume path) and is NOT an RSS-growth breach
(one transient scratch freed at query end, not retained state). It is an explicit, contracted deviation
— taking it does NOT re-open SPECIFY. Any OTHER divergence from C1–C5 still re-opens SPECIFY.

Build-surface note (not a guarantee — a ripple the build owns): `search_local_raw` /
`search_local_filtered` / `search_local` become async (they `.await` the yielding seam),
rippling to both call sites — the direct FT.SEARCH dispatch (`spsc_handler.rs:1820` →
`ft_search/dispatch.rs`) and the cross-shard `ShardMessage::VectorSearch` handler. Both already
run on the (async) shard event loop. The contract does not fix HOW the async ripple is wired,
only that the seam's five guarantees hold.

Least-sure flag surfaced at freeze: ⚠ [contract] G-NOBORROW + scratch-ownership is the freeze-first
risk — the sync path runs the WHOLE search holding `&mut idx` (incl. `&mut idx.scratch`); the contract
forces that borrow to be released before the first yield and the search to own its `SearchScratch` +
an `Arc<SegmentList>` (`load_full`) + a START-captured `key_hash_to_key`. If owning/re-installing the
per-index scratch is not cleanly expressible (or `load_full` + map-clone-at-start subtly changes
ordering), either G-IDENTITY (byte-identical result) or G-HOTPATH (no per-chunk alloc) breaks →
re-open SPECIFY. Second flag: ⚠ [spec] M1's anchor — wall-clock co-located p99 may be too jittery on
the VM; mitigated by the C5 deterministic proxy (tick/drain counts during the search) as the primary
assert, p99 as corroboration.

Status: FROZEN @ v1 — approved by Tin Dang 2026-06-15 (with SAFETY-NET clause for scratch ownership)
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
