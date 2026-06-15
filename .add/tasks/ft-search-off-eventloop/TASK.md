# TASK: FT.SEARCH off-event-loop: a heavy vector/text query must not stall the shard's 1ms tick or co-located commands

slug: ft-search-off-eventloop · created: 2026-06-15 · stage: production · risk: high · autonomy: conservative
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
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

Coverage target: behavior-level, not line-% — the gate is: every Must + Reject has a test or pin, and
the suite is RED for the right reason (missing seam / un-relieved stall), confirmed on the default
(monoio+graph) build. Follows the foundation-v1 red-suite shape: runtime-red (behavioral) +
compile-red (API-shape, fails to compile until the seam exists) + green-pins (invariants that stay
green). FT.* needs the default `graph` feature → runtime tests gate `#[cfg(feature = "graph")]`
(mirrors `ft_search_concurrent_readers.rs`); the compile-red file is feature-agnostic.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  # --- compile-red: `tests/ft_search_yield_red_api.rs` (cargo test --test ft_search_yield_red_api → COMPILE ERROR = red) ---
  - c2_search_mvcc_yielding_symbol_exists [Reject result_not_identical / M2]: reference
    `moon::vector::segment::holder::SegmentHolder::search_mvcc_yielding` as a value (the C2 seam) →
    unresolved import until BUILD ⇒ crate fails to compile (red). After build: resolves, asserts the
    signature shape (async, takes &mut SearchSnapshot + YieldBudget, returns SmallVec<[SearchResult;32]>).
  - c3_yield_budget_named_defaults [M1 bounded-cap]: reference `YieldBudget` + the named const
    `FT_SEARCH_YIELD_BUDGET`; assert `max_segments_per_chunk >= 1`, `max_graph_nodes_per_chunk > 0`,
    `max_brute_force_vecs_per_chunk > 0` (the explicit C3 cap exists, defaults sane).
  - c1_search_snapshot_owns_state [M3 G-NOBORROW]: reference `SearchSnapshot` + assert it is `'static`
    (owns `Arc<SegmentList>` + owned scratch; no borrow into idx) via a `fn assert_static<T:'static>()`
    bound — pins the capture-before-yield shape. (compile-red until the type exists.)

  # --- runtime-red + green-pins: `tests/ft_search_yield_red.rs` (spawn moon --shards 1; #[cfg(feature="graph")]) ---
  - m1_heavy_search_yields_cooperatively [M1 / Reject no_yield_progress] RED-NOW:
    arrange a heavy index (many vectors over forced-immutable segments) on a 1-shard server /
    act: run one heavy FT.SEARCH, then INFO / assert the per-shard counter
    `ft_search_cooperative_yields_total` is PRESENT and > 0 (deterministic C5 proxy — the search
    relinquished to the loop). RED now (field absent ⇒ parsed 0). Mirrors swf1's INFO-counter red.
    + assert (unchanged): FT.SEARCH still returned the correct hit count (the yield didn't drop results).
  - m1b_colocated_ping_progress_during_search [M1 corroboration, #[ignore] — jitter-sensitive]:
    while a heavy FT.SEARCH is in flight on the 1-shard loop, co-located PINGs' max RTT stays under a
    generous bound measurably below the search duration. Corroborates the win on wall-clock; #[ignore]
    so VM jitter never reds CI — run by hand at verify on a quiesced VM (flag #2).
  - m2_topk_known_neighbors_keys_resolved [M2 / Reject result_not_identical] GREEN-PIN:
    a deterministic small index with known vectors / FT.SEARCH KNN / assert the returned top-k doc
    KEYS + order match the known nearest neighbors, and no row is a synthetic `vec:<id>`
    (key_hash_to_key resolved). Green now; must stay green after build (guards G-IDENTITY at the wire).
  - m3_concurrent_hset_isolated_from_inflight_search [M3 / Reject snapshot_straddle] GREEN-PIN:
    issue an HSET concurrent with a search / assert the search result reflects the pre-write snapshot,
    AND the post-write doc IS visible to the NEXT search (write isolated, not lost). Green now
    (atomic today); the build's yield must keep it green (becomes the real interleave guard).
  - m4_ft_basic_correctness_smoke [M4 no-regression] GREEN-PIN: FT.CREATE/HSET/FT.SEARCH/FT.INFO
    num_docs basic correctness holds (a fast guard that the async-ripple build didn't break FT.*).

  # --- verify-time (not authored as unit tests; measured at §6) ---
  - M0 baseline + M1 wall-clock p99 (relative, best-of-N, monoio+tokio), M4 consistency 197/197 @1/4/12,
    clippy ×2 + fmt, audit-unsafe/unwrap (zero new), RSS-flat + lock-count (zero new cross-thread lock):
    recorded as VERIFY evidence per the milestone bench rule (CONVENTIONS: confirm instrument validity;
    full-dual-runtime gate; at-BUILD safety audit). G-HOTPATH (no per-chunk alloc) is checked by the
    no-alloc audit + manual review of the resume path, not a unit test.
</test_plan>

Green-pins reused (must stay green, not authored here): `tests/ft_search_concurrent_readers.rs`,
`tests/ft_search_as_of_*.rs`, `tests/ft_search_temporal_parity.rs` (existing MVCC/as-of isolation).

Tests live in: `tests/ft_search_yield_red.rs` `tests/ft_search_yield_red_api.rs` · MUST run red (missing seam / un-relieved stall) before Build.
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

- [x] all tests pass — §4 red suite GREEN on **both runtimes**: m1 (yields cooperatively,
      INFO `ft_search_cooperative_yields_total` > 0) flipped RED→GREEN on monoio (macOS +
      OrbStack VM) AND tokio+graph; m2 (top-k known neighbors), m3 (write visible+consistent),
      m4 (smoke) green pins hold on both; api compile-red 3/3 both runtimes.
- [x] coverage did not decrease — added the §4 suite (5 runtime + 3 compile-shape tests);
      regression suites unchanged and GREEN on tokio: `txn_ft_search_snapshot` (MVCC, 3),
      `ft_search_as_of_filter`/`_boundary` (AS_OF, 1+3), `lunaris_hybrid_ft_search`
      (HYBRID→Sync, 1), `ft_search_concurrent_readers` (2), `vector_edge_cases` (16).
- [x] no test or contract was altered during build — §3 contract FROZEN @ v1 untouched; the
      only test-file change is cargo-fmt whitespace on `tests/ft_search_yield_red.rs`
      (verified: zero assertion/comparison lines changed). No red driver weakened.
- [x] concurrency / timing of the risky operation is safe — the seam captures an OWNED
      `SearchSnapshot` (Arc-clone of the segment list + owned query/committed/scratch) under
      one `&mut idx` borrow BEFORE the first yield; it holds NO borrow into VectorStore/Index
      across `.await` (borrow-checker-enforced: the capture closure returns before any await).
      G-IDENTITY proof: the mutable segment is APPEND-ONLY (entries are only `.push`ed; deletes
      set `delete_lsn` in place), so a chunked scan over the START-captured `[0, mutable_len)`
      with the captured `snapshot_lsn` is byte-identical to the atomic sync scan — during-yield
      appends land beyond `mutable_len` (invisible), during-yield deletes carry
      `delete_lsn > snapshot_lsn` (still-visible, matching sync). Empirically confirmed by
      m2/m3 + `txn_ft_search_snapshot` MVCC snapshot tests under tokio. No new cross-thread
      lock. `cooperative_yield()` is `#[cfg]`-split per runtime (C4) — tokio:
      `tokio::task::yield_now()`; monoio: `monoio::time::sleep(Duration::ZERO)` (registers a
      thread-local `TimerEntry`, returns Pending → the search task PARKS so monoio's run loop
      empties its ready queue and reaps the io_uring CQ — see effectiveness section). BOTH
      primitives are thread-local: no cross-thread waker, no cross-thread lock.
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new crates; the
      capture parses the same client-supplied KNN args the legacy path already parsed; no
      new I/O, no logging of query content. audit-unsafe 218/218 (0 new unsafe blocks),
      audit-unwrap 0 new (a `match` replaced one annotated `as_of_lsn_opt.unwrap()`).
- [x] layering & dependencies follow CONVENTIONS.md — capture lives in the command layer
      (`command/vector_search/ft_search/dispatch.rs`), the seam in the engine layer
      (`vector/segment/holder.rs`), yield primitive in `runtime/` (runtime-abstracted, C4).
      The `Box<SearchSnapshot>` is ONE alloc per FT.SEARCH command (clippy::large_enum_variant
      fix) — NOT per-key/per-chunk, so G-HOTPATH (no per-chunk alloc on the resume path)
      holds; IVF query buffers are allocated once per query before the segment loop. The Box
      is covered by the §3 SAFETY-NET clause (one per-query alloc authorized). The C3 default
      `FT_SEARCH_YIELD_BUDGET.max_brute_force_vecs_per_chunk` is tuned to **16384** (the monoio
      timer-yield knee, see effectiveness section), operator-overridable via the
      `MOON_FT_YIELD_CHUNK` env var resolved once in `ft_search_yield_budget()` (OnceLock; no
      per-query parse). The env knob is a no-RSS, no-lock read of process env at first use.
- [ ] a person reviewed and approved the change — **gate escalated to human** (concurrency +
      architecture residue; not auto-PASS per run.md). Pending.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — every new symbol is referenced:
      `ft_search_capture` + `FtSearchPlan` → re-exported in `vector_search/mod.rs` and called
      in BOTH `handler_monoio/ft.rs:756,764` and `handler_sharded/ft.rs:659,667`;
      `SegmentHolder::search_mvcc_yielding` → called in both handlers' Yield arm;
      `SearchSnapshot`/`YieldBudget`/`FT_SEARCH_YIELD_BUDGET`/`load_full` → used by the seam +
      capture + handlers; `cooperative_yield` → called in the seam; INFO counter
      `ft_search_cooperative_yields_total` → written in `connection.rs`, asserted by m1.
      All confirmed via grep reference search + the m1 GREEN signal (the counter only bumps if
      the live wire path executed).
- [x] DEAD-CODE (code) — no new orphan: the old synchronous FT.SEARCH branch was REMOVED from
      both `with_shard` closures (not left behind); `handler_single.rs` left intentionally
      unwired and DOCUMENTED (legacy `run_with_shutdown`/embedded path — Mutex-guarded shared
      store, task-per-connection, no shard event loop; off the moon-binary FT.SEARCH path,
      `main.rs` calls `run_sharded` only). clippy -D warnings (both runtimes) reports zero
      unused/dead-code.

### Build-shape findings (recorded; none altered the frozen §3 contract)
1. Dispatch is SYNCHRONOUS (`with_shard` closures + `handle_shard_message_shared`), not
   "already async" as the §3 build-note assumed. Resolved within frozen guarantees: the async
   seam sits at the connection-handler layer; cross-shard scatter targets stay sync (§1-OUT).
2. Mutable segment mutates in place (freeze-first isolation risk). Resolved: the append-only
   invariant makes the chunked captured-length scan byte-identical to sync — no data copy, no
   RSS growth (Arc refcount bump only).
3. §3 C5 prose floated a tick/drain proxy counter; the frozen §4 test + build use a dedicated
   `ft_search_cooperative_yields_total` counter — a stronger, deterministic anchor, consistent
   with C4. Not a contract change.
4. clippy::large_enum_variant → boxed the `Yield` snapshot (one per-query alloc; §3 SAFETY-NET).

### M1 effectiveness — MEASURED, a monoio defect found + fixed (full record: `tmp/bench_ftsearch/RESULTS.md`)
Disk was cleaned (host freed to 26Gi; builds moved to VM home volume) and M1 effectiveness WAS
benchmarked on moon-dev (1-shard, 99k×768d brute-force mutable, COMPACT_THRESHOLD 100000, no
compaction; co-located PING latency while N threads loop a heavy FT.SEARCH). This surfaced the §1
lowest-confidence assumption as a REAL defect and drove the user's "fix monoio yield, re-bench"
decision (2026-06-15):

1. **First bench exposed a monoio-only defect.** The original `cooperative_yield()` = self-wake
   (`waker.wake_by_ref()` + Pending). On monoio's single-threaded io_uring loop the self-woken
   search task re-enters the ready queue every poll, so the loop never drains → never reaps the
   io_uring CQ → the co-located PING's read completion is never reaped until the search finishes.
   Result: co-located p99 ≈ T_search (68ms ≈ 44ms sync) — the yield fired (counter +386/search)
   but relieved NOTHING. On tokio the SAME code achieved the goal for free (p99 6ms « 63ms search)
   because tokio's scheduler pumps its I/O driver on an interval between task polls. **M1 was UNMET
   on monoio (the default production runtime).** → HARD-STOP back to build (Reject `no_yield_progress`).

2. **The fix: runtime-split `cooperative_yield()` (C4).** tokio keeps `yield_now()`; monoio uses
   `monoio::time::sleep(Duration::ZERO)` — registers a `TimerEntry`, returns Pending → the search
   PARKS → monoio's task queue empties → the run loop `park()`s → the io_uring CQ is reaped
   (co-located reads serviced) → the expired timer re-wakes the search. Verified this reaps the CQ.
   Cost: each monoio `sleep(ZERO)` ≈ 1.4ms (timer-wheel granularity), so the chunk must be COARSE.

3. **Re-bench (monoio, post-fix) — GOAL NOW MET.** Chunk sweep found the knee at
   `max_brute_force_vecs_per_chunk = 16384` (new default): co-located PING p99 **6.6ms** (1-thread)
   / **27ms** (3-thread saturated) vs sync **48ms / ~300ms** — **~7–11× relief**. The 1ms tick now
   fires + `drain_spsc_shared` runs mid-search. tokio gets the same relief for free.

**Trade-off (the §1 tuning flag, now quantified — monoio only):** the timer-yield is not free.
Heavy brute-force searches over a large *uncompacted* mutable segment pay ~**+19% latency / −22%
search QPS** at chunk=16384 (coarser chunk=32768 → −14% QPS / 14.9ms p99; finer 1024 → −82% QPS /
0.9ms p99 — full sweep in RESULTS.md, `MOON_FT_YIELD_CHUNK`-tunable). **Light/HNSW searches whose
total work is < one chunk never yield → ZERO cost** — the tax falls only on the transient
heavy-brute-force window before background compaction installs the HNSW graph. tokio pays ~0.

### GATE RECORD
Outcome: PASS — M1 MET on BOTH runtimes (monoio co-located p99 6.6ms/27ms vs sync 48/300ms,
  ~7–11× relief; tokio 6ms for free). Correctness + MVCC + dual-runtime + regression green;
  zero new unsafe/cross-thread-lock/RSS. Operating point CONFIRMED by human at the gate:
  default `max_brute_force_vecs_per_chunk = 16384` (the benchmarked knee — strongest co-located
  relief). Residual: monoio's −22% heavy-search QPS at this chunk is a documented, env-tunable
  (`MOON_FT_YIELD_CHUNK`) trade-off that falls ONLY on the transient uncompacted-brute-force
  window (light/HNSW searches never yield → zero cost) — accepted as a non-defect trade, not a
  RISK-ACCEPTED gap. The monoio yield defect found by the verify bench was FIXED (commit 7c4f8cd,
  runtime-split timer-park), not deferred.
If RISK-ACCEPTED -> owner: n/a · ticket: n/a · expires: n/a   (never for a security gap)
Reviewed by: Tin Dang · date: 2026-06-15

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors):
- `ft_search_cooperative_yields_total` (INFO) — the C5 proxy; a heavy FT.SEARCH that yields 0×
  on monoio = regression of the timer-park primitive (e.g. a monoio bump that changes
  `sleep(ZERO)` semantics) → the M1 win silently lost. Alert if heavy searches stop yielding.
- co-located command p99 on a shard running FT.SEARCH (M1/M0 monitor) — re-run the
  `tmp/bench_ftsearch` probe on real hardware to confirm the VM-measured 7–11× relief holds.
- heavy-search QPS vs the −22% budget at chunk=16384 — if a real-disk/multi-core box shifts the
  knee, retune `MOON_FT_YIELD_CHUNK` (the env knob exists precisely for this).

Spec delta for the next loop:
- The monoio yield cost (~1.4ms/`sleep(ZERO)`, timer-wheel-bound) is a real tax on heavy
  brute-force search throughput. A FUTURE cost-free monoio yield — a self-pipe/NOP io_uring op
  that pumps the CQ without a timer round-trip — would recover the −22% QPS. Out of scope here;
  candidate v3 perf item (mirrors xshard-read-fastpath's deferred RCU residual).
- Absolute co-located-p99 stays VM-untrusted (the §1 instrument flag held); GCloud bare-metal
  absolute validation is the same deferred/optional item the milestone already carries for xshard.

### Competency deltas
<!-- all 3 folded → foundation-version 3 (PROJECT.md changelog 2026-06-15; CONVENTIONS.md TDD×1 + ADD×2) -->
- [TDD · folded] A full-green test suite does NOT prove a perf MECHANISM is EFFECTIVE — the §4
  m1 counter test was green on monoio while the yield relieved nothing (co-located p99 ≈ full
  search). Only the §6 effectiveness benchmark caught the no-op. Lesson: for a perf Must, a
  mechanism-fired counter (proxy) and an effect-measured benchmark are DIFFERENT gates; the
  proxy can pass while the effect is absent. (evidence: m1 green + RESULTS.md "GOAL UNMET" pre-fix.)
- [ADD · folded] A runtime-abstracted primitive (`#[cfg]`-split `cooperative_yield`) needs
  per-runtime EFFECTIVENESS validation, not just per-runtime COMPILE+CORRECTNESS. The same
  self-wake code was correct on both runtimes but effective only on tokio — monoio's io_uring
  loop never reaps the CQ under a self-waking task. Lesson: when a contract guarantee (C4 "both
  runtimes") rests on scheduler behavior, the verify plan must measure the behavior on EACH
  runtime, not assume parity from shared code. (evidence: tokio p99 6ms vs monoio 68ms, identical code.)
- [ADD · folded] The verify-time benchmark earned a HARD-STOP→build→re-verify cycle WITHIN the
  task (not a deferral) because disk was cleaned and the instrument was made to resolve the
  signal — honoring the user's "gather real metrics for evidence" over the easier GATE-DEFER.
  Lesson: prefer making the instrument work over deferring the measurement when the deferral
  would hide a real defect. (evidence: monoio no-op would have shipped under the disk-full defer.)
