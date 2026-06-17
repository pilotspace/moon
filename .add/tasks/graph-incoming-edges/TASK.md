# TASK: CSR traversal returns Incoming / Both-direction edges post-compaction

slug: graph-incoming-edges · created: 2026-06-16 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: Immutable CSR traversal returns Incoming / Both-direction edges post-compaction
Framings weighed: derived in-memory reverse index (chosen) · on-demand reverse scan · persisted incoming CSR
  — chosen derived in-memory reverse index: build a reverse adjacency (dst-row → list of forward
    edge indices) from the CSR's EXISTING forward arrays (`col_indices` + `row_offsets`) the segment
    already exposes; lookup reuses the same `edge_meta`/`edge_created_ms`/`validity`/`node_meta` by
    edge index, so edge-type filter, tombstones, decay stamp, and deleted-node visibility are
    preserved with no new semantics. NO on-disk segment-format change → no recovery/migration risk
    (CLAUDE.md design-for-failure) AND keeps this task independent of the sibling
    graph-label-bitmap-overflow (which DOES touch node_meta's format). Built lazily + cached
    (interior mutability) so a graph that never runs an Incoming/Both query pays nothing.
    on-demand reverse scan (re-scan all rows' col_indices per query) = O(|E|) every query, rejected
    (needless re-work; the index is O(|E|) ONCE). persisted incoming CSR = a segment-format change
    (version bump + migration + recovery path) that would collide with the label task — rejected as
    out-of-proportion for a correctness fix where throughput is explicitly milestone-Out.
Must:
<must>
  - M1 On a COMPACTED graph (edges live only in immutable CSR segments, MemGraph empty for them),
    `Direction::Incoming` traversal of node N returns exactly the nodes with an edge → N (its
    predecessors), each as a `MergedNeighbor` — never empty where incoming edges exist. (Today the
    CSR branch `continue`s on Incoming, so post-compaction Incoming returns nothing: traversal.rs:191.)
  - M2 `Direction::Both` on a compacted graph returns the UNION of outgoing (successors) and incoming
    (predecessors) neighbors of N — today it returns only the outgoing half from CSR (silent
    under-return). Dedup is by NodeKey via the existing `seen` set (a node both predecessor AND
    successor appears once), matching the MemGraph `Both` behavior.
  - M3 An incoming edge carries the SAME per-edge attributes as the outgoing path: the edge-type
    filter (`edge_type_filter`) still applies, a tombstoned edge (cleared `validity` bit) is
    excluded, a target/source node deleted at/before the snapshot is excluded, and `edge_type` +
    `created_ms` are read from the same per-edge arrays by edge index (no fabricated values).
  - M4 Snapshot semantics unchanged: a CSR segment with `created_lsn > snapshot_lsn` is skipped for
    incoming exactly as for outgoing; incoming results merge with MemGraph incoming results and
    dedup across segments (newest-wins via `seen`), identical to the outgoing merge.
  - M5 `Direction::Outgoing` plans and results are BYTE-IDENTICAL to today — the reverse index is
    built/consulted only for Incoming/Both; the outgoing hot path adds no work and no allocation.
  - M6 The reverse index is DERIVED (not persisted): no change to `to_bytes`/`from_bytes`/segment
    header/version; an existing on-disk segment (heap OR mmap variant) loads unchanged and answers
    Incoming queries correctly. It is built at most once per segment and cached.
</must>
Reject:
<reject>
  - This feature adds NO new wire input or rejection — it makes an existing direction (Incoming/Both)
    correct on compacted data. A query for a node absent from a segment (`lookup_node` → None) is
    NOT an error: that segment simply contributes no incoming neighbors (a normal empty contribution,
    same as outgoing). No panic on any segment shape (empty col_indices, single node, self-loop).
</reject>
After:
<after>
  - For any compacted graph, Incoming/Both traversals return the same predecessor/union set that the
    pre-compaction MemGraph traversal returned for that direction — compaction is result-preserving
    across all three directions (today it silently drops the incoming half).
  - No segment-format/version change; heap + mmap segments both gain incoming lookup from their
    existing forward arrays; Outgoing is unchanged; the reverse index is lazy + cached.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 A DERIVED, lazily-built, cached in-memory reverse index (dst-row → forward-edge-indices, from
    `col_indices`+`row_offsets`) is the right representation — lowest confidence on WHERE the cache
    lives + memory cost: it adds ~2×u32 per edge per segment in RAM, and caching in the shared
    `Arc<CsrStorage>` needs interior mutability (`OnceLock`) since segments are read through `&`.
    If wrong (e.g. a huge-graph memory budget forbids the index, or `Arc` sharing makes lazy caching
    awkward): fall back to on-demand reverse scan (O(|E|)/query, correct but slow — acceptable since
    throughput is milestone-Out) or eager build at load. Either way correctness is unaffected; only
    the speed/memory trade moves. Resolve at contract: pin lazy-`OnceLock` cache; a scenario asserts
    Incoming is correct regardless of build strategy.
  ⚠ A2 Recovering each incoming edge's source NodeKey + attributes by FORWARD edge index is exact —
    edge index `e` maps to one (src_row via `row_offsets`, dst_row=`col_indices[e]`, `edge_meta[e]`,
    `edge_created_ms.get(e)`, `validity` bit `e`). Lowest confidence on the src_row recovery (must
    invert `row_offsets`, or store src_row alongside `e` in the index). If wrong: an incoming edge
    attributed to the wrong predecessor. Resolve: store `(src_row, edge_idx)` pairs in the index so
    no inversion is needed; a scenario asserts the predecessor identity + edge_type.
  - [x] MemGraph already handles Incoming/Both correctly (keeps per-node `incoming` adjacency,
    memgraph.rs:225-229) — so the defect and the fix are CSR-immutable-only; MemGraph path untouched.
  - [x] `CsrStorage` (Heap|Mmap) exposes `col_indices`/`row_offsets`/`edge_meta`/`node_meta`/
    `edge_created_ms`/`is_valid` publicly — enough to build + serve the reverse index for both
    variants without reaching into the on-disk layout.
  - [x] Graph traversal is NOT a CLAUDE.md hot path (not dispatch/protocol/event-loop/io); an O(|E|)
    one-time index build + per-query Vec is acceptable; no new `unsafe`.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# Compacted corpus G: nodes A,B,C,D. Edges A-[R1]->C, B-[R2]->C, C-[R1]->D.
# Frozen into ONE immutable CSR segment; MemGraph contributes nothing (post-compaction).
# Predecessors(C)={A,B}; Successors(C)={D}; Predecessors(D)={C}; Predecessors(A)={}.

Scenario: Incoming returns predecessors post-compaction  (M1 — the headline bug)
  Given corpus G compacted into a CSR segment (snapshot_lsn >= segment.created_lsn)
  When I read neighbors of C with Direction::Incoming
  Then exactly {A, B} are returned (C's predecessors)
  And the result is NOT empty (today the CSR branch continues past Incoming -> 0 neighbors)

Scenario: Both returns the union of successors and predecessors  (M2)
  Given corpus G compacted
  When I read neighbors of C with Direction::Both
  Then exactly {A, B, D} are returned (predecessors {A,B} ∪ successors {D})
  And a node that is both a predecessor and a successor appears once (NodeKey dedup)

Scenario: Incoming edges honor edge-type filter, tombstones, and node visibility  (M3)
  Given corpus G compacted
  When I read Incoming neighbors of C with edge_type_filter = R1
  Then only {A} is returned (B's edge is R2, excluded) and each result's edge_type = R1
  And after tombstoning the A-[R1]->C edge (clearing its validity bit), Incoming(C) = {B}
  And after deleting node A (deleted_lsn <= snapshot), Incoming(C) excludes A

Scenario: Incoming respects segment snapshot visibility  (M4)
  Given corpus G compacted into a segment with created_lsn = 10
  When I read Incoming neighbors of C at snapshot_lsn = 5
  Then the segment is skipped and contributes 0 incoming neighbors (same rule as outgoing)
  And at snapshot_lsn = 10 the segment contributes {A, B}

Scenario: Outgoing is byte-identical to today  (M5 — no regression)
  Given corpus G compacted
  When I read neighbors of C with Direction::Outgoing
  Then exactly {D} is returned, identical to the pre-change outgoing result
  And no reverse index is consulted for the Outgoing path

Scenario: Incoming works from a round-tripped segment with no format change  (M6)
  Given corpus G written via to_bytes and reloaded via from_bytes (heap variant)
  When I read Incoming neighbors of C
  Then {A, B} is returned — proving the incoming answer is DERIVED from the existing forward
       arrays, not from any newly-persisted incoming data
  And from_bytes parses the unchanged segment format/version without error

Scenario: Degenerate Incoming queries never panic  (Reject / robustness)
  # NOTE (found in tests): self-loops are UNREACHABLE — MemGraph::add_edge rejects src==dst with
  # `SelfLoop`, so a self-loop CSR segment cannot exist. The frozen §3 "no panic on any segment
  # shape" still holds (self-loop vacuously covered); we exercise the reachable degenerate shapes.
  Given corpus G compacted
  When I read Incoming neighbors of A (which has NO predecessors) and of a node absent from the segment
  Then both return an empty incoming contribution and neither query panics
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
Surface: graph traversal over a COMPACTED graph (immutable CSR segments). Internal seam frozen:
  SegmentMergeReader::neighbors_inner (the CSR branch) + a new incoming accessor on CsrStorage.

WIRE / OBSERVABLE BEHAVIOR  (per direction, for node N in a compacted segment)
  Direction::Incoming -> the set of N's predecessors {S : edge S->N live at snapshot}, each a
                         MergedNeighbor{ node:S, edge_type, created_ms }. (Was: empty — bug.)
  Direction::Both     -> predecessors ∪ successors, NodeKey-deduped via the existing `seen` set.
  Direction::Outgoing -> UNCHANGED (byte-identical to today).
  Edge-type filter, validity tombstones, segment snapshot (created_lsn > snapshot_lsn skips), and
  cross-segment + MemGraph merge/dedup apply to incoming EXACTLY as to outgoing (same arrays, by
  edge index). A node absent from a segment contributes nothing (not an error); no panic on any
  segment shape (empty, single-node, self-loop).

THE FROZEN SEAM
  1. Derived index type (NOT persisted):
       struct IncomingIndex { in_offsets: Vec<u32> /*len node_count+1*/,
                              in_pairs: Vec<(u32 src_row, u32 edge_idx)> /*len edge_count*/ }
     Built once from the segment's EXISTING forward arrays (`row_offsets` + `col_indices`): for each
     forward edge index e with src=row owning e and dst=col_indices[e], append (src, e) under dst.
     in_pairs stores src_row explicitly so no row_offsets inversion is needed (A2).
  2. Cache home: a `OnceLock<IncomingIndex>` field added to BOTH inner segment structs
     `CsrSegment` and `MmapCsrSegment` (3 constructor sites init it empty: mod.rs from_frozen +
     from_bytes, mmap.rs from_mmap_file). It is DERIVED + lazy (get-or-init on first incoming query)
     and EXCLUDED from `to_bytes`/`from_bytes` — NO segment header/version/format change, so existing
     on-disk segments load and answer incoming correctly, and this task stays independent of the
     sibling label-bitmap task. (Fallback if cache home is rejected: on-demand reverse scan, O(|E|)
     per query — correctness identical, throughput is milestone-Out.)
  3. New accessor mirroring the outgoing one:
       CsrStorage::for_each_incoming_edge_ms(dst_row: u32, f: impl FnMut(u32 src_row, EdgeMeta, u64))
     get-or-inits the variant's IncomingIndex, then for each (src_row, e) under dst_row with a set
     validity bit, yields (src_row, edge_meta[e], edge_created_ms.get(e).unwrap_or(0)).
  4. traversal.rs `neighbors_inner` CSR branch: remove the `if direction == Incoming { continue }`
     guard; run the EXISTING outgoing loop for Outgoing|Both, and for Incoming|Both additionally run
     for_each_incoming_edge_ms, resolving src_row -> NodeKey via node_meta[src_row].external_id with
     the SAME deleted-node visibility check + edge_type_filter + `seen` dedup as the outgoing loop.

SCHEMA / STATE
  Read-only over existing segments. NO new persisted structure, NO format/version bump, NO new index
  on disk. The only new state is an in-memory, lazily-derived, per-segment reverse index.

NAMES (glossary): Direction::{Outgoing,Incoming,Both} · SegmentMergeReader · CsrStorage ·
CsrSegment · MmapCsrSegment · IncomingIndex · for_each_incoming_edge_ms · for_each_neighbor_edge_ms ·
MergedNeighbor · NodeMeta.external_id · validity · edge_meta · edge_created_ms.
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-06-16)
Least-sure flag surfaced at freeze:
  ⚠ [contract] A1 — the cache home (a non-persisted `OnceLock<IncomingIndex>` on the two inner
    segment structs, lazy + ~2×u32/edge RAM) is the right place vs. the zero-cache on-demand reverse
    scan. Most likely wrong if a huge-graph memory budget forbids the per-segment index or the
    OnceLock-on-inner-struct caching reads awkwardly. Cost if wrong: only a speed/memory trade moves
    (correctness is identical either way); fallback = on-demand O(|E|)/query scan (throughput is
    milestone-Out). Pinned by scenario M6 (round-trip) + M1 (Incoming correct regardless of strategy).
  ⚠ [spec] A2 — storing `src_row` IN the index pair (not inverting `row_offsets` at query time) is
    what guarantees each incoming edge is attributed to the correct predecessor. If the build mis-maps
    e->src, an incoming edge points at the wrong node. Cost: wrong predecessor identity. Pinned by
    scenario M3 (predecessor identity + edge_type assertions).
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: ~90% of the new incoming path (IncomingIndex build + for_each_incoming_edge_ms +
the traversal.rs Incoming/Both branch). Tests at the traversal/CSR seam (the frozen layer), mirroring
the existing `src/graph/traversal.rs mod tests` harness (FrozenMemGraph -> CsrSegment::from_frozen ->
CsrStorage -> SegmentMergeReader). RED now: Incoming returns nothing / Both drops the incoming half.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  PLAN — src/graph/traversal.rs `mod tests` (build corpus G via FrozenMemGraph: A-[R1]->C, B-[R2]->C,
  C-[R1]->D; compact to ONE CsrStorage segment; MemGraph = None so only CSR contributes):
  - test_incoming_returns_predecessors_post_compaction (M1): Direction::Incoming of C -> {A,B}
    (NodeKeys), non-empty.  RED: today returns {} (the `continue`).
  - test_both_returns_union_of_pred_and_succ (M2): Direction::Both of C -> {A,B,D}, each NodeKey once.
    RED: today returns {D} only.
  - test_incoming_honors_edge_type_validity_and_node_visibility (M3): Incoming(C) with edge_type=R1
    -> {A} and result.edge_type==R1; after marking A->C deleted -> {B}; after deleting node A -> {B}.
  - test_incoming_respects_segment_snapshot (M4): segment created_lsn=10; Incoming(C) at snapshot 5
    -> {} (skipped); at snapshot 10 -> {A,B}.
  - [green-pin] test_outgoing_unchanged (M5): Direction::Outgoing of C -> {D} (guards no regression).
  - test_incoming_after_to_bytes_from_bytes_roundtrip (M6): round-trip the heap segment through
    to_bytes/from_bytes; Incoming(C) -> {A,B} (proves derived-not-persisted; format parses unchanged).
  - test_incoming_degenerate_no_panic (Reject, green-pin): Incoming(A) where A has no predecessors
    -> empty; a node absent from the segment -> empty; neither panics. (self-loop is unreachable —
    MemGraph rejects src==dst, so a self-loop segment can't exist; reachable shapes tested instead.)
</test_plan>

Tests live in: `src/graph/traversal.rs` · MUST run red (missing implementation) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): the lazy index cache must be race-free — `OnceLock::get_or_init`
serializes concurrent first-callers (the build is idempotent: same forward arrays → same index), so
two threads racing an Incoming query on a fresh segment both observe the same index, no torn state.
The derived index is read-only after init; no `unsafe`; the mmap `unsafe impl Send/Sync` stays valid
(OnceLock<IncomingIndex> is Send+Sync).
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — graph lib 402/0 (incl. the 5 incoming/both tests red→green, 2 green-pins,
  2 new IncomingIndex unit tests); broad integration 28/0 across 6 suites including graph_segment_merge
  (compaction→CSR), lunaris_cypher_shortest_path/temporal, txn_cypher_write_rollback, txn_graph_wiring.
- [x] coverage did not decrease — 7 traversal + 2 index unit tests ADDED; none removed/weakened.
  (The §2/§4 self-loop case was corrected DURING the tests phase — self-loops are unreachable, the
  test was red for the wrong reason — not weakened during build.)
- [x] no test or contract was altered during build — build touched only src (traversal.rs,
  csr/incoming.rs[new], csr/{mod,mmap,storage}.rs, compaction.rs); §3 contract and the test
  assertions unchanged.
- [x] concurrency / timing — the lazy cache uses `OnceLock::get_or_init` (race-free, idempotent
  build); index is read-only post-init; no lock held across await (no async here); mmap Send/Sync
  invariants preserved (OnceLock<IncomingIndex> is Send+Sync).
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new crate; pure
  in-memory derivation from existing arrays; no `unsafe` added.
- [x] layering & dependencies follow CONVENTIONS.md — no new PhysicalOp/format/version; reverse index
  derived from public CSR accessors; mirrors the existing for_each_neighbor_edge_ms + ShortestPath
  filter patterns. Not a hot path (graph traversal), so the one-time O(|E|) build is acceptable.
- [x] a person reviewed and approved the change — contract FROZEN @ v1 by Tin Dang; AUTO-RESOLVED
  verify (autonomy: auto): non-security, non-concurrency-residue, non-architecture; complete
  red→green evidence at unit + integration + clean clippy/fmt.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — IncomingIndex is built/consumed by CsrStorage::for_each_incoming_edge_ms,
  which traversal.rs neighbors_inner calls for Incoming|Both; the OnceLock field is populated via
  incoming_index() get-or-init. Confirmed at EFFECT level: integration + unit tests observe the
  predecessor sets that only a wired, evaluated reverse index can produce (Incoming {} → {A,B}).
- [x] DEAD-CODE (code) — every new symbol is referenced: IncomingIndex{build,incoming} (storage.rs),
  for_each_incoming_edge_ms (traversal.rs), incoming field (built lazily on first incoming query).
  No orphaned symbol (clippy --all-targets on the graph feature surfaced no dead-code warning).
- [x] SEMANTIC — n/a (code change; no frozen prose artifact).

### GATE RECORD
Outcome: PASS  (auto-resolved on complete two-level red→green evidence; no security/concurrency/
  architecture residue — A1 cache-home pinned green by M6 round-trip + the 402/0 incl. graph_segment_merge;
  A2 predecessor identity pinned green by the M3 identity + edge_type assertions)
If RISK-ACCEPTED -> owner: — · ticket: — · expires: —   (n/a)
Reviewed by: ADD auto-gate (autonomy: auto) · date: 2026-06-16

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): <error rate / per-rejection rate / latency>
Spec delta for the next loop: <what production taught you>

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->
