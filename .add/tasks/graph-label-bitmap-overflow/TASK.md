# TASK: Node label storage supports label ids >= 32 without silent truncation

slug: graph-label-bitmap-overflow · risk: high · autonomy: conservative · created: 2026-06-16 · stage: production
phase: done   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- risk: high — this is the only v3-2 task that changes the on-disk CSR segment format
     (new version-gated section + recovery/mmap reload path). autonomy: conservative -> verify
     escalates to a human gate (no unguarded auto-PASS on a persistence-format change). -->>
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower
     the autonomy level with `autonomy: conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: CSR node labels with id >= 32 are stored, persisted, and matched (no silent truncation)
Framings weighed: version-gated overflow section (chosen) · widen bitmap u32->u64 · per-node label list
  — chosen overflow section (user-approved 2026-06-16): keep NodeMeta's fixed 48-byte layout + the
    u32 `label_bitmap` as the fast path for labels 0-31; add a NEW version-gated segment section that
    stores, per node row, the labels >= 32. Mirrors EXACTLY how `edge_created_ms` was added at
    version 3 (a header offset field + a positionally-computed section gated on `version`). Unbounded
    (u16 label ids), and BACKWARD-COMPATIBLE — old (version <= 3) segments have no section and load
    unchanged. `LabelIndex` (the query structure) is built from BOTH the bitmap and the overflow.
    widen u32->u64 — rejected: only moves the cliff (32 -> 64), AND changes NodeMeta's size, breaking
      the compile-asserted 48-byte stride for every existing heap + mmap segment (hard migration).
    per-node variable label list (replace the bitmap) — rejected: a larger semantic change to
      NodeMeta + loses the O(1) 0-31 fast path for no extra correctness.
Must:
<must>
  - M1 A node assigned a label id >= 32 (e.g. 32, 40, 1000; ids are `u16`) is STORED — at build
    (`from_frozen`) the label is no longer dropped by the `if label < 32` guard; labels 0-31 still go
    to the u32 `label_bitmap`, labels >= 32 go to the new overflow store.
  - M2 A label query for an id >= 32 (`MATCH (a:Label40)`, or `LabelIndex::nodes_with_label(40)`)
    returns exactly the rows carrying it — `LabelIndex` is built from the bitmap (0-31) AND the
    overflow (>= 32), so all u16 labels are matchable. A 40-distinct-label graph matches every label.
  - M3 Labels >= 32 SURVIVE a persistence round-trip: `to_bytes`/`from_bytes` (heap) and the mmap
    `from_mmap_file` reload restore the overflow store, so a compacted-then-reloaded graph still
    matches label >= 32. The on-disk format is bumped to CSR version 4 with the new section.
  - M4 Labels >= 32 survive COMPACTION/segment-merge: when `compact_segments` builds a merged
    segment, each surviving node's overflow labels are carried to the merged segment under its new
    row index (re-mapped), exactly like the bitmap is carried today.
  - M5 BACKWARD COMPATIBILITY: a version <= 3 segment (no overflow section) loads with an EMPTY
    overflow store and behaves exactly as today (labels were 0-31 only). No existing on-disk segment
    is invalidated; NodeMeta's 48-byte layout and the u32 bitmap are unchanged.
  - M6 Labels 0-31 are BYTE-IDENTICAL in behavior and on the fast path — the bitmap path, its
    `LabelIndex` entries, and existing label queries are unchanged (no regression for the common case).
  - M7 The overflow store is SPARSE — only nodes that actually carry a label >= 32 occupy space;
    a graph with no >= 32 labels writes an empty/zero-length section (and may set offset 0).
</must>
Reject:
<reject>
  - No new wire rejection — this makes a previously-dropped label apply. Malformed/truncated overflow
    section bytes on load -> the existing CSR deserialization error path (`CsrError`), never a panic
    (parser defensiveness); a checksum already covers the segment. A label id that simply isn't
    present -> `nodes_with_label` returns None (normal empty result, not an error).
</reject>
After:
<after>
  - Node label storage is correct for all u16 label ids across the full lifecycle (build -> index ->
    query -> persist -> reload -> compact). The 32-label cliff is gone; labels 0-31 keep the u32
    fast path; >= 32 live in a sparse, version-gated, backward-compatible section.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ A1 The mmap reload path can serve the overflow store without breaking zero-copy — lowest
    confidence: NodeMeta is raw-cast from mmap (fixed 48-byte stride, UNCHANGED here), but the new
    overflow section is variable-length; the mmap variant must parse it into an OWNED store (like
    `validity`/`node_id_to_row` are owned, not pointers) rather than cast. If wrong (e.g. I try to
    zero-copy a variable section): mmap load breaks or mis-reads. Resolve: store overflow as an OWNED
    `HashMap<u32,SmallVec<u16>>` on BOTH segment variants, parsed at load; a scenario reloads via mmap.
  ⚠ A2 Compaction row-remap for overflow labels is correct — lowest confidence: merge reassigns CSR
    rows, and the bitmap is copied per-surviving-node; the overflow store is keyed by row, so it must
    be re-keyed to the NEW row, not the old. If wrong: a node's >= 32 labels attach to the wrong row
    after merge. Resolve: re-key overflow in the same loop that copies node_meta during merge; a
    scenario compacts a >= 32-label graph and re-queries.
  - [x] GraphSegmentHeader has spare padding (88 used / 128 on-disk) for a `label_overflow_offset:
    u64` field with NO on-disk size change — same trick used for `edge_created_ms_offset` at v3.
  - [x] The label QUERY path uses `LabelIndex::nodes_with_label` (Roaring), not the per-node u32
    bitmap directly (`neighbors_by_label` + NodeScan label filter route through LabelIndex) — so
    fixing LabelIndex's source fixes the query for all labels.
  - [x] CSR build/query is NOT a CLAUDE.md hot path; a HashMap overflow store + one extra section
    parse is acceptable; no new `unsafe` (the existing mmap unsafe is unchanged — overflow is owned).
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
# Labels are u16 ids. A "wide-label" node carries an id >= 32 (e.g. 40); the bitmap caps at 0-31.

Scenario: A label id >= 32 is stored, not dropped  (M1)
  Given a FrozenMemGraph with node N carrying labels {5, 40}
  When I build a CSR segment (from_frozen)
  Then label 5 is in N's u32 bitmap AND label 40 is in the overflow store for N's row
  And the build does NOT silently drop label 40

Scenario: A label query for an id >= 32 matches  (M2 — the headline)
  Given a CSR segment built from 40 nodes, node i carrying label id i (0..40)
  When I query LabelIndex::nodes_with_label(40) (and 32, 33, …)
  Then exactly the node(s) carrying that id are returned for EVERY id 0..40
  And no label in 0..40 returns an empty/None where a node carries it (no silent drop)

Scenario: Labels >= 32 survive a heap round-trip  (M3 — persistence)
  Given a CSR segment with a node carrying label 40
  When I serialize via to_bytes and reload via from_bytes
  Then the reloaded segment's LabelIndex still matches label 40 to that node
  And the segment version is 4 and the checksum validates

Scenario: Labels >= 32 survive an mmap reload  (M3 / A1 — zero-copy boundary)
  Given a CSR segment with a node carrying label 40 written to a file
  When I reload it via CsrStorage::from_file (mmap variant)
  Then label 40 still matches that node (the overflow store is parsed into an OWNED map, not cast)

Scenario: Labels >= 32 survive compaction / segment-merge  (M4 / A2 — row remap)
  Given two segments whose merged result re-assigns CSR rows, a surviving node carrying label 40
  When I compact them into one segment
  Then label 40 matches the surviving node under its NEW row index in the merged segment

Scenario: An old (version <= 3) segment loads unchanged  (M5 — backward compat)
  Given on-disk bytes of a version-3 segment (no overflow section, labels 0-31 only)
  When I load it
  Then it loads without error, labels 0-31 match as before, and the overflow store is empty
  And NodeMeta's 48-byte layout and the u32 bitmap are unchanged

Scenario: Labels 0-31 are unchanged on the fast path  (M6 — no regression)
  Given a CSR segment whose nodes carry only labels 0-31
  When I query any of those labels
  Then results are identical to before this change, and no overflow section work occurs

Scenario: A graph with no >= 32 labels writes an empty section  (M7 — sparse)
  Given a CSR segment whose nodes carry only labels 0-31
  When I serialize and reload it
  Then the overflow section is empty/zero-length (offset may be 0) and round-trips cleanly

Scenario: Malformed overflow bytes never panic  (Reject — defensiveness)
  Given segment bytes whose overflow section is truncated/corrupt
  When I attempt from_bytes
  Then it returns a CsrError (the existing deserialization error path), it does NOT panic, and the
       server stays up
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
Surface: CSR node-label storage + query (LabelIndex). Internal seams frozen: the on-disk segment
format (version 4), CsrSegment/MmapCsrSegment label storage, and LabelIndex::build.

ON-DISK FORMAT (version 4 — additive, gated on header.version)
  CSR_CURRENT_VERSION: 3 -> 4.
  GraphSegmentHeader gains `label_overflow_offset: u64` (uses existing 128-byte padding; on-disk
  header size UNCHANGED — mirrors `edge_created_ms_offset` added at v3). 0 / absent for version <= 3.
  NEW trailing section "label overflow" (written after the edge_created_ms section, covered by the
  existing trailing CRC32 checksum):
      [entry_count: u32]
      repeated entry_count times, ascending by row:
        [row: u32][label_count: u16][label_id: u16] * label_count   // only labels >= 32
  Only rows with >= 1 overflow label appear (SPARSE). entry_count = 0 -> empty section.
  Version <= 3 segments: section absent, parsed as empty (M5 backward compat).

IN-MEMORY (both segment variants own the parsed store — NOT zero-copy, A1)
  CsrSegment      gains: pub label_overflow: HashMap<u32 /*row*/, SmallVec<[u16; 4]> /*labels>=32*/>
  MmapCsrSegment  gains: the SAME owned `label_overflow` field (parsed at from_mmap_file; the mmap
  raw-pointer fields are unchanged — overflow is heap-owned, so the mmap unsafe Send/Sync is intact).
  CsrStorage forwards `label_overflow()` for both variants.

BUILD / INDEX
  from_frozen: for each node, partition `node.labels` — id < 32 -> set bit in the u32 `label_bitmap`
    (unchanged); id >= 32 -> push into `label_overflow[row]`. (Removes the silent drop at mod.rs:161.)
  LabelIndex::build signature -> build(node_meta: &[NodeMeta], overflow: &HashMap<u32, SmallVec<[u16;4]>>):
    index every set bitmap bit (0-31, as today) AND every (row, label) in overflow (>= 32) into the
    same `HashMap<u16, RoaringBitmap>`. nodes_with_label(id) then resolves ALL u16 ids.
  All from_frozen / from_bytes / from_mmap_file call the new build with the parsed overflow.

PERSIST / RELOAD / MERGE
  to_bytes: emit the section + set header.label_overflow_offset (version 4).
  from_bytes + from_mmap_file: parse the section when version >= 4 (else empty); bounds-checked,
    returns CsrError on truncation (never panics).
  compact_segments: when copying a surviving node's row into the merged segment, re-key its
    label_overflow entry from old row -> NEW row (same loop that carries the bitmap). (A2)

SCHEMA / STATE
  NodeMeta layout UNCHANGED (fixed 48 bytes, u32 bitmap retained as the 0-31 fast path). One new
  header field (no size change), one new trailing section, one new owned per-segment map. No new
  index type on disk (LabelIndex is still rebuilt at load, now from both sources).

NAMES (glossary): label_bitmap · label_overflow · LabelIndex::{build,nodes_with_label} ·
GraphSegmentHeader.label_overflow_offset · CSR_CURRENT_VERSION · CsrSegment · MmapCsrSegment ·
CsrStorage · compact_segments · from_frozen · from_bytes · from_mmap_file.
```

Status: FROZEN @ v1 — approved by Tin Dang (2026-06-16, representation chosen via decision prompt)
Least-sure flag surfaced at freeze:
  ⚠ [contract] A1 — the mmap variant must OWN the parsed overflow map (not zero-copy it), since the
    section is variable-length while NodeMeta stays a fixed raw-cast stride. Most likely wrong if I
    try to point into the mmap; cost: mmap reload breaks/mis-reads. Pinned by the mmap-reload scenario.
  ⚠ [spec] A2 — compaction must re-key overflow from OLD row -> NEW row in the merge loop. If the
    re-key is missed/wrong, a node's >= 32 labels attach to the wrong row post-merge. Pinned by the
    compaction scenario (build a >= 32-label graph, merge, re-query under the new row).
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: ~90% of the new label-overflow path (from_frozen partition, section
serialize/parse on heap+mmap, LabelIndex::build over both sources, compaction re-key). Tests at the
CSR/index seam (the frozen layer), mirroring existing `src/graph/csr/mod.rs` round-trip tests and
`src/graph/index.rs` LabelIndex tests. RED now: labels >= 32 are dropped at build (`if label < 32`).

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  CSR/index unit tests — src/graph/csr/mod.rs + src/graph/index.rs `mod tests`:
  - test_label_over_32_is_stored (M1): from_frozen node with labels {5,40} -> bitmap has bit 5,
    label_overflow[row] contains 40. RED: 40 dropped, overflow store doesn't exist yet.
  - test_label_index_matches_ids_0_to_39 (M2, headline): 40 nodes, node i carries label i; assert
    nodes_with_label(i) returns {i} for ALL i in 0..40 (esp. 32..40). RED: ids >= 32 return None.
  - test_overflow_survives_heap_roundtrip (M3): to_bytes -> from_bytes; nodes_with_label(40) still
    matches; assert header.version == 4 and checksum validates.
  - test_overflow_survives_mmap_reload (M3/A1): write_to_file -> CsrStorage::from_file (Mmap variant);
    nodes_with_label(40) matches (owned parse, not cast).
  - test_overflow_survives_compaction (M4/A2): build two segments, compact; a surviving wide-label
    node still matches label 40 under its NEW merged row.
  - [green-pin] test_labels_0_31_unchanged (M6): a 0-31-only segment matches its labels identically;
    a query for an absent id returns None (green now AND after).
  - test_no_wide_labels_empty_section (M7): a 0-31-only segment round-trips with an empty overflow
    section (entry_count 0). (green-pin: must hold once the section exists.)
  - test_malformed_overflow_bytes_error_no_panic (Reject): truncate the overflow section of a
    serialized v4 buffer; from_bytes returns Err(CsrError), does NOT panic.
  Backward-compat (M5): assert a buffer whose header.version=3 and which omits the section parses to
  an EMPTY overflow store with labels 0-31 intact (hand-craft a v3 buffer or force version=3 on
  serialize); no error.
</test_plan>

Tests live in: `src/graph/csr/mod.rs` `src/graph/index.rs` · MUST run red (missing implementation) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): the new on-disk section is parsed ONLY through the
shared bounds-checked `parse_label_overflow` (read2/read4 → `CsrError`, never panic);
rows are range-checked (`< node_count`) and strictly ascending so a corrupt-but-
valid-CRC header cannot drive an unbounded read. No new `unsafe`; the existing mmap
unsafe is untouched (overflow is heap-owned, not zero-copied).
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

Implemented (per the FROZEN @ v1 contract):
- `types.rs`: `CSR_CURRENT_VERSION 3 → 4`; `GraphSegmentHeader.label_overflow_offset: u64`
  (reuses 96→128 padding; size assert still 128).
- `index.rs`: `LabelIndex::build(node_meta, overflow)` indexes the u32 bitmap (0–31)
  AND the overflow map (≥32) into one `HashMap<u16, RoaringBitmap>`.
- `csr/mod.rs`: `CsrSegment.label_overflow: HashMap<u32, SmallVec<[u16;4]>>`; `from_frozen`
  partitions labels (<32→bitmap, ≥32→overflow[row]); `to_bytes` emits the section +
  sets the header offset (byte 88); `from_bytes` parses it via the shared helper.
- `csr/mmap.rs`: same owned field; `from_mmap_file` parses the section into an owned
  map (A1) — raw-pointer fields unchanged.
- `csr/storage.rs`: `CsrStorage::label_overflow()` forwards both variants.
- `compaction.rs`: re-keys overflow old-row → new merged-row, coherently with the
  winning node_meta (same created_lsn tie-break) (A2).

Test-surface updates forced by the CONTRACTED version bump (NOT weakening — assertions
stay equally strong; the backward-compat tests remain and now evidence M5):
- 6 fresh-segment assertions `version == 3` → `4` (basic/roundtrip/temporal/stamped).
- `downgrade_to_v2` strips the extra 4-byte empty overflow trailer + zeroes byte 88;
  v1-migration source-version assertion `3 → 4` + comment.
- `test_overflow_survives_mmap_reload`: fixture changed from edgeless (nc=2/ec=0 →
  node_meta byte 140, NOT 8-aligned → silent HEAP fallback) to nc=3/ec=2 (8-aligned)
  and now asserts `matches!(CsrStorage::Mmap(_))` so it genuinely exercises the mmap
  overflow parse (the A1 risk). RED-via-mmap re-confirmed empirically.

<!-- EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — graph lib **409/409**; full lib **3399 passed / 1 ignored / 0 failed**
      (`--test-threads=2`; the first full-lib run was SIGKILL'd by an OOM in the untouched
      vector suite under default parallelism, NOT a regression — disk 22Gi free at the time).
      Graph integration: graph_segment_merge 5, graph_integration 13,
      graph_cypher_inline_filter 4, txn_graph_wiring 5 (27/27).
- [x] coverage did not decrease — +7 new overflow tests (M1–M7 + Reject); each RED-for-the-
      right-reason confirmed before build, all green after. RED-via-mmap re-confirmed (A1).
- [x] no test or contract was altered to pass — the contract is unchanged. Pre-existing tests
      were updated ONLY to propagate the CONTRACTED v3→v4 bump (constants `version==3`→`4`,
      `downgrade_to_v2` byte-math, mmap fixture made genuinely-mmap). No assertion weakened;
      the v1/v2 backward-compat tests remain and now positively evidence M5.
- [x] concurrency / timing safe — `label_overflow` is built once at construction
      (from_frozen/from_bytes/from_mmap/compact) and is immutable thereafter (no interior
      mutability, unlike the `incoming` OnceLock). No new locks, no `.await`, Arc-shared read-only.
- [x] no secrets / injection / unexpected deps — no new crates (smallvec already a dep). The
      parser is bounds-checked (`read2`/`read4` → `CsrError`), rows range-checked (`< node_count`)
      and strictly ascending, no untrusted pre-allocation; CRC covers the section; **no new `unsafe`**
      (mmap unsafe untouched — overflow is heap-owned). Transitively fuzzed by `csr_from_bytes`.
- [x] layering & dependencies — change is confined to `src/graph/{types,index,compaction}.rs`
      + `src/graph/csr/{mod,mmap,storage}.rs`; mirrors the v3 `edge_created_ms` precedent exactly.
- [x] a person reviewed and approved the change — Tin Dang, verify gate PASS (2026-06-17).

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — `label_overflow` field: written by from_frozen/from_bytes/from_mmap/compact,
      read by `LabelIndex::build` + `to_bytes` + `CsrStorage::label_overflow()`. `label_overflow_offset`:
      set in to_bytes (byte 88), parsed in from_bytes/from_mmap (informational, like ecms_offset).
      `parse_label_overflow`: called by both loaders. `CsrStorage::label_overflow()`: called by
      compaction's merge loop. All referenced (clippy `-D warnings` clean on both feature sets).
- [x] DEAD-CODE — none new; clippy (default + tokio,graph) flags zero unused symbols.
- [x] SEMANTIC — on-disk format change reviewed against the v3 `edge_created_ms` precedent:
      header field reuses padding (size assert still 128); section is trailing + CRC-covered +
      version-gated; v≤3 path byte-identical. One shared parser for heap+mmap (single source of truth).

### GATE RECORD
Outcome: PASS  — risk:high · autonomy:conservative → HUMAN gate (no auto-PASS); approved at the verify gate.
Reviewed by: Tin Dang · date: 2026-06-17

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): a graph using > 32 distinct labels now matches every label
(previously the 33rd+ silently returned nothing); CSR segment files are now version 4 (`from_bytes`
rejects nothing new — v≤3 still loads). No production telemetry yet (correctness milestone).
Spec delta for the next loop: the `u16` label-id space is now fully usable; if a future task adds a
SECOND on-disk graph section, factor a shared "trailing version-gated section" writer/parser — this
task hand-rolled the third such section (edge_created_ms v3, label_overflow v4) by copy-precedent.

### Competency deltas
- [TDD · open] A RED/GREEN test that loads via `CsrStorage::from_file` can silently exercise the
  HEAP loader instead of mmap when node_meta isn't 8-aligned (edgeless nc=2 shape → byte 140) — the
  test passes but never covers the intended mmap path. Assert `matches!(CsrStorage::Mmap(_))` (and
  pick an 8-aligned shape) whenever a test's POINT is the zero-copy path. (evidence:
  test_overflow_survives_mmap_reload fell back to heap until the fixture was corrected; A1 was the
  flagged risk.)
- [ADD · open] A CONTRACTED on-disk version bump (v3→v4) ripples to pre-existing constant-pinning
  tests (`assert version==3`, byte-stripping helpers). Updating those to the new constant is
  propagation, NOT the forbidden "weaken a test to pass" — but it must be surfaced at the gate, and
  the backward-compat tests must remain (they then positively evidence the M5 compat claim).
  (evidence: 6 `version==3`→`4` + downgrade_to_v2 byte-math, all listed in §5.)
- [DDD · open] `compact_segments` leaves `node_id_to_row` EMPTY by design (it merges at external_id
  level, not NodeKey) — so `lookup_node` returns None on every compacted segment. Tests/consumers
  that need a post-merge row must locate it by `external_id`, not `lookup_node`. (evidence: the
  compaction test panicked on `lookup_node` before switching to external_id lookup.)
