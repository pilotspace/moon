# Moon Vector Search — Deep Review Report
**Date:** 2026-06-16  
**Reviewer:** Principal Rust / ANN Systems Engineer  
**Scope:** `src/vector/` + `src/command/vector_search/` — READ-ONLY audit

---

## Section A — Architecture Map

### A1. Component Inventory

| Component | File | Role |
|---|---|---|
| `SegmentHolder` | `src/vector/segment/holder.rs:147` | ArcSwap-based lock-free segment list; single atomic load per search |
| `SegmentList` | `src/vector/segment/holder.rs:38` | Owned snapshot: mutable + immutable + IVF + warm + cold tiers |
| `YieldBudget` / `FT_SEARCH_YIELD_BUDGET` | `src/vector/segment/holder.rs:55–86` | Co-operative yield budget: 1 segment/chunk, 1024 brute-force vecs/chunk (cross-arch tuned) |
| `SearchSnapshot` | `src/vector/segment/holder.rs:114` | Owned pre-yield capture: segments Arc, query, filter bitmap, MVCC state, scratch, key_hash_to_key |
| `MutableSegment` | `src/vector/segment/mutable.rs` | Append-only brute-force tier; `RwLock<MutableSegmentInner>` |
| `FrozenSegment` | `src/vector/segment/mutable.rs` | Snapshot of mutable for hand-off to compaction |
| `ImmutableSegment` | `src/vector/segment/immutable.rs` | HNSW + TQ/SQ8 codes + QJL signs + MVCC headers; post-compact immutable |
| `HnswBuilder` | `src/vector/hnsw/build.rs:91` | Single-threaded HNSW construction (M=16, EF=200); diversity heuristic; BFS reorder |
| `hnsw_search` | `src/vector/hnsw/search.rs` | HNSW graph traversal; TQ-ADC / SQ8-ADC / sub-centroid LUT distance; unsafe ILP inner loop |
| `compact` | `src/vector/segment/compaction.rs:450` | 8-step pipeline: filter → encode → HNSW build → recall gate → BFS reorder → ImmutableSegment |
| `merge_immutable` | `src/vector/segment/compaction.rs:1029` | Graph-union merge: concatenate TQ codes verbatim, rebuild HNSW, recall gate |
| `DistanceTable` | `src/vector/distance/mod.rs:23` | OnceLock dispatch table: AVX-512 > AVX2+FMA > NEON > scalar |
| `sq8` module | `src/vector/turbo_quant/sq8.rs` | SQ8 per-vector affine encode/decode; `sq8_l2_adc`, `sq8_ip_adc` |
| `encode_sq8_into` | `src/vector/turbo_quant/sq8.rs:49` | Hot-path encode: per-vec min/max, 255-level quantization, NaN guard |
| `BackgroundCompactor` | `src/vector/background_compact.rs` | Thread pool (flume-fed workers) for off-event-loop compaction and merge |
| `auto_index_hset` | `src/shard/spsc_handler.rs:1979` | HSET hook: extract vector blob, quantize, append to mutable, populate `key_hash_to_key` |
| `ft_compact` | `src/command/vector_search/ft_admin.rs:106` | FT.COMPACT command: calls `idx.force_compact()` (bypasses threshold) |
| `ft_info` | `src/command/vector_search/ft_info.rs:15` | FT.INFO: num_docs = mutable.len() + sum(imm.live_count()) |
| `search_mvcc_yielding` | `src/vector/segment/holder.rs:596` | Async associated fn driving chunked search against `SearchSnapshot` |

### A2. Data Flow — FT.SEARCH KNN (Dense, yielding path)

```
Client → RESP parse → dispatch_read → ft_search::dispatch
  │
  ├─ build_search_snapshot() [execute.rs:~640]
  │     load SegmentList Arc (O(1) refcount bump)
  │     clone key_hash_to_key (HashMap<u64,Bytes>)
  │     capture mutable_len, snapshot_lsn, committed treemap
  │     return SearchSnapshot { segments, scratch, ... }
  │
  ├─ cooperative_yield().await  [UnixStream park-reap, ~0.3µs]
  │
  ├─ SegmentHolder::search_mvcc_yielding(&mut snap, budget).await
  │     brute-force MVCC over [0, mutable_len) in chunks of 1024
  │       ├─ each chunk: brute_force_search_mvcc → TQ-ADC or SQ8-ADC
  │       └─ yield between chunks (bump cooperative_yield counter)
  │     for each immutable segment:
  │       ├─ hnsw_search (TQ-ADC LUT, optional sub-centroid, unsafe ILP loop)
  │       └─ yield after seg_cap (1) segments
  │     warm → cold → IVF (committed-by-definition, same yield pattern)
  │     sort_unstable + truncate(k)  → SmallVec<[SearchResult; 32]>
  │
  └─ build_search_response(&results, &snap.key_hash_to_key, offset, count)
       key_hash lookup → orig key or "vec:<id>" fallback
       → Frame::Array response
```

### A3. Segment Lifecycle State Machine

```
HSET arrives
  │
  ▼
MutableSegment.append()          ← key_hash_to_key populated HERE (spsc_handler.rs:2170)
  │
  │ mutable.len() >= compact_threshold (default 1000) OR FT.COMPACT
  ▼
FrozenSegment (freeze())         ← snapshot of mutable codes + entries
  │
  │ BackgroundCompactor.submit() → worker thread
  ▼
compact(frozen, collection, seed, persist)
  ├─ filter dead entries (delete_lsn != 0)
  ├─ build HNSW (exact f32 L2 if raw_f32 present; decoded-centroid L2 otherwise)
  ├─ BFS reorder TQ/SQ8 buffer + QJL signs + sub-centroid signs
  ├─ verify recall >= 0.95 (sample-based, #[allow(dead_code)] gate — see B findings)
  └─ ImmutableSegment::new(graph, tq_bfs, mvcc_headers, ...)
       ↓ segment.swap() — ArcSwap atomic store
ImmutableSegment (HNSW + codes)  ← immutable forever; no lossy re-encode

merge_immutable (background, optional):
  deduplicate by key_hash (highest insert_lsn wins)
  concatenate TQ codes verbatim (NO decode/re-encode)
  rebuild HNSW on union via decoded-centroid L2
  recall gate >= recall_tolerance
  → single merged ImmutableSegment
```

The no-re-encode guarantee for merge is sound: codes are concatenated byte-for-byte; only the graph topology is rebuilt. This avoids the quantization error accumulation the CLAUDE.md warns about.

### A4. Design Decisions and Trade-offs

**Lock-free segment access:** `ArcSwap<SegmentList>` gives wait-free reads (single atomic load). Compaction installs the new list atomically; in-flight searches hold their `Guard`/`Arc` until done. No lock anywhere in the search hot path. Well-executed.

**Brute-force mutable tier:** The mutable segment uses `parking_lot::RwLock<MutableSegmentInner>` internally. Search takes a read lock for the duration of `brute_force_search_mvcc`. This is acceptable (short critical section, read-biased), but the `search_mvcc_yielding` path chunks the scan precisely to limit how long the read lock is held across successive chunks.

**HNSW parameters:** M=16, EF_CONSTRUCTION=200 hardcoded in `compaction.rs:30-31`. These are sound defaults for recall-quality trade-offs. EF_RUNTIME is per-index user-configurable.

**Diversity heuristic:** `use_heuristic: true` default in `HnswBuilder`. Disabled when using TQ-ADC as the build oracle (noisy comparisons amplify quantization error). Switched to exact f32 L2 (when `has_raw`) or decoded-centroid L2 (Light mode) for build — correct decision. The heuristic is re-enabled when the oracle is accurate (`build.rs:121-126`).

**SQ8 vs TQ4:** SQ8 stores `dim + 8` bytes per vector (per-vec affine + NaN guard). TQ4 stores `padded_dim/2 + 4` bytes (shared codebook, FWHT-rotated). SQ8 is ~2× the storage but achieves near-FP32 recall at ≤384d where TQ4's concentration-of-distances error is documented. Both paths are fully supported through the compaction pipeline.

**Co-operative yield knee:** 1024 vecs/chunk is cross-arch validated (GCloud A/B: x86 Sapphire Rapids +2–3.5%, aarch64 Neoverse-N1 +2–3.4%). Documented in `holder.rs:73-81` with the precise rationale. The `MOON_FT_YIELD_CHUNK` env override is cached via `OnceLock` (not per-search).

**IVF/warm/cold segments:** Present in `SegmentList` and dispatched in all search paths but not yet counted by `ft_info` num_docs (see P2 finding).

### A5. Concurrency Model

| Concern | Mechanism |
|---|---|
| Segment list reads | `ArcSwap::load()` — wait-free |
| Segment list writes (compaction) | `ArcSwap::store(Arc::new(...))` — atomic |
| Mutable segment append | `parking_lot::RwLock` write lock (per append) |
| Mutable segment search | `parking_lot::RwLock` read lock (per brute-force scan) |
| Background compaction | Dedicated thread pool; `flume::bounded` channel; result sent via oneshot |
| key_hash_to_key | Plain `HashMap` on `VectorIndex`; accessed single-threaded within shard |
| Cooperative yield | `crate::runtime::cooperative_yield()` — UnixStream park-reap (cost-free on monoio) |

No std::sync locks anywhere in the vector path. No global locks. No lock held across `.await`. Clean.

---

## Section B — Code Audit (Ranked Findings)

### P0 — Correctness / Data-Loss

**[P0-1] `hnsw/search.rs:366` — `code_len = bytes_per_code - 4` computed unconditionally before the SQ8 branch guard**

`code_len` is defined at `src/vector/hnsw/search.rs:366`:
```rust
let code_len = bytes_per_code - 4; // nibble-packed codes (last 4 bytes are norm)
```
This line executes for both TQ and SQ8 segments. For SQ8, `bytes_per_code = dim + 8` (an 8-byte trailer per `sq8.rs:26`), so `code_len = dim + 4` — wrong. The value is unused for SQ8 because `dist_bfs` returns early at the `if is_sq8` branch (`search.rs:392`), but `code_len` is still used later at `search.rs:398` (`let code_only = &vectors_tq[offset..offset + code_len]`) and in the unsafe sub-centroid loop. The early-return at line 396 prevents the wrong `code_len` from being used in the live SQ8 search path during HNSW traversal, but this is a latent footgun: any future code touching `code_len` after the `is_sq8` check could silently read past the correct code region into the SQ8 trailer bytes.

**Severity:** P0 (correctness footgun; not currently a crash because the early-return guards it, but the invariant is broken by construction and any refactor risks activating it).

**Fix:** Compute the branch-conditional `code_len` only inside the non-SQ8 arm, or add an explicit guard:
```rust
let code_len = if is_sq8 { dim } else { bytes_per_code - 4 };
```
This pattern is already correct in `compaction.rs:1079` — the search path should match it exactly.

---

**[P0-2] `ft_info.rs:42-45` — `num_docs` omits IVF, warm, and cold segment vectors**

`ft_info` sums `mutable.len()` and `imm.live_count()` but never adds contributions from `snap.ivf`, `snap.warm`, or `snap.cold`:
```rust
// ft_info.rs:41-45
let snap = idx.segments.load();
let mut num_docs = snap.mutable.len();
for imm in snap.immutable.iter() {
    num_docs += imm.live_count() as usize;
}
```
By contrast, `SegmentHolder::total_vectors()` at `holder.rs:207` correctly sums all five tiers. `FT.INFO num_docs` is a documented invariant (CLAUDE.md: "FT.INFO num_docs must sum across all segments"). An index with IVF or warm segments will undercount. This is a silent correctness bug for any deployment that uses those tiers.

**Fix:** Mirror `total_vectors()` logic in `ft_info` — add loops for `snap.ivf`, `snap.warm`, and `snap.cold`.

---

### P1 — Performance Hot-Path / Unsafe-Without-SAFETY / Unwrap-in-Lib

**[P1-1] `ft_search/execute.rs:135, 160` and `dispatch.rs:295, 660` — O(N) `key_hash_to_key.clone()` on every FT.SEARCH**

Every synchronous search path clones the entire `key_hash_to_key: HashMap<u64, Bytes>` before returning results:
```rust
// execute.rs:135
let key_hash_to_key = idx.key_hash_to_key.clone();
```
And the yielding path at `dispatch.rs:660`:
```rust
key_hash_to_key: idx.key_hash_to_key.clone(),
```
For an index with N vectors this is O(N) allocation and copy on the search hot path. At N=100k, `HashMap<u64, Bytes>` (8-byte key + ~24-byte Bytes) is roughly 3.2 MB per search. With multiple concurrent searches this multiplies.

The map is only needed for resolving key names in the response builder, which touches at most `k` entries (typically 10–100). The fix is to avoid cloning the whole map and instead take the index borrow directly into `build_search_response`, or to share the map via `Arc`. The yielding path at `holder.rs:114` captures the whole map in `SearchSnapshot` because it needs to release the `&mut VectorIndex` borrow before yielding — but `Arc<HashMap<...>>` would suffice.

**Fix (minimal):** Wrap `VectorIndex::key_hash_to_key` in `Arc<HashMap<u64,Bytes>>`. `clone()` becomes O(1) refcount increment; no data copy.

---

**[P1-2] `hnsw/search.rs:417-458` — unsafe sub-centroid ILP loop: SAFETY comment present but bound proof is incomplete**

The unsafe block at `search.rs:439-458` reads `code_ptr`, `sign_ptr`, and `lut_ptr` without bounds checks. The SAFETY comment at line 437 states: "Bounds: code_only.len() == padded_dim/2, qi*32+31 < adc_lut.len(), sign_off + code_len/4 < sub_centroid_signs.len() (caller guarantees bpv)."

The issue: the sign bound "caller guarantees bpv" is an informal contract pinned to `sub_sign_bpv` computed at the caller site. There is no `debug_assert!` for the sign array bound, unlike the invariant checks at `search.rs:374-385` which cover the LUT and code arrays with `debug_assert_eq!`. A misconfiguration of `sub_sign_bpv` (e.g., during a compaction that produces a mismatched `sub_bpv`) would produce an out-of-bounds read in release builds with no diagnostic.

**Fix:** Add a `debug_assert!` analogous to the existing ones:
```rust
debug_assert!(
    sub_centroid_signs.len() >= (graph.num_nodes() as usize) * sub_sign_bpv,
    "sub_centroid_signs too small"
);
```

---

**[P1-3] `compaction.rs:554` — `code_len = bytes_per_code - 4` used for SQ8 sub-centroid sign computation without is_sq8 guard**

At `compaction.rs:554`:
```rust
let code_len = bytes_per_code - 4;
```
This `code_len` is used at line 752 (`let code_slice = &tq_bfs[code_offset..code_offset + code_len]`) inside the sub-centroid sign computation block (lines 724-816). The `is_sq8` branch at line 574 computes a different `code_len` in `all_rotated` but the outer `code_len` at 554 is the one used in the sign loop. For SQ8 segments `bytes_per_code = dim + 8`, so `code_len = dim + 4`, slicing 4 bytes into the SQ8 trailer. However, the sub-centroid sign loop is gated on `if !is_sq8 {...}` at lines 755/780 — the `is_sq8` arm at line 756 uses `cb` (A2 codebook) and the `else` arm at line 781 uses `codebook_opt`. But there is no `is_sq8` guard at line 751 (`let code_slice = &tq_bfs[...code_len]`); the slice is computed before the branch. For an SQ8 compaction, `code_slice` reads `dim+4` bytes instead of `dim` bytes, creating a wrong slice that is then (for TQ-only) used inside the if/else. The specific SQ8 code path does not reach the `code_slice` usage because neither `is_a2` nor the scalar-TQ codebook branch is hit — but the slice computation itself reads into the SQ8 trailer bytes unnecessarily.

**Fix:** Compute `code_slice` only inside the non-SQ8 branch, or guard the slice computation with `if !is_sq8`.

Note: this finding is related to P0-1 (same root cause: `bytes_per_code - 4` is a TQ-only invariant used as a universal constant).

---

**[P1-4] `distance/mod.rs:154` — `expect()` in `table()` called on hot search path**

```rust
// distance/mod.rs:153-154
DISTANCE_TABLE
    .get()
    .expect("distance table initialized by init()")
```
`table()` is annotated `#[inline(always)]` and called in the HNSW build and brute-force inner loops. The `expect` path is unreachable after `init()` but panics in library code. Per the CLAUDE.md rule: "No `unwrap()` or `expect()` in library code outside tests." The guard `if DISTANCE_TABLE.get().is_none() { init(); }` at line 148 makes this safe in practice, but the `expect` still violates the policy and produces a panic string that bloats binary size. Use `.unwrap_or_else(|| unreachable!(...))` or restructure to use `get_or_init`.

**Fix:** Refactor `table()` to call `DISTANCE_TABLE.get_or_init(|| { ... })` directly, eliminating the separate `init()` function and the `expect`.

---

### P2 — Maintainability / Design

**[P2-1] Recall gate (`verify_recall`) is `#[allow(dead_code)]` and NOT called in `compact()`**

The function `verify_recall` at `compaction.rs:869` is annotated `#[allow(dead_code)]` and is never invoked in the production `compact()` path. The 8-step pipeline comment at the top of the file lists "Step 4: Verify recall >= 0.95" but this step is absent from the actual code — the compaction proceeds directly from HNSW build to BFS reorder with no recall gate. Similarly, `MIN_RECALL: f32 = 0.95` and `RECALL_SAMPLE_SIZE: usize = 1000` are both `#[allow(dead_code)]`.

The recall gate IS exercised by `merge_immutable` (via `verify_merge_recall`) but not by the primary mutable→immutable compaction path. This means an index can compact to a low-recall HNSW graph without any in-process detection. Recall can only be measured externally after compaction completes.

**Fix:** Either call `verify_recall` within `compact()` (treating recall failure as `CompactionError::RecallTooLow`) or document explicitly that the production gate is external-only and remove the dead functions to avoid confusion.

---

**[P2-2] `compact_parallel` / `stitch_subgraphs` / `assign_to_cells` are all `#[allow(dead_code)]` in production**

These functions (~350 lines total, `compaction.rs:60-438`) implement parallel cell-partitioned HNSW build via `std::thread::scope`. They are retained for tests but production always uses the single-threaded `HnswBuilder` path (disabled at line 637: "Cell-parallel disabled: 2-coordinate spatial partitioning is meaningless at 384d+"). The dead code adds ~350 lines of maintained complexity without production benefit.

**Fix:** Move these to a `#[cfg(test)]` module, or gate the whole block with `#[cfg(test)]` to make intent explicit.

---

**[P2-3] `merge_immutable(MergeMode::KeepRaw)` silently falls back to `GraphUnion` with a warning**

`compaction.rs:1039-1047` — KeepRaw mode falls through to `merge_graph_union` with a `tracing::warn!`. A user who sets `MERGE_MODE=keep_raw` expecting lossless re-quantization (which is the entire point of KeepRaw) silently gets the same graph-union behaviour as the default. The API implies a guarantee that is not yet delivered. This is a correctness-of-expectation issue — not a data-loss bug, but a silent contract violation.

**Fix:** Return `CompactionError::PersistFailed("KeepRaw not yet implemented")` or document KeepRaw as an alias for GraphUnion until the raw sidecar (P2.5) is ready.

---

**[P2-4] `search_mvcc` at `holder.rs:573` says "3. Dirty set: currently empty" but `MvccContext::dirty_set` is already wired**

The test `test_holder_search_mvcc_dirty_set_merge` at `holder.rs:992` explicitly notes that dirty set scoring is deferred to Phase 66 with a `// NOTE:` comment and an `assert_eq!(results.len(), 1)` verifying the dirty entry does NOT appear. The `dirty_set` field on `MvccContext` is fully populated by callers but silently ignored in `search_mvcc`. Any caller who passes a non-empty `dirty_set` expecting read-your-own-writes semantics will get stale results. The "Phase 66" deferred note should be a `tracing::debug!` or a TODO that is visible in FT.SEARCH responses under EXPLAIN mode.

---

**[P2-5] `compaction.rs:554` and `hnsw/search.rs:366` — magic constant `4` (`bytes_per_code - 4`) is TQ-only but appears in SQ8 code paths**

As established in P0-1 and P1-3, the constant `4` (the TQ norm trailer size) is used in expressions that must be conditioned on `!is_sq8`. The SQ8 trailer is 8 bytes (two f32s: min and scale per `sq8.rs:26`). This pattern appears in at least 6 places. A named constant would make the distinction explicit:
```rust
const TQ_NORM_TRAILER_BYTES: usize = 4;
const SQ8_PARAMS_BYTES: usize = 8; // already exported from sq8.rs
```
All `bytes_per_code - 4` sites in shared-path code should use `if is_sq8 { dim } else { bytes_per_code - TQ_NORM_TRAILER_BYTES }`.

---

### P3 — Nits

**[P3-1] `src/vector/turbo_quant/sq8.rs:ip_adc` is unrolled for `l2_adc` but not for `sq8_ip_adc`**

`sq8_l2_adc` at `sq8.rs:117` uses 4-way loop unrolling with independent accumulators (`s0..s3`). `sq8_ip_adc` at `sq8.rs:146` uses a plain serial loop. For the InnerProduct metric under heavy load, this leaves ILP on the table. Low priority but inconsistent.

---

**[P3-2] `background_compact.rs:123` — `expect("failed to spawn compaction worker thread")` in library code**

Thread spawn failure (`std::thread::spawn` returning `Err`) is OS-level resource exhaustion. Panicking with `expect` at this point kills the shard. Prefer returning an error to the caller or falling back to synchronous compaction on startup.

---

**[P3-3] `ft_search/execute.rs:64, 166` — `format!()` inside error returns**

```rust
SearchRawResult::Error(Frame::Error(Bytes::from(format!(
    "ERR unknown vector field '@{}'",
    ...
))))
```
These are error paths (not hot), so this is not a correctness issue. However, `format!()` allocates on an error path inside the command dispatch layer — the CLAUDE.md rule only exempts "result building at the end of a command path." The field name is a `Bytes` slice; `Bytes::copy_from_slice` + a static prefix would avoid the allocation. P3 because it is only on the error path.

---

**[P3-4] `distance/mod.rs` — no NEON `l2_i8` implementation; comment says "auto-vectorization is faster"**

At `distance/mod.rs:113`:
```rust
// Use scalar l2_i8: the compiler auto-vectorizes with SDOT/SADALP
// which is 3.5x faster than our explicit vmovl+vmlal NEON chain.
l2_i8: scalar::l2_i8,
```
This is correctly documented and intentional. The scalar path (`scalar.rs`) is used on aarch64 for `l2_i8`. No action needed — recording it as a P3 to surface for future NEON SQ8 ADC work (CLAUDE.md mentions "NEON SQ8 ADC" as a deferred finding). The `neon::l2_i8` function in `neon.rs` exists but is not installed in the dispatch table.

---

### Clean Bills

**Unsafe SIMD audit (AVX2, AVX-512, NEON):** Every `unsafe` block in `distance/avx2.rs`, `distance/avx512.rs`, and `distance/neon.rs` has a `// SAFETY:` comment. All comments correctly identify the invariant (feature detection guard). `is_x86_feature_detected!` is called at init time; unsafe fn are tagged with `#[target_feature(enable = ...)]` as the secondary gate. SAFETY documentation is clean. Scalar fallback is present and exercised by `integration_tests` in `distance/mod.rs`. Both SIMD and scalar paths are tested against each other at multiple dimensions (1, 7, 16, 31, 64, 128, 256, 384, 768, 1024).

**SQ8 correctness:** `encode_sq8_into` correctly guards NaN/inf inputs (returns zero encoding) and constant vectors (scale=0). Round-to-nearest quantization with clamp guards boundary FP edge cases. `sq8_l2_adc` 4-way unrolled; `sq8_ip_adc` formula `min·Σq + scale·Σ(q_i·code_i)` is mathematically correct for the affine dequantization. Tests cover roundtrip error, self-distance, ranking preservation, constant vectors, and NaN guards.

**key_hash_to_key population:** `auto_index_hset` at `spsc_handler.rs:2170` populates `idx.key_hash_to_key.entry(key_hash).or_insert_with(...)` before calling `segment.mutable.append(...)`. The population happens in both `handle_vector_insert` (default field, line 2170) and `handle_vector_insert_field` (additional fields, line 2241). The key map is captured into `SearchSnapshot::key_hash_to_key` at yield time.

**Lock hygiene:** No `std::sync` locks used in vector code. `parking_lot::RwLock` throughout. No lock held across `.await` (the `search_mvcc_yielding` associated fn takes no lock; the mutable segment's RwLock is only held for the duration of each synchronous `brute_force_search_mvcc` chunk call — not across yields).

**compact threshold no-op trap:** Documented and fixed. `ft_compact` calls `force_compact()` which bypasses the threshold check (`store.rs:373`). The auto-compact path (`try_compact_if_needed`) correctly uses the threshold. `FT.COMPACT` never silently no-ops since `force_compact` was introduced.

**HnswBuilder random level:** LCG PRNG (Knuth MMIX constants) produces exponential distribution correctly. Level capped at 32 to prevent pathological single-path graphs. Max level tracks the current highest-level node; entry point updated when new node exceeds it.

**Merge no re-encode guarantee:** `merge_graph_union` concatenates TQ codes verbatim (`compaction.rs:1012`: "Concatenate TQ codes verbatim — no decode/re-encode"). Decoded vectors are used only as the HNSW build oracle (discarded after build). The no-lossy-merge invariant from CLAUDE.md is preserved.

---

## Verdict

The vector search subsystem is architecturally sound and shows evidence of careful, iterative development. The lock-free segment access (ArcSwap), the cooperative-yield pattern with cross-arch validated chunk size, the SQ8 affine quantizer, the BFS reorder for cache-friendly traversal, and the no-re-encode merge are all well-designed. The unsafe SIMD kernels are correctly annotated and cross-validated with scalar.

The two most urgent issues are structural rather than catastrophic: the `code_len = bytes_per_code - 4` constant leaks from TQ-only code into paths that also serve SQ8 segments (P0-1, P1-3), creating a latent OOB-read risk in the search unsafe loop and wrong-slice reads during compaction. The second is `ft_info` undercounting docs when IVF/warm/cold segments are populated (P0-2). The dominant performance concern is O(N) `HashMap` clone on every FT.SEARCH — addressable with `Arc<HashMap>` (P1-1).

---

## Top-3 Fix List

**Fix 1 (P0-1 + P1-3 + P2-5): Unify `code_len` computation across TQ and SQ8**

Replace all occurrences of `bytes_per_code - 4` in shared-path code with the branch-conditional form already used in `compaction.rs:1079`:
```rust
let code_len = if is_sq8 { dim } else { bytes_per_code - 4 };
```
Files affected: `src/vector/hnsw/search.rs:366`, `src/vector/segment/compaction.rs:554`. This closes the latent OOB read in the unsafe sub-centroid loop and the wrong-slice in sub-centroid sign computation for SQ8.

**Fix 2 (P0-2): FT.INFO num_docs — include IVF, warm, and cold tier counts**

In `src/command/vector_search/ft_info.rs` (lines 41-45 and the per-field block), add loops for `snap.ivf`, `snap.warm`, and `snap.cold`, mirroring `SegmentHolder::total_vectors()` at `holder.rs:207-223`. This makes `FT.INFO num_docs` match `HLEN` (total vector count) for all segment tier configurations.

**Fix 3 (P1-1): O(N) HashMap clone on every FT.SEARCH**

Wrap `VectorIndex::key_hash_to_key` in `Arc<HashMap<u64, bytes::Bytes>>`. Change all `.clone()` call sites (4 locations: `execute.rs:135`, `execute.rs:160`, `dispatch.rs:295`, `dispatch.rs:660`) to `Arc::clone(...)`. The `SearchSnapshot::key_hash_to_key` field type becomes `Arc<HashMap<u64, bytes::Bytes>>`. Downstream call sites in `build_search_response` and `build_hybrid_response` accept `&HashMap<u64, Bytes>` via deref — no further change required.
