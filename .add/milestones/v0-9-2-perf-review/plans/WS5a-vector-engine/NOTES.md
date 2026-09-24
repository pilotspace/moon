# WS5a-vector-engine — working notes

## Build hygiene on the shared target dir
- All worktrees share `/home/user/wt/target` and the moon crate's artifact
  hash is path-independent, so another worktree's build can make mine look
  FRESH (dep-info paths are package-relative). Seen once: `cargo test` ran a
  binary without my new tests. Mitigation used for every run: build with
  `--no-run --message-format=json`, copy the executable immediately, verify a
  marker test is listed, run the copy (`scratchpad/t.sh`); `touch src/lib.rs`
  before check/clippy gates.
- `CARGO_INCREMENTAL=0` from the orchestrator's disk-pressure update onward.

## moon#1193
- The 16-level budgeted arm is now `hnsw::adc_kernel::adc16_sum_budgeted`
  (safe, as_chunks). Completed sums are bit-identical to the unbudgeted arm.
- The unbudgeted arm keeps its pointer loop: the safe form measured
  0.80-0.97x of it (SLP-vectorized pairs of accumulators + shuffles).
- Sub-centroid signs: segment format v2 `sub_signs.bin`; v1 dirs stay
  16-level until merged (merge recomputes from the f16 sidecar). No startup
  recompute: the startup-reconcile cost model says restart cost matters.
- WARM (.mpf) signs deferred: the codes sub-header reserves `has_sub_signs`.

## moon#1192
- The issue's premise "mutable QJL data is all zeros ⇒ identical ranking" is
  false: prod(r=0) = |q|²+|x|²−2|x|⟨q̂,ĉ⟩, TQ-ADC = |x|²(1+|ĉ|²−2⟨q̂,ĉ⟩).
  Estimator study (clustered 3000 × 384/768d, 60 queries, R@10 vs exact):
  unit 384d 0.752→0.835, 768d 0.790→0.842, non-unit L2 0.117→0.833.
  Decision: switch (better recall, bit-identical to LIGHT, fixes non-unit L2
  and the cross-segment distance-scale mismatch).
- E2E caveat (release binaries, 5000×384d EXACT mutable, R@10 vs exact
  cosine): in-distribution queries (cos@10 0.78) 0.709 → 0.831; near-dup
  0.783 → 0.841; FAR / out-of-distribution queries (cos@10 0.14) 0.831 →
  0.728. The ‖ĉ‖² term costs ADC when the true neighbours are barely similar;
  it helps when they are. Recorded as a risk in the summary.
- Found: chunked MVCC scan had no TQ4A2 arm → `query_state.unwrap()` panic
  for LIGHT TQ4A2. Fixed in the same arm.
- Found: HEAD's compact indexed the internal-id-ordered QJL buffer by live
  position (rows after a dead entry carried a neighbour's signs).
- QJL data in fresh immutable segments is effectively never read
  (`rerank_with_prod` needs empty sub-signs AND no sidecar). Follow-up:
  drop it entirely (−M·⌈d/8⌉ B/vec in EXACT).

## moon#1196
- `PreparedTqQuery` (hnsw/prepared.rs): rotation + lazily built 16/32-level
  LUTs + unit query, `Sync`, shared by `Arc` with pool jobs. Same routines
  as the per-segment path ⇒ bit-identical. `matches()` = ptr-eq or
  (checksum, id, dims, quantization, metric).
- SESSION: `filter_session_results_in_db` probes the live set through
  `get_sorted_set_ref_if_alive` (no clone, no promotion). No cap: after the
  fix the per-query cost is O(k) regardless of session size, the set is
  billed to used_memory (moon#788), and a cap would silently re-surface
  results the session promised not to repeat.

## moon#1194 (vector parts)
- Payload index stored zero-copy RESP slices (field + value) for EVERY doc in
  the forward map → pinned read buffers. AOF-replay restart RSS, 16K docs:
  270 → 231 MB.
- Tag values containing a space / invalid UTF-8 are unreachable through the
  parser's TagEq → not tag-indexed. ">256 B" cut rejected (URLs are valid
  exact tags).
- Schema-aware mode not done: schema_fields lacks TAG/NUMERIC and is not
  persisted; TextMatch has no BM25 route. Opt-out env instead:
  MOON_VECTOR_PAYLOAD_TEXT=off.
- SUMMARY.md could not be written by this agent (harness refuses report
  files); its full content is in the final hand-off for the orchestrator.
