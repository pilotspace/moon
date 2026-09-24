# WS5a-vector-engine SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws5a-vector-engine`, base `a925e64` (code base `935c555`):
> 7 code commits + notes (5a3d14d). Measurement binary `/home/user/wt/bin/ws5a-vector-engine-r1`
> (release-fast @ `9223826`; later `0954cb0` only moves a `cfg(test)` counter); control
> `/home/user/wt/bin/baseline-935c555`. One release-fast build used (budget 2).

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1193 | FIXED | 8e48a73, 2b6b392 | New safe kernel `hnsw::adc_kernel::adc16_sum_budgeted` (8 accumulators, `as_chunks::<16>()`, zero unsafe, no bounds-check branches in the release loop); completed sums bit-identical to the unbudgeted twin (`budgeted_matches_unbudgeted_twin_bit_for_bit`, red on HEAD); old vs new within n·eps·Σ; top-k ids+order identical on clustered fixtures at 384/768/100/20/6d, L2 + cosine (`search_equivalence_tests::budgeted_adc_rewrite_keeps_topk_identical_on_fixture`). Micro (rustc -O, 3 reps): 384d 455/449/471 → 144/138/144 ns; 768d 946/930/971 → 366/280/375 ns. Segment format **v2** writes `sub_signs.bin` (v1 dirs load unchanged; wrong-sized file rejected); merge recomputes missing signs from the f16 sidecar, all-or-nothing (`segment_sub_signs_tests::*`, 5 tests, 2 red on HEAD). End-to-end: top-10 identical before/after restart for 300/300 queries (HEAD 136/300). | WARM `.mpf` signs (→ moon#1213); v1 dirs stay 16-level until merged |
| moon#1192 | FIXED — **results change by design** | 7e495fa | QJL computed on the compaction worker (live entries only, SIMD dot rows). PING max during an EXACT compaction submit 3170/3220/3200 → 12.5/11.1/12.6 ms; EXACT ingest+compact 16K docs 43.5/43.3/41.8 → 21.5/22.1/21.1 s. Holder no longer calls `prepare_query_prod`; the EXACT mutable scan is TQ-ADC+FastScan, bit-identical to LIGHT (red test). QPS 5000-mutable-only 40/39/36 → 2003/2156/1465; 4 segments + 500 mutable 195/173/186 → 791/840/835. The issue's premise was wrong (prod(r=0) ≠ ADC): recall vs exact cosine — in-distribution 0.709 → 0.831, near-duplicate 0.783 → 0.841, **far 0.831 → 0.728**, non-unit L2 0.117 → 0.833. Also fixed moon#1207 (TQ4A2 MVCC `unwrap()` panic) and moon#1208 (QJL rows misaligned after dead entries). EXACT mutable −388 B/vec at 384d. | `qjl_matrices` Arc cache; drop never-read immutable QJL data (→ moon#1213); far-query recall regression to watch |
| moon#1196 | FIXED | 17db36a, 0954cb0 | SESSION filtered inside the borrow (50 filters over a 200K-member session < 250 ms debug); `PreparedTqQuery` shared across serial loops + pool jobs (LUT builds 5 → 1 per query, bit-identical); tombstone guard once per search; filter bitmap `Arc` from capture. LIGHT 8 segments 301/448/430 → 382/581/534 QPS, p99 11.5/7.4/4.2 → 3.9/3.2/3.5 ms; results identical to baseline 300/300 (150/150 filtered). | No SESSION cap (see Risks 6) |
| moon#1194 (vector parts) | PARTIAL | ec7a280, 9223826 | f16 sidecar served from its mapped file after persist (heap −n·dim·2 B exactly; results identical; red test). Payload keys owned+interned instead of pinning RESP slices (red test). Tag values with a space or non-UTF-8 are no longer tag-indexed (no TAG filter can select them; parser guard test). RSS after restart 16K docs 270.1/270.0/270.2 → 230.8/231.2/230.8 MB; used_memory 92.9 → 80.4 MB. `MOON_VECTOR_PAYLOAD_TEXT=off` (default on), 20K docs: RSS ~390 → 237/250 MB, ingest 13 → 5.3/7.0 s. | Schema-aware indexing (needs an `index_persist` version bump; `schema_fields` has no TAG/NUMERIC entries and is not persisted; TextMatch has no BM25 route) and key_hash-map merge (not contained in owned files) → moon#1213 / WS11 |

## Measurements
Harness `ws5a_bench.py`, single connection; `--shards 1 --appendonly no --save "" --maxmemory 0 --disk-offload disable`, ports 7181–7192; 384d clustered unit vectors + 2 KB `content` + TAG + NUMERIC, COSINE, TQ4; B,N interleaved ×3. Controlled runs: COMPACT_THRESHOLD 100000 + K explicit FT.COMPACTs (uncontrolled runs gave 4–19 segments nondeterministically — not compared). Noisy shared 4-vCPU box: relative numbers only.

## Cross-ownership edits
None (`spsc_handler.rs` changes limited to `index_payload_field`, assigned to WS5a).

## Risks / things the orchestrator must re-check at integration
1. EXACT mutable-scan ranking changes (better in-distribution recall, worse far-query recall); LIGHT results identical to baseline. → CHANGELOG.
2. Segment format v2 (additive, backward-compatible). → CHANGELOG.
3. `SearchSnapshot.filter_bitmap` is now `Option<Arc<RoaringBitmap>>`; `SegmentSearchJob` has a `prepared` field — WS8 (moon#1182) must build snapshots accordingly.
4. The mapped sidecar has the same unlink contract as reloaded segments (Windows GC caveat unchanged).
5. Document `MOON_VECTOR_PAYLOAD_TEXT` in CLAUDE.md / the guide; the guide's EXACT/QJL timing text is stale.
6. No SESSION cap: per-query cost is now O(k), the set is billed to used_memory, and a cap would re-surface results the session promised not to repeat.

## Gates
fmt clean · tokio check clean · clippy ×2 clean · `check --lib --tests` clean · `cargo test --lib -- vector:: command::vector_search shard:: text::` 1546 passed · 20 integration suites green: monoio vector_edge_cases, vector_exact_rerank, vector_segment_merge, vector_stress, moonstore_warm_e2e, vector_update_tombstone, vector_memory_audit; tokio+graph ft_search_as_of_filter, ft_search_concurrent_readers, lunaris_hybrid_ft_search, txn_ft_search_snapshot, vector_flush_hdel_tombstone; `MOON_BIN`=r1 warm_segment_restart_893, vector_update_tombstones, vector_db_isolation, vector_del_unindex, vector_cold_hash_rescan, crash_recovery_vector_durability; `--ignored` ft_knn_prefilter_integrity, vector_rescan_live_write.

## Self-evaluation
Completeness 0.88 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.92 — completeness < 0.9 because schema-aware payload indexing (index_persist version bump), the key_hash-map merge (outside owned files), WARM sign persistence and the `qjl_matrices` Arc cache are deferred with reasons (routed to moon#1213 / WS11).
