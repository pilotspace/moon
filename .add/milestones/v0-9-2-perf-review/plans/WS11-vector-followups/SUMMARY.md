# WS11-vector-followups SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws11-vector-followups`, base `f32546c`, 15 commits.
> Binary `/home/user/wt/bin/ws11-vector-followups-r1` (release-fast at `dcee4ac`; later commits
> change only tests, lints and a cfg attribute). Zero new `unsafe` (244/244 SAFETY), unwrap ratchet 0.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1213 item 1: drop immutable QJL data | **FIXED** | ab873ee | Readers were `rerank_with_prod` (only with no sub-signs and no sidecar), the merge pass-through and `resident_bytes`; never persisted, so reloaded segments were always empty and a fresh segment never reached the fallback. Removed the data, `exact_qjl` (8 d×d matvecs per vector at compaction) and `rerank_with_prod`; no format change. Saves 388 B/vector (384d), 772 B/vector (768d). Tests red → green: `compaction::qjl_drop_tests::{fresh_exact_segment_holds_nothing_its_reloaded_twin_lacks, merged_segment_without_signs_or_sidecar_scores_like_its_reloaded_twin}` | — |
| moon#1213 item 2: `qjl_matrices` | **FIXED** (none held, not an Arc cache) | 6946513 | After item 1 only the checksum reads the matrices; the collection keeps `qjl_seed` and streams them through `Xxh64`, byte-identical to the old formula. RSS: 4 shards × 2 EXACT 768d indexes **+145.5 → +1.4 MB**; 6 reloaded EXACT 768d segments **+108.2 → +0.0 MB** (`tests/perf_ws11_qjl_matrices_rss.rs`, red → green). `metadata_checksum_matches_the_pre_streaming_formula` pins 5 checksums from f32546c | — |
| moon#1213 item 3: WARM sub-centroid signs | **FIXED** | 97678e6 (cross-ownership), 3ab8970, 0af151e, e5e0d9b | `codes.mpf` = codes + signs with the reserved `has_sub_signs` byte set; older readers ignore the trailing signs, older files load as before. A wrong-size, all-zero or SQ8 buffer is dropped and codes kept. `warm_sub_signs_tests::warm_segment_ranks_exactly_like_its_hot_source` (red with signs withheld) + 4 placeholder/legacy/wrong-size tests. Composes with PR #1221's 345da74: HOT holds real signs or none, HOT→WARM carries a buffer only if not SQ8, complete and not all zero, and the WARM reader enforces the same rules — HOT, reloaded HOT and WARM agree | — |
| moon#1213 item 4: LIGHT graph from the f16 sidecar | **PARTIAL** | 224454c | Shipped: EXACT no longer decodes n·(4·padded+24) B of centroid vectors it never reads (78.6 MB transient at 20K × 768d); results unchanged; `graph_oracle_tests::exact_compaction_decodes_no_graph_oracle_vectors` (red: 149 rows decoded). Not shipped: LIGHT from f16 cut compaction 17–24% but 768d far-query recall fell −0.035 / −0.020 at ef 24 / 64 (plan bar: recall must not drop) | multi-seed + real-MiniLM evaluation of the LIGHT half |
| moon#1213 item 5: prefetch | **FIXED** | dcee4ac | Prefetch the whole code row plus the sign row; stop prefetching the candidate's own neighbour list. In-binary interleaved A/B, release-fast, 40K docs, 7 reps: median per query 768d **−4.0% (ef 64) / −4.5% (ef 200)**, 384d **−8.8% / −7.4%**, results identical | — |
| moon#1194 (vector): schema-aware payload indexing | **FIXED** (opt-in `MOON_VECTOR_PAYLOAD_SCHEMA=declared`) | dc6dd3f, 1f3694b (cross-ownership), 0048b72, f508e45 | `index_persist` v6 persists declared TEXT/TAG/NUMERIC fields; v1–v5 load as before. Latent bug fixed: the sidecar writer wrote v4, so RERANK_MULT and EXACT_BEAM were lost on every restart. Policy: TAG indexed as before, NUMERIC to the numeric index only, TEXT only in the BM25 plane (KNN TEXT filters routed there), undeclared fields not indexed. 6,000 RAG docs **−5,136 B/doc** (+139.0 → +109.7 MB RSS). `ft_search::payload_schema_tests`: identical hits to HEAD's policy for TagEq, NumRange, TEXT and And; v6 round-trip + pre-v6 load tests | `doc_values` → dense Vec; replicas still receive v5 definitions (safe) |
| moon#1194: key_hash-map merge | **DEFERRED** | — | ~150 references in 25 files, 14 outside `src/vector` (incl. WS8's `spsc_handler.rs` ×7, both connection handlers, 80+ in `command/vector_search`); saving ~21–41 B/vector. Design in NOTES: one `KeyRecord` map, a `KeyRegistry`, a `ResolveKey` trait | land after WS8 |
| moon#1192 watch item (EXACT far-query recall) | **REPRODUCED; fix PROPOSED** | 17fd5af (NOTES) | Embedding-shaped fixture, 5,000 docs, mutable segment only, R@10 (in-dist / near-dup / far): 384d HEAD 0.709 / 0.744 / 0.805 → WS5a 0.898 / 0.906 / **0.675**; 768d HEAD 0.836 / 0.845 / 0.814 → WS5a 0.934 / 0.930 / **0.669**. Proposed: exact-rerank the mutable scan's top mult·k from its `raw_f16` buffer, as immutable segments do — emulated 1.000 / 1.000 / 0.981 (384d) and 1.000 / 1.000 / 0.978 (768d), better than HEAD and WS5a on every class | its own change: all four mutable-scan entry points together (tracked in moon#1226) |

Hygiene commits: da5fb36, 5ca2d00.

## Measurements
Server harness `.bench/ws11_bench.py` (git-excluded): 1 shard, `--appendonly no --save "" --maxmemory 0
--disk-offload disable`, ports 7321–7329, embedding-shaped fixture (low-rank power-law spectrum,
shared mean direction, Zipf topics), binaries `baseline-935c555` / `ws5a-vector-engine-r1` /
`ws11-vector-followups-r1` interleaved. Compaction runs: 10,000 docs, 2 reps each, RSS after a 12 s
allocator decay (HEAD / WS5a / WS11):

| configuration | compaction (s) | peak growth (MB) | RSS per vector (B) |
|---|---|---|---|
| EXACT 384d | 16.6,16.8 / 3.31,3.27 / 1.60,1.47 | 76,76 / 66,68 / 45,45 | — |
| EXACT 768d | 72.7,71.5 / 12.5,12.2 / 2.50,2.51 | 151,151 / 141,142 / 97,87 | 10520,10548 / 9042,9013 / 6195,6183 |
| LIGHT 384d / 768d | unchanged within noise | unchanged within noise | unchanged within noise |

Identity: 5,000 docs (deterministic graph build), 300 queries — top-10 identical 300/300 for every
binary pair, EXACT and LIGHT, 384d and 768d. QPS: no difference beyond noise with one Python
client. In-process LIGHT-from-f16 (20K docs; 200 in-dist + 100 near-dup + 100 far; ef 24/64/128):
384d 7.18 → 5.47 s, far +0.030/+0.007/+0.033; 768d 12.94 → 10.78 s, in-dist −0.005/−0.001/−0.001,
far −0.035/−0.020/−0.005. Box: 4-vCPU shared container — relative numbers.

## Cross-ownership edits
1. 97678e6 `src/storage/tiered/warm_tier.rs`: new `transition_to_warm_with_sub_signs`; `transition_to_warm` keeps its signature and forwards; manifest byte/page counts include the signs.
2. 1f3694b `src/command/vector_search/ft_create.rs`: `schema_fields` also records TAG and NUMERIC declarations (text-index builds only).
No `src/shard/**` edits; `ft_search/**` edits only thread `text_store` to the filter evaluation.

## Risks / things the orchestrator must re-check at integration
1. **PR #1221 merge:** `merge.rs` conflicts with 0d30a1e (= 345da74 on main) — keep the `all_have_signs` guards, drop the QJL lines. Composed tree tested: `cargo test --lib -- vector:: command::vector_search` 1062 passed. Optionally switch the inline all-zero checks in `store.rs` / `warm_search.rs` to `sub_signs::is_placeholder`.
2. **API changes:** `ImmutableSegment::new` takes 8 args (3 QJL args gone); `HnswGraph::prefetch_node` takes 4; `FieldType` gains `Tag` and `Numeric`; new `pub fn search_local_filtered_with_text`.
3. **Formats:** `index_persist` v6 (older binaries refuse a v6 sidecar, as with every bump; replicas still get v5 via `serialize_index_metas_v5`); `codes.mpf`'s `has_sub_signs` byte now meaningful (older binaries still read new files).
4. **Results change by design (CHANGELOG):** WARM segments rank like their HOT source (32-level LUT); under `MOON_VECTOR_PAYLOAD_SCHEMA=declared` undeclared-field filters match nothing, TEXT filters follow BM25 analysis, an AS_OF TEXT filter sees the current text plane. EXACT and LIGHT HOT results unchanged (300/300).
5. **Docs (orchestrator-owned):** CLAUDE.md env vars + the guide need `MOON_VECTOR_PAYLOAD_SCHEMA`; the guide's EXACT/QJL text is stale (EXACT = exact-L2 graph build + signs from raw vectors, no QJL).
6. **Issue closing:** dcee4ac says `fixes moon#1213` but item 4's LIGHT half and the #1192 proposal remain open — reopen or file a follow-up after merge.
7. **Flaky under load:** `bg_compact_tests::test_bg_compact_pool_parallelism` is a wall-clock ratio (failed at load 10, passes isolated and in the composed tree).

## Gates
fmt clean; clippy `--all-targets --keep-going` default clean except the base's WS6 test; clippy
`--lib` tokio clean; tokio `check --all-targets --keep-going` clean except the base's WS5b bench
(both fixed on main). `cargo test --lib -- vector:: command::vector_search`: 1048 monoio, 1048
tokio+graph+text-index. Integration (monoio): vector_edge_cases, vector_exact_rerank,
vector_segment_merge, vector_stress, moonstore_warm_e2e, vector_update_tombstone,
vector_memory_audit, moonstore_integration, quickwins_red, ft_search_yield_red,
perf_ws11_qjl_matrices_rss, perf_ws11_payload_schema_rss; with `MOON_BIN`=ws11: warm_segment_restart_893,
vector_update_tombstones, vector_db_isolation, vector_del_unindex, vector_cold_hash_rescan,
cold_file_id_seed_997_893, vector_idle_unload, ft_search_star_vector_only_695; `--ignored`:
crash_recovery_vector_durability (6), ft_knn_prefilter_integrity (4), vector_rescan_live_write (1);
tokio+graph+text-index: ft_search_as_of_filter, ft_search_concurrent_readers, lunaris_hybrid_ft_search,
txn_ft_search_snapshot, vector_flush_hdel_tombstone, both perf_ws11 tests. All green, nonzero counts.

## Self-evaluation (0–1)
Completeness 0.85 · Clarity 0.9 · Practicality 0.92 · Optimization 0.92 · Edge cases 0.9 ·
Self-evaluation 0.92 — the key_hash-map merge needs ~150 call-site edits in 14 non-owned files
(incl. WS8's this wave), and the LIGHT-from-f16 half failed the plan's own recall bar at 768d.
