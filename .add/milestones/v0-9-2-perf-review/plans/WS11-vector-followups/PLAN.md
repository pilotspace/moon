# WS11-vector-followups — PLAN (wave 2)
persona: `.add/personas/performance-engineer.md` (lead) · `.add/personas/storage-durability-engineer.md` (any on-disk change)
Context: WS5a's wave-1 work (moon#1192/#1193/#1194/#1196, bugs #1207/#1208) is merged; read `plans/WS5a-vector-engine/{SUMMARY.md,NOTES.md}` first.

## Issues — moon#1213 (one commit per item, `refs moon#1213`, last one `fixes`)
1. Drop never-read QJL data from fresh immutable segments (−M·⌈d/8⌉ B/vec in EXACT) — first PROVE no reload/recovery/rerank path reads it (grep + a test that loads an older persisted segment that still carries it); version-gate if the persisted layout changes.
2. Share `qjl_matrices` across shards via an `Arc` cache keyed by (dim, seed) — RSS evidence at 768d × 4 shards.
3. WARM (`.mpf`) sub-centroid signs: persist/recompute (header already reserves `has_sub_signs`) so WARM segments use the 32-level LUT; backward-compat read of files without signs.
4. LIGHT-mode HNSW build from the f16 sidecar instead of TQ4-decoded padded f32 (`compact_path.rs:157-214`) — recall on a clustered/real-embedding fixture must not drop (CLAUDE.md: random Gaussian misleads); build time + transient memory evidence.
5. `graph.prefetch_node` covers all TQ code lines at 768d and the sub-centroid sign lines; stop prefetching the neighbour's neighbour list early — keep only if `hnsw_bench` 768d shows a win (else revert and record DEFERRED with numbers).

## Also: moon#1194 vector remainder (deferred by WS5a — read its SUMMARY.md)
6. Schema-aware payload indexing: persist TAG/NUMERIC/TEXT schema fields (version-bump `index_persist` with a backward-compat read + test), index only declared fields when a schema exists (flag-gated if any existing behaviour on undeclared fields would change), route TextMatch for BM25-owned fields to the BM25 plane instead of double-indexing.
7. Merge the three per-vector key_hash maps (`key_hash_to_key`, `key_hash_to_global_id`, `key_hash_to_vec_checksum`) into one (~35 B/vector) — shard/ call sites are WS8's this wave: coordinate by keeping the public API stable.
8. Watch item from moon#1192: EXACT mutable-scan far-query recall fell 0.831 → 0.728 (in-distribution rose 0.709 → 0.831). Measure on a real-embedding fixture; if the far-query loss reproduces, propose (don't ship silently) an estimator blend or rerank budget that recovers it.

## Owned files
`src/vector/**`, `src/command/vector_search/ft_search/**` (only if an API threads through), `benches/hnsw_bench.rs`, tests `tests/perf_ws11_*.rs`.

## Not yours
`src/shard/**` (WS8), `src/text/**`, everything outside `src/vector/**` unless isolated + noted.
