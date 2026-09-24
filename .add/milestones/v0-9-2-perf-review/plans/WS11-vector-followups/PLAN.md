# WS11-vector-followups — PLAN (wave 2)
persona: `.add/personas/performance-engineer.md` (lead) · `.add/personas/storage-durability-engineer.md` (any on-disk change)
Context: WS5a's wave-1 work (moon#1192/#1193/#1194/#1196, bugs #1207/#1208) is merged; read `plans/WS5a-vector-engine/{SUMMARY.md,NOTES.md}` first.

## Issues — moon#1213 (one commit per item, `refs moon#1213`, last one `fixes`)
1. Drop never-read QJL data from fresh immutable segments (−M·⌈d/8⌉ B/vec in EXACT) — first PROVE no reload/recovery/rerank path reads it (grep + a test that loads an older persisted segment that still carries it); version-gate if the persisted layout changes.
2. Share `qjl_matrices` across shards via an `Arc` cache keyed by (dim, seed) — RSS evidence at 768d × 4 shards.
3. WARM (`.mpf`) sub-centroid signs: persist/recompute (header already reserves `has_sub_signs`) so WARM segments use the 32-level LUT; backward-compat read of files without signs.
4. LIGHT-mode HNSW build from the f16 sidecar instead of TQ4-decoded padded f32 (`compact_path.rs:157-214`) — recall on a clustered/real-embedding fixture must not drop (CLAUDE.md: random Gaussian misleads); build time + transient memory evidence.
5. `graph.prefetch_node` covers all TQ code lines at 768d and the sub-centroid sign lines; stop prefetching the neighbour's neighbour list early — keep only if `hnsw_bench` 768d shows a win (else revert and record DEFERRED with numbers).

## Owned files
`src/vector/**`, `src/command/vector_search/ft_search/**` (only if an API threads through), `benches/hnsw_bench.rs`, tests `tests/perf_ws11_*.rs`.

## Not yours
`src/shard/**` (WS8), `src/text/**`, everything outside `src/vector/**` unless isolated + noted.
