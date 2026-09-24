# WS5a-vector-engine — PLAN (wave 1)
persona: `.add/personas/performance-engineer.md` (read `docs/internal/vector-engine-internals.md` first)

## Issues
1. **moon#1193** budgeted 16-level TQ-ADC loop is serial and bounds-checked.
   - Must: 8-accumulator / 4-bytes-per-step structure like its twins, no new `unsafe` (`as_chunks::<16>()` on the LUT — MSRV 1.94); bit-identical distances vs the old loop on randomized codes (f32 summation order changes ⇒ assert within the tolerance the unbudgeted twin already accepts, and identical top-k on a fixture).
   - Should: persist sub-centroid signs (or recompute from the f16 sidecar on load) so reloaded HOT segments keep the 32-level LUT; stop zero-filling signs in GraphUnion merge. Any on-disk change = version bump + backward-compat read + test (CONVENTIONS "contracted version bump").
   - Evidence: `cargo bench --bench hnsw_bench -- hnsw_search` is heavy; at minimum a release-fast micro timing in a `#[test]`/example of `dist_bfs_budgeted` old vs new, else a single bench run.
2. **moon#1192** BUILD_MODE EXACT: whole-buffer QJL recompute on the shard thread at every compaction + dead per-query `prepare_query_prod`.
   - Must: skip `prepare_query_prod` and use TQ-ADC + FastScan for the mutable scan in EXACT (mutable residual norms are always 0 ⇒ identical ranking — prove with a test comparing results before/after on a fixture); recompute only `[0, n)` and move it into the compaction worker (off the shard thread); SIMD `dot_f32` for matvec rows.
   - Could: share `qjl_matrices` across shards via an `Arc` cache keyed by (dim, seed).
3. **moon#1196** FT.SEARCH per-query redundancy: SESSION map clone (filter inside the borrow, cap growth), one `PreparedTqQuery` (rotation + LUT) per query shared across segments/pool jobs, tombstone guard taken once per search (not per candidate), filtered-KNN bitmap cloned once.
4. **moon#1194 (vector parts)**: payload index schema-aware (index only schema-declared TAG/NUMERIC/TEXT fields when a schema exists; persist `schema_fields` if needed — version-gated; at minimum skip tag-indexing values > 256 B and skip payload text-indexing of fields the BM25 plane owns) — behind a config flag defaulting to the SAFE behaviour for existing indexes (filters on undeclared fields must keep working where they work today, or the change is flag-gated OFF by default — decide, justify in SUMMARY); f16 rerank sidecar swapped to the mmap'd file after it is persisted; merge the three per-vector key_hash maps if contained.

## Owned files
`src/vector/**`, `src/command/vector_search/**` EXCEPT `ft_text_search.rs` and the BM25 paths of `ft_aggregate.rs` (WS5b), `src/shard/spsc_handler.rs` ONLY `auto_index_hset*` / `index_payload_field` / payload-index population functions, `benches/hnsw_bench.rs`, `benches/*vector*`, tests `tests/perf_ws5a_*.rs`.

## Not yours
`src/text/**`, `src/graph/**`, the rest of `spsc_handler.rs` (WS6 owns the AofFold arm this wave; WS8 owns the rest in wave 2), `src/shard/coordinator.rs` (FT.SEARCH scatter ordering is WS8's moon#1182 in wave 2).
