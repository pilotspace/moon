# Moon Vector Search — User Guide

Moon provides Redis-compatible vector search with TurboQuant 4-bit compression. On the macOS development rig described under [Performance Benchmarks](#performance-benchmarks) it stores a 384-d vector in 452 B against Redis Stack's 3,840 B (8.5×) while matching its search QPS; that comparison has **not** been reproduced on a Linux host.

## Quick Start

```bash
# Start Moon
./moon --port 6379 --shards 1 --protected-mode no

# Create a vector index (Light mode — fast insert, low memory)
redis-cli FT.CREATE myidx ON HASH PREFIX 1 "doc:" SCHEMA \
  embedding VECTOR HNSW 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2

# Insert vectors (as binary f32 blobs in HASH fields)
redis-cli HSET doc:1 embedding <384_floats_as_bytes> title "Hello world"
redis-cli HSET doc:2 embedding <384_floats_as_bytes> title "Vector search"

# Search
redis-cli FT.SEARCH myidx "*=>[KNN 10 @embedding $query]" \
  PARAMS 2 query <query_vector_bytes> RETURN 0 DIALECT 2
```

## FT.CREATE Parameters

```
FT.CREATE <index_name> ON HASH PREFIX <count> <prefix>...
  SCHEMA <field> VECTOR HNSW <param_count>
    TYPE FLOAT32
    DIM <dimension>
    DISTANCE_METRIC <L2|COSINE|IP>
    [M <neighbors>]
    [EF_CONSTRUCTION <build_effort>]
    [EF_RUNTIME <search_beam>]
    [COMPACT_THRESHOLD <min_vectors>]
    [QUANTIZATION <TQ1|TQ2|TQ3|TQ4|SQ8>]
    [BUILD_MODE <LIGHT|EXACT>]
```

### Parameter Reference

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `DIM` | required | 1-65536 | Vector dimension |
| `TYPE` | FLOAT32 | FLOAT32 | Element type |
| `DISTANCE_METRIC` | L2 | L2, COSINE, IP | Distance function |
| `M` | 16 | 2-64 | HNSW max neighbors per layer. Higher = better recall, more memory |
| `EF_CONSTRUCTION` | 200 | 10-4096 | HNSW build effort. Higher = better graph quality, slower compaction |
| `EF_RUNTIME` | auto | 10-4096 | Search beam width. 0/omit = auto: max(k×15, 200). Higher = better recall, lower QPS. Tunable at runtime: `FT.CONFIG SET <idx> EF_RUNTIME <n>` (0 = restore auto) — applies to the next search, no rebuild |
| `COMPACT_THRESHOLD` | 1000 | 100-100000 | Min vectors before auto-compaction. Higher = fewer larger HNSW graphs |
| `QUANTIZATION` | TQ4 (COSINE/IP), SQ8 (L2) | TQ1-TQ4, SQ8 | Compression level. TQ4 = 4-bit (best compression, strongest on unit-sphere metrics; L2 uses a norm-corrected estimator), SQ8 = 8-bit (higher recall, all metrics — the default for L2) |
| `BUILD_MODE` | LIGHT | LIGHT, EXACT | HNSW build quality vs resource trade-off (see below) |

Two more per-index recall knobs are runtime-only (set via `FT.CONFIG`, persisted, applied on the next search):

| FT.CONFIG parameter | Default | Range | Description |
|---------------------|---------|-------|-------------|
| `RERANK_MULT` | 4 | 1-64 | Exact-rerank depth: re-score the top `mult×k` beam candidates with true f16 distances before truncation. Deeper recovers neighbors the quantized ranking dropped, at ~`mult·k·dim` f16 decodes per segment |
| `EXACT_BEAM` | OFF | ON/OFF | Navigate the HNSW beam with exact f16 distances instead of quantized estimates — recall becomes graph-limited (~1.0 at high ef). QPS cost grows with dimension; segments without an exact-rerank sidecar keep the quantized beam |

### BUILD_MODE: Light vs Exact

Both modes store and search the same segment layout; they differ only in how the HNSW graph is built at compaction, and in what the mutable segment retains until then.

| Aspect | LIGHT (default) | EXACT |
|--------|----------------|-------|
| **HNSW build oracle** | TQ-decoded centroid L2 (approximate) | Exact f32 L2 (retains raw vectors until compaction) |
| **Mutable-segment search** | TQ-ADC scan, then exact rerank of the top `RERANK_MULT·k` from the f16 rows | same |
| **Immutable-segment search** | HNSW beam (32-level sub-centroid LUT), then exact rerank of the top `RERANK_MULT·k` from the f16 sidecar | same |
| **Memory during insert** (384d TQ4, computed from the layout) | ~1.4 KB/vec: TQ code 260 B + FastScan shadow 256 B + sub-centroid signs 64 B + f16 row 768 B + 48 B entry | ~2.9 KB/vec: LIGHT + raw f32 1,536 B |
| **Memory after compaction** | TQ codes + sub-centroid signs + f16 exact-rerank sidecar + graph | same (EXACT keeps no QJL data since moon#1213) |
| **Compaction time (10K docs)** | graph from decoded centroids | 1.5–1.6 s at 384d, 2.5 s at 768d (Linux 4-vCPU container, WS11 — relative figures) |

No QJL correction runs in either mode any more: the mutable scan's QJL term was always multiplied by a zero residual (moon#1192), and compacted segments dropped their QJL data (moon#1213). Far / out-of-distribution queries used to lose recall on the mutable segment, which ranked by quantized ADC alone; it is now exact-reranked from the f16 rows it already keeps, like the immutable segments (moon#1226 — on an embedding-shaped 3,000 × 384d corpus, mutable-only, far-query R@10 went 0.703 → 0.990; relative evidence, not yet validated on real MiniLM embeddings).

**Recommendation**: Use `LIGHT` (default) for most workloads. Use `EXACT` when graph quality matters more than insert memory: it keeps each vector's raw f32 (4·dim bytes) until compaction, roughly doubling the mutable segment's footprint.

```bash
# Light mode (default) — fast insert, low memory, good recall
redis-cli FT.CREATE idx ... VECTOR HNSW 8 \
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2 BUILD_MODE LIGHT

# Exact mode — higher recall, more memory, slower compaction
redis-cli FT.CREATE idx ... VECTOR HNSW 8 \
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2 BUILD_MODE EXACT
```

### Tuning Profiles

**Maximum QPS** (R@10 ~89%, QPS ~3,000):
```
FT.CREATE idx ... VECTOR HNSW 10
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2
  M 12 EF_RUNTIME 100 COMPACT_THRESHOLD 1000 BUILD_MODE LIGHT
```

**Balanced** (R@10 ~92%, QPS ~1,400):
```
FT.CREATE idx ... VECTOR HNSW 8
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2
  BUILD_MODE EXACT
```

**High Recall** (R@10 ~95%, QPS ~800):
```
FT.CREATE idx ... VECTOR HNSW 14
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2
  M 24 EF_CONSTRUCTION 400 EF_RUNTIME 500 COMPACT_THRESHOLD 10000 BUILD_MODE EXACT
```

**Maximum Compression** (R@10 ~75%, 8× compression):
```
FT.CREATE idx ... VECTOR HNSW 8
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2 QUANTIZATION TQ2
```

## Commands

### FT.CREATE
Create a vector index with HNSW. Auto-indexes HSET commands matching the prefix.

### FT.SEARCH
```
FT.SEARCH <index> "*=>[KNN <k> @<field> $<param>]"
  PARAMS 2 <param> <vector_blob>
  [RETURN 0]
  [DIALECT 2]
```
Returns up to `k` nearest neighbors. The query vector must be a binary blob of `DIM × 4` bytes (little-endian f32).

A KNN prefilter goes before the arrow (`@lang:{en}=>[KNN …]`) or in `FILTER`. A tag value containing a space (`@body:{red apple}`) is a **full-text** filter: every word must occur in the field. Full-text filters are answered by the index's payload text index, which the process-wide opt-out `MOON_VECTOR_PAYLOAD_TEXT=off` drops to save memory (typically the largest per-document cost of a RAG-shaped index). With it off, a full-text filter is refused with `ERR full-text KNN filter … disabled by MOON_VECTOR_PAYLOAD_TEXT=off` rather than matching nothing; `FT.INFO` reports the setting as `payload_text_index` (`on`/`off`). The setting is environment-only, so give a primary and its replicas the same value. (Under `MOON_VECTOR_PAYLOAD_SCHEMA=declared`, a full-text filter on a declared `TEXT` field is answered by the BM25 plane and works either way.)

### FT.INFO
```
FT.INFO <index>
```
Returns index configuration (name, dimension, metric, quantization, build_mode) plus observability counters, additive across shards: `graph_segments` (immutable HNSW segment count) and `segments_with_exact_rerank` (how many of those segments still carry the f16 exact-rerank sidecar). Coverage below `graph_segments` means some segments answer with quantized ADC-only distances — a GraphUnion merge that drops a sidecar logs a `tracing::warn` when it happens. `payload_text_index` (`on`/`off`) says whether this process answers full-text KNN prefilters (see FT.SEARCH above).

### FT.COMPACT
```
FT.COMPACT <index>
```
Force compaction of the mutable segment into an HNSW immutable segment. Normally triggered automatically on first search.

### FT.DROPINDEX
```
FT.DROPINDEX <index>
```
Drop the index and free all associated memory.

### FLUSHALL / FLUSHDB / HDEL

`FLUSHALL` and `FLUSHDB` clear index contents — segments, key-hash maps, postings — while KEEPING the `FT.CREATE` definition. This matches what a restart produces today: vector/text index contents are always rebuilt from the keyspace on restart (only the `FT.CREATE` definition is durable). `FLUSHALL` clears every logical database's index contents; **`FLUSHDB` now scopes to the connection's currently-selected db** (WS5a) — an index owned by another db is left untouched.

> **WS5a: db-scoped indexes.** Every index is bound to exactly one logical db, set from the connection's currently-SELECTed db at `FT.CREATE` time. `FT.SEARCH`, `FT.INFO`, `FT._LIST`, `FT.DROPINDEX`, `FT.COMPACT`, `FT.CONFIG`, `FT.AGGREGATE`, `FT.CACHESEARCH`, `FT.RECOMMEND`, `FT.NAVIGATE`, `FT.INVALIDATE_RANGE`, and hybrid search all resolve indexes scoped to the caller's current db — an index owned by a different db is invisible (not merely empty), and this holds across single-shard, multi-shard tokio, and multi-shard monoio dispatch. The HSET auto-index hook and the DEL/HDEL/expiry auto-unindex hooks are similarly scoped: a write in db N only feeds/tombstones indexes owned by db N.
>
> **Naming**: index names stay globally unique per shard (not a composite `(db, name)` key) — creating a name that already exists in ANY db (including the same db) errors `"Index already exists"`. Want the same name reused per db? Rename it (e.g. `idx_db0`, `idx_db1`).
>
> **Known gaps** (documented, not silently mishandled): `SWAPDB` does **not** retag index ownership yet — an index stays bound to its original db even after `SWAPDB` moves the underlying keyspace, so `SWAPDB` + FT indexes is currently a footgun; avoid combining them until this closes. `MOVE`/`COPY` of an indexed hash does not auto-reindex into the target db (re-`HSET` there instead). The multi-shard remote leg of hybrid search's DFS scatter and `FT.AGGREGATE`'s partial-merge leg are unscoped on deployments with >1 shard (the single-shard fast path for both is fully scoped). The graph engine (`FT.NAVIGATE`'s underlying `GraphStore`, plus Cypher graph commands) remains structurally global across all logical dbs — not yet scoped to `SELECT`. None of this is the multi-tenancy isolation guarantee (see [Workspaces](guides/workspaces.md) for the shipped, UUID-prefix-based isolation mechanism) — it is a separate effort to scope FT indexes to Redis's `SELECT 0..N` logical databases. Full status: `.planning/v0.6.0-release/WS5A-NOTES.md` in the repo.

`HDEL key <vector-field>` tombstones that key in exactly the indexes whose vector field was removed (a sibling index keyed on a different field keeps its entry). Whole-key deletion (`DEL`/`UNLINK`) already tombstoned every index. Known limitations: an index with multiple vector fields tombstones the *whole* document if any one of its vector fields is removed (a later `HSET` re-indexes the remainder), and `TEXT`/`TAG`/`NUMERIC` field removal via `HDEL` is not yet re-indexed.

## How It Works

### Insert Path
1. Vector arrives via HSET
2. **TQ-MSE encoding**: normalize → zero-pad to power-of-2 → FWHT rotation → Lloyd-Max 4-bit quantize → nibble pack
3. Stored in mutable segment (384d TQ4, computed from the layout):
   - **Light mode**: ~1.4 KB/vec (TQ codes + norm, FastScan shadow, sub-centroid signs, f16 row for the exact rerank / sidecar)
   - **Exact mode**: ~2.9 KB/vec (Light + raw f32 retained for the exact-distance HNSW build)
4. **No HNSW at insert time** — append-only for maximum throughput (30K+ vec/s)

### Compaction
Triggered automatically on first search when mutable segment has ≥ `COMPACT_THRESHOLD` vectors:
1. Freeze mutable segment
2. **Light mode**: Build HNSW using TQ-decoded centroid pairwise distance
3. **Exact mode**: Build HNSW using exact f32 L2 pairwise distance (no QJL work since moon#1213)
4. BFS-reorder for cache locality
5. Compute sub-centroid sign bits (doubles quantization resolution: 16 → 32 levels)
6. Create immutable segment
7. **Adaptive-ef self-probe (AE-1):** 16 leave-self-out sample queries measure R@10 against the segment's own exact f16 sidecar across an ef ladder (24..256). A fully-saturated curve (flat ≈1.0 from the minimum rung) certifies the segment as "trivially easy" for min-ef search; every other segment keeps the full resolved ef at query time. In-memory only — not persisted, so a segment reloaded from disk always searches at the full beam.

### Search Path
1. Query vector → normalize → FWHT rotate
2. Build per-query LUT: precomputed distance² for each sub-centroid (32 entries × dim, fits L1 cache)
3. **HNSW beam search** with 32-level sub-centroid LUT scoring. Beam width (`ef`) is the full resolved value, unless the segment's compact-time saturation probe (AE-1, above) certified it "trivially easy" — then it searches at min-ef (24) instead. Never overridden when the user pins `EF_RUNTIME`.
4. **Exact rerank:** the top `4·k` beam candidates are re-scored against the segment's f16 sidecar with true metric distances (SIMD: NEON integer-rescale on aarch64, F16C+FMA on x86_64, scalar fallback) before truncation. Segments without a sidecar (pre-HQ-1 reload, or a GraphUnion merge that dropped one) fall back to quantized ADC-only distances — check `FT.INFO`'s `segments_with_exact_rerank` for coverage.
5. Merge results from mutable (brute-force) + immutable (HNSW) segments. The mutable scan ranks by TQ-ADC, then exact-reranks its top `RERANK_MULT·k` from the f16 rows it keeps (moon#1226), so every segment answers with the same distance convention
6. Return top-K results

## Memory Usage

| Stage | Light Mode | Exact Mode | Notes |
|-------|-----------|-----------|-------|
| During insert (mutable) | ~1.4 KB/vec | ~2.9 KB/vec | 384d TQ4, computed from the layout (see BUILD_MODE); Light skips raw f32 retention |
| After compaction (immutable) | ~452 B/vec | ~644 B/vec | Historical macOS figures from before the f16 exact-rerank sidecar (+2·dim B/vec) and before EXACT dropped its QJL data (moon#1213) — both modes now share one layout; not yet re-measured on Linux |
| Redis Stack (FP32) | — | — | ~3,840 B/vec |
| Qdrant (FP32) | — | — | ~1,536 B/vec |

**Moon Light stored 8.5× less memory per vector than Redis Stack on the macOS M4 Pro
rig below (452 B vs 3,840 B).** Not reproduced on Linux; `CLAUDE.md` requires
production numbers to come from a Linux host, so treat this as a development
reference rather than a production figure.

## Performance Benchmarks

Measured on macOS M4 Pro, single-client TCP, all-MiniLM-L6-v2 (384d, 10K vectors):

| Metric | Moon Light | Moon Exact | Redis Stack | Qdrant |
|--------|-----------|-----------|-------------|--------|
| Insert | **31,683 v/s** | 30,312 v/s | 4,747 v/s | 6,719 v/s |
| QPS (k=10) | **3,012** | 1,382 | 2,910 | 774 |
| p50 latency | **315 μs** | 715 μs | 313 μs | 984 μs |
| R@1 | 86% | 90% | 45% | 99% |
| R@10 | 89% | 92% | 95% | 96% |
| Memory/vec | **452 B** | 644 B | 3,840 B | ~1,536 B |

### Key Trade-offs

- **Moon Light**: Matches Redis QPS (3K), 6.7× faster insert, 8.5× less memory — all on the macOS rig above. Trades ~6% R@10 vs Redis.
- **Moon Exact**: 1.4× faster QPS than Qdrant, 4.7× faster insert, 2.4× less memory. Trades ~4% R@10.
- **First search latency**: Light ~1.6s, Exact ~8.6s (HNSW compaction). Subsequent searches are fast.

## Multi-Shard

```bash
# Start with multiple shards (requires --shards >= 2)
./moon --port 6379 --shards 4 --protected-mode no
```

FT.CREATE automatically broadcasts to all shards. FT.SEARCH scatters queries and merges results across shards. Use hash tags `{tag}` in key names for shard co-location if needed.

## Quantization Bit Widths

| Quantization | Bits/coord | Memory/vec (384d) | Expected R@10 |
|---|---|---|---|
| TQ1 | 1-bit | ~130 B | ~60% |
| TQ2 | 2-bit | ~195 B | ~75% |
| TQ3 | 3-bit | ~320 B | ~85% |
| **TQ4** | **4-bit** | **~452 B** | **~89%** |
| SQ8 | 8-bit | ~900 B | ~98% |

TQ4 (default) provides the best balance of compression and recall. Use SQ8 for higher recall at 2× the memory.
