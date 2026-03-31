# Moon Vector Search — User Guide

Moon provides Redis-compatible vector search with TurboQuant 4-bit compression, achieving up to 8.5× less memory per vector than Redis while matching its search QPS.

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
| `EF_RUNTIME` | auto | 10-4096 | Search beam width. 0/omit = auto: max(k×15, 200). Higher = better recall, lower QPS |
| `COMPACT_THRESHOLD` | 1000 | 100-100000 | Min vectors before auto-compaction. Higher = fewer larger HNSW graphs |
| `QUANTIZATION` | TQ4 | TQ1-TQ4, SQ8 | Compression level. TQ4 = 4-bit (best compression), SQ8 = 8-bit (higher recall) |
| `BUILD_MODE` | LIGHT | LIGHT, EXACT | HNSW build quality vs resource trade-off (see below) |

### BUILD_MODE: Light vs Exact

| Aspect | LIGHT (default) | EXACT |
|--------|----------------|-------|
| **HNSW build oracle** | TQ-decoded centroid L2 (approximate) | Exact f32 L2 (retains raw vectors) |
| **QJL correction** | Disabled (not needed with sub-centroid) | Enabled (M=8 dense Gaussian projections) |
| **Memory during insert** | ~372 B/vec | ~1,844 B/vec |
| **Memory after compaction** | ~452 B/vec | ~644 B/vec |
| **Compaction time (10K)** | ~1.6 s | ~8.6 s |
| **First-search latency** | ~1.6 s (compaction) | ~8.6 s (compaction + QJL recompute) |
| **R@10 (384d, 10K)** | ~89% | ~92% |
| **QPS** | ~3,000 | ~1,400 |

**Recommendation**: Use `LIGHT` (default) for most workloads. Use `EXACT` only when you need the extra 3% recall and can tolerate 5× more memory during insert and slower compaction.

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

### FT.INFO
```
FT.INFO <index>
```
Returns index configuration: name, dimension, metric, quantization, build_mode.

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

## How It Works

### Insert Path
1. Vector arrives via HSET
2. **TQ-MSE encoding**: normalize → zero-pad to power-of-2 → FWHT rotation → Lloyd-Max 4-bit quantize → nibble pack
3. Stored in mutable segment:
   - **Light mode**: ~372 B/vec (TQ codes + norm only)
   - **Exact mode**: ~1,844 B/vec (TQ codes + raw f32 retained for HNSW build)
4. **No HNSW at insert time** — append-only for maximum throughput (30K+ vec/s)

### Compaction
Triggered automatically on first search when mutable segment has ≥ `COMPACT_THRESHOLD` vectors:
1. Freeze mutable segment
2. **Light mode**: Build HNSW using TQ-decoded centroid pairwise distance
3. **Exact mode**: Recompute QJL signs, build HNSW using exact f32 L2 pairwise distance
4. BFS-reorder for cache locality
5. Compute sub-centroid sign bits (doubles quantization resolution: 16 → 32 levels)
6. Create immutable segment

### Search Path
1. Query vector → normalize → FWHT rotate
2. Build per-query LUT: precomputed distance² for each sub-centroid (32 entries × dim, fits L1 cache)
3. **HNSW beam search** with 32-level sub-centroid LUT scoring (no separate rerank needed)
4. Merge results from mutable (brute-force) + immutable (HNSW) segments
5. Return top-K results

## Memory Usage

| Stage | Light Mode | Exact Mode | Notes |
|-------|-----------|-----------|-------|
| During insert (mutable) | ~372 B/vec | ~1,844 B/vec | Light skips raw f32 retention |
| After compaction (immutable) | ~452 B/vec | ~644 B/vec | Light skips QJL signs |
| Redis Stack (FP32) | — | — | ~3,840 B/vec |
| Qdrant (FP32) | — | — | ~1,536 B/vec |

**Moon Light uses 8.5× less memory per vector than Redis.**

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

- **Moon Light**: Matches Redis QPS (3K), 6.7× faster insert, 8.5× less memory. Trades ~6% R@10 vs Redis.
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
