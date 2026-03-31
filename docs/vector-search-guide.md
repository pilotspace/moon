# Moon Vector Search — User Guide

Moon provides Redis-compatible vector search with TurboQuant 4-bit compression, achieving 6× less memory per vector than Redis while maintaining >90% recall.

## Quick Start

```bash
# Start Moon
./moon --port 6379 --shards 1 --protected-mode no

# Create a vector index
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
```

### Parameter Reference

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `DIM` | required | 1-65536 | Vector dimension |
| `TYPE` | FLOAT32 | FLOAT32 | Element type |
| `DISTANCE_METRIC` | L2 | L2, COSINE, IP | Distance function |
| `M` | 16 | 2-64 | HNSW max neighbors per layer. Higher = better recall, more memory |
| `EF_CONSTRUCTION` | 200 | 10-4096 | HNSW build effort. Higher = better graph quality, slower insert |
| `EF_RUNTIME` | auto | 10-4096 | Search beam width. 0/omit = auto: max(k×15, 200). Higher = better recall, lower QPS |
| `COMPACT_THRESHOLD` | 1000 | 100-100000 | Min vectors before auto-compaction. Higher = fewer larger HNSW graphs |
| `QUANTIZATION` | TQ4 | TQ1-TQ4, SQ8 | Compression level. TQ4 = 4-bit (best compression), SQ8 = 8-bit (less compression, higher recall) |

### Tuning Profiles

**High Recall** (R@10 ~95%, QPS ~800):
```
FT.CREATE idx ... VECTOR HNSW 14
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2
  M 24 EF_CONSTRUCTION 400 EF_RUNTIME 500 COMPACT_THRESHOLD 10000
```

**High QPS** (R@10 ~88%, QPS ~2000):
```
FT.CREATE idx ... VECTOR HNSW 10
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2
  M 12 EF_RUNTIME 100 COMPACT_THRESHOLD 1000
```

**Balanced** (R@10 ~92%, QPS ~1400):
```
FT.CREATE idx ... VECTOR HNSW 6
  TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2
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
Returns index configuration: name, dimension, metric, M, EF_CONSTRUCTION, EF_RUNTIME, COMPACT_THRESHOLD, QUANTIZATION.

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
3. Stored in mutable segment: ~260 bytes TQ code + raw f32 (retained for compaction)
4. **No HNSW at insert time** — append-only for maximum throughput

### Compaction
Triggered automatically on first search when mutable segment has ≥ `COMPACT_THRESHOLD` vectors:
1. Freeze mutable segment
2. Recompute QJL signs from retained raw f32 vectors
3. Build HNSW graph using **exact f32 L2** pairwise distance
4. BFS-reorder for cache locality
5. Compute sub-centroid sign bits (doubles quantization resolution)
6. Create immutable segment (644 bytes/vec steady state)

### Search Path
1. Query vector → normalize → FWHT rotate
2. Build per-query LUT: precomputed distance² for each centroid (fits L1 cache)
3. **HNSW beam search** with sub-centroid 32-level LUT scoring
4. Return top-K results

## Memory Usage

| Stage | Per Vector | Notes |
|-------|-----------|-------|
| During insert (mutable) | ~1,900 B | Includes raw f32 retention |
| After compaction (immutable) | ~644 B | TQ codes + signs + HNSW edges |
| Redis Stack (FP32) | ~3,840 B | For comparison |
| Qdrant (FP32) | ~1,536 B | For comparison |

**Moon uses 6× less memory per vector than Redis** at 4-bit quantization.

## Performance Expectations

Measured on macOS M4 Pro, single-client TCP, all-MiniLM-L6-v2 (384d, 10K vectors):

| Metric | Moon TQ-4bit | Redis Stack | Qdrant |
|--------|-------------|-------------|--------|
| Insert | 30,873 v/s | 4,182 v/s | 6,644 v/s |
| QPS (k=10) | 1,382 | 3,847 | 982 |
| p50 latency | 715 μs | 261 μs | 984 μs |
| R@1 | 90% | 45% | 100% |
| R@10 | 92% | 95% | 96% |
| Memory/vec | 644 B | 3,840 B | ~1,536 B |

### Trade-offs

- **Moon excels at**: Insert throughput (7× Redis), memory efficiency (6× less), QPS vs Qdrant (1.4× faster)
- **Moon trades off**: ~4% recall vs FP32 engines (92% vs 96%) due to 4-bit quantization
- **First search is slow** (~6s for 10K vectors) because it triggers HNSW compaction. Subsequent searches are fast.

## Multi-Shard

```bash
# Start with multiple shards
./moon --port 6379 --shards 4 --protected-mode no
```

FT.CREATE automatically broadcasts to all shards. FT.SEARCH scatters queries and merges results across shards.

## Quantization Bit Widths

| Quantization | Bits/coord | Memory/vec (384d) | Expected R@10 |
|---|---|---|---|
| TQ1 | 1-bit | ~130 B | ~60% |
| TQ2 | 2-bit | ~195 B | ~75% |
| TQ3 | 3-bit | ~320 B | ~85% |
| **TQ4** | **4-bit** | **~644 B** | **~92%** |
| SQ8 | 8-bit | ~900 B | ~98% |

TQ4 (default) provides the best balance of compression and recall.
