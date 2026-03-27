# Building ACID-compliant vector search into a Rust-based Redis replacement

A Rust-based Redis replacement can outperform both Redis's vector search and dedicated vector databases by combining **native HNSW indexing with MVCC-based ACID transactions, SIMD-accelerated distance computation, and a unified KV + vector storage engine**. The key architectural insight is that pgvector proves ACID compliance imposes only ~15–30% overhead versus non-transactional systems, while the Rust memory model and lock-free concurrency primitives eliminate the garbage-collection pauses and thread-safety overhead plaguing Go-based (Weaviate, Milvus) and C++-based (hnswlib) competitors. This blueprint details the specific data structures, algorithms, and architecture decisions required to achieve **>20K QPS at 0.95 recall on 768d embeddings** with full snapshot isolation guarantees.

---

## How Redis and competitors implement vector search today

**Redis's approach (v8.0+)** integrates the former RediSearch module directly into the core distribution. It offers three index types: FLAT (brute-force), HNSW (modified hnswlib), and SVS-VAMANA (Intel's graph with built-in SQ8 compression, added in v8.2). Vectors are stored as binary blobs in HASH or JSON fields, with the HNSW graph maintained in a separate in-memory structure. Redis's critical weakness is that **vectors are duplicated between the keyspace and the vector index**, roughly doubling memory consumption. A 768d float32 vector costs ~6.1 KB total (3 KB data + 3 KB index copy + ~300 bytes graph overhead at M=16). Redis's MULTI/EXEC transactions provide sequential execution but **no rollback on partial failure**, and vector index creation commands (FT.CREATE) cannot execute within transaction blocks at all. Redis 8.4 added FT.HYBRID for combining BM25 + vector search via Reciprocal Rank Fusion, and Redis 8.6 added SIMD-vectorized binary quantization. Redis benchmarks at billion scale show **200ms median latency at 90% recall** for 768d vectors — far above the <10ms target for RAG applications.

**Qdrant** (Rust-native) represents the strongest competitive target. Its segment-based architecture separates mutable (appendable) segments from immutable (optimized) segments, with background optimization building HNSW indexes on immutable segments. Qdrant recently replaced RocksDB with **Gridstore**, a pure-Rust key-value store using fixed 128-byte blocks with O(1) key lookup via sequential integer IDs. Its signature innovation is **filterable HNSW**: during index construction, Qdrant builds per-payload-value subgraphs that maintain graph connectivity under arbitrary filters, adding at most 2× total edges. Qdrant uses multiple Tokio runtimes (`search_runtime`, `update_runtime`, `general_runtime`) for workload isolation. However, Qdrant provides **no ACID transactions** — operations are WAL-durable but not transactional across collections.

**pgvector** is the only existing system achieving full ACID with vector indexes. It delegates MVCC entirely to PostgreSQL's infrastructure: the HNSW graph contains entries for all versions, and visibility checks occur at the heap-tuple level during index scans. WAL records capture page-level changes using PostgreSQL's Generic WAL API. Vacuum involves a costly three-pass process: collecting dead tuples, repairing graph connectivity (running full HNSW neighbor selection), then deleting entries. The pgvectorscale extension achieved **471 QPS at 99% recall on 50M 768d vectors** — 11.4× higher throughput than Qdrant in the same benchmark, demonstrating that ACID compliance need not sacrifice performance.

**Milvus** uses a disaggregated microservice architecture with growing segments (in-memory, accepting writes) that seal into immutable segments stored in object storage. DiskANN integration enables disk-resident indexing. **Weaviate** implements the ACORN algorithm (SIGMOD 2024) for filtered HNSW, using 2-hop neighbor expansion to maintain graph connectivity under restrictive filters. **LanceDB** is Rust-native with the Lance columnar format offering 1000× faster random access than Parquet and built-in MVCC versioning. **Turbopuffer** is a Rust-based, object-storage-first system using SPFresh/SPANN centroid-based indexes optimized for S3 access patterns, achieving 10× lower cost than memory-resident databases.

---

## Core storage engine architecture

The storage engine should adopt a **segment-based design inspired by Qdrant and Milvus**, but with a critical addition: an integrated MVCC layer providing snapshot isolation across both KV and vector operations.

### Unified KV + vector storage with MVCC

Each record in the system is a versioned key-value entry where the value may contain both arbitrary data fields and a vector embedding:

```
Record {
    key: Bytes,
    version: u64,          // Monotonic LSN from WAL
    insert_lsn: u64,       // Transaction that created this version
    delete_lsn: u64,       // Transaction that deleted (0 = active)
    fields: HashMap<Bytes, Bytes>,  // Arbitrary KV data
    vector: Option<AlignedVec<f32>>, // Optional embedding
    vector_dim: u16,
}
```

The **visibility rule** for snapshot isolation: a record is visible to snapshot S if `insert_lsn <= S.snapshot_lsn` and the inserting transaction committed, and `delete_lsn == 0 OR delete_lsn > S.snapshot_lsn`. This single visibility predicate governs both KV lookups and HNSW graph traversal, ensuring a query touching both KV attributes and vector similarity sees the same consistent snapshot.

### Segment architecture

Data resides in segments of two types:

- **Mutable segments** accept writes into an in-memory red-black tree (for KV lookups) and a flat vector buffer (for brute-force search on recently-inserted vectors). When a mutable segment exceeds **64 MB**, it freezes and becomes a candidate for optimization.
- **Immutable segments** contain a sorted KV store (custom Rust implementation similar to Qdrant's Gridstore, using sequential integer IDs with O(1) lookup), a fully-built HNSW index, payload indexes (Roaring bitmaps per indexed field), and an optional inverted index for BM25 text search.

A **SegmentHolder** coordinates reads across all segments. Every query fans out to all segments (typically 3–8 active segments), and results merge by distance. Background optimization merges small segments into larger ones, builds HNSW indexes, and runs vacuum to remove invisible versions.

### Write-ahead log design

A single WAL serves both KV and vector operations, ensuring atomic commits. WAL record types:

| Record Type | Payload | Size |
|------------|---------|------|
| `KV_PUT` | key, fields, vector_bytes, txn_id | Variable (vectors dominate) |
| `KV_DELETE` | key, txn_id | ~50 bytes |
| `TXN_COMMIT` | txn_id, commit_lsn | 16 bytes |
| `TXN_ABORT` | txn_id | 8 bytes |
| `CHECKPOINT` | segment_id, last_lsn | ~100 bytes |

**Delta encoding** for vector updates: when only metadata changes (not the vector), the WAL records only field diffs, avoiding re-logging the full vector. For 1536d float32 vectors (6 KB each), this reduces WAL volume by **>90%** for metadata-only updates. WAL segments are **32 MB** (matching Qdrant's default), pre-allocated for sequential writes, and fsync'd per commit (or batched with `group_commit_interval_us` for throughput). Crash recovery replays WAL from the last checkpoint LSN, reapplying operations idempotently.

---

## HNSW index with ACID guarantees

### Concurrent MVCC-aware HNSW

The HNSW index is the performance-critical component. The implementation should be **pure Rust**, drawing from `hnswlib-rs` (concurrent search + mutation via per-NodeId atomic swap) and `hnsw_rs` (multithreaded construction via `parking_lot`).

**Memory layout per node:**

```rust
struct HnswNode {
    id: u32,                          // 4 bytes — internal node ID
    insert_lsn: u64,                  // 8 bytes — MVCC insert version
    delete_lsn: AtomicU64,            // 8 bytes — MVCC delete version (atomic for lock-free reads)
    level: u8,                        // 1 byte
    neighbors_l0: [AtomicU32; M0],    // M0 * 4 bytes — base layer (M0 = 2*M = 32 → 128 bytes)
    neighbor_count_l0: AtomicU16,     // 2 bytes
    neighbors_upper: Vec<[AtomicU32; M]>, // M * 4 bytes per layer above 0
    vector_offset: u64,               // 8 bytes — pointer into vector storage
}
```

For **M=16** (M0=32), each node consumes ~170 bytes of graph metadata, plus vector storage. Total memory per vector at 768d float32: **3,072 + 170 = ~3,242 bytes** — roughly half of Redis's ~6,100 bytes due to eliminating vector duplication.

**Concurrency model:**

- **Reads (searches)**: Completely lock-free. Pin a `crossbeam::epoch::Guard`, traverse the graph reading `AtomicU32` neighbor lists. Visibility check via `insert_lsn`/`delete_lsn` against the transaction's snapshot — invisible nodes are **still traversed** (to maintain graph connectivity) but excluded from results.
- **Writes (inserts)**: Use **hash-striped locks** (Lucene's approach): `locks[hash(node_id) % 256]` as `parking_lot::RwLock`. Insert acquires shared lock on the inserting node's stripe and exclusive locks on neighbors being modified. This allows concurrent inserts in different graph regions.
- **Deletes**: Set `delete_lsn` atomically. The node remains in the graph for traversal. Background vacuum runs the three-pass pgvector-style cleanup: identify dead nodes → repair neighbor connectivity → reclaim storage.
- **Entry point updates**: Rare (only when a new node has a higher layer than current entry). Acquire global `entry_point_lock` in exclusive mode.

### Making HNSW changes durable

HNSW mutations are **not individually WAL-logged** at the graph level. Instead, the approach mirrors SingleStore-V: the KV WAL records the vector data insert/update/delete, and the HNSW index is reconstructed per-segment during background optimization. For mutable segments (recently-written data), brute-force search over the flat buffer provides immediate queryability without an HNSW index. This means:

- **No HNSW-specific WAL records** (eliminating the expensive neighbor-list WAL entries that plague pgvector)
- **Crash recovery** rebuilds the mutable segment's flat buffer from WAL; immutable segments with HNSW indexes are already on disk
- **Index build** happens asynchronously when segments freeze, not in the critical write path

This design gives **write throughput comparable to non-ACID systems** while maintaining full ACID for data operations. The tradeoff is that recently-written vectors (in the mutable segment) are searched via brute-force, but with segments limited to 64 MB (~20K 768d vectors), brute-force search takes <5ms with SIMD acceleration.

---

## SIMD-accelerated distance computation

Distance computation consumes **40–60% of CPU time** during HNSW traversal. The implementation must provide architecture-specific SIMD kernels with runtime detection:

```rust
pub fn l2_distance(x: &[f32], y: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") { return unsafe { l2_avx512(x, y) }; }
        if is_x86_feature_detected!("avx2")    { return unsafe { l2_avx2(x, y) }; }
        if is_x86_feature_detected!("fma")      { return unsafe { l2_fma(x, y) }; }
    }
    #[cfg(target_arch = "aarch64")]
    { return unsafe { l2_neon(x, y) }; }
    l2_scalar(x, y)
}
```

**Performance by ISA for 768d L2 distance:**

| ISA | Elements/cycle | Speedup vs scalar | Notes |
|-----|---------------|-------------------|-------|
| Scalar | 1 | 1× | Baseline |
| SSE4.1 (f32×4) | 4 | ~3.5× | Minimum x86 target |
| AVX2 (f32×8) | 8 | ~5× | Production standard |
| AVX-512 (f32×16) | 16 | ~5.5× | ~10% over AVX2 (memory-bound) |
| AVX-512 VNNI (int8) | 64 | ~15× | For SQ int8 distances |
| ARM NEON (f32×4) | 4 | ~3× | Apple Silicon / ARM servers |

For **quantized int8 distance**, AVX-512 VNNI's `_mm512_dpbusd_epi32` computes 64 int8 multiply-accumulates per instruction, achieving **~4× speedup over AVX2 int8** and enabling SQ searches at >50K QPS on SIFT-1M. The `simsimd` crate provides production-grade Rust bindings covering f32, f16, bf16, i8, and binary distances across all major ISAs.

**Binary quantization distance** uses XOR + popcount: AVX-512's `_mm512_popcnt_epi64` (VPOPCNTDQ extension) computes Hamming distance on 512-bit chunks, delivering up to **40× speedup** over float32 distance for 1536d vectors (OpenAI embeddings). Combined with oversampling (10× candidates) and float32 reranking, binary quantization achieves **96–99% recall** at a fraction of the compute cost.

---

## Quantization and memory-efficient storage

### Multi-level quantization strategy

The system should support four quantization levels, selectable per-collection:

| Method | Memory/vector (768d) | Compression | Recall@10 | Best for |
|--------|---------------------|-------------|-----------|----------|
| float32 | 3,072 B | 1× | 100% | Maximum accuracy |
| int8 SQ | 768 B + 6 KB calibration | 4× | 98–99% | **Production default** |
| RaBitQ | 104 B | ~30× | 95%+ (with rerank) | Large-scale, cost-sensitive |
| Binary | 96 B | 32× | 80–92% (raw) | Ultra-fast shortlisting |

**RaBitQ** (SIGMOD 2024) deserves special attention as a next-generation replacement for both PQ and BQ. It quantizes D-dimensional vectors into D-bit strings using a random orthogonal rotation, plus two float32 corrective scalars per vector. Unlike PQ, RaBitQ requires **no codebook storage or training**, provides **theoretical error bounds** (O(1/√D)), and uses bitwise operations for distance computation. Elastic has adopted it as "BBQ" (Better Binary Quantization), and LanceDB offers it as `IVF_RQ`. Extended RaBitQ (SIGMOD 2025) generalizes to 2–9 bits per dimension, achieving near-SQ quality at 8× compression.

**Recommended default**: Store **int8 SQ vectors in RAM** for HNSW traversal, **float32 vectors on mmap/SSD** for reranking. This gives 4× RAM reduction with <2% recall loss, and the reranking step with full-precision vectors recovers most of that 2%.

### Tiered storage architecture

```
┌─────────────────────────────────────────────────┐
│                   HOT TIER (RAM)                │
│  • HNSW graph structure (~170 bytes/vector)     │
│  • Quantized vectors (SQ int8: 1 byte/dim)      │
│  • Roaring bitmap payload indexes               │
│  • Actively-queried segment metadata            │
├─────────────────────────────────────────────────┤
│                WARM TIER (mmap/NVMe)            │
│  • Full-precision float32 vectors (for rerank)  │
│  • Payload data (accessed on demand)            │
│  • Less-frequently-accessed segments            │
├─────────────────────────────────────────────────┤
│               COLD TIER (SSD/Object Storage)    │
│  • Archived segments                            │
│  • DiskANN/Vamana indexes for cold data         │
│  • Backup and replication data                  │
└─────────────────────────────────────────────────┘
```

For **1 million 768d vectors**: hot tier = ~940 MB (graph 170 MB + SQ vectors 768 MB), warm tier = ~3 GB (float32 vectors via mmap), versus Redis's ~6+ GB all in RAM. At 10M vectors, the system requires **~9.4 GB RAM** versus Redis's ~60+ GB, a **6× reduction**.

Memory-mapped storage uses the `memmap2` crate with `madvise` hints: `MADV_RANDOM` for HNSW vector access patterns (random jumps during graph traversal) and `MADV_SEQUENTIAL` for batch operations. Qdrant benchmarks show that with vectors on mmap and graph in RAM, **1M 128d vectors fit in 625 MB** (vs. 1.2 GB all-RAM), with latency dependent on NVMe IOPS — modern NVMe SSDs at 500K+ IOPS deliver acceptable performance for warm data.

---

## Hybrid query engine with cost-based optimization

### Three-strategy filter execution

The query planner selects per-segment from three strategies based on filter selectivity estimation:

**Strategy 1 — Brute-force on filtered subset** (selectivity < 2% or < 20K matching vectors): Intersect Roaring bitmap indexes for all filter predicates, then compute distances to all matching vectors. With SIMD, brute-force over 20K 768d vectors takes ~2ms. This avoids HNSW entirely for highly selective filters.

**Strategy 2 — Filterable HNSW traversal** (selectivity 2–80%): During HNSW beam search, check each candidate against the precomputed Roaring bitmap allow-list. Non-matching nodes are still traversed (to maintain graph connectivity) but excluded from the result set. Implement ACORN-style 2-hop expansion: when a neighbor fails the filter, also explore that neighbor's neighbors, reaching filter-passing nodes in sparse graph regions. This handles negatively correlated filters (where the nearest vectors to the query are mostly filtered out) gracefully.

**Strategy 3 — Standard HNSW with post-filter** (selectivity > 80%): Perform normal HNSW search with oversampling (request 3× K candidates), then post-filter. When >80% of vectors pass the filter, graph connectivity is minimally affected and the overhead of per-node filter checks outweighs the benefit.

**Selectivity estimation** uses per-field statistics maintained by payload indexes: distinct value counts, min/max for numeric fields, and approximate cardinality from Roaring bitmap `len()`. For compound filters, assume independence (multiply selectivities) with a correction factor learned from query history.

### Hybrid BM25 + vector search

For combined keyword and semantic search, the engine runs BM25 (via inverted index with BlockMax WAND scoring) and vector similarity in parallel, then fuses results. Two fusion methods:

- **Reciprocal Rank Fusion**: `RRF_score(d) = Σ 1/(k + rank_r(d))` with k=60. Score-agnostic, robust, requires no normalization. **Recommended default**.
- **Linear combination**: `α × normalize(bm25) + (1-α) × normalize(vector)`. Higher potential accuracy when tuned, but requires min-max score normalization.

Redis 8.4's FT.HYBRID command validates this architecture direction — it implements exactly this pattern with RRF and linear combination options.

---

## Distributed architecture for billion-scale deployment

### Sharding and query routing

**Hash-based sharding** (consistent hashing on vector ID) is the pragmatic choice for a Redis replacement, as it provides even distribution and simple routing. Every ANN query must scatter to all shards — this is inherent to approximate search where the nearest vectors may reside on any shard. The SPIRE paper (SIGMOD 2025) demonstrated that naive graph sharding causes **80%+ of search steps to become cross-node RPCs** for a 100M-vector, 5-node HNSW setup, confirming that **each shard must maintain its own complete local HNSW index**.

**Query execution**: The coordinator broadcasts the query to all shards with parameter `local_k = K × α` (α = 1.5–3.0 oversampling factor). Each shard performs local HNSW search and returns `local_k` candidates. The coordinator merges results by distance, returning the global top-K. For **p99 latency** management, set aggressive per-shard timeouts; a slow shard can be excluded from results (ANN tolerates missing some candidates).

### Replication strategy

Use **WAL-based streaming replication**: each shard's WAL is streamed to replicas, which replay operations to maintain synchronized KV stores and rebuild HNSW indexes on their own immutable segments. This avoids the expense of transferring full HNSW graph snapshots (which can exceed 1 GB for 10M vectors).

The consistency model should be **configurable per-operation**:

- **Default (session consistency)**: Reads from the shard that processed the write within the same session see their own writes. Other sessions may see stale data briefly.
- **Strong (quorum)**: Writes acknowledged only after `(replication_factor / 2) + 1` replicas confirm WAL receipt. Reads served from any replica that has reached the required LSN.
- **Eventual**: Writes acknowledged after local WAL flush. Maximum throughput, suitable for bulk ingestion.

### Performance targets at scale

Based on analysis of all benchmark data, competitive targets for the system:

| Metric | 1M vectors (768d) | 10M vectors | 100M vectors |
|--------|-------------------|-------------|--------------|
| QPS at 0.95 recall (8-core) | **15,000** | **5,000** | **1,500** |
| p50 latency | **1 ms** | **3 ms** | **10 ms** |
| p99 latency | **5 ms** | **15 ms** | **50 ms** |
| RAM per node (SQ + graph) | **940 MB** | **9.4 GB** | **94 GB** |
| Write throughput | **50K vec/sec** | **30K vec/sec** | **15K vec/sec** |
| Index build time (16 cores) | **1 min** | **15 min** | **3 hours** |

These targets exceed Redis by **3–5×** on QPS (Redis achieves ~5,000 QPS at 0.95 recall on 768d at comparable scale), match or exceed Qdrant on filtered search, and maintain sub-10ms p99 for RAG applications at the 1M–10M scale most production deployments target.

---

## Recommended Rust crate ecosystem

The implementation should leverage these specific crates:

- **HNSW core**: `hnswlib-rs` for concurrent search + mutation with lock-free reads, or custom implementation using `parking_lot` for hash-striped locks and `crossbeam::epoch` for epoch-based memory reclamation
- **SIMD distances**: `simsimd` (production-grade, covers f32/f16/i8/binary across AVX2/AVX-512/NEON) supplemented by hand-written `std::arch` intrinsics for hot paths
- **Bitmap indexes**: `roaring` crate (RoaringBitmap/roaring-rs) for payload filtering
- **Memory mapping**: `memmap2` with `madvise` hints for tiered vector storage
- **Serialization**: `bincode` for WAL records and segment metadata
- **Async runtime**: `tokio` with dedicated runtime instances per workload type (search, write, background optimization)
- **Concurrency**: `crossbeam` for epoch-based reclamation, `parking_lot` for low-overhead mutexes

---

## Conclusion

The architectural blueprint presented here achieves a unique position in the vector database landscape: **full ACID transactions without the performance penalty traditionally associated with transactional guarantees**. Three design decisions make this possible. First, deferring HNSW construction to background segment optimization (rather than WAL-logging individual graph mutations) eliminates the write amplification that makes pgvector's approach expensive. Second, index-level MVCC visibility checks during graph traversal — where invisible nodes are traversed but excluded from results — provides snapshot isolation without requiring heap lookups per candidate. Third, the unified WAL for KV and vector operations ensures atomic upserts (vector + metadata in a single transaction) with a single fsync, matching Redis's write simplicity while adding rollback capability.

The most impactful technical bets are **RaBitQ quantization** (32× compression with theoretical error bounds, replacing both PQ and BQ), **ACORN-style filtered HNSW** (maintaining recall under restrictive filters where competitors degrade), and **Rust's ownership model** enabling truly lock-free concurrent reads without garbage-collection pauses. Combined with SIMD-accelerated distance computation achieving 5–15× speedups over scalar code, the system can realistically target **15,000 QPS at 0.95 recall on 768d embeddings** — outperforming Redis (in-memory but non-ACID), Qdrant (Rust but non-ACID), and pgvector (ACID but slower) at their respective weaknesses.