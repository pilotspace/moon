# Semantic Cache with Moon

Eliminate redundant LLM API calls by caching responses based on query vector similarity, not exact string matching.

## The problem

Traditional caches use exact key matching. But users ask the same question in many ways:

- "What is Moon's architecture?"
- "How is Moon designed internally?"
- "Explain Moon's system design"

All three should return the same cached answer, but an exact-match cache misses every time.

## The solution

Moon's semantic cache (`FT.CACHESEARCH`) compares query **embeddings** against cached entries. If a new query is semantically similar enough (within a configurable distance threshold), the cached response is returned instantly -- no LLM call needed.

## Cost savings

| Metric | Without cache | With semantic cache |
|--------|--------------|-------------------|
| LLM calls | 100 | ~20 (80% hit rate) |
| Avg latency | 800ms | ~10ms (cache hits) |
| API cost | $1.00 | ~$0.20 |

## Prerequisites

- Moon server running on `localhost:6379`
- Python 3.9+

## Setup

```bash
cd examples/semantic-cache
pip install -r requirements.txt
```

## Run

```bash
python semantic_cache.py
```

## How it works

1. **First query** arrives -- no cache entry exists. Full processing (simulated LLM call), then store the query vector + response in the cache.
2. **Similar query** arrives -- `FT.CACHESEARCH` finds a matching cache entry within the distance threshold. Returns the cached response in sub-millisecond time.
3. **Dissimilar query** arrives -- cache miss, full processing, store new entry.

## Key parameters

- **`threshold`**: Distance threshold for cache hits (lower = stricter matching for L2/COSINE). A threshold of `0.15` means queries within 0.15 cosine distance are considered equivalent.
- **`ttl`**: Time-to-live for cache entries in seconds. Prevents stale answers.

## Next steps

- Try the [RAG Quickstart](../rag-quickstart/) for the full retrieval pipeline
- Try the [GraphRAG](../graphrag/) example for knowledge-graph enrichment
