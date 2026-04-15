#!/usr/bin/env python3
"""Semantic Cache with Moon -- avoid redundant LLM calls via vector similarity.

Demonstrates:
  1. Cache miss: full LLM call, store result with embedding
  2. Cache hit: sub-ms response for semantically similar queries
  3. Timing and cost comparison

Usage:
  python semantic_cache.py
  python semantic_cache.py --host localhost --port 6379
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from typing import List, Optional, Tuple

from moondb import MoonClient, encode_vector


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DIM = 384
INDEX_NAME = "cache_idx"
CACHE_PREFIX = "cache:qa:"
CACHE_THRESHOLD = 0.15  # cosine distance -- lower = stricter match
CACHE_TTL = 3600  # 1 hour

# Simulated LLM cost per call
SIMULATED_LLM_LATENCY_MS = 800
SIMULATED_LLM_COST_USD = 0.01


# ---------------------------------------------------------------------------
# Mock embedding (same deterministic approach as rag_quickstart.py)
# ---------------------------------------------------------------------------

def mock_embedding(text: str) -> List[float]:
    """Deterministic 384-dim embedding from text via SHA-256 expansion."""
    text = text.lower().strip()
    h = hashlib.sha256(text.encode()).digest()
    values = []
    for i in range(DIM):
        byte_val = h[(i * 7) % len(h)] ^ (i & 0xFF)
        values.append((byte_val / 127.5) - 1.0)
    norm = sum(v * v for v in values) ** 0.5
    if norm > 0:
        values = [v / norm for v in values]
    return values


# ---------------------------------------------------------------------------
# Simulated LLM
# ---------------------------------------------------------------------------

# Pre-defined answers for our demo queries
KNOWLEDGE_BASE = {
    "architecture": (
        "Moon uses a thread-per-core, shared-nothing architecture. Each shard "
        "owns its event loop, hash table, and WAL writer. Cross-shard dispatch "
        "uses lock-free SPSC channels with zero global locks."
    ),
    "vector": (
        "Moon's vector search engine uses HNSW graphs with TurboQuant 4-bit "
        "quantization. It supports FT.CREATE, FT.SEARCH, and auto-indexes "
        "vectors on HSET. It achieves 2.56x Qdrant QPS on MiniLM embeddings."
    ),
    "persistence": (
        "Moon uses forkless persistence with per-shard Write-Ahead Logs. "
        "RDB snapshots iterate DashTable segments without fork(). The WAL "
        "advantage over Redis grows with pipeline depth."
    ),
    "default": (
        "Moon is a high-performance Redis-compatible server written in Rust "
        "with vector search, graph engine, and an embedded web console."
    ),
}


def simulated_llm_call(query: str) -> Tuple[str, float]:
    """Simulate an LLM API call with artificial latency.

    Returns (answer, latency_ms).
    """
    # Simulate network + inference latency
    time.sleep(SIMULATED_LLM_LATENCY_MS / 1000)

    query_lower = query.lower()
    if "architect" in query_lower or "design" in query_lower:
        answer = KNOWLEDGE_BASE["architecture"]
    elif "vector" in query_lower or "search" in query_lower:
        answer = KNOWLEDGE_BASE["vector"]
    elif "persist" in query_lower or "wal" in query_lower or "crash" in query_lower:
        answer = KNOWLEDGE_BASE["persistence"]
    else:
        answer = KNOWLEDGE_BASE["default"]

    return answer, SIMULATED_LLM_LATENCY_MS


# ---------------------------------------------------------------------------
# Semantic cache layer
# ---------------------------------------------------------------------------

class SemanticCache:
    """Semantic cache backed by Moon's FT.CACHESEARCH."""

    def __init__(self, client: MoonClient) -> None:
        self.client = client
        self.hits = 0
        self.misses = 0
        self.total_saved_ms = 0.0
        self.total_cost_saved = 0.0

    def setup(self) -> None:
        """Create the cache index."""
        try:
            self.client.vector.drop_index(INDEX_NAME)
        except Exception:
            pass

        self.client.vector.create_index(
            INDEX_NAME,
            prefix=CACHE_PREFIX,
            field_name="vec",
            algorithm="HNSW",
            dim=DIM,
            metric="COSINE",
            m=16,
            ef_construction=200,
            extra_schema=["query", "TEXT", "response", "TEXT"],
        )
        print(f"Cache index '{INDEX_NAME}' created (COSINE, threshold={CACHE_THRESHOLD})")

    def query(self, question: str) -> Tuple[str, bool, float]:
        """Query with semantic cache.

        Returns (answer, cache_hit, latency_ms).
        """
        embedding = mock_embedding(question)
        vec_bytes = encode_vector(embedding)

        # Try cache first
        t0 = time.perf_counter()
        cache_result = self.client.cache.lookup(
            INDEX_NAME,
            CACHE_PREFIX,
            vec_bytes,
            threshold=CACHE_THRESHOLD,
            k=1,
        )
        cache_latency = (time.perf_counter() - t0) * 1000

        if cache_result.cache_hit and cache_result.results:
            # Cache hit
            self.hits += 1
            saved_ms = SIMULATED_LLM_LATENCY_MS - cache_latency
            self.total_saved_ms += saved_ms
            self.total_cost_saved += SIMULATED_LLM_COST_USD
            answer = cache_result.results[0].fields.get("response", "")
            return answer, True, cache_latency

        # Cache miss -- call LLM
        answer, llm_latency = simulated_llm_call(question)
        self.misses += 1

        # Store in cache
        cache_key = f"{CACHE_PREFIX}{hashlib.md5(question.encode()).hexdigest()[:12]}"
        self.client.cache.store(
            cache_key,
            vec_bytes,
            response=answer,
            query=question,
            ttl=CACHE_TTL,
        )

        total_latency = cache_latency + llm_latency
        return answer, False, total_latency

    def report(self) -> None:
        """Print cache performance summary."""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0

        print(f"\n{'='*60}")
        print("  Semantic Cache Performance Report")
        print(f"{'='*60}")
        print(f"  Total queries:     {total}")
        print(f"  Cache hits:        {self.hits}")
        print(f"  Cache misses:      {self.misses}")
        print(f"  Hit rate:          {hit_rate:.0f}%")
        print(f"  Time saved:        {self.total_saved_ms:.0f}ms")
        print(f"  Cost saved:        ${self.total_cost_saved:.2f}")
        print(f"  Cost without cache: ${total * SIMULATED_LLM_COST_USD:.2f}")
        print(f"  Cost with cache:   ${self.misses * SIMULATED_LLM_COST_USD:.2f}")
        print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Demo queries -- pairs of semantically similar questions
# ---------------------------------------------------------------------------

DEMO_QUERIES = [
    # First query: cache miss (new topic)
    ("What is Moon's architecture?", "New topic -- expect cache miss"),
    # Second query: semantically similar -- expect cache hit
    ("How is Moon designed internally?", "Similar question -- expect cache hit"),
    # Third query: different wording, same meaning
    ("Explain the system design of Moon", "Same meaning -- expect cache hit"),

    # New topic: cache miss
    ("How does Moon handle persistence?", "New topic -- expect cache miss"),
    # Similar: cache hit
    ("What is Moon's WAL and crash recovery approach?", "Similar -- expect cache hit"),

    # New topic: cache miss
    ("Tell me about Moon's vector search capabilities", "New topic -- expect cache miss"),
    # Similar: cache hit
    ("What vector search engine does Moon use?", "Similar -- expect cache hit"),

    # Completely different: cache miss
    ("What is the weather today?", "Unrelated -- expect cache miss"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Semantic Cache with Moon")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  Semantic Cache Demo with Moon")
    print(f"{'='*60}\n")

    client = MoonClient(host=args.host, port=args.port, decode_responses=True)
    try:
        client.ping()
    except Exception as e:
        print(f"ERROR: Cannot connect to Moon at {args.host}:{args.port}")
        print(f"  {e}")
        sys.exit(1)
    print(f"Connected to Moon at {args.host}:{args.port}")

    cache = SemanticCache(client)
    cache.setup()

    print(f"\nRunning {len(DEMO_QUERIES)} queries...\n")

    for query, expectation in DEMO_QUERIES:
        answer, hit, latency = cache.query(query)
        status = "HIT" if hit else "MISS"
        print(f"  [{status:4s}] {latency:7.1f}ms | {query}")
        print(f"         Expected: {expectation}")
        if hit:
            print(f"         Answer: {answer[:80]}...")
        print()

    cache.report()


if __name__ == "__main__":
    main()
