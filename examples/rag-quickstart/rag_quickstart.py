#!/usr/bin/env python3
"""RAG Quickstart with Moon -- complete retrieval-augmented generation pipeline.

Demonstrates:
  1. Connect to Moon via moondb SDK
  2. Create a vector index (FT.CREATE)
  3. Store documents with embeddings (HSET, auto-indexed)
  4. Search for relevant context (FT.SEARCH KNN)
  5. Format results for LLM context
  6. (Optional) Generate an answer with OpenAI

Usage:
  python rag_quickstart.py                          # mock embeddings, no LLM
  python rag_quickstart.py --use-real-embeddings     # sentence-transformers
  python rag_quickstart.py --use-real-embeddings --generate-answer  # + OpenAI
"""

from __future__ import annotations

import argparse
import hashlib
import struct
import sys
import time
from typing import List

from moondb import MoonClient, SearchResult, encode_vector


# ---------------------------------------------------------------------------
# Sample documents -- 10 text chunks about Moon's architecture
# ---------------------------------------------------------------------------

DOCUMENTS = [
    {
        "id": "doc:1",
        "title": "Thread-per-core architecture",
        "text": (
            "Moon uses a thread-per-core, shared-nothing architecture. Each shard "
            "owns its own event loop, hash table, WAL writer, and Pub/Sub registry. "
            "There are no global locks; cross-shard dispatch uses lock-free SPSC channels."
        ),
        "category": "architecture",
    },
    {
        "id": "doc:2",
        "title": "Dual runtime support",
        "text": (
            "Moon supports two async runtimes: monoio (io_uring on Linux) for peak "
            "throughput, and tokio for portability and CI. Both compile from the same "
            "codebase via feature gates."
        ),
        "category": "architecture",
    },
    {
        "id": "doc:3",
        "title": "Vector search engine",
        "text": (
            "Moon includes an in-process vector search engine using HNSW graphs with "
            "TurboQuant 4-bit quantization. It supports FT.CREATE, FT.SEARCH, "
            "FT.COMPACT, and auto-indexes vectors on HSET."
        ),
        "category": "vector",
    },
    {
        "id": "doc:4",
        "title": "HNSW index parameters",
        "text": (
            "The HNSW index supports tunable parameters: M (max edges per node, "
            "default 16), EF_CONSTRUCTION (build-time search width, default 200), "
            "and EF_RUNTIME (query-time search width). Higher values improve recall "
            "at the cost of latency."
        ),
        "category": "vector",
    },
    {
        "id": "doc:5",
        "title": "Persistence and WAL",
        "text": (
            "Moon uses a forkless persistence model. RDB snapshots iterate DashTable "
            "segments incrementally without fork(). The per-shard Write-Ahead Log "
            "(WAL) batches fsync for high write throughput."
        ),
        "category": "persistence",
    },
    {
        "id": "doc:6",
        "title": "Memory optimization",
        "text": (
            "Moon achieves 27-35% less RSS than Redis at 1KB+ values through "
            "CompactKey (23-byte SSO), CompactValue (16-byte SSO with inline TTL), "
            "HeapString, B+ tree sorted sets, and per-request bumpalo arenas."
        ),
        "category": "memory",
    },
    {
        "id": "doc:7",
        "title": "Property graph engine",
        "text": (
            "Moon's graph engine supports 14 GRAPH.* commands with a Cypher subset. "
            "It uses CSR (Compressed Sparse Row) segments for cache-friendly traversal "
            "and SIMD-accelerated distance computation for hybrid graph+vector queries."
        ),
        "category": "graph",
    },
    {
        "id": "doc:8",
        "title": "Semantic cache",
        "text": (
            "Moon supports semantic caching via FT.CACHESEARCH, which checks for "
            "vector-similar cache entries before falling back to a full KNN search. "
            "This avoids redundant LLM API calls for semantically equivalent queries."
        ),
        "category": "cache",
    },
    {
        "id": "doc:9",
        "title": "Session-aware search",
        "text": (
            "FT.SEARCH supports a SESSION clause that tracks previously returned "
            "results per session key. Subsequent searches automatically deduplicate "
            "against the session history, ideal for paginated or conversational UIs."
        ),
        "category": "search",
    },
    {
        "id": "doc:10",
        "title": "Web console",
        "text": (
            "Moon ships an embedded web console with 7 views: Dashboard, Browser, "
            "Console (with Monaco editor and 233-command autocomplete), Vectors "
            "(3D UMAP projection), Graph (force-directed layout), Memory (treemap), "
            "and Help. Served at /ui/ from the binary itself."
        ),
        "category": "console",
    },
]


# ---------------------------------------------------------------------------
# Embedding helpers
# ---------------------------------------------------------------------------

DIM = 384  # MiniLM-L6 dimension


def mock_embedding(text: str) -> List[float]:
    """Generate a deterministic 384-dim embedding from text via SHA-256 expansion.

    This is NOT a real embedding model -- it produces consistent vectors from
    the same input text, so search results are reproducible without installing
    sentence-transformers. For production use, replace with a real model.
    """
    h = hashlib.sha256(text.encode()).digest()
    # Expand hash to fill DIM floats deterministically
    values = []
    for i in range(DIM):
        # Use hash bytes cyclically, mixed with index
        byte_val = h[(i * 7) % len(h)] ^ (i & 0xFF)
        # Map to [-1, 1] range
        values.append((byte_val / 127.5) - 1.0)
    # L2-normalize for cosine similarity
    norm = sum(v * v for v in values) ** 0.5
    if norm > 0:
        values = [v / norm for v in values]
    return values


def load_embedding_model():
    """Load sentence-transformers model (requires pip install sentence-transformers)."""
    try:
        from sentence_transformers import SentenceTransformer
        print("Loading MiniLM-L6-v2 model...")
        model = SentenceTransformer("all-MiniLM-L6-v2")
        print("Model loaded.")
        return model
    except ImportError:
        print("ERROR: sentence-transformers not installed.")
        print("  pip install sentence-transformers")
        sys.exit(1)


def embed_texts(texts: List[str], model=None) -> List[List[float]]:
    """Embed a list of texts using model or mock."""
    if model is not None:
        vectors = model.encode(texts, normalize_embeddings=True)
        return [v.tolist() for v in vectors]
    return [mock_embedding(t) for t in texts]


# ---------------------------------------------------------------------------
# RAG pipeline
# ---------------------------------------------------------------------------

def create_index(client: MoonClient, index_name: str) -> None:
    """Create a vector search index for our documents."""
    try:
        client.vector.drop_index(index_name)
    except Exception:
        pass  # Index may not exist yet

    client.vector.create_index(
        index_name,
        prefix="doc:",
        field_name="vec",
        algorithm="HNSW",
        dim=DIM,
        metric="COSINE",
        m=16,
        ef_construction=200,
        extra_schema=["title", "TEXT", "text", "TEXT", "category", "TAG"],
    )
    print(f"Created index '{index_name}' (HNSW, {DIM}d, COSINE)")


def store_documents(
    client: MoonClient,
    documents: list,
    model=None,
) -> None:
    """Store documents with their vector embeddings."""
    texts = [d["text"] for d in documents]
    vectors = embed_texts(texts, model)

    for doc, vec in zip(documents, vectors):
        vec_bytes = encode_vector(vec)
        client.hset(
            doc["id"],
            mapping={
                "title": doc["title"],
                "text": doc["text"],
                "category": doc["category"],
                "vec": vec_bytes,
            },
        )
    print(f"Stored {len(documents)} documents with embeddings")


def search_documents(
    client: MoonClient,
    index_name: str,
    query: str,
    k: int = 3,
    model=None,
) -> List[SearchResult]:
    """Search for documents relevant to the query."""
    query_vec = embed_texts([query], model)[0]

    results = client.vector.search(
        index_name,
        query_vec,
        k=k,
        field_name="vec",
        return_fields=["title", "text", "category"],
    )
    return results


def format_context(results: List[SearchResult]) -> str:
    """Format search results into an LLM context block."""
    context_parts = []
    for i, r in enumerate(results, 1):
        title = r.fields.get("title", "Untitled")
        text = r.fields.get("text", "")
        score = r.score
        context_parts.append(
            f"[Source {i}] {title} (relevance: {1 - score:.3f})\n{text}"
        )
    return "\n\n".join(context_parts)


def build_prompt(query: str, context: str) -> str:
    """Build a RAG prompt with retrieved context."""
    return (
        "You are a helpful assistant answering questions about the Moon "
        "database server. Use ONLY the provided context to answer. If the "
        "context does not contain the answer, say so.\n\n"
        f"## Context\n\n{context}\n\n"
        f"## Question\n\n{query}\n\n"
        "## Answer\n\n"
    )


def generate_answer(prompt: str) -> str:
    """Call OpenAI API to generate an answer (requires OPENAI_API_KEY)."""
    import os
    try:
        from openai import OpenAI
    except ImportError:
        print("ERROR: openai not installed. pip install openai")
        sys.exit(1)

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: Set OPENAI_API_KEY environment variable")
        sys.exit(1)

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=500,
    )
    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="RAG Quickstart with Moon")
    parser.add_argument("--host", default="localhost", help="Moon server host")
    parser.add_argument("--port", type=int, default=6379, help="Moon server port")
    parser.add_argument(
        "--use-real-embeddings",
        action="store_true",
        help="Use sentence-transformers instead of mock embeddings",
    )
    parser.add_argument(
        "--generate-answer",
        action="store_true",
        help="Call OpenAI API to generate final answer",
    )
    args = parser.parse_args()

    # --- Step 1: Connect ---
    print(f"\n{'='*60}")
    print("  RAG Quickstart with Moon")
    print(f"{'='*60}\n")

    client = MoonClient(host=args.host, port=args.port, decode_responses=True)
    try:
        client.ping()
    except Exception as e:
        print(f"ERROR: Cannot connect to Moon at {args.host}:{args.port}")
        print(f"  {e}")
        print(f"\nMake sure Moon is running:")
        print(f"  ./target/release/moon --port {args.port}")
        sys.exit(1)
    print(f"Connected to Moon at {args.host}:{args.port}")

    # --- Step 2: Load embedding model (optional) ---
    model = None
    if args.use_real_embeddings:
        model = load_embedding_model()
    else:
        print("Using mock embeddings (--use-real-embeddings for sentence-transformers)")

    # --- Step 3: Create index ---
    index_name = "rag_docs"
    create_index(client, index_name)

    # --- Step 4: Store documents ---
    store_documents(client, DOCUMENTS, model)

    # --- Step 5: Search ---
    queries = [
        "How does Moon handle persistence and crash recovery?",
        "What vector search algorithms does Moon support?",
        "How does Moon compare to Redis in memory usage?",
    ]

    for query in queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"{'='*60}")

        t0 = time.perf_counter()
        results = search_documents(client, index_name, query, k=3, model=model)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        print(f"\nSearch returned {len(results)} results in {elapsed_ms:.1f}ms:\n")
        for i, r in enumerate(results, 1):
            title = r.fields.get("title", "?")
            score = r.score
            print(f"  {i}. [{title}] (distance: {score:.4f})")

        # --- Step 6: Format context ---
        context = format_context(results)
        prompt = build_prompt(query, context)

        print(f"\n--- LLM Prompt ---")
        print(prompt[:500])
        if len(prompt) > 500:
            print(f"  ... ({len(prompt)} chars total)")

        # --- Step 7: Generate answer (optional) ---
        if args.generate_answer:
            print(f"\n--- LLM Answer ---")
            answer = generate_answer(prompt)
            print(answer)

    # --- Cleanup ---
    print(f"\n{'='*60}")
    print("Done! Explore the data in Moon Console: http://localhost:9100/ui/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
