#!/usr/bin/env python3
"""GraphRAG with Moon -- knowledge graph + vector search for richer retrieval.

Demonstrates:
  1. Create a knowledge graph (GRAPH.CREATE, GRAPH.ADDNODE, GRAPH.ADDEDGE)
  2. Store document vectors (HSET + FT.CREATE)
  3. Link graph nodes to documents via key properties
  4. FT.SEARCH with EXPAND GRAPH for graph-enriched results
  5. FT.NAVIGATE for multi-hop retrieval

Usage:
  python graphrag.py
  python graphrag.py --host localhost --port 6379
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from typing import Dict, List, Tuple

from moondb import MoonClient, encode_vector


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DIM = 384
INDEX_NAME = "knowledge_idx"
GRAPH_NAME = "knowledge"
DOC_PREFIX = "doc:"


# ---------------------------------------------------------------------------
# Mock embedding
# ---------------------------------------------------------------------------

def mock_embedding(text: str) -> List[float]:
    """Deterministic 384-dim embedding from text."""
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
# Knowledge graph data
# ---------------------------------------------------------------------------

# People and their documents
PEOPLE = [
    {
        "name": "Alice",
        "role": "Lead Engineer",
        "doc_key": "doc:alice",
        "text": (
            "Alice is the lead engineer on the Moon project. She designed the "
            "thread-per-core architecture and the shared-nothing shard model. "
            "She has 15 years of systems programming experience in Rust and C++."
        ),
    },
    {
        "name": "Bob",
        "role": "Vector Search Engineer",
        "doc_key": "doc:bob",
        "text": (
            "Bob specializes in vector search and information retrieval. He "
            "implemented the HNSW graph index and TurboQuant 4-bit quantization "
            "in Moon's vector engine. He previously worked on Elasticsearch."
        ),
    },
    {
        "name": "Charlie",
        "role": "Graph Engine Engineer",
        "doc_key": "doc:charlie",
        "text": (
            "Charlie built Moon's property graph engine with Cypher subset "
            "support. He implemented CSR segments for cache-friendly traversal "
            "and SIMD-accelerated cosine distance for hybrid graph+vector queries."
        ),
    },
]

# Projects and their documents
PROJECTS = [
    {
        "name": "Moon Project",
        "doc_key": "doc:moon_project",
        "text": (
            "Moon is a high-performance Redis-compatible server written in Rust. "
            "It features a thread-per-core architecture, dual runtime support "
            "(monoio + tokio), forkless persistence, and 27-35% less memory "
            "than Redis at 1KB+ values."
        ),
    },
    {
        "name": "Vector Engine",
        "doc_key": "doc:vector_engine",
        "text": (
            "Moon's vector search engine supports FT.CREATE, FT.SEARCH, "
            "FT.COMPACT, and auto-indexing on HSET. It uses HNSW graphs with "
            "TurboQuant quantization, achieving 2.56x Qdrant QPS on MiniLM "
            "embeddings with 96.7% recall."
        ),
    },
    {
        "name": "Graph Engine",
        "doc_key": "doc:graph_engine",
        "text": (
            "Moon's graph engine provides 14 GRAPH.* commands with a Cypher "
            "subset. It supports MATCH, WHERE, RETURN, CREATE, DELETE, SET, "
            "and MERGE. Hybrid graph+vector queries combine traversal with "
            "vector similarity for GraphRAG workloads."
        ),
    },
]

# Technologies
TECHNOLOGIES = [
    {
        "name": "Rust",
        "doc_key": "doc:rust",
        "text": (
            "Rust is a systems programming language focused on safety, speed, "
            "and concurrency. Moon is written entirely in Rust, using its "
            "ownership model for memory safety without garbage collection."
        ),
    },
    {
        "name": "HNSW",
        "doc_key": "doc:hnsw",
        "text": (
            "Hierarchical Navigable Small World (HNSW) is a graph-based "
            "algorithm for approximate nearest neighbor search. Moon uses HNSW "
            "as its primary vector index structure, with tunable M, "
            "EF_CONSTRUCTION, and EF_RUNTIME parameters."
        ),
    },
    {
        "name": "Cypher",
        "doc_key": "doc:cypher",
        "text": (
            "Cypher is a declarative graph query language originally developed "
            "for Neo4j. Moon implements a Cypher subset supporting MATCH, "
            "WHERE, RETURN, CREATE, DELETE, SET, MERGE, and ORDER BY clauses."
        ),
    },
]

# Relationships: (source_name, target_name, edge_type)
EDGES = [
    ("Alice", "Moon Project", "WORKS_ON"),
    ("Bob", "Vector Engine", "WORKS_ON"),
    ("Charlie", "Graph Engine", "WORKS_ON"),
    ("Alice", "Bob", "KNOWS"),
    ("Bob", "Charlie", "KNOWS"),
    ("Alice", "Charlie", "KNOWS"),
    ("Moon Project", "Rust", "USES"),
    ("Moon Project", "Vector Engine", "RELATES_TO"),
    ("Moon Project", "Graph Engine", "RELATES_TO"),
    ("Vector Engine", "HNSW", "RELATES_TO"),
    ("Graph Engine", "Cypher", "RELATES_TO"),
]


# ---------------------------------------------------------------------------
# Setup functions
# ---------------------------------------------------------------------------

def setup_index(client: MoonClient) -> None:
    """Create the vector search index."""
    try:
        client.vector.drop_index(INDEX_NAME)
    except Exception:
        pass

    client.vector.create_index(
        INDEX_NAME,
        prefix=DOC_PREFIX,
        field_name="vec",
        algorithm="HNSW",
        dim=DIM,
        metric="COSINE",
        m=16,
        ef_construction=200,
        extra_schema=["title", "TEXT", "text", "TEXT", "entity_type", "TAG"],
    )
    print(f"Created vector index '{INDEX_NAME}'")


def setup_graph(client: MoonClient) -> None:
    """Create the knowledge graph."""
    try:
        client.graph.delete(GRAPH_NAME)
    except Exception:
        pass

    client.graph.create(GRAPH_NAME)
    print(f"Created graph '{GRAPH_NAME}'")


def store_documents(client: MoonClient) -> None:
    """Store all documents with embeddings."""
    all_entities = (
        [(p, "person") for p in PEOPLE]
        + [(p, "project") for p in PROJECTS]
        + [(t, "technology") for t in TECHNOLOGIES]
    )

    for entity, entity_type in all_entities:
        vec = mock_embedding(entity["text"])
        client.hset(
            entity["doc_key"],
            mapping={
                "title": entity["name"],
                "text": entity["text"],
                "entity_type": entity_type,
                "vec": encode_vector(vec),
            },
        )
    print(f"Stored {len(all_entities)} documents with embeddings")


def build_graph(client: MoonClient) -> Dict[str, int]:
    """Build the knowledge graph and return name-to-node-id mapping."""
    all_entities = PEOPLE + PROJECTS + TECHNOLOGIES
    name_to_id: Dict[str, int] = {}

    for entity in all_entities:
        entity_type = "Person" if entity in PEOPLE else (
            "Project" if entity in PROJECTS else "Technology"
        )
        node_id = client.graph.add_node(
            GRAPH_NAME,
            entity_type,
            name=entity["name"],
            doc_key=entity.get("doc_key", ""),
        )
        name_to_id[entity["name"]] = node_id

    print(f"Added {len(name_to_id)} nodes to graph")

    edge_count = 0
    for src_name, dst_name, edge_type in EDGES:
        src_id = name_to_id.get(src_name)
        dst_id = name_to_id.get(dst_name)
        if src_id is not None and dst_id is not None:
            client.graph.add_edge(GRAPH_NAME, src_id, dst_id, edge_type)
            edge_count += 1

    print(f"Added {edge_count} edges to graph")
    return name_to_id


# ---------------------------------------------------------------------------
# Demo searches
# ---------------------------------------------------------------------------

def demo_vector_search(client: MoonClient, query: str, k: int = 3) -> None:
    """Standard vector search (no graph expansion)."""
    vec = mock_embedding(query)

    t0 = time.perf_counter()
    results = client.vector.search(
        INDEX_NAME, vec, k=k, field_name="vec",
        return_fields=["title", "text", "entity_type"],
    )
    elapsed = (time.perf_counter() - t0) * 1000

    print(f"\n  Vector Search ({elapsed:.1f}ms): {len(results)} results")
    for i, r in enumerate(results, 1):
        title = r.fields.get("title", "?")
        etype = r.fields.get("entity_type", "?")
        print(f"    {i}. [{etype}] {title} (distance: {r.score:.4f})")


def demo_graph_expanded_search(
    client: MoonClient, query: str, k: int = 3, depth: int = 2
) -> None:
    """Vector search with graph expansion -- pulls in related documents."""
    vec = mock_embedding(query)

    t0 = time.perf_counter()
    results = client.vector.search(
        INDEX_NAME, vec, k=k, field_name="vec",
        return_fields=["title", "text", "entity_type"],
        expand_graph=GRAPH_NAME,
        expand_depth=depth,
    )
    elapsed = (time.perf_counter() - t0) * 1000

    print(f"\n  Graph-Expanded Search ({elapsed:.1f}ms): {len(results)} results")
    for i, r in enumerate(results, 1):
        title = r.fields.get("title", "?")
        etype = r.fields.get("entity_type", "?")
        hops = r.graph_hops
        hop_str = f", {hops} hops" if hops is not None else ""
        print(f"    {i}. [{etype}] {title} (distance: {r.score:.4f}{hop_str})")


def demo_navigate(
    client: MoonClient, query: str, k: int = 5, hops: int = 3
) -> None:
    """Multi-hop navigation -- vector search guided by graph topology."""
    vec = mock_embedding(query)

    t0 = time.perf_counter()
    results = client.vector.navigate(
        INDEX_NAME, vec, k=k, field_name="vec",
        hops=hops, hop_penalty=0.05,
    )
    elapsed = (time.perf_counter() - t0) * 1000

    print(f"\n  FT.NAVIGATE ({elapsed:.1f}ms): {len(results)} results")
    for i, r in enumerate(results, 1):
        title = r.fields.get("title", "?")
        etype = r.fields.get("entity_type", "?")
        hops_val = r.graph_hops
        hop_str = f", {hops_val} hops" if hops_val is not None else ""
        print(f"    {i}. [{etype}] {title} (score: {r.score:.4f}{hop_str})")


def demo_graph_query(client: MoonClient) -> None:
    """Direct Cypher query on the knowledge graph."""
    print(f"\n  Cypher Query: MATCH (p:Person)-[:WORKS_ON]->(proj) RETURN p.name, proj.name")
    result = client.graph.ro_query(
        GRAPH_NAME,
        "MATCH (p:Person)-[:WORKS_ON]->(proj) RETURN p.name, proj.name",
    )
    for row in result.rows:
        print(f"    {row[0]} -> {row[1]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="GraphRAG with Moon")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  GraphRAG Demo with Moon")
    print(f"{'='*60}\n")

    client = MoonClient(host=args.host, port=args.port, decode_responses=True)
    try:
        client.ping()
    except Exception as e:
        print(f"ERROR: Cannot connect to Moon at {args.host}:{args.port}")
        print(f"  {e}")
        sys.exit(1)
    print(f"Connected to Moon at {args.host}:{args.port}")

    # --- Setup ---
    print(f"\n--- Setup ---")
    setup_index(client)
    setup_graph(client)
    store_documents(client)
    name_to_id = build_graph(client)

    # --- Demo 1: Cypher query ---
    print(f"\n{'='*60}")
    print("Demo 1: Direct graph query (Cypher)")
    print(f"{'='*60}")
    demo_graph_query(client)

    # --- Demo 2: Compare vector search vs graph-expanded search ---
    queries = [
        "Who works on the Moon project and what technologies do they use?",
        "Tell me about HNSW and vector search implementation",
        "What is the relationship between Alice and the graph engine?",
    ]

    for query in queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"{'='*60}")

        demo_vector_search(client, query)
        demo_graph_expanded_search(client, query)
        demo_navigate(client, query)

    # --- Summary ---
    print(f"\n{'='*60}")
    print("  GraphRAG Summary")
    print(f"{'='*60}")
    print()
    print("  Vector search finds documents by content similarity alone.")
    print("  Graph-expanded search pulls in related documents via graph edges,")
    print("  surfacing context that pure vector search would miss.")
    print()
    print("  FT.NAVIGATE combines both approaches with hop-depth penalties,")
    print("  balancing direct relevance with graph-discovered connections.")
    print()
    print("  Explore the graph in Moon Console: http://localhost:9100/ui/")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
