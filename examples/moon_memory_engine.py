#!/usr/bin/env python3
"""
Moon as a Memory Engine — RAG + GraphRAG + Semantic Cache

Demonstrates Moon's Knowledge Navigation Engine: vector search, graph traversal,
session-aware dedup, and semantic caching — all in one process, one RTT.

Prerequisites:
    pip install redis sentence-transformers
    # Start Moon:  ./moon --port 6399 --shards 1

Usage:
    python moon_memory_engine.py
"""

import struct
import time
import redis

# ── Configuration ─────────────────────────────────────────────────────────────

MOON_HOST = "localhost"
MOON_PORT = 6399
DIM = 4  # Use 4-dim for demo (real: 384/768 from sentence-transformers)
INDEX = "knowledge"
GRAPH = "kg"
CACHE_PREFIX = "cache:sem:"

# ── Helpers ───────────────────────────────────────────────────────────────────

def vec(*vals: float) -> bytes:
    """Pack floats into little-endian binary blob for Moon vector fields."""
    return struct.pack(f"<{len(vals)}f", *vals)

def knn_query(field: str, k: int, param: str = "q") -> str:
    """Build RediSearch-compatible KNN query string."""
    return f"*=>[KNN {k} @{field} ${param}]"

def filter_knn(filter_expr: str, field: str, k: int, param: str = "q") -> str:
    """Build filtered KNN query: @field:{value}=>[KNN k @vec $q]"""
    return f"{filter_expr}=>[KNN {k} @{field} ${param}]"

def print_results(label: str, result: list):
    """Pretty-print FT.SEARCH / FT.RECOMMEND results."""
    count = int(result[0])
    print(f"\n  {label}: {count} result(s)")
    i = 1
    while i < len(result):
        key = result[i].decode() if isinstance(result[i], bytes) else result[i]
        i += 1
        fields = {}
        if i < len(result) and isinstance(result[i], list):
            pairs = result[i]
            for j in range(0, len(pairs), 2):
                k = pairs[j].decode() if isinstance(pairs[j], bytes) else pairs[j]
                v = pairs[j+1].decode() if isinstance(pairs[j+1], bytes) else pairs[j+1]
                fields[k] = v
            i += 1
        elif i < len(result) and isinstance(result[i], bytes):
            # Flat key-value pairs
            while i < len(result) and isinstance(result[i], bytes):
                k_name = result[i].decode()
                if k_name.startswith("doc:") or k_name.startswith("cache:"):
                    break
                v_val = result[i+1].decode() if i+1 < len(result) and isinstance(result[i+1], bytes) else result[i+1]
                fields[k_name] = v_val
                i += 2
        score = fields.get("__vec_score", "?")
        title = fields.get("title", "")
        hops = fields.get("__graph_hops", "")
        hop_str = f" (hops={hops})" if hops else ""
        print(f"    {key}  score={score}  {title}{hop_str}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    r = redis.Redis(host=MOON_HOST, port=MOON_PORT, decode_responses=False)
    r.ping()
    print("Connected to Moon\n")

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 1: Create Vector Index                     ║
    # ╚══════════════════════════════════════════════════╝
    print("=" * 60)
    print("Step 1: Create vector index")
    print("=" * 60)

    try:
        r.execute_command("FT.DROPINDEX", INDEX)
    except Exception:
        pass

    r.execute_command(
        "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
        "vec", "VECTOR", "HNSW", "6", "DIM", str(DIM), "TYPE", "FLOAT32",
        "DISTANCE_METRIC", "COSINE",
    )
    print("  Created index 'knowledge' (4-dim cosine)")

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 2: Ingest Knowledge Documents              ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 2: Ingest knowledge documents")
    print("=" * 60)

    docs = [
        ("doc:ml_basics",     vec(0.9, 0.1, 0.0, 0.0), "Machine learning fundamentals",     "ml",      "science"),
        ("doc:neural_nets",   vec(0.8, 0.2, 0.1, 0.0), "Neural network architectures",      "ml",      "science"),
        ("doc:transformers",  vec(0.7, 0.3, 0.2, 0.0), "Transformer models and attention",   "ml",      "science"),
        ("doc:rag_overview",  vec(0.6, 0.2, 0.3, 0.1), "RAG retrieval-augmented generation", "rag",     "engineering"),
        ("doc:vector_db",     vec(0.5, 0.1, 0.4, 0.2), "Vector database internals",          "infra",   "engineering"),
        ("doc:graph_db",      vec(0.1, 0.1, 0.8, 0.1), "Graph database design patterns",     "infra",   "engineering"),
        ("doc:kg_construct",  vec(0.3, 0.2, 0.6, 0.1), "Knowledge graph construction",       "kg",      "science"),
        ("doc:llm_agents",    vec(0.4, 0.3, 0.2, 0.3), "LLM-based autonomous agents",        "agents",  "engineering"),
        ("doc:prompt_eng",    vec(0.5, 0.4, 0.1, 0.2), "Prompt engineering best practices",   "prompts", "engineering"),
        ("doc:fine_tuning",   vec(0.8, 0.1, 0.0, 0.2), "Fine-tuning language models",         "ml",      "science"),
    ]

    for key, vector, title, topic, category in docs:
        r.hset(key, mapping={
            "vec": vector, "title": title, "topic": topic, "category": category,
        })
    time.sleep(1)  # Let auto-indexing settle

    info = r.execute_command("FT.INFO", INDEX)
    for i, v in enumerate(info):
        if v == b"num_docs":
            print(f"  Indexed {int(info[i+1])} documents")
            break

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 3: Build Knowledge Graph                   ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 3: Build knowledge graph (GraphRAG)")
    print("=" * 60)

    try:
        r.execute_command("GRAPH.CREATE", GRAPH)
    except Exception:
        pass

    # Add nodes with _key property for vector↔graph bridge
    nodes = {}
    for key, _, title, _, _ in docs:
        nid = int(r.execute_command("GRAPH.ADDNODE", GRAPH, "document", "_key", key, "title", title))
        nodes[key] = nid

    # Build edges — knowledge relationships
    edges = [
        ("doc:ml_basics",    "doc:neural_nets",   "prerequisite"),
        ("doc:neural_nets",  "doc:transformers",   "prerequisite"),
        ("doc:transformers", "doc:rag_overview",   "enables"),
        ("doc:rag_overview", "doc:vector_db",      "uses"),
        ("doc:rag_overview", "doc:kg_construct",   "uses"),
        ("doc:kg_construct", "doc:graph_db",       "uses"),
        ("doc:transformers", "doc:llm_agents",     "enables"),
        ("doc:llm_agents",   "doc:prompt_eng",     "requires"),
        ("doc:ml_basics",    "doc:fine_tuning",    "technique"),
        ("doc:fine_tuning",  "doc:transformers",   "applies_to"),
    ]
    for src, dst, rel in edges:
        r.execute_command("GRAPH.ADDEDGE", GRAPH, str(nodes[src]), str(nodes[dst]), rel)

    print(f"  Created {len(nodes)} nodes, {len(edges)} edges")

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 4: RAG — Vector Search                     ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 4: RAG — Vector similarity search")
    print("=" * 60)

    # User asks: "How do neural networks work?"
    user_query = vec(0.85, 0.15, 0.05, 0.0)  # Close to ML docs

    result = r.execute_command(
        "FT.SEARCH", INDEX, knn_query("vec", 3),
        "PARAMS", "2", "q", user_query,
    )
    print_results("RAG search: 'How do neural networks work?'", result)

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 5: Filtered RAG — Topic-scoped search      ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 5: Filtered RAG — search only 'engineering' docs")
    print("=" * 60)

    result = r.execute_command(
        "FT.SEARCH", INDEX, filter_knn("@category:{engineering}", "vec", 3),
        "PARAMS", "2", "q", user_query,
    )
    print_results("Filtered: category=engineering", result)

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 6: GraphRAG — Expand via knowledge graph   ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 6: GraphRAG — vector search + graph expansion")
    print("=" * 60)

    # KNN finds nearest docs, then EXPAND GRAPH walks relationships
    result = r.execute_command(
        "FT.SEARCH", INDEX, knn_query("vec", 2),
        "PARAMS", "2", "q", user_query,
        "EXPAND", "GRAPH", GRAPH, "DEPTH", "2",
    )
    print_results("GraphRAG: KNN→graph expand depth=2", result)

    # Standalone graph expansion from a known document
    try:
        expand_result = r.execute_command(
            "FT.EXPAND", INDEX, "doc:transformers", "DEPTH", "2", "GRAPH", GRAPH,
        )
        if isinstance(expand_result, list):
            c = int(expand_result[0])
            keys = [expand_result[i].decode() for i in range(1, len(expand_result), 2) if isinstance(expand_result[i], bytes)]
            print(f"\n  FT.EXPAND from doc:transformers depth=2: {c} reachable nodes")
            for k in keys:
                print(f"    → {k}")
    except Exception as e:
        print(f"  FT.EXPAND: {e}")

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 7: FT.RECOMMEND — "More like this"         ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 7: FT.RECOMMEND — 'more like doc:transformers'")
    print("=" * 60)

    result = r.execute_command(
        "FT.RECOMMEND", INDEX,
        "POSITIVE", "doc:transformers",
        "NEGATIVE", "doc:graph_db",  # Push away from infra topics
        "K", "3",
    )
    print_results("Recommend (like transformers, unlike graph_db)", result)

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 8: Session-Aware Retrieval (Conversation)  ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 8: Session-aware retrieval (multi-turn conversation)")
    print("=" * 60)

    session_key = "session:user42:conv1"

    # Turn 1: User asks about ML
    result = r.execute_command(
        "FT.SEARCH", INDEX, knn_query("vec", 3),
        "PARAMS", "2", "q", user_query,
        "SESSION", session_key,
    )
    c1 = int(result[0])
    keys1 = [result[i].decode() for i in range(1, len(result), 2) if isinstance(result[i], bytes)]
    print(f"\n  Turn 1: {c1} results — {keys1}")

    # Turn 2: Same query — previously returned docs are filtered out
    result = r.execute_command(
        "FT.SEARCH", INDEX, knn_query("vec", 3),
        "PARAMS", "2", "q", user_query,
        "SESSION", session_key,
    )
    c2 = int(result[0])
    keys2 = [result[i].decode() for i in range(1, len(result), 2) if isinstance(result[i], bytes)]
    print(f"  Turn 2: {c2} results — {keys2}  (deduped!)")

    # Turn 3: Broaden search to see remaining docs
    result = r.execute_command(
        "FT.SEARCH", INDEX, knn_query("vec", 5),
        "PARAMS", "2", "q", user_query,
        "SESSION", session_key,
    )
    c3 = int(result[0])
    keys3 = [result[i].decode() for i in range(1, len(result), 2) if isinstance(result[i], bytes)]
    print(f"  Turn 3: {c3} results — {keys3}  (remaining unseen)")

    # Show session state
    stype = r.type(session_key)
    if stype == b"zset":
        members = r.zrange(session_key, 0, -1)
        print(f"  Session tracks {len(members)} seen docs: {[m.decode() for m in members]}")

    # Session supports TTL for conversation timeout
    r.expire(session_key, 3600)
    print(f"  Session TTL set to 1 hour")

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 9: Semantic Cache                          ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 9: Semantic cache — cache LLM responses by query similarity")
    print("=" * 60)

    # Simulate: store a cached LLM response for a previous query
    cached_query_vec = vec(0.85, 0.15, 0.05, 0.0)  # Same as user_query
    r.hset(f"{CACHE_PREFIX}q1", mapping={
        "vec": cached_query_vec,
        "response": "Neural networks are computing systems inspired by biological neurons...",
        "model": "gpt-4",
        "tokens": "150",
    })
    r.expire(f"{CACHE_PREFIX}q1", 3600)  # Cache entry expires in 1 hour

    # FT.CACHESEARCH: check cache first, fallback to KNN
    try:
        cache_result = r.execute_command(
            "FT.CACHESEARCH", INDEX, CACHE_PREFIX,
            knn_query("vec", 3),
            "PARAMS", "2", "q", user_query,
            "THRESHOLD", "0.95",
            "FALLBACK", "KNN", "3",
        )
        print(f"  FT.CACHESEARCH result: {cache_result}")
    except Exception as e:
        print(f"  FT.CACHESEARCH: {e}")

    # Manual cache lookup pattern (always works)
    print("\n  Manual cache pattern:")
    result = r.execute_command(
        "FT.SEARCH", INDEX, knn_query("vec", 1),
        "PARAMS", "2", "q", user_query,
    )
    if int(result[0]) > 0:
        best_key = result[1].decode()
        # Check if it's a cache entry
        if best_key.startswith(CACHE_PREFIX):
            cached = r.hgetall(best_key)
            print(f"  CACHE HIT: {cached.get(b'response', b'').decode()[:80]}...")
        else:
            print(f"  CACHE MISS — nearest doc: {best_key} (run LLM, then cache response)")

    # ╔══════════════════════════════════════════════════╗
    # ║  Step 10: FT.NAVIGATE — Multi-hop Knowledge Nav  ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Step 10: FT.NAVIGATE — multi-hop knowledge navigation")
    print("=" * 60)

    try:
        nav_result = r.execute_command(
            "FT.NAVIGATE", INDEX, knn_query("vec", 2),
            "HOPS", "2",
            "PARAMS", "2", "q", user_query,
        )
        print_results("NAVIGATE: KNN→graph→re-rank (2 hops)", nav_result)
    except Exception as e:
        print(f"  FT.NAVIGATE: {e}")

    # ╔══════════════════════════════════════════════════╗
    # ║  Summary                                         ║
    # ╚══════════════════════════════════════════════════╝
    print("\n" + "=" * 60)
    print("Moon Memory Engine — Feature Summary")
    print("=" * 60)
    print("""
  ┌─────────────────────────────────────────────────────┐
  │ Feature              │ Command                      │
  ├──────────────────────┼──────────────────────────────┤
  │ Vector search (RAG)  │ FT.SEARCH ... KNN            │
  │ Filtered search      │ FT.SEARCH @field:{val}=>KNN  │
  │ GraphRAG expansion   │ FT.SEARCH ... EXPAND GRAPH   │
  │ Graph traversal      │ FT.EXPAND                    │
  │ Recommendation       │ FT.RECOMMEND POSITIVE/NEG    │
  │ Session dedup        │ FT.SEARCH ... SESSION key    │
  │ Semantic cache       │ FT.CACHESEARCH               │
  │ Multi-hop navigation │ FT.NAVIGATE ... HOPS N       │
  │ Range search         │ FT.SEARCH ... RANGE dist     │
  │ Autocompact control  │ FT.CONFIG SET AUTOCOMPACT    │
  └──────────────────────┴──────────────────────────────┘

  One server. One connection. One RTT per operation.
  No Pinecone. No Neo4j. No Redis Stack. Just Moon.
    """)

    # Cleanup
    r.execute_command("FT.DROPINDEX", INDEX)
    r.delete(session_key)
    print("  Cleanup done.\n")


if __name__ == "__main__":
    main()
