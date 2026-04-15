#!/usr/bin/env python3
"""
Moon as a Memory Engine — RAG + GraphRAG + Semantic Cache

Demonstrates Moon's Knowledge Navigation Engine using the moondb SDK:
vector search, graph traversal, session-aware dedup, and semantic caching.

Prerequisites:
    pip install moondb   # or: pip install -e sdk/python
    # Start Moon:  ./moon --port 6399 --shards 1

Usage:
    python moon_memory_engine.py
"""

import sys
import time

sys.path.insert(0, "sdk/python")  # Use local SDK if not installed
from moondb import MoonClient, encode_vector

# ── Configuration ─────────────────────────────────────────────────────────────

MOON_PORT = 6399
DIM = 4  # 4-dim for demo (real: 384/768 from sentence-transformers)
INDEX = "knowledge"
GRAPH = "kg"

# ── Helpers ───────────────────────────────────────────────────────────────────

def vec(*vals: float) -> list[float]:
    return list(vals)

def show(label: str, results, max_show: int = 5):
    print(f"\n  {label}: {len(results)} result(s)")
    for r in results[:max_show]:
        title = r.fields.get("title", "")
        hops = f" (hops={r.graph_hops})" if r.graph_hops is not None else ""
        print(f"    {r.key:12s}  score={r.score:<12.6f}  {title[:55]}{hops}")
    if len(results) > max_show:
        print(f"    ... and {len(results) - max_show} more")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    moon = MoonClient(host="localhost", port=MOON_PORT)
    moon.ping()
    print("Connected to Moon via MoonClient SDK\n")

    # ── Step 1: Create Vector Index ───────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Create vector index")
    print("=" * 60)

    try:
        moon.vector.drop_index(INDEX)
    except Exception:
        pass

    moon.vector.create_index(INDEX, prefix="doc:", dim=DIM, metric="COSINE")
    print("  Created index 'knowledge' (4-dim cosine)")

    # ── Step 2: Ingest Knowledge Documents ────────────────────────────────────
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
        moon.hset(key, mapping={
            "vec": encode_vector(vector), "title": title,
            "topic": topic, "category": category,
        })
    time.sleep(1)

    info = moon.vector.index_info(INDEX)
    print(f"  Indexed {info.num_docs} documents")

    # ── Step 3: Build Knowledge Graph ─────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 3: Build knowledge graph (GraphRAG)")
    print("=" * 60)

    try:
        moon.graph.create(GRAPH)
    except Exception:
        pass

    nodes = {}
    for key, _, title, _, _ in docs:
        nid = moon.graph.add_node(GRAPH, "document", _key=key, title=title)
        nodes[key] = nid

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
        moon.graph.add_edge(GRAPH, nodes[src], nodes[dst], rel)

    print(f"  Created {len(nodes)} nodes, {len(edges)} edges")

    # ── Step 4: RAG — Vector Search ───────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 4: RAG — Vector similarity search")
    print("=" * 60)

    query = vec(0.85, 0.15, 0.05, 0.0)  # Close to ML docs
    results = moon.vector.search(INDEX, query, k=3)
    show("RAG: 'How do neural networks work?'", results)

    # ── Step 5: Filtered RAG ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 5: Filtered RAG — only 'engineering' docs")
    print("=" * 60)

    results = moon.vector.search(INDEX, query, k=3, filter_expr="@category:{engineering}")
    show("Filtered: category=engineering", results)

    # ── Step 6: GraphRAG ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 6: GraphRAG — vector search + graph expansion")
    print("=" * 60)

    results = moon.vector.search(INDEX, query, k=2, expand_graph=GRAPH, expand_depth=2)
    show("KNN + EXPAND GRAPH depth=2", results)

    expanded = moon.vector.expand(INDEX, ["doc:transformers"], depth=2, graph_name=GRAPH)
    print(f"\n  FT.EXPAND from doc:transformers depth=2: {len(expanded)} reachable")
    for r in expanded[:8]:
        print(f"    -> {r.key}")

    # ── Step 7: FT.RECOMMEND ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 7: FT.RECOMMEND — 'more like transformers, unlike graph_db'")
    print("=" * 60)

    results = moon.vector.recommend(
        INDEX,
        positive_keys=["doc:transformers"],
        negative_keys=["doc:graph_db"],
        k=3,
    )
    show("Recommend", results)

    # ── Step 8: Session-Aware Retrieval ───────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 8: Session-aware retrieval (multi-turn)")
    print("=" * 60)

    session_key = "session:user42:conv1"

    r1 = moon.session.search(INDEX, session_key, query, k=3)
    print(f"\n  Turn 1: {len(r1)} results — {[r.key for r in r1]}")

    r2 = moon.session.search(INDEX, session_key, query, k=3)
    print(f"  Turn 2: {len(r2)} results — {[r.key for r in r2]}  (deduped!)")

    r3 = moon.session.search(INDEX, session_key, query, k=5)
    print(f"  Turn 3: {len(r3)} results — {[r.key for r in r3]}  (remaining unseen)")

    history = moon.session.history(session_key)
    print(f"  Session history: {history}")

    moon.session.set_ttl(session_key, 3600)
    print(f"  Session TTL set to 1 hour")

    # ── Step 9: Semantic Cache ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 9: Semantic cache")
    print("=" * 60)

    moon.cache.store(
        "cache:sem:q1", query,
        response="Neural networks are computing systems inspired by biological neurons...",
        model="gpt-4", tokens="150",
        ttl=3600,
    )
    print("  Stored cache entry with 1h TTL")

    cr = moon.cache.lookup(INDEX, "cache:sem:", query, threshold=0.95)
    print(f"  FT.CACHESEARCH: cache_hit={cr.cache_hit}, {len(cr.results)} results")

    # ── Step 10: FT.NAVIGATE ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 10: FT.NAVIGATE — multi-hop knowledge navigation")
    print("=" * 60)

    results = moon.vector.navigate(INDEX, query, k=2, hops=2)
    show("NAVIGATE: KNN->graph->re-rank (2 hops)", results)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Moon Memory Engine — using moondb SDK")
    print("=" * 60)
    print("""
  from moondb import MoonClient
  moon = MoonClient(port=6399)

  moon.vector.create_index(...)     # FT.CREATE
  moon.vector.search(...)           # FT.SEARCH KNN
  moon.vector.search(filter_expr=)  # Filtered search
  moon.vector.search(expand_graph=) # GraphRAG
  moon.vector.expand(...)           # FT.EXPAND
  moon.vector.recommend(...)        # FT.RECOMMEND
  moon.vector.navigate(...)         # FT.NAVIGATE
  moon.session.search(...)          # Session-aware dedup
  moon.cache.lookup(...)            # Semantic cache
  moon.cache.store(...)             # Store LLM response
  moon.graph.add_node(...)          # GRAPH.ADDNODE
  moon.graph.add_edge(...)          # GRAPH.ADDEDGE

  One server. One SDK. One RTT per operation.
    """)

    # Cleanup
    moon.vector.drop_index(INDEX)
    moon.session.reset(session_key)
    moon.cache.invalidate("cache:sem:q1")
    print("  Cleanup done.\n")


if __name__ == "__main__":
    main()
