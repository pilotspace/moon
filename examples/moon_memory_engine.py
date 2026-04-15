#!/usr/bin/env python3
"""
Moon Memory Engine — RAG + GraphRAG + Session Memory in 80 lines.

    pip install moondb sentence-transformers
    ./moon --port 6399 --shards 1
    python examples/moon_memory_engine.py
"""

import sys, time, os, logging
sys.path.insert(0, "sdk/python")
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
os.environ["SAFETENSORS_FAST_GPU"] = "0"
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

from sentence_transformers import SentenceTransformer
from moondb import MoonClient, encode_vector

moon = MoonClient(host="localhost", port=6399)
model = SentenceTransformer("all-MiniLM-L6-v2")
DIM = 384

# ── 1. Create index ──────────────────────────────────────────────────────────

try: moon.vector.drop_index("docs")
except: pass

moon.vector.create_index("docs", prefix="doc:", dim=DIM, metric="COSINE")

# ── 2. Add knowledge ─────────────────────────────────────────────────────────

knowledge = [
    ("doc:rag",     "Retrieval-augmented generation combines search with LLM generation",  "rag"),
    ("doc:chunk",   "Document chunking splits text into passages for embedding",            "rag"),
    ("doc:embed",   "Embedding models convert text to dense vectors for similarity search", "rag"),
    ("doc:hnsw",    "HNSW is a graph-based algorithm for approximate nearest neighbor search", "infra"),
    ("doc:graphrag","GraphRAG expands vector search results through knowledge graph edges", "rag"),
    ("doc:agent",   "AI agents use tools and memory to solve multi-step tasks",             "agents"),
    ("doc:memory",  "Agent memory systems track conversation context across turns",         "agents"),
    ("doc:cache",   "Semantic caching avoids redundant LLM calls for similar questions",    "rag"),
    ("doc:rerank",  "Cross-encoder reranking improves retrieval precision after initial search", "rag"),
    ("doc:safety",  "AI safety covers alignment, guardrails, and responsible deployment",   "ethics"),
]

texts = [text for _, text, _ in knowledge]
embeddings = model.encode(texts, normalize_embeddings=True)

for (key, text, domain), emb in zip(knowledge, embeddings):
    moon.hset(key, mapping={"vec": encode_vector(emb.tolist()), "title": text, "domain": domain})

time.sleep(1)
print(f"Indexed {moon.vector.index_info('docs').num_docs} documents\n")

# ── 3. Build knowledge graph ─────────────────────────────────────────────────

try: moon.graph.create("kg")
except: pass

nodes = {}
for key, text, domain in knowledge:
    nodes[key] = moon.graph.add_node("kg", domain, _key=key)

for src, dst, rel in [
    ("doc:rag",   "doc:chunk",    "requires"),
    ("doc:rag",   "doc:embed",    "requires"),
    ("doc:rag",   "doc:rerank",   "uses"),
    ("doc:rag",   "doc:cache",    "optimized_by"),
    ("doc:embed", "doc:hnsw",     "indexed_by"),
    ("doc:rag",   "doc:graphrag", "extended_by"),
    ("doc:agent", "doc:memory",   "requires"),
    ("doc:agent", "doc:rag",      "uses"),
]:
    moon.graph.add_edge("kg", nodes[src], nodes[dst], rel)

print("Knowledge graph: 10 nodes, 8 edges\n")

# ── 4. Conversation with memory ──────────────────────────────────────────────

def title(key):
    """Fetch document title from Moon."""
    t = moon.hget(key, "title")
    return t.decode() if t else key

def ask(query, session="session:user1"):
    """One query through the memory engine: search → expand → answer."""
    q = model.encode(query, normalize_embeddings=True).tolist()

    # Session-aware search (deduplicates across turns)
    results = moon.session.search("docs", session, q, k=3)

    # If results are thin, expand through the graph
    if len(results) < 2 and results:
        expanded = moon.vector.expand("docs", [r.key for r in results], depth=2, graph_name="kg")
        seen = {r.key for r in results}
        results += [r for r in expanded if r.key not in seen][:2]

    print(f'Q: "{query}"')
    if not results:
        print("  (no new results — session exhausted)\n")
        return
    for r in results[:5]:
        print(f"  → {r.key:12s}  {title(r.key)[:60]}")
    print()

# Run a multi-turn conversation
print("=" * 65)
print("Multi-turn conversation with session memory")
print("=" * 65 + "\n")

ask("How does RAG work?")
ask("What about the retrieval part?")        # Session deduplicates Turn 1 results
ask("How do AI agents use memory?")          # Different topic, fresh results
ask("Tell me more about knowledge graphs")   # Graph expansion kicks in

# Show session state
history = moon.session.history("session:user1")
print(f"Session memory: {len(history)} docs seen across 4 turns")
print(f"  {history}\n")

# ── 5. Filtered search ───────────────────────────────────────────────────────

print("=" * 65)
print("Filtered search")
print("=" * 65 + "\n")

q = model.encode("How does retrieval work?", normalize_embeddings=True).tolist()
results = moon.vector.search("docs", q, k=3, filter_expr="@domain:{agents}")
print('Q: "How does retrieval work?" (filter: domain=agents)')
for r in results:
    print(f"  → {r.key:12s}  {title(r.key)[:60]}")

# ── 6. Recommend ──────────────────────────────────────────────────────────────

print(f"\n{'=' * 65}")
print("Recommendations")
print("=" * 65 + "\n")

results = moon.vector.recommend("docs", positive_keys=["doc:rag"], negative_keys=["doc:safety"], k=3)
print('"More like RAG, less like safety":')
for r in results:
    print(f"  → {r.key:12s}  score={r.score:.4f}  {title(r.key)[:50]}")

# Cleanup
moon.session.reset("session:user1")
moon.vector.drop_index("docs")
