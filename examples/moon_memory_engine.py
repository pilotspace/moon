#!/usr/bin/env python3
"""
Moon Memory Engine — AI Agent with RAG, GraphRAG, Session Memory & Semantic Cache

A complete agent loop that replaces Redis + Pinecone + Neo4j with one Moon server.
Demonstrates the real workflow an AI platform engineer would build:

  Query → Cache check → Session-aware retrieval → Graph expansion → LLM → Cache store

Supports real OpenAI calls (set OPENAI_API_KEY) or simulated LLM for demo.

Prerequisites:
    pip install moondb sentence-transformers   # or: pip install -e sdk/python
    # Optional: pip install openai
    # Start Moon: ./moon --port 6399 --shards 1

Usage:
    python moon_memory_engine.py                    # Interactive mode (simulated LLM)
    OPENAI_API_KEY=sk-... python moon_memory_engine.py   # With real OpenAI
    python moon_memory_engine.py --demo             # Run preset demo conversation
"""

import hashlib
import os
import sys
import struct
import time
from typing import Optional

sys.path.insert(0, "sdk/python")
from moondb import MoonClient, SearchResult, encode_vector

# ── Config ────────────────────────────────────────────────────────────────────

MOON_PORT = 6399
INDEX = "knowledge"
GRAPH = "kg"
CACHE_PREFIX = "doc:cache:"  # Must share index prefix ("doc:") to be searchable
DIM = 4  # 4-dim for demo; real: 384 from MiniLM

# ── Simulated embedding (replace with sentence-transformers in production) ────

# Keyword → vector mapping for deterministic 4-dim demo
KEYWORD_VECS = {
    "machine learning":  [0.9, 0.1, 0.0, 0.0],
    "neural network":    [0.8, 0.2, 0.1, 0.0],
    "transformer":       [0.7, 0.3, 0.2, 0.0],
    "rag":               [0.6, 0.2, 0.3, 0.1],
    "retrieval":         [0.6, 0.2, 0.3, 0.1],
    "vector database":   [0.5, 0.1, 0.4, 0.2],
    "graph":             [0.2, 0.1, 0.7, 0.1],
    "knowledge graph":   [0.3, 0.2, 0.6, 0.1],
    "agent":             [0.4, 0.3, 0.2, 0.3],
    "prompt":            [0.5, 0.4, 0.1, 0.2],
    "fine-tuning":       [0.8, 0.1, 0.0, 0.2],
    "safety":            [0.1, 0.1, 0.1, 0.8],
    "deploy":            [0.3, 0.1, 0.2, 0.5],
    "cache":             [0.4, 0.2, 0.3, 0.2],
}


def embed(text: str) -> list[float]:
    """Simple keyword-matching embedding for demo. Replace with MiniLM in production.

    Adds a text-hash perturbation so each unique query gets a unique vector.
    This makes semantic cache work (exact re-ask → distance ≈ 0 to cache entry).
    """
    text_lower = text.lower()
    best_match = [0.25, 0.25, 0.25, 0.25]
    best_score = 0
    for keyword, vec in KEYWORD_VECS.items():
        words = keyword.split()
        hits = sum(1 for w in words if w in text_lower)
        if hits > best_score:
            best_score = hits
            best_match = vec[:]
    # Perturb by text hash so each query is unique (like real embeddings)
    h = hash(text_lower) & 0xFFFF
    for i in range(4):
        best_match[i] += ((h >> (i * 4)) & 0xF) * 0.001
    return best_match


def cache_key(query: str) -> str:
    h = hashlib.md5(query.encode()).hexdigest()[:12]
    return f"{CACHE_PREFIX}{h}"


# ── LLM (real or simulated) ──────────────────────────────────────────────────

def call_llm(query: str, context_docs: list[SearchResult]) -> str:
    """Call LLM with retrieved context. Uses OpenAI if available, otherwise simulates."""
    context = "\n".join(
        f"- [{r.key}] {r.fields.get('title', '')}" for r in context_docs
    )
    prompt = f"Context:\n{context}\n\nQuestion: {query}\nAnswer concisely based on the context above."

    # Try real OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        try:
            import openai
            client = openai.OpenAI(api_key=api_key)
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Answer based only on the provided context. Be concise."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=150,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            pass  # Fall through to simulated

    # Simulated LLM
    if not context_docs:
        return "I don't have enough context to answer that question."
    titles = [r.fields.get("title", r.key) for r in context_docs[:3]]
    return f"Based on {len(context_docs)} retrieved documents ({', '.join(titles)}), " \
           f"here is a synthesized answer about '{query}'."


# ── Knowledge Base Setup ──────────────────────────────────────────────────────

DOCS = [
    ("doc:ml_basics",    [0.9,0.1,0.0,0.0], "Machine learning fundamentals and statistical learning theory", "ml", "science"),
    ("doc:neural_nets",  [0.8,0.2,0.1,0.0], "Neural network architectures: perceptrons to deep learning",   "ml", "science"),
    ("doc:transformers", [0.7,0.3,0.2,0.0], "Transformer models: attention mechanism and architectures",     "ml", "science"),
    ("doc:rag_overview", [0.6,0.2,0.3,0.1], "Retrieval-augmented generation: combining search with LLMs",   "rag", "engineering"),
    ("doc:dense_retr",   [0.55,0.15,0.35,0.15], "Dense passage retrieval with bi-encoder semantic search",   "rag", "engineering"),
    ("doc:chunking",     [0.5,0.2,0.35,0.1], "Document chunking strategies for RAG pipelines",              "rag", "engineering"),
    ("doc:vector_db",    [0.5,0.1,0.4,0.2], "Vector database internals: HNSW indexing and filtering",       "infra", "engineering"),
    ("doc:graph_db",     [0.1,0.1,0.8,0.1], "Graph database design patterns and query languages",           "infra", "engineering"),
    ("doc:kg_construct", [0.3,0.2,0.6,0.1], "Knowledge graph construction from unstructured text",          "kg", "science"),
    ("doc:graphrag",     [0.4,0.2,0.5,0.1], "GraphRAG: combining graph structure with vector retrieval",    "kg", "engineering"),
    ("doc:llm_agents",   [0.4,0.3,0.2,0.3], "LLM-based autonomous agents: ReAct and tool use",             "agents", "engineering"),
    ("doc:agent_memory", [0.35,0.25,0.15,0.35], "Memory systems for AI agents: working and long-term",      "agents", "engineering"),
    ("doc:sem_cache",    [0.4,0.2,0.3,0.2], "Semantic caching to reduce LLM API costs",                     "rag", "engineering"),
    ("doc:prompt_eng",   [0.5,0.4,0.1,0.2], "Prompt engineering: chain-of-thought and few-shot patterns",   "prompts", "engineering"),
    ("doc:fine_tuning",  [0.8,0.1,0.0,0.2], "Fine-tuning language models: LoRA, QLoRA, and adapters",       "ml", "science"),
    ("doc:ai_safety",    [0.1,0.1,0.1,0.8], "AI safety: alignment, guardrails, and responsible deployment", "ethics", "science"),
    ("doc:llm_serving",  [0.3,0.1,0.2,0.5], "LLM serving: KV-cache, batching, and quantization",           "mlops", "engineering"),
    ("doc:eval_rag",     [0.5,0.2,0.35,0.15], "RAG evaluation: faithfulness, relevance, and recall metrics","rag", "science"),
]

GRAPH_EDGES = [
    ("doc:ml_basics",    "doc:neural_nets",   "prerequisite"),
    ("doc:neural_nets",  "doc:transformers",   "prerequisite"),
    ("doc:transformers", "doc:rag_overview",   "enables"),
    ("doc:rag_overview", "doc:dense_retr",     "component"),
    ("doc:rag_overview", "doc:chunking",       "component"),
    ("doc:rag_overview", "doc:eval_rag",       "evaluated_by"),
    ("doc:dense_retr",   "doc:vector_db",      "uses"),
    ("doc:rag_overview", "doc:kg_construct",   "extends"),
    ("doc:kg_construct", "doc:graph_db",       "uses"),
    ("doc:kg_construct", "doc:graphrag",       "enables"),
    ("doc:graphrag",     "doc:rag_overview",   "enhances"),
    ("doc:transformers", "doc:llm_agents",     "enables"),
    ("doc:llm_agents",   "doc:agent_memory",   "requires"),
    ("doc:llm_agents",   "doc:prompt_eng",     "uses"),
    ("doc:agent_memory", "doc:sem_cache",      "uses"),
    ("doc:ml_basics",    "doc:fine_tuning",    "technique"),
    ("doc:fine_tuning",  "doc:transformers",   "applies_to"),
    ("doc:rag_overview", "doc:sem_cache",      "optimized_by"),
    ("doc:llm_agents",   "doc:ai_safety",      "constrained_by"),
    ("doc:llm_serving",  "doc:sem_cache",      "complements"),
]

TITLES = {d[0]: d[2] for d in DOCS}


def setup_knowledge_base(moon: MoonClient):
    """One-time setup: create index, ingest docs, build graph."""
    print("Setting up knowledge base...")

    # Index
    try:
        moon.vector.drop_index(INDEX)
    except Exception:
        pass
    moon.vector.create_index(INDEX, prefix="doc:", dim=DIM, metric="L2")

    # Docs
    for key, vec, title, domain, category in DOCS:
        moon.hset(key, mapping={
            "vec": encode_vector(vec), "title": title,
            "domain": domain, "category": category,
        })
    time.sleep(1)

    # Graph
    try:
        moon.graph.create(GRAPH)
    except Exception:
        pass
    nodes = {}
    for key, _, title, domain, _ in DOCS:
        nodes[key] = moon.graph.add_node(GRAPH, domain, _key=key, title=title)
    for src, dst, rel in GRAPH_EDGES:
        moon.graph.add_edge(GRAPH, nodes[src], nodes[dst], rel)

    info = moon.vector.index_info(INDEX)
    print(f"  {info.num_docs} docs indexed, {len(nodes)} graph nodes, {len(GRAPH_EDGES)} edges")
    print()


# ── Agent ─────────────────────────────────────────────────────────────────────

class MoonAgent:
    """AI agent powered by Moon as its memory engine.

    Architecture:
        Query → Embed → Cache check → Session search → Graph expand → LLM → Cache store

    Replaces: Redis (session) + Pinecone (vectors) + Neo4j (graph) + custom cache layer
    """

    def __init__(self, moon: MoonClient, session_id: str):
        self.moon = moon
        self.session_key = f"session:{session_id}"
        self.turn = 0
        self.total_tokens_saved = 0

    def ask(self, query: str) -> str:
        """Process a user query through the full memory engine pipeline."""
        self.turn += 1
        query_vec = embed(query)

        print(f"\n{'─'*60}")
        print(f"  Turn {self.turn}: \"{query}\"")
        print(f"{'─'*60}")

        # ── Step 1: Semantic Cache Check ──────────────────────────────────
        # Search top-K and check if any near-zero-distance result is a cache entry.
        # Cache entries share the index (prefix doc:cache:) and have response field.
        probe = self.moon.vector.search(INDEX, query_vec, k=3)
        for hit in probe:
            if hit.key.startswith(CACHE_PREFIX) and hit.score < 0.01 and "response" in hit.fields:
                tokens = int(hit.fields.get("tokens", "0"))
                self.total_tokens_saved += tokens
                print(f"  [CACHE HIT] key={hit.key}, score={hit.score:.6f}, saved ~{tokens} tokens")
                print(f"\n  Answer (cached): {hit.fields['response'][:200]}")
                return hit.fields["response"]

        print(f"  [CACHE MISS] Running retrieval pipeline...")

        # ── Step 2: Session-Aware Vector Search ───────────────────────────
        # Deduplicates against docs already shown in this conversation
        docs = self.moon.session.search(INDEX, self.session_key, query_vec, k=8)
        # Filter out cache entries from search results (they share the index)
        docs = [r for r in docs if not r.key.startswith(CACHE_PREFIX)][:5]
        print(f"  [SEARCH] {len(docs)} new results (session-deduped)")
        for r in docs[:3]:
            print(f"    {r.key:16s} score={r.score:.4f}  {TITLES.get(r.key, '')[:50]}")

        # ── Step 3: Graph Expansion (when results are thin) ───────────────
        if len(docs) < 3 and docs:
            seed_keys = [r.key for r in docs[:2]]
            expanded = self.moon.vector.expand(INDEX, seed_keys, depth=2, graph_name=GRAPH)
            # Add expanded docs not already in results
            seen_keys = {r.key for r in docs}
            new_from_graph = [r for r in expanded if r.key not in seen_keys]
            if new_from_graph:
                docs.extend(new_from_graph[:3])
                print(f"  [GRAPH EXPAND] +{len(new_from_graph[:3])} docs via graph traversal")
                for r in new_from_graph[:3]:
                    print(f"    {r.key:16s} (via graph)    {TITLES.get(r.key, '')[:50]}")

        # ── Step 4: Fallback — broader search without session filter ──────
        if not docs:
            print(f"  [FALLBACK] Session exhausted, searching without dedup...")
            all_results = self.moon.vector.search(INDEX, query_vec, k=5)
            docs = [r for r in all_results if not r.key.startswith(CACHE_PREFIX)][:3]
            print(f"  [SEARCH] {len(docs)} results (no session filter)")
            for r in docs[:3]:
                print(f"    {r.key:16s} score={r.score:.4f}  {TITLES.get(r.key, '')[:50]}")

        # ── Step 5: Call LLM ──────────────────────────────────────────────
        response = call_llm(query, docs)
        token_estimate = len(response.split()) * 2  # rough estimate
        print(f"  [LLM] Generated response (~{token_estimate} tokens)")

        # ── Step 6: Store in Semantic Cache ───────────────────────────────
        self.moon.cache.store(
            cache_key(query), query_vec,
            response=response,
            query=query,
            tokens=str(token_estimate),
            ttl=3600,
        )
        print(f"  [CACHED] Stored for 1h (future similar queries skip LLM)")

        print(f"\n  Answer: {response[:200]}")
        return response

    def status(self):
        """Show agent memory status."""
        history = self.moon.session.history(self.session_key)
        print(f"\n  Agent Status:")
        print(f"    Turns: {self.turn}")
        print(f"    Docs seen (session): {len(history)} — {history}")
        print(f"    Tokens saved (cache): ~{self.total_tokens_saved}")

    def reset(self):
        """Clear session and cache."""
        self.moon.session.reset(self.session_key)
        self.moon.cache.invalidate_prefix(CACHE_PREFIX)
        self.turn = 0
        self.total_tokens_saved = 0
        print("  Agent memory cleared.")


# ── Demo Conversation ─────────────────────────────────────────────────────────

DEMO_QUERIES = [
    "What is retrieval augmented generation?",
    "How does the retrieval part work technically?",
    "What about using knowledge graphs with RAG?",
    "What is retrieval augmented generation?",    # Repeat → cache hit!
    "How do AI agents use memory systems?",
    "Tell me about vector databases",
]


def run_demo(moon: MoonClient):
    """Run a preset conversation demonstrating the memory engine."""
    agent = MoonAgent(moon, "demo_user")

    print("=" * 60)
    print("  Moon Memory Engine — Agent Demo")
    print("  One server replaces Redis + Pinecone + Neo4j")
    print("=" * 60)

    for query in DEMO_QUERIES:
        agent.ask(query)

    agent.status()

    print(f"\n{'='*60}")
    print("  Key Observations:")
    print("  1. Turn 4 was a CACHE HIT (same question as Turn 1)")
    print("  2. Session dedup prevents repeating docs across turns")
    print("  3. Graph expansion finds related docs when results are thin")
    print("  4. Every operation: one server, one RTT, one SDK call")
    print(f"{'='*60}")

    agent.reset()


def run_interactive(moon: MoonClient):
    """Interactive REPL for the memory engine."""
    agent = MoonAgent(moon, "interactive")

    print("=" * 60)
    print("  Moon Memory Engine — Interactive Mode")
    print("  Type a question, 'status' for memory state, 'reset' to clear, 'quit' to exit")
    print("=" * 60)

    while True:
        try:
            query = input("\n  You: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not query:
            continue
        if query.lower() == "quit":
            break
        if query.lower() == "status":
            agent.status()
            continue
        if query.lower() == "reset":
            agent.reset()
            continue

        agent.ask(query)

    agent.reset()
    print("\n  Goodbye.\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    moon = MoonClient(host="localhost", port=MOON_PORT)
    moon.ping()

    setup_knowledge_base(moon)

    if "--demo" in sys.argv or not sys.stdin.isatty():
        run_demo(moon)
    else:
        run_interactive(moon)

    # Cleanup
    moon.vector.drop_index(INDEX)


if __name__ == "__main__":
    main()
