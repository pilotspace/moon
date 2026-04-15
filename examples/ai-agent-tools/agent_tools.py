#!/usr/bin/env python3
"""AI Agent Tools with Moon -- function calling schemas and simulated agent loop.

Demonstrates:
  1. Define Moon tools as OpenAI function-calling schemas
  2. Simulate an agent loop: query -> tool call -> Moon response -> answer
  3. Show tool schemas compatible with OpenAI, LangChain, etc.

Usage:
  python agent_tools.py
  python agent_tools.py --host localhost --port 6379
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from typing import Any, Dict, List, Optional

from moondb import MoonClient, encode_vector


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DIM = 384
INDEX_NAME = "agent_docs"
GRAPH_NAME = "agent_graph"
DOC_PREFIX = "doc:"
CACHE_PREFIX = "cache:agent:"


# ---------------------------------------------------------------------------
# Tool definitions (OpenAI function-calling format)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_documents",
            "description": (
                "Search for documents by semantic similarity to a natural "
                "language query. Returns the most relevant documents with "
                "titles, content, and relevance scores."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query",
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of results to return (default 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "navigate_knowledge",
            "description": (
                "Multi-hop knowledge navigation combining vector search with "
                "graph traversal. Finds relevant documents and expands through "
                "connected entities to discover related context."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query to navigate from",
                    },
                    "hops": {
                        "type": "integer",
                        "description": "Number of graph hops to explore (default 2)",
                        "default": 2,
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of results (default 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recommend_similar",
            "description": (
                "Recommend documents similar to given examples. Provide one "
                "or more document keys as positive examples and optionally "
                "negative examples to refine recommendations."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "positive_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Document keys to find similar items for",
                    },
                    "negative_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Document keys to avoid similarity to (optional)",
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of recommendations (default 5)",
                        "default": 5,
                    },
                },
                "required": ["positive_keys"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cache_lookup",
            "description": (
                "Check the semantic cache for a previously answered similar "
                "query. Returns cached response if found, avoiding redundant "
                "computation. Use before expensive operations."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Query to check in the cache",
                    },
                    "threshold": {
                        "type": "number",
                        "description": "Similarity threshold (default 0.15, lower = stricter)",
                        "default": 0.15,
                    },
                },
                "required": ["query"],
            },
        },
    },
]


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
# Sample data
# ---------------------------------------------------------------------------

DOCUMENTS = [
    {"id": "doc:arch", "title": "Moon Architecture", "text": "Moon uses a thread-per-core, shared-nothing architecture with lock-free SPSC channels."},
    {"id": "doc:vector", "title": "Vector Search", "text": "Moon's vector engine uses HNSW with TurboQuant 4-bit quantization for fast similarity search."},
    {"id": "doc:graph", "title": "Graph Engine", "text": "Moon's graph engine supports 14 GRAPH.* commands with a Cypher subset for knowledge graphs."},
    {"id": "doc:persist", "title": "Persistence", "text": "Moon uses forkless persistence with per-shard WAL for crash recovery without fork() overhead."},
    {"id": "doc:memory", "title": "Memory Optimization", "text": "CompactKey (23-byte SSO) and CompactValue reduce RSS by 27-35% vs Redis at 1KB+ values."},
    {"id": "doc:cache", "title": "Semantic Cache", "text": "FT.CACHESEARCH checks vector-similar cache entries before falling back to full KNN search."},
    {"id": "doc:session", "title": "Session Search", "text": "Session-aware search deduplicates results across multiple queries in a conversation."},
    {"id": "doc:console", "title": "Web Console", "text": "Moon ships a 7-view web console with Dashboard, Browser, Console, Vectors, Graph, Memory, Help."},
]


# ---------------------------------------------------------------------------
# Tool execution (wired to Moon)
# ---------------------------------------------------------------------------

class MoonToolExecutor:
    """Executes agent tool calls against a Moon server."""

    def __init__(self, client: MoonClient) -> None:
        self.client = client

    def setup(self) -> None:
        """Set up index and load sample data."""
        try:
            self.client.vector.drop_index(INDEX_NAME)
        except Exception:
            pass

        self.client.vector.create_index(
            INDEX_NAME,
            prefix=DOC_PREFIX,
            field_name="vec",
            algorithm="HNSW",
            dim=DIM,
            metric="COSINE",
            extra_schema=["title", "TEXT", "text", "TEXT"],
        )

        for doc in DOCUMENTS:
            vec = mock_embedding(doc["text"])
            self.client.hset(
                doc["id"],
                mapping={
                    "title": doc["title"],
                    "text": doc["text"],
                    "vec": encode_vector(vec),
                },
            )

    def execute(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool call and return structured results."""
        t0 = time.perf_counter()

        if tool_name == "search_documents":
            result = self._search(arguments)
        elif tool_name == "navigate_knowledge":
            result = self._navigate(arguments)
        elif tool_name == "recommend_similar":
            result = self._recommend(arguments)
        elif tool_name == "cache_lookup":
            result = self._cache_lookup(arguments)
        else:
            result = {"error": f"Unknown tool: {tool_name}"}

        elapsed = (time.perf_counter() - t0) * 1000
        result["execution_time_ms"] = round(elapsed, 1)
        return result

    def _search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args["query"]
        k = args.get("k", 5)
        vec = mock_embedding(query)

        results = self.client.vector.search(
            INDEX_NAME, vec, k=k, field_name="vec",
            return_fields=["title", "text"],
        )
        return {
            "tool": "search_documents",
            "query": query,
            "results": [
                {"key": r.key, "title": r.fields.get("title", ""), "text": r.fields.get("text", ""), "score": round(r.score, 4)}
                for r in results
            ],
        }

    def _navigate(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args["query"]
        hops = args.get("hops", 2)
        k = args.get("k", 5)
        vec = mock_embedding(query)

        results = self.client.vector.navigate(
            INDEX_NAME, vec, k=k, field_name="vec",
            hops=hops, hop_penalty=0.05,
        )
        return {
            "tool": "navigate_knowledge",
            "query": query,
            "hops": hops,
            "results": [
                {"key": r.key, "title": r.fields.get("title", ""), "text": r.fields.get("text", ""), "score": round(r.score, 4), "graph_hops": r.graph_hops}
                for r in results
            ],
        }

    def _recommend(self, args: Dict[str, Any]) -> Dict[str, Any]:
        positive = args["positive_keys"]
        negative = args.get("negative_keys")
        k = args.get("k", 5)

        results = self.client.vector.recommend(
            INDEX_NAME,
            positive_keys=positive,
            negative_keys=negative,
            k=k,
        )
        return {
            "tool": "recommend_similar",
            "positive_keys": positive,
            "results": [
                {"key": r.key, "title": r.fields.get("title", ""), "score": round(r.score, 4)}
                for r in results
            ],
        }

    def _cache_lookup(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args["query"]
        threshold = args.get("threshold", 0.15)
        vec = mock_embedding(query)

        result = self.client.cache.lookup(
            INDEX_NAME, CACHE_PREFIX, encode_vector(vec),
            threshold=threshold, k=1,
        )
        return {
            "tool": "cache_lookup",
            "query": query,
            "cache_hit": result.cache_hit,
            "results": [
                {"key": r.key, "fields": r.fields}
                for r in result.results
            ] if result.results else [],
        }


# ---------------------------------------------------------------------------
# Simulated agent loop
# ---------------------------------------------------------------------------

def simulated_tool_selection(query: str) -> List[Dict[str, Any]]:
    """Simulate LLM tool selection based on query patterns.

    In production, the LLM decides which tools to call based on the
    tool schemas and user query. Here we pattern-match for the demo.
    """
    q = query.lower()

    if "recommend" in q or "similar to" in q:
        return [{"name": "recommend_similar", "arguments": {"positive_keys": ["doc:vector"], "k": 3}}]
    elif "navigate" in q or "explore" in q or "related" in q:
        return [{"name": "navigate_knowledge", "arguments": {"query": query, "hops": 2, "k": 5}}]
    elif "cache" in q or "check if" in q:
        return [
            {"name": "cache_lookup", "arguments": {"query": query}},
            {"name": "search_documents", "arguments": {"query": query, "k": 3}},
        ]
    else:
        return [{"name": "search_documents", "arguments": {"query": query, "k": 3}}]


def simulated_answer(query: str, tool_results: List[Dict[str, Any]]) -> str:
    """Simulate LLM generating an answer from tool results."""
    all_texts = []
    for result in tool_results:
        for r in result.get("results", []):
            if "text" in r:
                all_texts.append(r["text"])
    context = " ".join(all_texts[:3])
    return f"Based on the retrieved information: {context[:200]}..."


# ---------------------------------------------------------------------------
# Demo queries
# ---------------------------------------------------------------------------

DEMO_QUERIES = [
    "How does Moon handle vector search?",
    "Navigate and explore topics related to persistence",
    "Recommend documents similar to the vector search article",
    "Check if we have cached info about Moon's architecture, then search",
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="AI Agent Tools with Moon")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  AI Agent Tools Demo with Moon")
    print(f"{'='*60}\n")

    client = MoonClient(host=args.host, port=args.port, decode_responses=True)
    try:
        client.ping()
    except Exception as e:
        print(f"ERROR: Cannot connect to Moon at {args.host}:{args.port}")
        print(f"  {e}")
        sys.exit(1)
    print(f"Connected to Moon at {args.host}:{args.port}")

    # --- Print tool schemas ---
    print(f"\n--- Available Tools ---\n")
    for tool in TOOLS:
        func = tool["function"]
        params = func["parameters"]["properties"]
        param_str = ", ".join(f"{k}: {v.get('type', '?')}" for k, v in params.items())
        print(f"  {func['name']}({param_str})")
        print(f"    {func['description'][:80]}...")
    print()

    # --- Setup ---
    executor = MoonToolExecutor(client)
    executor.setup()
    print(f"Loaded {len(DOCUMENTS)} documents into index '{INDEX_NAME}'")

    # --- Agent loop ---
    for query in DEMO_QUERIES:
        print(f"\n{'='*60}")
        print(f"User: {query}")
        print(f"{'='*60}")

        # Step 1: Tool selection (simulated LLM decision)
        tool_calls = simulated_tool_selection(query)
        print(f"\n  Agent decides to call {len(tool_calls)} tool(s):")
        for tc in tool_calls:
            print(f"    -> {tc['name']}({json.dumps(tc['arguments'], default=str)[:80]})")

        # Step 2: Execute tools against Moon
        tool_results = []
        for tc in tool_calls:
            result = executor.execute(tc["name"], tc["arguments"])
            tool_results.append(result)
            print(f"\n  Tool result ({tc['name']}, {result['execution_time_ms']}ms):")
            for r in result.get("results", [])[:3]:
                title = r.get("title", r.get("key", "?"))
                score = r.get("score", "n/a")
                print(f"    - {title} (score: {score})")
            if "cache_hit" in result:
                print(f"    Cache hit: {result['cache_hit']}")

        # Step 3: Generate answer (simulated LLM)
        answer = simulated_answer(query, tool_results)
        print(f"\n  Agent: {answer}")

    # --- Full tool schema output ---
    print(f"\n{'='*60}")
    print("  Full OpenAI-Compatible Tool Schemas")
    print(f"{'='*60}\n")
    print(json.dumps(TOOLS, indent=2))
    print()


if __name__ == "__main__":
    main()
