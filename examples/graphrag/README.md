# GraphRAG with Moon

Combine knowledge graph traversal with vector search for richer, more contextual retrieval.

## Why GraphRAG?

Standard RAG retrieves documents by vector similarity alone. GraphRAG adds a knowledge graph layer that captures **relationships** between entities. When you search for "What does Alice work on?", GraphRAG can:

1. Find documents mentioning Alice (vector search)
2. Traverse the knowledge graph to find Alice's team, projects, and related documents (graph expansion)
3. Return a richer context that includes connected information the vector search alone would miss

## What this example demonstrates

1. **Build a knowledge graph** using `GRAPH.CREATE`, `GRAPH.ADDNODE`, `GRAPH.ADDEDGE`
2. **Store document vectors** using `HSET` + `FT.CREATE` (auto-indexed)
3. **Link graph nodes to documents** via shared key properties
4. **Vector search with graph expansion** using `FT.SEARCH` with `EXPAND GRAPH`
5. **Multi-hop retrieval** using `FT.NAVIGATE` for graph-guided search

## Prerequisites

- Moon server running on `localhost:6379`
- Python 3.9+

## Setup

```bash
cd examples/graphrag
pip install -r requirements.txt
```

## Run

```bash
python graphrag.py
```

## Knowledge graph structure

The example builds a small knowledge graph about a software team:

```
  Alice ──WORKS_ON──> Moon Project ──USES──> Rust
    |                    |
    KNOWS                RELATES_TO
    |                    |
    v                    v
   Bob ──WORKS_ON──> Vector Engine ──RELATES_TO──> HNSW
    |
    KNOWS
    |
    v
  Charlie ──WORKS_ON──> Graph Engine ──RELATES_TO──> Cypher
```

Each person and project node is linked to a document containing detailed information.

## Key concepts

- **Graph expansion** (`EXPAND GRAPH`): After finding initial vector matches, Moon traverses the graph to pull in related documents within N hops.
- **Multi-hop navigation** (`FT.NAVIGATE`): Combines vector similarity with graph topology, applying a hop penalty to balance direct relevance vs. connected context.
- **Hybrid queries**: Use graph structure for relationship-aware retrieval that pure vector search cannot achieve.

## Next steps

- Try the [RAG Quickstart](../rag-quickstart/) for pure vector search
- Try the [Semantic Cache](../semantic-cache/) to cache GraphRAG results
- Try the [AI Agent Tools](../ai-agent-tools/) to expose GraphRAG as agent tools
