# RAG Quickstart with Moon

Build a Retrieval-Augmented Generation (RAG) pipeline in 30 minutes using Moon's vector search engine and the `moondb` Python SDK.

## What you will build

A complete RAG pipeline that:

1. Stores document chunks with vector embeddings in Moon
2. Searches for relevant context using KNN vector similarity
3. Formats retrieved context for LLM consumption
4. (Optional) Calls an LLM API to generate an answer

## Prerequisites

- Moon server running on `localhost:6379` (see [main README](../../README.md) for build instructions)
- Python 3.9+

## Setup

```bash
cd examples/rag-quickstart
pip install -r requirements.txt
```

## Run

```bash
# Zero-dependency mode (uses deterministic mock embeddings)
python rag_quickstart.py

# With real embeddings (requires sentence-transformers)
python rag_quickstart.py --use-real-embeddings

# With LLM answer generation (requires OPENAI_API_KEY)
python rag_quickstart.py --use-real-embeddings --generate-answer
```

## What the script does

1. **Connect** to Moon via the `moondb` SDK
2. **Create a vector index** with `FT.CREATE` (HNSW, 384 dimensions, cosine similarity)
3. **Store 10 sample documents** as Redis hashes with vector embeddings via `HSET`
4. **Search** for relevant documents using `FT.SEARCH` KNN
5. **Format results** into an LLM-ready context prompt
6. **(Optional)** Send the prompt to OpenAI and print the answer

## Key concepts

- **Auto-indexing**: Moon automatically indexes vectors when you `HSET` a key matching the index prefix. No separate insert call needed.
- **KNN search**: `FT.SEARCH` with `*=>[KNN k @field $vec]` syntax finds the k nearest neighbors.
- **Cosine similarity**: Best for text embeddings (sentence-transformers, OpenAI, etc.).

## Next steps

- Try the [Semantic Cache](../semantic-cache/) example to avoid redundant LLM calls
- Try the [GraphRAG](../graphrag/) example for knowledge-graph-enriched retrieval
- Explore the [AI Agent Tools](../ai-agent-tools/) example for function-calling integration
