# moondb

Python SDK for [MoonDB](https://github.com/tindang/moon) -- a Redis-compatible server with native vector search, graph engine, session-aware retrieval, and semantic caching.

## Install

```bash
pip install moondb

# With LangChain integration
pip install moondb[langchain]

# With LlamaIndex integration
pip install moondb[llamaindex]

# Everything
pip install moondb[all]
```

## Quick Start

```python
from moondb import MoonClient, encode_vector

client = MoonClient(host="localhost", port=6379)

# All standard Redis commands work
client.set("hello", "world")
client.get("hello")  # b"world"

# Create a vector index
client.vector.create_index(
    "products",
    prefix="product:",
    dim=384,
    metric="COSINE",
)

# Store documents with vectors
embedding = encode_vector([0.1, 0.2, 0.3, ...])  # 384-dim
client.hset("product:1", mapping={
    "vec": embedding,
    "title": "Wireless Headphones",
    "price": "99.99",
})

# Vector search
results = client.vector.search("products", [0.1, 0.2, ...], k=5)
for r in results:
    print(f"{r.key}: {r.score:.4f} - {r.fields.get('title')}")
```

## Features

### Vector Search (FT.*)

```python
# Create index with tuning parameters
client.vector.create_index(
    "docs", dim=768, metric="L2",
    ef_construction=400, ef_runtime=100,
    compact_threshold=10000,
)

# KNN search
results = client.vector.search("docs", query_vec, k=10)

# Session-aware search (deduplicates across calls)
results = client.vector.search(
    "docs", query_vec, session_key="session:user123"
)

# Search with graph expansion
results = client.vector.search(
    "docs", query_vec, expand_graph="knowledge", expand_depth=3
)

# Recommendations
recs = client.vector.recommend(
    "products",
    positive_keys=["product:1", "product:5"],
    negative_keys=["product:99"],
    k=10,
)

# Multi-hop navigation
results = client.vector.navigate(
    "knowledge", query_vec, hops=3, hop_penalty=0.05
)

# Semantic cache
from moondb.types import CacheSearchResult
result = client.vector.cache_search(
    "qa", "cache:qa:", query_vec, threshold=0.1
)
if result.cache_hit:
    print("Cached:", result.results[0].fields["response"])
```

### Graph Engine (GRAPH.*)

```python
# Create graph
client.graph.create("social")

# Add nodes
alice = client.graph.add_node("social", "Person", name="Alice", age="30")
bob = client.graph.add_node("social", "Person", name="Bob", age="25")

# Add edges
client.graph.add_edge("social", alice, bob, "KNOWS", weight=0.9)

# Cypher queries
result = client.graph.query(
    "social",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
)
for row in result.rows:
    print(row)

# Graph traversal
neighbors = client.graph.neighbors("social", alice, depth=2)
```

### Session-Aware Search

```python
# First search returns fresh results
results = client.session.search(
    "products", "session:user1", query_vec, k=5
)

# Subsequent searches filter out previously seen results
more_results = client.session.search(
    "products", "session:user1", query_vec, k=5
)

# Session management
history = client.session.history("session:user1")
client.session.set_ttl("session:user1", 3600)
client.session.reset("session:user1")
```

### Semantic Caching

```python
# Store a cache entry
client.cache.store(
    "cache:qa:hash123", embedding,
    response="The answer is 42",
    model="gpt-4",
    ttl=3600,
)

# Lookup with fallback
result = client.cache.lookup("qa", "cache:qa:", embedding, threshold=0.1)
if result.cache_hit:
    print("From cache:", result.results[0].fields["response"])
else:
    print("Cache miss, got", len(result.results), "search results")

# Invalidation
client.cache.invalidate("cache:qa:hash123")
client.cache.invalidate_prefix("cache:qa:")
```

## Async

```python
import asyncio
from moondb import AsyncMoonClient

async def main():
    client = AsyncMoonClient(host="localhost", port=6379)

    await client.vector.create_index("docs", dim=384, metric="COSINE")
    results = await client.vector.search("docs", [0.1, 0.2, ...])

    await client.graph.create("social")
    node_id = await client.graph.add_node("social", "Person", name="Alice")

    await client.aclose()

asyncio.run(main())
```

## LangChain Integration

```python
from langchain_openai import OpenAIEmbeddings
from moondb.integrations.langchain import MoonVectorStore

store = MoonVectorStore(
    index_name="knowledge",
    embedding=OpenAIEmbeddings(),
    moon_url="redis://localhost:6379",
    dim=1536,
    metric="COSINE",
)

# Add documents
store.add_texts(
    ["Document one", "Document two"],
    metadatas=[{"source": "web"}, {"source": "pdf"}],
)

# Search
docs = store.similarity_search("machine learning", k=5)
for doc in docs:
    print(doc.page_content, doc.metadata)

# With scores
results = store.similarity_search_with_score("AI research", k=3)
for doc, score in results:
    print(f"{score:.4f}: {doc.page_content}")
```

## LlamaIndex Integration

```python
from llama_index.core import VectorStoreIndex, StorageContext
from moondb.integrations.llamaindex import MoonVectorStore

vector_store = MoonVectorStore(
    index_name="rag_docs",
    moon_url="redis://localhost:6379",
    dim=1536,
    metric="COSINE",
    expand_graph="knowledge",  # Enable graph expansion in searches
)

storage_context = StorageContext.from_defaults(vector_store=vector_store)
index = VectorStoreIndex.from_documents(documents, storage_context=storage_context)

# Query with automatic graph expansion
query_engine = index.as_query_engine()
response = query_engine.query("What is machine learning?")
```

## Type Hints

All public APIs have full type annotations. Enable strict mypy checking:

```toml
[tool.mypy]
plugins = []
strict = true
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy moondb/

# Lint
ruff check moondb/
```
