"""MoonDB Python SDK -- Redis-compatible client with vector search and graph engine.

Quick start::

    from moondb import MoonClient

    client = MoonClient(host="localhost", port=6379)

    # All standard Redis commands work
    client.set("key", "value")

    # Vector search
    client.vector.create_index("my_idx", dim=384, metric="COSINE")
    client.hset("doc:1", mapping={"vec": encode_vector([0.1, 0.2, ...]), "title": "Hello"})
    results = client.vector.search("my_idx", [0.1, 0.2, ...], k=5)

    # Graph engine
    client.graph.create("social")
    client.graph.add_node("social", "Person", name="Alice")

    # Async variant
    from moondb import AsyncMoonClient
    async_client = AsyncMoonClient()
    await async_client.vector.search("my_idx", [0.1, 0.2, ...])
"""

__version__ = "0.1.0"

from .client import AsyncMoonClient, MoonClient
from .text import AsyncTextCommands, TextCommands
from .types import (
    AggregateStep,
    Avg,
    CacheSearchResult,
    Count,
    CountDistinct,
    Filter,
    GraphEdge,
    GraphNode,
    GroupBy,
    IndexInfo,
    Limit,
    Max,
    Min,
    QueryResult,
    Reducer,  # noqa: F401  -- importable type alias, not in __all__
    SearchResult,
    SortBy,
    Sum,
    TextSearchHit,
    decode_vector,
    encode_vector,
)

__all__ = [
    "MoonClient",
    "AsyncMoonClient",
    "TextCommands",
    "AsyncTextCommands",
    "SearchResult",
    "GraphNode",
    "GraphEdge",
    "IndexInfo",
    "QueryResult",
    "CacheSearchResult",
    "TextSearchHit",
    "AggregateStep",
    "GroupBy",
    "SortBy",
    "Filter",
    "Limit",
    "Count",
    "Sum",
    "Avg",
    "Min",
    "Max",
    "CountDistinct",
    "encode_vector",
    "decode_vector",
]
