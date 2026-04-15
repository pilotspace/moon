"""Type definitions for MoonDB Python SDK.

Provides typed dataclasses for search results, graph nodes/edges,
index info, and cache responses.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union


@dataclass(frozen=True)
class SearchResult:
    """A single vector search result.

    Attributes:
        key: The Redis key of the matching document.
        score: Distance/similarity score (lower is closer for L2/COSINE).
        fields: Hash fields returned with the result.
        graph_hops: Number of graph hops if result came from EXPAND (None otherwise).
        cache_hit: Whether this result came from semantic cache (None if not a cache search).

    Example::

        result = SearchResult(key="doc:1", score=0.123, fields={"title": "hello"})
        print(result.key, result.score)
    """

    key: str
    score: float
    fields: Dict[str, str] = field(default_factory=dict)
    graph_hops: Optional[int] = None
    cache_hit: Optional[bool] = None


@dataclass(frozen=True)
class GraphNode:
    """A node in a Moon graph.

    Attributes:
        node_id: Numeric node ID assigned by the server.
        label: Node label (e.g. "Person", "Document").
        properties: Key-value properties stored on the node.

    Example::

        node = GraphNode(node_id=1, label="Person", properties={"name": "Alice"})
    """

    node_id: int
    label: str
    properties: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class GraphEdge:
    """An edge in a Moon graph.

    Attributes:
        src_id: Source node ID.
        dst_id: Destination node ID.
        edge_type: Relationship type (e.g. "KNOWS", "LINKS_TO").
        weight: Edge weight (default 1.0).
        properties: Key-value properties stored on the edge.

    Example::

        edge = GraphEdge(src_id=1, dst_id=2, edge_type="KNOWS", weight=1.0)
    """

    src_id: int
    dst_id: int
    edge_type: str
    weight: float = 1.0
    properties: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class IndexInfo:
    """Metadata about a vector search index (from FT.INFO).

    Attributes:
        name: Index name.
        num_docs: Total indexed documents across all segments.
        dimension: Vector dimension.
        distance_metric: Distance metric (L2, COSINE, IP).
        fields: Schema field definitions.
        extra: Any additional key-value pairs from the server response.

    Example::

        info = client.vector.index_info("my_index")
        print(f"Index {info.name} has {info.num_docs} docs, dim={info.dimension}")
    """

    name: str
    num_docs: int = 0
    dimension: int = 0
    distance_metric: str = "L2"
    fields: List[Dict[str, str]] = field(default_factory=list)
    extra: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryResult:
    """Result from a GRAPH.QUERY or GRAPH.RO_QUERY command.

    Attributes:
        headers: Column names returned by the query.
        rows: List of result rows, each row is a list of values.
        stats: Execution statistics (e.g. nodes created, query time).

    Example::

        result = client.graph.query("social", "MATCH (n:Person) RETURN n.name")
        for row in result.rows:
            print(row)
    """

    headers: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)
    stats: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CacheSearchResult:
    """Result from FT.CACHESEARCH.

    Attributes:
        results: Search results (single result on cache hit, multiple on miss).
        cache_hit: True if the query was served from semantic cache.

    Example::

        cr = client.vector.cache_search("idx", "cache:", query_vec, threshold=0.95)
        if cr.cache_hit:
            print("Served from cache!")
    """

    results: List[SearchResult] = field(default_factory=list)
    cache_hit: bool = False


# -- Utility functions for vector encoding --


def encode_vector(vector: Union[Sequence[float], "numpy.ndarray"]) -> bytes:  # type: ignore[name-defined]
    """Encode a float vector to bytes (little-endian float32).

    Args:
        vector: List of floats or numpy array.

    Returns:
        Packed bytes suitable for Moon vector parameters.

    Example::

        blob = encode_vector([1.0, 0.0, 0.5])
        # Use as PARAMS value in FT.SEARCH
    """
    try:
        import numpy as np

        arr = np.asarray(vector, dtype=np.float32)
        return arr.tobytes()
    except ImportError:
        return struct.pack(f"<{len(vector)}f", *vector)


def decode_vector(data: bytes, dim: int) -> List[float]:
    """Decode bytes back to a list of floats (little-endian float32).

    Args:
        data: Packed float32 bytes.
        dim: Expected number of dimensions.

    Returns:
        List of float values.

    Raises:
        ValueError: If data length does not match expected dimension.

    Example::

        vec = decode_vector(blob, dim=384)
        print(len(vec))  # 384
    """
    expected = dim * 4
    if len(data) != expected:
        raise ValueError(f"Expected {expected} bytes for {dim} dimensions, got {len(data)}")
    return list(struct.unpack(f"<{dim}f", data))
