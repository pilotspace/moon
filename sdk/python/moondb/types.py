"""Type definitions for MoonDB Python SDK.

Provides typed dataclasses for search results, graph nodes/edges,
index info, cache responses, and full-text aggregate pipelines.
"""

from __future__ import annotations

import struct
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:  # pragma: no cover - import only for static type-checkers
    import numpy as np  # type: ignore[import-not-found]


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
    fields: dict[str, str] = field(default_factory=dict)
    graph_hops: int | None = None
    cache_hit: bool | None = None


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
    properties: dict[str, str] = field(default_factory=dict)


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
    properties: dict[str, str] = field(default_factory=dict)


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
    fields: list[dict[str, str]] = field(default_factory=list)
    extra: dict[str, str] = field(default_factory=dict)


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

    headers: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    stats: dict[str, str] = field(default_factory=dict)


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

    results: list[SearchResult] = field(default_factory=list)
    cache_hit: bool = False


# -- Full-text search (FT.SEARCH BM25) response type --


@dataclass(frozen=True)
class TextSearchHit:
    """A single FT.SEARCH / FT.SEARCH ... HYBRID result row.

    Attributes:
        id: Redis key of the matching document.
        score: BM25 score (FT.SEARCH) or weighted RRF score (HYBRID).
            Higher is better.
        fields: Hash fields returned (minus internal score/highlight markers).
        highlights: Optional map of field -> list of highlighted fragments
            (populated only when HIGHLIGHT was requested).

    Example::

        hit = TextSearchHit(id="issue:1", score=3.14, fields={"title": "Hello"})
        print(hit.id, hit.score)
    """

    id: str
    score: float
    fields: dict[str, Any] = field(default_factory=dict)
    highlights: dict[str, list[str]] | None = None


# -- FT.AGGREGATE pipeline DSL --
#
# No APPLY in v1 (APPLY is unsupported server-side, see 153-CONTEXT D-03).


@dataclass(frozen=True)
class Count:
    """COUNT reducer for FT.AGGREGATE GROUPBY (`REDUCE COUNT 0`).

    Attributes:
        alias: Optional alias for the result column (`AS <alias>`).
    """

    alias: str | None = None


@dataclass(frozen=True)
class Sum:
    """SUM reducer for FT.AGGREGATE GROUPBY (`REDUCE SUM 1 @<field>`).

    Attributes:
        field: Field to sum (emitted as `@<field>`).
        alias: Optional alias for the result column.
    """

    field: str
    alias: str | None = None


@dataclass(frozen=True)
class Avg:
    """AVG reducer for FT.AGGREGATE GROUPBY (`REDUCE AVG 1 @<field>`).

    Attributes:
        field: Field to average.
        alias: Optional alias for the result column.
    """

    field: str
    alias: str | None = None


@dataclass(frozen=True)
class Min:
    """MIN reducer for FT.AGGREGATE GROUPBY (`REDUCE MIN 1 @<field>`).

    Attributes:
        field: Field to take the minimum of.
        alias: Optional alias for the result column.
    """

    field: str
    alias: str | None = None


@dataclass(frozen=True)
class Max:
    """MAX reducer for FT.AGGREGATE GROUPBY (`REDUCE MAX 1 @<field>`).

    Attributes:
        field: Field to take the maximum of.
        alias: Optional alias for the result column.
    """

    field: str
    alias: str | None = None


@dataclass(frozen=True)
class CountDistinct:
    """COUNT_DISTINCT reducer for FT.AGGREGATE GROUPBY.

    Emits `REDUCE COUNT_DISTINCT 1 @<field>`.

    Attributes:
        field: Field to count distinct values of.
        alias: Optional alias for the result column.
    """

    field: str
    alias: str | None = None


# Reducer union covers every supported reducer dataclass. Importable as a type
# alias; not added to `__all__` (keep the public re-export surface narrow).
Reducer = Union[Count, Sum, Avg, Min, Max, CountDistinct]


@dataclass(frozen=True)
class GroupBy:
    """GROUPBY step for FT.AGGREGATE pipeline.

    Attributes:
        fields: Field names to group by (emitted as `@<field>` each).
        reducers: Reducers applied within each group.

    Example::

        GroupBy(fields=["priority"], reducers=[Count(alias="n")])
    """

    fields: list[str]
    reducers: list[Reducer] = field(default_factory=list)


@dataclass(frozen=True)
class SortBy:
    """SORTBY step for FT.AGGREGATE pipeline.

    Attributes:
        field: Field to sort by (emitted as `@<field>`).
        direction: "ASC" or "DESC" (validated at build time, not here).

    Example::

        SortBy("n", "DESC")
    """

    field: str
    direction: str = "ASC"


@dataclass(frozen=True)
class Filter:
    """FILTER step for FT.AGGREGATE pipeline.

    Attributes:
        expr: Filter expression (passed through as a single RESP arg).

    Example::

        Filter("@status:{open}")
    """

    expr: str


@dataclass(frozen=True)
class Limit:
    """LIMIT step for FT.AGGREGATE pipeline.

    Attributes:
        offset: Row offset.
        count: Max rows to return.

    Example::

        Limit(0, 10)
    """

    offset: int
    count: int


# Union of all supported pipeline step dataclasses.
AggregateStep = Union[GroupBy, SortBy, Filter, Limit]


# -- Utility functions for vector encoding --


def encode_vector(vector: Sequence[float] | np.ndarray) -> bytes:
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
        import numpy as _np

        arr = _np.asarray(vector, dtype=_np.float32)
        return bytes(arr.tobytes())
    except ImportError:
        return struct.pack(f"<{len(vector)}f", *vector)


def decode_vector(data: bytes, dim: int) -> list[float]:
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
