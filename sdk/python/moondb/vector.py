"""Vector search helpers for MoonDB (FT.* commands).

Wraps FT.CREATE, FT.SEARCH, FT.DROPINDEX, FT.INFO, FT.COMPACT, FT._LIST,
FT.CACHESEARCH, FT.EXPAND, FT.NAVIGATE, FT.RECOMMEND as typed Python methods.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from .types import (
    CacheSearchResult,
    IndexInfo,
    SearchResult,
    encode_vector,
)

if TYPE_CHECKING:
    import redis


class VectorCommands:
    """Vector search command helpers.

    Not instantiated directly -- accessed via ``MoonClient.vector``.

    All methods use ``execute_command()`` on the underlying Redis client to send
    raw RESP commands to the Moon server.

    Example::

        from moondb import MoonClient
        client = MoonClient()
        client.vector.create_index(
            "my_idx", prefix="doc:", dim=384, metric="COSINE"
        )
    """

    def __init__(self, client: "redis.Redis[Any]") -> None:
        self._client = client

    def create_index(
        self,
        name: str,
        *,
        prefix: str = "doc:",
        field_name: str = "vec",
        algorithm: str = "HNSW",
        dim: int = 384,
        dtype: str = "FLOAT32",
        metric: str = "L2",
        m: int = 16,
        ef_construction: int = 200,
        ef_runtime: Optional[int] = None,
        compact_threshold: Optional[int] = None,
        extra_schema: Optional[List[str]] = None,
    ) -> str:
        """Create a vector search index.

        Args:
            name: Index name.
            prefix: Key prefix filter (e.g. "doc:").
            field_name: Name of the vector field in the hash.
            algorithm: Index algorithm (HNSW or FLAT).
            dim: Vector dimension.
            dtype: Data type (FLOAT32, FLOAT64).
            metric: Distance metric (L2, COSINE, IP).
            m: HNSW M parameter (max edges per node).
            ef_construction: HNSW ef_construction parameter.
            ef_runtime: Optional per-index EF_RUNTIME override.
            compact_threshold: Optional per-index COMPACT_THRESHOLD override.
            extra_schema: Additional schema fields (e.g. ["title", "TEXT", "tag", "TAG"]).

        Returns:
            "OK" on success.

        Raises:
            redis.ResponseError: If index already exists or invalid parameters.

        Example::

            client.vector.create_index(
                "products", prefix="product:", dim=768,
                metric="COSINE", ef_construction=400,
            )
        """
        args: list[Any] = [
            "FT.CREATE", name,
            "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
        ]

        # Add extra schema fields before vector field
        if extra_schema:
            args.extend(extra_schema)

        # Vector field definition
        algo_params: list[Any] = [
            "TYPE", dtype,
            "DIM", str(dim),
            "DISTANCE_METRIC", metric,
            "M", str(m),
            "EF_CONSTRUCTION", str(ef_construction),
        ]
        if ef_runtime is not None:
            algo_params.extend(["EF_RUNTIME", str(ef_runtime)])
        if compact_threshold is not None:
            algo_params.extend(["COMPACT_THRESHOLD", str(compact_threshold)])

        args.extend([field_name, "VECTOR", algorithm, str(len(algo_params))])
        args.extend(algo_params)

        return self._client.execute_command(*args)  # type: ignore[no-any-return]

    def drop_index(self, name: str) -> str:
        """Drop a vector search index.

        Args:
            name: Index name to drop.

        Returns:
            "OK" on success.

        Example::

            client.vector.drop_index("my_idx")
        """
        return self._client.execute_command("FT.DROPINDEX", name)  # type: ignore[no-any-return]

    def index_info(self, name: str) -> IndexInfo:
        """Get index metadata.

        Args:
            name: Index name.

        Returns:
            IndexInfo with dimension, doc count, metric, and fields.

        Example::

            info = client.vector.index_info("my_idx")
            print(info.num_docs, info.dimension)
        """
        raw = self._client.execute_command("FT.INFO", name)
        return _parse_index_info(name, raw)

    def list_indexes(self) -> List[str]:
        """List all vector search indexes.

        Returns:
            List of index names.

        Example::

            indexes = client.vector.list_indexes()
            # ["idx1", "idx2"]
        """
        raw = self._client.execute_command("FT._LIST")
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return []

    def compact(self, name: str) -> str:
        """Trigger compaction on an index (flush mutable segment to immutable HNSW).

        Args:
            name: Index name.

        Returns:
            Server response string.

        Example::

            client.vector.compact("my_idx")
        """
        return self._client.execute_command("FT.COMPACT", name)  # type: ignore[no-any-return]

    def search(
        self,
        index: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        filter_expr: Optional[str] = None,
        return_fields: Optional[List[str]] = None,
        session_key: Optional[str] = None,
        expand_graph: Optional[str] = None,
        expand_depth: int = 2,
    ) -> List[SearchResult]:
        """Run a KNN vector search.

        Args:
            index: Index name.
            query_vector: Query vector as list of floats or pre-encoded bytes.
            k: Number of nearest neighbors to return.
            field_name: Vector field name in the schema.
            filter_expr: Optional filter expression (e.g. "@tag:{electronics}").
            return_fields: Fields to include in results (None = all).
            session_key: Optional session key for session-aware deduplication.
            expand_graph: If set, name of graph to expand results through (EXPAND GRAPH).
            expand_depth: Depth of graph expansion (default 2, used with expand_graph).

        Returns:
            List of SearchResult ordered by score (ascending = closest).

        Example::

            results = client.vector.search(
                "products", [0.1, 0.2, ...], k=5, field_name="embedding"
            )
            for r in results:
                print(r.key, r.score, r.fields.get("title"))
        """
        blob = _ensure_bytes(query_vector)

        # Build KNN query string
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = ["FT.SEARCH", index, query]

        # Filter
        if filter_expr:
            args.extend(["FILTER", filter_expr])

        # Return fields
        if return_fields:
            args.extend(["RETURN", str(len(return_fields))])
            args.extend(return_fields)

        # Session-aware search
        if session_key:
            args.extend(["SESSION", session_key])

        # Graph expansion
        if expand_graph:
            args.extend(["EXPAND", "GRAPH", expand_graph, "DEPTH", str(expand_depth)])

        # PARAMS
        args.extend(["PARAMS", "2", "query_vec", blob])

        raw = self._client.execute_command(*args)
        return _parse_search_results(raw)

    def cache_search(
        self,
        index: str,
        cache_prefix: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        threshold: float = 0.95,
        fallback_k: int = 10,
    ) -> CacheSearchResult:
        """Semantic cache search -- check cache first, fall back to KNN.

        Args:
            index: Index name.
            cache_prefix: Key prefix for cache entries (e.g. "cache:query:").
            query_vector: Query vector.
            k: KNN k for the query string.
            field_name: Vector field name.
            threshold: Distance threshold for cache hit (lower = stricter).
            fallback_k: Number of results on cache miss.

        Returns:
            CacheSearchResult with cache_hit flag and results.

        Example::

            cr = client.vector.cache_search("idx", "cache:", embedding, threshold=0.9)
            if cr.cache_hit:
                print("Cache hit!", cr.results[0].key)
        """
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = [
            "FT.CACHESEARCH", index, cache_prefix, query,
            "PARAMS", "2", "query_vec", blob,
            "THRESHOLD", str(threshold),
            "FALLBACK", "KNN", str(fallback_k),
        ]

        raw = self._client.execute_command(*args)
        return _parse_cache_search_results(raw)

    def recommend(
        self,
        index: str,
        positive_keys: List[str],
        *,
        negative_keys: Optional[List[str]] = None,
        k: int = 10,
        field_name: Optional[str] = None,
    ) -> List[SearchResult]:
        """Recommend items similar to positive examples, dissimilar to negatives.

        Uses FT.RECOMMEND to compute a centroid from positive example vectors,
        optionally adjusted by negative examples, then runs KNN.

        Args:
            index: Index name.
            positive_keys: Redis keys of positive example items.
            negative_keys: Redis keys of negative example items (optional).
            k: Number of recommendations to return.
            field_name: Vector field name (optional, uses index default).

        Returns:
            List of SearchResult (excluding the example keys).

        Example::

            recs = client.vector.recommend(
                "products",
                positive_keys=["product:1", "product:5"],
                negative_keys=["product:99"],
                k=5,
            )
        """
        args: list[Any] = ["FT.RECOMMEND", index, "POSITIVE"]
        args.extend(positive_keys)

        if negative_keys:
            args.append("NEGATIVE")
            args.extend(negative_keys)

        args.extend(["K", str(k)])

        if field_name:
            args.extend(["FIELD", field_name])

        raw = self._client.execute_command(*args)
        return _parse_search_results(raw)

    def navigate(
        self,
        index: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        hops: int = 2,
        hop_penalty: float = 0.1,
    ) -> List[SearchResult]:
        """Multi-hop knowledge navigation combining vector search and graph traversal.

        Uses FT.NAVIGATE to find initial KNN results, expand through the graph,
        and re-rank with hop-depth penalty.

        Args:
            index: Index name.
            query_vector: Query vector.
            k: Number of results.
            field_name: Vector field name.
            hops: Number of graph hops to explore.
            hop_penalty: Score penalty per hop (higher = prefer direct matches).

        Returns:
            List of SearchResult, re-ranked by combined score.

        Example::

            results = client.vector.navigate(
                "knowledge", embedding, k=10, hops=3, hop_penalty=0.05
            )
        """
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $vec_param]"

        args: list[Any] = [
            "FT.NAVIGATE", index, query,
            "HOPS", str(hops),
            "HOP_PENALTY", str(hop_penalty),
            "PARAMS", "2", "vec_param", blob,
        ]

        raw = self._client.execute_command(*args)
        return _parse_search_results(raw)

    def expand(
        self,
        index: str,
        keys: List[str],
        *,
        depth: int = 2,
        graph_name: Optional[str] = None,
    ) -> List[SearchResult]:
        """Expand from seed keys through the graph.

        Uses FT.EXPAND for standalone graph expansion from explicit Redis keys.

        Args:
            index: Index name (reserved for consistency).
            keys: Seed Redis keys to expand from.
            depth: Number of graph hops.
            graph_name: Optional named graph (auto-detected if omitted).

        Returns:
            List of SearchResult with graph_hops metadata.

        Example::

            expanded = client.vector.expand("idx", ["doc:1", "doc:5"], depth=3)
        """
        args: list[Any] = ["FT.EXPAND", index]
        args.extend(keys)
        args.extend(["DEPTH", str(depth)])

        if graph_name:
            args.extend(["GRAPH", graph_name])

        raw = self._client.execute_command(*args)
        return _parse_search_results(raw)


class AsyncVectorCommands:
    """Async variant of VectorCommands for ``redis.asyncio.Redis``.

    Provides the same API as ``VectorCommands`` but all methods are async.

    Example::

        from moondb import AsyncMoonClient
        client = AsyncMoonClient()
        await client.vector.create_index("idx", dim=384, metric="COSINE")
        results = await client.vector.search("idx", [0.1, 0.2, ...])
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    async def create_index(
        self,
        name: str,
        *,
        prefix: str = "doc:",
        field_name: str = "vec",
        algorithm: str = "HNSW",
        dim: int = 384,
        dtype: str = "FLOAT32",
        metric: str = "L2",
        m: int = 16,
        ef_construction: int = 200,
        ef_runtime: Optional[int] = None,
        compact_threshold: Optional[int] = None,
        extra_schema: Optional[List[str]] = None,
    ) -> str:
        """Create a vector search index (async). See ``VectorCommands.create_index``."""
        args: list[Any] = [
            "FT.CREATE", name,
            "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
        ]
        if extra_schema:
            args.extend(extra_schema)

        algo_params: list[Any] = [
            "TYPE", dtype,
            "DIM", str(dim),
            "DISTANCE_METRIC", metric,
            "M", str(m),
            "EF_CONSTRUCTION", str(ef_construction),
        ]
        if ef_runtime is not None:
            algo_params.extend(["EF_RUNTIME", str(ef_runtime)])
        if compact_threshold is not None:
            algo_params.extend(["COMPACT_THRESHOLD", str(compact_threshold)])

        args.extend([field_name, "VECTOR", algorithm, str(len(algo_params))])
        args.extend(algo_params)

        return await self._client.execute_command(*args)  # type: ignore[no-any-return]

    async def drop_index(self, name: str) -> str:
        """Drop a vector search index (async). See ``VectorCommands.drop_index``."""
        return await self._client.execute_command("FT.DROPINDEX", name)  # type: ignore[no-any-return]

    async def index_info(self, name: str) -> IndexInfo:
        """Get index metadata (async). See ``VectorCommands.index_info``."""
        raw = await self._client.execute_command("FT.INFO", name)
        return _parse_index_info(name, raw)

    async def list_indexes(self) -> List[str]:
        """List all indexes (async). See ``VectorCommands.list_indexes``."""
        raw = await self._client.execute_command("FT._LIST")
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return []

    async def compact(self, name: str) -> str:
        """Trigger compaction (async). See ``VectorCommands.compact``."""
        return await self._client.execute_command("FT.COMPACT", name)  # type: ignore[no-any-return]

    async def search(
        self,
        index: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        filter_expr: Optional[str] = None,
        return_fields: Optional[List[str]] = None,
        session_key: Optional[str] = None,
        expand_graph: Optional[str] = None,
        expand_depth: int = 2,
    ) -> List[SearchResult]:
        """Run a KNN vector search (async). See ``VectorCommands.search``."""
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = ["FT.SEARCH", index, query]
        if filter_expr:
            args.extend(["FILTER", filter_expr])
        if return_fields:
            args.extend(["RETURN", str(len(return_fields))])
            args.extend(return_fields)
        if session_key:
            args.extend(["SESSION", session_key])
        if expand_graph:
            args.extend(["EXPAND", "GRAPH", expand_graph, "DEPTH", str(expand_depth)])
        args.extend(["PARAMS", "2", "query_vec", blob])

        raw = await self._client.execute_command(*args)
        return _parse_search_results(raw)

    async def cache_search(
        self,
        index: str,
        cache_prefix: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        threshold: float = 0.95,
        fallback_k: int = 10,
    ) -> CacheSearchResult:
        """Semantic cache search (async). See ``VectorCommands.cache_search``."""
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = [
            "FT.CACHESEARCH", index, cache_prefix, query,
            "PARAMS", "2", "query_vec", blob,
            "THRESHOLD", str(threshold),
            "FALLBACK", "KNN", str(fallback_k),
        ]

        raw = await self._client.execute_command(*args)
        return _parse_cache_search_results(raw)

    async def recommend(
        self,
        index: str,
        positive_keys: List[str],
        *,
        negative_keys: Optional[List[str]] = None,
        k: int = 10,
        field_name: Optional[str] = None,
    ) -> List[SearchResult]:
        """Recommend items (async). See ``VectorCommands.recommend``."""
        args: list[Any] = ["FT.RECOMMEND", index, "POSITIVE"]
        args.extend(positive_keys)
        if negative_keys:
            args.append("NEGATIVE")
            args.extend(negative_keys)
        args.extend(["K", str(k)])
        if field_name:
            args.extend(["FIELD", field_name])

        raw = await self._client.execute_command(*args)
        return _parse_search_results(raw)

    async def navigate(
        self,
        index: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        hops: int = 2,
        hop_penalty: float = 0.1,
    ) -> List[SearchResult]:
        """Multi-hop navigation (async). See ``VectorCommands.navigate``."""
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $vec_param]"

        args: list[Any] = [
            "FT.NAVIGATE", index, query,
            "HOPS", str(hops),
            "HOP_PENALTY", str(hop_penalty),
            "PARAMS", "2", "vec_param", blob,
        ]

        raw = await self._client.execute_command(*args)
        return _parse_search_results(raw)

    async def expand(
        self,
        index: str,
        keys: List[str],
        *,
        depth: int = 2,
        graph_name: Optional[str] = None,
    ) -> List[SearchResult]:
        """Expand from seed keys (async). See ``VectorCommands.expand``."""
        args: list[Any] = ["FT.EXPAND", index]
        args.extend(keys)
        args.extend(["DEPTH", str(depth)])
        if graph_name:
            args.extend(["GRAPH", graph_name])

        raw = await self._client.execute_command(*args)
        return _parse_search_results(raw)


# -- Response parsing helpers --


def _to_str(val: Any) -> str:
    """Convert bytes or string to str."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def _ensure_bytes(vector: Union[Sequence[float], bytes]) -> bytes:
    """Convert vector to bytes if not already."""
    if isinstance(vector, bytes):
        return vector
    return encode_vector(vector)


def _parse_search_results(raw: Any) -> List[SearchResult]:
    """Parse FT.SEARCH / FT.RECOMMEND / FT.NAVIGATE / FT.EXPAND response.

    Moon returns: [total_count, key1, [field1, val1, ...], key2, [...], ...]
    """
    if not isinstance(raw, list) or len(raw) < 1:
        return []

    results: List[SearchResult] = []
    i = 1  # skip total count
    while i < len(raw):
        key = _to_str(raw[i])
        i += 1
        if i >= len(raw):
            break

        fields_raw = raw[i]
        i += 1

        fields: Dict[str, str] = {}
        score = 0.0
        graph_hops: Optional[int] = None
        cache_hit: Optional[bool] = None

        if isinstance(fields_raw, list):
            j = 0
            while j + 1 < len(fields_raw):
                fname = _to_str(fields_raw[j])
                fval = _to_str(fields_raw[j + 1])
                if fname == "__vec_score":
                    try:
                        score = float(fval)
                    except (ValueError, TypeError):
                        pass
                elif fname == "__graph_hops":
                    try:
                        graph_hops = int(fval)
                    except (ValueError, TypeError):
                        pass
                elif fname == "cache_hit":
                    cache_hit = fval.lower() == "true"
                else:
                    fields[fname] = fval
                j += 2

        results.append(SearchResult(
            key=key,
            score=score,
            fields=fields,
            graph_hops=graph_hops,
            cache_hit=cache_hit,
        ))

    return results


def _parse_cache_search_results(raw: Any) -> CacheSearchResult:
    """Parse FT.CACHESEARCH response."""
    results = _parse_search_results(raw)
    is_hit = any(r.cache_hit is True for r in results)
    return CacheSearchResult(results=results, cache_hit=is_hit)


def _parse_index_info(name: str, raw: Any) -> IndexInfo:
    """Parse FT.INFO response into IndexInfo.

    FT.INFO returns a flat list of alternating keys and values.
    """
    if not isinstance(raw, list):
        return IndexInfo(name=name)

    info: Dict[str, Any] = {}
    i = 0
    while i + 1 < len(raw):
        key = _to_str(raw[i])
        val = raw[i + 1]
        info[key] = val
        i += 2

    num_docs = 0
    dimension = 0
    metric = "L2"
    extra: Dict[str, str] = {}

    for k, v in info.items():
        if k == "num_docs":
            try:
                num_docs = int(v)
            except (ValueError, TypeError):
                pass
        elif k == "dimension" or k == "dim":
            try:
                dimension = int(v)
            except (ValueError, TypeError):
                pass
        elif k == "distance_metric":
            metric = _to_str(v)
        elif not isinstance(v, (list, dict)):
            extra[k] = _to_str(v)

    return IndexInfo(
        name=name,
        num_docs=num_docs,
        dimension=dimension,
        distance_metric=metric,
        extra=extra,
    )
