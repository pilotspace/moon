"""Semantic cache helpers for MoonDB (FT.CACHESEARCH).

Provides a high-level semantic caching layer that stores and retrieves
LLM responses based on query vector similarity, avoiding redundant
computation for semantically equivalent questions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from .types import CacheSearchResult, SearchResult, encode_vector

if TYPE_CHECKING:
    import redis


class CacheCommands:
    """Semantic cache command helpers.

    Accessed via ``MoonClient.cache``.

    Wraps FT.CACHESEARCH for single-RTT cache-or-search operations,
    plus helpers for storing cache entries and managing TTL.

    Example::

        from moondb import MoonClient
        client = MoonClient()

        # Store a cache entry
        client.cache.store(
            "cache:query:1", embedding, response="The answer is 42",
            index="qa_idx", ttl=3600,
        )

        # Check cache before running expensive search
        result = client.cache.lookup("qa_idx", "cache:query:", embedding)
        if result.cache_hit:
            print("Cached:", result.results[0].fields["response"])
    """

    def __init__(self, client: "redis.Redis[Any]") -> None:
        self._client = client

    def lookup(
        self,
        index: str,
        cache_prefix: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        threshold: float = 0.95,
        fallback_k: int = 10,
        k: int = 10,
        field_name: str = "vec",
    ) -> CacheSearchResult:
        """Look up a query in the semantic cache, falling back to KNN search.

        Uses FT.CACHESEARCH for a single round-trip cache probe + fallback.

        Args:
            index: Index name.
            cache_prefix: Key prefix for cache entries.
            query_vector: Query vector.
            threshold: Distance threshold for cache hit (lower = stricter for L2).
            fallback_k: Number of results on cache miss.
            k: K for the KNN query.
            field_name: Vector field name.

        Returns:
            CacheSearchResult with cache_hit flag and results.

        Example::

            result = client.cache.lookup("qa", "cache:qa:", embedding, threshold=0.1)
            if result.cache_hit:
                print("Hit:", result.results[0].fields.get("response"))
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
        return _parse_cache_result(raw)

    def store(
        self,
        key: str,
        vector: Union[Sequence[float], bytes],
        *,
        field_name: str = "vec",
        ttl: Optional[int] = None,
        **fields: str,
    ) -> bool:
        """Store a cache entry as a Redis hash with vector field.

        Args:
            key: Redis key for the cache entry (should match the cache_prefix).
            vector: The query vector to cache.
            field_name: Vector field name (must match index schema).
            ttl: Optional TTL in seconds.
            **fields: Additional hash fields (e.g. response="...", model="gpt-4").

        Returns:
            True if the entry was created.

        Example::

            client.cache.store(
                "cache:qa:abc123", embedding,
                response="The answer is 42",
                model="gpt-4",
                ttl=3600,
            )
        """
        blob = _ensure_bytes(vector)
        mapping: Dict[str, Any] = {field_name: blob}
        mapping.update(fields)

        result = self._client.hset(key, mapping=mapping)  # type: ignore[arg-type]

        if ttl is not None:
            self._client.expire(key, ttl)

        return bool(result)

    def invalidate(self, key: str) -> int:
        """Remove a cache entry.

        Args:
            key: Redis key of the cache entry to remove.

        Returns:
            1 if deleted, 0 if key did not exist.

        Example::

            client.cache.invalidate("cache:qa:abc123")
        """
        return self._client.delete(key)  # type: ignore[no-any-return]

    def invalidate_prefix(self, prefix: str, *, count: int = 1000) -> int:
        """Remove all cache entries matching a prefix using SCAN.

        Args:
            prefix: Key prefix to match (e.g. "cache:qa:").
            count: SCAN count hint per iteration.

        Returns:
            Number of keys deleted.

        Example::

            deleted = client.cache.invalidate_prefix("cache:qa:")
        """
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match=f"{prefix}*", count=count)
            if keys:
                deleted += self._client.delete(*keys)
            if cursor == 0:
                break
        return deleted


class AsyncCacheCommands:
    """Async variant of CacheCommands.

    Accessed via ``AsyncMoonClient.cache``. See ``CacheCommands`` for docs.

    Example::

        from moondb import AsyncMoonClient
        client = AsyncMoonClient()
        result = await client.cache.lookup("qa", "cache:qa:", embedding)
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    async def lookup(
        self,
        index: str,
        cache_prefix: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        threshold: float = 0.95,
        fallback_k: int = 10,
        k: int = 10,
        field_name: str = "vec",
    ) -> CacheSearchResult:
        """Cache lookup (async). See ``CacheCommands.lookup``."""
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = [
            "FT.CACHESEARCH", index, cache_prefix, query,
            "PARAMS", "2", "query_vec", blob,
            "THRESHOLD", str(threshold),
            "FALLBACK", "KNN", str(fallback_k),
        ]

        raw = await self._client.execute_command(*args)
        return _parse_cache_result(raw)

    async def store(
        self,
        key: str,
        vector: Union[Sequence[float], bytes],
        *,
        field_name: str = "vec",
        ttl: Optional[int] = None,
        **fields: str,
    ) -> bool:
        """Store cache entry (async). See ``CacheCommands.store``."""
        blob = _ensure_bytes(vector)
        mapping: Dict[str, Any] = {field_name: blob}
        mapping.update(fields)

        result = await self._client.hset(key, mapping=mapping)  # type: ignore[arg-type]

        if ttl is not None:
            await self._client.expire(key, ttl)

        return bool(result)

    async def invalidate(self, key: str) -> int:
        """Remove cache entry (async). See ``CacheCommands.invalidate``."""
        return await self._client.delete(key)  # type: ignore[no-any-return]

    async def invalidate_prefix(self, prefix: str, *, count: int = 1000) -> int:
        """Remove all entries with prefix (async). See ``CacheCommands.invalidate_prefix``."""
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=f"{prefix}*", count=count)
            if keys:
                deleted += await self._client.delete(*keys)
            if cursor == 0:
                break
        return deleted


# -- Internal helpers --


def _ensure_bytes(vector: Union[Sequence[float], bytes]) -> bytes:
    if isinstance(vector, bytes):
        return vector
    return encode_vector(vector)


def _to_str(val: Any) -> str:
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def _parse_cache_result(raw: Any) -> CacheSearchResult:
    """Parse FT.CACHESEARCH response."""
    if not isinstance(raw, list) or len(raw) < 1:
        return CacheSearchResult()

    results: List[SearchResult] = []
    is_hit = False
    i = 1
    while i < len(raw):
        key = _to_str(raw[i])
        i += 1
        if i >= len(raw):
            break

        fields_raw = raw[i]
        i += 1

        fields: Dict[str, str] = {}
        score = 0.0
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
                elif fname == "cache_hit":
                    cache_hit = fval.lower() == "true"
                    if cache_hit:
                        is_hit = True
                else:
                    fields[fname] = fval
                j += 2

        results.append(SearchResult(
            key=key, score=score, fields=fields, cache_hit=cache_hit,
        ))

    return CacheSearchResult(results=results, cache_hit=is_hit)
