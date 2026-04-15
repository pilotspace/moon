"""Session-aware search helpers for MoonDB.

Provides helpers for session-based vector search deduplication. Moon's
FT.SEARCH supports a SESSION clause that tracks previously seen results
per session key and filters them from subsequent searches.

Sessions are stored as Redis sorted sets keyed by the session key.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from .types import SearchResult, encode_vector

if TYPE_CHECKING:
    import redis


class SessionCommands:
    """Session-aware search helpers.

    Accessed via ``MoonClient.session``.

    Session-aware search uses a Redis sorted set to track which results
    a user has already seen. Subsequent searches automatically filter
    out previously returned documents.

    Example::

        from moondb import MoonClient
        client = MoonClient()

        # First search returns fresh results and records them
        results = client.session.search(
            "products", "user:alice:session",
            query_vector=[0.1, 0.2, ...],
        )

        # Second search filters out already-seen results
        more = client.session.search(
            "products", "user:alice:session",
            query_vector=[0.1, 0.2, ...],
        )

        # Reset session when done
        client.session.reset("user:alice:session")
    """

    def __init__(self, client: "redis.Redis[Any]") -> None:
        self._client = client

    def search(
        self,
        index: str,
        session_key: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        filter_expr: Optional[str] = None,
        return_fields: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        """Run a session-aware vector search.

        Results are automatically deduplicated against previous searches
        in the same session. New results are recorded in the session.

        Args:
            index: Index name.
            session_key: Redis key for the session sorted set.
            query_vector: Query vector.
            k: Number of nearest neighbors.
            field_name: Vector field name.
            filter_expr: Optional filter expression.
            return_fields: Optional field list to return.

        Returns:
            List of SearchResult not previously seen in this session.

        Example::

            results = client.session.search(
                "docs", "session:user123", [0.1, 0.2, ...], k=5
            )
        """
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = ["FT.SEARCH", index, query]

        if filter_expr:
            args.extend(["FILTER", filter_expr])

        if return_fields:
            args.extend(["RETURN", str(len(return_fields))])
            args.extend(return_fields)

        args.extend(["SESSION", session_key])
        args.extend(["PARAMS", "2", "query_vec", blob])

        raw = self._client.execute_command(*args)
        return _parse_search_results(raw)

    def reset(self, session_key: str) -> int:
        """Reset a session by deleting the session sorted set.

        Args:
            session_key: Session key to reset.

        Returns:
            1 if deleted, 0 if key did not exist.

        Example::

            client.session.reset("session:user123")
        """
        return self._client.delete(session_key)  # type: ignore[no-any-return]

    def history(self, session_key: str) -> List[str]:
        """Get all document keys previously seen in this session.

        Args:
            session_key: Session key.

        Returns:
            List of document keys (members of the session sorted set).

        Example::

            seen = client.session.history("session:user123")
            # ["doc:1", "doc:5", "doc:12"]
        """
        raw = self._client.zrange(session_key, 0, -1)
        return [x.decode("utf-8") if isinstance(x, bytes) else str(x) for x in raw]

    def set_ttl(self, session_key: str, seconds: int) -> bool:
        """Set a TTL on the session key for automatic expiration.

        Args:
            session_key: Session key.
            seconds: Time-to-live in seconds.

        Returns:
            True if TTL was set.

        Example::

            client.session.set_ttl("session:user123", 3600)  # 1 hour
        """
        return self._client.expire(session_key, seconds)  # type: ignore[no-any-return]


class AsyncSessionCommands:
    """Async variant of SessionCommands.

    Accessed via ``AsyncMoonClient.session``. See ``SessionCommands`` for docs.

    Example::

        from moondb import AsyncMoonClient
        client = AsyncMoonClient()
        results = await client.session.search(
            "products", "session:user1", [0.1, 0.2, ...]
        )
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    async def search(
        self,
        index: str,
        session_key: str,
        query_vector: Union[Sequence[float], bytes],
        *,
        k: int = 10,
        field_name: str = "vec",
        filter_expr: Optional[str] = None,
        return_fields: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        """Session-aware search (async). See ``SessionCommands.search``."""
        blob = _ensure_bytes(query_vector)
        query = f"*=>[KNN {k} @{field_name} $query_vec]"

        args: list[Any] = ["FT.SEARCH", index, query]
        if filter_expr:
            args.extend(["FILTER", filter_expr])
        if return_fields:
            args.extend(["RETURN", str(len(return_fields))])
            args.extend(return_fields)
        args.extend(["SESSION", session_key])
        args.extend(["PARAMS", "2", "query_vec", blob])

        raw = await self._client.execute_command(*args)
        return _parse_search_results(raw)

    async def reset(self, session_key: str) -> int:
        """Reset session (async). See ``SessionCommands.reset``."""
        return await self._client.delete(session_key)  # type: ignore[no-any-return]

    async def history(self, session_key: str) -> List[str]:
        """Get session history (async). See ``SessionCommands.history``."""
        raw = await self._client.zrange(session_key, 0, -1)
        return [x.decode("utf-8") if isinstance(x, bytes) else str(x) for x in raw]

    async def set_ttl(self, session_key: str, seconds: int) -> bool:
        """Set session TTL (async). See ``SessionCommands.set_ttl``."""
        return await self._client.expire(session_key, seconds)  # type: ignore[no-any-return]


# -- Internal helpers --


def _ensure_bytes(vector: Union[Sequence[float], bytes]) -> bytes:
    if isinstance(vector, bytes):
        return vector
    return encode_vector(vector)


def _to_str(val: Any) -> str:
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def _parse_search_results(raw: Any) -> List[SearchResult]:
    """Parse FT.SEARCH response (same format as vector.py parser)."""
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
                else:
                    fields[fname] = fval
                j += 2

        results.append(SearchResult(key=key, score=score, fields=fields))

    return results
