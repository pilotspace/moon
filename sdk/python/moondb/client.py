"""MoonDB client -- extends redis-py with Moon-specific command helpers.

``MoonClient`` inherits from ``redis.Redis`` and adds typed accessors for
vector search (FT.*), graph engine (GRAPH.*), session-aware search, and
semantic caching. All standard Redis commands remain available.
"""

from __future__ import annotations

from typing import Any

import redis
import redis.asyncio

from .cache import AsyncCacheCommands, CacheCommands
from .graph import AsyncGraphCommands, GraphCommands
from .session import AsyncSessionCommands, SessionCommands
from .text import AsyncTextCommands, TextCommands
from .vector import AsyncVectorCommands, VectorCommands


class MoonClient(redis.Redis):  # type: ignore[type-arg]
    """Redis-compatible client with Moon-specific extensions.

    Inherits all ``redis.Redis`` commands and adds typed helpers for
    Moon's vector search, graph engine, session-aware search, and
    semantic caching features.

    Args:
        host: Moon server hostname (default "localhost").
        port: Moon server port (default 6379).
        db: Database number (default 0).
        password: Optional authentication password.
        decode_responses: Whether to decode bytes to str (default False
            for binary vector compatibility).
        **kwargs: Additional arguments passed to ``redis.Redis``.

    Example::

        from moondb import MoonClient

        client = MoonClient(host="localhost", port=6379)

        # Standard Redis commands work
        client.set("key", "value")
        client.get("key")

        # Moon vector search
        client.vector.create_index("my_idx", dim=384, metric="COSINE")
        results = client.vector.search("my_idx", [0.1, 0.2, ...])

        # Moon graph engine
        client.graph.create("social")
        client.graph.add_node("social", "Person", name="Alice")

        # Session-aware search
        results = client.session.search("idx", "session:user1", [0.1, ...])

        # Semantic cache
        result = client.cache.lookup("idx", "cache:", embedding)
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        decode_responses: bool = False,
        **kwargs: Any,  # noqa: ANN401 -- forwarded to redis.Redis/redis.asyncio.Redis
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses,
            **kwargs,
        )
        self._vector = VectorCommands(self)
        self._graph = GraphCommands(self)
        self._session = SessionCommands(self)
        self._cache = CacheCommands(self)
        self._text = TextCommands(self)

    @property
    def vector(self) -> VectorCommands:
        """Vector search commands (FT.*)."""
        return self._vector

    @property
    def graph(self) -> GraphCommands:
        """Graph engine commands (GRAPH.*)."""
        return self._graph

    @property
    def session(self) -> SessionCommands:
        """Session-aware search commands."""
        return self._session

    @property
    def cache(self) -> CacheCommands:
        """Semantic cache commands (FT.CACHESEARCH)."""
        return self._cache

    @property
    def text(self) -> TextCommands:
        """Full-text search commands (FT.SEARCH BM25 + FT.AGGREGATE + HYBRID)."""
        return self._text

    def moon_info(self) -> dict[str, Any]:
        """Get Moon server info (parsed INFO output).

        Returns:
            Dictionary of server info sections and key-value pairs.

        Example::

            info = client.moon_info()
            print(info.get("server", {}).get("moon_version"))
        """
        return self.info()  # type: ignore[no-any-return]


class AsyncMoonClient(redis.asyncio.Redis):  # type: ignore[type-arg]
    """Async Redis-compatible client with Moon-specific extensions.

    Inherits all ``redis.asyncio.Redis`` commands and adds typed helpers
    for Moon's vector search, graph engine, session-aware search, and
    semantic caching features.

    Args:
        host: Moon server hostname (default "localhost").
        port: Moon server port (default 6379).
        db: Database number (default 0).
        password: Optional authentication password.
        decode_responses: Whether to decode bytes to str (default False).
        **kwargs: Additional arguments passed to ``redis.asyncio.Redis``.

    Example::

        from moondb import AsyncMoonClient

        client = AsyncMoonClient(host="localhost", port=6379)

        # Standard async Redis commands
        await client.set("key", "value")
        await client.get("key")

        # Async Moon vector search
        await client.vector.create_index("my_idx", dim=384, metric="COSINE")
        results = await client.vector.search("my_idx", [0.1, 0.2, ...])
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        decode_responses: bool = False,
        **kwargs: Any,  # noqa: ANN401 -- forwarded to redis.Redis/redis.asyncio.Redis
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses,
            **kwargs,
        )
        self._vector = AsyncVectorCommands(self)
        self._graph = AsyncGraphCommands(self)
        self._session = AsyncSessionCommands(self)
        self._cache = AsyncCacheCommands(self)
        self._text = AsyncTextCommands(self)

    @property
    def vector(self) -> AsyncVectorCommands:
        """Vector search commands (FT.*, async)."""
        return self._vector

    @property
    def graph(self) -> AsyncGraphCommands:
        """Graph engine commands (GRAPH.*, async)."""
        return self._graph

    @property
    def session(self) -> AsyncSessionCommands:
        """Session-aware search commands (async)."""
        return self._session

    @property
    def cache(self) -> AsyncCacheCommands:
        """Semantic cache commands (FT.CACHESEARCH, async)."""
        return self._cache

    @property
    def text(self) -> AsyncTextCommands:
        """Full-text search commands (FT.SEARCH BM25 + FT.AGGREGATE + HYBRID, async)."""
        return self._text

    async def moon_info(self) -> dict[str, Any]:
        """Get Moon server info (async).

        Returns:
            Dictionary of server info.
        """
        return await self.info()  # type: ignore[no-any-return]
