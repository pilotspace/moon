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

def _resolve_version() -> str:
    """The version this package was published as.

    Derived, never restated. A hand-maintained literal here drifted once
    already — the package shipped as 0.1.1 while this module kept answering
    "0.1.0" — and nothing could catch it, because a literal that is merely
    stale is still syntactically perfect.

    Installed (the normal case, including wheels): the installer wrote
    `pyproject.toml`'s version into distribution metadata, so that IS the
    published number.

    Not installed (a source checkout run in place, e.g. `pytest` from
    `sdk/python`): fall back to reading `pyproject.toml` itself, the same
    source of truth the installer would have used.
    """
    try:
        from importlib.metadata import version

        return version("moondb")
    except Exception:  # noqa: BLE001 - any metadata failure falls through
        pass

    try:
        import pathlib
        import sys

        if sys.version_info >= (3, 11):
            import tomllib
        else:  # pragma: no cover - Python 3.10 and older
            import tomli as tomllib

        pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
        with pyproject.open("rb") as fh:
            return str(tomllib.load(fh)["project"]["version"])
    except Exception:  # noqa: BLE001 - neither source available
        # Deliberately shaped like a version so callers that parse or compare
        # it keep working, and deliberately 0.0.0 so nothing mistakes it for a
        # real release.
        return "0.0.0.unknown"


__version__: str = _resolve_version()

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
