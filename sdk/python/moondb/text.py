"""Full-text search helpers for MoonDB (FT.SEARCH BM25 / FT.AGGREGATE / HYBRID).

Wraps Moon's full-text engine (Phases 149-152) as a typed Python surface:

- ``FT.CREATE`` with TEXT / TAG / NUMERIC fields
- ``FT.SEARCH`` with BM25 scoring, highlighting, return-fields, and language
  hints
- ``FT.AGGREGATE`` with a GROUPBY / SORTBY / FILTER / LIMIT pipeline DSL
- ``FT.SEARCH ... HYBRID VECTOR ... [SPARSE ...] FUSION RRF`` for fused
  dense+sparse+BM25 retrieval (v0.1.7 Phase 152)

All builders are pure functions — they return the RESP argument list without
touching the network — so they are trivially unit-testable via mock clients.

Example::

    from moondb import MoonClient
    client = MoonClient()

    client.text.create_text_index(
        "issues",
        [
            ("title", "TEXT", {"WEIGHT": 2.0}),
            ("body", "TEXT", {}),
            ("status", "TAG", {}),
            ("priority", "TAG", {"SORTABLE": True}),
        ],
        prefix="issue:",
    )

    hits = client.text.text_search("issues", "crash startup", limit=5)
    for h in hits:
        print(h.id, h.score, h.fields.get("title"))
"""

from __future__ import annotations

import contextlib
import math
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from .types import (
    AggregateStep,
    Avg,
    Count,
    CountDistinct,
    Filter,
    GroupBy,
    Limit,
    Max,
    Min,
    Reducer,
    SortBy,
    Sum,
    TextSearchHit,
    encode_vector,
)

if TYPE_CHECKING:  # pragma: no cover - static type-checkers only
    import redis


# -- Internal helpers ---------------------------------------------------------


def _to_str(val: Any) -> str:  # noqa: ANN401 -- RESP boundary: server values are dynamic
    """Convert bytes or string to str (UTF-8, replace errors)."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def _ensure_bytes(vector: Sequence[float] | bytes) -> bytes:
    """Convert vector to bytes if not already."""
    if isinstance(vector, bytes):
        return vector
    return encode_vector(vector)


def _maybe_cast_number(val: Any) -> Any:  # noqa: ANN401 -- RESP boundary: dynamic
    """Best-effort numeric coercion for FT.AGGREGATE row values.

    Redis returns all aggregate values as bulk strings. Try int first
    (no fractional part → keep int), then float, else the original str.
    """
    s = _to_str(val)
    if not s:
        return s
    # Try int (fast-path: no '.' / 'e' / 'E')
    if "." not in s and "e" not in s and "E" not in s:
        with contextlib.suppress(ValueError):
            return int(s)
    try:
        return float(s)
    except ValueError:
        return s


# -- Command argument builders -----------------------------------------------
#
# Each builder is a pure function returning the full RESP argument list for
# `execute_command(*args)`. Keeping them pure (no client coupling) makes the
# builders trivially unit-testable and lets the sync + async classes share
# a single code path.


# Whitelist of bare-flag schema options (emitted as a single token when truthy).
_SCHEMA_FLAG_OPTS: tuple[str, ...] = ("SORTABLE", "NOSTEM", "NOINDEX")

# Whitelist of key/value schema options (emitted as `KEY value` pair).
_SCHEMA_PARAM_OPTS: tuple[str, ...] = ("WEIGHT", "SEPARATOR", "PHONETIC")


def _build_create_index_args(
    name: str,
    schema: Sequence[tuple[str, str, dict[str, Any]]],
    prefix: str | None,
    on: str,
) -> list[Any]:
    """Build the FT.CREATE argument list.

    Emits a deterministic order: ``FT.CREATE <name> ON <on> [PREFIX 1 <p>]
    SCHEMA <field> <type> [opts...] ...``.

    Only whitelisted opt keys (:data:`_SCHEMA_FLAG_OPTS`,
    :data:`_SCHEMA_PARAM_OPTS`) are forwarded. Unknown opts are silently
    dropped (see threat_model T-153-01-05).
    """
    args: list[Any] = ["FT.CREATE", name, "ON", on]
    if prefix is not None:
        args.extend(["PREFIX", "1", prefix])
    args.append("SCHEMA")

    for field_name, field_type, opts in schema:
        args.append(field_name)
        args.append(field_type)
        for raw_key, raw_val in opts.items():
            key = str(raw_key).upper()
            if key in _SCHEMA_FLAG_OPTS:
                if raw_val:
                    args.append(key)
                # Truthy → flag appended; falsy → skip (no-op)
            elif key in _SCHEMA_PARAM_OPTS:
                args.append(key)
                args.append(str(raw_val))
            # Unknown opts are silently ignored (whitelist-only emission).

    return args


def _build_text_search_args(
    index: str,
    query: str,
    *,
    limit: int,
    offset: int,
    return_fields: list[str] | None,
    highlight: bool,
    highlight_fields: list[str] | None,
    lang: str | None,
) -> list[Any]:
    """Build the FT.SEARCH argument list for a BM25 text search.

    Empty ``query`` is coerced to ``"*"`` (match-all).
    """
    q = query if query else "*"
    args: list[Any] = ["FT.SEARCH", index, q, "LIMIT", str(offset), str(limit)]

    if return_fields:
        args.extend(["RETURN", str(len(return_fields)), *return_fields])

    if highlight:
        args.append("HIGHLIGHT")
        if highlight_fields:
            args.extend(["FIELDS", str(len(highlight_fields)), *highlight_fields])

    if lang:
        args.extend(["LANGUAGE", lang])

    return args


def _reducer_args(reducer: Reducer) -> list[Any]:
    """Emit the REDUCE block for a single reducer dataclass."""
    if isinstance(reducer, Count):
        block: list[Any] = ["REDUCE", "COUNT", "0"]
        if reducer.alias is not None:
            block.extend(["AS", reducer.alias])
        return block

    if isinstance(reducer, CountDistinct):
        block = ["REDUCE", "COUNT_DISTINCT", "1", f"@{reducer.field}"]
        if reducer.alias is not None:
            block.extend(["AS", reducer.alias])
        return block

    # Sum / Avg / Min / Max share the same shape: REDUCE <OP> 1 @<field>
    name_map: dict[type, str] = {Sum: "SUM", Avg: "AVG", Min: "MIN", Max: "MAX"}
    op = name_map.get(type(reducer))
    if op is None:  # Defensive: new reducer types without a handler
        raise TypeError(f"unsupported reducer type: {type(reducer).__name__}")
    # At this point `reducer` is Sum|Avg|Min|Max (each has .field / .alias).
    block = ["REDUCE", op, "1", f"@{reducer.field}"]
    if reducer.alias is not None:
        block.extend(["AS", reducer.alias])
    return block


def _build_aggregate_args(
    index: str,
    query: str,
    pipeline: Sequence[AggregateStep],
    *,
    limit: int,
    timeout_ms: int | None,
) -> list[Any]:
    """Build the FT.AGGREGATE argument list from a typed pipeline.

    If the pipeline has no explicit :class:`Limit` step, a trailing
    ``LIMIT 0 <limit>`` is appended so the server does not return an
    unbounded row set (D-04).
    """
    q = query if query else "*"
    args: list[Any] = ["FT.AGGREGATE", index, q]

    has_explicit_limit = False

    for step in pipeline:
        if isinstance(step, GroupBy):
            args.extend(["GROUPBY", str(len(step.fields)), *(f"@{f}" for f in step.fields)])
            for reducer in step.reducers:
                args.extend(_reducer_args(reducer))
        elif isinstance(step, SortBy):
            direction = step.direction.upper()
            if direction not in {"ASC", "DESC"}:
                raise ValueError(
                    f"SortBy direction must be 'ASC' or 'DESC', got {step.direction!r}"
                )
            args.extend(["SORTBY", "2", f"@{step.field}", direction])
        elif isinstance(step, Filter):
            args.extend(["FILTER", step.expr])
        elif isinstance(step, Limit):
            args.extend(["LIMIT", str(step.offset), str(step.count)])
            has_explicit_limit = True
        else:  # pragma: no cover - exhaustive over AggregateStep union
            raise TypeError(f"unsupported aggregate step: {type(step).__name__}")

    if not has_explicit_limit:
        args.extend(["LIMIT", "0", str(limit)])

    if timeout_ms is not None:
        args.extend(["TIMEOUT", str(timeout_ms)])

    return args


def _validate_hybrid_weights(weights: tuple[float, float, float]) -> None:
    """Client-side weight validation — raises ValueError before dispatch.

    Mitigates injection / DoS via malformed weights (threat_model T-153-01-03):
    - Every weight must be finite (no NaN, no ±inf).
    - Every weight must be non-negative (server contract).
    - At least one weight must be positive (otherwise RRF has no signal).
    """
    if len(weights) != 3:
        raise ValueError(f"weights must be a 3-tuple, got length {len(weights)}")
    for i, w in enumerate(weights):
        if not isinstance(w, (int, float)):
            raise ValueError(f"weights[{i}] must be numeric, got {type(w).__name__}")
        if math.isnan(w) or math.isinf(w):
            raise ValueError(f"weights[{i}] must be finite, got {w!r}")
        if w < 0.0:
            raise ValueError(f"weights[{i}] must be non-negative, got {w!r}")
    if sum(weights) <= 0.0:
        raise ValueError("weights must not all be zero")


def _build_hybrid_args(
    index: str,
    query: str,
    *,
    vector: Sequence[float] | bytes,
    vector_field: str,
    sparse: Sequence[float] | bytes | None,
    sparse_field: str,
    weights: tuple[float, float, float],
    k_per_stream: int | None,
    limit: int,
    return_fields: list[str] | None,
) -> list[Any]:
    """Build the FT.SEARCH ... HYBRID argument list.

    Layout (matches Phase 152 ``src/command/vector_search/hybrid.rs``)::

        FT.SEARCH <index> <query>
        HYBRID VECTOR @<vec_field> $hybrid_vec
        [SPARSE @<sparse_field> $hybrid_sparse]
        FUSION RRF
        WEIGHTS w_bm25 w_dense w_sparse
        [K_PER_STREAM N]
        [RETURN <n> field ...]
        LIMIT 0 <limit>
        PARAMS <2|4> hybrid_vec <blob> [hybrid_sparse <blob>]
    """
    _validate_hybrid_weights(weights)

    q = query if query else "*"
    vec_blob = _ensure_bytes(vector)

    args: list[Any] = [
        "FT.SEARCH",
        index,
        q,
        "HYBRID",
        "VECTOR",
        f"@{vector_field}",
        "$hybrid_vec",
    ]

    sparse_blob: bytes | None = None
    if sparse is not None:
        sparse_blob = _ensure_bytes(sparse)
        args.extend(["SPARSE", f"@{sparse_field}", "$hybrid_sparse"])

    args.extend(["FUSION", "RRF"])
    args.extend(["WEIGHTS", f"{weights[0]}", f"{weights[1]}", f"{weights[2]}"])

    if k_per_stream is not None:
        args.extend(["K_PER_STREAM", str(int(k_per_stream))])

    if return_fields:
        args.extend(["RETURN", str(len(return_fields)), *return_fields])

    args.extend(["LIMIT", "0", str(limit)])

    # PARAMS block trails everything else — matches vector.py convention.
    if sparse_blob is not None:
        args.extend(["PARAMS", "4", "hybrid_vec", vec_blob, "hybrid_sparse", sparse_blob])
    else:
        args.extend(["PARAMS", "2", "hybrid_vec", vec_blob])

    return args


# -- Response parsers --------------------------------------------------------


_HIGHLIGHT_PREFIX = "__highlight_"
_HIGHLIGHT_SEPARATOR = "\x1f"  # ASCII unit-separator; configurable only via server


def _parse_text_search_results(
    raw: Any,  # noqa: ANN401 -- RESP boundary: server response is dynamic
    *,
    highlight_requested: bool = False,
) -> list[TextSearchHit]:
    """Parse FT.SEARCH / FT.SEARCH ... HYBRID response.

    Moon (and Redis) return::

        [total_count, key1, [field1, val1, ...], key2, [...], ...]

    Defensive guarantees (threat_model T-153-01-02):
    - Non-list input → ``[]``.
    - Truncated rows (key without field-list tail) → stop, return what was
      parsed so far.
    - Non-list field block → empty ``fields`` for that row, score 0.
    - Any unparseable numeric → 0.
    """
    del highlight_requested  # reserved for future format-aware parsing

    if not isinstance(raw, list) or len(raw) < 1:
        return []

    results: list[TextSearchHit] = []
    i = 1  # skip total count
    while i < len(raw):
        key = _to_str(raw[i])
        i += 1
        if i >= len(raw):
            break

        fields_raw = raw[i]
        i += 1

        fields: dict[str, Any] = {}
        highlights: dict[str, list[str]] = {}
        score = 0.0

        if isinstance(fields_raw, list):
            j = 0
            while j + 1 < len(fields_raw):
                fname = _to_str(fields_raw[j])
                fval_raw = fields_raw[j + 1]

                if fname in ("__score__", "__text_score__"):
                    with contextlib.suppress(ValueError, TypeError):
                        score = float(_to_str(fval_raw))
                elif fname.startswith(_HIGHLIGHT_PREFIX):
                    orig_field = fname[len(_HIGHLIGHT_PREFIX):]
                    fval_str = _to_str(fval_raw)
                    fragments = (
                        fval_str.split(_HIGHLIGHT_SEPARATOR)
                        if _HIGHLIGHT_SEPARATOR in fval_str
                        else [fval_str]
                    )
                    highlights[orig_field] = fragments
                else:
                    # Binary blobs (e.g. raw vector bytes) stay as bytes; text
                    # values are decoded for ergonomic use in user code.
                    if isinstance(fval_raw, bytes):
                        try:
                            fields[fname] = fval_raw.decode("utf-8")
                        except UnicodeDecodeError:
                            fields[fname] = fval_raw
                    else:
                        fields[fname] = fval_raw
                j += 2

        results.append(
            TextSearchHit(
                id=key,
                score=score,
                fields=fields,
                highlights=highlights or None,
            )
        )

    return results


def _parse_aggregate_results(raw: Any) -> list[dict[str, Any]]:  # noqa: ANN401 -- RESP boundary
    """Parse FT.AGGREGATE response.

    Shape::

        [row_count, [k1, v1, k2, v2, ...], [k1, v1, ...], ...]

    Returns a list of row dicts. Numeric values are best-effort coerced via
    :func:`_maybe_cast_number`. Non-list input or malformed rows → ``[]``
    (defensive, threat_model T-153-01-02).
    """
    if not isinstance(raw, list) or len(raw) < 1:
        return []

    rows: list[dict[str, Any]] = []
    for row_raw in raw[1:]:
        if not isinstance(row_raw, list):
            continue
        row: dict[str, Any] = {}
        j = 0
        while j + 1 < len(row_raw):
            key = _to_str(row_raw[j])
            row[key] = _maybe_cast_number(row_raw[j + 1])
            j += 2
        rows.append(row)

    return rows


# -- Public sync command surface ---------------------------------------------


class TextCommands:
    """Full-text search command helpers.

    Accessed via :attr:`moondb.MoonClient.text`. Wraps FT.CREATE / FT.SEARCH /
    FT.AGGREGATE / FT.SEARCH HYBRID as typed Python methods.

    Not instantiated directly by user code; :class:`moondb.MoonClient.__init__`
    creates the singleton.

    Example::

        client.text.create_text_index(
            "issues",
            [("title", "TEXT", {"WEIGHT": 2.0}), ("body", "TEXT", {})],
            prefix="issue:",
        )
        hits = client.text.text_search("issues", "crash", limit=10)
    """

    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    # -- FT.CREATE ----------------------------------------------------------

    def create_text_index(
        self,
        name: str,
        schema: Sequence[tuple[str, str, dict[str, Any]]],
        *,
        prefix: str | None = None,
        on: str = "HASH",
    ) -> str:
        """Create a full-text / tag / numeric index.

        Args:
            name: Index name (e.g. ``"issues"``).
            schema: Sequence of ``(field_name, field_type, opts)`` tuples.
                ``field_type`` is typically ``"TEXT"``, ``"TAG"``, or
                ``"NUMERIC"``. ``opts`` may include whitelisted flags
                (``SORTABLE``, ``NOSTEM``, ``NOINDEX``) or params
                (``WEIGHT``, ``SEPARATOR``, ``PHONETIC``). Unknown opts
                are silently dropped.
            prefix: Optional key prefix filter (e.g. ``"issue:"``). When
                ``None``, the index applies to all keys on the HASH data
                type.
            on: Data type the index tracks (default ``"HASH"``).

        Returns:
            ``"OK"`` on success.

        Example::

            client.text.create_text_index(
                "issues",
                [
                    ("title", "TEXT", {"WEIGHT": 2.0}),
                    ("status", "TAG", {"SORTABLE": True}),
                ],
                prefix="issue:",
            )
        """
        args = _build_create_index_args(name, schema, prefix, on)
        return self._client.execute_command(*args)  # type: ignore[no-any-return,no-untyped-call]

    def drop_text_index(self, name: str) -> str:
        """Drop a full-text index (alias for ``FT.DROPINDEX``).

        Args:
            name: Index name.

        Returns:
            ``"OK"`` on success.
        """
        return self._client.execute_command("FT.DROPINDEX", name)  # type: ignore[no-any-return,no-untyped-call]

    # -- FT.SEARCH (BM25) ---------------------------------------------------

    def text_search(
        self,
        index: str,
        query: str,
        *,
        limit: int = 10,
        offset: int = 0,
        return_fields: list[str] | None = None,
        highlight: bool = False,
        highlight_fields: list[str] | None = None,
        lang: str | None = None,
    ) -> list[TextSearchHit]:
        """Run a BM25 text search against a full-text index.

        Args:
            index: Index name.
            query: Query string. Empty string is coerced to ``"*"``
                (match-all).
            limit: Max rows to return (default 10).
            offset: Row offset for pagination (default 0).
            return_fields: Explicit subset of hash fields to return.
            highlight: If True, request highlighted fragments.
            highlight_fields: Restrict highlighting to these fields.
            lang: Optional language hint (e.g. ``"english"``).

        Returns:
            Ordered list of :class:`TextSearchHit` (highest BM25 first).
        """
        args = _build_text_search_args(
            index,
            query,
            limit=limit,
            offset=offset,
            return_fields=return_fields,
            highlight=highlight,
            highlight_fields=highlight_fields,
            lang=lang,
        )
        raw = self._client.execute_command(*args)  # type: ignore[no-untyped-call]
        return _parse_text_search_results(raw, highlight_requested=highlight)

    # -- FT.AGGREGATE -------------------------------------------------------

    def aggregate(
        self,
        index: str,
        query: str,
        pipeline: Sequence[AggregateStep],
        *,
        limit: int = 10,
        timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run an FT.AGGREGATE pipeline.

        Args:
            index: Index name.
            query: Filter query (``"*"`` for match-all; empty coerced).
            pipeline: Sequence of :class:`AggregateStep` dataclasses
                (:class:`GroupBy`, :class:`SortBy`, :class:`Filter`,
                :class:`Limit`).
            limit: Default row cap, applied only if the pipeline lacks
                an explicit :class:`Limit` step.
            timeout_ms: Optional server-side timeout in milliseconds.

        Returns:
            List of row dicts keyed by reducer alias / GROUPBY field.

        Example::

            rows = client.text.aggregate(
                "issues",
                "*",
                [
                    GroupBy(["priority"], [Count(alias="n")]),
                    SortBy("n", "DESC"),
                ],
            )
        """
        args = _build_aggregate_args(
            index, query, pipeline, limit=limit, timeout_ms=timeout_ms
        )
        raw = self._client.execute_command(*args)  # type: ignore[no-untyped-call]
        return _parse_aggregate_results(raw)

    # -- FT.SEARCH ... HYBRID ----------------------------------------------

    def hybrid_search(
        self,
        index: str,
        query: str,
        *,
        vector: Sequence[float] | bytes,
        vector_field: str = "vec",
        sparse: Sequence[float] | bytes | None = None,
        sparse_field: str = "sparse_vec",
        weights: tuple[float, float, float] = (1.0, 1.0, 1.0),
        k_per_stream: int | None = None,
        limit: int = 10,
        return_fields: list[str] | None = None,
    ) -> list[TextSearchHit]:
        """Run a BM25 + dense [+ sparse] hybrid search fused via RRF.

        Args:
            index: Index name.
            query: BM25 query string (empty → ``"*"``).
            vector: Dense query vector (list of floats or pre-encoded bytes).
            vector_field: Name of the dense vector field in the schema.
            sparse: Optional sparse query vector.
            sparse_field: Name of the sparse vector field.
            weights: ``(w_bm25, w_dense, w_sparse)`` RRF weights. Each
                weight must be finite and non-negative; at least one must
                be positive. Violations raise :exc:`ValueError` before
                any command is sent.
            k_per_stream: Optional per-stream top-K (controls recall vs
                latency on the dense side).
            limit: Max rows after fusion.
            return_fields: Explicit subset of hash fields to return.

        Returns:
            Ordered list of :class:`TextSearchHit` with RRF ``score``.

        Raises:
            ValueError: If ``weights`` is malformed (NaN, inf, negative,
                all-zero, wrong length).
        """
        args = _build_hybrid_args(
            index,
            query,
            vector=vector,
            vector_field=vector_field,
            sparse=sparse,
            sparse_field=sparse_field,
            weights=weights,
            k_per_stream=k_per_stream,
            limit=limit,
            return_fields=return_fields,
        )
        raw = self._client.execute_command(*args)  # type: ignore[no-untyped-call]
        return _parse_text_search_results(raw, highlight_requested=False)


# -- Public async command surface --------------------------------------------


class AsyncTextCommands:
    """Async variant of :class:`TextCommands` for ``redis.asyncio.Redis``.

    Mirrors the full sync surface — identical argument construction, identical
    response parsing — with ``async def`` + ``await`` on the dispatch call.

    Accessed via :attr:`moondb.AsyncMoonClient.text`.
    """

    def __init__(self, client: Any) -> None:  # noqa: ANN401 -- redis.asyncio.Redis has no Generic
        self._client = client

    async def create_text_index(
        self,
        name: str,
        schema: Sequence[tuple[str, str, dict[str, Any]]],
        *,
        prefix: str | None = None,
        on: str = "HASH",
    ) -> str:
        """Async variant. See :meth:`TextCommands.create_text_index`."""
        args = _build_create_index_args(name, schema, prefix, on)
        return await self._client.execute_command(*args)  # type: ignore[no-any-return]

    async def drop_text_index(self, name: str) -> str:
        """Async variant. See :meth:`TextCommands.drop_text_index`."""
        return await self._client.execute_command("FT.DROPINDEX", name)  # type: ignore[no-any-return]

    async def text_search(
        self,
        index: str,
        query: str,
        *,
        limit: int = 10,
        offset: int = 0,
        return_fields: list[str] | None = None,
        highlight: bool = False,
        highlight_fields: list[str] | None = None,
        lang: str | None = None,
    ) -> list[TextSearchHit]:
        """Async variant. See :meth:`TextCommands.text_search`."""
        args = _build_text_search_args(
            index,
            query,
            limit=limit,
            offset=offset,
            return_fields=return_fields,
            highlight=highlight,
            highlight_fields=highlight_fields,
            lang=lang,
        )
        raw = await self._client.execute_command(*args)
        return _parse_text_search_results(raw, highlight_requested=highlight)

    async def aggregate(
        self,
        index: str,
        query: str,
        pipeline: Sequence[AggregateStep],
        *,
        limit: int = 10,
        timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Async variant. See :meth:`TextCommands.aggregate`."""
        args = _build_aggregate_args(
            index, query, pipeline, limit=limit, timeout_ms=timeout_ms
        )
        raw = await self._client.execute_command(*args)
        return _parse_aggregate_results(raw)

    async def hybrid_search(
        self,
        index: str,
        query: str,
        *,
        vector: Sequence[float] | bytes,
        vector_field: str = "vec",
        sparse: Sequence[float] | bytes | None = None,
        sparse_field: str = "sparse_vec",
        weights: tuple[float, float, float] = (1.0, 1.0, 1.0),
        k_per_stream: int | None = None,
        limit: int = 10,
        return_fields: list[str] | None = None,
    ) -> list[TextSearchHit]:
        """Async variant. See :meth:`TextCommands.hybrid_search`."""
        args = _build_hybrid_args(
            index,
            query,
            vector=vector,
            vector_field=vector_field,
            sparse=sparse,
            sparse_field=sparse_field,
            weights=weights,
            k_per_stream=k_per_stream,
            limit=limit,
            return_fields=return_fields,
        )
        raw = await self._client.execute_command(*args)
        return _parse_text_search_results(raw, highlight_requested=False)


# Re-export every builder + parser + union for test + adapter consumption.
__all__ = [
    "TextCommands",
    "AsyncTextCommands",
    "_build_create_index_args",
    "_build_text_search_args",
    "_build_aggregate_args",
    "_build_hybrid_args",
    "_parse_text_search_results",
    "_parse_aggregate_results",
    "_maybe_cast_number",
    "_validate_hybrid_weights",
    "_reducer_args",
    "_ensure_bytes",
    "_to_str",
]


# Suppress "imported but unused" noise: the Union below anchors static
# analyzers that rely on the concrete reducer types staying reachable from
# the module's public surface via `moondb.text`.
_UNUSED_UNION_ANCHOR = Union[Count, Sum, Avg, Min, Max, CountDistinct]
del _UNUSED_UNION_ANCHOR
