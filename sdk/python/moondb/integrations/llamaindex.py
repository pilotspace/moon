"""LlamaIndex VectorStore adapter for MoonDB.

Implements the LlamaIndex ``BasePydanticVectorStore`` interface, enabling
MoonDB as a vector store for LlamaIndex RAG pipelines.

Install: ``pip install moondb[llamaindex]``

Example::

    from llama_index.core import VectorStoreIndex, StorageContext
    from llama_index.core.schema import TextNode
    from moondb.integrations.llamaindex import MoonVectorStore

    vector_store = MoonVectorStore(
        index_name="docs",
        moon_url="redis://localhost:6379",
        dim=384,
    )

    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    index = VectorStoreIndex([], storage_context=storage_context)
"""

from __future__ import annotations

import contextlib
import uuid
from typing import Any

try:
    from llama_index.core.schema import BaseNode, MetadataMode, TextNode
    from llama_index.core.vector_stores.types import (
        BasePydanticVectorStore,
        VectorStoreQuery,
        VectorStoreQueryMode,
        VectorStoreQueryResult,
    )
    from llama_index.core.vector_stores.utils import (
        node_to_metadata_dict,
    )
except ImportError as e:
    raise ImportError(
        "llama-index-core is required for the LlamaIndex integration. "
        "Install with: pip install moondb[llamaindex]"
    ) from e

from pydantic import PrivateAttr

from ..client import MoonClient
from ..types import SearchResult, TextSearchHit, encode_vector

# Stream-score field markers emitted by the server-side hybrid path
# (mirrors src/command/vector_search/hybrid.rs). When present on a
# TextSearchHit they are surfaced as metadata["stream_hits"] for parity
# with the LangChain adapter.
_BM25_STREAM_FIELD = "__bm25_score__"
_DENSE_STREAM_FIELD = "__dense_score__"
_SPARSE_STREAM_FIELD = "__sparse_score__"
_STREAM_FIELDS: tuple[str, ...] = (_BM25_STREAM_FIELD, _DENSE_STREAM_FIELD, _SPARSE_STREAM_FIELD)


class MoonVectorStore(BasePydanticVectorStore):
    """LlamaIndex VectorStore backed by MoonDB.

    Uses MoonDB's FT.CREATE / FT.SEARCH for vector storage and retrieval.
    Supports graph expansion via EXPAND GRAPH for enhanced RAG.

    Attributes:
        index_name: Name of the Moon vector index.
        moon_url: Redis connection URL.
        prefix: Key prefix for stored nodes.
        vector_field: Vector field name in hash schema.
        content_field: Field for text content.
        dim: Vector dimension.
        metric: Distance metric.
        expand_graph: Optional graph name for EXPAND GRAPH in searches.
        expand_depth: Graph expansion depth.
        stores_text: Whether this store keeps text (always True).
        is_embedding_query: Whether queries need embedding (always True).

    Example::

        store = MoonVectorStore(index_name="rag", dim=1536, metric="COSINE")
    """

    stores_text: bool = True
    is_embedding_query: bool = True

    # Config fields
    index_name: str
    moon_url: str = "redis://localhost:6379"
    prefix: str = "node:"
    vector_field: str = "vec"
    content_field: str = "content"
    dim: int = 384
    metric: str = "COSINE"
    expand_graph: str | None = None
    expand_depth: int = 2
    # Hybrid retrieval defaults — overridable per-instance. The triple maps
    # to (w_bm25, w_dense, w_sparse) for FT.SEARCH ... HYBRID FUSION RRF
    # (Plan 01 TextCommands.hybrid_search). Sparse weight defaults to 0.0
    # so users without a sparse field do not need to opt out (D-07).
    hybrid_weights: tuple[float, float, float] = (1.0, 1.0, 0.0)
    hybrid_k_per_stream: int | None = None

    # Private client (not serialized). Canonical pydantic-v2 private-attr
    # declaration: writes via ``self._client = ...`` route through the
    # pydantic descriptor into ``self.__pydantic_private__["_client"]``,
    # and ``model_dump()`` excludes the attribute by construction.
    _client: MoonClient | None = PrivateAttr(default=None)

    # Pydantic v2 model config (replaces deprecated class-based Config) —
    # MoonClient is not a pydantic model, so allow arbitrary types.
    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, **kwargs: Any) -> None:  # noqa: ANN401 -- pydantic init contract
        # Pop the non-declared ``moon_client`` kwarg BEFORE ``super().__init__``.
        # BasePydanticVectorStore uses strict field validation; if we let
        # pydantic see a live MoonClient under an undeclared key it would
        # raise "extra input not permitted". This also prevents the client
        # from ever touching the serialisable model state (T-153-05-02).
        #
        # NOTE: this is an ``__init__`` override, NOT a ``model_post_init``
        # hook — ``model_post_init(self, __context)`` cannot see popped
        # kwargs. ``__context`` carries only the ``context=`` dict from
        # ``model_validate(..., context=...)`` calls.
        client = kwargs.pop("moon_client", None)
        super().__init__(**kwargs)
        # PrivateAttr storage: direct assignment after ``super().__init__``
        # routes through the pydantic descriptor into
        # ``self.__pydantic_private__["_client"]``. Preserves the caller-
        # supplied client so ``_ensure_client()`` short-circuits to it
        # instead of rebuilding one from the default ``moon_url``.
        if client is not None:
            self._client = client
        self._ensure_client()
        self._ensure_index()

    def _ensure_client(self) -> MoonClient:
        """Lazily create the Moon client."""
        if self._client is None:
            self._client = MoonClient.from_url(self.moon_url)  # type: ignore[assignment]
        return self._client

    def _ensure_index(self) -> None:
        """Create the vector index if it doesn't exist."""
        client = self._ensure_client()
        # Index may already exist -- ignore the "already exists" error path.
        with contextlib.suppress(Exception):
            client.vector.create_index(
                self.index_name,
                prefix=self.prefix,
                field_name=self.vector_field,
                dim=self.dim,
                metric=self.metric,
                extra_schema=[self.content_field, "TEXT"],
            )

    @property
    def client(self) -> MoonClient:
        """Return the underlying MoonClient."""
        return self._ensure_client()

    def add(
        self,
        nodes: list[BaseNode],
        **add_kwargs: Any,  # noqa: ANN401 -- LlamaIndex BasePydanticVectorStore contract
    ) -> list[str]:
        """Add nodes to the vector store.

        Args:
            nodes: List of LlamaIndex nodes to store.

        Returns:
            List of node IDs.

        Example::

            from llama_index.core.schema import TextNode

            nodes = [
                TextNode(text="Hello world", embedding=[0.1, 0.2, ...]),
                TextNode(text="Goodbye", embedding=[0.3, 0.4, ...]),
            ]
            ids = vector_store.add(nodes)
        """
        client = self._ensure_client()
        ids: list[str] = []

        pipeline = client.pipeline(transaction=False)
        for node in nodes:
            node_id = node.node_id or uuid.uuid4().hex[:12]
            key = f"{self.prefix}{node_id}"

            embedding = node.get_embedding()
            if embedding is None:
                continue

            text = node.get_content(metadata_mode=MetadataMode.NONE)

            mapping: dict[str, Any] = {
                self.vector_field: encode_vector(embedding),
                self.content_field: text,
                "_node_id": node_id,
            }

            # Store metadata — metadata serialization may fail for nodes with
            # non-JSON-serializable content; fall back to raw .metadata dict.
            try:
                meta_dict = node_to_metadata_dict(node, remove_text=True)
                for mk, mv in meta_dict.items():
                    mapping[f"meta_{mk}"] = str(mv)
            except Exception:  # noqa: BLE001 -- defensive fallback
                if hasattr(node, "metadata") and node.metadata:
                    for mk, mv in node.metadata.items():
                        mapping[f"meta_{mk}"] = str(mv)

            pipeline.hset(key, mapping=mapping)  # type: ignore[arg-type]
            ids.append(node_id)

        pipeline.execute()
        return ids

    def delete(
        self,
        ref_doc_id: str,
        **delete_kwargs: Any,  # noqa: ANN401 -- LlamaIndex BasePydanticVectorStore contract
    ) -> None:
        """Delete a node by document ID.

        Args:
            ref_doc_id: Document/node ID to delete.

        Example::

            vector_store.delete("abc123")
        """
        client = self._ensure_client()
        key = f"{self.prefix}{ref_doc_id}"
        client.delete(key)

    def query(
        self,
        query: VectorStoreQuery,
        **kwargs: Any,  # noqa: ANN401 -- LlamaIndex BasePydanticVectorStore contract
    ) -> VectorStoreQueryResult:
        """Query the vector store.

        Dispatches based on ``query.mode``:

        - :attr:`VectorStoreQueryMode.DEFAULT` (or ``None``) — dense KNN
          (existing behavior, unchanged for zero regression).
        - :attr:`VectorStoreQueryMode.HYBRID` — BM25 + dense (+ optional
          sparse) fused server-side via Moon's RRF (Plan 01
          ``TextCommands.hybrid_search``). Requires both
          ``query.query_embedding`` and ``query.query_str``; otherwise
          raises :exc:`ValueError`. Hybrid weights resolve from
          ``query.alpha`` (LlamaIndex canonical: ``alpha=1.0`` ⇒ pure
          dense, ``0.0`` ⇒ pure BM25) when set, else from
          :attr:`hybrid_weights`.
        - :attr:`VectorStoreQueryMode.TEXT_SEARCH` — BM25-only via
          ``TextCommands.text_search``. Returns an empty result if
          ``query.query_str`` is missing/blank.
        - Any other mode raises :exc:`ValueError` (no silent fallthrough —
          threat_model T-153-02-07).

        Args:
            query: LlamaIndex VectorStoreQuery with embedding and parameters.

        Returns:
            VectorStoreQueryResult with nodes, similarities, and IDs.

        Example::

            from llama_index.core.vector_stores.types import (
                VectorStoreQuery,
                VectorStoreQueryMode,
            )

            # Dense-only (default)
            q = VectorStoreQuery(query_embedding=[0.1] * 4, similarity_top_k=5)
            res = vector_store.query(q)

            # Hybrid (BM25 + dense)
            q = VectorStoreQuery(
                query_embedding=[0.1] * 4,
                query_str="auth crash",
                mode=VectorStoreQueryMode.HYBRID,
                similarity_top_k=5,
                alpha=0.7,  # 70% dense, 30% BM25
            )
            res = vector_store.query(q)

            # BM25-only text search
            q = VectorStoreQuery(
                query_str="memory leak",
                mode=VectorStoreQueryMode.TEXT_SEARCH,
                similarity_top_k=5,
            )
            res = vector_store.query(q)
        """
        del kwargs  # Reserved for forward-compatible LlamaIndex extensions

        mode = query.mode if query.mode is not None else VectorStoreQueryMode.DEFAULT

        if mode == VectorStoreQueryMode.DEFAULT:
            return self._query_dense(query)
        if mode == VectorStoreQueryMode.HYBRID:
            return self._query_hybrid(query)
        if mode == VectorStoreQueryMode.TEXT_SEARCH:
            return self._query_text(query)

        raise ValueError(
            f"Unsupported VectorStoreQueryMode: {mode!r}. Supported: "
            f"DEFAULT, HYBRID, TEXT_SEARCH"
        )

    # -- Mode dispatchers ----------------------------------------------------

    def _query_dense(self, query: VectorStoreQuery) -> VectorStoreQueryResult:
        """Dense KNN path — existing behavior preserved verbatim."""
        if query.query_embedding is None:
            return VectorStoreQueryResult(nodes=[], similarities=[], ids=[])

        client = self._ensure_client()
        k = query.similarity_top_k or 10

        search_results = client.vector.search(
            self.index_name,
            query.query_embedding,
            k=k,
            field_name=self.vector_field,
            expand_graph=self.expand_graph,
            expand_depth=self.expand_depth,
        )

        nodes: list[TextNode] = []
        similarities: list[float] = []
        ids: list[str] = []
        for sr in search_results:
            node = self._sr_to_node(sr)
            nodes.append(node)
            # Convert distance to similarity (1 / (1 + distance) for L2)
            similarities.append(1.0 / (1.0 + sr.score) if sr.score >= 0 else 0.0)
            ids.append(node.id_)

        return VectorStoreQueryResult(nodes=nodes, similarities=similarities, ids=ids)

    def _query_hybrid(self, query: VectorStoreQuery) -> VectorStoreQueryResult:
        """BM25 + dense (+ optional sparse) RRF fusion via Plan 01 TextCommands."""
        if query.query_embedding is None or not (query.query_str or "").strip():
            raise ValueError(
                "HYBRID mode requires both query_embedding and query_str"
            )

        client = self._ensure_client()
        k = query.similarity_top_k or 10
        weights = self._resolve_hybrid_weights(query)

        hits = client.text.hybrid_search(
            self.index_name,
            query.query_str or "",
            vector=query.query_embedding,
            vector_field=self.vector_field,
            weights=weights,
            k_per_stream=self.hybrid_k_per_stream,
            limit=k,
        )
        return self._hits_to_result(hits)

    def _query_text(self, query: VectorStoreQuery) -> VectorStoreQueryResult:
        """BM25-only path via Plan 01 TextCommands.text_search."""
        text = (query.query_str or "").strip()
        if not text:
            return VectorStoreQueryResult(nodes=[], similarities=[], ids=[])

        client = self._ensure_client()
        k = query.similarity_top_k or 10
        hits = client.text.text_search(self.index_name, text, limit=k)
        return self._hits_to_result(hits)

    # -- Hybrid weight resolution -------------------------------------------

    def _resolve_hybrid_weights(
        self,
        query: VectorStoreQuery,
    ) -> tuple[float, float, float]:
        """Map LlamaIndex ``alpha`` to ``(bm25, dense, sparse)`` weights.

        LlamaIndex convention: ``alpha=1.0`` ⇒ pure dense, ``alpha=0.0`` ⇒
        pure BM25. Out-of-range alphas are clamped to ``[0.0, 1.0]`` so the
        Plan 01 ``TextCommands.hybrid_search`` non-negativity invariant
        always holds (no client-side ValueError surprise from alpha=1.5).
        Sparse weight is left at the configured default — alpha does not
        encode sparse mixing in LlamaIndex.
        """
        alpha = getattr(query, "alpha", None)
        if alpha is None:
            return self.hybrid_weights
        clamped = max(0.0, min(1.0, float(alpha)))
        sparse = self.hybrid_weights[2]
        return (1.0 - clamped, clamped, sparse)

    # -- Result builders -----------------------------------------------------

    def _hits_to_result(
        self,
        hits: list[TextSearchHit],
    ) -> VectorStoreQueryResult:
        """Convert ``TextSearchHit`` list (text/hybrid) to a query result.

        BM25 / RRF scores are already similarity-shaped (higher = better),
        so they pass through verbatim — no ``1/(1+d)`` inversion (D-09).
        """
        nodes: list[TextNode] = []
        similarities: list[float] = []
        ids: list[str] = []
        for hit in hits:
            node = self._hit_to_node(hit)
            nodes.append(node)
            similarities.append(hit.score)
            ids.append(node.id_)
        return VectorStoreQueryResult(nodes=nodes, similarities=similarities, ids=ids)

    def _split_metadata(
        self,
        fields: dict[str, Any],
    ) -> tuple[str, str | None, dict[str, Any], dict[str, float]]:
        """Strip ``content``, ``_node_id``, ``meta_*`` prefix, stream scores.

        Returns ``(text, node_id, metadata, stream_hits)``. ``node_id`` is
        ``None`` if the server did not emit ``_node_id`` (callers fall back
        to the row key).
        """
        text = ""
        node_id: str | None = None
        metadata: dict[str, Any] = {}
        stream_hits: dict[str, float] = {}

        for fk, fv in fields.items():
            if fk == self.content_field:
                text = str(fv) if fv is not None else ""
                continue
            if fk == "_node_id":
                node_id = str(fv) if fv is not None else None
                continue
            if fk == self.vector_field:
                continue
            if fk in _STREAM_FIELDS:
                with contextlib.suppress(TypeError, ValueError):
                    stream_hits[fk.strip("_").replace("_score", "")] = float(fv)
                continue
            if fk.startswith("meta_"):
                metadata[fk[5:]] = fv
            else:
                metadata[fk] = fv

        return text, node_id, metadata, stream_hits

    def _sr_to_node(self, sr: SearchResult) -> TextNode:
        """Build a :class:`TextNode` from a vector-KNN :class:`SearchResult`."""
        text, node_id, metadata, _ = self._split_metadata(sr.fields)
        # Existing dense path does not surface stream_hits (no hybrid scores).
        if sr.graph_hops is not None:
            metadata["__graph_hops"] = sr.graph_hops
        return TextNode(
            text=text,
            id_=node_id or sr.key,
            metadata=metadata,
        )

    def _hit_to_node(self, hit: TextSearchHit) -> TextNode:
        """Build a :class:`TextNode` from a hybrid/text :class:`TextSearchHit`."""
        text, node_id, metadata, stream_hits = self._split_metadata(hit.fields)
        if stream_hits:
            metadata["stream_hits"] = stream_hits
        return TextNode(
            text=text,
            id_=node_id or hit.id,
            metadata=metadata,
        )
