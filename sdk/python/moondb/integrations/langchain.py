"""LangChain VectorStore adapter for MoonDB.

Implements the LangChain ``VectorStore`` interface, enabling MoonDB as a
drop-in vector store for any LangChain application.

Install: ``pip install moondb[langchain]``

Example::

    from langchain_openai import OpenAIEmbeddings
    from moondb.integrations.langchain import MoonVectorStore

    store = MoonVectorStore(
        index_name="docs",
        embedding=OpenAIEmbeddings(),
        moon_url="redis://localhost:6379",
    )

    # Add documents
    store.add_texts(["Hello world", "How are you?"], metadatas=[{"src": "a"}])

    # Search
    results = store.similarity_search("greeting", k=3)
    for doc in results:
        print(doc.page_content, doc.metadata)
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterable
from typing import Any

try:
    from langchain_core.callbacks import CallbackManagerForRetrieverRun
    from langchain_core.documents import Document
    from langchain_core.embeddings import Embeddings
    from langchain_core.retrievers import BaseRetriever
    from langchain_core.vectorstores import VectorStore
except ImportError as e:
    raise ImportError(
        "langchain-core is required for the LangChain integration. "
        "Install with: pip install moondb[langchain]"
    ) from e

from ..client import MoonClient
from ..types import SearchResult, TextSearchHit, encode_vector

# Valid search_type values accepted by MoonVectorStore.similarity_search /
# MoonVectorStore.as_retriever. Listed here so the ValueError message stays in
# sync with the dispatcher.
_VALID_SEARCH_TYPES: tuple[str, ...] = ("similarity", "hybrid", "mmr")

# Server-side per-stream score field markers (mirrors src/command/vector_search/hybrid.rs).
_BM25_STREAM_FIELD = "__bm25_score__"
_DENSE_STREAM_FIELD = "__dense_score__"
_SPARSE_STREAM_FIELD = "__sparse_score__"
_STREAM_FIELDS: tuple[str, ...] = (_BM25_STREAM_FIELD, _DENSE_STREAM_FIELD, _SPARSE_STREAM_FIELD)


class MoonVectorStore(VectorStore):
    """LangChain VectorStore backed by MoonDB.

    Uses MoonDB's FT.CREATE / FT.SEARCH for vector storage and retrieval,
    with automatic embedding via a LangChain Embeddings model.

    Args:
        index_name: Name of the Moon vector index.
        embedding: LangChain Embeddings instance for text-to-vector conversion.
        moon_url: Redis connection URL (default "redis://localhost:6379").
        moon_client: Optional pre-configured MoonClient (overrides moon_url).
        prefix: Key prefix for stored documents (default "doc:").
        vector_field: Vector field name in the hash schema (default "vec").
        content_field: Field name for document text content (default "content").
        metadata_field: Field name prefix for metadata (default "metadata_").
        dim: Vector dimension (auto-detected from embedding if not set).
        metric: Distance metric (default "COSINE").
        session_key: Optional session key for session-aware deduplication.
        create_index: Whether to create the index on init (default True).

    Example::

        from langchain_openai import OpenAIEmbeddings
        from moondb.integrations.langchain import MoonVectorStore

        store = MoonVectorStore(
            index_name="knowledge",
            embedding=OpenAIEmbeddings(model="text-embedding-3-small"),
            dim=1536,
        )
    """

    def __init__(
        self,
        index_name: str,
        embedding: Embeddings,
        *,
        moon_url: str = "redis://localhost:6379",
        moon_client: MoonClient | None = None,
        prefix: str = "doc:",
        vector_field: str = "vec",
        content_field: str = "content",
        metadata_field: str = "metadata_",
        dim: int | None = None,
        metric: str = "COSINE",
        session_key: str | None = None,
        create_index: bool = True,
    ) -> None:
        self._index_name = index_name
        self._embedding = embedding
        self._prefix = prefix
        self._vector_field = vector_field
        self._content_field = content_field
        self._metadata_field = metadata_field
        self._metric = metric
        self._session_key = session_key

        if moon_client is not None:
            self._client = moon_client
        else:
            self._client = MoonClient.from_url(moon_url)  # type: ignore[assignment]

        # Auto-detect dimension
        if dim is None:
            sample = self._embedding.embed_query("dimension probe")
            dim = len(sample)
        self._dim = dim

        if create_index:
            self._ensure_index()

    def _ensure_index(self) -> None:
        """Create the vector index if it doesn't already exist."""
        # Index may already exist -- ignore the "already exists" error path.
        with contextlib.suppress(Exception):
            self._client.vector.create_index(
                self._index_name,
                prefix=self._prefix,
                field_name=self._vector_field,
                dim=self._dim,
                metric=self._metric,
                extra_schema=[self._content_field, "TEXT"],
            )

    @property
    def embeddings(self) -> Embeddings:
        """Return the embedding model."""
        return self._embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,  # noqa: ANN401 -- LangChain VectorStore base contract
    ) -> list[str]:
        """Add texts with embeddings to the vector store.

        Args:
            texts: Iterable of text strings to embed and store.
            metadatas: Optional list of metadata dicts (one per text).
            ids: Optional list of document IDs. Auto-generated if not provided.

        Returns:
            List of document IDs.

        Example::

            ids = store.add_texts(
                ["Document one", "Document two"],
                metadatas=[{"source": "web"}, {"source": "pdf"}],
            )
        """
        texts_list = list(texts)
        vectors = self._embedding.embed_documents(texts_list)

        if metadatas is None:
            metadatas = [{}] * len(texts_list)

        if ids is None:
            ids = [f"{self._prefix}{uuid.uuid4().hex[:12]}" for _ in texts_list]
        else:
            ids = [
                id_ if id_.startswith(self._prefix) else f"{self._prefix}{id_}"
                for id_ in ids
            ]

        pipeline = self._client.pipeline(transaction=False)
        for doc_id, text, vector, meta in zip(ids, texts_list, vectors, metadatas):
            mapping: dict[str, Any] = {
                self._vector_field: encode_vector(vector),
                self._content_field: text,
            }
            for k, v in meta.items():
                mapping[f"{self._metadata_field}{k}"] = str(v)

            pipeline.hset(doc_id, mapping=mapping)  # type: ignore[arg-type]

        pipeline.execute()
        return ids

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,  # noqa: ANN401 -- LangChain VectorStore base contract
    ) -> list[Document]:
        """Search for documents similar to the query text.

        Args:
            query: Query text to embed and search.
            k: Number of results to return (default 4).
            search_type: Optional retrieval mode. ``"similarity"`` (default —
                dense KNN), ``"hybrid"`` (BM25 + dense fused via Moon's
                server-side RRF — see :meth:`similarity_search_with_score`),
                or ``"mmr"`` (delegates to similarity for now).
            hybrid_weights: ``(bm25, dense, sparse)`` weights for hybrid mode
                (default ``(1.0, 1.0, 0.0)``).
            k_per_stream: Per-stream candidate cap for hybrid mode.

        Returns:
            List of LangChain Document objects.

        Example::

            docs = store.similarity_search("machine learning", k=3)
            for doc in docs:
                print(doc.page_content)

            # Hybrid BM25 + dense
            docs = store.similarity_search(
                "auth crash",
                k=5,
                search_type="hybrid",
                hybrid_weights=(1.0, 1.5, 0.0),
            )
        """
        results = self.similarity_search_with_score(query, k=k, **kwargs)
        return [doc for doc, _ in results]

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        *,
        search_type: str = "similarity",
        hybrid_weights: tuple[float, float, float] = (1.0, 1.0, 0.0),
        k_per_stream: int | None = None,
        **kwargs: Any,  # noqa: ANN401 -- LangChain VectorStore base contract
    ) -> list[tuple[Document, float]]:
        """Search with relevance scores.

        Args:
            query: Query text.
            k: Number of results.
            search_type: ``"similarity"`` (dense KNN, default), ``"hybrid"``
                (BM25 + dense fused via RRF, server-side), or ``"mmr"``
                (currently delegates to similarity — kept for API parity).
            hybrid_weights: ``(w_bm25, w_dense, w_sparse)`` RRF weights when
                ``search_type="hybrid"``. Default ``(1.0, 1.0, 0.0)`` excludes
                the sparse stream. Validation (finite, non-negative, at least
                one positive) is delegated to :class:`moondb.text.TextCommands`.
            k_per_stream: Optional per-stream top-K override for hybrid mode.

        Returns:
            List of (Document, score) tuples. Lower score = more similar for
            ``"similarity"``; higher RRF score = more relevant for ``"hybrid"``.

        Raises:
            ValueError: If ``search_type`` is not one of
                ``"similarity"`` / ``"hybrid"`` / ``"mmr"``, or if
                ``hybrid_weights`` is malformed (delegated to TextCommands).

        Example::

            results = store.similarity_search_with_score("AI", k=5)
            for doc, score in results:
                print(f"{score:.4f} {doc.page_content}")

            # Hybrid mode
            results = store.similarity_search_with_score(
                "AI",
                k=5,
                search_type="hybrid",
                hybrid_weights=(1.0, 1.5, 0.0),
            )
        """
        del kwargs  # Reserved for future LangChain base-class kwargs (filter, etc.)

        if search_type in ("similarity", "mmr"):
            # NOTE: MoonVectorStore does not currently implement an MMR-specific
            # path; "mmr" delegates to dense KNN to preserve API parity with the
            # LangChain VectorStore contract. Future work: integrate maximal
            # marginal relevance reranking on the returned hits.
            return self._similarity_search_with_score(query, k)

        if search_type == "hybrid":
            return self._hybrid_search_with_score(query, k, hybrid_weights, k_per_stream)

        raise ValueError(
            f"Unsupported search_type: {search_type!r}. Valid: "
            f"{', '.join(repr(s) for s in _VALID_SEARCH_TYPES)}"
        )

    # -- Internal dispatchers -------------------------------------------------

    def _similarity_search_with_score(
        self,
        query: str,
        k: int,
    ) -> list[tuple[Document, float]]:
        """Dense KNN path (existing behavior — preserved verbatim)."""
        vector = self._embedding.embed_query(query)

        search_results = self._client.vector.search(
            self._index_name,
            vector,
            k=k,
            field_name=self._vector_field,
            session_key=self._session_key,
        )

        return [(self._sr_to_document(sr), sr.score) for sr in search_results]

    def _hybrid_search_with_score(
        self,
        query: str,
        k: int,
        hybrid_weights: tuple[float, float, float],
        k_per_stream: int | None,
    ) -> list[tuple[Document, float]]:
        """BM25 + dense [+ sparse] hybrid fused server-side via RRF."""
        vector = self._embedding.embed_query(query)
        hits: list[TextSearchHit] = self._client.text.hybrid_search(
            self._index_name,
            query,
            vector=vector,
            vector_field=self._vector_field,
            weights=hybrid_weights,
            k_per_stream=k_per_stream,
            limit=k,
        )
        return [(self._hit_to_document(h), h.score) for h in hits]

    # -- Hit -> Document converters ------------------------------------------

    def _split_metadata(
        self,
        fields: dict[str, Any],
    ) -> tuple[str, dict[str, Any], dict[str, float]]:
        """Strip ``content``, ``metadata_*`` prefix, and per-stream score fields.

        Returns a ``(content, metadata, stream_hits)`` triple where:
        - ``content`` is the page-content string (empty string if absent).
        - ``metadata`` keys have any ``metadata_`` prefix stripped.
        - ``stream_hits`` is the per-stream RRF score breakdown (empty if the
          server emitted no ``__bm25_score__``/``__dense_score__``/
          ``__sparse_score__`` markers).
        """
        content = ""
        metadata: dict[str, Any] = {}
        stream_hits: dict[str, float] = {}

        for raw_key, raw_val in fields.items():
            if raw_key == self._content_field:
                content = str(raw_val) if raw_val is not None else ""
                continue
            if raw_key in _STREAM_FIELDS:
                # Defensive: server should always emit floats here, but
                # malformed input must not crash the adapter.
                with contextlib.suppress(TypeError, ValueError):
                    stream_hits[raw_key.strip("_").replace("_score", "")] = float(raw_val)
                continue
            if raw_key.startswith(self._metadata_field):
                clean_key = raw_key[len(self._metadata_field):]
                metadata[clean_key] = raw_val
            else:
                metadata[raw_key] = raw_val

        return content, metadata, stream_hits

    def _sr_to_document(self, sr: SearchResult) -> Document:
        """Convert a vector-KNN :class:`SearchResult` to a LangChain Document."""
        content, metadata, stream_hits = self._split_metadata(sr.fields)
        metadata["key"] = sr.key
        metadata["score"] = sr.score
        if stream_hits:
            metadata["stream_hits"] = stream_hits
        return Document(page_content=content, metadata=metadata)

    def _hit_to_document(self, hit: TextSearchHit) -> Document:
        """Convert a hybrid/text :class:`TextSearchHit` to a LangChain Document."""
        content, metadata, stream_hits = self._split_metadata(hit.fields)
        metadata["key"] = hit.id
        metadata["score"] = hit.score
        if stream_hits:
            metadata["stream_hits"] = stream_hits
        return Document(page_content=content, metadata=metadata)

    def similarity_search_by_vector(
        self,
        embedding: list[float],
        k: int = 4,
        **kwargs: Any,  # noqa: ANN401 -- LangChain VectorStore base contract
    ) -> list[Document]:
        """Search by raw embedding vector.

        Args:
            embedding: Pre-computed embedding vector.
            k: Number of results.

        Returns:
            List of LangChain Document objects.

        Example::

            docs = store.similarity_search_by_vector([0.1, 0.2, ...], k=3)
        """
        search_results = self._client.vector.search(
            self._index_name,
            embedding,
            k=k,
            field_name=self._vector_field,
            session_key=self._session_key,
        )

        docs: list[Document] = []
        for sr in search_results:
            content = sr.fields.get(self._content_field, "")
            metadata: dict[str, Any] = {"key": sr.key, "score": sr.score}
            for field_key, field_val in sr.fields.items():
                if field_key.startswith(self._metadata_field):
                    metadata[field_key[len(self._metadata_field):]] = field_val
                elif field_key != self._content_field:
                    metadata[field_key] = field_val
            docs.append(Document(page_content=content, metadata=metadata))

        return docs

    def as_retriever(self, **kwargs: Any) -> BaseRetriever:  # noqa: ANN401 -- LangChain base contract
        """Return a LangChain retriever wrapping this vector store.

        Adds first-class support for ``search_type="hybrid"`` (which the
        upstream LangChain ``VectorStoreRetriever`` rejects in its allowed-list
        validator). For ``"similarity"`` / ``"mmr"`` /
        ``"similarity_score_threshold"`` the call is forwarded to the base
        implementation verbatim, preserving every existing API contract.

        Args:
            search_type: Optional retrieval mode. ``"similarity"`` (default),
                ``"hybrid"`` (BM25+dense RRF via Moon), ``"mmr"``, or
                ``"similarity_score_threshold"`` (handled upstream).
            search_kwargs: Dict forwarded to ``similarity_search`` on each
                ``invoke()`` — common keys are ``k``, ``hybrid_weights``,
                ``k_per_stream``.

        Returns:
            A LangChain :class:`BaseRetriever`. For hybrid mode this is a
            dedicated :class:`MoonHybridRetriever` that re-dispatches through
            ``similarity_search(search_type="hybrid", ...)``.

        Example::

            retriever = store.as_retriever(
                search_type="hybrid",
                search_kwargs={"k": 5, "hybrid_weights": (1.0, 1.5, 0.0)},
            )
            docs = retriever.invoke("auth crash on startup")
        """
        search_type = kwargs.get("search_type", "similarity")
        if search_type == "hybrid":
            search_kwargs = dict(kwargs.get("search_kwargs") or {})
            return MoonHybridRetriever(vectorstore=self, search_kwargs=search_kwargs)

        if search_type not in ("similarity", "mmr", "similarity_score_threshold"):
            raise ValueError(
                f"Unsupported search_type: {search_type!r}. Valid: "
                f"'similarity', 'hybrid', 'mmr', 'similarity_score_threshold'"
            )

        return super().as_retriever(**kwargs)

    def delete(
        self,
        ids: list[str] | None = None,
        **kwargs: Any,  # noqa: ANN401 -- LangChain VectorStore base contract
    ) -> bool | None:
        """Delete documents by ID.

        Args:
            ids: List of document IDs to delete.

        Returns:
            True if deletion succeeded.

        Example::

            store.delete(["doc:abc123", "doc:def456"])
        """
        if ids is None:
            return False
        pipeline = self._client.pipeline(transaction=False)
        for doc_id in ids:
            pipeline.delete(doc_id)
        pipeline.execute()
        return True

    @classmethod
    def from_texts(
        cls: type[MoonVectorStore],
        texts: list[str],
        embedding: Embeddings,
        metadatas: list[dict[str, Any]] | None = None,
        **kwargs: Any,  # noqa: ANN401 -- LangChain VectorStore base contract
    ) -> MoonVectorStore:
        """Create a MoonVectorStore from texts (class method).

        Args:
            texts: List of texts to embed and store.
            embedding: Embeddings model.
            metadatas: Optional metadata list.
            **kwargs: Passed to MoonVectorStore constructor.

        Returns:
            Initialized MoonVectorStore with texts added.

        Example::

            store = MoonVectorStore.from_texts(
                ["doc 1", "doc 2"],
                OpenAIEmbeddings(),
                index_name="my_idx",
            )
        """
        index_name = kwargs.pop("index_name", f"idx_{uuid.uuid4().hex[:8]}")
        store = cls(index_name=index_name, embedding=embedding, **kwargs)
        store.add_texts(texts, metadatas=metadatas)
        return store


class MoonHybridRetriever(BaseRetriever):
    """LangChain retriever that dispatches through Moon's hybrid search.

    Returned by :meth:`MoonVectorStore.as_retriever` when
    ``search_type="hybrid"`` is requested. Wraps the vector store so the
    standard LangChain ``invoke()`` / ``ainvoke()`` API surfaces fused BM25 +
    dense results with no extra wiring on the caller side.

    The class deliberately mirrors :class:`langchain_core.vectorstores.base.VectorStoreRetriever`
    in shape (``vectorstore``, ``search_kwargs`` fields) so downstream code that
    inspects the retriever (e.g. for chain composition) keeps working.

    Attributes:
        vectorstore: The underlying :class:`MoonVectorStore`.
        search_kwargs: Dict forwarded to ``similarity_search`` on each call.
            Common keys: ``k``, ``hybrid_weights``, ``k_per_stream``.

    Example::

        retriever = store.as_retriever(
            search_type="hybrid",
            search_kwargs={"k": 5, "hybrid_weights": (1.0, 1.5, 0.0)},
        )
        docs = retriever.invoke("auth crash")
    """

    vectorstore: MoonVectorStore
    search_kwargs: dict[str, Any] = {}

    # Pydantic v2 model config (replaces deprecated class-based Config) —
    # MoonVectorStore is not a pydantic model, so allow arbitrary types.
    model_config = {"arbitrary_types_allowed": True}

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> list[Document]:
        """Synchronous retrieval — re-dispatches through hybrid path."""
        del run_manager  # callbacks are handled by the BaseRetriever wrapper
        kwargs = dict(self.search_kwargs)
        k = kwargs.pop("k", 4)
        return self.vectorstore.similarity_search(
            query,
            k=k,
            search_type="hybrid",
            **kwargs,
        )
