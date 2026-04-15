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

import hashlib
import uuid
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type

try:
    from langchain_core.documents import Document
    from langchain_core.embeddings import Embeddings
    from langchain_core.vectorstores import VectorStore
except ImportError as e:
    raise ImportError(
        "langchain-core is required for the LangChain integration. "
        "Install with: pip install moondb[langchain]"
    ) from e

from ..client import MoonClient
from ..types import SearchResult, encode_vector


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
        moon_client: Optional[MoonClient] = None,
        prefix: str = "doc:",
        vector_field: str = "vec",
        content_field: str = "content",
        metadata_field: str = "metadata_",
        dim: Optional[int] = None,
        metric: str = "COSINE",
        session_key: Optional[str] = None,
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
        try:
            self._client.vector.create_index(
                self._index_name,
                prefix=self._prefix,
                field_name=self._vector_field,
                dim=self._dim,
                metric=self._metric,
                extra_schema=[self._content_field, "TEXT"],
            )
        except Exception:
            # Index may already exist -- ignore
            pass

    @property
    def embeddings(self) -> Embeddings:
        """Return the embedding model."""
        return self._embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        *,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
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
            ids = [f"{self._prefix}{id_}" if not id_.startswith(self._prefix) else id_ for id_ in ids]

        pipeline = self._client.pipeline(transaction=False)
        for doc_id, text, vector, meta in zip(ids, texts_list, vectors, metadatas):
            mapping: Dict[str, Any] = {
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
        **kwargs: Any,
    ) -> List[Document]:
        """Search for documents similar to the query text.

        Args:
            query: Query text to embed and search.
            k: Number of results to return (default 4).

        Returns:
            List of LangChain Document objects.

        Example::

            docs = store.similarity_search("machine learning", k=3)
            for doc in docs:
                print(doc.page_content)
        """
        results = self.similarity_search_with_score(query, k=k, **kwargs)
        return [doc for doc, _ in results]

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Tuple[Document, float]]:
        """Search with relevance scores.

        Args:
            query: Query text.
            k: Number of results.

        Returns:
            List of (Document, score) tuples. Lower score = more similar.

        Example::

            results = store.similarity_search_with_score("AI", k=5)
            for doc, score in results:
                print(f"{score:.4f} {doc.page_content}")
        """
        vector = self._embedding.embed_query(query)

        search_results = self._client.vector.search(
            self._index_name,
            vector,
            k=k,
            field_name=self._vector_field,
            session_key=self._session_key,
        )

        docs_and_scores: List[Tuple[Document, float]] = []
        for sr in search_results:
            content = sr.fields.get(self._content_field, "")
            metadata: Dict[str, Any] = {"key": sr.key, "score": sr.score}

            # Extract metadata fields
            for field_key, field_val in sr.fields.items():
                if field_key.startswith(self._metadata_field):
                    clean_key = field_key[len(self._metadata_field):]
                    metadata[clean_key] = field_val
                elif field_key != self._content_field:
                    metadata[field_key] = field_val

            docs_and_scores.append((
                Document(page_content=content, metadata=metadata),
                sr.score,
            ))

        return docs_and_scores

    def similarity_search_by_vector(
        self,
        embedding: List[float],
        k: int = 4,
        **kwargs: Any,
    ) -> List[Document]:
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

        docs: List[Document] = []
        for sr in search_results:
            content = sr.fields.get(self._content_field, "")
            metadata: Dict[str, Any] = {"key": sr.key, "score": sr.score}
            for field_key, field_val in sr.fields.items():
                if field_key.startswith(self._metadata_field):
                    metadata[field_key[len(self._metadata_field):]] = field_val
                elif field_key != self._content_field:
                    metadata[field_key] = field_val
            docs.append(Document(page_content=content, metadata=metadata))

        return docs

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> Optional[bool]:
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
        cls: Type["MoonVectorStore"],
        texts: List[str],
        embedding: Embeddings,
        metadatas: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> "MoonVectorStore":
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
