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

import uuid
from typing import Any, Dict, List, Optional, Sequence

try:
    from llama_index.core.schema import BaseNode, MetadataMode, TextNode
    from llama_index.core.vector_stores.types import (
        BasePydanticVectorStore,
        VectorStoreQuery,
        VectorStoreQueryResult,
    )
    from llama_index.core.vector_stores.utils import (
        metadata_dict_to_node,
        node_to_metadata_dict,
    )
except ImportError as e:
    raise ImportError(
        "llama-index-core is required for the LlamaIndex integration. "
        "Install with: pip install moondb[llamaindex]"
    ) from e

from ..client import MoonClient
from ..types import encode_vector


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
    expand_graph: Optional[str] = None
    expand_depth: int = 2

    # Private client (not serialized)
    _client: Optional[MoonClient] = None

    class Config:
        """Pydantic config."""

        arbitrary_types_allowed = True

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = None
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
        try:
            client.vector.create_index(
                self.index_name,
                prefix=self.prefix,
                field_name=self.vector_field,
                dim=self.dim,
                metric=self.metric,
                extra_schema=[self.content_field, "TEXT"],
            )
        except Exception:
            pass  # Index may already exist

    @property
    def client(self) -> MoonClient:
        """Return the underlying MoonClient."""
        return self._ensure_client()

    def add(
        self,
        nodes: List[BaseNode],
        **add_kwargs: Any,
    ) -> List[str]:
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
        ids: List[str] = []

        pipeline = client.pipeline(transaction=False)
        for node in nodes:
            node_id = node.node_id or uuid.uuid4().hex[:12]
            key = f"{self.prefix}{node_id}"

            embedding = node.get_embedding()
            if embedding is None:
                continue

            text = node.get_content(metadata_mode=MetadataMode.NONE)

            mapping: Dict[str, Any] = {
                self.vector_field: encode_vector(embedding),
                self.content_field: text,
                "_node_id": node_id,
            }

            # Store metadata
            try:
                meta_dict = node_to_metadata_dict(node, remove_text=True)
                for mk, mv in meta_dict.items():
                    mapping[f"meta_{mk}"] = str(mv)
            except Exception:
                # Graceful fallback if metadata serialization fails
                if hasattr(node, "metadata") and node.metadata:
                    for mk, mv in node.metadata.items():
                        mapping[f"meta_{mk}"] = str(mv)

            pipeline.hset(key, mapping=mapping)  # type: ignore[arg-type]
            ids.append(node_id)

        pipeline.execute()
        return ids

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
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
        **kwargs: Any,
    ) -> VectorStoreQueryResult:
        """Query the vector store.

        Args:
            query: LlamaIndex VectorStoreQuery with embedding and parameters.

        Returns:
            VectorStoreQueryResult with nodes, similarities, and IDs.

        Example::

            from llama_index.core.vector_stores.types import VectorStoreQuery

            q = VectorStoreQuery(query_embedding=[0.1, 0.2, ...], similarity_top_k=5)
            result = vector_store.query(q)
            for node, score in zip(result.nodes, result.similarities):
                print(node.text, score)
        """
        client = self._ensure_client()

        if query.query_embedding is None:
            return VectorStoreQueryResult(nodes=[], similarities=[], ids=[])

        k = query.similarity_top_k or 10

        search_results = client.vector.search(
            self.index_name,
            query.query_embedding,
            k=k,
            field_name=self.vector_field,
            expand_graph=self.expand_graph,
            expand_depth=self.expand_depth,
        )

        nodes: List[TextNode] = []
        similarities: List[float] = []
        ids: List[str] = []

        for sr in search_results:
            text = sr.fields.get(self.content_field, "")
            node_id = sr.fields.get("_node_id", sr.key)

            # Reconstruct metadata
            metadata: Dict[str, Any] = {}
            for fk, fv in sr.fields.items():
                if fk.startswith("meta_"):
                    metadata[fk[5:]] = fv
                elif fk not in (self.content_field, self.vector_field, "_node_id"):
                    metadata[fk] = fv

            if sr.graph_hops is not None:
                metadata["__graph_hops"] = sr.graph_hops

            node = TextNode(
                text=text,
                id_=node_id,
                metadata=metadata,
            )

            nodes.append(node)
            # Convert distance to similarity (1 / (1 + distance) for L2)
            similarity = 1.0 / (1.0 + sr.score) if sr.score >= 0 else 0.0
            similarities.append(similarity)
            ids.append(node_id)

        return VectorStoreQueryResult(
            nodes=nodes,
            similarities=similarities,
            ids=ids,
        )
