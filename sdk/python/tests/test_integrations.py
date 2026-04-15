"""Tests for framework integrations (LangChain, LlamaIndex).

Tests mock the external framework classes and verify the adapters
build correct commands and parse responses properly.
"""

from __future__ import annotations

import struct
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest


# -- LangChain integration tests --


class TestLangChainAdapter:
    """Test LangChain VectorStore adapter."""

    @pytest.fixture
    def mock_deps(self) -> None:
        """Skip if langchain-core not installed."""
        try:
            import langchain_core  # noqa: F401
        except ImportError:
            pytest.skip("langchain-core not installed")

    def test_import(self, mock_deps: None) -> None:
        from moondb.integrations.langchain import MoonVectorStore
        assert MoonVectorStore is not None

    def test_add_texts_builds_pipeline(self, mock_deps: None) -> None:
        from moondb.integrations.langchain import MoonVectorStore

        mock_embedding = MagicMock()
        mock_embedding.embed_query.return_value = [0.1] * 4
        mock_embedding.embed_documents.return_value = [[0.1] * 4, [0.2] * 4]

        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline

        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                embedding=mock_embedding,
                moon_client=mock_client,
                dim=4,
                create_index=False,
            )

        ids = store.add_texts(["hello", "world"])

        assert len(ids) == 2
        assert mock_pipeline.hset.call_count == 2
        mock_pipeline.execute.assert_called_once()

    def test_similarity_search_calls_vector_search(self, mock_deps: None) -> None:
        from moondb.integrations.langchain import MoonVectorStore

        mock_embedding = MagicMock()
        mock_embedding.embed_query.return_value = [0.1] * 4

        mock_client = MagicMock()
        mock_client.vector = MagicMock()
        mock_client.vector.search.return_value = [
            MagicMock(
                key="doc:1", score=0.1,
                fields={"content": "Hello", "metadata_source": "test"},
            ),
        ]

        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                embedding=mock_embedding,
                moon_client=mock_client,
                dim=4,
                create_index=False,
            )

        docs = store.similarity_search("query", k=3)
        assert len(docs) == 1
        assert docs[0].page_content == "Hello"
        mock_client.vector.search.assert_called_once()

    def test_from_texts_class_method(self, mock_deps: None) -> None:
        from moondb.integrations.langchain import MoonVectorStore

        mock_embedding = MagicMock()
        mock_embedding.embed_query.return_value = [0.1] * 4
        mock_embedding.embed_documents.return_value = [[0.1] * 4]

        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline

        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore.from_texts(
                ["hello"],
                mock_embedding,
                index_name="test",
                moon_client=mock_client,
                dim=4,
                create_index=False,
            )

        assert store is not None


# -- LlamaIndex integration tests --


class TestLlamaIndexAdapter:
    """Test LlamaIndex VectorStore adapter."""

    @pytest.fixture
    def mock_deps(self) -> None:
        """Skip if llama-index-core not installed."""
        try:
            import llama_index.core  # noqa: F401
        except ImportError:
            pytest.skip("llama-index-core not installed")

    def test_import(self, mock_deps: None) -> None:
        from moondb.integrations.llamaindex import MoonVectorStore
        assert MoonVectorStore is not None

    def test_add_nodes(self, mock_deps: None) -> None:
        from llama_index.core.schema import TextNode
        from moondb.integrations.llamaindex import MoonVectorStore

        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline

        with patch.object(MoonVectorStore, "_ensure_client", return_value=mock_client), \
             patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(index_name="test", dim=4)
            store._client = mock_client

        nodes = [
            TextNode(text="Hello", embedding=[0.1] * 4),
            TextNode(text="World", embedding=[0.2] * 4),
        ]

        ids = store.add(nodes)
        assert len(ids) == 2
        assert mock_pipeline.hset.call_count == 2

    def test_query(self, mock_deps: None) -> None:
        from llama_index.core.vector_stores.types import VectorStoreQuery
        from moondb.integrations.llamaindex import MoonVectorStore

        mock_client = MagicMock()
        mock_client.vector = MagicMock()
        mock_client.vector.search.return_value = [
            MagicMock(
                key="node:1", score=0.1,
                fields={"content": "Hello", "_node_id": "abc"},
                graph_hops=None,
            ),
        ]

        with patch.object(MoonVectorStore, "_ensure_client", return_value=mock_client), \
             patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(index_name="test", dim=4)
            store._client = mock_client

        q = VectorStoreQuery(query_embedding=[0.1] * 4, similarity_top_k=5)
        result = store.query(q)

        assert len(result.nodes) == 1
        assert result.nodes[0].text == "Hello"
        assert len(result.similarities) == 1
        mock_client.vector.search.assert_called_once()
