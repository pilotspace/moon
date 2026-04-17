"""Tests for framework integrations (LangChain, LlamaIndex).

Tests mock the external framework classes and verify the adapters
build correct commands and parse responses properly.
"""

from __future__ import annotations

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

    def test_similarity_search_hybrid_dispatches_to_text_hybrid_search(
        self, mock_deps: None
    ) -> None:
        """search_type='hybrid' must call client.text.hybrid_search, not vector.search."""
        from moondb.integrations.langchain import MoonVectorStore
        from moondb.types import TextSearchHit

        mock_embedding = MagicMock()
        mock_embedding.embed_query.return_value = [0.1] * 4

        mock_client = MagicMock()
        mock_client.text = MagicMock()
        mock_client.text.hybrid_search.return_value = [
            TextSearchHit(id="doc:1", score=5.0, fields={"content": "Hi"}),
        ]

        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                embedding=mock_embedding,
                moon_client=mock_client,
                dim=4,
                create_index=False,
            )

        docs = store.similarity_search(
            "q", k=3, search_type="hybrid", hybrid_weights=(1.0, 1.5, 0.0)
        )

        assert len(docs) == 1
        assert docs[0].page_content == "Hi"
        assert docs[0].metadata["score"] == 5.0
        assert docs[0].metadata["key"] == "doc:1"
        # Hybrid dispatch invariant: vector.search must NOT be called
        mock_client.vector.search.assert_not_called()
        mock_client.text.hybrid_search.assert_called_once()
        kwargs = mock_client.text.hybrid_search.call_args.kwargs
        assert kwargs["weights"] == (1.0, 1.5, 0.0)
        assert kwargs["limit"] == 3
        assert kwargs["vector_field"] == "vec"

    def test_unsupported_search_type_raises(self, mock_deps: None) -> None:
        """Unknown search_type must raise ValueError (no silent fallthrough)."""
        from moondb.integrations.langchain import MoonVectorStore

        mock_embedding = MagicMock()
        mock_embedding.embed_query.return_value = [0.1] * 4
        mock_client = MagicMock()

        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                embedding=mock_embedding,
                moon_client=mock_client,
                dim=4,
                create_index=False,
            )

        with pytest.raises(ValueError, match="Unsupported search_type"):
            store.similarity_search("q", search_type="nope")


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

    def test_query_hybrid_dispatches_to_text_hybrid_search(
        self, mock_deps: None
    ) -> None:
        """Mode=HYBRID must call client.text.hybrid_search, not vector.search."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.integrations.llamaindex import MoonVectorStore
        from moondb.types import TextSearchHit

        mock_client = MagicMock()
        mock_client.text = MagicMock()
        mock_client.text.hybrid_search.return_value = [
            TextSearchHit(
                id="node:2",
                score=8.5,
                fields={"content": "World", "_node_id": "xyz", "meta_src": "q"},
            ),
        ]

        with patch.object(MoonVectorStore, "_ensure_client", return_value=mock_client), \
             patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(index_name="test", dim=4)
            store._client = mock_client

        q = VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="crash",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=3,
        )
        result = store.query(q)

        assert len(result.nodes) == 1
        assert result.nodes[0].text == "World"
        assert result.nodes[0].id_ == "xyz"
        # text scores are higher-is-better; passed through verbatim (no inversion)
        assert result.similarities == [8.5]
        # Hybrid dispatch invariant: vector.search must NOT be called
        mock_client.vector.search.assert_not_called()
        mock_client.text.hybrid_search.assert_called_once()

    # -- Gap 4 fix (Plan 153-05): moon_client kwarg must be preserved -------

    def test_moon_client_kwarg_is_preserved(self, mock_deps: None) -> None:
        """MoonVectorStore(moon_client=c, ...) must retain the supplied client.

        Gap 4 regression guard: prior to Plan 153-05, ``__init__`` reset
        ``self._client = None`` after ``super().__init__(**kwargs)``,
        discarding any caller-supplied client and routing commands to a
        fresh default ``moon_url`` client instead.
        """
        from moondb.integrations.llamaindex import MoonVectorStore

        mock_client = MagicMock(name="user-supplied-client")
        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                moon_client=mock_client,
                dim=4,
            )

        # Canonical pydantic-v2 storage (or object.__setattr__ fallback).
        assert store._client is mock_client
        # _ensure_client must return the same instance (no re-creation).
        assert store._ensure_client() is mock_client

    def test_moon_client_kwarg_uses_pydantic_private_storage(
        self, mock_deps: None
    ) -> None:
        """Lock the canonical PrivateAttr storage path (WARNING 5 iter-1 fix).

        Asserts the supplied client lands in ``__pydantic_private__["_client"]``
        — the pydantic-v2 canonical private-attribute storage shape. If this
        test fails with KeyError/AttributeError, the implementation has fallen
        back to ``object.__setattr__`` and SUMMARY.md must document it.
        """
        from moondb.integrations.llamaindex import MoonVectorStore

        mock_client = MagicMock()
        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(index_name="t", moon_client=mock_client, dim=4)

        # pydantic-v2 private-attr storage — present iff PrivateAttr path is live
        assert hasattr(store, "__pydantic_private__")
        assert store.__pydantic_private__.get("_client") is mock_client

    def test_moon_client_default_still_works(self, mock_deps: None) -> None:
        """Without moon_client kwarg, _ensure_client falls back to MoonClient.from_url."""
        from moondb.integrations.llamaindex import MoonVectorStore

        with patch(
            "moondb.integrations.llamaindex.MoonClient.from_url",
            return_value=MagicMock(name="default-url-client"),
        ) as from_url, patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                moon_url="redis://example:6379",
                dim=4,
            )

        # from_url called exactly once, with the declared moon_url.
        from_url.assert_called_once_with("redis://example:6379")
        # _ensure_client on subsequent calls reuses the cached client.
        assert store._ensure_client() is from_url.return_value

    def test_moon_client_kwarg_does_not_leak_into_pydantic_model_dump(
        self, mock_deps: None
    ) -> None:
        """moon_client popped pre-super() + PrivateAttr storage means model_dump() is clean.

        Threat T-153-05-02: a live MoonClient leaking into model_dump would
        be serialised into logs / telemetry / checkpoints. Neither the
        non-declared ``moon_client`` kwarg nor the private ``_client``
        attribute must appear in the dumped representation.
        """
        from moondb.integrations.llamaindex import MoonVectorStore

        mock_client = MagicMock()
        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(index_name="t", moon_client=mock_client, dim=4)

        dump = store.model_dump()
        assert "moon_client" not in dump
        assert "_client" not in dump
