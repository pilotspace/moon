"""Cross-adapter hybrid retrieval unit tests for MoonDB.

Covers the LangChain and LlamaIndex MoonVectorStore extensions that ship in
Plan 153-02:

- LangChain: ``similarity_search(search_type="hybrid", ...)`` and
  ``as_retriever(search_type="hybrid", ...)``
- LlamaIndex: ``query(VectorStoreQuery(mode=HYBRID|TEXT_SEARCH, ...))``
- Cross-cutting concerns: hybrid weights mapping (LangChain default + custom,
  LlamaIndex ``alpha`` + clamping), per-stream score metadata, mode guards,
  optional-dep ImportError preservation, Unicode + empty-query edges.

All tests are mock-based — no real Moon server required. They are dependency-
gated: skipped if ``langchain_core`` or ``llama_index.core`` is missing.
"""

from __future__ import annotations

import importlib
import math
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# -- Dependency guards (mirrors test_integrations.py mock_deps pattern) -------


def _skip_if_no_langchain() -> None:
    try:
        import langchain_core  # noqa: F401
    except ImportError:
        pytest.skip("langchain-core not installed")


def _skip_if_no_llamaindex() -> None:
    try:
        import llama_index.core  # noqa: F401
    except ImportError:
        pytest.skip("llama-index-core not installed")


@pytest.fixture
def lc_deps() -> None:
    """Skip if langchain-core not installed."""
    _skip_if_no_langchain()


@pytest.fixture
def li_deps() -> None:
    """Skip if llama-index-core not installed."""
    _skip_if_no_llamaindex()


# -- Helpers -----------------------------------------------------------------


def _build_langchain_store(
    *,
    hybrid_return: list[Any] | None = None,
    embedding_dim: int = 4,
) -> tuple[Any, MagicMock, MagicMock]:
    """Construct a LangChain MoonVectorStore wired to mocks."""
    from moondb.integrations.langchain import MoonVectorStore

    mock_embedding = MagicMock()
    mock_embedding.embed_query.return_value = [0.1] * embedding_dim

    mock_client = MagicMock()
    mock_client.text = MagicMock()
    mock_client.text.hybrid_search.return_value = hybrid_return or []

    with patch.object(MoonVectorStore, "_ensure_index"):
        store = MoonVectorStore(
            index_name="test",
            embedding=mock_embedding,
            moon_client=mock_client,
            dim=embedding_dim,
            create_index=False,
        )

    return store, mock_client, mock_embedding


def _build_llamaindex_store(
    *,
    hybrid_return: list[Any] | None = None,
    text_return: list[Any] | None = None,
    embedding_dim: int = 4,
) -> tuple[Any, MagicMock]:
    """Construct a LlamaIndex MoonVectorStore wired to mocks."""
    from moondb.integrations.llamaindex import MoonVectorStore

    mock_client = MagicMock()
    mock_client.text = MagicMock()
    mock_client.text.hybrid_search.return_value = hybrid_return or []
    mock_client.text.text_search.return_value = text_return or []

    with patch.object(MoonVectorStore, "_ensure_client", return_value=mock_client), \
         patch.object(MoonVectorStore, "_ensure_index"):
        store = MoonVectorStore(index_name="test", dim=embedding_dim)
        store._client = mock_client

    return store, mock_client


# =============================================================================
# Hybrid weights mapping
# =============================================================================


class TestHybridWeightsMapping:
    """Verify weight construction across both adapters."""

    def test_langchain_default_weights_no_sparse(self, lc_deps: None) -> None:
        """Default LangChain hybrid weights are (1.0, 1.0, 0.0)."""
        from moondb.types import TextSearchHit

        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d", score=1.0, fields={})],
        )

        store.similarity_search("q", k=3, search_type="hybrid")

        kwargs = mock_client.text.hybrid_search.call_args.kwargs
        assert kwargs["weights"] == (1.0, 1.0, 0.0)

    def test_langchain_custom_weights(self, lc_deps: None) -> None:
        """Custom hybrid_weights pass through verbatim to TextCommands."""
        from moondb.types import TextSearchHit

        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d", score=1.0, fields={})],
        )

        store.similarity_search(
            "q",
            k=3,
            search_type="hybrid",
            hybrid_weights=(1.0, 1.5, 0.0),
        )

        assert mock_client.text.hybrid_search.call_args.kwargs["weights"] == (
            1.0,
            1.5,
            0.0,
        )

    def test_langchain_k_per_stream_passthrough(self, lc_deps: None) -> None:
        """k_per_stream forwards to the TextCommands call."""
        from moondb.types import TextSearchHit

        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d", score=1.0, fields={})],
        )

        store.similarity_search(
            "q", k=3, search_type="hybrid", k_per_stream=20
        )

        assert mock_client.text.hybrid_search.call_args.kwargs["k_per_stream"] == 20

    def test_llamaindex_alpha_mapping(self, li_deps: None) -> None:
        """LlamaIndex alpha=0.7 → weights (0.3, 0.7, 0.0) (canonical mapping)."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, mock_client = _build_llamaindex_store(
            hybrid_return=[TextSearchHit(id="n", score=1.0, fields={})],
        )

        store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=3,
            alpha=0.7,
        ))

        weights = mock_client.text.hybrid_search.call_args.kwargs["weights"]
        # Float arithmetic: 1.0 - 0.7 yields ~0.30000000000000004
        assert math.isclose(weights[0], 0.3, abs_tol=1e-9)
        assert math.isclose(weights[1], 0.7, abs_tol=1e-9)
        assert weights[2] == 0.0

    def test_llamaindex_alpha_clamped_above_one(self, li_deps: None) -> None:
        """alpha=1.5 clamps to (0.0, 1.0, 0.0) — no negative weight reaches the wire."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, mock_client = _build_llamaindex_store(
            hybrid_return=[TextSearchHit(id="n", score=1.0, fields={})],
        )

        store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=3,
            alpha=1.5,
        ))

        assert mock_client.text.hybrid_search.call_args.kwargs["weights"] == (
            0.0,
            1.0,
            0.0,
        )

    def test_llamaindex_alpha_clamped_below_zero(self, li_deps: None) -> None:
        """alpha=-0.5 clamps to (1.0, 0.0, 0.0) — pure BM25."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, mock_client = _build_llamaindex_store(
            hybrid_return=[TextSearchHit(id="n", score=1.0, fields={})],
        )

        store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=3,
            alpha=-0.5,
        ))

        assert mock_client.text.hybrid_search.call_args.kwargs["weights"] == (
            1.0,
            0.0,
            0.0,
        )


# =============================================================================
# Mode guards (LlamaIndex)
# =============================================================================


class TestHybridModeGuards:
    """Verify mode-dispatch error paths and TEXT_SEARCH branch."""

    def test_llamaindex_hybrid_requires_query_str(self, li_deps: None) -> None:
        """HYBRID mode without query_str must raise ValueError."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        store, _ = _build_llamaindex_store()

        with pytest.raises(ValueError, match="HYBRID mode requires"):
            store.query(VectorStoreQuery(
                query_embedding=[0.1] * 4,
                mode=VectorStoreQueryMode.HYBRID,
            ))

    def test_llamaindex_hybrid_requires_query_embedding(self, li_deps: None) -> None:
        """HYBRID mode without query_embedding must raise ValueError."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        store, _ = _build_llamaindex_store()

        with pytest.raises(ValueError, match="HYBRID mode requires"):
            store.query(VectorStoreQuery(
                query_str="q",
                mode=VectorStoreQueryMode.HYBRID,
            ))

    def test_llamaindex_text_search_mode(self, li_deps: None) -> None:
        """TEXT_SEARCH dispatches to text.text_search, NOT text.hybrid_search."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, mock_client = _build_llamaindex_store(
            text_return=[TextSearchHit(id="n:1", score=2.5, fields={"content": "A"})],
        )

        result = store.query(VectorStoreQuery(
            query_str="memory leak",
            mode=VectorStoreQueryMode.TEXT_SEARCH,
            similarity_top_k=4,
        ))

        assert len(result.nodes) == 1
        assert result.nodes[0].text == "A"
        mock_client.text.text_search.assert_called_once()
        mock_client.text.hybrid_search.assert_not_called()
        mock_client.vector.search.assert_not_called()
        kwargs = mock_client.text.text_search.call_args.kwargs
        assert kwargs["limit"] == 4

    def test_llamaindex_text_search_empty_query_returns_empty(self, li_deps: None) -> None:
        """TEXT_SEARCH with empty/whitespace query_str returns empty result."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        store, mock_client = _build_llamaindex_store()

        result = store.query(VectorStoreQuery(
            query_str="   ",  # whitespace
            mode=VectorStoreQueryMode.TEXT_SEARCH,
        ))

        assert result.nodes == []
        assert result.similarities == []
        assert result.ids == []
        mock_client.text.text_search.assert_not_called()

    def test_llamaindex_unsupported_mode_raises(self, li_deps: None) -> None:
        """MMR (or any unhandled mode) must raise ValueError -- no silent fallthrough."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        store, _ = _build_llamaindex_store()

        with pytest.raises(ValueError, match="Unsupported VectorStoreQueryMode"):
            store.query(VectorStoreQuery(
                query_embedding=[0.1] * 4,
                mode=VectorStoreQueryMode.MMR,
            ))


# =============================================================================
# Score mapping
# =============================================================================


class TestHybridScoreMapping:
    """Verify per-stream RRF scores and similarity score shape."""

    def test_langchain_stream_hits_metadata(self, lc_deps: None) -> None:
        """Per-stream __bm25_score__/__dense_score__ surface as metadata.stream_hits."""
        from moondb.types import TextSearchHit

        hit = TextSearchHit(
            id="doc:1",
            score=10.0,
            fields={
                "content": "Hello",
                "metadata_src": "kb",
                "__bm25_score__": "0.45",
                "__dense_score__": "0.32",
                "__sparse_score__": "0.0",
            },
        )

        store, mock_client, _ = _build_langchain_store(hybrid_return=[hit])

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        assert len(docs) == 1
        meta = docs[0].metadata
        assert "stream_hits" in meta
        assert math.isclose(meta["stream_hits"]["bm25"], 0.45)
        assert math.isclose(meta["stream_hits"]["dense"], 0.32)
        assert math.isclose(meta["stream_hits"]["sparse"], 0.0)
        # Stream score field markers must NOT pollute top-level metadata
        assert "__bm25_score__" not in meta
        assert "__dense_score__" not in meta
        assert "__sparse_score__" not in meta

    def test_langchain_no_stream_hits_when_absent(self, lc_deps: None) -> None:
        """Without __*_score__ fields, metadata.stream_hits is omitted (not empty {})."""
        from moondb.types import TextSearchHit

        store, _, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="doc:1", score=1.0, fields={"content": "x"})],
        )

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        assert "stream_hits" not in docs[0].metadata

    def test_llamaindex_text_similarity_is_bm25_score(self, li_deps: None) -> None:
        """TEXT_SEARCH similarities equal hit.score verbatim — no 1/(1+d) inversion."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, _ = _build_llamaindex_store(
            text_return=[
                TextSearchHit(id="n:1", score=4.2, fields={"content": "A"}),
                TextSearchHit(id="n:2", score=2.1, fields={"content": "B"}),
            ],
        )

        result = store.query(VectorStoreQuery(
            query_str="q",
            mode=VectorStoreQueryMode.TEXT_SEARCH,
            similarity_top_k=2,
        ))

        # BM25 / RRF scores are higher-is-better; passed through unchanged.
        assert result.similarities == [4.2, 2.1]

    def test_llamaindex_hybrid_stream_hits_in_metadata(self, li_deps: None) -> None:
        """Hybrid mode surfaces per-stream scores into node.metadata.stream_hits."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        hit = TextSearchHit(
            id="n:1",
            score=8.0,
            fields={
                "content": "X",
                "_node_id": "abc",
                "__bm25_score__": "0.5",
                "__dense_score__": "0.7",
            },
        )

        store, _ = _build_llamaindex_store(hybrid_return=[hit])

        result = store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=1,
        ))

        meta = result.nodes[0].metadata
        assert "stream_hits" in meta
        assert math.isclose(meta["stream_hits"]["bm25"], 0.5)
        assert math.isclose(meta["stream_hits"]["dense"], 0.7)


# =============================================================================
# Optional-dep guards (threat_model T-153-02-01 / T-153-02-02)
# =============================================================================


class TestOptionalDepGuards:
    """Verify ImportError-with-install-hint preserved for both adapters."""

    def test_langchain_guard_preserved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Importing langchain adapter without langchain_core raises ImportError."""
        # Drop the adapter and ALL langchain_core submodules first so the
        # try/except fires fresh on re-import.
        for mod_name in list(sys.modules):
            if mod_name.startswith("langchain_core") or mod_name == (
                "moondb.integrations.langchain"
            ):
                monkeypatch.delitem(sys.modules, mod_name, raising=False)

        # Then mark langchain_core as None — Python's import machinery treats
        # a None entry as "not importable" and raises ImportError.
        monkeypatch.setitem(sys.modules, "langchain_core", None)

        with pytest.raises(ImportError) as exc_info:
            importlib.import_module("moondb.integrations.langchain")

        # Install hint must be present so the user knows how to fix it.
        assert "moondb[langchain]" in str(exc_info.value)

    def test_llamaindex_guard_preserved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Importing llamaindex adapter without llama_index.core raises ImportError."""
        # Drop the adapter and ALL llama_index submodules first.
        for mod_name in list(sys.modules):
            if mod_name.startswith("llama_index") or mod_name == (
                "moondb.integrations.llamaindex"
            ):
                monkeypatch.delitem(sys.modules, mod_name, raising=False)

        monkeypatch.setitem(sys.modules, "llama_index", None)
        monkeypatch.setitem(sys.modules, "llama_index.core", None)

        with pytest.raises(ImportError) as exc_info:
            importlib.import_module("moondb.integrations.llamaindex")

        assert "moondb[llamaindex]" in str(exc_info.value)


# =============================================================================
# Unicode + empty-query edges
# =============================================================================


class TestUnicodeAndEmpty:
    """Verify Unicode and empty-query passthrough invariants."""

    def test_langchain_hybrid_unicode_query(self, lc_deps: None) -> None:
        """Unicode query strings pass through to TextCommands.hybrid_search verbatim."""
        from moondb.types import TextSearchHit

        unicode_q = "café résumé 🎉"
        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d", score=1.0, fields={"content": "x"})],
        )

        store.similarity_search(unicode_q, k=2, search_type="hybrid")

        # Positional args[1] of hybrid_search is the query string
        call = mock_client.text.hybrid_search.call_args
        assert call.args[1] == unicode_q

    def test_langchain_hybrid_empty_query_passes_through(self, lc_deps: None) -> None:
        """Empty query string passes through; server-side resolves to match-all."""
        from moondb.types import TextSearchHit

        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d", score=1.0, fields={"content": "x"})],
        )

        store.similarity_search("", k=2, search_type="hybrid")

        # Empty string travels to TextCommands; match-all coercion is its job.
        assert mock_client.text.hybrid_search.call_args.args[1] == ""

    def test_langchain_k_zero_returns_empty(self, lc_deps: None) -> None:
        """k=0 short-circuits to an empty list (server returns nothing)."""
        store, mock_client, _ = _build_langchain_store(hybrid_return=[])

        docs = store.similarity_search("q", k=0, search_type="hybrid")

        assert docs == []
        # Even with k=0 we still dispatch (server-side enforces the limit).
        mock_client.text.hybrid_search.assert_called_once()
        assert mock_client.text.hybrid_search.call_args.kwargs["limit"] == 0


# =============================================================================
# LangChain as_retriever hybrid wiring
# =============================================================================


class TestLangChainHybridRetriever:
    """Verify as_retriever(search_type='hybrid') wires through hybrid path."""

    def test_as_retriever_hybrid_returns_hybrid_retriever(self, lc_deps: None) -> None:
        """as_retriever(search_type='hybrid') returns a MoonHybridRetriever instance."""
        from moondb.integrations.langchain import MoonHybridRetriever

        store, _, _ = _build_langchain_store()

        retriever = store.as_retriever(
            search_type="hybrid",
            search_kwargs={"k": 5, "hybrid_weights": (1.0, 1.5, 0.0)},
        )

        assert isinstance(retriever, MoonHybridRetriever)
        assert retriever.vectorstore is store
        assert retriever.search_kwargs == {"k": 5, "hybrid_weights": (1.0, 1.5, 0.0)}

    def test_as_retriever_hybrid_invoke_dispatches(self, lc_deps: None) -> None:
        """retriever.invoke('q') calls similarity_search(search_type='hybrid', ...)."""
        from moondb.types import TextSearchHit

        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d", score=2.0, fields={"content": "Hi"})],
        )

        retriever = store.as_retriever(
            search_type="hybrid",
            search_kwargs={"k": 3, "hybrid_weights": (1.0, 1.0, 0.0)},
        )

        docs = retriever.invoke("auth crash")

        assert len(docs) == 1
        assert docs[0].page_content == "Hi"
        mock_client.text.hybrid_search.assert_called_once()
        kwargs = mock_client.text.hybrid_search.call_args.kwargs
        assert kwargs["limit"] == 3
        assert kwargs["weights"] == (1.0, 1.0, 0.0)

    def test_as_retriever_default_delegates_upstream(self, lc_deps: None) -> None:
        """search_type='similarity' (default) returns the upstream LangChain retriever."""
        from moondb.integrations.langchain import MoonHybridRetriever

        store, _, _ = _build_langchain_store()

        retriever = store.as_retriever(search_type="similarity", search_kwargs={"k": 2})

        # Should NOT be MoonHybridRetriever — it must be the LangChain default.
        assert not isinstance(retriever, MoonHybridRetriever)
