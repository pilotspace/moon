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


def _normalise_hybrid_return(value: Any) -> tuple[Any, Any]:  # noqa: ANN401 -- mock duck-type
    """Shape-absorber for the ``hybrid_search`` mock return value (B-01 iter-3).

    Plan 153-04 switches the adapters to call
    ``client.text.hybrid_search(..., return_stream_hits=True)`` which returns
    a 2-tuple ``(hits, trailer_dict)``. Pre-existing test call sites pass
    ``hybrid_return=[hit, ...]`` as a plain list. This helper wraps a list
    into ``(list, {})``, passes 2-tuples through verbatim, and turns ``None``
    into ``([], {})``. Centralising here keeps the 21 existing call sites
    (lines 120, 133, 154, 173, 200, 227, 261, 276, 293, 318, 337, 370, 389,
    406, 442, 516, 530, 541, 563, 578, 600) byte-for-byte unchanged.
    """
    if value is None:
        return ([], {})
    if isinstance(value, tuple) and len(value) == 2:
        return value
    return (value, {})


def _build_langchain_store(
    *,
    hybrid_return: Any = None,  # noqa: ANN401 -- list or pre-wrapped 2-tuple
    embedding_dim: int = 4,
) -> tuple[Any, MagicMock, MagicMock]:
    """Construct a LangChain MoonVectorStore wired to mocks."""
    from moondb.integrations.langchain import MoonVectorStore

    mock_embedding = MagicMock()
    mock_embedding.embed_query.return_value = [0.1] * embedding_dim

    mock_client = MagicMock()
    mock_client.text = MagicMock()
    mock_client.text.hybrid_search.return_value = _normalise_hybrid_return(hybrid_return)

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
    hybrid_return: Any = None,  # noqa: ANN401 -- list or pre-wrapped 2-tuple
    text_return: list[Any] | None = None,
    embedding_dim: int = 4,
) -> tuple[Any, MagicMock]:
    """Construct a LlamaIndex MoonVectorStore wired to mocks."""
    from moondb.integrations.llamaindex import MoonVectorStore

    mock_client = MagicMock()
    mock_client.text = MagicMock()
    mock_client.text.hybrid_search.return_value = _normalise_hybrid_return(hybrid_return)
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


# =============================================================================
# LlamaIndex hybrid mode — end-to-end client-preservation (Plan 153-05 Gap 4)
# =============================================================================


class TestLlamaIndexHybridMode:
    """End-to-end verification that a user-supplied moon_client is actually
    the one invoked during a hybrid query.

    This class locks the Plan 153-05 fix at the integration boundary: not
    just "_client is c" (that is what test_integrations.py asserts) but
    "the hybrid query path routes through c.text.hybrid_search and nowhere
    else". Catches regressions where a future refactor reintroduces a
    shadow client at query time.
    """

    def test_llamaindex_hybrid_uses_supplied_client(self, li_deps: None) -> None:
        """HYBRID query on a store built with moon_client=c dispatches to c.text.hybrid_search."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.integrations.llamaindex import MoonVectorStore
        from moondb.types import TextSearchHit

        user_client = MagicMock(name="user-supplied-client")
        user_client.text = MagicMock()
        user_client.text.hybrid_search.return_value = [
            TextSearchHit(
                id="node:1",
                score=7.5,
                fields={"content": "answer", "_node_id": "abc"},
            ),
        ]

        # Patch _ensure_index only; DO NOT patch _ensure_client — we want
        # the real code path to resolve self._client from the popped kwarg.
        with patch.object(MoonVectorStore, "_ensure_index"):
            store = MoonVectorStore(
                index_name="test",
                moon_client=user_client,
                dim=4,
            )

        result = store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=1,
        ))

        # The supplied client's hybrid_search MUST have been invoked.
        user_client.text.hybrid_search.assert_called_once()
        # And the dense KNN path must NOT have been invoked.
        user_client.vector.search.assert_not_called()
        assert len(result.nodes) == 1
        assert result.nodes[0].text == "answer"


# =============================================================================
# Plan 153-04 — Hybrid parser bug fixes (Gap 2 / Gap 3 / Gap 5)
# =============================================================================


class TestLangChainHybridHydration:
    """Gap 2 + Gap 3 — LangChain hybrid must hydrate page_content + return k docs."""

    def test_hybrid_hydrates_via_hgetall_when_fields_empty(self, lc_deps: None) -> None:
        """Empty hit.fields → adapter HGETALLs content via the pipeline helper."""
        from moondb.types import TextSearchHit

        hits = [
            TextSearchHit(id="doc:1", score=0.5, fields={}),
            TextSearchHit(id="doc:2", score=0.3, fields={}),
        ]
        store, mock_client, _ = _build_langchain_store(hybrid_return=hits)

        # Stub the pipelined HGETALL: one dict per queued hit.
        pipe = mock_client.pipeline.return_value
        pipe.execute.return_value = [
            {b"content": b"Hello", b"metadata_src": b"a"},
            {b"content": b"World", b"metadata_src": b"b"},
        ]

        docs = store.similarity_search("q", k=2, search_type="hybrid")

        assert len(docs) == 2
        assert docs[0].page_content == "Hello"
        assert docs[1].page_content == "World"
        assert docs[0].metadata["src"] == "a"
        assert docs[1].metadata["src"] == "b"

    def test_hybrid_returns_exactly_k_documents(self, lc_deps: None) -> None:
        """Three hits with populated fields → exactly 3 docs (Gap 3 k+3 fix)."""
        from moondb.types import TextSearchHit

        hits = [
            TextSearchHit(id=f"doc:{i}", score=0.5 - i * 0.1, fields={"content": f"c{i}"})
            for i in range(3)
        ]
        store, _, _ = _build_langchain_store(hybrid_return=hits)

        docs = store.similarity_search("q", k=3, search_type="hybrid")

        assert len(docs) == 3

    def test_hybrid_score_is_rrf_score(self, lc_deps: None) -> None:
        """docs[0].metadata['score'] carries hit.score (the parsed __rrf_score)."""
        from moondb.types import TextSearchHit

        store, _, _ = _build_langchain_store(
            hybrid_return=[
                TextSearchHit(id="doc:1", score=0.832, fields={"content": "X"}),
            ],
        )

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        assert docs[0].metadata["score"] == 0.832

    def test_hybrid_populated_fields_skip_hydration(self, lc_deps: None) -> None:
        """BLOCKER 4 lock at the adapter boundary — mock without pipeline stub stays green."""
        from moondb.types import TextSearchHit

        # Hit ALREADY has populated fields → hydrate_hits_content fast-path
        # must NOT invoke mock_client.pipeline(). This is the iter-1
        # safe-fallback contract at the LangChain adapter boundary.
        store, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="doc:1", score=0.4, fields={"content": "Hi"})],
        )

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        assert len(docs) == 1
        assert docs[0].page_content == "Hi"
        mock_client.pipeline.assert_not_called()


class TestLangChainHybridStreamHits:
    """WARNING 6 — stream_hits surfaced from trailer AND per-hit markers."""

    def test_stream_hits_from_trailer_when_present(self, lc_deps: None) -> None:
        """Server trailer wins — explicit int counts flow through to metadata."""
        from moondb.types import TextSearchHit

        store, _, _ = _build_langchain_store(
            hybrid_return=(
                [TextSearchHit(id="d:1", score=0.5, fields={"content": "X"})],
                {"bm25": 5, "dense": 4, "sparse": 0},
            ),
        )

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        assert docs[0].metadata["stream_hits"] == {"bm25": 5, "dense": 4, "sparse": 0}

    def test_stream_hits_from_per_hit_markers_when_trailer_empty(self, lc_deps: None) -> None:
        """Empty trailer → fall back to per-hit __bm25_score__/__dense_score__ fields."""
        from moondb.types import TextSearchHit

        hit = TextSearchHit(
            id="d:1",
            score=0.5,
            fields={
                "content": "X",
                "__bm25_score__": "0.45",
                "__dense_score__": "0.32",
            },
        )
        store, _, _ = _build_langchain_store(hybrid_return=[hit])

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        stream = docs[0].metadata["stream_hits"]
        assert math.isclose(stream["bm25"], 0.45)
        assert math.isclose(stream["dense"], 0.32)

    def test_stream_hits_trailer_wins_when_both_present(self, lc_deps: None) -> None:
        """Trailer int counts take priority over per-hit float markers."""
        from moondb.types import TextSearchHit

        hit = TextSearchHit(
            id="d:1",
            score=0.5,
            fields={
                "content": "X",
                "__bm25_score__": "0.45",
                "__dense_score__": "0.32",
            },
        )
        store, _, _ = _build_langchain_store(
            hybrid_return=([hit], {"bm25": 5, "dense": 4}),
        )

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        # Trailer ints override per-hit floats.
        assert docs[0].metadata["stream_hits"] == {"bm25": 5, "dense": 4}

    def test_stream_hits_omitted_when_both_empty(self, lc_deps: None) -> None:
        """No trailer + no per-hit markers → stream_hits key absent from metadata."""
        from moondb.types import TextSearchHit

        store, _, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d:1", score=0.5, fields={"content": "X"})],
        )

        docs = store.similarity_search("q", k=1, search_type="hybrid")

        assert "stream_hits" not in docs[0].metadata


class TestLlamaIndexHybridHydration:
    """Gap 5 — LlamaIndex HYBRID must hydrate node.text + similarity from __rrf_score."""

    def test_hybrid_hydrates_node_text_via_hgetall(self, li_deps: None) -> None:
        """Empty hit.fields → adapter HGETALLs content; node.text populated."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, mock_client = _build_llamaindex_store(
            hybrid_return=[TextSearchHit(id="node:1", score=0.5, fields={})],
        )

        pipe = mock_client.pipeline.return_value
        pipe.execute.return_value = [
            {b"content": b"Hello", b"_node_id": b"abc", b"meta_src": b"kb"},
        ]

        result = store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=1,
        ))

        assert len(result.nodes) == 1
        assert result.nodes[0].text == "Hello"

    def test_hybrid_returns_exactly_k_nodes(self, li_deps: None) -> None:
        """Three hits with populated fields → exactly 3 nodes (Gap 5 k+3 fix)."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        hits = [
            TextSearchHit(id=f"n:{i}", score=0.5 - i * 0.1, fields={"content": f"c{i}"})
            for i in range(3)
        ]
        store, _ = _build_llamaindex_store(hybrid_return=hits)

        result = store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=3,
        ))

        assert len(result.nodes) == 3

    def test_hybrid_similarity_is_rrf_score(self, li_deps: None) -> None:
        """result.similarities carries hit.score (parsed __rrf_score)."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, _ = _build_llamaindex_store(
            hybrid_return=[
                TextSearchHit(id="n:1", score=0.832, fields={"content": "X"}),
            ],
        )

        result = store.query(VectorStoreQuery(
            query_embedding=[0.1] * 4,
            query_str="q",
            mode=VectorStoreQueryMode.HYBRID,
            similarity_top_k=1,
        ))

        assert result.similarities == [0.832]

    def test_text_search_hydrates_node_text(self, li_deps: None) -> None:
        """TEXT_SEARCH path: hits with empty fields also get HGETALL hydration."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        store, mock_client = _build_llamaindex_store(
            text_return=[TextSearchHit(id="n:1", score=2.5, fields={})],
        )

        pipe = mock_client.pipeline.return_value
        pipe.execute.return_value = [
            {b"content": b"from_hgetall", b"_node_id": b"abc"},
        ]

        result = store.query(VectorStoreQuery(
            query_str="q",
            mode=VectorStoreQueryMode.TEXT_SEARCH,
            similarity_top_k=1,
        ))

        assert result.nodes[0].text == "from_hgetall"

    def test_hybrid_stream_hits_from_per_hit_markers_preserved(self, li_deps: None) -> None:
        """WARNING 6 lock — LlamaIndex hybrid surfaces per-hit markers when trailer empty."""
        from llama_index.core.vector_stores.types import (
            VectorStoreQuery,
            VectorStoreQueryMode,
        )

        from moondb.types import TextSearchHit

        hit = TextSearchHit(
            id="n:1",
            score=0.5,
            fields={
                "content": "X",
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

        stream = result.nodes[0].metadata["stream_hits"]
        assert math.isclose(stream["bm25"], 0.5)
        assert math.isclose(stream["dense"], 0.7)


class TestHybridRegressionGuard:
    """BLOCKER 4 — hydrate_hits_content safe-fallback preserves populated fields."""

    def test_hydrate_hits_preserves_populated_fields(self) -> None:
        """All hits have fields → fast-path returns list verbatim, no pipeline call."""
        from moondb.integrations._hybrid_parse import hydrate_hits_content
        from moondb.types import TextSearchHit

        mock_client = MagicMock()
        hits = [TextSearchHit(id="x", score=1.0, fields={"content": "Hi"})]

        out = hydrate_hits_content(mock_client, hits)

        assert out[0].fields == {"content": "Hi"}
        mock_client.pipeline.assert_not_called()

    def test_hydrate_empty_list(self) -> None:
        """hydrate_hits_content([]) → []."""
        from moondb.integrations._hybrid_parse import hydrate_hits_content

        out = hydrate_hits_content(MagicMock(), [])
        assert out == []

    def test_hydrate_pipeline_shape_mismatch_falls_back(self) -> None:
        """pipe.execute() returns non-list → keep originals unchanged (BLOCKER 4 #3)."""
        from moondb.integrations._hybrid_parse import hydrate_hits_content
        from moondb.types import TextSearchHit

        mock_client = MagicMock()
        pipe = mock_client.pipeline.return_value
        # Non-list return from execute() (realistic MagicMock default shape).
        pipe.execute.return_value = MagicMock()

        hits = [TextSearchHit(id="x", score=1.0, fields={})]
        out = hydrate_hits_content(mock_client, hits)

        assert out == hits

    def test_hydrate_server_fields_win_over_hgetall(self) -> None:
        """hit.fields with content → fast-path short-circuits; stays unchanged."""
        from moondb.integrations._hybrid_parse import hydrate_hits_content
        from moondb.types import TextSearchHit

        mock_client = MagicMock()
        hit = TextSearchHit(id="x", score=1.0, fields={"content": "original"})

        out = hydrate_hits_content(mock_client, [hit])

        assert out[0].fields["content"] == "original"
        mock_client.pipeline.assert_not_called()


class TestHybridHelperContract:
    """B-01 iter-3 — lock the _normalise_hybrid_return shape absorber."""

    def test_build_langchain_store_wraps_list_into_two_tuple(self, lc_deps: None) -> None:
        """Passing a list hybrid_return → mock returns (list, {}) 2-tuple."""
        from moondb.types import TextSearchHit

        _, mock_client, _ = _build_langchain_store(
            hybrid_return=[TextSearchHit(id="d:1", score=1.0, fields={"content": "X"})],
        )
        assigned = mock_client.text.hybrid_search.return_value

        assert isinstance(assigned, tuple)
        assert len(assigned) == 2
        assert isinstance(assigned[0], list)
        assert len(assigned[0]) == 1
        assert assigned[1] == {}

    def test_build_llamaindex_store_passes_through_prewrapped_two_tuple(
        self, li_deps: None
    ) -> None:
        """Passing a pre-wrapped 2-tuple → mock returns it verbatim."""
        from moondb.types import TextSearchHit

        hits = [TextSearchHit(id="n:1", score=0.5, fields={"content": "Y"})]
        trailer = {"bm25": 3, "dense": 2}
        _, mock_client = _build_llamaindex_store(hybrid_return=(hits, trailer))

        assert mock_client.text.hybrid_search.return_value == (hits, trailer)
