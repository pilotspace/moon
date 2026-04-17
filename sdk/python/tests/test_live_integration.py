"""End-to-end integration tests against a live moon server.

Plan 153-06 Task 3: close Gap 1 (BLOCKER) from 153-UAT.md by asserting
the PilotSpace example + LangChain/LlamaIndex adapters actually work
against a running moon process on 127.0.0.1:6399.

All tests are marked ``@pytest.mark.integration`` and skip cleanly via
the ``moon_available`` fixture in conftest.py when the server is not
reachable. To run locally::

    # Terminal 1: start moon
    ./target/release/moon --port 6399 --shards 1 --protected-mode no

    # Terminal 2
    cd sdk/python && uv run --extra langchain --extra llamaindex \\
        pytest tests/test_live_integration.py -m integration
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import math
import sys
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest

from moondb import encode_vector

if TYPE_CHECKING:
    from langchain_core.embeddings import Embeddings

# --------------------------------------------------------------------------
# pilotspace_example loader — matches test_example.py portability pattern
# --------------------------------------------------------------------------


def _load_example_module() -> ModuleType:
    try:
        import examples.pilotspace_example as _mod  # type: ignore[import-not-found]
    except ImportError:
        _mod = None  # type: ignore[assignment]
    if _mod is not None:
        return _mod
    path = Path(__file__).parent.parent / "examples" / "pilotspace_example.py"
    spec = importlib.util.spec_from_file_location("pilotspace_example", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pilotspace_example"] = mod
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------
# Deterministic, network-free embeddings — SHA-256 seeded
# --------------------------------------------------------------------------


def _sha256_vector(text: str, dim: int) -> list[float]:
    """Deterministic [-1, 1) float vector of length ``dim``.

    Uses repeated SHA-256 rounds so two different texts produce clearly
    distinct vectors — adequate for BM25 + dense fusion tests that only
    need non-degenerate cosine similarity.
    """
    out: list[float] = []
    chunk = text.encode("utf-8")
    while len(out) < dim:
        digest = hashlib.sha256(chunk).digest()
        for i in range(0, len(digest), 2):
            if len(out) >= dim:
                break
            # Map a 2-byte slice into [-1, 1).
            raw = int.from_bytes(digest[i : i + 2], "little")
            out.append((raw / 0xFFFF) * 2.0 - 1.0)
        chunk = digest  # re-hash for next round
    return out


class _DeterministicEmbedding:
    """LangChain Embeddings-compatible duck type — no network, no model."""

    def __init__(self, dim: int = 4) -> None:
        self._dim = dim

    def embed_query(self, text: str) -> list[float]:
        return _sha256_vector(text, self._dim)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [_sha256_vector(t, self._dim) for t in texts]


# --------------------------------------------------------------------------
# Test 3a: pilotspace_example against live moon
# --------------------------------------------------------------------------


@pytest.mark.integration
def test_pilotspace_example_runs_against_live_moon(
    live_moon_client: Any,  # noqa: ANN401, ARG001 -- fixture consumed for connection
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Plan 06 Gap 1 truth: the example exits 0 against a live moon
    server and prints all three demo section headers with populated
    results."""
    pilotspace = _load_example_module()
    rc = pilotspace.main(
        [
            "--host",
            "127.0.0.1",
            "--port",
            "6399",
            "--num-issues",
            "20",
            "--dim",
            "32",
        ]
    )
    assert rc == 0, f"pilotspace_example exited non-zero: {rc}"
    captured = capsys.readouterr()
    out = captured.out
    # All three section headers must be present.
    assert "== Text search: 'crash on startup' ==" in out
    assert "== Aggregate facet (by priority) ==" in out
    assert "== Hybrid search: 'crash bug memory' ==" in out
    assert "Done." in out


# --------------------------------------------------------------------------
# Test 3b: LangChain hybrid search returns populated documents
# --------------------------------------------------------------------------


@pytest.mark.integration
def test_langchain_hybrid_returns_populated_documents(
    live_moon_client: Any,  # noqa: ANN401
) -> None:
    """Plan 04 contract (via live moon): hybrid search returns exactly
    k Documents with non-empty page_content and a surfaced RRF score
    in metadata."""
    if importlib.util.find_spec("langchain_core") is None:
        pytest.skip("langchain_core not installed")

    from moondb.integrations.langchain import MoonVectorStore

    store = MoonVectorStore(
        index_name="_test_lc_hybrid",
        embedding=cast("Embeddings", _DeterministicEmbedding(dim=4)),
        moon_client=live_moon_client,
        prefix="_doc_lc:",
        dim=4,
    )
    store.add_texts(
        [
            "crash on startup when config is missing",
            "memory leak in session cache",
            "auth flow returns 500 on expired JWT",
            "login button unresponsive after oauth",
            "slow startup latency after v2.3",
        ]
    )

    docs = store.similarity_search(
        "crash",
        k=3,
        search_type="hybrid",
        hybrid_weights=(1.0, 1.0, 0.0),
    )
    assert len(docs) == 3, f"expected exactly 3 docs, got {len(docs)}"
    for doc in docs:
        assert isinstance(doc.page_content, str)
        assert doc.page_content.strip() != "", "page_content must be populated"
        score = doc.metadata.get("score")
        assert score is not None, "metadata['score'] must be set"
        assert float(score) > 0.0, f"score must be positive, got {score}"


# --------------------------------------------------------------------------
# Test 3c: LlamaIndex HYBRID query with alpha
# --------------------------------------------------------------------------


@pytest.mark.integration
def test_llamaindex_hybrid_with_alpha_returns_populated_nodes(
    live_moon_client: Any,  # noqa: ANN401
) -> None:
    """Plan 05 client-preservation + Plan 04 hybrid parser fix validated
    end-to-end. Also locks WARNING 8/9 fixes: alpha→weights kwarg
    propagation (MagicMock pre-step) + math.isclose weight comparison."""
    if importlib.util.find_spec("llama_index.core") is None:
        pytest.skip("llama_index.core not installed")

    from llama_index.core.vector_stores.types import (
        VectorStoreQuery,
        VectorStoreQueryMode,
    )

    from moondb.integrations.llamaindex import MoonVectorStore

    # ------------------------------------------------------------------
    # WARNING 9 pre-step: confirm alpha=0.7 → client.text.hybrid_search
    # is called with weights kwarg ≈ (0.3, 0.7, 0.0). No live server
    # traffic in this block — MagicMock-backed throw-away store.
    # ------------------------------------------------------------------
    quick_mock = MagicMock()
    quick_mock.text.hybrid_search.return_value = ([], {})
    quick_store = MoonVectorStore(
        index_name="_alpha_probe",
        moon_client=quick_mock,
        dim=4,
        prefix="_probe:",
    )
    with contextlib.suppress(Exception):
        quick_store.query(
            VectorStoreQuery(
                query_embedding=[0.1] * 4,
                query_str="x",
                mode=VectorStoreQueryMode.HYBRID,
                similarity_top_k=1,
                alpha=0.7,
            )
        )
    assert quick_mock.text.hybrid_search.call_count >= 1
    call = quick_mock.text.hybrid_search.call_args
    weights = call.kwargs.get("weights")
    assert weights is not None, f"weights kwarg missing: {call}"
    # WARNING 8 / WARNING 9: math.isclose — no float-equality brittleness.
    assert math.isclose(weights[0], 0.3, abs_tol=1e-9)
    assert math.isclose(weights[1], 0.7, abs_tol=1e-9)
    assert math.isclose(weights[2], 0.0, abs_tol=1e-9)

    # ------------------------------------------------------------------
    # Live portion: seed 5 hashes, query with HYBRID + alpha=0.7.
    # ------------------------------------------------------------------
    store = MoonVectorStore(
        index_name="_test_li_hybrid",
        moon_client=live_moon_client,
        dim=4,
        prefix="_node_li:",
    )

    # Wait for the store to auto-create its index; then HSET nodes by hand
    # so the body text actually lands in the "content" field the adapter
    # hydrates from.
    sample_texts = [
        "crash on startup when config is missing",
        "memory leak in session cache",
        "auth flow returns 500 on expired JWT",
        "login button unresponsive after oauth",
        "slow startup latency after v2.3",
    ]
    for idx, text in enumerate(sample_texts):
        live_moon_client.hset(
            f"_node_li:{idx:03d}",
            mapping={
                "content": text,
                "vec": encode_vector(_sha256_vector(text, 4)),
            },
        )

    q = VectorStoreQuery(
        query_embedding=_sha256_vector("crash", 4),
        query_str="crash",
        mode=VectorStoreQueryMode.HYBRID,
        similarity_top_k=3,
        alpha=0.7,
    )
    result = store.query(q)
    assert result.nodes is not None
    assert len(result.nodes) == 3, f"expected 3 nodes, got {len(result.nodes)}"
    for node in result.nodes:
        node_text = getattr(node, "text", None) or getattr(node, "get_content", lambda: "")()
        assert node_text is not None
        assert str(node_text).strip() != "", "node text must be populated"
    assert result.similarities is not None
    for sim in result.similarities:
        assert sim > 0.0, f"similarity must be positive, got {sim}"

    # WARNING 8 fix: alpha→weights resolver uses math.isclose.
    resolved = store._resolve_hybrid_weights(q)
    assert math.isclose(resolved[0], 0.3, abs_tol=1e-9)
    assert math.isclose(resolved[1], 0.7, abs_tol=1e-9)
    assert math.isclose(resolved[2], 0.0, abs_tol=1e-9)


# --------------------------------------------------------------------------
# Test 3d: adapters actually use the caller-supplied client
# --------------------------------------------------------------------------


@pytest.mark.integration
def test_adapters_use_supplied_client(
    live_moon_client: Any,  # noqa: ANN401
) -> None:
    """Plan 05 truth: LangChain + LlamaIndex adapters preserve the
    caller-supplied ``moon_client`` across pydantic / explicit
    __init__ paths. Proves the routing works end-to-end by exercising
    one minimal op against each through the shared client."""
    if (
        importlib.util.find_spec("langchain_core") is None
        or importlib.util.find_spec("llama_index.core") is None
    ):
        pytest.skip("langchain_core or llama_index.core not installed")

    from moondb.integrations.langchain import MoonVectorStore as LcStore
    from moondb.integrations.llamaindex import MoonVectorStore as LiStore

    lc_store = LcStore(
        index_name="_test_lc_hybrid",
        embedding=cast("Embeddings", _DeterministicEmbedding(dim=4)),
        moon_client=live_moon_client,
        prefix="_doc_lc:",
        dim=4,
    )
    li_store = LiStore(
        index_name="_test_li_hybrid",
        moon_client=live_moon_client,
        dim=4,
        prefix="_node_li:",
    )
    assert lc_store._client is live_moon_client
    assert li_store._client is live_moon_client

    # Minimal op through each adapter to prove the connection is real.
    lc_ids = lc_store.add_texts(["ping test"])
    assert len(lc_ids) == 1
    # LlamaIndex: seed a single node via the shared client's HSET.
    live_moon_client.hset(
        "_node_li:ping",
        mapping={"content": "ping test", "vec": encode_vector(_sha256_vector("ping", 4))},
    )
    from llama_index.core.vector_stores.types import (
        VectorStoreQuery,
        VectorStoreQueryMode,
    )

    q = VectorStoreQuery(
        query_embedding=_sha256_vector("ping", 4),
        mode=VectorStoreQueryMode.DEFAULT,
        similarity_top_k=1,
    )
    li_result = li_store.query(q)
    # Just assert the query completed without raising — content parsing
    # is covered by test 3c.
    assert li_result is not None
