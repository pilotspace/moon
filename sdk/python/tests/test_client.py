"""Tests for MoonClient and AsyncMoonClient.

Tests focus on SDK structure, type helpers, and command argument building.
Integration tests against a live Moon server are marked with @pytest.mark.integration.
"""

from __future__ import annotations

import struct
from unittest.mock import MagicMock, patch

import pytest

from moondb import (
    AsyncMoonClient,
    MoonClient,
    SearchResult,
    GraphNode,
    GraphEdge,
    IndexInfo,
    QueryResult,
    CacheSearchResult,
    encode_vector,
    decode_vector,
)


class TestEncodeVector:
    """Test vector encoding/decoding utilities."""

    def test_encode_list(self) -> None:
        vec = [1.0, 2.0, 3.0]
        result = encode_vector(vec)
        assert isinstance(result, bytes)
        assert len(result) == 12  # 3 * 4 bytes
        # Verify round-trip
        decoded = struct.unpack("<3f", result)
        assert decoded == pytest.approx((1.0, 2.0, 3.0))

    def test_encode_empty(self) -> None:
        result = encode_vector([])
        assert result == b""

    def test_decode_vector(self) -> None:
        data = struct.pack("<3f", 1.0, 2.0, 3.0)
        result = decode_vector(data, dim=3)
        assert result == pytest.approx([1.0, 2.0, 3.0])

    def test_decode_wrong_size(self) -> None:
        data = struct.pack("<2f", 1.0, 2.0)
        with pytest.raises(ValueError, match="Expected 12 bytes"):
            decode_vector(data, dim=3)

    def test_encode_with_numpy(self) -> None:
        try:
            import numpy as np
            vec = np.array([1.0, 2.0, 3.0], dtype=np.float32)
            result = encode_vector(vec)
            assert len(result) == 12
            decoded = decode_vector(result, dim=3)
            assert decoded == pytest.approx([1.0, 2.0, 3.0])
        except ImportError:
            pytest.skip("numpy not installed")


class TestSearchResult:
    """Test SearchResult dataclass."""

    def test_basic(self) -> None:
        sr = SearchResult(key="doc:1", score=0.5, fields={"title": "Hello"})
        assert sr.key == "doc:1"
        assert sr.score == 0.5
        assert sr.fields["title"] == "Hello"
        assert sr.graph_hops is None
        assert sr.cache_hit is None

    def test_with_graph_hops(self) -> None:
        sr = SearchResult(key="doc:1", score=0.1, fields={}, graph_hops=2)
        assert sr.graph_hops == 2

    def test_frozen(self) -> None:
        sr = SearchResult(key="doc:1", score=0.5)
        with pytest.raises(AttributeError):
            sr.key = "doc:2"  # type: ignore[misc]


class TestGraphNode:
    """Test GraphNode dataclass."""

    def test_basic(self) -> None:
        node = GraphNode(node_id=1, label="Person", properties={"name": "Alice"})
        assert node.node_id == 1
        assert node.label == "Person"
        assert node.properties["name"] == "Alice"


class TestGraphEdge:
    """Test GraphEdge dataclass."""

    def test_defaults(self) -> None:
        edge = GraphEdge(src_id=1, dst_id=2, edge_type="KNOWS")
        assert edge.weight == 1.0
        assert edge.properties == {}


class TestIndexInfo:
    """Test IndexInfo dataclass."""

    def test_defaults(self) -> None:
        info = IndexInfo(name="test")
        assert info.num_docs == 0
        assert info.dimension == 0
        assert info.distance_metric == "L2"


class TestQueryResult:
    """Test QueryResult dataclass."""

    def test_empty(self) -> None:
        qr = QueryResult()
        assert qr.headers == []
        assert qr.rows == []
        assert qr.stats == {}


class TestCacheSearchResult:
    """Test CacheSearchResult dataclass."""

    def test_defaults(self) -> None:
        cr = CacheSearchResult()
        assert cr.results == []
        assert cr.cache_hit is False


class TestMoonClientInit:
    """Test MoonClient initialization."""

    @patch("redis.Redis.__init__", return_value=None)
    def test_default_params(self, mock_init: MagicMock) -> None:
        client = MoonClient.__new__(MoonClient)
        # Verify properties exist
        assert hasattr(MoonClient, "vector")
        assert hasattr(MoonClient, "graph")
        assert hasattr(MoonClient, "session")
        assert hasattr(MoonClient, "cache")

    def test_properties_are_typed(self) -> None:
        """Verify property return type annotations."""
        from moondb.vector import VectorCommands
        from moondb.graph import GraphCommands
        from moondb.session import SessionCommands
        from moondb.cache import CacheCommands

        # Check the property annotations on the class
        assert MoonClient.vector.fget is not None
        assert MoonClient.graph.fget is not None
        assert MoonClient.session.fget is not None
        assert MoonClient.cache.fget is not None


class TestAsyncMoonClientInit:
    """Test AsyncMoonClient initialization."""

    def test_has_async_properties(self) -> None:
        assert hasattr(AsyncMoonClient, "vector")
        assert hasattr(AsyncMoonClient, "graph")
        assert hasattr(AsyncMoonClient, "session")
        assert hasattr(AsyncMoonClient, "cache")


class TestVersionExported:
    """Test package metadata."""

    def test_version(self) -> None:
        import moondb
        assert moondb.__version__ == "0.1.0"

    def test_all_exports(self) -> None:
        import moondb
        for name in moondb.__all__:
            assert hasattr(moondb, name), f"Missing export: {name}"
