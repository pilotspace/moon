"""Tests for vector search command helpers.

Tests response parsing logic without requiring a live Moon server.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moondb.types import SearchResult, IndexInfo, CacheSearchResult
from moondb.vector import (
    VectorCommands,
    _parse_search_results,
    _parse_index_info,
    _parse_cache_search_results,
    _ensure_bytes,
)


class TestParseSearchResults:
    """Test FT.SEARCH response parsing."""

    def test_empty_response(self) -> None:
        assert _parse_search_results(None) == []
        assert _parse_search_results([]) == []
        assert _parse_search_results("not a list") == []

    def test_no_results(self) -> None:
        # [total_count] with no result pairs
        assert _parse_search_results([0]) == []

    def test_single_result(self) -> None:
        raw = [
            1,  # total
            b"doc:1",
            [b"__vec_score", b"0.123", b"title", b"Hello"],
        ]
        results = _parse_search_results(raw)
        assert len(results) == 1
        assert results[0].key == "doc:1"
        assert results[0].score == pytest.approx(0.123)
        assert results[0].fields["title"] == "Hello"

    def test_multiple_results(self) -> None:
        raw = [
            2,
            b"doc:1", [b"__vec_score", b"0.1", b"title", b"A"],
            b"doc:2", [b"__vec_score", b"0.5", b"title", b"B"],
        ]
        results = _parse_search_results(raw)
        assert len(results) == 2
        assert results[0].key == "doc:1"
        assert results[1].key == "doc:2"

    def test_graph_hops(self) -> None:
        raw = [
            1,
            b"doc:1",
            [b"__vec_score", b"0.2", b"__graph_hops", b"3"],
        ]
        results = _parse_search_results(raw)
        assert results[0].graph_hops == 3

    def test_cache_hit_field(self) -> None:
        raw = [
            1,
            b"cache:q:1",
            [b"__vec_score", b"0.01", b"cache_hit", b"true", b"response", b"The answer"],
        ]
        results = _parse_search_results(raw)
        assert results[0].cache_hit is True
        assert results[0].fields["response"] == "The answer"

    def test_string_values(self) -> None:
        """Test with string values (decode_responses=True)."""
        raw = [
            1,
            "doc:1",
            ["__vec_score", "0.5", "title", "World"],
        ]
        results = _parse_search_results(raw)
        assert results[0].key == "doc:1"
        assert results[0].fields["title"] == "World"


class TestParseCacheSearchResults:
    """Test FT.CACHESEARCH response parsing."""

    def test_cache_hit(self) -> None:
        raw = [
            1,
            b"cache:q:1",
            [b"__vec_score", b"0.01", b"cache_hit", b"true", b"response", b"42"],
        ]
        result = _parse_cache_search_results(raw)
        assert result.cache_hit is True
        assert len(result.results) == 1

    def test_cache_miss(self) -> None:
        raw = [
            2,
            b"doc:1", [b"__vec_score", b"0.3", b"cache_hit", b"false"],
            b"doc:2", [b"__vec_score", b"0.5", b"cache_hit", b"false"],
        ]
        result = _parse_cache_search_results(raw)
        assert result.cache_hit is False
        assert len(result.results) == 2


class TestParseIndexInfo:
    """Test FT.INFO response parsing."""

    def test_basic(self) -> None:
        raw = [
            b"index_name", b"my_idx",
            b"num_docs", b"100",
            b"dimension", b"384",
            b"distance_metric", b"COSINE",
        ]
        info = _parse_index_info("my_idx", raw)
        assert info.name == "my_idx"
        assert info.num_docs == 100
        assert info.dimension == 384
        assert info.distance_metric == "COSINE"

    def test_empty(self) -> None:
        info = _parse_index_info("test", None)
        assert info.name == "test"
        assert info.num_docs == 0

    def test_non_numeric(self) -> None:
        raw = [b"num_docs", b"not_a_number"]
        info = _parse_index_info("test", raw)
        assert info.num_docs == 0


class TestEnsureBytes:
    """Test _ensure_bytes helper."""

    def test_passthrough_bytes(self) -> None:
        data = b"\x00\x01\x02"
        assert _ensure_bytes(data) is data

    def test_converts_list(self) -> None:
        result = _ensure_bytes([1.0, 2.0])
        assert isinstance(result, bytes)
        assert len(result) == 8


class TestVectorCommandsArgs:
    """Test that VectorCommands builds correct command arguments."""

    def setup_method(self) -> None:
        self.mock_client = MagicMock()
        self.vc = VectorCommands(self.mock_client)

    def test_create_index_basic(self) -> None:
        self.vc.create_index("test_idx", dim=128, metric="L2")

        args = self.mock_client.execute_command.call_args[0]
        assert args[0] == "FT.CREATE"
        assert args[1] == "test_idx"
        assert "ON" in args
        assert "HASH" in args
        assert "SCHEMA" in args
        assert "VECTOR" in args
        assert "DIM" in args
        assert "128" in args

    def test_create_index_with_extras(self) -> None:
        self.vc.create_index(
            "test_idx", dim=384, metric="COSINE",
            extra_schema=["title", "TEXT"],
            ef_runtime=100, compact_threshold=5000,
        )

        args = self.mock_client.execute_command.call_args[0]
        assert "title" in args
        assert "TEXT" in args
        assert "EF_RUNTIME" in args
        assert "100" in args
        assert "COMPACT_THRESHOLD" in args
        assert "5000" in args

    def test_search_basic(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.search("idx", [1.0, 2.0], k=5)

        args = self.mock_client.execute_command.call_args[0]
        assert args[0] == "FT.SEARCH"
        assert args[1] == "idx"
        assert "KNN 5" in args[2]
        assert "PARAMS" in args

    def test_search_with_session(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.search("idx", [1.0], session_key="sess:1")

        args = self.mock_client.execute_command.call_args[0]
        assert "SESSION" in args
        assert "sess:1" in args

    def test_search_with_expand(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.search("idx", [1.0], expand_graph="social", expand_depth=3)

        args = self.mock_client.execute_command.call_args[0]
        assert "EXPAND" in args
        assert "GRAPH" in args
        assert "social" in args
        assert "DEPTH" in args

    def test_drop_index(self) -> None:
        self.vc.drop_index("test_idx")
        args = self.mock_client.execute_command.call_args[0]
        assert args == ("FT.DROPINDEX", "test_idx")

    def test_compact(self) -> None:
        self.vc.compact("test_idx")
        args = self.mock_client.execute_command.call_args[0]
        assert args == ("FT.COMPACT", "test_idx")

    def test_recommend(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.recommend(
            "idx",
            positive_keys=["doc:1", "doc:2"],
            negative_keys=["doc:99"],
            k=5,
        )

        args = self.mock_client.execute_command.call_args[0]
        assert args[0] == "FT.RECOMMEND"
        assert "POSITIVE" in args
        assert "doc:1" in args
        assert "NEGATIVE" in args
        assert "doc:99" in args
        assert "K" in args

    def test_navigate(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.navigate("idx", [1.0, 2.0], hops=3, hop_penalty=0.05)

        args = self.mock_client.execute_command.call_args[0]
        assert args[0] == "FT.NAVIGATE"
        assert "HOPS" in args
        assert "3" in args
        assert "HOP_PENALTY" in args

    def test_expand(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.expand("idx", ["doc:1", "doc:2"], depth=3, graph_name="kg")

        args = self.mock_client.execute_command.call_args[0]
        assert args[0] == "FT.EXPAND"
        assert "doc:1" in args
        assert "DEPTH" in args
        assert "GRAPH" in args
        assert "kg" in args

    def test_cache_search(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.vc.cache_search("idx", "cache:", [1.0], threshold=0.9, fallback_k=5)

        args = self.mock_client.execute_command.call_args[0]
        assert args[0] == "FT.CACHESEARCH"
        assert "cache:" in args
        assert "THRESHOLD" in args
        assert "0.9" in args
        assert "FALLBACK" in args
