"""Tests for TextCommands + AsyncTextCommands (mock-based, no live server).

Mirrors the patterns in ``tests/test_vector.py`` (mock-based command-arg
assertion) and ``tests/test_client.py`` (dataclass + client-init checks).

Real-server integration tests for adapter flows live in Plan 02 / 03.
"""

from __future__ import annotations

import math
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moondb import (
    AggregateStep,
    AsyncMoonClient,
    AsyncTextCommands,
    Avg,
    Count,
    CountDistinct,
    Filter,
    GroupBy,
    Limit,
    Max,
    Min,
    MoonClient,
    SortBy,
    Sum,
    TextCommands,
    TextSearchHit,
)
from moondb.text import (
    _build_aggregate_args,
    _build_create_index_args,
    _build_hybrid_args,
    _build_text_search_args,
    _ensure_bytes,
    _maybe_cast_number,
    _parse_aggregate_results,
    _parse_text_search_results,
    _reducer_args,
    _validate_hybrid_weights,
)

# -- Dataclass smoke tests ---------------------------------------------------


class TestTextSearchHit:
    """Validate TextSearchHit constructor, defaults, and frozenness."""

    def test_basic_construct(self) -> None:
        hit = TextSearchHit(id="doc:1", score=3.14, fields={"title": "Hello"})
        assert hit.id == "doc:1"
        assert hit.score == pytest.approx(3.14)
        assert hit.fields == {"title": "Hello"}

    def test_defaults(self) -> None:
        hit = TextSearchHit(id="doc:1", score=1.0)
        assert hit.fields == {}
        assert hit.highlights is None

    def test_frozen(self) -> None:
        hit = TextSearchHit(id="doc:1", score=1.0)
        with pytest.raises(AttributeError):
            hit.id = "doc:2"  # type: ignore[misc]

    def test_with_highlights(self) -> None:
        hit = TextSearchHit(
            id="doc:1",
            score=1.0,
            fields={"body": "crash"},
            highlights={"body": ["<em>crash</em>"]},
        )
        assert hit.highlights == {"body": ["<em>crash</em>"]}


class TestAggregateDSL:
    """Validate GroupBy / SortBy / Filter / Limit + reducers."""

    def test_groupby_basic(self) -> None:
        g = GroupBy(fields=["priority"], reducers=[Count(alias="n")])
        assert g.fields == ["priority"]
        assert g.reducers == [Count(alias="n")]

    def test_groupby_default_reducers(self) -> None:
        g = GroupBy(fields=["priority"])
        assert g.reducers == []

    def test_sortby_default_asc(self) -> None:
        s = SortBy("n")
        assert s.direction == "ASC"

    def test_sortby_desc_stored_verbatim(self) -> None:
        s = SortBy("n", "DESC")
        assert s.field == "n"
        assert s.direction == "DESC"

    def test_filter_construct(self) -> None:
        f = Filter("@status:{open}")
        assert f.expr == "@status:{open}"

    def test_limit_construct(self) -> None:
        lim = Limit(0, 10)
        assert lim.offset == 0
        assert lim.count == 10

    def test_count_alias_default(self) -> None:
        assert Count().alias is None

    def test_sum_requires_field(self) -> None:
        with pytest.raises(TypeError):
            Sum()  # type: ignore[call-arg]

    def test_all_reducers_default_alias_none(self) -> None:
        assert Sum(field="x").alias is None
        assert Avg(field="x").alias is None
        assert Min(field="x").alias is None
        assert Max(field="x").alias is None
        assert CountDistinct(field="x").alias is None

    def test_reducers_frozen(self) -> None:
        r = Count(alias="n")
        with pytest.raises(AttributeError):
            r.alias = "m"  # type: ignore[misc]

    def test_aggregate_step_union_imports(self) -> None:
        # AggregateStep is a type alias; simply ensure it imports at module level.
        assert AggregateStep is not None


# -- Builder: FT.CREATE ------------------------------------------------------


class TestBuildCreateIndexArgs:
    """Validate FT.CREATE argument construction."""

    def test_basic_text_index_with_prefix(self) -> None:
        args = _build_create_index_args(
            "idx",
            [("title", "TEXT", {"WEIGHT": 2.0}), ("body", "TEXT", {})],
            prefix="issue:",
            on="HASH",
        )
        assert args[:8] == [
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "issue:",
            "SCHEMA",
        ]
        assert args[8:] == ["title", "TEXT", "WEIGHT", "2.0", "body", "TEXT"]

    def test_no_prefix(self) -> None:
        args = _build_create_index_args(
            "idx", [("title", "TEXT", {})], prefix=None, on="HASH"
        )
        assert "PREFIX" not in args
        assert args == ["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"]

    def test_tag_with_sortable_flag(self) -> None:
        args = _build_create_index_args(
            "idx",
            [("status", "TAG", {"SORTABLE": True})],
            prefix=None,
            on="HASH",
        )
        assert args[-3:] == ["status", "TAG", "SORTABLE"]

    def test_tag_noindex_flag(self) -> None:
        args = _build_create_index_args(
            "idx", [("raw", "TAG", {"NOINDEX": True})], prefix=None, on="HASH"
        )
        assert "NOINDEX" in args
        # NOINDEX is a flag, NOT followed by a value
        idx = args.index("NOINDEX")
        assert idx == len(args) - 1

    def test_falsy_flag_skipped(self) -> None:
        args = _build_create_index_args(
            "idx", [("f", "TAG", {"SORTABLE": False})], prefix=None, on="HASH"
        )
        assert "SORTABLE" not in args

    def test_unknown_opts_dropped(self) -> None:
        # Threat model T-153-01-05: unknown opts are whitelist-filtered.
        args = _build_create_index_args(
            "idx",
            [("title", "TEXT", {"EVIL_DROP_TABLE": "x"})],
            prefix=None,
            on="HASH",
        )
        assert "EVIL_DROP_TABLE" not in args
        assert "x" not in args

    def test_opt_key_case_insensitive(self) -> None:
        args = _build_create_index_args(
            "idx",
            [("title", "TEXT", {"weight": 1.5})],
            prefix=None,
            on="HASH",
        )
        # Lowercased "weight" is normalized to "WEIGHT" on emit.
        assert "WEIGHT" in args
        assert "1.5" in args

    def test_multiple_fields(self) -> None:
        args = _build_create_index_args(
            "idx",
            [
                ("title", "TEXT", {"WEIGHT": 2.0}),
                ("body", "TEXT", {"NOSTEM": True}),
                ("status", "TAG", {}),
                ("priority", "TAG", {"SORTABLE": True}),
            ],
            prefix="issue:",
            on="HASH",
        )
        # Verify each field's position in the schema
        assert args.index("title") < args.index("body")
        assert args.index("body") < args.index("status")
        assert args.index("status") < args.index("priority")
        assert "NOSTEM" in args
        assert "SORTABLE" in args


# -- Builder: FT.SEARCH ------------------------------------------------------


class TestBuildTextSearchArgs:
    """Validate FT.SEARCH (BM25) argument construction."""

    def test_basic(self) -> None:
        args = _build_text_search_args(
            "idx",
            "crash startup",
            limit=10,
            offset=0,
            return_fields=None,
            highlight=False,
            highlight_fields=None,
            lang=None,
        )
        assert args == ["FT.SEARCH", "idx", "crash startup", "LIMIT", "0", "10"]

    def test_with_return_fields(self) -> None:
        args = _build_text_search_args(
            "idx",
            "q",
            limit=5,
            offset=0,
            return_fields=["title", "body"],
            highlight=False,
            highlight_fields=None,
            lang=None,
        )
        assert "RETURN" in args
        assert args[args.index("RETURN") + 1] == "2"
        assert "title" in args
        assert "body" in args

    def test_with_highlight_fields(self) -> None:
        args = _build_text_search_args(
            "idx",
            "q",
            limit=10,
            offset=0,
            return_fields=None,
            highlight=True,
            highlight_fields=["body"],
            lang=None,
        )
        assert "HIGHLIGHT" in args
        assert "FIELDS" in args
        assert args[args.index("FIELDS") + 1] == "1"
        assert args[args.index("FIELDS") + 2] == "body"

    def test_with_language(self) -> None:
        args = _build_text_search_args(
            "idx",
            "q",
            limit=10,
            offset=0,
            return_fields=None,
            highlight=False,
            highlight_fields=None,
            lang="english",
        )
        assert "LANGUAGE" in args
        assert "english" in args

    def test_empty_query_becomes_star(self) -> None:
        args = _build_text_search_args(
            "idx",
            "",
            limit=10,
            offset=0,
            return_fields=None,
            highlight=False,
            highlight_fields=None,
            lang=None,
        )
        assert args[2] == "*"


# -- Builder: FT.AGGREGATE ---------------------------------------------------


class TestBuildAggregateArgs:
    """Validate FT.AGGREGATE pipeline argument construction."""

    def test_groupby_with_count(self) -> None:
        args = _build_aggregate_args(
            "idx",
            "*",
            [GroupBy(["priority"], [Count(alias="n")])],
            limit=10,
            timeout_ms=None,
        )
        # ... GROUPBY 1 @priority REDUCE COUNT 0 AS n ... LIMIT 0 10
        assert "GROUPBY" in args
        idx = args.index("GROUPBY")
        assert args[idx + 1] == "1"
        assert args[idx + 2] == "@priority"
        assert "REDUCE" in args
        assert "COUNT" in args
        assert "AS" in args
        assert "n" in args

    def test_multi_reducer_group(self) -> None:
        args = _build_aggregate_args(
            "idx",
            "*",
            [
                GroupBy(
                    ["g"],
                    [
                        Sum("amount", alias="total"),
                        Avg("amount", alias="mean"),
                        CountDistinct("user_id", alias="distinct_users"),
                    ],
                )
            ],
            limit=10,
            timeout_ms=None,
        )
        assert "SUM" in args
        assert "AVG" in args
        assert "COUNT_DISTINCT" in args
        assert "@amount" in args
        assert "@user_id" in args
        assert "total" in args
        assert "distinct_users" in args

    def test_sortby_desc(self) -> None:
        args = _build_aggregate_args(
            "idx", "*", [SortBy("n", "DESC")], limit=10, timeout_ms=None
        )
        idx = args.index("SORTBY")
        assert args[idx + 1] == "2"
        assert args[idx + 2] == "@n"
        assert args[idx + 3] == "DESC"

    def test_sortby_invalid_direction_raises(self) -> None:
        with pytest.raises(ValueError, match="ASC"):
            _build_aggregate_args(
                "idx", "*", [SortBy("n", "SIDEWAYS")], limit=10, timeout_ms=None
            )

    def test_filter_step(self) -> None:
        args = _build_aggregate_args(
            "idx",
            "*",
            [Filter("@status:{open}")],
            limit=10,
            timeout_ms=None,
        )
        idx = args.index("FILTER")
        assert args[idx + 1] == "@status:{open}"

    def test_explicit_limit_no_trailing_default(self) -> None:
        args = _build_aggregate_args(
            "idx",
            "*",
            [Limit(5, 25)],
            limit=10,
            timeout_ms=None,
        )
        # Exactly one LIMIT, with the explicit values
        limit_indices = [i for i, a in enumerate(args) if a == "LIMIT"]
        assert len(limit_indices) == 1
        idx = limit_indices[0]
        assert args[idx + 1] == "5"
        assert args[idx + 2] == "25"

    def test_no_limit_step_appends_default(self) -> None:
        args = _build_aggregate_args(
            "idx",
            "*",
            [GroupBy(["priority"], [Count(alias="n")])],
            limit=7,
            timeout_ms=None,
        )
        # Default trailing `LIMIT 0 7`
        assert args[-3:] == ["LIMIT", "0", "7"]

    def test_timeout_ms_appended(self) -> None:
        args = _build_aggregate_args(
            "idx", "*", [Limit(0, 10)], limit=10, timeout_ms=500
        )
        assert "TIMEOUT" in args
        assert "500" in args

    def test_empty_query_becomes_star(self) -> None:
        args = _build_aggregate_args(
            "idx", "", [Limit(0, 10)], limit=10, timeout_ms=None
        )
        assert args[2] == "*"


# -- Reducer unit tests ------------------------------------------------------


class TestReducerArgs:
    """Validate each reducer dataclass emits correct REDUCE block."""

    def test_count_no_alias(self) -> None:
        assert _reducer_args(Count()) == ["REDUCE", "COUNT", "0"]

    def test_count_with_alias(self) -> None:
        assert _reducer_args(Count(alias="n")) == ["REDUCE", "COUNT", "0", "AS", "n"]

    def test_sum(self) -> None:
        assert _reducer_args(Sum("amount", alias="t")) == [
            "REDUCE",
            "SUM",
            "1",
            "@amount",
            "AS",
            "t",
        ]

    def test_avg(self) -> None:
        assert _reducer_args(Avg("score"))[:4] == ["REDUCE", "AVG", "1", "@score"]

    def test_min_max(self) -> None:
        assert _reducer_args(Min("x"))[:4] == ["REDUCE", "MIN", "1", "@x"]
        assert _reducer_args(Max("x"))[:4] == ["REDUCE", "MAX", "1", "@x"]

    def test_count_distinct(self) -> None:
        assert _reducer_args(CountDistinct("user_id", alias="u")) == [
            "REDUCE",
            "COUNT_DISTINCT",
            "1",
            "@user_id",
            "AS",
            "u",
        ]


# -- Builder: FT.SEARCH ... HYBRID -------------------------------------------


class TestBuildHybridArgs:
    """Validate hybrid search argument construction + weight validation."""

    def test_vector_only(self) -> None:
        args = _build_hybrid_args(
            "idx",
            "crash",
            vector=[0.1, 0.2, 0.3, 0.4],
            vector_field="vec",
            sparse=None,
            sparse_field="sparse_vec",
            weights=(1.0, 1.5, 0.0),
            k_per_stream=None,
            limit=5,
            return_fields=None,
        )
        # Layout
        assert args[0:3] == ["FT.SEARCH", "idx", "crash"]
        assert "HYBRID" in args
        assert "VECTOR" in args
        assert "@vec" in args
        assert "$hybrid_vec" in args
        assert "FUSION" in args
        assert "RRF" in args
        assert "WEIGHTS" in args
        # sum weights params arg == 2 (vector only)
        assert "PARAMS" in args
        pi = args.index("PARAMS")
        assert args[pi + 1] == "2"
        assert args[pi + 2] == "hybrid_vec"
        # Sparse SHOULD NOT appear
        assert "SPARSE" not in args
        assert "$hybrid_sparse" not in args

    def test_with_sparse(self) -> None:
        args = _build_hybrid_args(
            "idx",
            "q",
            vector=[0.1, 0.2, 0.3, 0.4],
            vector_field="dense",
            sparse=[0.0, 1.0, 0.0, 0.0],
            sparse_field="sparse",
            weights=(1.0, 1.0, 1.0),
            k_per_stream=None,
            limit=10,
            return_fields=None,
        )
        assert "SPARSE" in args
        assert "@sparse" in args
        assert "$hybrid_sparse" in args
        pi = args.index("PARAMS")
        assert args[pi + 1] == "4"

    def test_k_per_stream_included(self) -> None:
        args = _build_hybrid_args(
            "idx",
            "q",
            vector=[0.1] * 4,
            vector_field="vec",
            sparse=None,
            sparse_field="sparse_vec",
            weights=(1.0, 1.0, 1.0),
            k_per_stream=60,
            limit=5,
            return_fields=None,
        )
        assert "K_PER_STREAM" in args
        idx = args.index("K_PER_STREAM")
        assert args[idx + 1] == "60"

    def test_k_per_stream_omitted(self) -> None:
        args = _build_hybrid_args(
            "idx",
            "q",
            vector=[0.1] * 4,
            vector_field="vec",
            sparse=None,
            sparse_field="sparse_vec",
            weights=(1.0, 1.0, 1.0),
            k_per_stream=None,
            limit=5,
            return_fields=None,
        )
        assert "K_PER_STREAM" not in args

    def test_empty_query_becomes_star(self) -> None:
        args = _build_hybrid_args(
            "idx",
            "",
            vector=[0.1] * 4,
            vector_field="vec",
            sparse=None,
            sparse_field="sparse_vec",
            weights=(1.0, 1.0, 1.0),
            k_per_stream=None,
            limit=5,
            return_fields=None,
        )
        assert args[2] == "*"

    def test_return_fields(self) -> None:
        args = _build_hybrid_args(
            "idx",
            "q",
            vector=[0.1] * 4,
            vector_field="vec",
            sparse=None,
            sparse_field="sparse_vec",
            weights=(1.0, 1.0, 1.0),
            k_per_stream=None,
            limit=5,
            return_fields=["title"],
        )
        assert "RETURN" in args
        idx = args.index("RETURN")
        assert args[idx + 1] == "1"
        assert args[idx + 2] == "title"


class TestHybridWeightsValidation:
    """Threat-model T-153-01-03: weights must be validated client-side."""

    def test_all_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="zero"):
            _validate_hybrid_weights((0.0, 0.0, 0.0))

    def test_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            _validate_hybrid_weights((1.0, -0.5, 1.0))

    def test_nan_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            _validate_hybrid_weights((1.0, math.nan, 1.0))

    def test_inf_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            _validate_hybrid_weights((1.0, math.inf, 1.0))

    def test_wrong_length_raises(self) -> None:
        with pytest.raises(ValueError, match="3-tuple"):
            _validate_hybrid_weights((1.0, 1.0))  # type: ignore[arg-type]

    def test_non_numeric_raises(self) -> None:
        with pytest.raises(ValueError, match="numeric"):
            _validate_hybrid_weights(("1.0", 1.0, 1.0))  # type: ignore[arg-type]

    def test_all_zero_fails_via_build_hybrid_args(self) -> None:
        with pytest.raises(ValueError):
            _build_hybrid_args(
                "idx",
                "q",
                vector=[0.1] * 4,
                vector_field="vec",
                sparse=None,
                sparse_field="sparse_vec",
                weights=(0.0, 0.0, 0.0),
                k_per_stream=None,
                limit=5,
                return_fields=None,
            )


# -- Parser: text search -----------------------------------------------------


class TestParseTextSearchResults:
    """Defensive parsing of FT.SEARCH responses (threat_model T-153-01-02)."""

    def test_none_returns_empty(self) -> None:
        assert _parse_text_search_results(None) == []

    def test_non_list_returns_empty(self) -> None:
        assert _parse_text_search_results("not a list") == []

    def test_empty_list_returns_empty(self) -> None:
        assert _parse_text_search_results([]) == []

    def test_zero_count_returns_empty(self) -> None:
        assert _parse_text_search_results([0]) == []

    def test_single_hit_with_score(self) -> None:
        raw = [
            1,
            b"issue:1",
            [b"__score__", b"3.14", b"title", b"Hello"],
        ]
        hits = _parse_text_search_results(raw)
        assert len(hits) == 1
        assert hits[0].id == "issue:1"
        assert hits[0].score == pytest.approx(3.14)
        assert hits[0].fields["title"] == "Hello"
        assert hits[0].highlights is None

    def test_fallback_text_score(self) -> None:
        raw = [
            1,
            b"issue:1",
            [b"__text_score__", b"2.71", b"title", b"World"],
        ]
        hits = _parse_text_search_results(raw)
        assert hits[0].score == pytest.approx(2.71)

    def test_highlights_extracted(self) -> None:
        raw = [
            1,
            b"issue:1",
            [
                b"__score__",
                b"1.0",
                b"title",
                b"Hello",
                b"__highlight_body",
                b"<em>crash</em>\x1fat <em>startup</em>",
            ],
        ]
        hits = _parse_text_search_results(raw, highlight_requested=True)
        assert hits[0].highlights is not None
        assert hits[0].highlights["body"] == ["<em>crash</em>", "at <em>startup</em>"]

    def test_unicode_preserved(self) -> None:
        raw = [
            1,
            b"issue:1",
            [b"__score__", b"1.0", b"title", "crise \u00e9clair".encode()],
        ]
        hits = _parse_text_search_results(raw)
        assert hits[0].fields["title"] == "crise \u00e9clair"

    def test_truncated_row_ignored(self) -> None:
        # Key without field-list → stop mid-way, never raise.
        raw = [1, b"issue:1"]
        hits = _parse_text_search_results(raw)
        assert hits == []

    def test_non_list_field_block_yields_empty_fields(self) -> None:
        raw = [1, b"issue:1", "garbage"]
        hits = _parse_text_search_results(raw)
        assert len(hits) == 1
        assert hits[0].fields == {}
        assert hits[0].score == 0.0

    def test_malformed_score_tolerated(self) -> None:
        raw = [
            1,
            b"issue:1",
            [b"__score__", b"not_a_float", b"title", b"H"],
        ]
        hits = _parse_text_search_results(raw)
        assert hits[0].score == 0.0
        assert hits[0].fields["title"] == "H"

    def test_binary_value_kept_as_bytes_on_decode_failure(self) -> None:
        bad = b"\xff\xfe\x00\x01"
        raw = [1, b"issue:1", [b"__score__", b"1.0", b"blob", bad]]
        hits = _parse_text_search_results(raw)
        assert hits[0].fields["blob"] == bad


# -- Parser: aggregate -------------------------------------------------------


class TestParseAggregateResults:
    """Defensive parsing of FT.AGGREGATE responses."""

    def test_empty(self) -> None:
        assert _parse_aggregate_results([]) == []
        assert _parse_aggregate_results(None) == []
        assert _parse_aggregate_results("garbage") == []

    def test_single_row(self) -> None:
        raw = [1, [b"priority", b"P0", b"n", b"42"]]
        rows = _parse_aggregate_results(raw)
        assert rows == [{"priority": "P0", "n": 42}]

    def test_numeric_coercion(self) -> None:
        raw = [3, [b"x", b"10"], [b"x", b"3.14"], [b"x", b"abc"]]
        rows = _parse_aggregate_results(raw)
        assert rows[0]["x"] == 10
        assert rows[1]["x"] == pytest.approx(3.14)
        assert rows[2]["x"] == "abc"

    def test_malformed_row_skipped(self) -> None:
        raw = [2, "garbage", [b"k", b"v"]]
        rows = _parse_aggregate_results(raw)
        assert rows == [{"k": "v"}]


class TestMaybeCastNumber:
    def test_int(self) -> None:
        assert _maybe_cast_number(b"42") == 42
        assert _maybe_cast_number("7") == 7

    def test_float(self) -> None:
        assert _maybe_cast_number(b"3.14") == pytest.approx(3.14)
        assert _maybe_cast_number("1e3") == pytest.approx(1000.0)

    def test_fallback_str(self) -> None:
        assert _maybe_cast_number(b"not a number") == "not a number"

    def test_empty(self) -> None:
        assert _maybe_cast_number(b"") == ""


# -- TextCommands mock-based wiring tests -----------------------------------


class TestTextCommandsArgs:
    """Assert execute_command is invoked with the correct RESP arguments."""

    def setup_method(self) -> None:
        self.mock_client: Any = MagicMock()
        self.tc = TextCommands(self.mock_client)

    def test_create_text_index_dispatches(self) -> None:
        self.tc.create_text_index(
            "idx", [("title", "TEXT", {"WEIGHT": 2.0})], prefix="doc:"
        )
        args = self.mock_client.execute_command.call_args.args
        assert args[0] == "FT.CREATE"
        assert args[1] == "idx"
        assert "SCHEMA" in args
        assert "title" in args
        assert "TEXT" in args
        assert "WEIGHT" in args

    def test_drop_text_index(self) -> None:
        self.tc.drop_text_index("idx")
        assert self.mock_client.execute_command.call_args.args == (
            "FT.DROPINDEX",
            "idx",
        )

    def test_text_search_dispatches(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.tc.text_search("idx", "crash", limit=5, highlight=True, highlight_fields=["body"])
        args = self.mock_client.execute_command.call_args.args
        assert args[0] == "FT.SEARCH"
        assert args[1] == "idx"
        assert args[2] == "crash"
        assert "HIGHLIGHT" in args
        assert "body" in args

    def test_text_search_empty_query(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.tc.text_search("idx", "")
        args = self.mock_client.execute_command.call_args.args
        assert args[2] == "*"

    def test_aggregate_dispatches(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.tc.aggregate(
            "idx",
            "*",
            [GroupBy(["priority"], [Count(alias="n")]), SortBy("n", "DESC")],
        )
        args = self.mock_client.execute_command.call_args.args
        assert args[0] == "FT.AGGREGATE"
        assert "GROUPBY" in args
        assert "SORTBY" in args

    def test_hybrid_search_dispatches(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.tc.hybrid_search(
            "idx", "crash", vector=[0.1] * 4, weights=(1.0, 1.5, 0.0), k_per_stream=60
        )
        args = self.mock_client.execute_command.call_args.args
        assert args[0] == "FT.SEARCH"
        assert "HYBRID" in args
        assert "FUSION" in args
        assert "RRF" in args
        assert "K_PER_STREAM" in args

    def test_hybrid_search_empty_query(self) -> None:
        self.mock_client.execute_command.return_value = [0]
        self.tc.hybrid_search("idx", "", vector=[0.1] * 4)
        args = self.mock_client.execute_command.call_args.args
        assert args[2] == "*"

    def test_hybrid_zero_weights_raises_before_dispatch(self) -> None:
        with pytest.raises(ValueError):
            self.tc.hybrid_search("idx", "q", vector=[0.1] * 4, weights=(0.0, 0.0, 0.0))
        # CRITICAL: no command was sent
        self.mock_client.execute_command.assert_not_called()

    def test_hybrid_nan_weights_raises_before_dispatch(self) -> None:
        with pytest.raises(ValueError):
            self.tc.hybrid_search(
                "idx", "q", vector=[0.1] * 4, weights=(1.0, math.nan, 1.0)
            )
        self.mock_client.execute_command.assert_not_called()


# -- Client wiring -----------------------------------------------------------


class TestTextCommandsWiring:
    """Verify MoonClient.text / AsyncMoonClient.text properties are wired."""

    @patch("redis.Redis.__init__", return_value=None)
    def test_moon_client_has_text_property(self, _init: MagicMock) -> None:
        client = MoonClient.__new__(MoonClient)
        # Manually set _text (bypass __init__)
        client._text = TextCommands(MagicMock())
        assert isinstance(client.text, TextCommands)

    def test_moon_client_class_property(self) -> None:
        assert hasattr(MoonClient, "text")
        assert MoonClient.text.fget is not None

    def test_async_moon_client_class_property(self) -> None:
        assert hasattr(AsyncMoonClient, "text")
        assert AsyncMoonClient.text.fget is not None


# -- Async surface -----------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncTextCommands:
    """Mock-based tests for AsyncTextCommands via AsyncMock."""

    async def test_create_text_index(self) -> None:
        mock = MagicMock()
        mock.execute_command = AsyncMock(return_value="OK")
        atc = AsyncTextCommands(mock)
        result = await atc.create_text_index(
            "idx", [("title", "TEXT", {"WEIGHT": 2.0})], prefix="doc:"
        )
        assert result == "OK"
        mock.execute_command.assert_awaited_once()
        args = mock.execute_command.call_args.args
        assert args[0] == "FT.CREATE"

    async def test_drop_text_index(self) -> None:
        mock = MagicMock()
        mock.execute_command = AsyncMock(return_value="OK")
        atc = AsyncTextCommands(mock)
        result = await atc.drop_text_index("idx")
        assert result == "OK"
        assert mock.execute_command.call_args.args == ("FT.DROPINDEX", "idx")

    async def test_text_search(self) -> None:
        mock = MagicMock()
        mock.execute_command = AsyncMock(return_value=[0])
        atc = AsyncTextCommands(mock)
        hits = await atc.text_search("idx", "q")
        assert hits == []
        mock.execute_command.assert_awaited_once()

    async def test_aggregate(self) -> None:
        mock = MagicMock()
        mock.execute_command = AsyncMock(return_value=[0])
        atc = AsyncTextCommands(mock)
        rows = await atc.aggregate("idx", "*", [Limit(0, 10)])
        assert rows == []
        args = mock.execute_command.call_args.args
        assert args[0] == "FT.AGGREGATE"

    async def test_hybrid_search(self) -> None:
        mock = MagicMock()
        mock.execute_command = AsyncMock(return_value=[0])
        atc = AsyncTextCommands(mock)
        hits = await atc.hybrid_search("idx", "q", vector=[0.1] * 4)
        assert hits == []
        args = mock.execute_command.call_args.args
        assert "HYBRID" in args

    async def test_hybrid_weights_validation_async(self) -> None:
        mock = MagicMock()
        mock.execute_command = AsyncMock()
        atc = AsyncTextCommands(mock)
        with pytest.raises(ValueError):
            await atc.hybrid_search(
                "idx", "q", vector=[0.1] * 4, weights=(0.0, 0.0, 0.0)
            )
        mock.execute_command.assert_not_called()


# -- Encoded vector helpers --------------------------------------------------


class TestEnsureBytesHelper:
    def test_passthrough_bytes(self) -> None:
        b = b"\x00\x01\x02"
        assert _ensure_bytes(b) is b

    def test_list_of_floats(self) -> None:
        out = _ensure_bytes([1.0, 2.0])
        assert isinstance(out, bytes)
        assert len(out) == 8


# -- Parser: hybrid response (Plan 153-04 — Gap 3 + Gap 5 fix) --------------


class TestParseHybridResponse:
    """Parser handling of FT.SEARCH HYBRID responses.

    The server response is::

        [total_count,
         key1, [__rrf_score, "0.5"],
         key2, [__rrf_score, "0.3"],
         bm25_hits, 5,
         dense_hits, 4,
         sparse_hits, 0]

    The parser MUST:
      - Recognise ``__rrf_score`` / ``__vec_score`` as score markers.
      - Stop the doc-pair loop via type-guard (second element not a list →
        trailer start). Raw[0] is a capped count, not a reliable stop signal.
      - Optionally extract the ``bm25_hits``/``dense_hits``/``sparse_hits``
        trailer into a ``{"bm25": int, "dense": int, "sparse": int}`` dict
        when ``return_stream_hits=True``.
      - Preserve backward-compat (list return when ``return_stream_hits=False``).
    """

    def test_parse_extracts_rrf_score(self) -> None:
        """__rrf_score marker populates hit.score (Gap 3 root cause #2)."""
        raw = [1, "doc:1", ["__rrf_score", "0.033"]]
        hits = _parse_text_search_results(raw)
        assert len(hits) == 1
        assert hits[0].id == "doc:1"
        assert hits[0].score == pytest.approx(0.033)

    def test_parse_strips_bm25_dense_sparse_trailer(self) -> None:
        """Flat 6-item trailer must not be parsed as synthetic docs (Gap 3 root cause #1)."""
        raw = [
            2,
            "doc:1", ["__rrf_score", "0.5"],
            "doc:2", ["__rrf_score", "0.3"],
            "bm25_hits", 5,
            "dense_hits", 4,
            "sparse_hits", 0,
        ]
        hits = _parse_text_search_results(raw)
        # Expect exactly 2 real docs — NOT 5 (2 docs + 3 synthetic rows).
        assert len(hits) == 2
        assert [h.id for h in hits] == ["doc:1", "doc:2"]
        assert hits[0].score == pytest.approx(0.5)
        assert hits[1].score == pytest.approx(0.3)

    def test_parse_strips_dense_sparse_only_trailer(self) -> None:
        """Vector-only hybrid has a 4-item trailer (no bm25_hits)."""
        raw = [
            1,
            "doc:1", ["__vec_score", "0.5"],
            "dense_hits", 3,
            "sparse_hits", 2,
        ]
        hits = _parse_text_search_results(raw)
        assert len(hits) == 1
        assert hits[0].id == "doc:1"
        assert hits[0].score == pytest.approx(0.5)

    def test_parse_return_stream_hits_flag(self) -> None:
        """return_stream_hits=True yields (hits, trailer_dict) tuple."""
        raw = [
            2,
            "doc:1", ["__rrf_score", "0.5"],
            "doc:2", ["__rrf_score", "0.3"],
            "bm25_hits", 5,
            "dense_hits", 4,
            "sparse_hits", 0,
        ]
        result = _parse_text_search_results(raw, return_stream_hits=True)
        assert isinstance(result, tuple)
        hits, stream = result
        assert len(hits) == 2
        assert stream == {"bm25": 5, "dense": 4, "sparse": 0}

    @pytest.mark.parametrize(
        "marker",
        ["__score__", "__text_score__", "__rrf_score", "__vec_score"],
    )
    def test_parse_mixed_rrf_and_score_markers(self, marker: str) -> None:
        """Parser accepts all four score markers equivalently."""
        raw = [1, "doc:1", [marker, "0.25"]]
        hits = _parse_text_search_results(raw)
        assert hits[0].score == pytest.approx(0.25)

    def test_parse_ignores_malformed_trailer(self) -> None:
        """A truncated or malformed trailer must not crash; return real docs + empty stream."""
        raw = [
            1,
            "doc:1", ["__rrf_score", "0.5"],
            "bm25_hits",  # value missing
        ]
        hits, stream = _parse_text_search_results(raw, return_stream_hits=True)
        assert len(hits) == 1
        assert hits[0].id == "doc:1"
        # Malformed trailer silently drops — must not crash.
        assert isinstance(stream, dict)

    def test_parse_preserves_existing_backward_compat_shape(self) -> None:
        """Without the kwarg, return type stays list[TextSearchHit] (96+ existing tests lock)."""
        raw = [
            2,
            "doc:1", ["__rrf_score", "0.5"],
            "doc:2", ["__rrf_score", "0.3"],
        ]
        result = _parse_text_search_results(raw)
        assert isinstance(result, list)
        assert len(result) == 2


# -- Public hybrid_search kwarg (WARNING 7 fix) -----------------------------


class TestPublicHybridSearchKwarg:
    """``TextCommands.hybrid_search(return_stream_hits=True)`` must return (hits, trailer).

    Locks the public contract adapters use so there are no private
    ``_build_hybrid_args`` imports.
    """

    def test_hybrid_search_public_kwarg_return_stream_hits_true(self) -> None:
        """Sync hybrid_search with the new kwarg yields a 2-tuple."""
        client: Any = MagicMock()
        client.execute_command.return_value = [
            2,
            "doc:1", ["__rrf_score", "0.5"],
            "doc:2", ["__rrf_score", "0.3"],
            "bm25_hits", 5,
            "dense_hits", 4,
            "sparse_hits", 0,
        ]
        tc = TextCommands(client)
        result = tc.hybrid_search(
            "idx",
            "q",
            vector=[0.1, 0.2, 0.3, 0.4],
            return_stream_hits=True,
        )
        assert isinstance(result, tuple)
        hits, stream = result
        assert len(hits) == 2
        assert stream == {"bm25": 5, "dense": 4, "sparse": 0}

    def test_hybrid_search_public_kwarg_default_preserves_list_contract(self) -> None:
        """Default call (no kwarg) still returns list[TextSearchHit]."""
        client: Any = MagicMock()
        client.execute_command.return_value = [
            2,
            "doc:1", ["__rrf_score", "0.5"],
            "doc:2", ["__rrf_score", "0.3"],
            "bm25_hits", 5,
            "dense_hits", 4,
            "sparse_hits", 0,
        ]
        tc = TextCommands(client)
        result = tc.hybrid_search(
            "idx", "q", vector=[0.1, 0.2, 0.3, 0.4]
        )
        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_async_hybrid_search_public_kwarg_return_stream_hits(self) -> None:
        """AsyncTextCommands.hybrid_search mirrors the sync 2-tuple contract."""
        client: Any = MagicMock()
        client.execute_command = AsyncMock(
            return_value=[
                2,
                "doc:1", ["__rrf_score", "0.5"],
                "doc:2", ["__rrf_score", "0.3"],
                "bm25_hits", 5,
                "dense_hits", 4,
                "sparse_hits", 0,
            ]
        )
        atc = AsyncTextCommands(client)
        result = await atc.hybrid_search(
            "idx",
            "q",
            vector=[0.1, 0.2, 0.3, 0.4],
            return_stream_hits=True,
        )
        assert isinstance(result, tuple)
        hits, stream = result
        assert len(hits) == 2
        assert stream == {"bm25": 5, "dense": 4, "sparse": 0}
