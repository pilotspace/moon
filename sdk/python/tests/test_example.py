"""Dry-run unit tests for ``examples/pilotspace_example.py``.

Covers deterministic fixture generation, CLI dispatch, error-handling
paths, and dry-run smoke output. Patterned after ``tests/test_text.py``
(mock-based dispatch assertion) and ``tests/test_client.py`` (CLI
surface tests).

The test module imports the example via ``importlib.util`` when the
``examples`` package is not yet pip-installed into the env — this keeps
the tests portable across development checkouts (``pip install -e .``
vs. ``pip install .`` vs. running from a raw clone).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


def _load_example_module() -> ModuleType:
    """Load ``examples/pilotspace_example.py`` as a module.

    Try direct ``from examples.pilotspace_example`` first; fall back to
    ``importlib.util`` if the ``examples`` package is not on
    ``sys.path`` (e.g. raw checkout without ``pip install -e .``).
    """
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


pilotspace_example = _load_example_module()

build_sample_issues = pilotspace_example.build_sample_issues
query_embedding = pilotspace_example.query_embedding
run_demo = pilotspace_example.run_demo
main = pilotspace_example.main
SampleIssue = pilotspace_example.SampleIssue
DEFAULT_SEED = pilotspace_example.DEFAULT_SEED
DEFAULT_DIM = pilotspace_example.DEFAULT_DIM
DEFAULT_NUM_ISSUES = pilotspace_example.DEFAULT_NUM_ISSUES
SAMPLE_TITLES = pilotspace_example.SAMPLE_TITLES
SAMPLE_BODIES = pilotspace_example.SAMPLE_BODIES
_build_arg_parser = pilotspace_example._build_arg_parser
_build_dry_run_client = pilotspace_example._build_dry_run_client
_safe_drop_index = pilotspace_example._safe_drop_index


# -- Fixture builders --------------------------------------------------------


class TestBuildSampleIssues:
    """build_sample_issues: determinism + shape + content invariants."""

    def test_deterministic_same_seed(self) -> None:
        a = build_sample_issues(seed=DEFAULT_SEED)
        b = build_sample_issues(seed=DEFAULT_SEED)
        assert len(a) == len(b)
        for x, y in zip(a, b):
            assert x.id == y.id
            assert x.title == y.title
            assert x.body == y.body
            assert x.status == y.status
            assert x.priority == y.priority
            assert x.embedding == y.embedding

    def test_different_seeds_differ(self) -> None:
        a = build_sample_issues(seed=0x1)
        b = build_sample_issues(seed=0x2)
        # Embeddings must differ (XOR with different base seeds)
        assert a[0].embedding != b[0].embedding

    def test_n_and_dim(self) -> None:
        issues = build_sample_issues(n=7, dim=64)
        assert len(issues) == 7
        for issue in issues:
            assert len(issue.embedding) == 64

    def test_default_n_and_dim(self) -> None:
        issues = build_sample_issues()
        assert len(issues) == DEFAULT_NUM_ISSUES
        assert len(issues[0].embedding) == DEFAULT_DIM

    def test_title_and_body_count_at_least_20(self) -> None:
        assert len(SAMPLE_TITLES) >= 20
        assert len(set(SAMPLE_TITLES)) >= 20, "titles must be distinct"
        assert len(SAMPLE_BODIES) >= 20
        assert len(set(SAMPLE_BODIES)) >= 20, "bodies must be distinct"

    def test_priority_distribution_cycles_evenly(self) -> None:
        issues = build_sample_issues(n=20)
        priorities = [i.priority for i in issues]
        # With n=20 and 3 priorities cycling, counts are 7, 7, 6 in some order
        assert priorities.count("P0") >= 6
        assert priorities.count("P1") >= 6
        assert priorities.count("P2") >= 6
        assert priorities.count("P0") + priorities.count("P1") + priorities.count("P2") == 20

    def test_status_distribution_cycles(self) -> None:
        issues = build_sample_issues(n=20)
        statuses = set(i.status for i in issues)
        assert statuses == {"open", "in_progress", "closed"}

    def test_unicode_present(self) -> None:
        # At least one title OR body contains a non-ASCII character
        any_unicode = any(
            not s.isascii() for s in SAMPLE_TITLES
        ) or any(not s.isascii() for s in SAMPLE_BODIES)
        assert any_unicode, "at least one title or body must contain Unicode"

    def test_sample_issue_is_frozen(self) -> None:
        issue = SampleIssue(
            id="x", title="t", body="b", status="open", priority="P0", embedding=[0.1]
        )
        with pytest.raises((AttributeError, Exception)):
            issue.id = "y"  # type: ignore[misc]

    def test_issue_ids_are_zero_padded(self) -> None:
        issues = build_sample_issues(n=12)
        # All IDs shaped "issue:NNN" with 3-digit pad
        for i, issue in enumerate(issues):
            assert issue.id == f"issue:{i:03d}"


class TestQueryEmbedding:
    """query_embedding: deterministic stub vector."""

    def test_deterministic(self) -> None:
        a = query_embedding("crash on startup")
        b = query_embedding("crash on startup")
        assert a == b

    def test_different_text_differs(self) -> None:
        a = query_embedding("crash")
        b = query_embedding("memory")
        assert a != b

    def test_dim_customizable(self) -> None:
        v = query_embedding("x", dim=128)
        assert len(v) == 128

    def test_default_dim_is_384(self) -> None:
        v = query_embedding("x")
        assert len(v) == DEFAULT_DIM

    def test_unicode_query(self) -> None:
        v = query_embedding("café résumé 🎉")
        assert len(v) == DEFAULT_DIM


# -- run_demo: mock-based dispatch assertions --------------------------------


class TestRunDemo:
    """run_demo: verify dispatch + return shape + error handling."""

    def setup_method(self) -> None:
        self.client: Any = _build_dry_run_client()

    def test_returns_dict_with_three_keys(self) -> None:
        out = run_demo(self.client)
        assert set(out.keys()) == {"text_hits", "facet_rows", "hybrid_hits"}

    def test_calls_create_text_index(self) -> None:
        run_demo(self.client)
        self.client.text.create_text_index.assert_called_once()
        call = self.client.text.create_text_index.call_args
        # name must be "issues", schema must include title, body, status, priority
        assert call.args[0] == "issues"
        schema = call.args[1]
        field_names = [f[0] for f in schema]
        assert "title" in field_names
        assert "body" in field_names
        assert "status" in field_names
        assert "priority" in field_names

    def test_calls_text_search(self) -> None:
        run_demo(self.client)
        self.client.text.text_search.assert_called_once()
        call = self.client.text.text_search.call_args
        assert call.args[0] == "issues"
        assert "crash" in call.args[1]

    def test_calls_aggregate(self) -> None:
        run_demo(self.client)
        self.client.text.aggregate.assert_called_once()
        call = self.client.text.aggregate.call_args
        assert call.args[0] == "issues"

    def test_calls_hybrid_search(self) -> None:
        run_demo(self.client)
        self.client.text.hybrid_search.assert_called_once()

    def test_hybrid_weights_forwarded(self) -> None:
        run_demo(self.client)
        call = self.client.text.hybrid_search.call_args
        assert call.kwargs["weights"] == (1.0, 1.5, 0.0)

    def test_ft_dropindex_called_first(self) -> None:
        run_demo(self.client)
        first_call = self.client.execute_command.call_args_list[0]
        # Plan 06 drops the Redis-only DD flag (BLOCKER 1). Moon's
        # FT.DROPINDEX is strict-arity 1. With `_probe_tag_cached=True`
        # pre-populated on the dry-run client, the probe never fires
        # execute_command, so the first call is the real drop.
        assert first_call.args == ("FT.DROPINDEX", "issues")

    def test_dropindex_error_swallowed_when_index_missing(self) -> None:
        # Simulate server saying the index does not exist
        self.client.execute_command.side_effect = Exception(
            "Unknown: index not found"
        )
        # Must not raise
        out = run_demo(self.client)
        assert "text_hits" in out

    def test_dropindex_error_swallowed_case_insensitive(self) -> None:
        self.client.execute_command.side_effect = Exception("NO SUCH INDEX")
        out = run_demo(self.client)
        assert "text_hits" in out

    def test_dropindex_error_reraised_on_other_errors(self) -> None:
        self.client.execute_command.side_effect = Exception("auth required")
        with pytest.raises(Exception, match="auth required"):
            run_demo(self.client)

    def test_seeds_num_issues_via_pipeline(self) -> None:
        run_demo(self.client, num_issues=5)
        # The dry-run client exposes .pipeline -> MagicMock; count hset calls
        pipe = self.client.pipeline.return_value
        assert pipe.hset.call_count == 5

    def test_custom_num_issues_and_dim(self) -> None:
        out = run_demo(self.client, num_issues=3, dim=64)
        assert set(out.keys()) == {"text_hits", "facet_rows", "hybrid_hits"}
        # Verify the query vector was the correct dim
        hybrid_call = self.client.text.hybrid_search.call_args
        assert len(hybrid_call.kwargs["vector"]) == 64


class TestSafeDropIndex:
    """_safe_drop_index: error classification."""

    def test_swallows_unknown(self) -> None:
        client = MagicMock()
        client.execute_command.side_effect = Exception("Unknown Index 'foo'")
        _safe_drop_index(client, "foo")  # must not raise

    def test_swallows_not_found(self) -> None:
        client = MagicMock()
        client.execute_command.side_effect = Exception("index not found")
        _safe_drop_index(client, "foo")

    def test_reraises_auth_error(self) -> None:
        client = MagicMock()
        client.execute_command.side_effect = Exception("NOAUTH required")
        with pytest.raises(Exception, match="NOAUTH"):
            _safe_drop_index(client, "foo")

    def test_safe_drop_index_swallows_wrong_number_of_arguments(self) -> None:
        """Plan 06 (Gap 1): moon's FT.DROPINDEX returns
        'wrong number of arguments' when the caller passes extra tokens
        such as the Redis-only DD flag. _safe_drop_index must treat this
        as benign so older RediSearch-style scripts don't hard-fail."""
        client = MagicMock()
        client.execute_command.side_effect = Exception(
            "wrong number of arguments for 'FT.DROPINDEX' command"
        )
        _safe_drop_index(client, "foo")  # must not raise

    def test_safe_drop_index_calls_ft_dropindex_without_dd_flag(self) -> None:
        """Plan 06 (Gap 1): the real call path must omit the DD flag."""
        client = MagicMock()
        client.execute_command.return_value = "OK"
        _safe_drop_index(client, "issues")
        client.execute_command.assert_called_once_with("FT.DROPINDEX", "issues")


# -- _probe_tag_support: capability probe + cache ---------------------------


class TestProbeTagSupport:
    """Plan 06: _probe_tag_support honours the on-client cache and short-
    circuits without any server calls when the cache key is set."""

    def test_probe_tag_support_uses_cache_when_present(self) -> None:
        """When the cache key is pre-populated, the probe MUST short-
        circuit — no call to create_text_index, no call to
        execute_command. This is what preserves the existing
        TestRunDemo assertions (BLOCKER 1/2/3)."""
        probe = pilotspace_example._probe_tag_support
        client = MagicMock()
        client.__dict__["_probe_tag_cached"] = False
        assert probe(client) is False
        assert client.text.create_text_index.call_count == 0
        assert client.execute_command.call_count == 0

    def test_probe_tag_support_returns_false_when_create_rejects_tag(self) -> None:
        probe = pilotspace_example._probe_tag_support
        client = MagicMock()
        # No pre-populated cache → probe path runs.
        client.__dict__.pop("_probe_tag_cached", None)
        client.text.create_text_index.side_effect = Exception(
            "ERR expected VECTOR, SPARSE, or TEXT after field name"
        )
        assert probe(client) is False
        # Cache must now be populated as False so repeat probes are free.
        assert client.__dict__["_probe_tag_cached"] is False

    def test_probe_tag_support_returns_true_and_drops_probe_index(self) -> None:
        probe = pilotspace_example._probe_tag_support
        client = MagicMock()
        client.__dict__.pop("_probe_tag_cached", None)
        client.text.create_text_index.return_value = "OK"
        client.execute_command.return_value = "OK"
        assert probe(client) is True
        # execute_command must have been called with FT.DROPINDEX on the
        # synthetic probe index name.
        dropindex_calls = [
            c
            for c in client.execute_command.call_args_list
            if c.args[:2] == ("FT.DROPINDEX", "__moon_probe_tag__")
        ]
        assert len(dropindex_calls) >= 1
        assert client.__dict__["_probe_tag_cached"] is True


class TestRunDemoProbeFallback:
    """Plan 06: run_demo must fall back to a TEXT-only schema when the
    probe reports that TAG is not accepted by the server."""

    def test_run_demo_falls_back_to_text_schema_when_tag_unsupported(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        client = _build_dry_run_client()
        # Force fallback path: mark probe as "tag not supported".
        client.__dict__["_probe_tag_cached"] = False
        run_demo(client, num_issues=2, dim=4)
        # Schema must not contain any TAG fields; all four should be TEXT.
        call = client.text.create_text_index.call_args
        schema = call.args[1]
        field_types = [f[1] for f in schema]
        assert field_types.count("TAG") == 0
        assert field_types.count("TEXT") == 4
        # Stderr warning must surface the fallback event.
        err = capsys.readouterr().err
        assert "TAG field type not supported" in err

    def test_probe_does_not_interfere_with_dropindex_error_classification(self) -> None:
        """BLOCKER 3 extra safety: the probe's own FT.DROPINDEX success
        must not mask a REAL drop-index failure downstream. In run_demo
        the execute_command call order is:

        1. Real FT.DROPINDEX 'issues'  (via _safe_drop_index, pre-probe)
        2. Probe FT.DROPINDEX '__moon_probe_tag__'  (probe cleanup)

        Call 1 raises 'auth required' — must propagate and not be
        swallowed by the probe's own try/except clean-up."""
        client = _build_dry_run_client()
        # Clear the cache so the probe actually runs downstream.
        client.__dict__.pop("_probe_tag_cached", None)

        def execute_side(*args: Any, **_kwargs: Any) -> Any:  # noqa: ANN401
            # First and only call: real FT.DROPINDEX 'issues' →
            # raise. Probe never reaches its own execute_command
            # because run_demo re-raises before then.
            if args[:2] == ("FT.DROPINDEX", "issues"):
                raise Exception("auth required")
            return "OK"

        client.execute_command.side_effect = execute_side
        with pytest.raises(Exception, match="auth required"):
            run_demo(client)


# -- main / CLI --------------------------------------------------------------


class TestMain:
    """CLI entry point: argparse defaults + dry-run smoke."""

    def test_dry_run_returns_zero(self) -> None:
        assert main(["--dry-run"]) == 0

    def test_dry_run_prints_all_three_headers(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["--dry-run"])
        out = capsys.readouterr().out
        assert "== Text search" in out
        assert "== Aggregate facet" in out
        assert "== Hybrid search" in out

    def test_dry_run_prints_dry_run_banner(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["--dry-run"])
        out = capsys.readouterr().out
        assert "[dry-run]" in out

    def test_dry_run_with_custom_dim(self) -> None:
        assert main(["--dry-run", "--dim", "64", "--num-issues", "5"]) == 0

    def test_dry_run_with_custom_seed(self) -> None:
        assert main(["--dry-run", "--seed", "42"]) == 0

    def test_dry_run_with_custom_index(self, capsys: pytest.CaptureFixture[str]) -> None:
        rc = main(["--dry-run", "--index", "tickets"])
        assert rc == 0
        out = capsys.readouterr().out
        # Three headers still printed regardless of index name
        assert "== Text search" in out

    def test_real_mode_connection_error_returns_2(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Patch MoonClient so instantiation raises a ConnectionError
        with patch.object(
            pilotspace_example,
            "MoonClient",
            side_effect=ConnectionError("ECONNREFUSED 127.0.0.1:6399"),
        ):
            rc = main([])
        assert rc == 2
        captured = capsys.readouterr()
        assert "ERROR" in captured.err
        assert "cannot reach Moon" in captured.err

    def test_real_mode_ping_failure_returns_2(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Instantiation succeeds, ping raises -> still exit 2
        mock_client = MagicMock()
        mock_client.ping.side_effect = ConnectionError("server gone")
        with patch.object(
            pilotspace_example, "MoonClient", return_value=mock_client
        ):
            rc = main([])
        assert rc == 2
        captured = capsys.readouterr()
        assert "cannot reach Moon" in captured.err


class TestArgParser:
    """Argparse defaults: confirm documented values."""

    def test_default_host(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.host == "localhost"

    def test_default_port(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.port == 6399

    def test_default_seed_is_0xc0de(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.seed == DEFAULT_SEED
        assert args.seed == 0xC0DE

    def test_default_num_issues(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.num_issues == DEFAULT_NUM_ISSUES

    def test_default_dim(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.dim == DEFAULT_DIM

    def test_default_index(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.index == "issues"

    def test_dry_run_off_by_default(self) -> None:
        args = _build_arg_parser().parse_args([])
        assert args.dry_run is False

    def test_dry_run_flag_parses(self) -> None:
        args = _build_arg_parser().parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_custom_host_port(self) -> None:
        args = _build_arg_parser().parse_args(
            ["--host", "moon.example", "--port", "1234"]
        )
        assert args.host == "moon.example"
        assert args.port == 1234
