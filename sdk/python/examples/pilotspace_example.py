"""PilotSpace-shaped demo for the moondb SDK.

Seeds an ``issues`` index with 20 deterministic sample issues, then runs
three representative queries:

1. **BM25 text search** -- ``client.text.text_search("issues", "crash on
   startup", limit=5)`` with field highlighting.
2. **Aggregate facet** -- ``client.text.aggregate(...)`` with
   ``GroupBy(["priority"], [Count(alias="n")]) + SortBy("n", "DESC")``.
3. **Hybrid search** -- ``client.text.hybrid_search("issues", "crash bug
   memory", vector=..., weights=(1.0, 1.5, 0.0))``.

Run against a local Moon server::

    uv run python examples/pilotspace_example.py --host localhost --port 6399

Run in CI / without a server (mocked client, no network)::

    uv run python examples/pilotspace_example.py --dry-run

Deterministic: every run with the same ``--seed`` yields the same sample
issues + vectors. Idempotent: the script calls ``FT.DROPINDEX issues DD``
before every creation so re-runs against the same server do not error.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import sys
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

try:
    import numpy as np
except ImportError:  # pragma: no cover - exercised by CI env without numpy
    print(
        "ERROR: numpy is required to run this example.\n"
        "  Install with: pip install 'moondb[examples]'\n"
        "  Or with uv:   uv sync --extra examples",
        file=sys.stderr,
    )
    sys.exit(2)

from moondb import (
    Count,
    GroupBy,
    Limit,
    MoonClient,
    SortBy,
    TextSearchHit,
    encode_vector,
)

# -- Constants ---------------------------------------------------------------

DEFAULT_SEED: int = 0xC0DE
DEFAULT_INDEX: str = "issues"
DEFAULT_DIM: int = 384
DEFAULT_NUM_ISSUES: int = 20

STATUSES: tuple[str, ...] = ("open", "in_progress", "closed")
PRIORITIES: tuple[str, ...] = ("P0", "P1", "P2")

# 24 realistic bug-tracker titles (>= 20 as required). Each 3-8 words, each
# contains at least one of the anchor terms (crash, startup, memory, login,
# auth, bug) so BM25 has signal to rank against.
SAMPLE_TITLES: tuple[str, ...] = (
    "Login button unresponsive after OAuth redirect",
    "Memory leak in session cache under load",
    "Crash on startup when config is missing",
    "Auth flow returns 500 on expired JWT",
    "Null pointer at cold startup of worker",
    "Login form CSRF token rotation bug",
    "Memory spike during bulk export",
    "Crash reporter fails to upload stack traces",
    "Auth middleware drops headers on redirect",
    "Startup latency regression after v2.3 upgrade",
    "Intermittent login failures on mobile Safari",
    "Heap fragmentation causes crash after 24h",
    "Resume café file upload corrupted",
    "OAuth login loop on Chromium browsers",
    "Startup crash when locale is unset",
    "Memory usage grows without bound in cache",
    "Auth token not refreshed before expiry",
    "Crash during graceful shutdown sequence",
    "Login rate limit incorrectly triggered",
    "Memory leak in websocket handler on disconnect",
    "Startup probe fails when sidecar is slow",
    "Crash bug in vector indexer on empty input",
    "Auth provider timeout not surfaced to user",
    "Login session revoked prematurely after idle",
)

# Parallel list of longer paragraph bodies. Each references at least two anchor
# terms so the "crash on startup" / "crash bug memory" queries retrieve
# relevant hits rather than an empty set.
SAMPLE_BODIES: tuple[str, ...] = (
    "Reproduction: click the login button, wait for the OAuth redirect, "
    "and the page hangs with a spinner. Happens on startup of a fresh browser "
    "session. Suspect auth callback race condition after memory pressure.",
    "Long-running processes show unbounded memory growth in the session "
    "cache. After 12h of sustained load the server OOMs and triggers a crash. "
    "Heap profile points at the session-key eviction path.",
    "If the config file is absent at startup the server panics instead of "
    "falling back to defaults. This is a hard crash, not a graceful exit — "
    "systemd keeps restarting in a tight loop until the disk fills with logs.",
    "Expired JWT tokens from the auth provider return a generic 500 instead "
    "of a 401. The login retry logic then treats this as a transient error "
    "and loops forever. Happens right after a memory-pressure event.",
    "Cold-start crash in the worker pool when the warmup routine runs before "
    "the thread-local cache is allocated. Stack trace shows a null pointer "
    "deref in the memory allocator path, not in our startup code.",
    "CSRF token rotation occasionally marks a valid login form as stale. "
    "Users see a 'please refresh and try again' error on submit. Correlates "
    "with auth cookie refresh timing; no memory or crash signal, just UX.",
    "During bulk CSV export the process RSS grows 10x and then the OOM "
    "killer reaps it. Not quite a crash bug but a memory regression. "
    "Introduced in the v2.3 refactor of the export pipeline.",
    "When the app crashes the stack-trace upload to the crash reporter "
    "silently fails if auth to the reporter's endpoint is expired. We lose "
    "the crash context. Affects startup reports too.",
    "Auth middleware strips headers on 302 redirect, so the login flow loses "
    "the session cookie and the user is sent back to the start. Happens on "
    "mobile browsers mostly. Not a crash, but blocks every auth attempt.",
    "Startup takes 12s after upgrading from v2.2 to v2.3 (was 3s). Profiling "
    "points at a new sync memory-map step in the crash-safe WAL init path. "
    "Login requests time out during the window.",
    "Mobile Safari users report intermittent login failures. The auth pop-up "
    "opens, they complete MFA, and on return the page shows a stale "
    "'session expired' message. No crash, no memory issue, just login UX.",
    "After ~24h of uptime the server crashes with a SIGSEGV in the memory "
    "allocator. Heap fragmentation is the root cause. A scheduled restart "
    "works around it but is not a real fix. Login handlers also affected.",
    "File uploads whose names contain accented characters (café, résumé) "
    "corrupt on disk. The bug is in our upload parser, not the auth or "
    "login path. No crash, but data loss on restart (memory-buffered).",
    "OAuth login on Chromium-based browsers enters a redirect loop about "
    "10% of the time. Seems correlated with auth cookie SameSite=None "
    "changes. No crash or memory leak, pure login UX bug.",
    "Server crashes at startup if LC_ALL is unset. The locale-aware date "
    "formatter panics during config parse. This is a pure startup crash, "
    "no memory or auth code involved.",
    "Cache memory grows linearly and never reclaims. No hard OOM yet, but "
    "a memory leak bug — after a week the process uses 4x the expected "
    "working set. Login latency also regresses.",
    "Auth tokens are not refreshed before expiry on the background worker "
    "path. The token expires mid-request, the backend rejects it, and we "
    "surface a generic login-required error. No crash, just confusing UX.",
    "During a clean shutdown the drain handler sometimes triggers a crash "
    "in the connection-close path. Looks like a memory use-after-free on a "
    "session-slot struct. Reproduces under load with many active logins.",
    "Login rate limiter triggers on legitimate users who open 3+ tabs. The "
    "per-IP bucket counts each tab as a new login attempt. No crash, but a "
    "real UX bug that locks users out for 15 minutes.",
    "Websocket handler on abrupt client disconnect leaks memory — each "
    "closed socket retains a 256KB read buffer. After ~10k connections the "
    "process crashes under memory pressure. Affects streaming login tokens.",
    "Kubernetes startup probe fails when the sidecar container is slow to "
    "come up. The main process then restarts, creating a crash loop. "
    "Memory footprint of the sidecar is also unusually high.",
    "Vector indexer panics on an empty input batch. This is a reproducible "
    "crash bug — zero-length input should return an empty result, not raise. "
    "No memory issue, no auth path involved, pure logic bug.",
    "Auth provider timeouts are silently swallowed by our retry loop, so "
    "the user sees a 'login again' prompt without any indication of why. "
    "No crash, no memory leak, but the login UX is confusing.",
    "Session is revoked after 30s of client idleness even when the user is "
    "actively interacting. The heartbeat check uses the wrong timestamp. "
    "Users get kicked back to the login screen. No crash or memory leak.",
)


# -- Public data model -------------------------------------------------------


@dataclass(frozen=True)
class SampleIssue:
    """Deterministic PilotSpace-shaped issue fixture.

    Attributes:
        id: Hash key (e.g. ``"issue:003"``).
        title: Short bug-tracker title (3-8 words).
        body: Longer paragraph body (~30-80 words).
        status: One of ``{"open", "in_progress", "closed"}``.
        priority: One of ``{"P0", "P1", "P2"}``.
        embedding: Dense vector of length ``dim`` (default 384), as a
            plain ``list[float]`` so it survives a round-trip through
            ``json.dumps`` / fixtures without depending on numpy.
    """

    id: str
    title: str
    body: str
    status: str
    priority: str
    embedding: list[float] = field(default_factory=list)


# -- Fixture builders --------------------------------------------------------


def build_sample_issues(
    n: int = DEFAULT_NUM_ISSUES,
    dim: int = DEFAULT_DIM,
    seed: int = DEFAULT_SEED,
) -> list[SampleIssue]:
    """Build a deterministic list of :class:`SampleIssue` fixtures.

    Two successive calls with the same seed return structurally identical
    lists. Titles / bodies cycle through :data:`SAMPLE_TITLES` /
    :data:`SAMPLE_BODIES`; statuses + priorities cycle through
    :data:`STATUSES` / :data:`PRIORITIES`; embeddings are seeded per-issue
    via :func:`numpy.random.default_rng` so vectors are deterministic.

    Args:
        n: Number of issues to generate (default 20).
        dim: Vector dimensionality (default 384).
        seed: Base RNG seed (default :data:`DEFAULT_SEED`).

    Returns:
        Deterministic list of ``n`` :class:`SampleIssue` instances.
    """
    issues: list[SampleIssue] = []
    for i in range(n):
        # Offset the seed per-issue so each embedding is distinct yet
        # deterministic. Using (seed ^ i) rather than a single RNG drawn
        # n*dim samples makes later lookups (e.g. the query vector) easier
        # to reason about.
        rng = np.random.default_rng(seed ^ i)
        emb: list[float] = rng.standard_normal(dim).astype(np.float32).tolist()
        issues.append(
            SampleIssue(
                id=f"issue:{i:03d}",
                title=SAMPLE_TITLES[i % len(SAMPLE_TITLES)],
                body=SAMPLE_BODIES[i % len(SAMPLE_BODIES)],
                status=STATUSES[i % len(STATUSES)],
                priority=PRIORITIES[i % len(PRIORITIES)],
                embedding=emb,
            )
        )
    return issues


def query_embedding(
    text: str,
    dim: int = DEFAULT_DIM,
    seed: int = DEFAULT_SEED,
) -> list[float]:
    """Deterministic stub query vector derived from ``text``.

    The real demo would call a sentence-transformer / OpenAI embedding
    model; for a dependency-free deterministic fixture we hash the query
    text (SHA-256) and use the first 4 bytes as a secondary RNG seed,
    XOR-ed with the base ``seed`` for consistency with
    :func:`build_sample_issues`.

    Args:
        text: Query string.
        dim: Vector dimensionality (default 384).
        seed: Base RNG seed (default :data:`DEFAULT_SEED`).

    Returns:
        A ``list[float]`` of length ``dim``.
    """
    text_digest = hashlib.sha256(text.encode("utf-8")).digest()
    text_seed = int.from_bytes(text_digest[:4], "little")
    rng = np.random.default_rng(seed ^ text_seed)
    values: list[float] = rng.standard_normal(dim).astype(np.float32).tolist()
    return values


# -- Demo body ---------------------------------------------------------------


def _safe_drop_index(client: Any, index: str) -> None:  # noqa: ANN401 -- Mock|MoonClient
    """Best-effort ``FT.DROPINDEX`` for idempotent re-runs.

    Swallows only errors whose message indicates the index does not exist
    or that the server rejects the RediSearch-only ``DD`` flag. Any other
    error (auth, network, server bug) is re-raised so users see the real
    problem (threat_model T-153-03-06 / T-153-06-01).

    Plan 06 (Gap 1): the call path omits the ``DD`` flag because moon's
    ``FT.DROPINDEX`` is strict-arity 1. Both moon and RediSearch accept
    the single-argument form. The ``"wrong number of arguments"`` match
    is kept in the swallow list so older on-disk caches of this module
    (or downstream forks that still pass ``DD``) fail softly.
    """
    try:
        client.execute_command("FT.DROPINDEX", index)
    except Exception as exc:  # noqa: BLE001 -- we re-raise non-matching errors
        msg = str(exc).lower()
        if any(
            marker in msg
            for marker in (
                "unknown",
                "not found",
                "no such index",
                "wrong number of arguments",
            )
        ):
            return
        raise


# -- TAG-support capability probe (Plan 06) ---------------------------------

_PROBE_CACHE_KEY = "_probe_tag_cached"
_PROBE_INDEX = "__moon_probe_tag__"


def _probe_tag_support(client: Any) -> bool:  # noqa: ANN401 -- Mock|MoonClient
    """Return ``True`` iff the server accepts ``TAG`` in ``FT.CREATE`` schema.

    Result is cached on ``client.__dict__[_PROBE_CACHE_KEY]`` so:

    - repeat probes during a single demo run don't re-query the server,
    - tests can pre-populate the cache for deterministic ordering
      (the dry-run MagicMock client ships with the key pre-set to
      ``True``, so existing TestRunDemo assertions on
      ``assert_called_once`` and ``call_args_list[0]`` keep their
      single-call semantics — see threat_model T-153-06-05 / T-153-06-07).

    When the cached value is a ``bool`` it is returned verbatim with
    zero server calls. Otherwise the probe creates a tiny throw-away
    ``FT.CREATE __moon_probe_tag__ SCHEMA f TAG`` index, drops it, and
    caches the outcome.
    """
    cached = client.__dict__.get(_PROBE_CACHE_KEY)
    if isinstance(cached, bool):
        return cached
    try:
        client.text.create_text_index(_PROBE_INDEX, [("f", "TAG", {})])
    except Exception:  # noqa: BLE001 -- any server-side rejection → TAG unsupported
        client.__dict__[_PROBE_CACHE_KEY] = False
        return False
    with contextlib.suppress(Exception):
        client.execute_command("FT.DROPINDEX", _PROBE_INDEX)
    client.__dict__[_PROBE_CACHE_KEY] = True
    return True


def run_demo(
    client: Any,  # noqa: ANN401 -- MoonClient or MagicMock
    *,
    index: str = DEFAULT_INDEX,
    num_issues: int = DEFAULT_NUM_ISSUES,
    dim: int = DEFAULT_DIM,
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Run the full PilotSpace demo against ``client``.

    1. Drop the existing index (idempotent — no-op on first run).
    2. Create the text+tag schema (vector field is tracked but the VECTOR
       opt is not re-emitted through ``create_text_index`` — Plan 01's
       surface is TEXT/TAG/NUMERIC; the vector index is built implicitly
       by the server on HSET for ``vec`` when it is numeric-blob shaped).
    3. Seed ``num_issues`` hashes via the pipeline.
    4. Run three demo queries and print their results.

    Args:
        client: A :class:`moondb.MoonClient` or a :class:`MagicMock` for
            dry-run mode.
        index: Index name (default ``"issues"``).
        num_issues: Number of sample issues to seed (default 20).
        dim: Vector dimensionality (default 384).
        seed: Base RNG seed (default :data:`DEFAULT_SEED`).

    Returns:
        ``dict`` with keys ``text_hits``, ``facet_rows``, ``hybrid_hits``
        for downstream assertion by :mod:`tests.test_example`.
    """
    # 1. Idempotent drop
    _safe_drop_index(client, index)

    # 2. Create text + tag index. The ``vec`` field is left out of the
    # FT schema here; PilotSpace-style ingestion builds the vector index
    # implicitly (or via a sibling ``client.vector.create_index`` call).
    # We keep this plan-faithful to Plan 01's create_text_index contract
    # which is TEXT/TAG/NUMERIC-only.
    prefix = "issue:" if index == "issues" else f"{index.rstrip('s')}:"
    supports_tag = _probe_tag_support(client)
    schema: list[tuple[str, str, dict[str, Any]]]
    if supports_tag:
        schema = [
            ("title", "TEXT", {"WEIGHT": 2.0}),
            ("body", "TEXT", {}),
            ("status", "TAG", {}),
            ("priority", "TAG", {"SORTABLE": True}),
        ]
    else:
        print(
            "[warning] TAG field type not supported by server; "
            "falling back to TEXT for status/priority.",
            file=sys.stderr,
        )
        schema = [
            ("title", "TEXT", {"WEIGHT": 2.0}),
            ("body", "TEXT", {}),
            ("status", "TEXT", {}),
            ("priority", "TEXT", {"SORTABLE": True}),
        ]
    client.text.create_text_index(index, schema, prefix=prefix)

    # 3. Seed the hashes. Use a pipeline when the client exposes one
    # (real MoonClient), otherwise fall back to per-issue execute_command
    # (some mock clients in tests may not wire a pipeline factory).
    issues = build_sample_issues(n=num_issues, dim=dim, seed=seed)
    pipeline_factory = getattr(client, "pipeline", None)
    if callable(pipeline_factory):
        pipe = pipeline_factory(transaction=False)
        for issue in issues:
            pipe.hset(
                issue.id,
                mapping={
                    "title": issue.title,
                    "body": issue.body,
                    "status": issue.status,
                    "priority": issue.priority,
                    "vec": encode_vector(issue.embedding),
                },
            )
        pipe.execute()
    else:  # pragma: no cover - defensive fallback for exotic mock clients
        for issue in issues:
            client.execute_command(
                "HSET",
                issue.id,
                "title",
                issue.title,
                "body",
                issue.body,
                "status",
                issue.status,
                "priority",
                issue.priority,
                "vec",
                encode_vector(issue.embedding),
            )

    # 4. Demo A -- BM25 text search
    print("== Text search: 'crash on startup' ==")
    text_hits: list[TextSearchHit] = client.text.text_search(
        index,
        "crash on startup",
        limit=5,
        highlight=True,
        highlight_fields=["body"],
    )
    if not text_hits:
        print("  (no hits)")
    for hit in text_hits:
        title = hit.fields.get("title", "")
        print(f"  [{hit.score:.3f}] {hit.id}  title={title!r}")

    # 5. Demo B -- priority facet via FT.AGGREGATE
    print("\n== Aggregate facet (by priority) ==")
    facet_rows: list[dict[str, Any]] = client.text.aggregate(
        index,
        "*",
        [
            GroupBy(["priority"], [Count(alias="n")]),
            SortBy("n", "DESC"),
            Limit(0, 10),
        ],
    )
    if not facet_rows:
        print("  (no rows)")
    for row in facet_rows:
        print(f"  priority={row.get('priority')!s:<8} n={row.get('n')}")

    # 6. Demo C -- hybrid BM25 + dense fusion via RRF
    print("\n== Hybrid search: 'crash bug memory' ==")
    q_vec = query_embedding("crash bug memory", dim=dim, seed=seed)
    hybrid_hits: list[TextSearchHit] = client.text.hybrid_search(
        index,
        "crash bug memory",
        vector=q_vec,
        vector_field="vec",
        weights=(1.0, 1.5, 0.0),
        limit=5,
    )
    if not hybrid_hits:
        print("  (no hits)")
    for hit in hybrid_hits:
        title = hit.fields.get("title", "")
        print(f"  [{hit.score:.3f}] {hit.id}  title={title!r}")

    return {
        "text_hits": text_hits,
        "facet_rows": facet_rows,
        "hybrid_hits": hybrid_hits,
    }


# -- Dry-run mock ------------------------------------------------------------


def _build_dry_run_client() -> MagicMock:
    """Build a :class:`MagicMock` pre-configured with plausible responses.

    Return values are fixed-size (bounded per threat_model T-153-03-03)
    and use real :class:`TextSearchHit` / plain-dict shapes so the demo
    printers exercise the same code paths they do against a live server.
    """
    client = MagicMock(name="DryRunClient")
    client.text.text_search.return_value = [
        TextSearchHit(
            id="issue:002",
            score=4.218,
            fields={"title": SAMPLE_TITLES[2]},
        ),
        TextSearchHit(
            id="issue:014",
            score=3.477,
            fields={"title": SAMPLE_TITLES[14]},
        ),
        TextSearchHit(
            id="issue:004",
            score=2.910,
            fields={"title": SAMPLE_TITLES[4]},
        ),
    ]
    client.text.aggregate.return_value = [
        {"priority": "P0", "n": 7},
        {"priority": "P1", "n": 7},
        {"priority": "P2", "n": 6},
    ]
    client.text.hybrid_search.return_value = [
        TextSearchHit(
            id="issue:003",
            score=0.832,
            fields={"title": SAMPLE_TITLES[3]},
        ),
        TextSearchHit(
            id="issue:001",
            score=0.817,
            fields={"title": SAMPLE_TITLES[1]},
        ),
    ]
    # Drop-index: FT.DROPINDEX returns "OK"
    client.execute_command.return_value = "OK"
    # Pipeline: return a mock pipeline with fluent .hset + .execute
    pipe = MagicMock(name="DryRunPipeline")
    client.pipeline.return_value = pipe
    # BLOCKER 1/2/3 fix (Plan 153-06): pre-populate the TAG-support probe
    # cache so `_probe_tag_support(client)` short-circuits to True without
    # firing any server calls. Existing TestRunDemo assertions on
    # `create_text_index.assert_called_once()` and
    # `execute_command.call_args_list[0]` are preserved because the probe
    # branch is never entered during dry-run tests.
    client.__dict__["_probe_tag_cached"] = True
    return client


# -- CLI entry point ---------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse parser (isolated for testability)."""
    parser = argparse.ArgumentParser(
        prog="pilotspace_example",
        description="PilotSpace-shaped moondb demo (text + aggregate + hybrid)",
    )
    parser.add_argument("--host", default="localhost", help="Moon server hostname")
    parser.add_argument("--port", type=int, default=6399, help="Moon server port")
    parser.add_argument(
        "--index", default=DEFAULT_INDEX, help="FT index name (default: issues)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Deterministic RNG seed (default: {DEFAULT_SEED:#x})",
    )
    parser.add_argument(
        "--num-issues",
        type=int,
        default=DEFAULT_NUM_ISSUES,
        help=f"Number of sample issues (default: {DEFAULT_NUM_ISSUES})",
    )
    parser.add_argument(
        "--dim",
        type=int,
        default=DEFAULT_DIM,
        help=f"Vector dimensionality (default: {DEFAULT_DIM})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Use a mocked client (no server needed)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Returns:
        0 on success, 2 on connection failure (cannot reach Moon).
    """
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    client: Any
    if args.dry_run:
        print("[dry-run] Using mocked MoonClient; no network.")
        client = _build_dry_run_client()
    else:
        try:
            client = MoonClient(
                host=args.host, port=args.port, decode_responses=False
            )
            client.ping()
        except Exception as exc:  # noqa: BLE001 -- CLI error surface
            print(
                f"ERROR: cannot reach Moon at {args.host}:{args.port} -- {exc}",
                file=sys.stderr,
            )
            return 2

    run_demo(
        client,
        index=args.index,
        num_issues=args.num_issues,
        dim=args.dim,
        seed=args.seed,
    )
    print("\nDone.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
