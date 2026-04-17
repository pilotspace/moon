"""Shared pytest fixtures for moondb SDK tests.

Plan 153-06 (Gap 1 closure): the `live_moon_client` fixture builds an
end-to-end moon-backed client with hygienic setup + teardown cleanup
of well-known test indexes and key patterns. Tests that require a
live server skip cleanly when moon is not reachable.
"""

from __future__ import annotations

import contextlib
import socket
from collections.abc import Iterator
from typing import Any

import pytest

_MOON_HOST = "127.0.0.1"
_MOON_PORT = 6399

# Indexes consumed by the integration + probe test suites. Cleaned up
# on setup AND teardown to avoid polluting the live server and to
# guarantee each test starts from a clean state.
_TEST_INDEXES: tuple[str, ...] = (
    "issues",
    "_test_lc_hybrid",
    "_test_li_hybrid",
    "__moon_probe_tag__",
    "_alpha_probe",
)

# Key-space patterns matching the prefixes used by test fixtures.
_TEST_KEY_PATTERNS: tuple[str, ...] = (
    "issue:*",
    "_doc_lc:*",
    "_node_li:*",
    "_probe:*",
)


def _moon_reachable(
    host: str = _MOON_HOST,
    port: int = _MOON_PORT,
    timeout: float = 0.5,
) -> bool:
    """Cheap TCP probe — moon is reachable iff the port accepts a connection."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def moon_available() -> bool:
    """Session-scoped skip gate — yields True when moon answers on 127.0.0.1:6399."""
    if not _moon_reachable():
        pytest.skip(
            f"moon not reachable on {_MOON_HOST}:{_MOON_PORT} -- "
            "start a local moon or deselect with `-m 'not integration'`."
        )
    return True


@pytest.fixture
def live_moon_client(moon_available: bool) -> Iterator[Any]:
    """Per-test MoonClient connected to the local server.

    Responsibilities:
    - Ping the server to prove the connection works.
    - Drop all test indexes and delete matching keys on setup (clean slate).
    - Yield the client.
    - Drop indexes + delete keys again on teardown (no cross-test leaks).
    """
    from moondb import MoonClient

    del moon_available  # consumed for its skip side-effect
    client = MoonClient(host=_MOON_HOST, port=_MOON_PORT, decode_responses=False)
    client.ping()
    _cleanup(client)
    try:
        yield client
    finally:
        _cleanup(client)
        client.close()


def _cleanup(client: Any) -> None:  # noqa: ANN401 -- MoonClient has dynamic attrs
    """Best-effort index + key cleanup. Errors are swallowed."""
    for idx in _TEST_INDEXES:
        with contextlib.suppress(Exception):
            client.execute_command("FT.DROPINDEX", idx)
    for pattern in _TEST_KEY_PATTERNS:
        with contextlib.suppress(Exception):
            for key in client.scan_iter(match=pattern, count=100):
                client.delete(key)
