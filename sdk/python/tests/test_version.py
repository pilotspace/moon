"""ADD task `sdk-wire-form-fixes` — GUARD 3: the version a caller reads is the
version that was published.

`moondb.__version__` was a hand-maintained string literal, and it drifted: the
package shipped to PyPI as 0.1.1 while `__version__` still answered "0.1.0".
Anything that keys on it — a bug report, a server-side compatibility check, a
user pinning a workaround to "SDK >= x" — was reading a number that had not
been true since the previous release.

Asserting the two are equal would only catch the drift AFTER it happened, and
would then have to be fixed by hand in the same edit that caused it. So the
fix is structural: `__version__` is derived from the installed distribution
metadata, which is `pyproject.toml`'s `version` by construction. This test
guards the derivation, not a snapshot of the number.
"""

from __future__ import annotations

import pathlib
import re
import sys

import pytest

import moondb

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10 and older
    tomllib = pytest.importorskip("tomli")


def _pyproject_version() -> str:
    """The version as declared in the packaging source of truth."""
    root = pathlib.Path(__file__).resolve().parent.parent
    with (root / "pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)["project"]["version"]


def swf5_version_matches_pyproject() -> None:
    """`moondb.__version__` must equal the version that gets published."""
    assert moondb.__version__ == _pyproject_version(), (
        f"moondb.__version__ is {moondb.__version__!r} but the package "
        f"publishes as {_pyproject_version()!r} — a caller reading "
        f"__version__ is being told the wrong release."
    )


# pytest collects by name; the swf-prefixed name above is the one referenced in
# the task record, so bind it to a collected test.
test_swf5_version_matches_pyproject = swf5_version_matches_pyproject


def test_swf5b_version_is_not_a_hardcoded_literal() -> None:
    """The equality above must hold by construction, not by remembering.

    A literal that happens to match today is exactly the state this package was
    already in once, and it silently stopped being true. Assert the module
    derives the value instead of restating it.
    """
    src = (
        pathlib.Path(moondb.__file__).read_text(encoding="utf-8")
        if moondb.__file__
        else ""
    )
    literal = re.search(r'^__version__\s*=\s*["\']', src, re.MULTILINE)
    assert literal is None, (
        "__version__ is assigned a string literal in moondb/__init__.py. "
        "Derive it from the installed distribution metadata "
        "(importlib.metadata.version) so it cannot drift from pyproject.toml."
    )


def test_swf5c_version_is_a_usable_release_string() -> None:
    """Whatever the derivation returns must still look like a version.

    Guards the fallback path: if the package is imported from a source tree
    that was never installed, `importlib.metadata` raises, and a fallback that
    returned "" or "unknown" would satisfy the two tests above while still
    handing callers something useless.
    """
    assert re.fullmatch(r"\d+\.\d+\.\d+([.-]?\w+)*", moondb.__version__), (
        f"moondb.__version__ is {moondb.__version__!r}, which is not a "
        f"release string a caller can compare or report."
    )
