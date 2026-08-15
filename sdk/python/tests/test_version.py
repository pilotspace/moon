"""ADD task `sdk-wire-form-fixes` — GUARD 3: the version a caller reads is the
version that was published.

`moondb.__version__` was a hand-maintained string literal, and it drifted: the
package shipped to PyPI as 0.1.1 while `__version__` still answered "0.1.0".
Anything that keys on it — a bug report, a server-side compatibility check, a
user pinning a workaround to "SDK >= x" — was reading a number that had not
been true since the previous release. The test that covered it asserted the
same stale literal, so the suite stayed green through two releases.

Asserting the two are equal would only catch the drift AFTER it happened, and
would then be "fixed" by hand-editing the same literal that caused it. So the
fix is structural: `__version__` is derived from the installed distribution
metadata, which is `pyproject.toml`'s `version` by construction. These tests
guard the derivation, not a snapshot of the number.

Written as `unittest.TestCase` deliberately, so it needs NOTHING beyond the
standard library: the CI runner (Ubuntu 24.04 / Python 3.14) has no pytest, and
PEP 668 blocks `pip install --user` while `python3-venv` is not installed —
verified on the runner, not assumed. `unittest` collects this, and so does
pytest, so the same file serves CI and local development.
"""

from __future__ import annotations

import pathlib
import re
import sys
import unittest

import moondb

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10 and older
    import tomli as tomllib


def _pyproject_version() -> str:
    """The version as declared in the packaging source of truth."""
    root = pathlib.Path(__file__).resolve().parent.parent
    with (root / "pyproject.toml").open("rb") as fh:
        return str(tomllib.load(fh)["project"]["version"])


class VersionDerivationTest(unittest.TestCase):
    """`__version__` must equal what ships, by construction rather than by memory."""

    def test_swf5_version_matches_pyproject(self) -> None:
        published = _pyproject_version()
        self.assertEqual(
            moondb.__version__,
            published,
            f"moondb.__version__ is {moondb.__version__!r} but the package "
            f"publishes as {published!r} — a caller reading __version__ is "
            f"being told the wrong release.",
        )

    def test_swf5b_version_is_not_a_hardcoded_literal(self) -> None:
        """The equality above must hold by construction, not by remembering.

        A literal that happens to match today is exactly the state this package
        was already in once, and it silently stopped being true.
        """
        src = (
            pathlib.Path(moondb.__file__).read_text(encoding="utf-8")
            if moondb.__file__
            else ""
        )
        self.assertIsNone(
            re.search(r'^__version__\s*(:\s*str\s*)?=\s*["\']', src, re.MULTILINE),
            "__version__ is assigned a string literal in moondb/__init__.py. "
            "Derive it from the installed distribution metadata "
            "(importlib.metadata.version) so it cannot drift from pyproject.toml.",
        )

    def test_swf5c_version_is_a_usable_release_string(self) -> None:
        """Whatever the derivation returns must still look like a version.

        Guards the fallback path: imported from a source tree that was never
        installed, `importlib.metadata` raises, and a fallback returning `""`
        or `"unknown"` would satisfy both tests above while handing callers
        something useless.
        """
        self.assertRegex(
            moondb.__version__,
            r"^\d+\.\d+\.\d+([.-]?\w+)*$",
            f"moondb.__version__ is {moondb.__version__!r}, which is not a "
            f"release string a caller can compare or report.",
        )


if __name__ == "__main__":
    unittest.main()
