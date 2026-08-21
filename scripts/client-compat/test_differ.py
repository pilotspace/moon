#!/usr/bin/env python3
"""Unit tests for the client-compat differ.

stdlib `unittest`, deliberately — `pytest` is absent from the moon-dev VM that
runs the self-hosted `client-compat` CI job, and a PR-gating job must not depend
on a package hand-installed into a runner that gets rebuilt.

No servers are spawned here; every test is pure. The end-to-end tests that need a
real `redis-server` live in `test_e2e.py`.

Run:  python3 -m unittest discover -s scripts/client-compat -v
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from differ import (  # noqa: E402
    HarnessError,
    binary_provenance,
    compare,
    default_moon_bin,
    encode_command,
    enforce_binary_freshness,
    load_manifest,
    parse_resp,
)


def w(path, text):
    """Write a manifest to a temp file and return its path."""
    fd, p = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    return p


# ===========================================================================
# RESP codec — the type byte must survive parsing, or nothing else can work
# ===========================================================================


class TestParse(unittest.TestCase):
    def test_integer_and_bulk_are_distinct_kinds(self):
        self.assertEqual(parse_resp(b":1\r\n").kind, "integer")
        self.assertEqual(parse_resp(b"$1\r\n1\r\n").kind, "bulk")

    def test_resp3_scalar_kinds(self):
        self.assertEqual(parse_resp(b",1.5\r\n").kind, "double")
        self.assertEqual(parse_resp(b"#t\r\n").kind, "boolean")
        self.assertEqual(parse_resp(b"_\r\n").kind, "null")

    def test_resp3_aggregates_keep_their_own_kinds(self):
        self.assertEqual(parse_resp(b"%1\r\n$1\r\na\r\n:1\r\n").kind, "map")
        self.assertEqual(parse_resp(b"~1\r\n$1\r\na\r\n").kind, "set")
        self.assertEqual(parse_resp(b">1\r\n$1\r\na\r\n").kind, "push")
        self.assertEqual(parse_resp(b"*1\r\n$1\r\na\r\n").kind, "array")

    def test_nested_array_children(self):
        node = parse_resp(b"*2\r\n*1\r\n:1\r\n:2\r\n")
        self.assertEqual(node.kind, "array")
        self.assertEqual(len(node.children), 2)
        self.assertEqual(node.children[0].kind, "array")

    def test_unparseable_reply_raises_protocol_parse(self):
        with self.assertRaises(HarnessError) as cm:
            parse_resp(b"garbage without a type byte\r\n")
        self.assertEqual(cm.exception.code, "ERR_PROTOCOL_PARSE")

    def test_truncated_reply_raises_protocol_parse(self):
        with self.assertRaises(HarnessError) as cm:
            parse_resp(b"*2\r\n:1\r\n")
        self.assertEqual(cm.exception.code, "ERR_PROTOCOL_PARSE")


class TestEncode(unittest.TestCase):
    def test_command_encodes_as_resp_array_of_bulks(self):
        self.assertEqual(
            encode_command(["GET", "k"]),
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
        )


# ===========================================================================
# Comparator — TYPE -> SHAPE -> VALUE, and it must name which one diverged
# ===========================================================================


class TestCompare(unittest.TestCase):
    def test_identical_replies_pass(self):
        v = compare(parse_resp(b":1\r\n"), parse_resp(b":1\r\n"), "exact")
        self.assertTrue(v.ok)
        self.assertIsNone(v.divergence)

    def test_type_divergence_reports_type(self):
        # The exact defect class the deep review found: Redis answers Integer,
        # Moon answers Bulk. Rendered as text both look like "1".
        v = compare(parse_resp(b":1\r\n"), parse_resp(b"$1\r\n1\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "type")

    def test_resp3_map_is_not_equal_to_flat_array(self):
        # ZRANDMEMBER/HRANDFIELD class. A parser that flattens a Map into pairs
        # passes this wrongly — which is why it is asserted explicitly.
        m = parse_resp(b"%1\r\n$1\r\na\r\n$1\r\nb\r\n")
        a = parse_resp(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n")
        v = compare(m, a, "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "type")

    def test_set_is_not_equal_to_array(self):
        v = compare(parse_resp(b"~1\r\n$1\r\na\r\n"),
                    parse_resp(b"*1\r\n$1\r\na\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "type")

    def test_double_is_not_equal_to_bulk(self):
        v = compare(parse_resp(b",1.5\r\n"),
                    parse_resp(b"$3\r\n1.5\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "type")

    def test_shape_divergence_reports_shape_not_type(self):
        v = compare(parse_resp(b"*2\r\n:1\r\n:2\r\n"),
                    parse_resp(b"*3\r\n:1\r\n:2\r\n:3\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "shape")

    def test_nesting_difference_is_a_shape_divergence(self):
        v = compare(parse_resp(b"*1\r\n*1\r\n:1\r\n"),
                    parse_resp(b"*1\r\n:1\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "shape")

    def test_value_divergence_reports_value(self):
        v = compare(parse_resp(b":1\r\n"), parse_resp(b":2\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "value")


class TestPolicies(unittest.TestCase):
    A = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    B = b"*2\r\n$1\r\nb\r\n$1\r\na\r\n"

    def test_sorted_policy_accepts_reordering(self):
        v = compare(parse_resp(self.A), parse_resp(self.B), "sorted")
        self.assertTrue(v.ok)

    def test_exact_policy_rejects_the_same_reordering(self):
        # The pair is the point: `sorted` must be a declared relaxation, not the
        # default. If both pass, the harness is fuzzy-matching.
        v = compare(parse_resp(self.A), parse_resp(self.B), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "value")

    def test_numeric_tolerance_within_bound_passes(self):
        v = compare(parse_resp(b":100\r\n"), parse_resp(b":103\r\n"),
                    "numeric_tolerance", tolerance=5)
        self.assertTrue(v.ok)

    def test_numeric_tolerance_outside_bound_fails(self):
        v = compare(parse_resp(b":100\r\n"), parse_resp(b":200\r\n"),
                    "numeric_tolerance", tolerance=5)
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "value")

    def test_type_only_policy_ignores_value(self):
        v = compare(parse_resp(b"$1\r\na\r\n"), parse_resp(b"$1\r\nz\r\n"),
                    "type_only")
        self.assertTrue(v.ok)

    def test_type_only_policy_still_catches_a_type_change(self):
        v = compare(parse_resp(b"$1\r\n1\r\n"), parse_resp(b":1\r\n"),
                    "type_only")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "type")

    def test_ignore_value_still_catches_shape(self):
        v = compare(parse_resp(b"*2\r\n:1\r\n:2\r\n"),
                    parse_resp(b"*1\r\n:1\r\n"), "ignore_value")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "shape")


class TestErrorComparison(unittest.TestCase):
    def test_same_code_different_message_passes(self):
        v = compare(
            parse_resp(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
            parse_resp(b"-WRONGTYPE wrong kind\r\n"),
            "exact",
        )
        self.assertTrue(v.ok)

    def test_different_code_fails_on_value(self):
        v = compare(parse_resp(b"-WRONGTYPE x\r\n"),
                    parse_resp(b"-ERR x\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "value")

    def test_error_versus_non_error_is_a_type_divergence(self):
        v = compare(parse_resp(b"-ERR x\r\n"),
                    parse_resp(b"+OK\r\n"), "exact")
        self.assertFalse(v.ok)
        self.assertEqual(v.divergence, "type")


# ===========================================================================
# Manifest — every reject fires at LOAD time, before a server is spawned
# ===========================================================================

GOOD = """
entries:
  - name: get_missing
    setup: ["DEL k"]
    command: "GET k"
    policy: exact
"""


class TestManifest(unittest.TestCase):
    def test_valid_manifest_loads(self):
        entries = load_manifest(w(None, GOOD))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].name, "get_missing")
        self.assertEqual(entries[0].policy, "exact")

    def test_entry_defaults_to_the_full_matrix(self):
        e = load_manifest(w(None, GOOD))[0]
        self.assertEqual(sorted(e.protocols), ["resp2", "resp3"])
        self.assertEqual(sorted(e.contexts), ["multi", "pipeline", "standalone"])

    def test_missing_required_field_rejected(self):
        with self.assertRaises(HarnessError) as cm:
            load_manifest(w(None, "entries:\n  - name: x\n    policy: exact\n"))
        self.assertEqual(cm.exception.code, "ERR_BAD_MANIFEST")

    def test_unknown_policy_rejected(self):
        with self.assertRaises(HarnessError) as cm:
            load_manifest(w(None, """
entries:
  - name: x
    command: "GET k"
    policy: approximately
"""))
        self.assertEqual(cm.exception.code, "ERR_BAD_MANIFEST")

    def test_numeric_tolerance_without_tolerance_rejected(self):
        with self.assertRaises(HarnessError) as cm:
            load_manifest(w(None, """
entries:
  - name: x
    command: "TTL k"
    policy: numeric_tolerance
"""))
        self.assertEqual(cm.exception.code, "ERR_BAD_MANIFEST")

    def test_waiver_without_reason_rejected(self):
        with self.assertRaises(HarnessError) as cm:
            load_manifest(w(None, """
entries:
  - name: x
    command: "GET k"
    policy: exact
    expect_diff: {}
"""))
        self.assertEqual(cm.exception.code, "ERR_UNREASONED_WAIVER")

    def test_waiver_with_reason_is_carried_verbatim(self):
        e = load_manifest(w(None, """
entries:
  - name: x
    command: "GET k"
    policy: exact
    expect_diff:
      reason: "MOONERR namespace is intentional"
"""))[0]
        self.assertEqual(e.expect_diff, "MOONERR namespace is intentional")

    def test_unknown_context_rejected(self):
        with self.assertRaises(HarnessError) as cm:
            load_manifest(w(None, """
entries:
  - name: x
    command: "GET k"
    policy: exact
    contexts: [standalone, telepathy]
"""))
        self.assertEqual(cm.exception.code, "ERR_BAD_MANIFEST")


if __name__ == "__main__":
    unittest.main()


class TestInjectionIsTestOnly(unittest.TestCase):
    def test_shipped_manifest_injects_nothing(self):
        """The shipped manifest must never fabricate a Moon reply.

        `inject_moon_reply` exists so the harness's OWN tests can build a
        divergence without depending on a live Moon defect. An injected reply
        in the shipped manifest would be a comparison against a fixture rather
        than against Moon — a green that proves nothing.
        """
        here = os.path.dirname(os.path.abspath(__file__))
        entries = load_manifest(os.path.join(here, "manifest.yaml"))
        offenders = [e.name for e in entries if e.inject_moon_reply is not None]
        self.assertEqual(offenders, [], f"shipped manifest fabricates replies: {offenders}")


class TestBinaryProvenance(unittest.TestCase):
    """moon#461: a harness that tests the wrong binary is worse than one that
    refuses to run — it produces a confident, false compatibility picture.

    Measured 2026-08-10 on a tree where the fix was already green: a two-day-old
    `target/release/moon` reported PASS=94 FAIL=32, including a divergence from
    a function deleted hours earlier. Same tree, MOON_BIN pinned: PASS=128 FAIL=0.
    """

    def _tree(self, bin_age_offset, src_age_offset):
        """Build a fake checkout: <root>/src/lib.rs and <root>/target/release/moon.

        Ages are seconds relative to a fixed base, so the test never races the
        clock the way a `touch`-then-compare would.
        """
        root = tempfile.mkdtemp(prefix="prov-")
        src = os.path.join(root, "src")
        os.makedirs(src)
        srcfile = os.path.join(src, "lib.rs")
        with open(srcfile, "w") as f:
            f.write("fn main() {}\n")
        bindir = os.path.join(root, "target", "release")
        os.makedirs(bindir)
        binpath = os.path.join(bindir, "moon")
        with open(binpath, "w") as f:
            f.write("#!/bin/sh\nexit 0\n")
        os.chmod(binpath, 0o755)
        base = 1_700_000_000
        os.utime(binpath, (base + bin_age_offset, base + bin_age_offset))
        os.utime(srcfile, (base + src_age_offset, base + src_age_offset))
        return root, binpath

    def test_binary_older_than_sources_is_reported_stale(self):
        root, binpath = self._tree(bin_age_offset=0, src_age_offset=3600)
        prov = binary_provenance(binpath, root)
        self.assertTrue(prov.stale,
                        "a binary older than the newest src/ file must be stale")
        self.assertGreater(prov.newest_source_mtime, prov.mtime)

    def test_binary_newer_than_sources_is_not_stale(self):
        root, binpath = self._tree(bin_age_offset=3600, src_age_offset=0)
        prov = binary_provenance(binpath, root)
        self.assertFalse(prov.stale,
                         "a binary built after the last source edit is current")

    def test_strict_refuses_a_stale_binary_with_the_contracted_code(self):
        root, binpath = self._tree(bin_age_offset=0, src_age_offset=3600)
        prov = binary_provenance(binpath, root)
        with self.assertRaises(HarnessError) as cm:
            enforce_binary_freshness(prov, strict=True)
        self.assertEqual(cm.exception.code, "ERR_STALE_BINARY")

    def test_non_strict_warns_but_does_not_refuse(self):
        """Warn, do not block: the default run must stay usable on a tree whose
        binary is intentionally older (a bisect, a pinned MOON_BIN)."""
        root, binpath = self._tree(bin_age_offset=0, src_age_offset=3600)
        prov = binary_provenance(binpath, root)
        enforce_binary_freshness(prov, strict=False)  # must not raise

    def test_provenance_renders_the_path_and_a_timestamp(self):
        """The header line is the whole point — it makes the mistake
        self-diagnosing instead of requiring someone to suspect it."""
        root, binpath = self._tree(bin_age_offset=3600, src_age_offset=0)
        line = binary_provenance(binpath, root).describe()
        self.assertIn(binpath, line)
        self.assertRegex(line, r"\d{4}-\d{2}-\d{2}")

    def test_default_bin_prefers_the_newer_of_the_two_build_layouts(self):
        """`target/release/moon` is explicitly quarantined in this repo while
        everyday work builds `release-fast`. First-found order therefore picks
        the stale one by construction (moon#461 item 3)."""
        root = tempfile.mkdtemp(prefix="prov2-")
        base = 1_700_000_000
        made = {}
        for rel, age in (("target/release/moon", 0),
                         ("target-fast/release-fast/moon", 3600)):
            p = os.path.join(root, rel)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "w") as f:
                f.write("#!/bin/sh\nexit 0\n")
            os.chmod(p, 0o755)
            os.utime(p, (base + age, base + age))
            made[rel] = p
        self.assertEqual(default_moon_bin(root), made["target-fast/release-fast/moon"])
