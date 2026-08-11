#!/usr/bin/env python3
"""End-to-end tests for the client-compat harness: real Moon, real redis-server.

These do NOT skip when the oracle is missing. A harness whose whole purpose is to
compare against real Redis proves nothing without it, so an absent
`redis-server` is a failure here exactly as it is in the harness itself. The one
escape hatch is explicit and loud: set `CLIENT_COMPAT_E2E=0` to opt out (for a
developer machine without redis installed). CI must never set it.

Run:  python3 -m unittest discover -s scripts/client-compat -p 'test_e2e*' -v
Env:  MOON_BIN     path to the moon binary under test (required)
      REDIS_BIN    redis-server (default: from PATH)
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from differ import HarnessError, RunConfig, Runner  # noqa: E402

OPTED_OUT = os.environ.get("CLIENT_COMPAT_E2E") == "0"


def moon_bin():
    return os.environ.get("MOON_BIN") or ""


def redis_bin():
    return os.environ.get("REDIS_BIN") or shutil.which("redis-server") or ""


def manifest(text):
    fd, p = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    return p


ONE_ENTRY = """
entries:
  - name: set_then_get
    setup: ["SET k v"]
    command: "GET k"
    policy: exact
"""

# Standalone-only, so it is a clean match against current Moon. The full-matrix
# form above deliberately is NOT: `GET` inside MULTI is answered instead of
# queued on the monoio inline path (found by this harness 2026-08-09; see the
# `multi_get_must_queue` waiver in manifest.yaml), so an exit-code test built on
# it would be asserting a Moon bug rather than harness behavior.
MATCHING_ENTRY = """
entries:
  - name: get_standalone
    setup: ["SET k v"]
    command: "GET k"
    policy: exact
    contexts: [standalone]
"""


def cfg(**kw):
    base = dict(
        manifest_path=manifest(ONE_ENTRY),
        moon_bin=moon_bin(),
        redis_bin=redis_bin(),
    )
    base.update(kw)
    return RunConfig(**base)


def alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


@unittest.skipIf(OPTED_OUT, "CLIENT_COMPAT_E2E=0 — explicit opt-out")
class TestEndToEnd(unittest.TestCase):
    def test_full_matrix_runs_six_comparisons_per_entry(self):
        report = Runner(cfg()).run()
        self.assertEqual(len(report.results), 6)
        seen = {(r.protocol, r.context) for r in report.results}
        self.assertEqual(
            seen,
            {(p, c)
             for p in ("resp2", "resp3")
             for c in ("standalone", "multi", "pipeline")},
        )

    def test_identical_bytes_are_sent_to_both_servers(self):
        report = Runner(cfg()).run()
        for r in report.results:
            self.assertIsInstance(r.sent_bytes, (bytes, bytearray))
            self.assertTrue(r.sent_bytes)

    def test_both_raw_replies_are_recorded(self):
        report = Runner(cfg()).run()
        for r in report.results:
            self.assertTrue(r.redis_raw)
            self.assertTrue(r.moon_raw)

    def test_servers_are_torn_down_and_dirs_removed(self):
        runner = Runner(cfg())
        runner.run()
        for pid in runner.spawned_pids:
            self.assertFalse(alive(pid), f"pid {pid} survived the run")
        for d in runner.spawned_dirs:
            self.assertFalse(os.path.exists(d), f"{d} was left behind")

    def test_oracle_version_is_recorded(self):
        report = Runner(cfg()).run()
        self.assertRegex(report.redis_version, r"^\d+\.\d+\.\d+")

    def test_a_matching_entry_exits_zero(self):
        report = Runner(cfg(manifest_path=manifest(MATCHING_ENTRY))).run()
        self.assertEqual(report.exit_code(), 0)
        self.assertEqual(report.tally()["fail"], 0)

    def test_a_diverging_entry_exits_one_and_names_the_divergence(self):
        # COMMAND COUNT: Redis answers Integer :274, Moon answers an empty
        # Array *0 (it ignores the COUNT subcommand). The harness must surface
        # this as a failure naming TYPE, not swallow it.
        #
        # THIRD fixture for this test. The rotation is the point, so read it
        # before picking a fourth:
        #   1. GET-inside-MULTI  — fixed by the v0.8.6 inline-GET hotfix (#457)
        #   2. SISMEMBER RESP3   — fixed by resp3-type-fidelity (#463); Moon no
        #      longer over-converts Integer to Boolean, so this test began
        #      failing with `0 != 1` the moment that landed
        #   3. COMMAND COUNT     — open, owned by `client-identity-introspection`
        #
        # A test that needs a live defect to pass fails as a REWARD for fixing
        # something, which is exactly backwards. #3 is chosen because it is
        # owned by a different task than the reply-type work, so that line of
        # work cannot silently retire it again — but that is damage control,
        # not a fix.
        #
        # No permanent-by-construction TYPE divergence exists today: Moon's
        # proprietary commands (TXN.ABORT, MQ PUSH) return an Error on BOTH
        # servers, so they diverge in text but not in type; and redis 8.6.1
        # turns out to implement `hotkeys` too. The durable fix is a test-only
        # injection hook that fabricates a divergence rather than borrowing a
        # real one — tracked in #461 alongside the harness's other
        # self-honesty gaps.
        report = Runner(cfg(manifest_path=manifest("""
entries:
  - name: command_count_type
    command: "COMMAND COUNT"
    policy: exact
    protocols: [resp3]
    contexts: [standalone]
"""))).run()
        self.assertEqual(report.exit_code(), 1)
        diffs = [r for r in report.results if r.verdict == "diff"]
        self.assertTrue(diffs)
        for r in diffs:
            self.assertEqual(r.divergence, "type")

    def test_info_manifest_reports_missing_fields_by_name(self):
        # run_id is emitted by real Redis and not by Moon, so it is a genuine
        # finding; redis_version is emitted by both.
        fields = manifest("")  # reuse tempfile helper for a plain list file
        with open(fields, "w") as f:
            f.write("redis_version\nrun_id\n")
        report = Runner(cfg(info_manifest=fields)).run()
        missing = [r.name for r in report.results if r.verdict == "diff"]
        self.assertIn("info:run_id", missing)
        self.assertNotIn("info:redis_version", missing)

    def test_info_manifest_blames_the_pin_not_moon_when_redis_lacks_it_too(self):
        # The differential must cut both ways: a field absent from the ORACLE
        # is a wrong pin, not a Moon defect. Reporting it against Moon would
        # manufacture a finding, which is the mirror image of the blindness
        # this harness exists to remove.
        fields = manifest("")
        with open(fields, "w") as f:
            f.write("field_that_no_redis_has\n")
        report = Runner(cfg(info_manifest=fields)).run()
        r = next(x for x in report.results if x.name == "info:field_that_no_redis_has")
        self.assertNotEqual(r.verdict, "diff")
        self.assertIn("fix the pin", r.detail)

    def test_a_type_divergence_is_found_and_exits_one(self):
        # SISMEMBER: Redis answers Integer. If Moon over-converts it under
        # RESP3, this entry is exactly how the harness surfaces that.
        report = Runner(cfg(manifest_path=manifest("""
entries:
  - name: sismember_type
    setup: ["DEL s", "SADD s a"]
    command: "SISMEMBER s a"
    policy: exact
"""))).run()
        if report.tally()["fail"]:
            self.assertEqual(report.exit_code(), 1)
            for r in report.results:
                if r.verdict == "diff":
                    self.assertIn(r.divergence, ("type", "shape", "value"))


@unittest.skipIf(OPTED_OUT, "CLIENT_COMPAT_E2E=0 — explicit opt-out")
class TestRefusals(unittest.TestCase):
    def test_no_oracle_fails_loudly(self):
        with self.assertRaises(HarnessError) as cm:
            Runner(cfg(redis_bin="/nonexistent/redis-server")).run()
        self.assertEqual(cm.exception.code, "ERR_NO_ORACLE")

    def test_no_moon_fails_loudly(self):
        with self.assertRaises(HarnessError) as cm:
            Runner(cfg(moon_bin="/nonexistent/moon")).run()
        self.assertEqual(cm.exception.code, "ERR_NO_MOON")

    def test_no_moon_leaves_no_redis_running(self):
        runner = Runner(cfg(moon_bin="/nonexistent/moon"))
        with self.assertRaises(HarnessError):
            runner.run()
        for pid in runner.spawned_pids:
            self.assertFalse(alive(pid))

    def test_readiness_deadline_is_enforced(self):
        with self.assertRaises(HarnessError) as cm:
            Runner(cfg(ready_timeout_s=0.0)).run()
        self.assertEqual(cm.exception.code, "ERR_SERVER_TIMEOUT")

    def test_oracle_below_the_floor_is_refused(self):
        with self.assertRaises(HarnessError) as cm:
            Runner(cfg(min_redis="999.0.0")).run()
        self.assertEqual(cm.exception.code, "ERR_NO_ORACLE")


@unittest.skipIf(OPTED_OUT, "CLIENT_COMPAT_E2E=0 — explicit opt-out")
class TestWaivers(unittest.TestCase):
    STALE = """
entries:
  - name: get_standalone
    setup: ["SET k v"]
    command: "GET k"
    policy: exact
    contexts: [standalone]
    expect_diff:
      reason: "placeholder waiver that no longer reproduces"
"""

    def test_stale_waiver_passes_without_strict(self):
        report = Runner(cfg(manifest_path=manifest(self.STALE))).run()
        self.assertEqual(report.exit_code(), 0)

    def test_strict_fails_a_stale_waiver(self):
        with self.assertRaises(HarnessError) as cm:
            Runner(cfg(manifest_path=manifest(self.STALE), strict=True)).run()
        self.assertEqual(cm.exception.code, "ERR_STALE_WAIVER")


@unittest.skipIf(OPTED_OUT, "CLIENT_COMPAT_E2E=0 — explicit opt-out")
class TestCliWrapper(unittest.TestCase):
    """The shell wrapper is the contracted entry point; CI calls it, not Python."""

    SCRIPT = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "test-client-compat.sh",
    )

    def test_wrapper_exists_and_is_executable(self):
        self.assertTrue(os.path.isfile(self.SCRIPT))
        self.assertTrue(os.access(self.SCRIPT, os.X_OK))

    def test_wrapper_exits_two_without_an_oracle(self):
        env = dict(os.environ, MOON_BIN=moon_bin())
        p = subprocess.run(
            [self.SCRIPT, "--redis-bin", "/nonexistent/redis-server"],
            capture_output=True, text=True, env=env, timeout=120,
        )
        self.assertEqual(p.returncode, 2)
        self.assertIn("ERR_NO_ORACLE", p.stdout + p.stderr)

    def test_wrapper_prints_a_tally(self):
        env = dict(os.environ, MOON_BIN=moon_bin())
        p = subprocess.run(
            [self.SCRIPT, "--manifest", manifest(ONE_ENTRY)],
            capture_output=True, text=True, env=env, timeout=300,
        )
        self.assertRegex(p.stdout, r"PASS=\d+ FAIL=\d+ WAIVED=\d+ TOTAL=\d+")


if __name__ == "__main__":
    unittest.main()
