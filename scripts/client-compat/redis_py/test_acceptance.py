"""EC2 — acceptance suite driven by UNMODIFIED redis-py against a live Moon.

The raw-RESP differ (`scripts/client-compat/differ.py`) compares byte-for-byte
replies against a real redis-server. It is precise and it is blind to one whole
class of defect: everything a client library does *around* the reply — the
handshake it opens with, the connection it reuses, the Python type it decodes
into, the second command it issues on your behalf. A server can answer every
byte correctly and still be unusable from redis-py.

So this suite deliberately does NOT hand-roll sockets. It uses redis-py's own
idioms — connection pools, `pipeline()`, `pubsub()`, `scan_iter()`, `Lock`,
`from_url` — because the thing under test is whether the library works, not
whether the wire format parses.

Stdlib `unittest`, no pytest: the CI runner (Ubuntu 24.04 / Python 3.14) has no
pytest, PEP 668 blocks `pip install --user`, and `python3.14-venv` is not
installed. redis-py itself is present as the distro package `python3-redis`
(6.4.0). Verified on the runner, not assumed.

The server binary comes from `MOON_BIN`. There is no fallback to
`target/release/moon` — a stale binary of unknown provenance is how a green run
stops meaning anything.
"""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import unittest

import redis


def _free_port() -> int:
    """A port that is free right now, held until the caller binds it.

    The listener stays open until this returns, which narrows but does not
    close the reserve-then-release window. Moon binds with SO_REUSEPORT, so a
    collision would not error — it would silently share the port. The suite
    asserts on a per-instance run_id below to catch exactly that.
    """
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class MoonServer:
    """One moon process, torn down on exit even if a test raises."""

    def __init__(self, *extra: str) -> None:
        self.dir = tempfile.mkdtemp(prefix="moon-redispy-")
        self.port = _free_port()
        moon_bin = os.environ.get("MOON_BIN")
        if not moon_bin or not os.access(moon_bin, os.X_OK):
            raise RuntimeError(
                f"MOON_BIN is not an executable: {moon_bin!r}. Pin it "
                f"explicitly — falling back to target/release/moon runs a "
                f"binary of unknown provenance and a green result would be "
                f"meaningless."
            )
        self._err = open(os.path.join(self.dir, "moon.err"), "wb")
        self.proc = subprocess.Popen(
            [
                moon_bin,
                "--port", str(self.port),
                "--shards", "2",
                "--admin-port", "0",
                "--appendonly", "no",
                "--disk-free-min-pct", "0",
                "--dir", self.dir,
                *extra,
            ],
            stdout=subprocess.DEVNULL,
            stderr=self._err,
        )
        self._await_ready()

    def _await_ready(self, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"moon exited with {self.proc.returncode} before "
                    f"answering PING\n{self._log()}"
                )
            try:
                c = redis.Redis(host="127.0.0.1", port=self.port,
                                socket_connect_timeout=0.5)
                if c.ping():
                    c.close()
                    return
            except redis.exceptions.RedisError:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"moon never answered PING\n{self._log()}")

    def _log(self) -> str:
        try:
            with open(os.path.join(self.dir, "moon.err")) as f:
                return "--- moon stderr ---\n" + f.read()
        except OSError:
            return "(no stderr captured)"

    def close(self) -> None:
        self.proc.kill()
        self.proc.wait()
        self._err.close()


class RedisPyAcceptance(unittest.TestCase):
    """Every test drives redis-py's own API, never a raw socket."""

    server: MoonServer

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = MoonServer()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.close()

    def client(self, **kw) -> redis.Redis:
        c = redis.Redis(host="127.0.0.1", port=self.server.port,
                        decode_responses=True, **kw)
        self.addCleanup(c.close)
        return c

    # -- handshake ---------------------------------------------------------

    def test_rp1_resp2_and_resp3_both_negotiate(self):
        """redis-py opens with HELLO and adapts; both protocols must work.

        redis-py 5+ also sends CLIENT SETINFO on connect. A server that
        errors on it fails the *connection*, not just that command — which is
        how a deny-list ACL once broke every redis-py client wholesale.
        """
        for proto in (2, 3):
            with self.subTest(protocol=proto):
                c = self.client(protocol=proto)
                self.assertTrue(c.ping())
                self.assertEqual(c.set("rp1", f"p{proto}"), True)
                self.assertEqual(c.get("rp1"), f"p{proto}")

    def test_rp2_client_info_parses_and_identifies_this_connection(self):
        """redis-py parses CLIENT INFO into a dict by splitting on `field=`.

        A malformed or short line does not raise — it silently yields a dict
        missing the keys the caller asked for, so this asserts on the fields an
        application actually reads rather than on the reply being non-empty.
        """
        c = self.client()
        info = c.client_info()
        for field in ("id", "addr", "laddr", "db", "age", "resp"):
            self.assertIn(field, info, f"CLIENT INFO has no {field!r}: {info}")
        self.assertGreater(int(info["id"]), 0)
        # The row must describe THIS connection, not an arbitrary one.
        self.assertEqual(int(info["db"]), 0)

    def test_rp2b_cmd_field_is_a_known_gap(self):
        """KNOWN GAP, pinned so it cannot close unnoticed.

        Redis reports the command the connection is executing — `client|info`
        for this very call. Moon reports the literal `NULL` for every
        connection: `src/client_registry.rs` hardcodes `cmd=NULL`, and there is
        no per-command hook to feed it (`ClientLiveState::touch()` is called
        only from the CLIENT LIST/INFO paths, to refresh flags). Wiring a
        truthful value means a write on all three dispatch paths.

        Anyone profiling a stuck connection reads this field, so it is worth
        fixing — but a narrow special-case that sets it only for the self row
        would satisfy a test while still lying about every other row.

        This assertion is written to FAIL when the gap closes, so the fix
        deletes it rather than leaving a stale pin behind.
        """
        c = self.client()
        self.assertEqual(
            c.client_info()["cmd"],
            "NULL",
            "CLIENT INFO now reports a real `cmd` — the known gap is closed. "
            "Delete this test and assert `cmd == 'client|info'` in rp2.",
        )

    # -- decoding ----------------------------------------------------------

    def test_rp3_hash_decodes_to_dict(self):
        c = self.client()
        c.delete("rp3")
        c.hset("rp3", mapping={"a": "1", "b": "2"})
        self.assertEqual(c.hgetall("rp3"), {"a": "1", "b": "2"})

    def test_rp4_set_decodes_to_set(self):
        c = self.client()
        c.delete("rp4")
        c.sadd("rp4", "x", "y", "z")
        self.assertEqual(c.smembers("rp4"), {"x", "y", "z"})

    def test_rp5_zset_withscores_decodes_to_float_pairs(self):
        c = self.client()
        c.delete("rp5")
        c.zadd("rp5", {"a": 1.5, "b": 2.0})
        self.assertEqual(
            c.zrange("rp5", 0, -1, withscores=True),
            [("a", 1.5), ("b", 2.0)],
        )

    def test_rp6_resp3_map_and_double_types(self):
        """Under RESP3 redis-py relies on the SERVER to type the reply.

        A Map arrives as a dict and a Double as a float without redis-py
        converting anything, so a server that emits an Array where Redis emits
        a Map silently hands the caller a different Python type.
        """
        c = self.client(protocol=3)
        c.delete("rp6")
        c.hset("rp6", mapping={"k": "v"})
        self.assertIsInstance(c.hgetall("rp6"), dict)
        c.zadd("rp6z", {"m": 3.25})
        self.assertIsInstance(c.zscore("rp6z", "m"), float)

    # -- pipelining and transactions ---------------------------------------

    def test_rp7_pipeline_without_transaction(self):
        """A pipeline must execute in order and be visible to its own later commands."""
        c = self.client()
        c.delete("rp7:a", "rp7:b")
        with c.pipeline(transaction=False) as p:
            p.set("rp7:a", "1")
            p.set("rp7:b", "2")
            p.get("rp7:a")
            p.get("rp7:b")
            out = p.execute()
        self.assertEqual(out[:2], [True, True])
        self.assertEqual(out[-2:], ["1", "2"])

    def test_rp7b_mget_in_a_pipeline_sees_its_own_batch(self):
        """moon#507, fixed: MGET must observe the SETs from its own batch.

        This was a KNOWN GAP pinned as an inverted probe (assert that at least
        one trial is broken). It is now a direct assertion, which is the shape
        the probe itself asked for when it started failing.

        Still twenty independent key groups rather than one, and for the same
        reason the probe needed them: whether a group is affected depends on
        which shard owns its keys relative to the connection's own shard, so a
        single trial only samples one placement. Twenty makes a regression that
        reaches even half of placements essentially certain to be caught.
        """
        c = self.client()
        for i in range(20):
            a, b = f"{{rp7b{i}}}a", f"{{rp7b{i}}}b"
            c.delete(a, b)
            with c.pipeline(transaction=False) as p:
                p.set(a, "1")
                p.set(b, "2")
                p.mget(a, b)
                out = p.execute()
            self.assertEqual(out[:2], [True, True], "the SETs themselves failed")
            self.assertEqual(
                out[-1], ["1", "2"],
                f"MGET of {a}/{b} did not see the SETs that acked earlier in "
                f"its OWN pipeline batch — read-your-own-writes violated "
                f"(moon#507 regressed)",
            )
            self.assertEqual(c.mget(a, b), ["1", "2"])

    def test_rp8_multi_exec_transaction(self):
        c = self.client()
        c.delete("rp8")
        with c.pipeline() as p:
            p.multi()
            p.incr("rp8")
            p.incr("rp8")
            self.assertEqual(p.execute(), [1, 2])

    def test_rp9_watch_aborts_on_concurrent_write(self):
        """redis-py's own optimistic-locking idiom, the WATCH/EXEC CAS path.

        A server that does not actually invalidate a WATCH returns a result
        here instead of raising, and every redis-py `Lock` and counter built
        on this pattern silently loses updates.
        """
        c = self.client()
        other = self.client()
        c.set("rp9", "1")
        with c.pipeline() as p:
            p.watch("rp9")
            other.set("rp9", "2")          # concurrent write invalidates it
            p.multi()
            p.get("rp9")
            with self.assertRaises(redis.exceptions.WatchError):
                p.execute()

    def test_rp10_watch_commits_when_untouched(self):
        """The other half: an untouched WATCH must NOT spuriously abort."""
        c = self.client()
        c.set("rp10", "1")
        with c.pipeline() as p:
            p.watch("rp10")
            p.multi()
            p.set("rp10", "2")
            self.assertEqual(p.execute(), [True])
        self.assertEqual(c.get("rp10"), "2")

    # -- iteration ---------------------------------------------------------

    def test_rp11_scan_iter_reaches_every_key(self):
        """SCAN's cursor contract, exercised the way applications hit it.

        redis-py drives the cursor to exhaustion; a cursor that repeats or
        drops keys shows up here as a wrong count, not as a protocol error.
        """
        c = self.client()
        c.flushdb()
        expected = {f"rp11:{i}" for i in range(500)}
        with c.pipeline(transaction=False) as p:
            for k in expected:
                p.set(k, "v")
            p.execute()
        self.assertEqual(set(c.scan_iter(match="rp11:*", count=10)), expected)

    def test_rp12_hscan_iter_reaches_every_field(self):
        c = self.client()
        c.delete("rp12")
        fields = {f"f{i}": str(i) for i in range(300)}
        c.hset("rp12", mapping=fields)
        self.assertEqual(dict(c.hscan_iter("rp12", count=10)), fields)

    # -- pub/sub -----------------------------------------------------------

    def test_rp13_pubsub_channel_and_pattern(self):
        c = self.client()
        ps = c.pubsub()
        self.addCleanup(ps.close)
        ps.subscribe("rp13")
        ps.psubscribe("rp13:*")
        # Drain the two confirmations redis-py surfaces as messages.
        for _ in range(2):
            self.assertIsNotNone(self._await_message(ps))

        publisher = self.client()
        publisher.publish("rp13", "direct")
        m = self._await_message(ps)
        self.assertEqual((m["type"], m["data"]), ("message", "direct"))

        publisher.publish("rp13:x", "patterned")
        m = self._await_message(ps)
        self.assertEqual((m["type"], m["data"]), ("pmessage", "patterned"))

    @staticmethod
    def _await_message(ps, timeout: float = 5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            m = ps.get_message(timeout=0.2)
            if m is not None:
                return m
        raise AssertionError("no pub/sub message within 5s")

    # -- library-level constructs -----------------------------------------

    def test_rp14_lock_is_exclusive(self):
        """`redis.lock.Lock` acquisition is SET NX PX — a held lock is exclusive.

        Release is covered separately: it goes through EVALSHA, which has its
        own known gap (moon#508, see below).
        """
        c = self.client()
        lock = c.lock("rp14", timeout=5, blocking_timeout=2)
        self.assertTrue(lock.acquire())
        self.assertFalse(
            c.lock("rp14", timeout=5, blocking_timeout=0.2).acquire(),
            "a held lock was acquired twice — SET NX is not exclusive",
        )

    def test_rp14b_single_key_evalsha_is_a_known_gap(self):
        """KNOWN GAP (moon#508), amplified for the same reason as rp7b.

        At `--shards >= 2`, EVALSHA of a script declaring ONE key is rejected
        with `CROSSSLOT Keys in script don't hash to the same slot and shard`.
        One key cannot cross slots. `SCRIPT EXISTS` returns `[True]`, so it is
        the key-slot check and not a cache miss.

        This is what breaks `redis.lock.Lock.release()`, which is a single-key
        EVALSHA — so the most-used construct in redis-py fails for about half
        of all lock names. Like moon#507 it fires per-KEY (~50% at two shards),
        which is why this probe runs twenty distinct keys instead of one.
        """
        c = self.client()
        sha = c.script_load("return redis.call('get', KEYS[1])")
        self.assertEqual(c.script_exists(sha), [True], "the script did not cache")
        rejected = []
        for i in range(20):
            key = f"rp14b:{i}"
            c.set(key, "v")
            try:
                self.assertEqual(c.evalsha(sha, 1, key), "v")
            except redis.exceptions.RedisError as e:
                rejected.append((key, str(e)))
        self.assertTrue(
            rejected,
            "all 20 single-key EVALSHA calls succeeded — moon#508 is fixed. "
            "Delete this probe and restore the release half of rp14.",
        )

    def test_rp15_from_url_and_pool_reuse(self):
        """`from_url` is how most applications construct a client at all."""
        url = f"redis://127.0.0.1:{self.server.port}/0"
        c = redis.Redis.from_url(url, decode_responses=True)
        self.addCleanup(c.close)
        self.assertTrue(c.ping())
        # Same pool, many round trips: a connection that is not reusable shows
        # up as an error on the second or third command, not the first.
        for i in range(50):
            c.set("rp15", str(i))
        self.assertEqual(c.get("rp15"), "49")

    def test_rp16_select_isolates_databases(self):
        c0 = self.client(db=0)
        c1 = self.client(db=1)
        c0.set("rp16", "zero")
        c1.set("rp16", "one")
        self.assertEqual(c0.get("rp16"), "zero")
        self.assertEqual(c1.get("rp16"), "one")


if __name__ == "__main__":
    unittest.main()
