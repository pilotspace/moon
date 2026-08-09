#!/usr/bin/env python3
"""Raw-RESP differential harness: Moon vs a real redis-server.

Why this exists: `scripts/test-commands.sh` already compares Moon to Redis, but
it compares `redis-cli` **rendered text**, which destroys the reply type before
the comparison happens (it even does `tr -d '(integer) '`), and it never sends
`-3`, so the entire RESP3 surface is uncompared. That blindness is why ~22
type-level compatibility defects survived into v0.8.5.

This differ speaks RESP on a raw socket so the type byte survives to the
assertion, and it compares in a fixed order — TYPE, then SHAPE, then VALUE — so a
finding says which of the three diverged. A type defect and a value defect are
different bugs and must not be reported as one.

Contract: see `.add/tasks/client-compat-harness/TASK.md` §3 (FROZEN @ v1).
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Optional

POLICIES = ("exact", "sorted", "type_only", "numeric_tolerance", "ignore_value")
CONTEXTS = ("standalone", "multi", "pipeline")
PROTOCOLS = ("resp2", "resp3")


class HarnessError(Exception):
    """A refusal the harness must make loudly. `.code` is the contracted token."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


class _Incomplete(Exception):
    """Internal: the buffer holds a prefix of a reply, not a malformed one."""


# ===========================================================================
# RESP codec
# ===========================================================================

_KIND_BY_BYTE = {
    ord("+"): "simple",
    ord("-"): "error",
    ord(":"): "integer",
    ord("$"): "bulk",
    ord("*"): "array",
    ord("_"): "null",
    ord(","): "double",
    ord("#"): "boolean",
    ord("%"): "map",
    ord("~"): "set",
    ord(">"): "push",
    ord("("): "bignum",
    ord("="): "verbatim",
    ord("!"): "bloberror",
}

_AGGREGATES = {"array", "map", "set", "push"}


@dataclass
class Node:
    """One parsed RESP reply. `kind` is the wire type, never normalized away."""

    kind: str
    value: Optional[bytes] = None
    children: Optional[list["Node"]] = None

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        if self.children is not None:
            return f"<{self.kind}[{len(self.children)}]>"
        return f"<{self.kind} {self.value!r}>"


def _line(buf: bytes, i: int) -> tuple[bytes, int]:
    end = buf.find(b"\r\n", i)
    if end < 0:
        raise _Incomplete()
    return buf[i:end], end + 2


def _int(raw: bytes) -> int:
    try:
        return int(raw)
    except ValueError:
        raise HarnessError("ERR_PROTOCOL_PARSE", f"not an integer: {raw!r}")


def _parse_at(buf: bytes, i: int) -> tuple[Node, int]:
    if i >= len(buf):
        raise _Incomplete()
    kind = _KIND_BY_BYTE.get(buf[i])
    if kind is None:
        raise HarnessError("ERR_PROTOCOL_PARSE", f"unknown type byte {buf[i:i + 1]!r}")
    raw, j = _line(buf, i + 1)

    if kind in ("simple", "error", "integer", "double", "boolean", "bignum"):
        return Node(kind, raw), j
    if kind == "null":
        return Node("null"), j

    if kind in ("bulk", "verbatim", "bloberror"):
        n = _int(raw)
        if n < 0:
            return Node("null"), j            # RESP2 null bulk
        if len(buf) < j + n + 2:
            raise _Incomplete()
        return Node(kind, buf[j:j + n]), j + n + 2

    # aggregates
    n = _int(raw)
    if n < 0:
        return Node("null"), j                # RESP2 null array
    count = n * 2 if kind == "map" else n
    kids: list[Node] = []
    for _ in range(count):
        child, j = _parse_at(buf, j)
        kids.append(child)
    return Node(kind, None, kids), j


def parse_resp(data: bytes) -> Node:
    """Parse exactly one reply. Trailing bytes are ignored (pipelines)."""
    try:
        node, _ = _parse_at(data, 0)
    except _Incomplete:
        raise HarnessError("ERR_PROTOCOL_PARSE", "truncated reply")
    return node


def encode_command(args: list[str]) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a.encode() if isinstance(a, str) else a
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


# ===========================================================================
# Comparator — TYPE -> SHAPE -> VALUE
# ===========================================================================


@dataclass
class Verdict:
    ok: bool
    divergence: Optional[str] = None   # 'type' | 'shape' | 'value'
    detail: str = ""


def _shape_sig(n: Node) -> Any:
    """Structure only: kinds and arity, all the way down. No leaf payloads."""
    if n.children is None:
        return n.kind
    return (n.kind, tuple(_shape_sig(c) for c in n.children))


def _error_code(v: Optional[bytes]) -> bytes:
    # Redis error text is not a stable API, but clients DO match the code.
    return (v or b"").split(b" ", 1)[0]


def _sorted_key(n: Node) -> bytes:
    return repr(_flatten(n)).encode()


def _flatten(n: Node) -> Any:
    if n.children is None:
        return (n.kind, n.value)
    return (n.kind, [_flatten(c) for c in n.children])


def _value_cmp(a: Node, b: Node, policy: str, tolerance: Optional[float]) -> Verdict:
    if a.kind == "error":
        if _error_code(a.value) != _error_code(b.value):
            return Verdict(False, "value",
                           f"error code {_error_code(a.value)!r} != {_error_code(b.value)!r}")
        return Verdict(True)

    if a.children is not None:
        left, right = a.children, b.children or []
        if policy == "sorted":
            left = sorted(left, key=_sorted_key)
            right = sorted(right, key=_sorted_key)
        for x, y in zip(left, right):
            v = _value_cmp(x, y, policy, tolerance)
            if not v.ok:
                return v
        return Verdict(True)

    if policy == "numeric_tolerance":
        try:
            da, db = float(a.value or b"0"), float(b.value or b"0")
        except ValueError:
            return Verdict(False, "value", "non-numeric under numeric_tolerance")
        if abs(da - db) > (tolerance or 0):
            return Verdict(False, "value", f"{da} vs {db} exceeds tolerance {tolerance}")
        return Verdict(True)

    if a.value != b.value:
        return Verdict(False, "value", f"{a.value!r} != {b.value!r}")
    return Verdict(True)


def compare(expected: Node, actual: Node, policy: str = "exact",
            tolerance: Optional[float] = None) -> Verdict:
    """Compare an oracle reply against Moon's. Order is TYPE -> SHAPE -> VALUE.

    TYPE is the top-level wire kind. A RESP3 Map is never equal to a flat Array
    and a Double is never equal to a Bulk — collapsing those is exactly the
    blindness this harness replaces. Structural differences BELOW the top level
    (arity, nesting, a nested kind change) are SHAPE, so a caller can tell
    "wrong reply type" from "right type, wrong structure".
    """
    if policy not in POLICIES:
        raise HarnessError("ERR_BAD_MANIFEST", f"unknown policy {policy!r}")

    if expected.kind != actual.kind:
        return Verdict(False, "type", f"{expected.kind} != {actual.kind}")

    if _shape_sig(expected) != _shape_sig(actual):
        return Verdict(False, "shape", "structure differs below the top level")

    if policy in ("type_only", "ignore_value"):
        return Verdict(True)

    return _value_cmp(expected, actual, policy, tolerance)


# ===========================================================================
# Manifest
# ===========================================================================


@dataclass
class Entry:
    name: str
    command: str
    policy: str
    setup: list[str] = field(default_factory=list)
    tolerance: Optional[float] = None
    contexts: tuple[str, ...] = CONTEXTS
    protocols: tuple[str, ...] = PROTOCOLS
    expect_diff: Optional[str] = None


def load_manifest(path: str) -> list[Entry]:
    """Load and validate. Every refusal here fires BEFORE a server is spawned."""
    try:
        import yaml
    except ImportError:  # pragma: no cover - environment guard
        raise HarnessError("ERR_BAD_MANIFEST", "pyyaml is required to read the manifest")

    try:
        with open(path) as f:
            doc = yaml.safe_load(f) or {}
    except OSError as e:
        raise HarnessError("ERR_BAD_MANIFEST", f"cannot read {path}: {e}")

    raw = doc.get("entries")
    if not isinstance(raw, list) or not raw:
        raise HarnessError("ERR_BAD_MANIFEST", "manifest has no 'entries' list")

    entries: list[Entry] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise HarnessError("ERR_BAD_MANIFEST", f"entry {i} is not a mapping")
        for required in ("name", "command", "policy"):
            if not item.get(required):
                raise HarnessError("ERR_BAD_MANIFEST",
                                   f"entry {i} is missing '{required}'")
        policy = item["policy"]
        if policy not in POLICIES:
            raise HarnessError("ERR_BAD_MANIFEST",
                               f"entry {item['name']}: unknown policy {policy!r}")
        tolerance = item.get("tolerance")
        if policy == "numeric_tolerance" and tolerance is None:
            raise HarnessError("ERR_BAD_MANIFEST",
                               f"entry {item['name']}: numeric_tolerance needs 'tolerance'")

        contexts = tuple(item.get("contexts") or CONTEXTS)
        for c in contexts:
            if c not in CONTEXTS:
                raise HarnessError("ERR_BAD_MANIFEST",
                                   f"entry {item['name']}: unknown context {c!r}")
        protocols = tuple(item.get("protocols") or PROTOCOLS)
        for p in protocols:
            if p not in PROTOCOLS:
                raise HarnessError("ERR_BAD_MANIFEST",
                                   f"entry {item['name']}: unknown protocol {p!r}")

        waiver = None
        if "expect_diff" in item and item["expect_diff"] is not None:
            ed = item["expect_diff"]
            reason = ed.get("reason") if isinstance(ed, dict) else None
            if not reason:
                # A waiver without a reason is an unexplained pass, which is the
                # thing this harness exists to make impossible.
                raise HarnessError("ERR_UNREASONED_WAIVER",
                                   f"entry {item['name']}: expect_diff needs a reason")
            waiver = reason

        entries.append(Entry(
            name=item["name"],
            command=item["command"],
            policy=policy,
            setup=list(item.get("setup") or []),
            tolerance=tolerance,
            contexts=contexts,
            protocols=protocols,
            expect_diff=waiver,
        ))
    return entries


# ===========================================================================
# Wire connection
# ===========================================================================


class RespConn:
    def __init__(self, port: int, protocol: str, timeout: float = 10.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""
        if protocol == "resp3":
            self.roundtrip(["HELLO", "3"])

    def send(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def read_reply(self) -> tuple[bytes, Optional[Node]]:
        """Return (raw bytes of exactly one reply, parsed node or None)."""
        while True:
            try:
                node, end = _parse_at(self.buf, 0)
            except _Incomplete:
                chunk = self.sock.recv(65536)
                if not chunk:
                    raise HarnessError("ERR_PROTOCOL_PARSE", "connection closed mid-reply")
                self.buf += chunk
                continue
            except HarnessError:
                raw, self.buf = self.buf, b""
                return raw, None
            raw, self.buf = self.buf[:end], self.buf[end:]
            return raw, node

    def roundtrip(self, args: list[str]) -> tuple[bytes, Optional[Node]]:
        self.send(encode_command(args))
        return self.read_reply()

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


# ===========================================================================
# Runner
# ===========================================================================


@dataclass
class RunConfig:
    manifest_path: str
    moon_bin: str
    redis_bin: str
    contexts: tuple[str, ...] = CONTEXTS
    protocols: tuple[str, ...] = PROTOCOLS
    strict: bool = False
    info_manifest: Optional[str] = None
    min_redis: str = "7.4.0"
    ready_timeout_s: float = 15.0
    name_filter: Optional[str] = None
    record_path: Optional[str] = None


@dataclass
class Result:
    name: str
    protocol: str
    context: str
    sent_bytes: bytes
    redis_raw: bytes
    moon_raw: bytes
    verdict: str                      # pass | waived | diff | parse_error
    divergence: Optional[str] = None
    detail: str = ""
    waiver_reason: Optional[str] = None


@dataclass
class Report:
    results: list[Result]
    redis_version: str
    moon_version: str
    manifest_path: str

    def tally(self) -> dict[str, int]:
        t = {"pass": 0, "fail": 0, "waived": 0, "total": len(self.results)}
        for r in self.results:
            if r.verdict == "pass":
                t["pass"] += 1
            elif r.verdict == "waived":
                t["waived"] += 1
            else:
                t["fail"] += 1
        return t

    def exit_code(self) -> int:
        return 1 if self.tally()["fail"] else 0

    def to_json(self) -> str:
        return json.dumps({
            "redis_version": self.redis_version,
            "moon_version": self.moon_version,
            "generated_from_manifest": self.manifest_path,
            "results": [{
                "name": r.name,
                "protocol": r.protocol,
                "context": r.context,
                "sent_bytes": r.sent_bytes.decode("latin1"),
                "redis_raw": r.redis_raw.decode("latin1"),
                "moon_raw": r.moon_raw.decode("latin1"),
                "verdict": r.verdict,
                "divergence": r.divergence,
                "detail": r.detail,
                "waiver_reason": r.waiver_reason,
            } for r in self.results],
        }, indent=2)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _version_tuple(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", v)[:3]) or (0,)


class Runner:
    def __init__(self, cfg: RunConfig):
        self.cfg = cfg
        self.spawned_pids: list[int] = []
        self.spawned_dirs: list[str] = []
        self._procs: list[subprocess.Popen] = []

    # -- lifecycle ---------------------------------------------------------

    def _spawn(self, argv: list[str], data_dir: str) -> subprocess.Popen:
        proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        self._procs.append(proc)
        self.spawned_pids.append(proc.pid)
        self.spawned_dirs.append(data_dir)
        return proc

    def _await_ready(self, port: int, who: str) -> None:
        deadline = time.time() + self.cfg.ready_timeout_s
        while True:
            try:
                c = RespConn(port, "resp2", timeout=1.0)
                _, node = c.roundtrip(["PING"])
                c.close()
                if node is not None:
                    return
            except (OSError, HarnessError):
                pass
            if time.time() >= deadline:
                raise HarnessError("ERR_SERVER_TIMEOUT",
                                   f"{who} did not answer PING on :{port}")
            time.sleep(0.05)

    def _teardown(self) -> None:
        for p in self._procs:
            try:
                p.kill()
                p.wait(timeout=10)
            except Exception:
                pass
        for d in self.spawned_dirs:
            shutil.rmtree(d, ignore_errors=True)

    def _server_field(self, port: int, field_name: str) -> str:
        c = RespConn(port, "resp2")
        _, node = c.roundtrip(["INFO", "server"])
        c.close()
        body = (node.value or b"").decode("latin1") if node else ""
        m = re.search(rf"^{field_name}:(.+)$", body, re.M)
        return m.group(1).strip() if m else ""

    # -- comparison --------------------------------------------------------

    def _exchange(self, conn: RespConn, argv: list[str], context: str
                  ) -> tuple[bytes, bytes, Optional[Node]]:
        """Run one command in one context. Returns (sent, raw, compared node)."""
        cmd = encode_command(argv)
        if context == "standalone":
            conn.send(cmd)
            raw, node = conn.read_reply()
            return cmd, raw, node

        if context == "multi":
            conn.roundtrip(["MULTI"])
            conn.send(cmd)
            conn.read_reply()                       # +QUEUED
            sent = encode_command(["MULTI"]) + cmd + encode_command(["EXEC"])
            conn.send(encode_command(["EXEC"]))
            raw, node = conn.read_reply()
            if node is not None and node.children:
                return sent, raw, node.children[0]
            return sent, raw, node

        # pipeline: the command under test is bracketed, sent in ONE write, so a
        # shape that only changes when batched is observable.
        ping = encode_command(["PING"])
        sent = ping + cmd + ping
        conn.send(sent)
        conn.read_reply()
        raw, node = conn.read_reply()
        conn.read_reply()
        return sent, raw, node

    def _prepare(self, conn: RespConn, entry: Entry) -> None:
        conn.roundtrip(["FLUSHALL"])
        for s in entry.setup:
            conn.roundtrip(shlex.split(s))

    def _compare_entry(self, entry: Entry, protocol: str, context: str,
                       rport: int, mport: int) -> Result:
        argv = shlex.split(entry.command)
        rc = RespConn(rport, protocol)
        mc = RespConn(mport, protocol)
        try:
            self._prepare(rc, entry)
            self._prepare(mc, entry)
            r_sent, r_raw, r_node = self._exchange(rc, argv, context)
            m_sent, m_raw, m_node = self._exchange(mc, argv, context)
        finally:
            rc.close()
            mc.close()

        assert r_sent == m_sent, "byte-identical send is a contract invariant"

        if r_node is None or m_node is None:
            return Result(entry.name, protocol, context, r_sent, r_raw, m_raw,
                          "parse_error", None, "ERR_PROTOCOL_PARSE",
                          entry.expect_diff)

        v = compare(r_node, m_node, entry.policy, entry.tolerance)
        if v.ok:
            return Result(entry.name, protocol, context, r_sent, r_raw, m_raw,
                          "pass", None, "", entry.expect_diff)
        verdict = "waived" if entry.expect_diff else "diff"
        return Result(entry.name, protocol, context, r_sent, r_raw, m_raw,
                      verdict, v.divergence, v.detail, entry.expect_diff)

    def _info_coverage(self, rport: int, mport: int) -> list[Result]:
        with open(self.cfg.info_manifest) as f:
            fields = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
        rc, mc = RespConn(rport, "resp2"), RespConn(mport, "resp2")
        try:
            sent = encode_command(["INFO"])
            rc.send(sent)
            r_raw, r_node = rc.read_reply()
            mc.send(sent)
            m_raw, m_node = mc.read_reply()
        finally:
            rc.close()
            mc.close()
        moon_body = (m_node.value or b"").decode("latin1") if m_node else ""
        out = []
        for fname in fields:
            present = re.search(rf"^{re.escape(fname)}:", moon_body, re.M) is not None
            out.append(Result(f"info:{fname}", "resp2", "standalone", sent,
                              r_raw, m_raw,
                              "pass" if present else "diff",
                              None if present else "value",
                              "" if present else f"INFO is missing '{fname}'"))
        return out

    # -- entry point -------------------------------------------------------

    def run(self) -> Report:
        cfg = self.cfg
        # 1. Manifest first: every load-time refusal must fire before a spawn.
        entries = load_manifest(cfg.manifest_path)
        if cfg.name_filter:
            entries = [e for e in entries if cfg.name_filter in e.name]

        # 2. Binaries. Refusing here means nothing is ever left running.
        if not cfg.redis_bin or not os.access(cfg.redis_bin, os.X_OK):
            raise HarnessError("ERR_NO_ORACLE",
                               f"redis-server not executable: {cfg.redis_bin!r}")
        if not cfg.moon_bin or not os.access(cfg.moon_bin, os.X_OK):
            raise HarnessError("ERR_NO_MOON",
                               f"moon binary not executable: {cfg.moon_bin!r}")

        try:
            rport, mport = _free_port(), _free_port()
            rdir = tempfile.mkdtemp(prefix="cc-redis-")
            mdir = tempfile.mkdtemp(prefix="cc-moon-")

            self._spawn([cfg.redis_bin, "--port", str(rport),
                         "--bind", "127.0.0.1", "--save", "",
                         "--appendonly", "no", "--protected-mode", "no",
                         "--dir", rdir], rdir)
            self._await_ready(rport, "redis-server")

            redis_version = self._server_field(rport, "redis_version")
            if _version_tuple(redis_version) < _version_tuple(cfg.min_redis):
                raise HarnessError(
                    "ERR_NO_ORACLE",
                    f"oracle {redis_version} is below the {cfg.min_redis} floor")

            self._spawn([cfg.moon_bin, "--port", str(mport),
                         "--bind", "127.0.0.1", "--shards", "1",
                         "--appendonly", "no", "--protected-mode", "no",
                         "--disk-free-min-pct", "0", "--dir", mdir], mdir)
            self._await_ready(mport, "moon")
            moon_version = self._server_field(mport, "redis_version")

            results: list[Result] = []
            stale: list[str] = []
            for entry in entries:
                diverged = False
                for protocol in entry.protocols:
                    if protocol not in cfg.protocols:
                        continue
                    for context in entry.contexts:
                        if context not in cfg.contexts:
                            continue
                        r = self._compare_entry(entry, protocol, context,
                                                rport, mport)
                        diverged = diverged or r.verdict in ("diff", "waived",
                                                             "parse_error")
                        results.append(r)
                if entry.expect_diff and not diverged:
                    stale.append(entry.name)

            if cfg.info_manifest:
                results.extend(self._info_coverage(rport, mport))

            report = Report(results, redis_version, moon_version,
                            cfg.manifest_path)
            if cfg.record_path:
                with open(cfg.record_path, "w") as f:
                    f.write(report.to_json())

            # A waiver that no longer reproduces is a permanent blind spot in
            # waiting: under --strict it fails rather than quietly passing.
            if cfg.strict and stale:
                raise HarnessError("ERR_STALE_WAIVER",
                                   "waivers no longer reproduce: " + ", ".join(stale))
            return report
        finally:
            self._teardown()


# ===========================================================================
# CLI
# ===========================================================================


def _default_moon_bin() -> str:
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for candidate in ("target/release/moon", "target-fast/release-fast/moon"):
        p = os.path.join(root, candidate)
        if os.access(p, os.X_OK):
            return p
    return os.environ.get("MOON_BIN", "")


def main(argv: list[str]) -> int:
    import argparse

    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(prog="test-client-compat")
    ap.add_argument("--manifest", default=os.path.join(here, "manifest.yaml"))
    ap.add_argument("--contexts", default=",".join(CONTEXTS))
    ap.add_argument("--protocols", default=",".join(PROTOCOLS))
    ap.add_argument("--filter")
    ap.add_argument("--info-manifest", nargs="?", const=os.path.join(here, "info_fields.txt"))
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--record", default="tmp/client-compat-record.json")
    ap.add_argument("--moon-bin", default=os.environ.get("MOON_BIN") or _default_moon_bin())
    ap.add_argument("--redis-bin", default=os.environ.get("REDIS_BIN")
                    or shutil.which("redis-server") or "")
    ap.add_argument("--min-redis", default="7.4.0")
    a = ap.parse_args(argv)

    os.makedirs(os.path.dirname(a.record) or ".", exist_ok=True)
    cfg = RunConfig(
        manifest_path=a.manifest,
        moon_bin=a.moon_bin,
        redis_bin=a.redis_bin,
        contexts=tuple(a.contexts.split(",")),
        protocols=tuple(a.protocols.split(",")),
        strict=a.strict,
        info_manifest=a.info_manifest,
        min_redis=a.min_redis,
        name_filter=a.filter,
        record_path=a.record,
    )

    try:
        report = Runner(cfg).run()
    except HarnessError as e:
        print(f"{e.code}: {e.detail}", file=sys.stderr)
        return 2

    for r in report.results:
        if r.verdict in ("diff", "parse_error"):
            print(f"  FAIL [{r.divergence or r.verdict}] {r.name} "
                  f"({r.protocol}/{r.context}): {r.detail}")
            print(f"    REDIS: {r.redis_raw[:120]!r}")
            print(f"    MOON:  {r.moon_raw[:120]!r}")
        elif r.verdict == "waived":
            print(f"  WAIVED {r.name} ({r.protocol}/{r.context}): {r.waiver_reason}")

    t = report.tally()
    print(f"oracle: redis {report.redis_version} · moon {report.moon_version}")
    print(f"PASS={t['pass']} FAIL={t['fail']} WAIVED={t['waived']} TOTAL={t['total']}")
    return report.exit_code()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
