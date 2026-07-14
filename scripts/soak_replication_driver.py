#!/usr/bin/env python3
"""soak_replication_driver.py -- acked-write ledger driver for the 24h
replication soak harness (task #61, gates the v0.7.0 "Replication GA"
release headline).

Speaks raw RESP over a plain socket (no redis-py dependency) so the exact
wire behaviour matches what `tests/replication_multishard.rs` /
`tests/replication_hardening.rs` already exercise against real moon
processes.

Three subcommands, invoked by `scripts/soak-replication-24h.sh`:

  writer   -- runs forever (until SIGTERM/SIGINT): SET soak:<seq> <seq>:<ts>
              on the master, then WAIT 1 <timeout-ms>. Only a WAIT>=1 reply
              appends the seq to the ACKED ledger (line fsync'd immediately).
              A WAIT==0 (timeout) appends to the INFLIGHT ledger instead --
              that write is allowed to be lost OR present on recovery, it is
              simply not a durability *guarantee*, so it never fails the
              soak. A connection failure (master down) pauses the writer
              (retries the SAME seq/key once connectivity returns) instead
              of advancing -- this is what keeps the acked ledger's
              zero-data-loss claim meaningful across a master kill -9.

  catchup  -- polls a replica until its last few ACKED ledger entries read
              back correctly, or times out. This is the data-driven resync
              gate: `INFO replication` flipping to `master_link_status:up`
              only means the TCP link is back, not that the backlog/RDB
              replay has actually landed -- running the strict `verify`
              sample immediately after link-up (without this gate) would
              produce false SOAK-FAIL positives during the catch-up window.

  verify   -- samples the ACKED ledger (random N + last M, or --full for an
              end-of-soak sweep) and asserts every sampled seq reads back
              correctly on BOTH master and replica. Any mismatch prints
              `SOAK-FAIL seq=<n> side=<m|r> cycle=<k>` and the process exits
              1 immediately -- the orchestrator treats that as fatal.

Every socket op is timeout-bound; nothing in this script can hang the
24h/30m soak indefinitely on a wedged connection.
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import socket
import sys
import time

DEFAULT_SOCK_TIMEOUT = 5.0


class RespError(Exception):
    """A RESP `-ERR ...` reply, or a malformed/short read."""


class RespConn:
    """Minimal blocking RESP2 client: one TCP connection, lazily
    (re)established on first use after any close()."""

    def __init__(self, host: str, port: int, timeout: float = DEFAULT_SOCK_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None

    def connect(self) -> None:
        self.close()
        s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        s.settimeout(self.timeout)
        self.sock = s

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _ensure(self) -> None:
        if self.sock is None:
            self.connect()

    def cmd(self, *parts: str):
        """Send one RESP array command and return the parsed reply."""
        self._ensure()
        assert self.sock is not None
        out = bytearray(f"*{len(parts)}\r\n".encode())
        for p in parts:
            b = p if isinstance(p, bytes) else str(p).encode()
            out += f"${len(b)}\r\n".encode() + b + b"\r\n"
        self.sock.sendall(bytes(out))
        return self._read_reply()

    def _readline(self) -> bytes:
        assert self.sock is not None
        buf = bytearray()
        while not buf.endswith(b"\r\n"):
            chunk = self.sock.recv(1)
            if not chunk:
                raise RespError("connection closed while reading line")
            buf += chunk
        return bytes(buf[:-2])

    def _read_exact(self, n: int) -> bytes:
        assert self.sock is not None
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise RespError("connection closed mid-bulk")
            data += chunk
        return bytes(data)

    def _read_reply(self):
        line = self._readline()
        if not line:
            raise RespError("empty reply line")
        tag, rest = line[:1], line[1:]
        if tag == b"+":
            return rest.decode(errors="replace")
        if tag == b"-":
            raise RespError(rest.decode(errors="replace"))
        if tag == b":":
            return int(rest)
        if tag == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self._read_exact(n)
            self._read_exact(2)  # trailing CRLF
            return data.decode(errors="replace")
        if tag == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self._read_reply() for _ in range(n)]
        raise RespError(f"unknown reply type byte: {line!r}")


def split_addr(s: str) -> tuple[str, int]:
    host, port = s.rsplit(":", 1)
    return host, int(port)


# ---------------------------------------------------------------------------
# Ledger I/O
# ---------------------------------------------------------------------------


def last_line(path: str) -> str | None:
    """Read the final non-blank line of an append-only file without loading
    the whole thing (ledgers can grow to hundreds of thousands of lines over
    24h)."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        pos = size
        buf = b""
        while pos > 0:
            step = min(8192, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
            if buf.count(b"\n") >= 2 or pos == 0:
                break
        lines = [l for l in buf.splitlines() if l.strip()]
        return lines[-1].decode(errors="replace") if lines else None


def resume_seq(ledger_path: str, inflight_path: str) -> int:
    """Next seq to write: 1 + the highest seq recorded in EITHER file (both
    grow monotonically in seq order since a single writer thread owns
    them)."""
    hi = -1
    for path in (ledger_path, inflight_path):
        line = last_line(path)
        if line is None:
            continue
        try:
            seq = int(line.split(" ", 1)[0])
        except ValueError:
            continue
        hi = max(hi, seq)
    return hi + 1


def load_ledger(path: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    if not os.path.exists(path):
        return out
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(" ", 1)
            if len(parts) != 2:
                continue
            try:
                seq = int(parts[0])
            except ValueError:
                continue
            out.append((seq, parts[1]))
    return out


def tail_ledger(path: str, n: int) -> list[tuple[int, str]]:
    all_entries = load_ledger(path)
    return all_entries[-n:] if n > 0 else []


# ---------------------------------------------------------------------------
# writer
# ---------------------------------------------------------------------------


def mode_writer(args: argparse.Namespace) -> int:
    host, port = split_addr(args.master)
    # The read timeout must comfortably exceed the WAIT deadline we ask the
    # server for, or a slow-but-legitimate WAIT reply misreads as a
    # connection failure.
    sock_timeout = max(args.timeout, args.wait_timeout_ms / 1000.0 + 2.0)
    conn = RespConn(host, port, timeout=sock_timeout)

    seq = resume_seq(args.ledger, args.inflight)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    stop = {"flag": False}

    def handle_sig(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    acked_count = 0
    inflight_count = 0
    paused_count = 0
    print(f"writer: starting at seq={seq} rate={args.rate}/s master={args.master}", file=sys.stderr, flush=True)

    with open(args.ledger, "a", buffering=1) as ledger, open(args.inflight, "a", buffering=1) as inflight:
        while not stop["flag"]:
            t0 = time.time()
            value = f"{seq}:{t0}"
            key = f"soak:{seq}"
            try:
                r = conn.cmd("SET", key, value)
                if r != "OK":
                    raise RespError(f"unexpected SET reply: {r!r}")
                w = conn.cmd("WAIT", "1", str(args.wait_timeout_ms))
            except (RespError, OSError, socket.timeout):
                # Master is down or the link dropped mid-command. We do NOT
                # know whether the SET committed, so we do NOT advance seq --
                # retry the SAME seq (with a fresh value) once connectivity
                # returns. This is the "pause acking during known-down
                # windows" behaviour the soak's zero-loss claim depends on.
                conn.close()
                paused_count += 1
                if stop["flag"]:
                    break
                time.sleep(args.retry_interval)
                continue

            if isinstance(w, int) and w >= 1:
                ledger.write(f"{seq} {value}\n")
                ledger.flush()
                os.fsync(ledger.fileno())
                acked_count += 1
            else:
                # WAIT timed out (replica down/lagging) -- NOT a loss, just
                # unacked. Recorded separately so verify never treats it as
                # a durability guarantee.
                inflight.write(f"{seq} {value}\n")
                inflight.flush()
                inflight_count += 1

            seq += 1
            if (acked_count + inflight_count) % 500 == 0:
                print(
                    f"writer: seq={seq} acked={acked_count} inflight={inflight_count} paused_retries={paused_count}",
                    file=sys.stderr,
                    flush=True,
                )

            elapsed = time.time() - t0
            if interval > elapsed and not stop["flag"]:
                time.sleep(interval - elapsed)

    conn.close()
    print(
        f"writer: stopped at seq={seq} acked={acked_count} inflight={inflight_count} paused_retries={paused_count}",
        file=sys.stderr,
        flush=True,
    )
    return 0


# ---------------------------------------------------------------------------
# catchup
# ---------------------------------------------------------------------------


def mode_catchup(args: argparse.Namespace) -> int:
    target = tail_ledger(args.ledger, args.check_last)
    if not target:
        print(f"SOAK-CATCHUP-OK cycle={args.cycle} target=none (empty ledger)")
        return 0

    host, port = split_addr(args.replica)
    conn = RespConn(host, port, timeout=args.timeout)
    deadline = time.time() + args.timeout_sec
    last_mismatch: tuple[int, object] | None = None

    while time.time() < deadline:
        ok = True
        for seq, value in target:
            try:
                got = conn.cmd("GET", f"soak:{seq}")
            except (RespError, OSError, socket.timeout):
                conn.close()
                ok = False
                last_mismatch = (seq, "<connect-error>")
                break
            if got != value:
                ok = False
                last_mismatch = (seq, got)
                break
        if ok:
            print(f"SOAK-CATCHUP-OK cycle={args.cycle} target_seq={target[-1][0]}")
            conn.close()
            return 0
        time.sleep(args.poll_interval)

    seq, got = last_mismatch if last_mismatch else (None, None)
    print(
        f"SOAK-CATCHUP-TIMEOUT cycle={args.cycle} seq={seq} got={got!r} after={args.timeout_sec}s"
    )
    conn.close()
    return 1


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def get_with_retry(conn: RespConn, key: str, retries: int, backoff: float):
    last_err: Exception | None = None
    for _ in range(retries + 1):
        try:
            return conn.cmd("GET", key)
        except (RespError, OSError, socket.timeout) as e:
            last_err = e
            conn.close()
            time.sleep(backoff)
    raise last_err if last_err else RuntimeError("unreachable")


def mode_verify(args: argparse.Namespace) -> int:
    entries = load_ledger(args.ledger)
    total = len(entries)
    if total == 0:
        print(f"SOAK-VERIFY-OK cycle={args.cycle} checked=0 ledger_total=0 (empty ledger, nothing acked yet)")
        return 0

    if args.full:
        sample = entries
    else:
        tail_n = min(args.tail, total)
        tail = entries[-tail_n:] if tail_n > 0 else []
        pool = entries[: total - tail_n] if tail_n > 0 else entries
        k = min(args.sample, len(pool))
        sample = random.sample(pool, k) + tail

    m_host, m_port = split_addr(args.master)
    r_host, r_port = split_addr(args.replica)
    mconn = RespConn(m_host, m_port, timeout=args.timeout)
    rconn = RespConn(r_host, r_port, timeout=args.timeout)

    checked = 0
    try:
        for seq, value in sample:
            key = f"soak:{seq}"
            for side, conn in (("m", mconn), ("r", rconn)):
                try:
                    got = get_with_retry(conn, key, retries=args.retries, backoff=args.retry_backoff)
                except Exception as e:
                    print(
                        f"SOAK-FAIL seq={seq} side={side} cycle={args.cycle} "
                        f"reason=connect-error expected={value!r} err={e}"
                    )
                    return 1
                if got != value:
                    print(
                        f"SOAK-FAIL seq={seq} side={side} cycle={args.cycle} "
                        f"expected={value!r} got={got!r}"
                    )
                    return 1
            checked += 1
    finally:
        mconn.close()
        rconn.close()

    print(f"SOAK-VERIFY-OK cycle={args.cycle} checked={checked} ledger_total={total}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="mode", required=True)

    w = sub.add_parser("writer", help="continuously SET+WAIT against the master, appending to the acked ledger")
    w.add_argument("--master", required=True, help="host:port")
    w.add_argument("--ledger", required=True, help="path to the acked-write ledger (appended, fsync'd)")
    w.add_argument("--inflight", required=True, help="path to the unacked/in-flight ledger (appended)")
    w.add_argument("--rate", type=float, default=10.0, help="target ops/sec (0 = as fast as possible)")
    w.add_argument("--wait-timeout-ms", type=int, default=3000)
    w.add_argument("--timeout", type=float, default=DEFAULT_SOCK_TIMEOUT, help="socket connect/read timeout (s)")
    w.add_argument("--retry-interval", type=float, default=1.0, help="pause between reconnect attempts when the master is down")
    w.set_defaults(func=mode_writer)

    c = sub.add_parser("catchup", help="poll a replica until its last N acked entries read back correctly")
    c.add_argument("--replica", required=True, help="host:port")
    c.add_argument("--ledger", required=True)
    c.add_argument("--check-last", type=int, default=5)
    c.add_argument("--timeout-sec", type=float, default=90.0)
    c.add_argument("--poll-interval", type=float, default=0.5)
    c.add_argument("--timeout", type=float, default=DEFAULT_SOCK_TIMEOUT, help="per-GET socket timeout (s)")
    c.add_argument("--cycle", default="0")
    c.set_defaults(func=mode_catchup)

    v = sub.add_parser("verify", help="sample (or fully sweep) the acked ledger against master + replica")
    v.add_argument("--master", required=True, help="host:port")
    v.add_argument("--replica", required=True, help="host:port")
    v.add_argument("--ledger", required=True)
    v.add_argument("--sample", type=int, default=1000)
    v.add_argument("--tail", type=int, default=200)
    v.add_argument("--full", action="store_true", help="verify every acked entry (end-of-soak sweep)")
    v.add_argument("--cycle", default="0")
    v.add_argument("--timeout", type=float, default=DEFAULT_SOCK_TIMEOUT)
    v.add_argument("--retries", type=int, default=5, help="connect-error retries per GET before declaring SOAK-FAIL")
    v.add_argument("--retry-backoff", type=float, default=1.0)
    v.set_defaults(func=mode_verify)

    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
