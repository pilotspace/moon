#!/usr/bin/env python3
"""moon#719 acceptance: does a Cypher WRITE narrow through the property index?

Reproduces the discriminator from the issue verbatim -- the same graph, the
same connection, the same predicate, differing only in whether the statement
writes:

    MATCH (n:Probe {_key: '<k>'}) SET n.id = '<hex>' RETURN n     <- subject
    MATCH (n:Probe {_key: '<k>'})                    RETURN n     <- control

and reports each statement's cost at an EARLY window (few nodes present) and a
LATE window (many nodes present). A write that narrows grows like the read;
a write that degrades to a full label scan grows with the node count.

The harness REFUSES to report rather than print a believable wrong number:

  R1  the two binaries must differ by sha256 (a shared CARGO_TARGET_DIR
      silently yields one binary and a ~0% delta -- the most plausible
      wrong answer this measurement can produce)
  R2  the server answering must actually be moon (a stray redis-server on
      the port will happily PONG and return errors that parse as "fast")
  R3  the CONTROL binary must exhibit the defect (write growth clearly
      above read growth). If it does not, the harness is not exercising
      the bug and every number below it is meaningless
  R4  both binaries must return identical row counts for every batch --
      a "faster" plan that stopped matching is not a fix

Usage:  bench-cypher-write-index-719.py <control-moon> <fix-moon>
"""

import hashlib
import os
import socket
import subprocess
import sys
import tempfile
import time

EARLY, LATE, BATCH = 1_000, 10_000, 50
PORT = 7719


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Conn:
    """Minimal RESP client. One socket for the whole run: a fresh connection
    per probe would add ~100ms of spawn/page-fault noise per measurement."""

    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.s.recv(65536)
            if not chunk:
                raise IOError("server closed the connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read(self):
        line = self._line()
        tag, rest = line[:1], line[1:]
        if tag in (b"+", b":", b","):
            return rest
        if tag == b"-":
            raise RuntimeError(rest.decode(errors="replace"))
        if tag == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(65536)
            out, self.buf = self.buf[:n], self.buf[n + 2:]
            return out
        if tag in (b"*", b"~", b">"):
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        if tag == b"%":
            n = int(rest)
            return [(self._read(), self._read()) for _ in range(n)]
        if tag == b"_":
            return None
        if tag == b"#":
            return rest == b"t"
        raise RuntimeError(f"unparsed reply tag {tag!r} ({line[:64]!r})")

    def cmd(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a.encode() if isinstance(a, str) else a
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.s.sendall(b"".join(out))
        return self._read()

    def pipeline(self, cmds):
        """Send every command, then drain every reply. Draining must be
        byte-exact: an O(n^2) accumulate-and-count drain makes the CLIENT the
        bottleneck and flattens the very growth being measured."""
        out = []
        for args in cmds:
            out.append(b"*%d\r\n" % len(args))
            for a in args:
                a = a.encode() if isinstance(a, str) else a
                out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.s.sendall(b"".join(out))
        return [self._read() for _ in cmds]


def rows_of(reply):
    """GRAPH.QUERY returns [header, rows, stats]; count the data rows."""
    if isinstance(reply, list) and len(reply) >= 2 and isinstance(reply[1], list):
        return len(reply[1])
    return 0


def spawn(binary, port):
    d = tempfile.mkdtemp(prefix="moon719-")
    p = subprocess.Popen(
        [binary, "--port", str(port), "--shards", "1", "--dir", d,
         "--appendonly", "no", "--disk-free-min-pct", "0"],
        stdout=open(os.path.join(d, "out.log"), "w"),
        stderr=subprocess.STDOUT,
    )
    for _ in range(600):
        if p.poll() is not None:
            raise RuntimeError(f"{binary} exited rc={p.returncode}; see {d}/out.log")
        try:
            c = Conn(port)
            if c.cmd("PING") == b"PONG":
                # R2: prove it is moon, not a foreign listener that also PONGs.
                info = c.cmd("INFO", "server") or b""
                if b"moon" not in info.lower():
                    raise RuntimeError(
                        f"R2 REFUSED: something on :{port} answers PING but is not moon"
                    )
                return p, c, d
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    raise RuntimeError(f"{binary} never became ready on :{port}")


def seed(c, upto, have):
    """Grow the graph to `upto` nodes. Pure CREATE -- no scan operator, so
    seeding cost is not part of what is being measured."""
    step = 500
    for lo in range(have, upto, step):
        hi = min(lo + step, upto)
        c.pipeline([
            ("GRAPH.QUERY", "g", "CREATE (n:Probe {_key: 'k%d'})" % i)
            for i in range(lo, hi)
        ])
    return upto


def timed(c, stmt_of, present):
    """Median-of-5 wall time for one BATCH of statements over distinct keys."""
    samples, counts = [], set()
    for r in range(5):
        base = (present // 2 + r * BATCH) % max(present, 1)
        cmds = [("GRAPH.QUERY", "g", stmt_of((base + i) % present)) for i in range(BATCH)]
        t0 = time.perf_counter()
        replies = c.pipeline(cmds)
        samples.append((time.perf_counter() - t0) * 1000.0)
        counts.add(sum(rows_of(x) for x in replies))
    samples.sort()
    return samples[len(samples) // 2], counts


WRITE = lambda k: ("MATCH (n:Probe {_key: 'k%d'}) SET n.id = '%032x' RETURN n" % (k, k))
READ = lambda k: ("MATCH (n:Probe {_key: 'k%d'}) RETURN n" % k)


def measure(binary, label):
    proc, c, d = spawn(binary, PORT)
    try:
        c.cmd("GRAPH.CREATE", "g")
        have = seed(c, EARLY, 0)
        w_early, wc_e = timed(c, WRITE, have)
        r_early, rc_e = timed(c, READ, have)
        have = seed(c, LATE, have)
        w_late, wc_l = timed(c, WRITE, have)
        r_late, rc_l = timed(c, READ, have)
        print(f"  {label:<8} write {w_early:7.2f} -> {w_late:7.2f} ms "
              f"({w_late / w_early:5.2f}x)   read {r_early:7.2f} -> {r_late:7.2f} ms "
              f"({r_late / r_early:5.2f}x)")
        return {
            "write_growth": w_late / w_early, "read_growth": r_late / r_early,
            "w_early": w_early, "w_late": w_late, "r_early": r_early, "r_late": r_late,
            "counts": (sorted(wc_e), sorted(rc_e), sorted(wc_l), sorted(rc_l)),
        }
    finally:
        proc.kill()
        proc.wait()


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    ctl_bin, fix_bin = sys.argv[1], sys.argv[2]

    ctl_sha, fix_sha = sha256(ctl_bin), sha256(fix_bin)
    print(f"control {ctl_sha[:16]}  {ctl_bin}")
    print(f"fix     {fix_sha[:16]}  {fix_bin}")
    if ctl_sha == fix_sha:
        sys.exit("R1 REFUSED: the two binaries are byte-identical -- "
                 "a shared CARGO_TARGET_DIR built one binary twice")

    print(f"\n{BATCH} statements, {EARLY} nodes present -> {LATE} nodes present, "
          f"median of 5:")
    # Interleave the legs so a drifting machine cannot masquerade as a win.
    ctl = measure(ctl_bin, "control")
    fix = measure(fix_bin, "fix")
    ctl2 = measure(ctl_bin, "control2")
    fix2 = measure(fix_bin, "fix2")

    if ctl["counts"] != fix["counts"]:
        sys.exit(f"\nR4 REFUSED: the two binaries disagree on matched rows -- "
                 f"control {ctl['counts']} vs fix {fix['counts']}")

    ctl_g = (ctl["write_growth"] + ctl2["write_growth"]) / 2
    ctl_r = (ctl["read_growth"] + ctl2["read_growth"]) / 2
    fix_g = (fix["write_growth"] + fix2["write_growth"]) / 2
    fix_r = (fix["read_growth"] + fix2["read_growth"]) / 2

    print(f"\n  mean write growth  control {ctl_g:5.2f}x   fix {fix_g:5.2f}x")
    print(f"  mean read growth   control {ctl_r:5.2f}x   fix {fix_r:5.2f}x")

    if ctl_g < ctl_r * 2:
        sys.exit(f"\nR3 REFUSED: the control binary does not exhibit the defect "
                 f"(write growth {ctl_g:.2f}x vs read growth {ctl_r:.2f}x). "
                 f"This harness is not exercising moon#719; ignore every number above.")

    late_speedup = ((ctl["w_late"] + ctl2["w_late"]) /
                    (fix["w_late"] + fix2["w_late"]))
    print(f"\n  VERDICT: write growth {ctl_g:.2f}x -> {fix_g:.2f}x; "
          f"the {LATE}-node batch is {late_speedup:.2f}x faster.")
    print("  Rows matched are identical in both binaries (R4).")


if __name__ == "__main__":
    main()
