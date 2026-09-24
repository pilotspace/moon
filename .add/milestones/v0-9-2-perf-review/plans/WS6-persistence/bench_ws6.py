#!/usr/bin/env python3
"""WS6-persistence evidence harness (moon#1185 #1186 #1187 #1188 #1181).

Interleaved A/B of two moon binaries on this host. Every scenario starts a
fresh server per rep on a fresh data dir, measures, and shuts it down.

  python3 bench_ws6.py <scenario> <binA> <binB> [reps]
  scenarios: rewrite bgsave cdc walrot aof

Relative evidence only (4 vCPU VM, same-host client, virtio disk).
"""
import os, shutil, socket, subprocess, sys, threading, time

PORT = int(os.environ.get("WS6_PORT", "7231"))
ROOT = os.environ.get("WS6_BENCH_DIR", "/home/user/wt/WS6-persistence/.bench/run")


def resp_encode(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        elif isinstance(a, int):
            a = str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(1 << 20)
            if not d:
                raise EOFError
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            d = self.s.recv(1 << 20)
            if not d:
                raise EOFError
            self.buf += d
        v, self.buf = self.buf[:n], self.buf[n + 2:]
        return v

    def reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in (b"+", b"-"):
            return rest if t == b"+" else Exception(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n)
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.reply() for _ in range(n)]
        raise ValueError(line)

    def cmd(self, *args):
        self.s.sendall(resp_encode(*args))
        return self.reply()

    def pipeline(self, cmds):
        self.s.sendall(b"".join(cmds))
        for _ in cmds:
            r = self.reply()
            if isinstance(r, Exception):
                raise r


def start(binary, tag, extra):
    d = os.path.join(ROOT, tag)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    args = [binary, "--port", str(PORT), "--dir", d, "--maxmemory", "0",
            "--disk-offload", "disable", "--auto-aof-rewrite-percentage", "0",
            "--disk-free-min-pct", "0"] + extra
    log = open(os.path.join(d, "server.log"), "wb")
    p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(300):
        try:
            c = Conn(PORT)
            if c.cmd("PING") == b"PONG":
                return p, c, d
        except OSError:
            pass
        time.sleep(0.05)
    p.kill()
    raise RuntimeError("server did not start: " + open(os.path.join(d, "server.log")).read()[-2000:])


def stop(p, c, d):
    try:
        c.s.sendall(resp_encode("SHUTDOWN", "NOSAVE"))
    except OSError:
        pass
    try:
        p.wait(timeout=20)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    shutil.rmtree(d, ignore_errors=True)


def preload(c, n, dsize, prefix="key:"):
    val = b"x" * dsize
    batch = []
    for i in range(n):
        batch.append(resp_encode("SET", "%s%08d" % (prefix, i), val))
        if len(batch) == 20000:
            c.pipeline(batch)
            batch = []
    if batch:
        c.pipeline(batch)


def status(pid):
    out = {}
    for line in open("/proc/%d/status" % pid):
        k, v = line.split(":", 1)
        if k in ("VmHWM", "VmRSS"):
            out[k] = int(v.split()[0]) // 1024  # MiB
    return out


def reset_hwm(pid):
    with open("/proc/%d/clear_refs" % pid, "w") as f:
        f.write("5")


class Prober(threading.Thread):
    """PING loop on its own connection: max / p99 RTT while running."""

    def __init__(self, port):
        super().__init__(daemon=True)
        self.c = Conn(port)
        self.stop_flag = False
        self.rtts = []

    def run(self):
        while not self.stop_flag:
            t = time.perf_counter()
            self.c.cmd("PING")
            self.rtts.append((time.perf_counter() - t) * 1000)
            time.sleep(0.001)

    def finish(self):
        self.stop_flag = True
        self.join()
        r = sorted(self.rtts)
        return {"max_ms": round(r[-1], 1), "p99_ms": round(r[int(len(r) * 0.99)], 2), "n": len(r)}


def info_field(c, section, field):
    raw = c.cmd("INFO", section)
    for line in raw.decode().splitlines():
        if line.startswith(field + ":"):
            return line.split(":", 1)[1].strip()
    return None


def wait_field(c, section, field, want, timeout=600):
    t = time.time()
    while time.time() - t < timeout:
        if info_field(c, section, field) == want:
            return time.time() - t
        time.sleep(0.02)
    raise RuntimeError("timeout waiting for %s=%s" % (field, want))


def sc_rewrite(binary, tag, n=1_500_000, dsize=100, shards=1):
    p, c, d = start(binary, tag, ["--shards", str(shards), "--appendonly", "yes",
                                  "--appendfsync", "everysec", "--save", ""])
    try:
        preload(c, n, dsize)
        time.sleep(1.5)
        before = status(p.pid)
        reset_hwm(p.pid)
        pr = Prober(PORT)
        pr.start()
        time.sleep(0.2)
        c.cmd("BGREWRITEAOF")
        time.sleep(0.05)
        took = wait_field(c, "persistence", "aof_rewrite_in_progress", "0")
        time.sleep(0.2)
        lat = pr.finish()
        after = status(p.pid)
        return {"rss_before_mib": before["VmRSS"], "hwm_delta_mib": after["VmHWM"] - before["VmRSS"],
                "rewrite_s": round(took, 2), **lat}
    finally:
        stop(p, c, d)


def sc_bgsave(binary, tag, n=1_500_000, dsize=100, shards=1):
    p, c, d = start(binary, tag, ["--shards", str(shards), "--appendonly", "no", "--save", ""])
    try:
        preload(c, n, dsize)
        time.sleep(1.5)
        before = status(p.pid)
        reset_hwm(p.pid)
        pr = Prober(PORT)
        pr.start()
        time.sleep(0.2)
        t = time.time()
        c.cmd("BGSAVE")
        time.sleep(0.05)
        wait_field(c, "persistence", "rdb_bgsave_in_progress", "0")
        took = time.time() - t
        time.sleep(0.2)
        lat = pr.finish()
        after = status(p.pid)
        return {"rss_before_mib": before["VmRSS"], "hwm_delta_mib": after["VmHWM"] - before["VmRSS"],
                "bgsave_s": round(took, 2), **lat}
    finally:
        stop(p, c, d)


def biggest_wal(d):
    best = (None, -1, 0)
    for sh in os.listdir(d):
        wal = os.path.join(d, sh, "wal-v3")
        if sh.startswith("shard-") and os.path.isdir(wal):
            files = [f for f in os.listdir(wal) if f.endswith(".wal")]
            size = sum(os.path.getsize(os.path.join(wal, f)) for f in files)
            if size > best[1]:
                best = (wal, size, len(files))
    return best[0], best[2]


def sc_cdc(binary, tag, n=1_000_000):
    # KV records reach the WAL on the SPSC (cross-shard) path: 2 shards.
    p, c, d = start(binary, tag, ["--shards", "2", "--appendonly", "yes", "--appendfsync", "everysec",
                                  "--wal-kv-log", "on", "--save", ""])
    try:
        preload(c, n, 8)
        time.sleep(2.0)
        wal, segs = biggest_wal(d)
        # 4 probe connections: with SO_REUSEPORT at least one lands on the
        # shard serving the CDC.READ connection (whose stall we measure).
        probers = [Prober(PORT) for _ in range(4)]
        for pr in probers:
            pr.start()
        time.sleep(0.2)
        cursor = n + 50  # at/after the tail
        polls = []
        t_end = time.time() + 3.0
        while time.time() < t_end:
            t = time.perf_counter()
            r = c.cmd("CDC.READ", wal, cursor, "LIMIT", 100)
            polls.append((time.perf_counter() - t) * 1000)
            cursor = r[0]
        lats = [pr.finish() for pr in probers]
        lat = {"max_ms": max(l["max_ms"] for l in lats), "p99_ms": max(l["p99_ms"] for l in lats)}
        polls_sorted = sorted(polls)
        return {"records": n, "segments": segs, "polls_in_3s": len(polls),
                "first_poll_ms": round(polls[0], 1), "median_poll_ms": round(polls_sorted[len(polls) // 2], 2),
                **lat}
    finally:
        stop(p, c, d)


def sc_walrot(binary, tag):
    p, c, d = start(binary, tag, ["--shards", "2", "--appendonly", "yes", "--appendfsync", "everysec",
                                  "--wal-kv-log", "on", "--save", ""])
    try:
        pr = Prober(PORT)
        pr.start()
        t = time.time()
        out = subprocess.run(["redis-benchmark", "-p", str(PORT), "-t", "set", "-d", "1024", "-P", "16",
                              "-c", "8", "-n", "400000", "-r", "100000", "-q"],
                             capture_output=True, text=True).stdout
        took = time.time() - t
        lat = pr.finish()
        wal, segs = biggest_wal(d)
        rps = out.replace("\r", "\n").strip().splitlines()[-1]
        return {"segments": segs, "wall_s": round(took, 1), "bench": rps.strip(), **lat}
    finally:
        stop(p, c, d)


def sc_aof(binary, tag, which):
    p, c, d = start(binary, tag, ["--shards", "1", "--appendonly", "yes", "--appendfsync", "everysec",
                                  "--save", ""])
    try:
        if which == "setex":
            cmd = ["SET", "key:__rand_int__", "vvvvvvvvvvvvvvvv", "EX", "100"]
        elif which == "hset":
            cmd = ["HSET", "h:__rand_int__", "f", "vvvvvvvvvvvvvvvv"]
        else:
            cmd = ["SET", "key:__rand_int__", "vvvvvvvvvvvvvvvv"]
        out = subprocess.run(["redis-benchmark", "-p", str(PORT), "-P", "16", "-c", "50", "-n", "1000000",
                              "-r", "1000000", "-q"] + cmd, capture_output=True, text=True).stdout
        import re
        m = re.findall(r"([0-9.]+) requests per second", out.replace("\r", "\n"))
        return {"rps": float(m[-1])}
    finally:
        stop(p, c, d)


def main():
    scen, a, b = sys.argv[1], sys.argv[2], sys.argv[3]
    reps = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    extra = sys.argv[5:] if len(sys.argv) > 5 else []
    for rep in range(reps):
        for label, binary in (("A", a), ("B", b)):
            if scen == "rewrite":
                r = sc_rewrite(binary, "rw", shards=int(extra[0]) if extra else 1)
            elif scen == "bgsave":
                r = sc_bgsave(binary, "bs", shards=int(extra[0]) if extra else 1)
            elif scen == "cdc":
                r = sc_cdc(binary, "cdc", n=int(extra[0]) if extra else 1_000_000)
            elif scen == "walrot":
                r = sc_walrot(binary, "wr")
            elif scen == "aof":
                r = sc_aof(binary, "aof", extra[0] if extra else "setex")
            print(scen, extra, "rep", rep, label, os.path.basename(binary), r, flush=True)


if __name__ == "__main__":
    main()
