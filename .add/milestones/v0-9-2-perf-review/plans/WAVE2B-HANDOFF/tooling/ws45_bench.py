#!/usr/bin/env python3
"""moon#1295 harness: a big collection, a BGSAVE held mid-walk, one in-place
write (HSET of one new field by default). Reports the write's latency, the max
PING gap on a second connection and the RSS peak during the write window, then
releases the save, kill -9s, restarts from the snapshot and checks the size.

Usage: bench.py BIN PORT [--fields N] [--kind hash|list|set|zset] [--no-uring]
Prints one JSON line."""
import json, os, signal, socket, subprocess, sys, tempfile, threading, time

BIN, PORT = sys.argv[1], int(sys.argv[2])
FIELDS = 5_000_000
KIND = "hash"
for i, a in enumerate(sys.argv):
    if a == "--fields":
        FIELDS = int(sys.argv[i + 1])
    if a == "--kind":
        KIND = sys.argv[i + 1]
NO_URING = "--no-uring" in sys.argv
SCRATCH = os.path.join(os.environ.get("SCRATCH", "/tmp/moon-work"), "ws45/runs")
os.makedirs(SCRATCH, exist_ok=True)


def enc(*parts):
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
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

    def reply(self):
        line = self._line()
        t = line[:1]
        if t in (b"+", b"-", b":"):
            return line
        if t == b"$":
            n = int(line[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(1 << 20)
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if t == b"*":
            n = int(line[1:])
            return None if n < 0 else [self.reply() for _ in range(n)]
        raise ValueError(line)

    def cmd(self, *parts):
        self.s.sendall(enc(*parts))
        return self.reply()


def spawn(port, d, env):
    return subprocess.Popen(
        [BIN, "--port", str(port), "--shards", "1", "--appendonly", "no", "--save", "",
         "--maxmemory", "0", "--disk-offload", "disable", "--dir", d],
        stdout=subprocess.DEVNULL, stderr=open(os.path.join(d, "stderr"), "a"), env=env)


def connect(port):
    for _ in range(600):
        try:
            c = Conn(port)
            if c.cmd("PING") == b"+PONG":
                return c
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("server did not come up")


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def info(c, field):
    r = c.cmd("INFO", "persistence").decode()
    for line in r.split("\r\n"):
        if line.startswith(field + ":"):
            return line.split(":", 1)[1]
    return None


def load(c):
    per = 1000
    batch = []
    sent = 0
    i = 0
    while i < FIELDS:
        n = min(per, FIELDS - i)
        if KIND == "hash":
            parts = ["HSET", "big"]
            for j in range(i, i + n):
                parts += [f"f{j}", f"v{j}"]
        elif KIND == "list":
            parts = ["RPUSH", "big"] + [f"e{j}" for j in range(i, i + n)]
        elif KIND == "set":
            parts = ["SADD", "big"] + [f"m{j}" for j in range(i, i + n)]
        else:
            parts = ["ZADD", "big"]
            for j in range(i, i + n):
                parts += [str(j), f"m{j}"]
        batch.append(enc(*parts))
        i += n
        if len(batch) == 50 or i >= FIELDS:
            c.s.sendall(b"".join(batch))
            for _ in batch:
                r = c.reply()
                assert not (isinstance(r, bytes) and r.startswith(b"-")), r
            batch = []
    # filler keys
    for b in range(20):
        c.s.sendall(b"".join(enc("SET", f"fill:{b}:{k}", "x" * 16) for k in range(1000)))
        for _ in range(1000):
            c.reply()


def size_cmd():
    return {"hash": "HLEN", "list": "LLEN", "set": "SCARD", "zset": "ZCARD"}[KIND]


def write_cmd():
    return {"hash": ("HSET", "big", "newfield", "x"), "list": ("LPUSH", "big", "newelem"),
            "set": ("SADD", "big", "newmember"), "zset": ("ZADD", "big", "-1", "newmember")}[KIND]


env = dict(os.environ)
env["MOON_DISK_FREE_MIN_PCT"] = "0"
if NO_URING:
    env["MOON_NO_URING"] = "1"
d = tempfile.mkdtemp(prefix="ws45-", dir=SCRATCH)
hold = os.path.join(d, "snapshot.hold")
env["MOON_TEST_SNAPSHOT_HOLD_FILE"] = hold
out = {"bin": os.path.basename(BIN), "kind": KIND, "fields": FIELDS}
p = spawn(PORT, d, env)
p2 = None
try:
    c = connect(PORT)
    t0 = time.time()
    load(c)
    out["load_s"] = round(time.time() - t0, 2)
    assert int(c.cmd(size_cmd(), "big")[1:]) == FIELDS
    open(hold, "w").close()
    assert c.cmd("BGSAVE").startswith(b"+")
    tmp = os.path.join(d, "shard-0.rrdshard.tmp")
    for _ in range(400):
        if os.path.exists(tmp) or info(c, "rdb_bgsave_in_progress") == "1":
            break
        time.sleep(0.01)
    time.sleep(0.2)
    # PING loop + RSS sampler
    stop = threading.Event()
    gaps = []
    rss = []
    def pinger():
        pc = Conn(PORT)
        last = time.perf_counter()
        while not stop.is_set():
            pc.cmd("PING")
            now = time.perf_counter()
            gaps.append((now, now - last))
            last = now
    def sampler():
        while not stop.is_set():
            rss.append((time.perf_counter(), rss_kb(p.pid)))
            time.sleep(0.002)
    th = [threading.Thread(target=pinger), threading.Thread(target=sampler)]
    for t in th:
        t.start()
    time.sleep(2.0)
    rss_before = rss_kb(p.pid)
    w0 = time.perf_counter()
    r = c.cmd(*write_cmd())
    w1 = time.perf_counter()
    time.sleep(0.5)
    # release the save, pinging on (the walk's own stall on big values)
    r0 = time.perf_counter()
    os.remove(hold)
    for _ in range(6000):
        if info(c, "rdb_bgsave_in_progress") == "0":
            break
        time.sleep(0.01)
    r1 = time.perf_counter()
    out["release_to_done_s"] = round(r1 - r0, 2)
    time.sleep(0.2)
    stop.set()
    for t in th:
        t.join()
    rel = [g for (t, g) in gaps if r0 <= t <= r1 + 0.2]
    out["release_max_gap_ms"] = round(max(rel) * 1000, 1) if rel else None
    out["write_reply"] = r.decode() if isinstance(r, bytes) else str(r)
    out["write_ms"] = round((w1 - w0) * 1000, 1)
    window = [g for (t, g) in gaps if w0 - 0.05 <= t <= w1 + 0.5]
    out["max_ping_gap_ms"] = round(max(window) * 1000, 1) if window else None
    out["pings_in_window"] = len(window)
    srt = sorted(window)
    out["gap_p50_ms"] = round(srt[len(srt)//2] * 1000, 2) if srt else None
    out["gap_p99_ms"] = round(srt[int(len(srt)*0.99)] * 1000, 2) if srt else None
    out["top5_gaps_ms"] = [round(g * 1000, 1) for g in srt[-5:]]
    out["gaps_over_20ms"] = sum(1 for g in srt if g > 0.02)
    base = sorted(g for (t, g) in gaps if t < w0 - 0.05)
    out["baseline_pings"] = len(base)
    out["baseline_max_ms"] = round(base[-1] * 1000, 1) if base else None
    out["baseline_over_20ms"] = sum(1 for g in base if g > 0.02)
    out["rss_before_mb"] = round(rss_before / 1024, 1)
    peak = max([v for (t, v) in rss if w0 - 0.05 <= t <= w1 + 0.5] + [rss_before])
    out["rss_peak_mb"] = round(peak / 1024, 1)
    out["rss_spike_mb"] = round((peak - rss_before) / 1024, 1)
    out["bgsave_status"] = info(c, "rdb_last_bgsave_status")
    p.send_signal(signal.SIGKILL)
    p.wait()
    port2 = PORT + 1
    p2 = spawn(port2, d, env)
    c2 = connect(port2)
    for _ in range(600):
        n = c2.cmd(size_cmd(), "big")
        if n is not None and n != b":0":
            break
        time.sleep(0.05)
    out["size_after_restart"] = int(n[1:])
    if KIND == "hash":
        out["newfield_after_restart"] = int(c2.cmd("HEXISTS", "big", "newfield")[1:])
        out["f12345"] = (c2.cmd("HGET", "big", "f12345") or b"").decode()
    out["ok"] = out["size_after_restart"] == FIELDS and out["bgsave_status"] == "ok"
finally:
    for proc in (p, p2):
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            proc.wait()
    subprocess.run(["rm", "-rf", d])
print(json.dumps(out), flush=True)
