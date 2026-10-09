"""Latency of a spanning MSET (keys on all 4 shards) vs a single-shard MSET under a policy."""
import os, subprocess, sys, tempfile, time, statistics
sys.path.insert(0, os.path.dirname(__file__))
from resp import Conn, wait_up
from xxh import shard

binary, port, policy = sys.argv[1], int(sys.argv[2]), sys.argv[3]
N = int(sys.argv[4]) if len(sys.argv) > 4 else 300
d = tempfile.mkdtemp(prefix="lat-", dir=os.path.dirname(os.path.abspath(__file__)))
env = dict(os.environ, MOON_DISK_FREE_MIN_PCT="0")
p = subprocess.Popen([binary, "--port", str(port), "--shards", "4", "--appendonly", "yes",
                      "--appendfsync", policy, "--dir", d, "--disk-free-min-pct", "0"],
                     env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    c = wait_up(port, 30)
    per = {}
    for i in range(10000):
        k = "s%d" % i
        per.setdefault(shard(k, 4), []).append(k)
    span = [per[s][0] for s in range(4)]
    same = ["{t}a", "{t}b", "{t}c", "{t}d"]
    res = {}
    for name, keys in (("span4", span), ("same", same), ("span4", span), ("same", same)):
        lat = []
        for i in range(N):
            args = ["MSET"]
            for k in keys:
                args += [k, i]
            t = time.perf_counter()
            r = c.send(*args)
            lat.append((time.perf_counter() - t) * 1e6)
            assert r == "+OK", r
        res.setdefault(name, []).extend(lat)
    for name, lat in res.items():
        lat.sort()
        print("%s %s p50=%.0fus p90=%.0fus mean=%.0fus" % (policy, name, lat[len(lat) // 2], lat[int(len(lat) * .9)], statistics.mean(lat)))
finally:
    p.kill(); p.wait()
    subprocess.run(["rm", "-rf", d])
