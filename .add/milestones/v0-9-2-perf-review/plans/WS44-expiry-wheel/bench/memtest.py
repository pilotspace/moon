#!/usr/bin/env python3
"""ws44 memory probe. usage: memtest.py BIN PORT CASE N [ENV=VAL ...]
CASE: notl | ttl | notl_long | ttl_long | ttl_sparse | ttl_sparse_long | ttl_mixed
Starts a fresh --shards 1 server, loads N keys by pipelined SET, prints INFO deltas."""
import os, socket, subprocess, sys, time, random, shutil, tempfile

bin_, port, case, n = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
extra_env = dict(a.split("=", 1) for a in sys.argv[5:])
d = tempfile.mkdtemp(prefix="ws44mem", dir="/tmp" if os.path.isdir("/home/user/wt/lane-c/.bench") else None)
env = dict(os.environ, MOON_DISK_FREE_MIN_PCT="0", **extra_env)
p = subprocess.Popen([bin_, "--port", str(port), "--dir", d, "--shards", "1", "--appendonly", "no",
                      "--save", "", "--disk-offload", "disable", "--maxmemory", "0"],
                     env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def conn():
    for _ in range(100):
        try:
            s = socket.create_connection(("127.0.0.1", port)); return s
        except OSError: time.sleep(0.1)
    raise SystemExit("no server")
s = conn(); f = s.makefile("rb")
def cmd(*a):
    b = b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
    s.sendall(b); return f.readline()
def info():
    s.sendall(b"*2\r\n$4\r\nINFO\r\n$6\r\nmemory\r\n")
    l = f.readline(); ln = int(l[1:]); body = f.read(ln + 2).decode()
    return dict(x.split(":", 1) for x in body.splitlines() if ":" in x)
def stat(): 
    time.sleep(1.5); i = info()
    return {k: int(i[k]) for k in ("used_memory", "used_memory_rss", "allocator_allocated", "allocator_active", "allocator_resident") if k in i}
rnd = random.Random(7)
long_ = case.endswith("long")
def key(i): return (b"user:session:token:%010d" % i) if long_ else (b"k:%07d" % i)
ttl = case.startswith("ttl")
sparse = "sparse" in case
base = stat()
batch = []
BATCH = 500
def flush():
    global batch
    if not batch: return
    s.sendall(b"".join(batch)); 
    for _ in range(len(batch)): f.readline()
    batch = []
for i in range(n):
    a = [b"SET", key(i), b"v%07d" % i]
    if ttl:
        if sparse: secs = rnd.randint(86400, 30 * 86400)
        else: secs = 3600
        a += [b"EX", str(secs).encode()]
    batch.append(b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a))
    if len(batch) >= BATCH: flush()
flush()
after = stat()
dbsize = int(cmd(b"DBSIZE")[1:])
out = {k: (after[k] - base[k]) / n for k in after}
print(f"{case} N={n} dbsize={dbsize} " + " ".join(f"{k}/key={v:.1f}" for k, v in out.items()))
s.close(); p.terminate(); p.wait(); shutil.rmtree(d, ignore_errors=True)
