#!/usr/bin/env python3
"""ws44 drain-rate probe (moon#1288 shape): load N keys PX 3000, SIGSTOP the
server until all are past their deadline, SIGCONT, time DBSIZE -> 1.
usage: drain.py BIN PORT N [ENV=VAL ...] ; prints keys/s."""
import os, signal, socket, subprocess, sys, tempfile, time, shutil
bin_, port, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
extra = dict(a.split("=", 1) for a in sys.argv[4:])
d = tempfile.mkdtemp(prefix="ws44drain")
p = subprocess.Popen([bin_, "--port", str(port), "--dir", d, "--shards", "1", "--appendonly", "no", "--save", "",
                      "--disk-offload", "disable", "--maxmemory", "0"],
                     env=dict(os.environ, MOON_DISK_FREE_MIN_PCT="0", **extra), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for _ in range(100):
    try: s = socket.create_connection(("127.0.0.1", port)); break
    except OSError: time.sleep(0.1)
f = s.makefile("rb")
def cmd(*a):
    s.sendall(b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)); return f.readline()
def dbsize(): return int(cmd(b"DBSIZE")[1:])
cmd(b"SET", b"live", b"1")
ttl_ms = int(os.environ.get("TTL_MS", max(3000, n // 40)))
t_load = time.time()
B = []
for i in range(n):
    a = [b"SET", b"k:%d" % i, b"v", b"PX", str(ttl_ms).encode()]
    B.append(b"*5\r\n" + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a))
    if len(B) == 1000:
        s.sendall(b"".join(B)); [f.readline() for _ in B]; B = []
if B: s.sendall(b"".join(B)); [f.readline() for _ in B]
load = time.time() - t_load
assert load < ttl_ms / 1000 - 0.5, f"load {load:.1f}s too slow for ttl {ttl_ms}ms"
os.kill(p.pid, signal.SIGSTOP)
time.sleep(ttl_ms / 1000 - load + 0.5)
os.kill(p.pid, signal.SIGCONT)
def ticks():
    x=open(f'/proc/{p.pid}/stat').read().rsplit(')',1)[1].split()
    return int(x[11])+int(x[12])
start = dbsize(); c0=ticks(); t0 = time.time(); size = start
while size > 1 and time.time() - t0 < 120:
    time.sleep(0.005); size = dbsize()
took = time.time() - t0
cpu_us = (ticks()-c0)*10000/max(1,start-size)
print(f"drain N={n} start={start} took={took:.3f}s rate={(start - size) / took:,.0f} keys/s cpu_us_per_key={cpu_us:.2f} left={size} wheel={extra.get('MOON_EXPIRY_WHEEL','0')}")
s.close(); p.terminate(); p.wait(); shutil.rmtree(d, ignore_errors=True)
