#!/usr/bin/env python3
import socket, struct, random, math, time

DIM = 384
sock = socket.socket()
sock.connect(("127.0.0.1", 6400))
sock.settimeout(10)

# First verify PING works
sock.sendall(b"*1\r\n$4\r\nPING\r\n")
r = sock.recv(4096)
print(f"PING: {r}")

# Check how many vectors are indexed
sock.sendall(b"*3\r\n$9\r\nFT.SEARCH\r\n$6\r\nminilm\r\n$1\r\n*\r\n")
time.sleep(1)
r = b""
sock.settimeout(2)
try:
    while True:
        chunk = sock.recv(8192)
        if not chunk: break
        r += chunk
except: pass
print(f"FT.SEARCH *: {r[:300]}")

# Try KNN query
random.seed(1000000)
v = [random.gauss(0,1) for _ in range(DIM)]
norm = math.sqrt(sum(x*x for x in v))
v = [x/norm for x in v]
blob = struct.pack(f"{DIM}f", *v)

query = "*=>[KNN 10 @emb $BLOB AS score]"
args = ["FT.SEARCH", "minilm", query, "PARAMS", "2", "BLOB", blob, "DIALECT", "2"]
parts = [f"*{len(args)}\r\n".encode()]
for a in args:
    if isinstance(a, bytes):
        parts.append(f"${len(a)}\r\n".encode() + a + b"\r\n")
    else:
        s = str(a)
        parts.append(f"${len(s)}\r\n{s}\r\n".encode())
cmd = b"".join(parts)

sock.settimeout(10)
sock.sendall(cmd)
time.sleep(2)

r = b""
sock.settimeout(3)
try:
    while True:
        chunk = sock.recv(16384)
        if not chunk: break
        r += chunk
except: pass
print(f"\nKNN Response ({len(r)} bytes): {r[:500]}")
sock.close()
