#!/usr/bin/env python3
import socket, struct, random, math, time

DIM = 384
sock = socket.socket()
sock.connect(("127.0.0.1", 6400))
sock.settimeout(5)

# Generate query vector
random.seed(1000000)
v = [random.gauss(0,1) for _ in range(DIM)]
norm = math.sqrt(sum(x*x for x in v))
v = [x/norm for x in v]
blob = struct.pack(f"{DIM}f", *v)

# Build RESP command manually
query = "*=>[KNN 10 @emb $BLOB AS score]"
parts = []
args = ["FT.SEARCH", "minilm", query, "PARAMS", "2", "BLOB", blob, "DIALECT", "2"]
parts.append(f"*{len(args)}\r\n".encode())
for a in args:
    if isinstance(a, bytes):
        parts.append(f"${len(a)}\r\n".encode())
        parts.append(a)
        parts.append(b"\r\n")
    else:
        s = str(a)
        parts.append(f"${len(s)}\r\n{s}\r\n".encode())

cmd = b"".join(parts)
print(f"Command length: {len(cmd)} bytes")
print(f"First 200 bytes: {cmd[:200]}")
sock.sendall(cmd)

# Read response
time.sleep(1)
data = b""
sock.settimeout(2)
try:
    while True:
        chunk = sock.recv(8192)
        if not chunk:
            break
        data += chunk
except:
    pass

print(f"\nResponse length: {len(data)} bytes")
print(f"Response: {data[:1000]}")
sock.close()
