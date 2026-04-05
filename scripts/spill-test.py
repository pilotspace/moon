#!/usr/bin/env python3
"""Insert keys to trigger eviction + spill-to-disk."""
import socket, time

PORT = 6501
N_KEYS = 2000
VAL_SIZE = 10240  # 10KB per key

sock = socket.socket()
sock.connect(("127.0.0.1", PORT))
sock.settimeout(10)

# PING
sock.sendall(b"*1\r\n$4\r\nPING\r\n")
r = sock.recv(4096)
print(f"PING: {r.strip()}")

# Insert N_KEYS × VAL_SIZE
val = b"X" * VAL_SIZE
sent = 0
for i in range(N_KEYS):
    key = f"k:{i}"
    cmd = f"*3\r\n${3}\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n".encode() + val + b"\r\n"
    sock.sendall(cmd)
    sent += 1
    # Drain every 200 to avoid buffer bloat
    if sent % 200 == 0:
        time.sleep(0.2)
        sock.settimeout(0.3)
        drained = 0
        try:
            while True:
                d = sock.recv(65536)
                drained += len(d)
        except:
            pass
        sock.settimeout(10)
        print(f"  Sent {sent}/{N_KEYS}, drained {drained} bytes")

# Final drain
time.sleep(1)
sock.settimeout(0.5)
try:
    while True:
        sock.recv(65536)
except:
    pass

# Check how many keys exist
sock.settimeout(5)
sock.sendall(b"*1\r\n$4\r\nINFO\r\n")
time.sleep(0.5)
r = b""
sock.settimeout(1)
try:
    while True:
        chunk = sock.recv(8192)
        if not chunk:
            break
        r += chunk
except:
    pass
# Count "keys=" in response
text = r.decode(errors="replace")
for line in text.split("\n"):
    if "keys=" in line or "used_memory" in line or "evicted" in line:
        print(f"  {line.strip()}")

sock.close()
print(f"Done: sent {sent} keys × {VAL_SIZE}B = {sent * VAL_SIZE // 1024 // 1024}MB")
