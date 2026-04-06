#!/bin/bash
exec > /tmp/monoio-direct-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

timeout 10 python3 << 'PYEOF'
import socket, time

# Test 1: single SET/GET
s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
s.sendall(b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
print(f'SET: {s.recv(100)!r}')
s.sendall(b'*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
print(f'GET: {s.recv(100)!r}')
s.close()

# Test 2: pipeline 10 SETs
s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
batch = b''
for i in range(10):
    k = f'k{i}'.encode()
    batch += b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n'
s.sendall(batch)
resp = b''
while resp.count(b'\r\n') < 10:
    try: resp += s.recv(4096)
    except: break
print(f'PIPELINE 10: {resp.count(b"+OK")} OKs in {len(resp)} bytes')
s.close()

# Test 3: throughput (5 connections, 200 ops each)
t0 = time.time()
ops = 0
for c in range(5):
    s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
    for batch_num in range(20):
        batch = b''
        for i in range(10):
            k = f'k{c}_{batch_num}_{i}'.encode()
            batch += b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n'
        s.sendall(batch)
        resp = b''
        while resp.count(b'\r\n') < 10:
            try: resp += s.recv(4096)
            except: break
        ops += 10
    s.close()
t1 = time.time()
print(f'THROUGHPUT: {ops} ops in {t1-t0:.2f}s = {ops/(t1-t0):.0f} ops/s')

# Test 4: concurrent connections throughput
import threading
results = []
def worker(wid):
    total = 0
    s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
    for batch_num in range(100):
        batch = b''
        for i in range(16):
            k = f'w{wid}_{batch_num}_{i}'.encode()
            batch += b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n'
        s.sendall(batch)
        resp = b''
        while resp.count(b'\r\n') < 16:
            try:
                chunk = s.recv(8192)
                if not chunk: break
                resp += chunk
            except: break
        total += 16
    s.close()
    results.append(total)

t0 = time.time()
threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
for t in threads: t.start()
for t in threads: t.join()
t1 = time.time()
total_ops = sum(results)
print(f'CONCURRENT 10x1600: {total_ops} ops in {t1-t0:.2f}s = {total_ops/(t1-t0):.0f} ops/s')
PYEOF

pkill -9 -f 'target/release/moon'
echo DONE
