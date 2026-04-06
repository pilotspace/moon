#!/bin/bash
exec > /tmp/monoio-pybench-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

timeout 30 python3 << 'PYEOF'
import socket, time, threading

def bench_thread(tid, batches, pipeline):
    s = socket.socket()
    s.settimeout(5)
    s.connect(('127.0.0.1', 6399))
    ops = 0
    for _ in range(batches):
        batch = b''
        for i in range(pipeline):
            k = f'k{tid}_{ops+i}'.encode()
            batch += b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n'
        s.sendall(batch)
        resp = b''
        while resp.count(b'\r\n') < pipeline:
            try:
                chunk = s.recv(16384)
                if not chunk: break
                resp += chunk
            except: break
        ops += pipeline
    s.close()
    return ops

# Single thread, varying pipeline
for p in [1, 8, 16, 64]:
    batches = max(100, 10000 // p)
    t0 = time.time()
    ops = bench_thread(0, batches, p)
    t1 = time.time()
    print(f'1 conn p={p}: {ops/(t1-t0):.0f} SET/s ({ops} ops in {t1-t0:.2f}s)')

# Multi-threaded: 10 connections, p=16
print('')
results = []
def worker(tid):
    ops = bench_thread(tid, 500, 16)
    results.append(ops)

t0 = time.time()
threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
for t in threads: t.start()
for t in threads: t.join()
t1 = time.time()
total = sum(results)
print(f'10 conns p=16: {total/(t1-t0):.0f} SET/s ({total} ops in {t1-t0:.2f}s)')

# 50 connections, p=64
results = []
t0 = time.time()
threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
for t in threads: t.start()
for t in threads: t.join()
t1 = time.time()
total = sum(results)
print(f'50 conns p=16: {total/(t1-t0):.0f} SET/s ({total} ops in {t1-t0:.2f}s)')
PYEOF

pkill -9 -f 'target/release/moon'
echo DONE
