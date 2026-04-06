#!/bin/bash
exec > /tmp/multi-client-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

timeout 15 python3 << 'PYEOF'
import socket, threading, time

def client_worker(tid, results):
    s = socket.socket()
    s.settimeout(3)
    try:
        s.connect(('127.0.0.1', 6399))
        # Send CONFIG GET save (what redis-benchmark does)
        s.send(b'*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n')
        resp = b''
        while b'\r\n' not in resp or len(resp) < 5:
            chunk = s.recv(4096)
            if not chunk: break
            resp += chunk
        results[tid] = f'CONFIG: {len(resp)} bytes'

        # Now send SET
        k = f't{tid}'.encode()
        s.send(b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n')
        resp = s.recv(100)
        results[tid] += f', SET: {resp!r}'
    except Exception as e:
        results[tid] = f'ERROR: {e}'
    finally:
        s.close()

# Test 1: 1 client (baseline)
print('=== 1 client ===')
results = {}
t = threading.Thread(target=client_worker, args=(0, results))
t.start(); t.join()
print(results)

# Test 2: 5 clients simultaneous
print('\n=== 5 clients ===')
results = {}
threads = [threading.Thread(target=client_worker, args=(i, results)) for i in range(5)]
for t in threads: t.start()
for t in threads: t.join()
for k, v in sorted(results.items()):
    print(f'  client {k}: {v}')

# Test 3: 10 clients simultaneous
print('\n=== 10 clients ===')
results = {}
threads = [threading.Thread(target=client_worker, args=(i, results)) for i in range(10)]
for t in threads: t.start()
for t in threads: t.join()
ok = sum(1 for v in results.values() if 'SET' in v)
err = sum(1 for v in results.values() if 'ERROR' in v)
print(f'  {ok} OK, {err} ERROR out of {len(results)}')
for k, v in sorted(results.items()):
    if 'ERROR' in v:
        print(f'  client {k}: {v}')

# Test 4: 50 clients simultaneous
print('\n=== 50 clients ===')
results = {}
threads = [threading.Thread(target=client_worker, args=(i, results)) for i in range(50)]
for t in threads: t.start()
for t in threads: t.join()
ok = sum(1 for v in results.values() if 'SET' in v)
err = sum(1 for v in results.values() if 'ERROR' in v)
print(f'  {ok} OK, {err} ERROR out of {len(results)}')
PYEOF

pkill -9 -f 'target/release/moon'
echo DONE
