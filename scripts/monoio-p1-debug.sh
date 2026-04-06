#!/bin/bash
exec > /tmp/monoio-p1-debug.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

# Test 1: redis-benchmark c=1 p=1 n=10 with 30s timeout
echo '=== redis-benchmark c=1 p=1 n=10 ==='
timeout 30 taskset -c 4-7 redis-benchmark -p 6399 -c 1 -n 10 -P 1 -t set -d 64 -q --csv 2>&1
echo "RC=$?"

# Test 2: python direct SET/GET/SET/GET on same connection
echo ''
echo '=== Python multi-command same connection ==='
timeout 10 python3 << 'PYEOF'
import socket, time
s = socket.socket(); s.settimeout(5); s.connect(('127.0.0.1', 6399))
for i in range(5):
    k = f'py{i}'.encode()
    s.send(b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n')
    try:
        r = s.recv(100)
        print(f'SET {i}: {r!r}')
    except Exception as e:
        print(f'SET {i} ERROR: {e}')
        break
s.close()
PYEOF

# Test 3: pipeline 16 commands in one send
echo ''
echo '=== Python pipeline 16 ==='
timeout 10 python3 << 'PYEOF'
import socket
s = socket.socket(); s.settimeout(5); s.connect(('127.0.0.1', 6399))
batch = b''
for i in range(16):
    k = f'pp{i}'.encode()
    batch += b'*3\r\n$3\r\nSET\r\n$' + str(len(k)).encode() + b'\r\n' + k + b'\r\n$5\r\nvalue\r\n'
s.send(batch)
resp = b''
while resp.count(b'\r\n') < 16:
    try:
        chunk = s.recv(4096)
        if not chunk: break
        resp += chunk
    except: break
print(f'Pipeline 16: {resp.count(b"+OK")} OKs in {len(resp)} bytes')
s.close()
PYEOF

pkill -9 -f 'target/release/moon'
echo DONE
