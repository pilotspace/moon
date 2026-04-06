#!/bin/bash
exec > /tmp/monoio-debug-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

timeout 15 python3 << 'PYEOF'
import socket, time

# Test 1: Two commands on SAME connection (RESP format)
print('=== Test 1: Two SETs on same connection ===')
s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
s.send(b'*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n')
try:
    r = s.recv(100)
    print(f'SET 1: {r!r}')
except Exception as e:
    print(f'SET 1 ERROR: {e}')
    s.close()
    exit()

s.send(b'*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$4\r\nval2\r\n')
try:
    r = s.recv(100)
    print(f'SET 2: {r!r}')
except Exception as e:
    print(f'SET 2 ERROR: {e}')
s.close()

# Test 2: Pipeline 2 commands at once
print('')
print('=== Test 2: Pipeline 2 SETs ===')
s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
s.send(b'*3\r\n$3\r\nSET\r\n$4\r\nkey3\r\n$4\r\nval3\r\n'
       b'*3\r\n$3\r\nSET\r\n$4\r\nkey4\r\n$4\r\nval4\r\n')
try:
    r = s.recv(100)
    print(f'Pipeline 2: {r!r} ({r.count(b"+OK")} OKs)')
except Exception as e:
    print(f'Pipeline 2 ERROR: {e}')
s.close()

# Test 3: Inline PING then RESP SET on same connection
print('')
print('=== Test 3: Inline PING then SET ===')
s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
s.send(b'PING\r\n')
try:
    r = s.recv(100)
    print(f'PING: {r!r}')
except Exception as e:
    print(f'PING ERROR: {e}')
    s.close()
    exit()

s.send(b'*3\r\n$3\r\nSET\r\n$4\r\nkey5\r\n$4\r\nval5\r\n')
try:
    r = s.recv(100)
    print(f'SET after PING: {r!r}')
except Exception as e:
    print(f'SET after PING ERROR: {e}')
s.close()

# Test 4: CONFIG GET (what redis-benchmark sends first)
print('')
print('=== Test 4: CONFIG GET save ===')
s = socket.socket(); s.settimeout(3); s.connect(('127.0.0.1', 6399))
s.send(b'*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n')
try:
    r = s.recv(500)
    print(f'CONFIG GET: {r!r}')
except Exception as e:
    print(f'CONFIG GET ERROR: {e}')
s.close()
PYEOF

pkill -9 -f 'target/release/moon'
echo DONE
