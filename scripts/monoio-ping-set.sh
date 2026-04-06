#!/bin/bash
exec > /tmp/monoio-ping-set-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

timeout 10 python3 << 'PYEOF'
import socket

s = socket.socket()
s.settimeout(2)
s.connect(('127.0.0.1', 6399))

# Inline PING
s.send(b'PING\r\n')
print('INLINE PING:', repr(s.recv(100)))

# RESP PING
s.send(b'*1\r\n$4\r\nPING\r\n')
print('RESP PING:', repr(s.recv(100)))

# RESP SET
s.send(b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
try:
    data = s.recv(100)
    print('SET:', repr(data))
except Exception as e:
    print('SET ERROR:', e)

# RESP GET
s.send(b'*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
try:
    data = s.recv(100)
    print('GET:', repr(data))
except Exception as e:
    print('GET ERROR:', e)

s.close()
PYEOF

pkill -9 -f 'target/release/moon'
echo DONE
