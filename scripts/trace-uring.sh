#!/bin/bash
exec > /tmp/trace-uring-result.txt 2>&1

pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
MPID=$!
sleep 2

# Strace for 5 seconds — capture ALL syscalls on the shard thread
SHARD_TID=$(ls /proc/$MPID/task/ | grep -v $MPID | head -1)
echo "MAIN=$MPID SHARD=$SHARD_TID"

timeout 5 strace -p $SHARD_TID -e io_uring_enter,epoll_wait,read,write,writev,sendto,recvfrom -f 2>/tmp/strace-shard-full.txt &
sleep 1

# Send 1 PING via raw socket
timeout 3 python3 << 'PYEOF'
import socket, time
s = socket.socket()
s.settimeout(2)
s.connect(("127.0.0.1", 6399))
time.sleep(0.1)
s.send(b"*1\r\n$4\r\nPING\r\n")
try:
    data = s.recv(100)
    print(f"GOT: {data!r}")
except Exception as e:
    print(f"ERR: {e}")
s.close()
PYEOF
sleep 3

echo "=== STRACE (first 80 lines) ==="
head -80 /tmp/strace-shard-full.txt

kill -9 $MPID 2>/dev/null
echo DONE
