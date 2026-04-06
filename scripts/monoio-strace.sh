#!/bin/bash
exec > /tmp/monoio-strace-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
MPID=$!
sleep 2
SHARD=$(ls /proc/$MPID/task/ | sort -n | tail -1)
echo "MAIN=$MPID SHARD=$SHARD THREADS=$(ls /proc/$MPID/task/ | wc -l)"

# Strace ALL threads for 4 seconds
timeout 3 redis-cli -p 6399 SET testkey testval &
CLI_PID=$!
sleep 0.5
timeout 3 strace -p $MPID -f -e io_uring_enter,recvfrom,sendto,writev,read,write 2>/tmp/strace-monoio.txt &
sleep 2
wait $CLI_PID 2>/dev/null
echo "CLI_RC=$?"

echo "=== STRACE (first 50 lines) ==="
head -50 /tmp/strace-monoio.txt 2>/dev/null

kill -9 $MPID 2>/dev/null
echo DONE
