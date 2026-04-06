#!/bin/bash
exec > /tmp/strace-sync-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
MPID=$!
sleep 2
SHARD_TID=$(ls /proc/$MPID/task/ | grep -v $MPID | head -1)
echo "MAIN=$MPID SHARD=$SHARD_TID"

timeout 4 strace -p $SHARD_TID -e io_uring_enter 2>/tmp/strace-enter.txt &
sleep 1

timeout 2 redis-cli -p 6399 PING
echo "PING_RC=$?"
sleep 2

echo "=== io_uring_enter calls ==="
head -30 /tmp/strace-enter.txt
kill -9 $MPID 2>/dev/null
echo DONE
