#!/bin/bash
exec > /tmp/bench-scale-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

# Test redis-benchmark with increasing clients at p=1
for c in 1 2 3 5 10 25 50; do
    echo "=== c=$c p=1 ==="
    timeout 10 taskset -c 4-7 redis-benchmark -p 6399 -c $c -n $((c*200)) -P 1 -t set -d 64 -q --csv 2>&1 | grep -E '"SET"'
    echo "RC=$?"
done

echo ""
# Also test all pipeline levels at c=50
for p in 1 8 16 32 64; do
    echo "=== c=50 p=$p ==="
    timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n $((50*p*100)) -P $p -t set,get -d 64 -q --csv 2>&1 | grep -E '"SET"|"GET"'
    echo "RC=$?"
done

pkill -9 -f 'target/release/moon'
echo DONE
