#!/bin/bash
exec > /tmp/monoio-scale-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3

# Scale up clients progressively
for c in 1 5 10 25 50; do
    for p in 1 16 64; do
        n=$((c * p * 100))
        [ $n -lt 1000 ] && n=1000
        echo "=== c=$c p=$p n=$n ==="
        timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c $c -n $n -P $p -t set,get -d 64 --csv -q 2>&1
        echo "RC=$?"
    done
done

pkill -9 -f 'target/release/moon'
echo DONE
