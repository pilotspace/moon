#!/bin/bash
exec > /tmp/monoio-central-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

echo '=== Moon monoio (central listener, no per-shard accept) ==='
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /tmp/moon.log 2>&1 &
sleep 3
head -5 /tmp/moon.log

echo '=== redis-cli tests ==='
for i in 1 2 3 4 5; do
    timeout 3 redis-cli -p 6399 SET "key$i" "val$i"
    echo "SET$i=$?"
done
timeout 3 redis-cli -p 6399 GET key3
echo "GET=$?"

echo '=== redis-benchmark p=1 c=1 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 1 -n 1000 -P 1 -t set,get -d 64 --csv -q
echo "B1=$?"

echo '=== redis-benchmark p=16 c=50 ==='
timeout 30 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 100000 -P 16 -t set,get -d 64 --csv -q
echo "B16=$?"

echo '=== redis-benchmark p=64 c=50 ==='
timeout 30 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 500000 -P 64 -t set,get -d 64 --csv -q
echo "B64=$?"

pkill -9 -f 'target/release/moon'
echo DONE
