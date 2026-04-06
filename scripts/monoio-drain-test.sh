#!/bin/bash
exec > /tmp/monoio-drain-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

echo '=== Moon monoio (conn_rx drain fix) ==='
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /tmp/moon.log 2>&1 &
sleep 3
head -5 /tmp/moon.log

echo '=== Functional ==='
for i in 1 2 3; do
    timeout 3 redis-cli -p 6399 SET "k$i" "v$i"
    echo "SET$i=$?"
done
timeout 3 redis-cli -p 6399 GET k2
echo "GET=$?"

echo '=== Benchmark p=1 c=50 ==='
timeout 20 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 200000 -P 1 -t set,get -d 64 --csv -q
echo "B1=$?"

echo '=== Benchmark p=8 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 500000 -P 8 -t set,get -d 64 --csv -q
echo "B8=$?"

echo '=== Benchmark p=16 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 500000 -P 16 -t set,get -d 64 --csv -q
echo "B16=$?"

echo '=== Benchmark p=64 ==='
timeout 30 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 500000 -P 64 -t set,get -d 64 --csv -q
echo "B64=$?"

pkill -9 -f 'target/release/moon'
echo DONE
