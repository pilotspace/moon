#!/bin/bash
exec > /tmp/monoio-final-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

echo '=== Starting Moon (monoio, inline accept) ==='
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /tmp/moon.log 2>&1 &
sleep 3
head -5 /tmp/moon.log

echo '=== redis-cli tests ==='
timeout 3 redis-cli -p 6399 SET foo bar
echo "SET=$?"
timeout 3 redis-cli -p 6399 GET foo
echo "GET=$?"

echo '=== redis-benchmark p=1 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 1 -n 100 -P 1 -t set -d 64 -q --csv
echo "BENCH1=$?"

echo '=== redis-benchmark p=16 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 100000 -P 16 -t set,get -d 64 -q --csv
echo "BENCH16=$?"

echo '=== redis-benchmark p=64 ==='
timeout 30 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 500000 -P 64 -t set,get -d 64 -q --csv
echo "BENCH64=$?"

pkill -9 -f 'target/release/moon'
echo DONE
