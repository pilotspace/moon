#!/bin/bash
exec > /tmp/monoio-quick-result.txt 2>&1
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /tmp/moon-monoio.log 2>&1 &
sleep 3
cat /tmp/moon-monoio.log

echo '=== COMMANDS ==='
timeout 3 redis-cli -p 6399 PING
echo "PING=$?"
timeout 3 redis-cli -p 6399 SET foo bar
echo "SET=$?"
timeout 3 redis-cli -p 6399 GET foo
echo "GET=$?"

echo '=== BENCHMARK p=1 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 10000 -P 1 -t set,get -d 64 --csv -q
echo "BENCH1=$?"

echo '=== BENCHMARK p=16 ==='
timeout 15 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 100000 -P 16 -t set,get -d 64 --csv -q
echo "BENCH16=$?"

echo '=== BENCHMARK p=64 ==='
timeout 30 taskset -c 4-7 redis-benchmark -p 6399 -c 50 -n 500000 -P 64 -t set,get -d 64 --csv -q
echo "BENCH64=$?"

pkill -9 -f 'target/release/moon'
echo DONE
