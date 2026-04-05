#!/usr/bin/env bash
set -euo pipefail

OUT=/tmp/bench-live.txt
> "$OUT"

redis-cli -p 6400 FLUSHALL > /dev/null 2>&1 || true
redis-cli -p 6399 FLUSHALL > /dev/null 2>&1 || true

echo "=== LIVE BENCHMARK $(date) ===" >> "$OUT"
echo "" >> "$OUT"

# p=1 GET
echo "--- Redis GET p=1 c=50 n=100K ---" >> "$OUT"
redis-benchmark -p 6399 -c 50 -n 100000 -t get --csv 2>&1 | grep -v "^\"test\"" >> "$OUT"

echo "--- Moon GET p=1 c=50 n=100K ---" >> "$OUT"
timeout 8 redis-benchmark -p 6400 -c 50 -n 100000 -t get 2>&1 | tr '\r' '\n' | grep -v "nan" | grep "overall:" | head -1 >> "$OUT" || echo "TIMEOUT" >> "$OUT"

# p=1 SPOP
echo "--- Redis SPOP p=1 ---" >> "$OUT"
redis-benchmark -p 6399 -c 50 -n 100000 -t spop --csv 2>&1 | grep -v "^\"test\"" >> "$OUT"

echo "--- Moon SPOP p=1 ---" >> "$OUT"
timeout 8 redis-benchmark -p 6400 -c 50 -n 100000 -t spop 2>&1 | tr '\r' '\n' | grep -v "nan" | grep "overall:" | head -1 >> "$OUT" || echo "TIMEOUT" >> "$OUT"

# p=16
echo "--- Redis SET p=16 c=50 n=200K ---" >> "$OUT"
redis-benchmark -p 6399 -c 50 -n 200000 -t set -P 16 --csv 2>&1 | grep -v "^\"test\"" >> "$OUT"

echo "--- Moon SET p=16 ---" >> "$OUT"
timeout 10 redis-benchmark -p 6400 -c 50 -n 200000 -t set -P 16 2>&1 | tr '\r' '\n' | grep -v "nan" | grep "overall:" | head -1 >> "$OUT" || echo "TIMEOUT" >> "$OUT"

echo "--- Redis GET p=16 ---" >> "$OUT"
redis-benchmark -p 6399 -c 50 -n 200000 -t get -P 16 --csv 2>&1 | grep -v "^\"test\"" >> "$OUT"

echo "--- Moon GET p=16 ---" >> "$OUT"
timeout 10 redis-benchmark -p 6400 -c 50 -n 200000 -t get -P 16 2>&1 | tr '\r' '\n' | grep -v "nan" | grep "overall:" | head -1 >> "$OUT" || echo "TIMEOUT" >> "$OUT"

# p=64 high throughput
echo "--- Redis SET p=64 c=100 n=1M ---" >> "$OUT"
redis-benchmark -p 6399 -c 100 -n 1000000 -t set -P 64 --csv 2>&1 | grep -v "^\"test\"" >> "$OUT"

echo "--- Moon SET p=64 c=100 n=1M ---" >> "$OUT"
timeout 10 redis-benchmark -p 6400 -c 100 -n 1000000 -t set -P 64 2>&1 | tr '\r' '\n' | grep -v "nan" | grep "overall:" | head -1 >> "$OUT" || echo "TIMEOUT" >> "$OUT"

echo "--- Redis GET p=64 c=100 n=1M ---" >> "$OUT"
redis-benchmark -p 6399 -c 100 -n 1000000 -t get -P 64 --csv 2>&1 | grep -v "^\"test\"" >> "$OUT"

echo "--- Moon GET p=64 c=100 n=1M ---" >> "$OUT"
timeout 10 redis-benchmark -p 6400 -c 100 -n 1000000 -t get -P 64 2>&1 | tr '\r' '\n' | grep -v "nan" | grep "overall:" | head -1 >> "$OUT" || echo "TIMEOUT" >> "$OUT"

echo "" >> "$OUT"
echo "=== DONE ===" >> "$OUT"

cat "$OUT"
