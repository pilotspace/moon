#!/bin/bash
# Recovery test with separate data + offload dirs
MOON=$HOME/moon/target/release/moon
killall moon 2>/dev/null; sleep 1
rm -rf /tmp/mr-data /tmp/mr-offload

# Phase 1: Insert
echo "=== Insert 1000 keys ==="
$MOON --port 16379 --shards 1 --protected-mode no \
    --disk-offload enable --disk-offload-dir /tmp/mr-offload \
    --appendonly yes --appendfsync everysec --dir /tmp/mr-data > /dev/null 2>&1 &
sleep 2

python3 << 'PYEOF'
import redis, time
r = redis.Redis(host='127.0.0.1', port=16379, decode_responses=True)
N = 1000
for i in range(N):
    r.set(f'r:{i}', f'{i}-hello-world')
time.sleep(3)
pre = sum(1 for i in range(N) if r.get(f'r:{i}') is not None)
print(f'Before crash: {pre}/{N}')
PYEOF

# Phase 2: Crash
echo "=== SIGKILL ==="
kill -9 $(pgrep -f "port 16379") 2>/dev/null; sleep 1
echo "AOF file:"
ls -la /tmp/mr-data/appendonly.aof 2>/dev/null

# Phase 3: Recover
echo "=== Recovery ==="
$MOON --port 16379 --shards 1 --protected-mode no \
    --disk-offload enable --disk-offload-dir /tmp/mr-offload \
    --appendonly yes --appendfsync everysec --dir /tmp/mr-data > /dev/null 2>&1 &
sleep 5

python3 << 'PYEOF'
import redis
r = redis.Redis(host='127.0.0.1', port=16379, decode_responses=True)
N = 1000
post = sum(1 for i in range(N) if r.get(f'r:{i}') is not None)
correct = sum(1 for i in range(N) if r.get(f'r:{i}') == f'{i}-hello-world')
print(f'After recovery: {post}/{N} accessible, {correct}/{N} correct')
if post >= N:
    print('FULL RECOVERY!')
elif post > 0:
    print(f'PARTIAL: {post}/{N} ({N-post} lost to appendfsync window)')
else:
    print('BROKEN: 0 recovered')
PYEOF

killall moon 2>/dev/null
rm -rf /tmp/mr-data /tmp/mr-offload
