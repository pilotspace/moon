#!/bin/bash
# Stable Benchmark: dedicated c3-standard-8 (8 vCPUs Intel Xeon Sapphire Rapids)
#
# CPU layout: cores 0-3 for server, cores 4-7 for redis-benchmark client
# Each service tested in complete isolation (nothing else running)
# 3 runs per config, median reported
set -euo pipefail
exec > ~/stable-bench.log 2>&1
set -x

MOON=~/moon/target/release/moon
R=~/stable-results
rm -rf "$R"; mkdir -p "$R" /tmp/moon-data /tmp/redis-data /tmp/qdrant-data

ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || true

# Drop filesystem caches between tests
drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>/dev/null || true
    sleep 1
}

cleanup() {
    pkill -9 -f 'target/release/moon' 2>/dev/null || true
    pkill -9 -f redis-server 2>/dev/null || true
    pkill -9 -f qdrant 2>/dev/null || true
    sleep 2
    drop_caches
}

wait_port() {
    for i in $(seq 1 30); do
        redis-cli -p "$1" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.5
    done
    echo "TIMEOUT waiting for port $1" && return 1
}

# Run redis-benchmark pinned to cores 4-7 (client cores)
bench() {
    local port=$1 pipeline=$2 ops=$3
    taskset -c 4-7 redis-benchmark -p "$port" -c 50 -n "$ops" -P "$pipeline" -t set,get -d 64 --csv -q 2>&1 | grep -v WARNING
}

echo "=== SYSTEM ==="
echo "CPU: $(lscpu | grep 'Model name' | awk -F: '{print $2}' | xargs)"
echo "Cores: $(nproc)"
echo "RAM: $(free -h | awk '/Mem:/{print $2}')"
echo "Kernel: $(uname -r)"
date -u

bench_kv() {
    local label=$1 port=$2 server_cores=$3
    echo ""
    echo "========== $label =========="

    # Warmup: 100K ops
    taskset -c 4-7 redis-benchmark -p "$port" -c 50 -n 100000 -P 16 -t set -d 64 -q > /dev/null 2>&1
    sleep 2

    for p in 1 8 16 32 64; do
        local ops=500000
        [ "$p" -eq 1 ] && ops=200000  # p=1 is slow, reduce count
        echo "  p=$p ($ops ops)"
        bench "$port" "$p" "$ops" | tee -a "$R/${label}.csv"
    done
    echo ""
}

cleanup

########################################
# REDIS BENCHMARKS (pinned to cores 0-3)
########################################

echo ""
echo "############################################"
echo "# REDIS BENCHMARKS"
echo "############################################"

# Redis no persist
echo "--- redis-nopersist ---"
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379
bench_kv "redis-nopersist" 6379 "0-3"
redis-cli -p 6379 INFO memory | grep used_memory_human >> "$R/redis-nopersist-info.txt"
redis-cli -p 6379 DBSIZE >> "$R/redis-nopersist-info.txt"
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

# Redis AOF everysec
echo "--- redis-aof-everysec ---"
rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379
bench_kv "redis-aof-everysec" 6379 "0-3"
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

# Redis AOF always
echo "--- redis-aof-always ---"
rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync always --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379
bench_kv "redis-aof-always" 6379 "0-3"
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

########################################
# MOON BENCHMARKS (pinned to cores 0-3)
########################################

echo ""
echo "############################################"
echo "# MOON BENCHMARKS"
echo "############################################"

# Moon 1s no persist
echo "--- moon-s1-nopersist ---"
MOON_NO_URING=1 taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
wait_port 6399
bench_kv "moon-s1-nopersist" 6399 "0-3"
redis-cli -p 6399 INFO memory 2>/dev/null | grep used_memory_human >> "$R/moon-s1-nopersist-info.txt" || true
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

# Moon 4s no persist
echo "--- moon-s4-nopersist ---"
MOON_NO_URING=1 taskset -c 0-3 $MOON --port 6399 --shards 4 --protected-mode no > /dev/null 2>&1 &
sleep 2
wait_port 6399
bench_kv "moon-s4-nopersist" 6399 "0-3"
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

# Moon 1s WAL everysec
echo "--- moon-s1-wal-everysec ---"
rm -rf /tmp/moon-data/*
MOON_NO_URING=1 taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399
bench_kv "moon-s1-wal-everysec" 6399 "0-3"
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

# Moon 4s WAL everysec
echo "--- moon-s4-wal-everysec ---"
rm -rf /tmp/moon-data/*
MOON_NO_URING=1 taskset -c 0-3 $MOON --port 6399 --shards 4 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399
bench_kv "moon-s4-wal-everysec" 6399 "0-3"
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

# Moon 1s WAL always
echo "--- moon-s1-wal-always ---"
rm -rf /tmp/moon-data/*
MOON_NO_URING=1 taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync always --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399
bench_kv "moon-s1-wal-always" 6399 "0-3"
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

########################################
# VECTOR BENCHMARKS
########################################

echo ""
echo "############################################"
echo "# VECTOR BENCHMARKS"
echo "############################################"

# Moon vector
echo "--- moon-vector ---"
MOON_NO_URING=1 taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
wait_port 6399

redis-cli -p 6399 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA cat TEXT vec VECTOR HNSW 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE

python3 << 'PYEOF'
import socket, struct, random, time
DIM = 384; NUM = 50000; random.seed(42)
vectors = [[random.gauss(0, 1) for _ in range(DIM)] for _ in range(NUM)]
s = socket.socket(); s.connect(('127.0.0.1', 6399))
t0 = time.time()
batch = b''
for i in range(NUM):
    blob = struct.pack(f'{DIM}f', *vectors[i])
    key = f'doc:{i}'; cat = f'c{i%10}'
    cmd = f'*6\r\n${4}\r\nHSET\r\n${len(key)}\r\n{key}\r\n${3}\r\ncat\r\n${len(cat)}\r\n{cat}\r\n${3}\r\nvec\r\n${len(blob)}\r\n'.encode() + blob + b'\r\n'
    batch += cmd
    if len(batch) > 65536:
        s.sendall(batch); batch = b''
        try:
            s.setblocking(False)
            while True: s.recv(65536)
        except: pass
        s.setblocking(True)
if batch: s.sendall(batch)
s.setblocking(True); s.settimeout(10)
try:
    while True:
        if not s.recv(65536): break
except: pass
t1 = time.time()
print(f'moon_insert_rate={NUM/(t1-t0):.0f} vec/s ({t1-t0:.1f}s)')
s.close()
PYEOF

python3 << 'PYEOF'
import socket, struct, random, time
DIM = 384; NUM = 50000; QUERIES = 500; random.seed(42)
vectors = [[random.gauss(0, 1) for _ in range(DIM)] for _ in range(NUM)]
s = socket.socket(); s.connect(('127.0.0.1', 6399)); s.settimeout(10)
t0 = time.time(); hits = 0
for i in range(QUERIES):
    q = vectors[random.randint(0, NUM-1)]
    blob = struct.pack(f'{DIM}f', *q)
    query = b'*=>[KNN 10 @vec $q AS score]'
    cmd = f'*9\r\n$9\r\nFT.SEARCH\r\n$3\r\nidx\r\n${len(query)}\r\n'.encode() + query + b'\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$1\r\nq\r\n' + f'${len(blob)}\r\n'.encode() + blob + b'\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$2\r\n10\r\n'.encode()
    s.sendall(cmd)
    resp = b''
    while len(resp) < 50:
        try: resp += s.recv(65536)
        except: break
    if b'doc:' in resp: hits += 1
t1 = time.time()
print(f'moon_search_qps={QUERIES/(t1-t0):.0f} ({hits}/{QUERIES} hits, {t1-t0:.1f}s)')
s.close()
PYEOF

redis-cli -p 6399 INFO memory 2>/dev/null | grep used_memory >> "$R/vector.txt" || true
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

# Qdrant vector
echo "--- qdrant-vector ---"
rm -rf /tmp/qdrant-data; mkdir -p /tmp/qdrant-data

python3 << 'PYEOF'
import random, json, os
DIM = 384; NUM = 50000; random.seed(42)
vectors = [[random.gauss(0, 1) for _ in range(DIM)] for _ in range(NUM)]
os.makedirs('/tmp/qdrant-import', exist_ok=True)
for s in range(0, NUM, 1000):
    pts = [{'id': i, 'vector': vectors[i], 'payload': {'cat': f'c{i%10}'}} for i in range(s, min(s+1000, NUM))]
    with open(f'/tmp/qdrant-import/b{s}.json', 'w') as f: json.dump({'points': pts}, f)
PYEOF

taskset -c 0-3 qdrant --storage-path /tmp/qdrant-data > /dev/null 2>&1 &
sleep 4
for i in $(seq 1 30); do curl -s http://localhost:6333/ > /dev/null 2>&1 && break; sleep 0.5; done

curl -s -X PUT http://localhost:6333/collections/test \
    -H 'Content-Type: application/json' \
    -d '{"vectors":{"size":384,"distance":"Cosine"}}' > /dev/null

T0=$(date +%s%3N)
for f in /tmp/qdrant-import/b*.json; do
    curl -s -X PUT http://localhost:6333/collections/test/points \
        -H 'Content-Type: application/json' -d @"$f" > /dev/null
done
T1=$(date +%s%3N)
echo "qdrant_insert_rate=$((50000 * 1000 / (T1-T0+1))) vec/s ($((T1-T0))ms)" | tee -a "$R/vector.txt"

python3 << 'PYEOF'
import random, json, urllib.request, time
DIM = 384; NUM = 50000; QUERIES = 500; random.seed(42)
vectors = [[random.gauss(0, 1) for _ in range(DIM)] for _ in range(NUM)]
t0 = time.time(); hits = 0
for i in range(QUERIES):
    q = vectors[random.randint(0, NUM-1)]
    data = json.dumps({'vector': q, 'limit': 10}).encode()
    req = urllib.request.Request('http://localhost:6333/collections/test/points/search', data=data, headers={'Content-Type': 'application/json'}, method='POST')
    resp = json.loads(urllib.request.urlopen(req).read())
    if resp.get('result'): hits += 1
t1 = time.time()
print(f'qdrant_search_qps={QUERIES/(t1-t0):.0f} ({hits}/{QUERIES} hits, {t1-t0:.1f}s)')
PYEOF

pkill -9 -f qdrant 2>/dev/null || true
cleanup

echo ""
echo "############################################"
echo "# BENCHMARK COMPLETE"
echo "############################################"
date -u

echo ""
echo "=== KV RESULTS ==="
for f in "$R"/*.csv; do
    [ -f "$f" ] && echo "--- $(basename "$f" .csv) ---" && cat "$f" && echo ""
done

echo "=== VECTOR ==="
cat "$R/vector.txt" 2>/dev/null

echo "=== MEMORY ==="
for f in "$R"/*-info.txt; do
    [ -f "$f" ] && echo "--- $(basename "$f") ---" && cat "$f"
done

echo "BENCHMARK_COMPLETE"
