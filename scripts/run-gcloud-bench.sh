#!/bin/bash
# Self-contained benchmark: runs all 3 scenarios, writes results to /tmp/bench-results/
set -euo pipefail

MOON="$HOME/moon/target/release/moon"
R="$HOME/bench-results"
rm -rf "$R" /tmp/moon-data /tmp/redis-data /tmp/qdrant-data
mkdir -p "$R" /tmp/moon-data /tmp/redis-data /tmp/qdrant-data

ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || true

pkill -9 -f 'moon --port' 2>/dev/null || true
pkill -9 -f redis-server 2>/dev/null || true
pkill -9 -f qdrant 2>/dev/null || true
sleep 1

echo "=== INSTANCE INFO ==="
echo "CPU: $(lscpu | grep 'Model name' | awk -F: '{print $2}' | xargs)"
echo "Cores: $(nproc)"
echo "RAM: $(free -h | awk '/Mem:/{print $2}')"
echo "Kernel: $(uname -r)"
echo ""

wait_port() {
    for i in $(seq 1 30); do
        redis-cli -p "$1" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.5
    done
    echo "TIMEOUT waiting for port $1" && return 1
}

# ============================
# SCENARIO 1: No Persistence
# ============================
echo "========== SCENARIO 1: NO PERSISTENCE =========="

# --- Redis no persist ---
echo "--- Redis (no persist) ---"
redis-server --port 6379 --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6379 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s1-redis-nopersist.csv"
done
redis-cli -p 6379 DBSIZE >> "$R/s1-redis-info.txt"
redis-cli -p 6379 INFO memory | grep used_memory_human >> "$R/s1-redis-info.txt"
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

# --- Moon no persist (1 shard) ---
echo "--- Moon (no persist, 1 shard) ---"
$MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
wait_port 6399

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s1-moon-s1-nopersist.csv"
done
redis-cli -p 6399 DBSIZE >> "$R/s1-moon-s1-info.txt" 2>/dev/null || true
redis-cli -p 6399 INFO memory | grep used_memory_human >> "$R/s1-moon-s1-info.txt" 2>/dev/null || true
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# --- Moon no persist (4 shards) ---
echo "--- Moon (no persist, 4 shards) ---"
$MOON --port 6399 --shards 4 --protected-mode no > /dev/null 2>&1 &
sleep 2
wait_port 6399

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s1-moon-s4-nopersist.csv"
done
redis-cli -p 6399 DBSIZE >> "$R/s1-moon-s4-info.txt" 2>/dev/null || true
redis-cli -p 6399 INFO memory | grep used_memory_human >> "$R/s1-moon-s4-info.txt" 2>/dev/null || true
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# ============================
# SCENARIO 2: Persistence
# ============================
echo ""
echo "========== SCENARIO 2: PERSISTENCE =========="

# --- Redis AOF everysec ---
echo "--- Redis (AOF everysec) ---"
rm -rf /tmp/redis-data/*
redis-server --port 6379 --save "" --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6379 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s2-redis-aof-everysec.csv"
done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

# --- Redis AOF always ---
echo "--- Redis (AOF always) ---"
rm -rf /tmp/redis-data/*
redis-server --port 6379 --save "" --appendonly yes --appendfsync always --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6379 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s2-redis-aof-always.csv"
done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

# --- Moon WAL everysec (1 shard) ---
echo "--- Moon (WAL everysec, 1 shard) ---"
rm -rf /tmp/moon-data/*
$MOON --port 6399 --shards 1 --protected-mode no --aof-enabled --appendfsync everysec --data-dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s2-moon-s1-wal-everysec.csv"
done
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# --- Moon WAL everysec (4 shards) ---
echo "--- Moon (WAL everysec, 4 shards) ---"
rm -rf /tmp/moon-data/*
$MOON --port 6399 --shards 4 --protected-mode no --aof-enabled --appendfsync everysec --data-dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s2-moon-s4-wal-everysec.csv"
done
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# --- Moon WAL always (1 shard) ---
echo "--- Moon (WAL always, 1 shard) ---"
rm -rf /tmp/moon-data/*
$MOON --port 6399 --shards 1 --protected-mode no --aof-enabled --appendfsync always --data-dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s2-moon-s1-wal-always.csv"
done
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# --- Moon WAL always (4 shards) ---
echo "--- Moon (WAL always, 4 shards) ---"
rm -rf /tmp/moon-data/*
$MOON --port 6399 --shards 4 --protected-mode no --aof-enabled --appendfsync always --data-dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
wait_port 6399

for p in 1 8 16 32 64; do
    echo "Pipeline=$p"
    redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a "$R/s2-moon-s4-wal-always.csv"
done
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# ============================
# SCENARIO 3: Vector Search
# ============================
echo ""
echo "========== SCENARIO 3: VECTOR SEARCH =========="

DIM=384
NUM=50000

# Generate vectors with Python
python3 -c "
import random, struct, json, time, os

DIM=$DIM; NUM=$NUM
random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]

# Redis/Moon RESP pipeline
with open('/tmp/vec-pipe.txt','w') as f:
    for i,v in enumerate(vectors):
        blob = struct.pack(f'{DIM}f', *v)
        # Write as redis-cli pipe format
        args = ['HSET', f'doc:{i}', 'cat', f'c{i%10}']
        args.append('vec')
        f.write(f'{len(args)+1}\n')
        for a in args:
            f.write(f'{a}\n')
        f.write(f'BLOB:{blob.hex()}\n')

# Save raw vectors for search queries
with open('/tmp/vec-queries.bin','wb') as f:
    for i in range(100):
        v = vectors[random.randint(0, NUM-1)]
        f.write(struct.pack(f'{DIM}f', *v))

# Qdrant batches
os.makedirs('/tmp/qdrant-import', exist_ok=True)
bs = 1000
for s in range(0, NUM, bs):
    e = min(s+bs, NUM)
    pts = [{'id':i, 'vector':vectors[i], 'payload':{'cat':f'c{i%10}'}} for i in range(s,e)]
    with open(f'/tmp/qdrant-import/b{s}.json','w') as f:
        json.dump({'points':pts}, f)

print(f'Generated {NUM} vectors dim={DIM}')
"

# --- Moon vector ---
echo "--- Moon vector search ---"
rm -rf /tmp/moon-data/*
$MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
wait_port 6399

redis-cli -p 6399 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA cat TEXT vec VECTOR HNSW 6 TYPE FLOAT32 DIM $DIM DISTANCE_METRIC COSINE 2>/dev/null

# Insert via pipeline
MOON_T0=$(date +%s%3N)
for i in $(seq 0 $((NUM-1))); do
    cat_val="c$((i % 10))"
    redis-cli -p 6399 HSET "doc:$i" cat "$cat_val" vec "$(python3 -c "
import random,struct
random.seed(42)
vs=[[random.gauss(0,1) for _ in range($DIM)] for _ in range($((i+1)))]
v=vs[$i]
print(struct.pack(f'${DIM}f',*v).hex())
")" > /dev/null 2>&1
done &
MOON_INSERT_PID=$!

# Actually this per-vector insert with python is too slow. Use a bulk approach.
kill $MOON_INSERT_PID 2>/dev/null || true

# Bulk insert with python
python3 -c "
import socket, struct, random, time

DIM=$DIM; NUM=$NUM
random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6399))

t0 = time.time()
batch = b''
for i in range(NUM):
    blob = struct.pack(f'{DIM}f', *vectors[i])
    cat_val = f'c{i%10}'
    key = f'doc:{i}'
    cmd = f'*6\r\n\$4\r\nHSET\r\n\${len(key)}\r\n{key}\r\n\$3\r\ncat\r\n\${len(cat_val)}\r\n{cat_val}\r\n\$3\r\nvec\r\n\${len(blob)}\r\n'.encode() + blob + b'\r\n'
    batch += cmd
    if len(batch) > 65536:
        s.sendall(batch)
        batch = b''
        # Drain responses
        try:
            s.setblocking(False)
            while True:
                s.recv(65536)
        except:
            pass
        s.setblocking(True)

if batch:
    s.sendall(batch)

# Drain all remaining responses
s.setblocking(True)
s.settimeout(5)
try:
    while True:
        data = s.recv(65536)
        if not data:
            break
except:
    pass

t1 = time.time()
print(f'moon_insert_sec={t1-t0:.2f}')
print(f'moon_insert_rate={NUM/(t1-t0):.0f} vec/s')
s.close()
" 2>&1 | tee -a "$R/s3-vector.txt"

# Search
python3 -c "
import socket, struct, random, time

DIM=$DIM; NUM=$NUM
random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]
QUERIES = 100

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6399))
s.settimeout(10)

t0 = time.time()
hits = 0
for i in range(QUERIES):
    qvec = vectors[random.randint(0, NUM-1)]
    blob = struct.pack(f'{DIM}f', *qvec)
    # FT.SEARCH idx '*=>[KNN 10 @vec \$q AS score]' PARAMS 2 q <blob> LIMIT 0 10
    query = b'*=>[KNN 10 @vec \$q AS score]'
    params_key = b'q'
    cmd = (
        f'*9\r\n\$9\r\nFT.SEARCH\r\n\$3\r\nidx\r\n'
        f'\${len(query)}\r\n'.encode() + query + b'\r\n'
        f'\$6\r\nPARAMS\r\n\$1\r\n2\r\n'
        f'\$1\r\nq\r\n'
        f'\${len(blob)}\r\n'.encode() + blob + b'\r\n'
        f'\$5\r\nLIMIT\r\n\$1\r\n0\r\n\$2\r\n10\r\n'.encode()
    )
    s.sendall(cmd)
    resp = b''
    while b'\r\n' in resp or len(resp) < 10:
        try:
            chunk = s.recv(65536)
            if not chunk: break
            resp += chunk
            if resp.count(b'\r\n') > 5: break
        except:
            break
    if b'doc:' in resp:
        hits += 1

t1 = time.time()
qps = QUERIES / (t1 - t0)
print(f'moon_search_queries={QUERIES}')
print(f'moon_search_sec={t1-t0:.2f}')
print(f'moon_search_qps={qps:.0f}')
print(f'moon_search_hits={hits}/{QUERIES}')
s.close()
" 2>&1 | tee -a "$R/s3-vector.txt"

redis-cli -p 6399 INFO memory 2>/dev/null | grep used_memory_human >> "$R/s3-vector.txt" || true
pkill -9 -f 'moon --port' 2>/dev/null || true
sleep 1

# --- Redis vector (check if FT module available) ---
echo "--- Redis vector search ---"
redis-server --port 6379 --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379

if redis-cli -p 6379 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA cat TEXT vec VECTOR HNSW 6 TYPE FLOAT32 DIM $DIM DISTANCE_METRIC COSINE 2>&1 | grep -qi "unknown\|ERR"; then
    echo "redis_vector=NOT_AVAILABLE (no RediSearch module)" | tee -a "$R/s3-vector.txt"
else
    echo "Redis FT module available - benchmarking..."
    # Same bulk insert for Redis
    python3 -c "
import socket, struct, random, time

DIM=$DIM; NUM=$NUM
random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6379))

t0 = time.time()
batch = b''
for i in range(NUM):
    blob = struct.pack(f'{DIM}f', *vectors[i])
    cat_val = f'c{i%10}'
    key = f'doc:{i}'
    cmd = f'*6\r\n\$4\r\nHSET\r\n\${len(key)}\r\n{key}\r\n\$3\r\ncat\r\n\${len(cat_val)}\r\n{cat_val}\r\n\$3\r\nvec\r\n\${len(blob)}\r\n'.encode() + blob + b'\r\n'
    batch += cmd
    if len(batch) > 65536:
        s.sendall(batch)
        batch = b''
        try:
            s.setblocking(False)
            while True: s.recv(65536)
        except: pass
        s.setblocking(True)
if batch: s.sendall(batch)
s.setblocking(True); s.settimeout(5)
try:
    while True:
        if not s.recv(65536): break
except: pass
t1 = time.time()
print(f'redis_insert_sec={t1-t0:.2f}')
print(f'redis_insert_rate={NUM/(t1-t0):.0f} vec/s')
s.close()
" 2>&1 | tee -a "$R/s3-vector.txt"
fi
redis-cli -p 6379 INFO memory 2>/dev/null | grep used_memory_human >> "$R/s3-vector.txt" || true
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

# --- Qdrant ---
echo "--- Qdrant vector search ---"
rm -rf /tmp/qdrant-data/*
qdrant --storage-path /tmp/qdrant-data > /dev/null 2>&1 &
sleep 3

# Wait for Qdrant HTTP
for i in $(seq 1 30); do
    curl -s http://localhost:6333/ >/dev/null 2>&1 && break
    sleep 0.5
done

curl -s -X PUT http://localhost:6333/collections/test \
    -H "Content-Type: application/json" \
    -d "{\"vectors\":{\"size\":$DIM,\"distance\":\"Cosine\"}}" > /dev/null

# Insert batches
QDRANT_T0=$(date +%s%3N)
for f in /tmp/qdrant-import/b*.json; do
    curl -s -X PUT http://localhost:6333/collections/test/points \
        -H "Content-Type: application/json" -d @"$f" > /dev/null
done
QDRANT_T1=$(date +%s%3N)
QDRANT_INSERT_MS=$((QDRANT_T1 - QDRANT_T0))
echo "qdrant_insert_ms=$QDRANT_INSERT_MS" | tee -a "$R/s3-vector.txt"
echo "qdrant_insert_rate=$((NUM * 1000 / (QDRANT_INSERT_MS + 1))) vec/s" | tee -a "$R/s3-vector.txt"

# Search
python3 -c "
import random, json, urllib.request, time

DIM=$DIM; NUM=$NUM
random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]
QUERIES=100

t0 = time.time()
hits = 0
for i in range(QUERIES):
    q = vectors[random.randint(0, NUM-1)]
    data = json.dumps({'vector': q, 'limit': 10}).encode()
    req = urllib.request.Request(
        'http://localhost:6333/collections/test/points/search',
        data=data, headers={'Content-Type':'application/json'}, method='POST')
    resp = json.loads(urllib.request.urlopen(req).read())
    if resp.get('result'): hits += 1
t1 = time.time()
print(f'qdrant_search_queries={QUERIES}')
print(f'qdrant_search_sec={t1-t0:.2f}')
print(f'qdrant_search_qps={QUERIES/(t1-t0):.0f}')
print(f'qdrant_search_hits={hits}/{QUERIES}')
" 2>&1 | tee -a "$R/s3-vector.txt"

pkill -9 -f qdrant 2>/dev/null || true
sleep 1

# ============================
# FINAL REPORT
# ============================
echo ""
echo "========== ALL BENCHMARKS COMPLETE =========="
echo "Results in: $R"
echo ""
echo "--- Result files ---"
ls -la "$R"/
echo ""
echo "--- KV Benchmark Data ---"
for f in "$R"/s1-*.csv "$R"/s2-*.csv; do
    [ -f "$f" ] && echo "=== $(basename $f) ===" && cat "$f" && echo ""
done
echo ""
echo "--- Vector Data ---"
cat "$R/s3-vector.txt" 2>/dev/null
echo ""
echo "BENCHMARK_COMPLETE"
