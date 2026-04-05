#!/bin/bash
exec > ~/bench-final.log 2>&1
set -x

pkill -9 -f 'target/release/moon' 2>/dev/null
pkill -9 -f redis-server 2>/dev/null
pkill -9 -f qdrant 2>/dev/null
sleep 2
ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || true

MOON=~/moon/target/release/moon
R=~/bench-final
rm -rf $R; mkdir -p $R /tmp/moon-data /tmp/redis-data

echo '=== SANITY ==='
MOON_NO_URING=1 $MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
redis-benchmark -p 6399 -c 10 -n 1000 -t ping -q
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

echo '=== S1: NO PERSISTENCE ==='
redis-server --port 6379 --save '' --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
for p in 1 8 16 32 64; do
  redis-benchmark -p 6379 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s1-redis.csv
done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; sleep 1

MOON_NO_URING=1 $MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
for p in 1 8 16 32 64; do
  redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s1-moon-s1.csv
done
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

MOON_NO_URING=1 $MOON --port 6399 --shards 4 --protected-mode no > /dev/null 2>&1 &
sleep 2
for p in 1 8 16 32 64; do
  redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s1-moon-s4.csv
done
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

echo '=== S2: PERSISTENCE ==='
rm -rf /tmp/redis-data/*
redis-server --port 6379 --save '' --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
for p in 1 8 16 32 64; do
  redis-benchmark -p 6379 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s2-redis-everysec.csv
done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; sleep 1

rm -rf /tmp/redis-data/*
redis-server --port 6379 --save '' --appendonly yes --appendfsync always --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
for p in 1 8 16 32 64; do
  redis-benchmark -p 6379 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s2-redis-always.csv
done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; sleep 1

rm -rf /tmp/moon-data/*
MOON_NO_URING=1 $MOON --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
for p in 1 8 16 32 64; do
  redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s2-moon-s1-everysec.csv
done
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

rm -rf /tmp/moon-data/*
MOON_NO_URING=1 $MOON --port 6399 --shards 4 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
for p in 1 8 16 32 64; do
  redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s2-moon-s4-everysec.csv
done
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

rm -rf /tmp/moon-data/*
MOON_NO_URING=1 $MOON --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync always --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 2
for p in 1 8 16 32 64; do
  redis-benchmark -p 6399 -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | tee -a $R/s2-moon-s1-always.csv
done
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

echo '=== S3: VECTOR ==='
python3 << 'PYEOF'
import random, json, os
DIM=384; NUM=50000; random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]
os.makedirs('/tmp/qdrant-import', exist_ok=True)
for s in range(0, NUM, 1000):
    pts = [{'id':i, 'vector':vectors[i], 'payload':{'cat':f'c{i%10}'}} for i in range(s, min(s+1000,NUM))]
    with open(f'/tmp/qdrant-import/b{s}.json','w') as f: json.dump({'points':pts}, f)
print('GENERATED')
PYEOF

MOON_NO_URING=1 $MOON --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 2
redis-cli -p 6399 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA cat TEXT vec VECTOR HNSW 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE

python3 << 'PYEOF'
import socket, struct, random, time
DIM=384; NUM=50000; random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]
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
s.setblocking(True); s.settimeout(5)
try:
    while True:
        if not s.recv(65536): break
except: pass
t1 = time.time()
print(f'moon_insert={NUM/(t1-t0):.0f} vec/s ({t1-t0:.1f}s)')
s.close()
PYEOF

python3 << 'PYEOF'
import socket, struct, random, time
DIM=384; NUM=50000; random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]
s = socket.socket(); s.connect(('127.0.0.1', 6399)); s.settimeout(10)
t0 = time.time(); hits = 0
for i in range(100):
    q = vectors[random.randint(0,NUM-1)]
    blob = struct.pack(f'{DIM}f', *q)
    query_str = '*=>[KNN 10 @vec $q AS score]'
    query = query_str.encode()
    cmd = f'*9\r\n$9\r\nFT.SEARCH\r\n$3\r\nidx\r\n${len(query)}\r\n'.encode() + query + b'\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$1\r\nq\r\n' + f'${len(blob)}\r\n'.encode() + blob + b'\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$2\r\n10\r\n'.encode()
    s.sendall(cmd)
    resp = b''
    while len(resp) < 50:
        try: resp += s.recv(65536)
        except: break
    if b'doc:' in resp: hits += 1
t1 = time.time()
print(f'moon_search={100/(t1-t0):.0f} QPS ({hits}/100 hits)')
s.close()
PYEOF
pkill -9 -f 'target/release/moon' 2>/dev/null; sleep 1

rm -rf /tmp/qdrant-data
qdrant --storage-path /tmp/qdrant-data > /dev/null 2>&1 &
sleep 3
curl -s -X PUT http://localhost:6333/collections/test -H 'Content-Type: application/json' -d '{"vectors":{"size":384,"distance":"Cosine"}}' > /dev/null
T0=$(date +%s%3N)
for f in /tmp/qdrant-import/b*.json; do curl -s -X PUT http://localhost:6333/collections/test/points -H 'Content-Type: application/json' -d @$f > /dev/null; done
T1=$(date +%s%3N)
echo "qdrant_insert=$((50000 * 1000 / (T1-T0+1))) vec/s ($((T1-T0))ms)" | tee -a $R/s3-vector.txt

python3 << 'PYEOF'
import random, json, urllib.request, time
DIM=384; NUM=50000; random.seed(42)
vectors = [[random.gauss(0,1) for _ in range(DIM)] for _ in range(NUM)]
t0=time.time(); hits=0
for i in range(100):
    q=vectors[random.randint(0,NUM-1)]
    data=json.dumps({'vector':q,'limit':10}).encode()
    req=urllib.request.Request('http://localhost:6333/collections/test/points/search',data=data,headers={'Content-Type':'application/json'},method='POST')
    resp=json.loads(urllib.request.urlopen(req).read())
    if resp.get('result'): hits+=1
t1=time.time()
print(f'qdrant_search={100/(t1-t0):.0f} QPS ({hits}/100 hits)')
PYEOF
pkill -9 -f qdrant; sleep 1

echo '=== ALL DONE ==='
echo '--- S1 Redis ---'; cat $R/s1-redis.csv 2>/dev/null
echo '--- S1 Moon s1 ---'; cat $R/s1-moon-s1.csv 2>/dev/null
echo '--- S1 Moon s4 ---'; cat $R/s1-moon-s4.csv 2>/dev/null
echo '--- S2 Redis everysec ---'; cat $R/s2-redis-everysec.csv 2>/dev/null
echo '--- S2 Redis always ---'; cat $R/s2-redis-always.csv 2>/dev/null
echo '--- S2 Moon s1 everysec ---'; cat $R/s2-moon-s1-everysec.csv 2>/dev/null
echo '--- S2 Moon s4 everysec ---'; cat $R/s2-moon-s4-everysec.csv 2>/dev/null
echo '--- S2 Moon s1 always ---'; cat $R/s2-moon-s1-always.csv 2>/dev/null
echo '--- S3 Vector ---'; cat $R/s3-vector.txt 2>/dev/null
echo 'BENCHMARK_COMPLETE'
